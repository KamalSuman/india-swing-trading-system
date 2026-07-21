from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from dataclasses import dataclass, field, fields
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.identity import content_id
from india_swing.risk.swing_portfolio import SwingPortfolioSnapshot


PORTFOLIO_EVIDENCE_SCHEMA_VERSION = "swing-portfolio-evidence/v1"
PORTFOLIO_ARTIFACT_SCHEMA_VERSION = "swing-portfolio-artifact/v1"
PORTFOLIO_CODEC_VERSION = "swing-portfolio-artifact-json/v1"
MAXIMUM_PORTFOLIO_ARTIFACT_BYTES = 256 * 1024

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class SwingPortfolioArtifactError(ValueError):
    pass


class SwingPortfolioArtifactNotFound(SwingPortfolioArtifactError):
    pass


class SwingPortfolioEvidenceKind(str, Enum):
    BROKER_FUNDS = "BROKER_FUNDS"
    BROKER_POSITIONS = "BROKER_POSITIONS"
    ENGINE_RISK_LEDGER = "ENGINE_RISK_LEDGER"
    ENGINE_PNL_LEDGER = "ENGINE_PNL_LEDGER"


class SwingPortfolioVerificationStatus(str, Enum):
    MANUAL_RECONCILED_PAPER_ONLY = "MANUAL_RECONCILED_PAPER_ONLY"


def _sha(value: object, name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise SwingPortfolioArtifactError(f"{name} must be a lowercase SHA-256")
    return value


def _utc(value: object, name: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is None:
        raise SwingPortfolioArtifactError(f"{name} must be timezone-aware")
    try:
        offset = value.utcoffset()
    except Exception:
        raise SwingPortfolioArtifactError(f"{name} has invalid timezone behavior") from None
    if offset is None:
        raise SwingPortfolioArtifactError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class SwingPortfolioEvidenceBinding:
    kind: SwingPortfolioEvidenceKind
    evidence_id: str
    observed_at: datetime
    source_version: str
    schema_version: str = PORTFOLIO_EVIDENCE_SCHEMA_VERSION
    binding_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.kind) is not SwingPortfolioEvidenceKind:
            raise SwingPortfolioArtifactError("portfolio evidence kind must be exact")
        _sha(self.evidence_id, "evidence_id")
        object.__setattr__(self, "observed_at", _utc(self.observed_at, "observed_at"))
        if (
            type(self.source_version) is not str
            or not self.source_version
            or self.source_version != self.source_version.strip()
            or len(self.source_version) > 128
        ):
            raise SwingPortfolioArtifactError("portfolio evidence source version is invalid")
        if self.schema_version != PORTFOLIO_EVIDENCE_SCHEMA_VERSION:
            raise SwingPortfolioArtifactError("unsupported portfolio evidence schema")
        object.__setattr__(self, "binding_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "binding_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.binding_id != self._calculated_id():
            raise SwingPortfolioArtifactError("portfolio evidence identity failed")


@dataclass(frozen=True, slots=True)
class SwingPortfolioSnapshotArtifact:
    portfolio: SwingPortfolioSnapshot
    portfolio_snapshot_id: str
    evidence: tuple[SwingPortfolioEvidenceBinding, ...]
    reconciled_at: datetime
    verification_status: SwingPortfolioVerificationStatus = (
        SwingPortfolioVerificationStatus.MANUAL_RECONCILED_PAPER_ONLY
    )
    mode: str = "PAPER_ONLY"
    schema_version: str = PORTFOLIO_ARTIFACT_SCHEMA_VERSION
    artifact_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.portfolio) is not SwingPortfolioSnapshot:
            raise SwingPortfolioArtifactError("portfolio snapshot must be exact")
        self.portfolio.verify_content_identity()
        _sha(self.portfolio_snapshot_id, "portfolio_snapshot_id")
        if self.portfolio_snapshot_id != self.portfolio.portfolio_snapshot_id:
            raise SwingPortfolioArtifactError("portfolio snapshot identity differs")
        if (
            type(self.evidence) is not tuple
            or any(type(value) is not SwingPortfolioEvidenceBinding for value in self.evidence)
        ):
            raise SwingPortfolioArtifactError("portfolio evidence must be an exact tuple")
        expected_kinds = tuple(SwingPortfolioEvidenceKind)
        if tuple(value.kind for value in self.evidence) != expected_kinds:
            raise SwingPortfolioArtifactError(
                "portfolio evidence must contain every required kind in canonical order"
            )
        for value in self.evidence:
            value.verify_content_identity()
        if len({value.evidence_id for value in self.evidence}) != len(self.evidence):
            raise SwingPortfolioArtifactError("portfolio evidence IDs must be unique")
        object.__setattr__(self, "reconciled_at", _utc(self.reconciled_at, "reconciled_at"))
        if (
            self.reconciled_at != self.portfolio.as_of
            or any(value.observed_at > self.reconciled_at for value in self.evidence)
        ):
            raise SwingPortfolioArtifactError("portfolio reconciliation time is inconsistent")
        if type(self.verification_status) is not SwingPortfolioVerificationStatus:
            raise SwingPortfolioArtifactError("portfolio verification status must be exact")
        if (
            self.verification_status
            is not SwingPortfolioVerificationStatus.MANUAL_RECONCILED_PAPER_ONLY
            or self.mode != "PAPER_ONLY"
            or self.schema_version != PORTFOLIO_ARTIFACT_SCHEMA_VERSION
        ):
            raise SwingPortfolioArtifactError("portfolio artifact authority is invalid")
        object.__setattr__(self, "artifact_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "artifact_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self.portfolio.verify_content_identity()
        for value in self.evidence:
            value.verify_content_identity()
        if self.artifact_id != self._calculated_id():
            raise SwingPortfolioArtifactError("portfolio artifact content identity failed")


def _decimal(value: Decimal) -> str:
    if type(value) is not Decimal or not value.is_finite():
        raise SwingPortfolioArtifactError("portfolio contains an invalid Decimal")
    return str(value)


def _artifact_data(value: SwingPortfolioSnapshotArtifact) -> dict[str, object]:
    value.verify_content_identity()
    portfolio = value.portfolio
    return {
        "artifact_id": value.artifact_id,
        "evidence": [
            {
                "binding_id": item.binding_id,
                "evidence_id": item.evidence_id,
                "kind": item.kind.value,
                "observed_at": item.observed_at.isoformat(),
                "schema_version": item.schema_version,
                "source_version": item.source_version,
            }
            for item in value.evidence
        ],
        "mode": value.mode,
        "portfolio": {
            "as_of": portfolio.as_of.isoformat(),
            "capital": _decimal(portfolio.capital),
            "cash_available": _decimal(portfolio.cash_available),
            "currency": portfolio.currency,
            "daily_realized_pnl": _decimal(portfolio.daily_realized_pnl),
            "gross_exposure": _decimal(portfolio.gross_exposure),
            "open_positions": portfolio.open_positions,
            "open_risk": _decimal(portfolio.open_risk),
            "pilot_realized_pnl": _decimal(portfolio.pilot_realized_pnl),
            "portfolio_snapshot_id": portfolio.portfolio_snapshot_id,
            "schema_version": portfolio.schema_version,
        },
        "portfolio_snapshot_id": value.portfolio_snapshot_id,
        "reconciled_at": value.reconciled_at.isoformat(),
        "schema_version": value.schema_version,
        "verification_status": value.verification_status.value,
    }


def encode_swing_portfolio_artifact(value: SwingPortfolioSnapshotArtifact) -> bytes:
    if type(value) is not SwingPortfolioSnapshotArtifact:
        raise SwingPortfolioArtifactError("portfolio artifact must be exact")
    payload = (
        json.dumps(
            {
                "codec_schema_version": PORTFOLIO_CODEC_VERSION,
                "artifact": _artifact_data(value),
            },
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    if len(payload) > MAXIMUM_PORTFOLIO_ARTIFACT_BYTES:
        raise SwingPortfolioArtifactError("portfolio artifact exceeds its size limit")
    return payload


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise SwingPortfolioArtifactError("portfolio artifact contains duplicate keys")
        result[key] = value
    return result


def _strict_object(value: object, expected: set[str]) -> dict[str, object]:
    if type(value) is not dict or set(value) != expected:
        raise SwingPortfolioArtifactError("stored portfolio artifact has invalid fields")
    return value


def _strict_datetime(value: object) -> datetime:
    if type(value) is not str:
        raise ValueError
    result = datetime.fromisoformat(value)
    if (
        result.tzinfo is None
        or result.isoformat() != value
        or result.astimezone(timezone.utc).isoformat() != value
    ):
        raise ValueError
    return result


def _strict_decimal(value: object) -> Decimal:
    if type(value) is not str:
        raise ValueError
    result = Decimal(value)
    if not result.is_finite() or str(result) != value:
        raise ValueError
    return result


_PORTFOLIO_FIELDS = {
    "as_of",
    "capital",
    "cash_available",
    "currency",
    "daily_realized_pnl",
    "gross_exposure",
    "open_positions",
    "open_risk",
    "pilot_realized_pnl",
    "portfolio_snapshot_id",
    "schema_version",
}
_EVIDENCE_FIELDS = {
    "binding_id",
    "evidence_id",
    "kind",
    "observed_at",
    "schema_version",
    "source_version",
}
_ARTIFACT_FIELDS = {
    "artifact_id",
    "evidence",
    "mode",
    "portfolio",
    "portfolio_snapshot_id",
    "reconciled_at",
    "schema_version",
    "verification_status",
}


def decode_swing_portfolio_artifact(payload: bytes) -> SwingPortfolioSnapshotArtifact:
    if (
        type(payload) is not bytes
        or not payload
        or len(payload) > MAXIMUM_PORTFOLIO_ARTIFACT_BYTES
    ):
        raise SwingPortfolioArtifactError("stored portfolio artifact is invalid")
    try:
        raw = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        envelope = _strict_object(raw, {"codec_schema_version", "artifact"})
        if envelope["codec_schema_version"] != PORTFOLIO_CODEC_VERSION:
            raise ValueError
        value = _strict_object(envelope["artifact"], _ARTIFACT_FIELDS)
        portfolio_raw = _strict_object(value["portfolio"], _PORTFOLIO_FIELDS)
        stored_portfolio_id = _sha(
            portfolio_raw["portfolio_snapshot_id"],
            "portfolio_snapshot_id",
        )
        portfolio = SwingPortfolioSnapshot(
            capital=_strict_decimal(portfolio_raw["capital"]),
            cash_available=_strict_decimal(portfolio_raw["cash_available"]),
            gross_exposure=_strict_decimal(portfolio_raw["gross_exposure"]),
            open_risk=_strict_decimal(portfolio_raw["open_risk"]),
            open_positions=portfolio_raw["open_positions"],
            daily_realized_pnl=_strict_decimal(portfolio_raw["daily_realized_pnl"]),
            pilot_realized_pnl=_strict_decimal(portfolio_raw["pilot_realized_pnl"]),
            as_of=_strict_datetime(portfolio_raw["as_of"]),
            currency=portfolio_raw["currency"],
            schema_version=portfolio_raw["schema_version"],
        )
        if portfolio.portfolio_snapshot_id != stored_portfolio_id:
            raise SwingPortfolioArtifactError("stored portfolio identity differs")
        evidence_raw = value["evidence"]
        if type(evidence_raw) is not list:
            raise ValueError
        evidence: list[SwingPortfolioEvidenceBinding] = []
        for item in evidence_raw:
            item = _strict_object(item, _EVIDENCE_FIELDS)
            stored_binding_id = _sha(item["binding_id"], "binding_id")
            binding = SwingPortfolioEvidenceBinding(
                kind=SwingPortfolioEvidenceKind(item["kind"]),
                evidence_id=item["evidence_id"],
                observed_at=_strict_datetime(item["observed_at"]),
                source_version=item["source_version"],
                schema_version=item["schema_version"],
            )
            if binding.binding_id != stored_binding_id:
                raise SwingPortfolioArtifactError("stored portfolio evidence differs")
            evidence.append(binding)
        stored_artifact_id = _sha(value["artifact_id"], "artifact_id")
        artifact = SwingPortfolioSnapshotArtifact(
            portfolio=portfolio,
            portfolio_snapshot_id=value["portfolio_snapshot_id"],
            evidence=tuple(evidence),
            reconciled_at=_strict_datetime(value["reconciled_at"]),
            verification_status=SwingPortfolioVerificationStatus(
                value["verification_status"]
            ),
            mode=value["mode"],
            schema_version=value["schema_version"],
        )
        if artifact.artifact_id != stored_artifact_id:
            raise SwingPortfolioArtifactError("stored portfolio artifact identity differs")
        if encode_swing_portfolio_artifact(artifact) != payload:
            raise SwingPortfolioArtifactError("stored portfolio encoding is not canonical")
        return artifact
    except SwingPortfolioArtifactError:
        raise
    except (
        InvalidOperation,
        KeyError,
        TypeError,
        UnicodeDecodeError,
        ValueError,
        json.JSONDecodeError,
    ):
        raise SwingPortfolioArtifactError("stored portfolio artifact is invalid") from None


def _is_link_like(path: Path) -> bool:
    try:
        status = os.lstat(path)
    except OSError:
        return path.is_symlink()
    return path.is_symlink() or bool(
        getattr(status, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


class LocalSwingPortfolioArtifactStore:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @property
    def artifacts_root(self) -> Path:
        return self.root / "portfolio_snapshots"

    def path_for(self, artifact_id: str) -> Path:
        try:
            value = _sha(artifact_id, "artifact_id")
        except SwingPortfolioArtifactError:
            raise SwingPortfolioArtifactError("portfolio artifact ID is invalid") from None
        return self.artifacts_root / f"{value}.json"

    def _assert_artifacts_root(self) -> Path:
        try:
            paths = (self.root, self.artifacts_root)
            if any(
                not path.exists() or not path.is_dir() or _is_link_like(path)
                for path in paths
            ):
                raise SwingPortfolioArtifactError("portfolio store root is unsafe")
            resolved_root = self.root.resolve()
            resolved_artifacts = self.artifacts_root.resolve()
        except SwingPortfolioArtifactError:
            raise
        except OSError:
            raise SwingPortfolioArtifactError("portfolio store is unavailable") from None
        if resolved_artifacts != resolved_root / "portfolio_snapshots":
            raise SwingPortfolioArtifactError("portfolio store root is unsafe")
        return self.artifacts_root

    def put(self, value: SwingPortfolioSnapshotArtifact) -> SwingPortfolioSnapshotArtifact:
        if type(value) is not SwingPortfolioSnapshotArtifact:
            raise SwingPortfolioArtifactError("portfolio artifact must be exact")
        payload = encode_swing_portfolio_artifact(value)
        try:
            self.root.mkdir(parents=True, exist_ok=True)
        except OSError:
            raise SwingPortfolioArtifactError("portfolio store is unavailable") from None
        if _is_link_like(self.root):
            raise SwingPortfolioArtifactError("portfolio store root cannot be a link")
        try:
            self.artifacts_root.mkdir(exist_ok=True)
        except OSError:
            raise SwingPortfolioArtifactError("portfolio store is unavailable") from None
        if _is_link_like(self.artifacts_root):
            raise SwingPortfolioArtifactError("portfolio store root cannot be a link")
        self._assert_artifacts_root()
        target = self.path_for(value.artifact_id)
        try:
            with advisory_file_lock(self.artifacts_root / ".portfolio.lock"):
                if target.exists():
                    stored = self.get(value.artifact_id)
                    if stored != value:
                        raise SwingPortfolioArtifactError(
                            "portfolio artifact ID already stores different content"
                        )
                    return stored
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".portfolio-",
                    suffix=".tmp",
                    dir=self.artifacts_root,
                )
                temporary = Path(temporary_name)
                try:
                    with os.fdopen(descriptor, "wb") as handle:
                        handle.write(payload)
                        handle.flush()
                        os.fsync(handle.fileno())
                    os.link(temporary, target)
                finally:
                    temporary.unlink(missing_ok=True)
        except SwingPortfolioArtifactError:
            raise
        except (FileLockUnavailable, FileSafetyError, OSError):
            raise SwingPortfolioArtifactError("portfolio artifact could not be published") from None
        return self.get(value.artifact_id)

    def get(self, artifact_id: str) -> SwingPortfolioSnapshotArtifact:
        path = self.path_for(artifact_id)
        try:
            root_exists = self.artifacts_root.exists()
        except OSError:
            raise SwingPortfolioArtifactError("portfolio store is unavailable") from None
        if not root_exists:
            raise SwingPortfolioArtifactNotFound("portfolio artifact was not found")
        self._assert_artifacts_root()
        try:
            payload = read_stable_regular_file(
                path,
                maximum_bytes=MAXIMUM_PORTFOLIO_ARTIFACT_BYTES,
            )
        except FileSafetyError:
            try:
                exists = path.exists()
            except OSError:
                raise SwingPortfolioArtifactError("portfolio store is unavailable") from None
            if not exists:
                raise SwingPortfolioArtifactNotFound("portfolio artifact was not found") from None
            raise SwingPortfolioArtifactError(
                "portfolio artifact could not be read safely"
            ) from None
        value = decode_swing_portfolio_artifact(payload)
        if value.artifact_id != artifact_id:
            raise SwingPortfolioArtifactError("portfolio artifact path differs from content")
        return value


class StoredSwingPortfolioSource:
    """Exact-ID portfolio source with a decision-window freshness contract."""

    def __init__(
        self,
        *,
        store: LocalSwingPortfolioArtifactStore,
        artifact_id: str,
        expected_portfolio_snapshot_id: str,
        decision_not_before: datetime,
        decision_deadline: datetime,
        maximum_age_seconds: int,
    ) -> None:
        if type(store) is not LocalSwingPortfolioArtifactStore:
            raise SwingPortfolioArtifactError("portfolio store must be exact")
        _sha(artifact_id, "artifact_id")
        _sha(expected_portfolio_snapshot_id, "expected_portfolio_snapshot_id")
        self.store = store
        self.artifact_id = artifact_id
        self.expected_portfolio_snapshot_id = expected_portfolio_snapshot_id
        self.decision_not_before = _utc(decision_not_before, "decision_not_before")
        self.decision_deadline = _utc(decision_deadline, "decision_deadline")
        if self.decision_not_before >= self.decision_deadline:
            raise SwingPortfolioArtifactError("portfolio decision window is invalid")
        if type(maximum_age_seconds) is not int or not 1 <= maximum_age_seconds <= 86_400:
            raise SwingPortfolioArtifactError("portfolio maximum age is invalid")
        self.maximum_age_seconds = maximum_age_seconds
        self._source_id = content_id(
            {
                "kind": "STORED_RECONCILED_PORTFOLIO",
                "artifact_id": artifact_id,
                "expected_portfolio_snapshot_id": expected_portfolio_snapshot_id,
                "decision_not_before": self.decision_not_before,
                "decision_deadline": self.decision_deadline,
                "maximum_age_seconds": maximum_age_seconds,
                "mode": "PAPER_ONLY",
            },
            length=64,
        )

    @property
    def source_id(self) -> str:
        return self._source_id

    def read_portfolio(self) -> SwingPortfolioSnapshot:
        artifact = self.store.get(self.artifact_id)
        portfolio = artifact.portfolio
        if portfolio.portfolio_snapshot_id != self.expected_portfolio_snapshot_id:
            raise SwingPortfolioArtifactError("portfolio snapshot differs from the job binding")
        earliest_allowed = self.decision_not_before - timedelta(
            seconds=self.maximum_age_seconds
        )
        if not earliest_allowed <= portfolio.as_of <= self.decision_deadline:
            raise SwingPortfolioArtifactError("portfolio artifact is stale or future-dated")
        artifact.verify_content_identity()
        return portfolio
