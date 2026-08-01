"""Offline sealing of the initial empty promoted paper portfolio.

This module deliberately has no clock, environment, network, broker, cloud,
notification, discovery, or order capability.  The caller supplies four exact
raw evidence payloads and one strict request.  Evidence is archived first and
the existing portfolio artifact is the final commit marker.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Mapping, Protocol

from india_swing._filesystem import FileSafetyError, read_stable_regular_file
from india_swing.operations.portfolio_store import (
    LocalSwingPortfolioArtifactStore,
    SwingPortfolioEvidenceBinding,
    SwingPortfolioEvidenceKind,
    SwingPortfolioSnapshotArtifact,
)
from india_swing.risk.swing_portfolio import SwingPortfolioSnapshot


GENESIS_REQUEST_SCHEMA_VERSION = "promoted-paper-portfolio-genesis-request/v1"
MANUAL_RECONCILIATION_ACK = (
    "I_HAVE_MANUALLY_RECONCILED_THE_FOUR_EVIDENCE_FILES_FOR_PAPER_ONLY_USE"
)
MAXIMUM_GENESIS_REQUEST_BYTES = 32 * 1024
MAXIMUM_GENESIS_EVIDENCE_BYTES = 8 * 1024 * 1024

_SHA = re.compile(r"[0-9a-f]{64}\Z")
_SOURCE_VERSION = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/+:-]{0,127}\Z")
_CAPITAL_TEXT = re.compile(r"(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?\Z")
_ERR = "promoted paper portfolio genesis is invalid"
_REQUEST_KEYS = {
    "as_of",
    "capital",
    "evidence",
    "manual_reconciliation_ack",
    "schema_version",
}
_EVIDENCE_KEYS = {"expected_sha256", "kind", "observed_at", "source_version"}


class PromotedPaperPortfolioGenesisError(ValueError):
    """The paper-only genesis request or evidence failed closed."""


def _fail() -> None:
    raise PromotedPaperPortfolioGenesisError(_ERR)


def _sha(value: object) -> str:
    if type(value) is not str or _SHA.fullmatch(value) is None:
        _fail()
    return value


def canonical_utc_z_timestamp(value: datetime) -> str:
    if type(value) is not datetime or value.tzinfo is None:
        _fail()
    failed = False
    converted: datetime | None = None
    try:
        if value.utcoffset() != timedelta(0):
            failed = True
        else:
            converted = value.astimezone(timezone.utc)
    except Exception:
        failed = True
    if failed or converted is None:
        _fail()
    text = converted.isoformat()
    return text[:-6] + "Z"


def _parse_timestamp(value: object) -> datetime:
    if type(value) is not str or not value.endswith("Z"):
        _fail()
    failed = False
    parsed: datetime | None = None
    try:
        parsed = datetime.fromisoformat(value)
        if (
            parsed.tzinfo is None
            or parsed.utcoffset() != timedelta(0)
            or canonical_utc_z_timestamp(parsed) != value
        ):
            failed = True
    except Exception:
        failed = True
    if failed or parsed is None:
        _fail()
    return parsed.astimezone(timezone.utc)


def _capital(value: object) -> Decimal:
    if (
        type(value) is not str
        or len(value) > 128
        or _CAPITAL_TEXT.fullmatch(value) is None
    ):
        _fail()
    failed = False
    parsed: Decimal | None = None
    try:
        parsed = Decimal(value)
        if not parsed.is_finite() or parsed <= 0 or _canonical_decimal(parsed) != value:
            failed = True
    except (InvalidOperation, ValueError):
        failed = True
    if failed or parsed is None:
        _fail()
    return parsed


def _canonical_decimal(value: Decimal) -> str:
    text = str(value)
    if len(text) > 128 or _CAPITAL_TEXT.fullmatch(text) is None:
        _fail()
    return text


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            _fail()
        value[key] = item
    return value


def _reject_number(_: str) -> object:
    _fail()


@dataclass(frozen=True, slots=True)
class GenesisEvidenceDescriptor:
    kind: SwingPortfolioEvidenceKind
    expected_sha256: str
    observed_at: datetime
    source_version: str

    def __post_init__(self) -> None:
        if type(self.kind) is not SwingPortfolioEvidenceKind:
            _fail()
        _sha(self.expected_sha256)
        object.__setattr__(self, "observed_at", _parse_timestamp(canonical_utc_z_timestamp(self.observed_at)))
        if type(self.source_version) is not str or _SOURCE_VERSION.fullmatch(self.source_version) is None:
            _fail()

    def replay(self) -> GenesisEvidenceDescriptor:
        failed = False
        replayed: GenesisEvidenceDescriptor | None = None
        try:
            replayed = GenesisEvidenceDescriptor(
                kind=self.kind,
                expected_sha256=self.expected_sha256,
                observed_at=self.observed_at,
                source_version=self.source_version,
            )
            if replayed != self:
                failed = True
        except Exception:
            failed = True
        if failed or replayed is None:
            _fail()
        return replayed


@dataclass(frozen=True, slots=True)
class PromotedPaperPortfolioGenesisRequest:
    as_of: datetime
    capital: Decimal
    manual_reconciliation_ack: str
    evidence: tuple[GenesisEvidenceDescriptor, ...]
    schema_version: str = GENESIS_REQUEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "as_of", _parse_timestamp(canonical_utc_z_timestamp(self.as_of)))
        if type(self.capital) is not Decimal or not self.capital.is_finite() or self.capital <= 0:
            _fail()
        object.__setattr__(self, "capital", Decimal(_canonical_decimal(self.capital)))
        if self.manual_reconciliation_ack != MANUAL_RECONCILIATION_ACK:
            _fail()
        if (
            type(self.evidence) is not tuple
            or any(type(item) is not GenesisEvidenceDescriptor for item in self.evidence)
            or tuple(item.kind for item in self.evidence) != tuple(SwingPortfolioEvidenceKind)
        ):
            _fail()
        if len({item.expected_sha256 for item in self.evidence}) != len(self.evidence):
            _fail()
        for item in self.evidence:
            if item.observed_at > self.as_of:
                _fail()
            item.replay()
        if self.schema_version != GENESIS_REQUEST_SCHEMA_VERSION:
            _fail()

    def replay(self) -> PromotedPaperPortfolioGenesisRequest:
        failed = False
        replayed: PromotedPaperPortfolioGenesisRequest | None = None
        try:
            evidence = tuple(item.replay() for item in self.evidence)
            replayed = PromotedPaperPortfolioGenesisRequest(
                as_of=self.as_of,
                capital=self.capital,
                manual_reconciliation_ack=self.manual_reconciliation_ack,
                evidence=evidence,
                schema_version=self.schema_version,
            )
            if replayed != self:
                failed = True
        except Exception:
            failed = True
        if failed or replayed is None:
            _fail()
        return replayed


def encode_promoted_paper_portfolio_genesis_request(
    value: PromotedPaperPortfolioGenesisRequest,
) -> bytes:
    failed = False
    replayed: PromotedPaperPortfolioGenesisRequest | None = None
    try:
        if type(value) is not PromotedPaperPortfolioGenesisRequest:
            failed = True
        else:
            replayed = value.replay()
    except Exception:
        failed = True
    if failed or replayed is None:
        _fail()
    raw = {
        "as_of": canonical_utc_z_timestamp(replayed.as_of),
        "capital": _canonical_decimal(replayed.capital),
        "evidence": [
            {
                "expected_sha256": item.expected_sha256,
                "kind": item.kind.value,
                "observed_at": canonical_utc_z_timestamp(item.observed_at),
                "source_version": item.source_version,
            }
            for item in replayed.evidence
        ],
        "manual_reconciliation_ack": replayed.manual_reconciliation_ack,
        "schema_version": replayed.schema_version,
    }
    payload = json.dumps(raw, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    if len(payload) > MAXIMUM_GENESIS_REQUEST_BYTES:
        _fail()
    return payload


def decode_promoted_paper_portfolio_genesis_request(
    payload: bytes,
) -> PromotedPaperPortfolioGenesisRequest:
    if type(payload) is not bytes or not payload or len(payload) > MAXIMUM_GENESIS_REQUEST_BYTES:
        _fail()
    failed = False
    root: object = None
    try:
        root = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
    except Exception:
        failed = True
    if failed or type(root) is not dict or set(root) != _REQUEST_KEYS:
        _fail()
    evidence_raw = root["evidence"]
    if type(evidence_raw) is not list or len(evidence_raw) != len(SwingPortfolioEvidenceKind):
        _fail()
    constructed: PromotedPaperPortfolioGenesisRequest | None = None
    try:
        evidence = tuple(
            GenesisEvidenceDescriptor(
                kind=SwingPortfolioEvidenceKind(item["kind"]),
                expected_sha256=_sha(item["expected_sha256"]),
                observed_at=_parse_timestamp(item["observed_at"]),
                source_version=item["source_version"],
            )
            for item in evidence_raw
            if type(item) is dict and set(item) == _EVIDENCE_KEYS
        )
        if len(evidence) != len(evidence_raw):
            failed = True
        else:
            constructed = PromotedPaperPortfolioGenesisRequest(
                as_of=_parse_timestamp(root["as_of"]),
                capital=_capital(root["capital"]),
                manual_reconciliation_ack=root["manual_reconciliation_ack"],
                evidence=evidence,
                schema_version=root["schema_version"],
            )
    except Exception:
        failed = True
    if failed or constructed is None:
        _fail()
    if encode_promoted_paper_portfolio_genesis_request(constructed) != payload:
        _fail()
    return constructed


class PromotedPortfolioEvidenceArchive(Protocol):
    def create_or_verify(
        self, kind: SwingPortfolioEvidenceKind, expected_sha256: str, payload: bytes
    ) -> Path: ...


def _is_link_like(path: Path) -> bool:
    try:
        value = os.lstat(path)
    except OSError:
        return path.is_symlink()
    return path.is_symlink() or bool(
        getattr(value, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


class LocalPromotedPortfolioEvidenceArchive:
    def __init__(self, portfolio_artifact_root: Path) -> None:
        failed = False
        root: Path | None = None
        try:
            root = Path(portfolio_artifact_root)
            if not root.is_absolute() or ".." in root.parts:
                failed = True
        except Exception:
            failed = True
        if failed or root is None:
            _fail()
        self.portfolio_artifact_root = root
        self.root = self.portfolio_artifact_root / "reconciliation_evidence"

    def path_for(self, kind: SwingPortfolioEvidenceKind, expected_sha256: str) -> Path:
        if type(kind) is not SwingPortfolioEvidenceKind:
            _fail()
        digest = _sha(expected_sha256)
        return self.root / kind.value / f"{digest}.bin"

    def _prepare_parent(self, kind: SwingPortfolioEvidenceKind) -> Path:
        failed = False
        parent: Path | None = None
        try:
            for ancestor in (self.portfolio_artifact_root, *self.portfolio_artifact_root.parents):
                if ancestor.exists() and (
                    not ancestor.is_dir() or _is_link_like(ancestor)
                ):
                    failed = True
                    break
            if failed:
                raise OSError("unsafe portfolio evidence ancestor")
            self.portfolio_artifact_root.mkdir(parents=True, exist_ok=True)
            self.root.mkdir(exist_ok=True)
            parent = self.root / kind.value
            parent.mkdir(exist_ok=True)
            chain = (self.portfolio_artifact_root, self.root, parent)
            if any(not path.is_dir() or _is_link_like(path) for path in chain):
                failed = True
            else:
                base = self.portfolio_artifact_root.resolve()
                if self.root.resolve() != base / "reconciliation_evidence" or parent.resolve() != self.root.resolve() / kind.value:
                    failed = True
        except Exception:
            failed = True
        if failed or parent is None:
            _fail()
        return parent

    def create_or_verify(
        self, kind: SwingPortfolioEvidenceKind, expected_sha256: str, payload: bytes
    ) -> Path:
        if type(payload) is not bytes or not payload or len(payload) > MAXIMUM_GENESIS_EVIDENCE_BYTES:
            _fail()
        digest = _sha(expected_sha256)
        if hashlib.sha256(payload).hexdigest() != digest:
            _fail()
        parent = self._prepare_parent(kind)
        target = self.path_for(kind, digest)
        if target.exists():
            failed = False
            existing: bytes | None = None
            try:
                existing = read_stable_regular_file(target, maximum_bytes=MAXIMUM_GENESIS_EVIDENCE_BYTES)
            except Exception:
                failed = True
            if failed or existing != payload or hashlib.sha256(existing).hexdigest() != digest:
                _fail()
            return target
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0) | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOINHERIT", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor: int | None = None
        failed = False
        try:
            descriptor = os.open(target, flags, 0o600)
            with os.fdopen(descriptor, "wb", closefd=True) as handle:
                descriptor = None
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
        except Exception:
            failed = True
        finally:
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except OSError:
                    pass
        if failed:
            _fail()
        verify_failed = False
        stored: bytes | None = None
        try:
            stored = read_stable_regular_file(target, maximum_bytes=MAXIMUM_GENESIS_EVIDENCE_BYTES)
        except Exception:
            verify_failed = True
        if verify_failed or stored != payload or hashlib.sha256(stored).hexdigest() != digest:
            _fail()
        return target


def seal_promoted_paper_portfolio_genesis(
    *,
    request: PromotedPaperPortfolioGenesisRequest,
    evidence_payloads: Mapping[SwingPortfolioEvidenceKind, bytes],
    evidence_archive: PromotedPortfolioEvidenceArchive,
    portfolio_store: LocalSwingPortfolioArtifactStore,
) -> SwingPortfolioSnapshotArtifact:
    failed = False
    replayed: PromotedPaperPortfolioGenesisRequest | None = None
    payloads: dict[SwingPortfolioEvidenceKind, bytes] = {}
    try:
        if type(request) is not PromotedPaperPortfolioGenesisRequest:
            failed = True
        else:
            replayed = request.replay()
        if type(evidence_payloads) is not dict or set(evidence_payloads) != set(SwingPortfolioEvidenceKind):
            failed = True
        else:
            payloads = dict(evidence_payloads)
        if type(portfolio_store) is not LocalSwingPortfolioArtifactStore:
            failed = True
        if not callable(getattr(evidence_archive, "create_or_verify", None)):
            failed = True
    except Exception:
        failed = True
    if failed or replayed is None:
        _fail()

    for item in replayed.evidence:
        payload = payloads.get(item.kind)
        if type(payload) is not bytes or not payload or len(payload) > MAXIMUM_GENESIS_EVIDENCE_BYTES or hashlib.sha256(payload).hexdigest() != item.expected_sha256:
            _fail()

    snapshot_failed = False
    artifact: SwingPortfolioSnapshotArtifact | None = None
    try:
        snapshot = SwingPortfolioSnapshot(
            capital=replayed.capital,
            cash_available=replayed.capital,
            gross_exposure=Decimal("0"),
            open_risk=Decimal("0"),
            open_positions=0,
            daily_realized_pnl=Decimal("0"),
            pilot_realized_pnl=Decimal("0"),
            as_of=replayed.as_of,
        )
        bindings = tuple(
            SwingPortfolioEvidenceBinding(
                kind=item.kind,
                evidence_id=item.expected_sha256,
                observed_at=item.observed_at,
                source_version=item.source_version,
            )
            for item in replayed.evidence
        )
        artifact = SwingPortfolioSnapshotArtifact(
            portfolio=snapshot,
            portfolio_snapshot_id=snapshot.portfolio_snapshot_id,
            evidence=bindings,
            reconciled_at=replayed.as_of,
        )
        artifact.verify_content_identity()
    except Exception:
        snapshot_failed = True
    if snapshot_failed or artifact is None:
        _fail()

    for item in replayed.evidence:
        archive_failed = False
        try:
            evidence_archive.create_or_verify(item.kind, item.expected_sha256, payloads[item.kind])
        except Exception:
            archive_failed = True
        if archive_failed:
            _fail()

    publish_failed = False
    stored: SwingPortfolioSnapshotArtifact | None = None
    try:
        portfolio_store.put(artifact)
        stored = portfolio_store.get(artifact.artifact_id)
        stored.verify_content_identity()
        p = stored.portfolio
        if (
            stored != artifact
            or p.capital != replayed.capital
            or p.cash_available != replayed.capital
            or p.gross_exposure != 0
            or p.open_risk != 0
            or p.open_positions != 0
            or p.daily_realized_pnl != 0
            or p.pilot_realized_pnl != 0
            or tuple(item.evidence_id for item in stored.evidence)
            != tuple(item.expected_sha256 for item in replayed.evidence)
        ):
            publish_failed = True
    except Exception:
        publish_failed = True
    if publish_failed or stored is None:
        _fail()
    return stored
