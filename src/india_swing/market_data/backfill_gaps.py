from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path

from india_swing.identity import content_id

from .models import (
    LISTING_KEY_PATTERN,
    MARKET_DATA_PROVIDER_PATTERN,
    NSE_EQUITY_ISIN_PATTERN,
    NSE_SECURITY_SERIES_PATTERN,
    SHA256_IDENTIFIER,
)


HISTORICAL_BACKFILL_SESSION_GAP_SCHEMA_VERSION = "historical-backfill-session-gap/v1"
HISTORICAL_BACKFILL_SESSION_GAP_POLICY_VERSION = (
    "historical-backfill-session-gap-policy/v1"
)
HISTORICAL_BACKFILL_SESSION_GAP_DATASET = "historical-backfill-session-gaps"
GAP_FILENAME_SUFFIX = ".json"


class HistoricalBackfillGapError(ValueError):
    pass


class HistoricalBackfillGapIntegrityError(HistoricalBackfillGapError):
    pass


class HistoricalBackfillGapClassification(str, Enum):
    UNRESOLVED_EMPTY_PROVIDER_RESPONSE = "UNRESOLVED_EMPTY_PROVIDER_RESPONSE"


def _sha256(value: object, field_name: str) -> None:
    if type(value) is not str or SHA256_IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a lowercase SHA-256")


def _provider(value: object, field_name: str) -> None:
    if type(value) is not str or MARKET_DATA_PROVIDER_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be canonical uppercase provider text")


def _utc(value: object, field_name: str) -> datetime:
    if type(value) is not datetime:
        raise TypeError(f"{field_name} must be an exact datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class HistoricalBackfillSessionGapEvidence:
    """Durable, collection-only evidence that one provider session was empty.

    This is never proof of zero trading. NSE EOD or other official evidence
    is required by a future task to adjudicate a gap; this evidence cannot be
    deleted, resolved, accepted, or mutated by this module.
    """

    plan_id: str
    request_id: str
    provider: str
    provider_version: str
    provider_instrument_id: str
    listing_key: str
    security_series: str
    isin: str
    session: date
    response_observed_at: datetime
    normalized_response_sha256: str
    classification: HistoricalBackfillGapClassification = (
        HistoricalBackfillGapClassification.UNRESOLVED_EMPTY_PROVIDER_RESPONSE
    )
    collection_only: bool = True
    actionable: bool = False
    schema_version: str = HISTORICAL_BACKFILL_SESSION_GAP_SCHEMA_VERSION
    policy_version: str = HISTORICAL_BACKFILL_SESSION_GAP_POLICY_VERSION
    evidence_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(
            self,
            "response_observed_at",
            _utc(self.response_observed_at, "gap response_observed_at"),
        )
        object.__setattr__(self, "evidence_id", self._calculated_id())

    def _validate(self) -> None:
        _sha256(self.plan_id, "gap plan_id")
        _sha256(self.request_id, "gap request_id")
        _provider(self.provider, "gap provider")
        if (
            type(self.provider_version) is not str
            or not self.provider_version
            or len(self.provider_version) > 128
        ):
            raise ValueError("gap provider_version must be bounded text")
        if (
            type(self.provider_instrument_id) is not str
            or not self.provider_instrument_id
            or self.provider_instrument_id != self.provider_instrument_id.strip()
            or len(self.provider_instrument_id) > 128
        ):
            raise ValueError("gap provider_instrument_id must be bounded canonical text")
        if (
            type(self.listing_key) is not str
            or LISTING_KEY_PATTERN.fullmatch(self.listing_key) is None
        ):
            raise ValueError("gap listing_key must be canonical NSE:TRADINGSYMBOL text")
        if (
            type(self.security_series) is not str
            or NSE_SECURITY_SERIES_PATTERN.fullmatch(self.security_series) is None
        ):
            raise ValueError("gap security_series must be canonical NSE series text")
        if type(self.isin) is not str or NSE_EQUITY_ISIN_PATTERN.fullmatch(self.isin) is None:
            raise ValueError("gap isin must be a canonical Indian equity ISIN")
        if type(self.session) is not date:
            raise TypeError("gap session must be an exact date")
        _utc(self.response_observed_at, "gap response_observed_at")
        if (
            type(self.normalized_response_sha256) is not str
            or SHA256_IDENTIFIER.fullmatch(self.normalized_response_sha256) is None
        ):
            raise ValueError("gap normalized_response_sha256 must be a lowercase SHA-256")
        if (
            type(self.classification) is not HistoricalBackfillGapClassification
            or self.classification
            is not HistoricalBackfillGapClassification.UNRESOLVED_EMPTY_PROVIDER_RESPONSE
        ):
            raise ValueError("unsupported historical backfill gap classification")
        if self.collection_only is not True:
            raise ValueError("historical backfill session gaps must remain collection-only")
        if self.actionable is not False:
            raise ValueError("historical backfill session gaps cannot authorize trading")
        if (
            self.schema_version != HISTORICAL_BACKFILL_SESSION_GAP_SCHEMA_VERSION
            or self.policy_version != HISTORICAL_BACKFILL_SESSION_GAP_POLICY_VERSION
        ):
            raise ValueError("unsupported historical backfill session gap contract")

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "evidence_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.evidence_id != self._calculated_id():
            raise HistoricalBackfillGapIntegrityError(
                "historical backfill session gap evidence identity failed"
            )


_EXPECTED_GAP_KEYS = {
    "schema_version",
    "policy_version",
    "evidence_id",
    "plan_id",
    "request_id",
    "provider",
    "provider_version",
    "provider_instrument_id",
    "listing_key",
    "security_series",
    "isin",
    "session",
    "response_observed_at",
    "normalized_response_sha256",
    "classification",
    "collection_only",
    "actionable",
}


def _reject_duplicate_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise HistoricalBackfillGapError(
                "historical backfill session gap state contains duplicate keys"
            )
        value[key] = item
    return value


def _gap_value(evidence: HistoricalBackfillSessionGapEvidence) -> dict[str, object]:
    return {
        "schema_version": evidence.schema_version,
        "policy_version": evidence.policy_version,
        "evidence_id": evidence.evidence_id,
        "plan_id": evidence.plan_id,
        "request_id": evidence.request_id,
        "provider": evidence.provider,
        "provider_version": evidence.provider_version,
        "provider_instrument_id": evidence.provider_instrument_id,
        "listing_key": evidence.listing_key,
        "security_series": evidence.security_series,
        "isin": evidence.isin,
        "session": evidence.session.isoformat(),
        "response_observed_at": evidence.response_observed_at.isoformat(),
        "normalized_response_sha256": evidence.normalized_response_sha256,
        "classification": evidence.classification.value,
        "collection_only": evidence.collection_only,
        "actionable": evidence.actionable,
    }


def _gap_from_bytes(payload: bytes) -> HistoricalBackfillSessionGapEvidence:
    try:
        root = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_pairs,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        if type(root) is not dict or set(root) != _EXPECTED_GAP_KEYS:
            raise ValueError
        claimed_evidence_id = root["evidence_id"]
        if type(claimed_evidence_id) is not str:
            raise ValueError
        evidence = HistoricalBackfillSessionGapEvidence(
            plan_id=root["plan_id"],
            request_id=root["request_id"],
            provider=root["provider"],
            provider_version=root["provider_version"],
            provider_instrument_id=root["provider_instrument_id"],
            listing_key=root["listing_key"],
            security_series=root["security_series"],
            isin=root["isin"],
            session=date.fromisoformat(root["session"]),
            response_observed_at=datetime.fromisoformat(
                root["response_observed_at"]
            ),
            normalized_response_sha256=root["normalized_response_sha256"],
            classification=HistoricalBackfillGapClassification(
                root["classification"]
            ),
            collection_only=root["collection_only"],
            actionable=root["actionable"],
            schema_version=root["schema_version"],
            policy_version=root["policy_version"],
        )
        if claimed_evidence_id != evidence.evidence_id or root != _gap_value(evidence):
            raise ValueError
        return evidence
    except HistoricalBackfillGapError:
        raise
    except (
        AttributeError,
        KeyError,
        TypeError,
        ValueError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ):
        raise HistoricalBackfillGapIntegrityError(
            "historical backfill session gap evidence is malformed"
        ) from None


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


class LocalHistoricalBackfillSessionGapStore:
    """Atomic, plan-scoped local store for durable session-gap evidence.

    Deliberately exposes only exact-plan loading -- no latest/listing
    operation exists, matching the collection-only, non-actionable contract
    of the evidence itself.
    """

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def _request_dir(self, plan_id: str, request_id: str) -> Path:
        _sha256(plan_id, "plan_id")
        _sha256(request_id, "request_id")
        return (
            self.root
            / HISTORICAL_BACKFILL_SESSION_GAP_DATASET
            / plan_id
            / request_id
        )

    def _path(self, plan_id: str, request_id: str, session: date) -> Path:
        if type(session) is not date:
            raise TypeError("session must be an exact date")
        return self._request_dir(plan_id, request_id) / f"{session.isoformat()}{GAP_FILENAME_SUFFIX}"

    def put(
        self,
        evidence: HistoricalBackfillSessionGapEvidence,
    ) -> HistoricalBackfillSessionGapEvidence:
        if type(evidence) is not HistoricalBackfillSessionGapEvidence:
            raise TypeError(
                "evidence must be an exact HistoricalBackfillSessionGapEvidence"
            )
        evidence.verify_content_identity()
        path = self._path(evidence.plan_id, evidence.request_id, evidence.session)
        if path.exists():
            existing = self._read(path)
            if existing != evidence:
                raise HistoricalBackfillGapError(
                    "conflicting historical backfill session gap evidence "
                    "already persisted for this request/session"
                )
            return existing

        path.parent.mkdir(parents=True, exist_ok=True)
        payload = (
            json.dumps(
                _gap_value(evidence),
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8")
        descriptor, temporary_name = tempfile.mkstemp(
            dir=path.parent,
            prefix=".gap.",
            suffix=".tmp",
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temporary, path)
            except FileExistsError:
                existing = self._read(path)
                if existing != evidence:
                    raise HistoricalBackfillGapError(
                        "conflicting historical backfill session gap evidence "
                        "already persisted for this request/session"
                    )
                return existing
            temporary.unlink()
            _fsync_directory(path.parent)
        finally:
            if temporary.exists():
                temporary.unlink()
        loaded = self._read(path)
        if loaded != evidence:
            raise HistoricalBackfillGapIntegrityError(
                "historical backfill session gap evidence failed write "
                "verification"
            )
        return loaded

    def load_unresolved(
        self,
        plan_id: str,
    ) -> tuple[HistoricalBackfillSessionGapEvidence, ...]:
        """Return every durable session-gap for one exact plan; never a listing."""

        _sha256(plan_id, "plan_id")
        base = self.root / HISTORICAL_BACKFILL_SESSION_GAP_DATASET / plan_id
        if not base.exists():
            return ()
        if base.is_symlink() or not base.is_dir():
            raise HistoricalBackfillGapIntegrityError(
                "historical backfill session gap plan path is not a real directory"
            )
        results: list[HistoricalBackfillSessionGapEvidence] = []
        for request_dir in sorted(base.iterdir()):
            if (
                request_dir.is_symlink()
                or not request_dir.is_dir()
                or SHA256_IDENTIFIER.fullmatch(request_dir.name) is None
            ):
                raise HistoricalBackfillGapIntegrityError(
                    "historical backfill session gap request path is invalid"
                )
            for session_file in sorted(request_dir.iterdir()):
                if (
                    session_file.is_symlink()
                    or not session_file.is_file()
                    or session_file.suffix != GAP_FILENAME_SUFFIX
                ):
                    raise HistoricalBackfillGapIntegrityError(
                        "historical backfill session gap file is invalid"
                    )
                try:
                    date.fromisoformat(session_file.stem)
                except ValueError:
                    raise HistoricalBackfillGapIntegrityError(
                        "historical backfill session gap filename is invalid"
                    ) from None
                evidence = self._read(session_file)
                if (
                    evidence.plan_id != plan_id
                    or evidence.request_id != request_dir.name
                    or evidence.session.isoformat() != session_file.stem
                ):
                    raise HistoricalBackfillGapIntegrityError(
                        "historical backfill session gap path and evidence disagree"
                    )
                results.append(evidence)
        return tuple(
            sorted(results, key=lambda value: (value.request_id, value.session))
        )

    @staticmethod
    def _read(path: Path) -> HistoricalBackfillSessionGapEvidence:
        if path.is_symlink() or not path.is_file():
            raise HistoricalBackfillGapIntegrityError(
                "historical backfill session gap path is not a real file"
            )
        return _gap_from_bytes(path.read_bytes())
