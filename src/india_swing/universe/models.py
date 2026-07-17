from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum

from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness


COLLECTION_UNIVERSE_OBSERVATION_SCHEMA_VERSION = (
    "nse-cm-collection-universe-observation/v1"
)
COLLECTION_UNIVERSE_SNAPSHOT_SCHEMA_VERSION = (
    "nse-cm-collection-universe-snapshot/v1"
)
COLLECTION_UNIVERSE_POLICY_VERSION = "nse-cm-broad-equity-no-market-cap-cutoff/v1"
COLLECTION_UNIVERSE_CODEC_VERSION = "nse-cm-collection-universe-json/v1"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_SYMBOL = re.compile(r"[A-Z0-9&-]{1,10}\Z")
_SERIES = re.compile(r"[A-Z0-9]{1,2}\Z")
_ISIN = re.compile(r"[A-Z]{2}[A-Z0-9]{9}[0-9]\Z")
_REASON = re.compile(r"[A-Z][A-Z0-9_]{2,127}\Z")


class CollectionUniverseDisposition(str, Enum):
    IN_SCOPE_UNVERIFIED_EQUITY = "IN_SCOPE_UNVERIFIED_EQUITY"
    EXCLUDED_NON_EQUITY = "EXCLUDED_NON_EQUITY"
    EXCLUDED_TEST_SECURITY = "EXCLUDED_TEST_SECURITY"
    EXCLUDED_ALTERNATIVE_VENUE = "EXCLUDED_ALTERNATIVE_VENUE"


class CollectionUniverseError(RuntimeError):
    pass


class CollectionUniverseIntegrityError(CollectionUniverseError):
    pass


class CollectionUniverseConflict(CollectionUniverseError):
    pass


class CollectionUniverseNotFound(CollectionUniverseError):
    pass


def _sha(value: str, name: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise CollectionUniverseIntegrityError(
            f"{name} must be a full lowercase SHA-256"
        )


def _utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise CollectionUniverseIntegrityError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class CollectedUniverseObservation:
    market_session_claim: date
    knowledge_time: datetime
    source_artifact_id: str
    source_manifest_id: str
    source_record_id: str
    financial_instrument_id: int
    symbol: str
    series: str
    validated_isin: str | None
    disposition: CollectionUniverseDisposition
    included_in_broad_equity_scope: bool
    permitted_to_trade: int
    normal_market_status: int
    normal_market_eligible: bool
    delete_flag: str
    listing_timestamp: int
    removal_timestamp: int
    readmission_timestamp: int
    schema_version: str = COLLECTION_UNIVERSE_OBSERVATION_SCHEMA_VERSION
    observation_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.market_session_claim) is not date:
            raise TypeError("universe market_session_claim must be a date")
        object.__setattr__(
            self,
            "knowledge_time",
            _utc(self.knowledge_time, "universe knowledge_time"),
        )
        for value, name in (
            (self.source_artifact_id, "source_artifact_id"),
            (self.source_manifest_id, "source_manifest_id"),
            (self.source_record_id, "source_record_id"),
        ):
            _sha(value, name)
        if (
            type(self.financial_instrument_id) is not int
            or self.financial_instrument_id <= 0
        ):
            raise CollectionUniverseIntegrityError(
                "financial instrument ID must be positive"
            )
        if not isinstance(self.symbol, str) or _SYMBOL.fullmatch(self.symbol) is None:
            raise CollectionUniverseIntegrityError("universe symbol is invalid")
        if not isinstance(self.series, str) or _SERIES.fullmatch(self.series) is None:
            raise CollectionUniverseIntegrityError("universe series is invalid")
        if self.validated_isin is not None and (
            not isinstance(self.validated_isin, str)
            or _ISIN.fullmatch(self.validated_isin) is None
        ):
            raise CollectionUniverseIntegrityError("universe ISIN is invalid")
        if not isinstance(self.disposition, CollectionUniverseDisposition):
            raise TypeError("universe disposition must be exact")
        if type(self.included_in_broad_equity_scope) is not bool:
            raise TypeError("broad equity scope flag must be bool")
        expected_in_scope = (
            self.disposition
            is CollectionUniverseDisposition.IN_SCOPE_UNVERIFIED_EQUITY
        )
        if self.included_in_broad_equity_scope is not expected_in_scope:
            raise CollectionUniverseIntegrityError(
                "broad equity scope flag differs from source disposition"
            )
        if self.permitted_to_trade not in (0, 1, 2):
            raise CollectionUniverseIntegrityError("permitted-to-trade is invalid")
        if type(self.normal_market_status) is not int or not 1 <= self.normal_market_status <= 6:
            raise CollectionUniverseIntegrityError("normal market status is invalid")
        if type(self.normal_market_eligible) is not bool:
            raise TypeError("normal market eligibility must be bool")
        if self.delete_flag not in ("N", "Y"):
            raise CollectionUniverseIntegrityError("delete flag is invalid")
        for value, name in (
            (self.listing_timestamp, "listing_timestamp"),
            (self.removal_timestamp, "removal_timestamp"),
            (self.readmission_timestamp, "readmission_timestamp"),
        ):
            if type(value) is not int or value < 0:
                raise CollectionUniverseIntegrityError(
                    f"{name} must be a non-negative integer"
                )
        if self.schema_version != COLLECTION_UNIVERSE_OBSERVATION_SCHEMA_VERSION:
            raise CollectionUniverseIntegrityError(
                "unsupported universe observation schema"
            )
        object.__setattr__(self, "observation_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema_version": self.schema_version,
                "market_session_claim": self.market_session_claim,
                "knowledge_time": self.knowledge_time,
                "source_artifact_id": self.source_artifact_id,
                "source_manifest_id": self.source_manifest_id,
                "source_record_id": self.source_record_id,
                "financial_instrument_id": self.financial_instrument_id,
                "symbol": self.symbol,
                "series": self.series,
                "validated_isin": self.validated_isin,
                "disposition": self.disposition,
                "included_in_broad_equity_scope": (
                    self.included_in_broad_equity_scope
                ),
                "permitted_to_trade": self.permitted_to_trade,
                "normal_market_status": self.normal_market_status,
                "normal_market_eligible": self.normal_market_eligible,
                "delete_flag": self.delete_flag,
                "listing_timestamp": self.listing_timestamp,
                "removal_timestamp": self.removal_timestamp,
                "readmission_timestamp": self.readmission_timestamp,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.observation_id != self._calculated_id():
            raise CollectionUniverseIntegrityError(
                "universe observation identity failed"
            )


@dataclass(frozen=True, slots=True)
class CollectionUniverseSnapshot:
    market_session_claim: date
    cutoff: datetime
    knowledge_time: datetime
    calendar_snapshot_id: str
    source_artifact_id: str
    source_manifest_id: str
    source_raw_sha256: str
    source_normalized_sha256: str
    observations: tuple[CollectedUniverseObservation, ...]
    reason_codes: tuple[str, ...]
    readiness: ReferenceReadiness = ReferenceReadiness.COLLECTION_ONLY
    actionable: bool = False
    policy_version: str = COLLECTION_UNIVERSE_POLICY_VERSION
    schema_version: str = COLLECTION_UNIVERSE_SNAPSHOT_SCHEMA_VERSION
    snapshot_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.market_session_claim) is not date:
            raise TypeError("universe snapshot session claim must be a date")
        object.__setattr__(self, "cutoff", _utc(self.cutoff, "universe cutoff"))
        object.__setattr__(
            self,
            "knowledge_time",
            _utc(self.knowledge_time, "universe knowledge_time"),
        )
        if self.knowledge_time > self.cutoff:
            raise CollectionUniverseIntegrityError(
                "universe source was unavailable at cutoff"
            )
        for value, name in (
            (self.calendar_snapshot_id, "calendar_snapshot_id"),
            (self.source_artifact_id, "source_artifact_id"),
            (self.source_manifest_id, "source_manifest_id"),
            (self.source_raw_sha256, "source_raw_sha256"),
            (self.source_normalized_sha256, "source_normalized_sha256"),
        ):
            _sha(value, name)
        if (
            type(self.observations) is not tuple
            or not self.observations
            or any(
                type(value) is not CollectedUniverseObservation
                for value in self.observations
            )
            or self.observations
            != tuple(sorted(self.observations, key=lambda value: value.source_record_id))
        ):
            raise CollectionUniverseIntegrityError(
                "universe observations must be non-empty, exact, and source ordered"
            )
        if len({value.source_record_id for value in self.observations}) != len(
            self.observations
        ):
            raise CollectionUniverseIntegrityError(
                "universe source records must be unique"
            )
        if len({(value.symbol, value.series) for value in self.observations}) != len(
            self.observations
        ):
            raise CollectionUniverseIntegrityError(
                "universe symbol-series keys must be unique"
            )
        for value in self.observations:
            value.verify_content_identity()
            if (
                value.market_session_claim != self.market_session_claim
                or value.knowledge_time != self.knowledge_time
                or value.source_artifact_id != self.source_artifact_id
                or value.source_manifest_id != self.source_manifest_id
            ):
                raise CollectionUniverseIntegrityError(
                    "universe observation lineage differs from its snapshot"
                )
        if (
            type(self.reason_codes) is not tuple
            or not self.reason_codes
            or self.reason_codes != tuple(sorted(set(self.reason_codes)))
            or any(_REASON.fullmatch(value) is None for value in self.reason_codes)
        ):
            raise CollectionUniverseIntegrityError(
                "collection universe requires sorted reason codes"
            )
        required_reasons = {
            "BOARD_CLASSIFICATION_UNVERIFIED",
            "CALENDAR_PROVENANCE_UNVERIFIED",
            "POINT_IN_TIME_LISTING_STATE_UNVERIFIED",
            "STABLE_IDENTITY_UNAVAILABLE",
            "SURVEILLANCE_STATE_UNAVAILABLE",
            "UNVERIFIED_MANUAL_ACQUISITION",
            "UNVERIFIED_REPORT_DATE",
        }
        if not required_reasons.issubset(self.reason_codes):
            raise CollectionUniverseIntegrityError(
                "collection universe is missing mandatory blockers"
            )
        if self.readiness is not ReferenceReadiness.COLLECTION_ONLY or self.actionable:
            raise CollectionUniverseIntegrityError(
                "manual universe snapshots must remain collection-only"
            )
        if (
            self.policy_version != COLLECTION_UNIVERSE_POLICY_VERSION
            or self.schema_version != COLLECTION_UNIVERSE_SNAPSHOT_SCHEMA_VERSION
        ):
            raise CollectionUniverseIntegrityError(
                "unsupported collection universe contract"
            )
        object.__setattr__(self, "snapshot_id", self._calculated_id())

    @property
    def in_scope_observations(self) -> tuple[CollectedUniverseObservation, ...]:
        return tuple(
            value
            for value in self.observations
            if value.included_in_broad_equity_scope
        )

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema_version": self.schema_version,
                "policy_version": self.policy_version,
                "market_session_claim": self.market_session_claim,
                "cutoff": self.cutoff,
                "knowledge_time": self.knowledge_time,
                "calendar_snapshot_id": self.calendar_snapshot_id,
                "source_artifact_id": self.source_artifact_id,
                "source_manifest_id": self.source_manifest_id,
                "source_raw_sha256": self.source_raw_sha256,
                "source_normalized_sha256": self.source_normalized_sha256,
                "observations": self.observations,
                "reason_codes": self.reason_codes,
                "readiness": self.readiness,
                "actionable": self.actionable,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        for value in self.observations:
            if type(value) is not CollectedUniverseObservation:
                raise CollectionUniverseIntegrityError(
                    "universe snapshot graph is invalid"
                )
            value.verify_content_identity()
        if self.snapshot_id != self._calculated_id():
            raise CollectionUniverseIntegrityError(
                "collection universe snapshot identity failed"
            )
