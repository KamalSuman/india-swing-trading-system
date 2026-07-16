from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal

from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness


TICK_SIZE_OBSERVATION_SCHEMA_VERSION = "nse-cm-tick-size-observation/v1"
TICK_SIZE_SNAPSHOT_SCHEMA_VERSION = "nse-cm-tick-size-snapshot/v1"
TICK_SIZE_POLICY_VERSION = "nse-cm-bid-interval-paise-collection/v1"
TICK_SIZE_CODEC_VERSION = "nse-cm-tick-size-json/v1"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_SYMBOL = re.compile(r"[A-Z0-9&-]{1,10}\Z")
_SERIES = re.compile(r"[A-Z0-9]{1,2}\Z")
_ISIN = re.compile(r"[A-Z]{2}[A-Z0-9]{9}[0-9]\Z")
_REASON = re.compile(r"[A-Z][A-Z0-9_]{2,127}\Z")


class TickSizeError(RuntimeError):
    pass


class TickSizeIntegrityError(TickSizeError):
    pass


class TickSizeConflict(TickSizeError):
    pass


class TickSizeNotFound(TickSizeError):
    pass


def _sha(value: str, name: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise TickSizeIntegrityError(f"{name} must be a full lowercase SHA-256")


def _aware_utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise TickSizeIntegrityError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class CollectedTickSizeObservation:
    market_session_claim: date
    knowledge_time: datetime
    source_artifact_id: str
    source_manifest_id: str
    source_record_id: str
    financial_instrument_id: int
    symbol: str
    series: str
    validated_isin: str | None
    bid_interval_paise: int
    schema_version: str = TICK_SIZE_OBSERVATION_SCHEMA_VERSION
    observation_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.market_session_claim) is not date:
            raise TypeError("tick-size market_session_claim must be a date")
        object.__setattr__(
            self,
            "knowledge_time",
            _aware_utc(self.knowledge_time, "tick-size knowledge_time"),
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
            raise TickSizeIntegrityError("financial instrument ID must be positive")
        if not isinstance(self.symbol, str) or _SYMBOL.fullmatch(self.symbol) is None:
            raise TickSizeIntegrityError("tick-size symbol is invalid")
        if not isinstance(self.series, str) or _SERIES.fullmatch(self.series) is None:
            raise TickSizeIntegrityError("tick-size series is invalid")
        if self.validated_isin is not None and (
            not isinstance(self.validated_isin, str)
            or _ISIN.fullmatch(self.validated_isin) is None
        ):
            raise TickSizeIntegrityError("tick-size ISIN is invalid")
        if type(self.bid_interval_paise) is not int or self.bid_interval_paise <= 0:
            raise TickSizeIntegrityError("bid interval must be positive integer paise")
        if self.schema_version != TICK_SIZE_OBSERVATION_SCHEMA_VERSION:
            raise TickSizeIntegrityError("unsupported tick-size observation schema")
        object.__setattr__(self, "observation_id", self._calculated_id())

    @property
    def tick_size_rupees(self) -> Decimal:
        return Decimal(self.bid_interval_paise) / Decimal(100)

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
                "bid_interval_paise": self.bid_interval_paise,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.observation_id != self._calculated_id():
            raise TickSizeIntegrityError("tick-size observation identity failed")


@dataclass(frozen=True, slots=True)
class CollectionTickSizeSnapshot:
    market_session_claim: date
    cutoff: datetime
    knowledge_time: datetime
    source_artifact_id: str
    source_manifest_id: str
    source_raw_sha256: str
    source_normalized_sha256: str
    observations: tuple[CollectedTickSizeObservation, ...]
    reason_codes: tuple[str, ...]
    readiness: ReferenceReadiness = ReferenceReadiness.COLLECTION_ONLY
    actionable: bool = False
    policy_version: str = TICK_SIZE_POLICY_VERSION
    schema_version: str = TICK_SIZE_SNAPSHOT_SCHEMA_VERSION
    snapshot_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.market_session_claim) is not date:
            raise TypeError("tick-size snapshot session claim must be a date")
        object.__setattr__(self, "cutoff", _aware_utc(self.cutoff, "snapshot cutoff"))
        object.__setattr__(
            self,
            "knowledge_time",
            _aware_utc(self.knowledge_time, "snapshot knowledge_time"),
        )
        if self.knowledge_time > self.cutoff:
            raise TickSizeIntegrityError("tick-size snapshot was not known by cutoff")
        for value, name in (
            (self.source_artifact_id, "source_artifact_id"),
            (self.source_manifest_id, "source_manifest_id"),
            (self.source_raw_sha256, "source_raw_sha256"),
            (self.source_normalized_sha256, "source_normalized_sha256"),
        ):
            _sha(value, name)
        if (
            type(self.observations) is not tuple
            or not self.observations
            or any(type(value) is not CollectedTickSizeObservation for value in self.observations)
            or self.observations
            != tuple(sorted(self.observations, key=lambda value: value.source_record_id))
        ):
            raise TickSizeIntegrityError(
                "tick-size observations must be non-empty, exact, and source ordered"
            )
        if len({value.source_record_id for value in self.observations}) != len(
            self.observations
        ):
            raise TickSizeIntegrityError("tick-size source records must be unique")
        if len(
            {(value.symbol, value.series) for value in self.observations}
        ) != len(self.observations):
            raise TickSizeIntegrityError("tick-size listing keys must be unique")
        for value in self.observations:
            value.verify_content_identity()
            if (
                value.market_session_claim != self.market_session_claim
                or value.knowledge_time != self.knowledge_time
                or value.source_artifact_id != self.source_artifact_id
                or value.source_manifest_id != self.source_manifest_id
            ):
                raise TickSizeIntegrityError(
                    "tick-size observation lineage differs from its snapshot"
                )
        if (
            type(self.reason_codes) is not tuple
            or self.reason_codes != tuple(sorted(set(self.reason_codes)))
            or not self.reason_codes
            or any(_REASON.fullmatch(value) is None for value in self.reason_codes)
        ):
            raise TickSizeIntegrityError(
                "collection tick-size snapshot requires sorted reason codes"
            )
        if self.readiness is not ReferenceReadiness.COLLECTION_ONLY or self.actionable:
            raise TickSizeIntegrityError(
                "manual tick-size snapshots must remain collection-only"
            )
        if (
            self.policy_version != TICK_SIZE_POLICY_VERSION
            or self.schema_version != TICK_SIZE_SNAPSHOT_SCHEMA_VERSION
        ):
            raise TickSizeIntegrityError("unsupported tick-size snapshot contract")
        object.__setattr__(self, "snapshot_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema_version": self.schema_version,
                "policy_version": self.policy_version,
                "market_session_claim": self.market_session_claim,
                "cutoff": self.cutoff,
                "knowledge_time": self.knowledge_time,
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
            if type(value) is not CollectedTickSizeObservation:
                raise TickSizeIntegrityError("tick-size snapshot graph is invalid")
            value.verify_content_identity()
        if self.snapshot_id != self._calculated_id():
            raise TickSizeIntegrityError("tick-size snapshot identity failed")
