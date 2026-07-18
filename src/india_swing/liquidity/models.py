from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal

from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness


LIQUIDITY_POLICY_VERSION = "trailing-raw-eod-liquidity-collection/v1"
LIQUIDITY_SCHEMA_VERSION = "liquidity-snapshot/v1"
LIQUIDITY_OBSERVATION_SCHEMA_VERSION = "liquidity-observation/v1"
LIQUIDITY_SOURCE_SCHEMA_VERSION = "liquidity-source-session/v1"
LIQUIDITY_CODEC_VERSION = "liquidity-json/v1"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_ISIN = re.compile(r"[A-Z]{2}[A-Z0-9]{9}[0-9]\Z")
_REASON = re.compile(r"[A-Z][A-Z0-9_]{2,127}\Z")
ZERO = Decimal("0")


class LiquidityError(RuntimeError):
    pass


class LiquidityIntegrityError(LiquidityError):
    pass


class LiquidityConflict(LiquidityError):
    pass


class LiquidityNotFound(LiquidityError):
    pass


def _sha(value: str, name: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise LiquidityIntegrityError(f"{name} must be a full lowercase SHA-256")


def _utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise LiquidityIntegrityError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class LiquiditySourceSession:
    market_session: date
    artifact_id: str
    cutoff: datetime
    knowledge_time: datetime
    schema_version: str = LIQUIDITY_SOURCE_SCHEMA_VERSION
    binding_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.market_session) is not date:
            raise TypeError("liquidity source session must be a date")
        _sha(self.artifact_id, "liquidity source artifact_id")
        object.__setattr__(self, "cutoff", _utc(self.cutoff, "source cutoff"))
        object.__setattr__(
            self,
            "knowledge_time",
            _utc(self.knowledge_time, "source knowledge_time"),
        )
        if self.knowledge_time > self.cutoff:
            raise LiquidityIntegrityError("liquidity source was not known by its cutoff")
        if self.schema_version != LIQUIDITY_SOURCE_SCHEMA_VERSION:
            raise LiquidityIntegrityError("unsupported liquidity source schema")
        object.__setattr__(self, "binding_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema_version": self.schema_version,
                "market_session": self.market_session,
                "artifact_id": self.artifact_id,
                "cutoff": self.cutoff,
                "knowledge_time": self.knowledge_time,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.binding_id != self._calculated_id():
            raise LiquidityIntegrityError("liquidity source identity failed")


@dataclass(frozen=True, slots=True)
class CollectedLiquidityObservation:
    candidate_id: str
    validated_isin: str
    series: str
    symbols: tuple[str, ...]
    observed_sessions: tuple[date, ...]
    bar_ids: tuple[str, ...]
    supplied_session_count: int
    minimum_history_sessions: int
    median_daily_traded_value: Decimal
    median_daily_volume: Decimal
    median_delivery_percent: Decimal | None
    schema_version: str = LIQUIDITY_OBSERVATION_SCHEMA_VERSION
    observation_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha(self.candidate_id, "liquidity candidate_id")
        if (
            not isinstance(self.validated_isin, str)
            or _ISIN.fullmatch(self.validated_isin) is None
        ):
            raise LiquidityIntegrityError("liquidity observation ISIN is invalid")
        if (
            not isinstance(self.series, str)
            or not self.series
            or self.series != self.series.upper()
        ):
            raise LiquidityIntegrityError("liquidity observation series is invalid")
        if (
            type(self.symbols) is not tuple
            or not self.symbols
            or self.symbols != tuple(sorted(set(self.symbols)))
        ):
            raise LiquidityIntegrityError("liquidity symbols must be sorted and unique")
        if (
            type(self.observed_sessions) is not tuple
            or not self.observed_sessions
            or self.observed_sessions != tuple(sorted(set(self.observed_sessions)))
        ):
            raise LiquidityIntegrityError(
                "liquidity observed sessions must be sorted and unique"
            )
        if (
            type(self.bar_ids) is not tuple
            or len(self.bar_ids) != len(self.observed_sessions)
            or len(set(self.bar_ids)) != len(self.bar_ids)
        ):
            raise LiquidityIntegrityError(
                "liquidity bar IDs must exactly cover observed sessions"
            )
        for value in self.bar_ids:
            _sha(value, "liquidity bar_id")
        if (
            type(self.supplied_session_count) is not int
            or self.supplied_session_count < len(self.observed_sessions)
        ):
            raise LiquidityIntegrityError("supplied session count is inconsistent")
        if (
            type(self.minimum_history_sessions) is not int
            or self.minimum_history_sessions <= 0
        ):
            raise LiquidityIntegrityError("minimum history sessions must be positive")
        for value, name in (
            (self.median_daily_traded_value, "median_daily_traded_value"),
            (self.median_daily_volume, "median_daily_volume"),
        ):
            if type(value) is not Decimal or not value.is_finite() or value <= ZERO:
                raise LiquidityIntegrityError(f"{name} must be a positive Decimal")
        if self.median_delivery_percent is not None and (
            type(self.median_delivery_percent) is not Decimal
            or not self.median_delivery_percent.is_finite()
            or not ZERO <= self.median_delivery_percent <= Decimal(100)
        ):
            raise LiquidityIntegrityError("median delivery percent is invalid")
        if self.schema_version != LIQUIDITY_OBSERVATION_SCHEMA_VERSION:
            raise LiquidityIntegrityError("unsupported liquidity observation schema")
        object.__setattr__(self, "observation_id", self._calculated_id())

    @property
    def observed_session_count(self) -> int:
        return len(self.observed_sessions)

    @property
    def meets_minimum_history(self) -> bool:
        return self.observed_session_count >= self.minimum_history_sessions

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema_version": self.schema_version,
                "candidate_id": self.candidate_id,
                "validated_isin": self.validated_isin,
                "series": self.series,
                "symbols": self.symbols,
                "observed_sessions": self.observed_sessions,
                "bar_ids": self.bar_ids,
                "supplied_session_count": self.supplied_session_count,
                "minimum_history_sessions": self.minimum_history_sessions,
                "median_daily_traded_value": self.median_daily_traded_value,
                "median_daily_volume": self.median_daily_volume,
                "median_delivery_percent": self.median_delivery_percent,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.observation_id != self._calculated_id():
            raise LiquidityIntegrityError("liquidity observation identity failed")


@dataclass(frozen=True, slots=True)
class CollectionLiquiditySnapshot:
    decision_cutoff: datetime
    minimum_history_sessions: int
    source_sessions: tuple[LiquiditySourceSession, ...]
    observations: tuple[CollectedLiquidityObservation, ...]
    reason_codes: tuple[str, ...]
    readiness: ReferenceReadiness = ReferenceReadiness.COLLECTION_ONLY
    actionable: bool = False
    policy_version: str = LIQUIDITY_POLICY_VERSION
    schema_version: str = LIQUIDITY_SCHEMA_VERSION
    snapshot_id: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "decision_cutoff",
            _utc(self.decision_cutoff, "liquidity decision_cutoff"),
        )
        if (
            type(self.minimum_history_sessions) is not int
            or self.minimum_history_sessions <= 0
        ):
            raise LiquidityIntegrityError("minimum history sessions must be positive")
        if (
            type(self.source_sessions) is not tuple
            or not self.source_sessions
            or any(
                type(value) is not LiquiditySourceSession
                for value in self.source_sessions
            )
            or self.source_sessions
            != tuple(sorted(self.source_sessions, key=lambda value: value.market_session))
        ):
            raise LiquidityIntegrityError(
                "liquidity sources must be non-empty, exact, and session ordered"
            )
        if len({value.market_session for value in self.source_sessions}) != len(
            self.source_sessions
        ):
            raise LiquidityIntegrityError("liquidity source sessions must be unique")
        for value in self.source_sessions:
            value.verify_content_identity()
            if value.knowledge_time > self.decision_cutoff:
                raise LiquidityIntegrityError(
                    "liquidity source was unavailable at the decision cutoff"
                )
        if (
            type(self.observations) is not tuple
            or not self.observations
            or any(
                type(value) is not CollectedLiquidityObservation
                for value in self.observations
            )
            or self.observations
            != tuple(sorted(self.observations, key=lambda value: value.candidate_id))
        ):
            raise LiquidityIntegrityError(
                "liquidity observations must be non-empty, exact, and candidate ordered"
            )
        if len({value.candidate_id for value in self.observations}) != len(
            self.observations
        ):
            raise LiquidityIntegrityError("liquidity candidate IDs must be unique")
        supplied_sessions = {value.market_session for value in self.source_sessions}
        for value in self.observations:
            value.verify_content_identity()
            if (
                value.minimum_history_sessions != self.minimum_history_sessions
                or value.supplied_session_count != len(self.source_sessions)
                or not set(value.observed_sessions).issubset(supplied_sessions)
            ):
                raise LiquidityIntegrityError(
                    "liquidity observation coverage differs from its snapshot"
                )
        if (
            type(self.reason_codes) is not tuple
            or not self.reason_codes
            or self.reason_codes != tuple(sorted(set(self.reason_codes)))
            or any(_REASON.fullmatch(value) is None for value in self.reason_codes)
        ):
            raise LiquidityIntegrityError(
                "collection liquidity requires sorted reason codes"
            )
        has_short_history = any(
            not value.meets_minimum_history for value in self.observations
        )
        if has_short_history != ("INSUFFICIENT_HISTORY" in self.reason_codes):
            raise LiquidityIntegrityError(
                "liquidity history reason differs from candidate coverage"
            )
        if self.readiness is not ReferenceReadiness.COLLECTION_ONLY or self.actionable:
            raise LiquidityIntegrityError(
                "raw liquidity snapshots must remain collection-only"
            )
        if (
            self.policy_version != LIQUIDITY_POLICY_VERSION
            or self.schema_version != LIQUIDITY_SCHEMA_VERSION
        ):
            raise LiquidityIntegrityError("unsupported liquidity snapshot contract")
        object.__setattr__(self, "snapshot_id", self._calculated_id())

    @property
    def coverage_start(self) -> date:
        return self.source_sessions[0].market_session

    @property
    def coverage_end(self) -> date:
        return self.source_sessions[-1].market_session

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema_version": self.schema_version,
                "policy_version": self.policy_version,
                "decision_cutoff": self.decision_cutoff,
                "minimum_history_sessions": self.minimum_history_sessions,
                "source_sessions": self.source_sessions,
                "observations": self.observations,
                "reason_codes": self.reason_codes,
                "readiness": self.readiness,
                "actionable": self.actionable,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        for value in self.source_sessions:
            value.verify_content_identity()
        for value in self.observations:
            value.verify_content_identity()
        if self.snapshot_id != self._calculated_id():
            raise LiquidityIntegrityError("liquidity snapshot identity failed")
