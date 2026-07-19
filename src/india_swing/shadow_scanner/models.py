from __future__ import annotations

import re
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum

from india_swing.identity import content_id


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_REASON = re.compile(r"[A-Z][A-Z0-9_]*\Z")
ZERO = Decimal("0")


class ShadowScanError(ValueError):
    pass


class ShadowScanStatus(str, Enum):
    RANKED = "RANKED"
    NO_CANDIDATE = "NO_CANDIDATE"


def _sha(value: str, name: str) -> None:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise ShadowScanError(f"{name} must be a lowercase SHA-256")


def _decimal(value: Decimal, name: str, *, positive: bool = False) -> None:
    if type(value) is not Decimal or not value.is_finite():
        raise ShadowScanError(f"{name} must be a finite Decimal")
    if positive and value <= ZERO:
        raise ShadowScanError(f"{name} must be positive")


def _utc(value: datetime, name: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is None:
        raise ShadowScanError(f"{name} must be timezone-aware")
    try:
        offset = value.utcoffset()
    except Exception:
        raise ShadowScanError(f"{name} has invalid timezone behavior") from None
    if offset is None:
        raise ShadowScanError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class CollectionShadowScannerConfig:
    minimum_history_sessions: int = 120
    momentum_lookback_sessions: int = 20
    minimum_median_traded_value: Decimal = Decimal("10000000")
    minimum_delivery_percent: Decimal = Decimal("20")
    allowed_series: tuple[str, ...] = ("EQ",)
    top_n: int = 20
    policy_version: str = "collection-shadow-momentum/v1"
    config_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.minimum_history_sessions, "minimum_history_sessions"),
            (self.momentum_lookback_sessions, "momentum_lookback_sessions"),
            (self.top_n, "top_n"),
        ):
            if type(value) is not int or value <= 0:
                raise ShadowScanError(f"{name} must be a positive integer")
        if self.momentum_lookback_sessions > self.minimum_history_sessions:
            raise ShadowScanError("momentum lookback cannot exceed minimum history")
        _decimal(
            self.minimum_median_traded_value,
            "minimum_median_traded_value",
        )
        if self.minimum_median_traded_value < ZERO:
            raise ShadowScanError("minimum traded value cannot be negative")
        _decimal(self.minimum_delivery_percent, "minimum_delivery_percent")
        if not ZERO <= self.minimum_delivery_percent <= Decimal("100"):
            raise ShadowScanError("minimum delivery percent must be between 0 and 100")
        if (
            type(self.allowed_series) is not tuple
            or not self.allowed_series
            or self.allowed_series != tuple(sorted(set(self.allowed_series)))
            or any(
                type(value) is not str
                or not value
                or value != value.strip().upper()
                for value in self.allowed_series
            )
        ):
            raise ShadowScanError("allowed_series must be sorted unique uppercase text")
        if self.policy_version != "collection-shadow-momentum/v1":
            raise ShadowScanError("unsupported shadow scanner policy")
        object.__setattr__(self, "config_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "config_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        try:
            fresh = CollectionShadowScannerConfig(
                minimum_history_sessions=self.minimum_history_sessions,
                momentum_lookback_sessions=self.momentum_lookback_sessions,
                minimum_median_traded_value=self.minimum_median_traded_value,
                minimum_delivery_percent=self.minimum_delivery_percent,
                allowed_series=self.allowed_series,
                top_n=self.top_n,
                policy_version=self.policy_version,
            )
        except Exception:
            raise ShadowScanError("scanner configuration identity failed") from None
        if self.config_id != fresh.config_id:
            raise ShadowScanError("scanner configuration identity failed")


@dataclass(frozen=True, slots=True)
class CollectionShadowCandidate:
    market_session: date
    symbol: str
    series: str
    validated_isin: str
    financial_instrument_id: int
    current_close: Decimal
    tick_size_rupees: Decimal
    lookback_sessions: tuple[date, ...]
    bar_ids: tuple[str, ...]
    lookback_return_pct: Decimal
    positive_session_fraction: Decimal
    median_daily_traded_value: Decimal
    median_daily_volume: Decimal
    median_delivery_percent: Decimal
    evidence_ids: tuple[str, ...]
    warnings: tuple[str, ...]
    schema_version: str = "collection-shadow-candidate/v1"
    candidate_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.market_session) is not date:
            raise ShadowScanError("candidate market_session must be a date")
        for value, name in (
            (self.symbol, "symbol"),
            (self.series, "series"),
            (self.validated_isin, "validated_isin"),
        ):
            if type(value) is not str or not value or value != value.strip().upper():
                raise ShadowScanError(f"candidate {name} is invalid")
        if type(self.financial_instrument_id) is not int or self.financial_instrument_id <= 0:
            raise ShadowScanError("candidate financial instrument ID must be positive")
        for value, name in (
            (self.current_close, "current_close"),
            (self.tick_size_rupees, "tick_size_rupees"),
            (self.median_daily_traded_value, "median_daily_traded_value"),
            (self.median_daily_volume, "median_daily_volume"),
            (self.median_delivery_percent, "median_delivery_percent"),
        ):
            _decimal(value, name, positive=True)
        _decimal(self.lookback_return_pct, "lookback_return_pct")
        _decimal(self.positive_session_fraction, "positive_session_fraction")
        if not ZERO <= self.positive_session_fraction <= Decimal("1"):
            raise ShadowScanError("positive session fraction must be between 0 and 1")
        if not ZERO <= self.median_delivery_percent <= Decimal("100"):
            raise ShadowScanError("median delivery percent must be between 0 and 100")
        if (
            type(self.lookback_sessions) is not tuple
            or not self.lookback_sessions
            or self.lookback_sessions != tuple(sorted(set(self.lookback_sessions)))
            or self.lookback_sessions[-1] != self.market_session
        ):
            raise ShadowScanError("candidate lookback sessions are invalid")
        if (
            type(self.bar_ids) is not tuple
            or len(self.bar_ids) != len(self.lookback_sessions)
            or len(set(self.bar_ids)) != len(self.bar_ids)
        ):
            raise ShadowScanError("candidate bar lineage is invalid")
        for value in self.bar_ids:
            _sha(value, "candidate bar_id")
        if (
            type(self.evidence_ids) is not tuple
            or not self.evidence_ids
            or len(set(self.evidence_ids)) != len(self.evidence_ids)
        ):
            raise ShadowScanError("candidate evidence IDs must be non-empty and unique")
        for value in self.evidence_ids:
            _sha(value, "candidate evidence_id")
        if (
            type(self.warnings) is not tuple
            or not self.warnings
            or self.warnings != tuple(sorted(set(self.warnings)))
            or any(_REASON.fullmatch(value) is None for value in self.warnings)
        ):
            raise ShadowScanError("candidate warnings must be sorted reason codes")
        if self.schema_version != "collection-shadow-candidate/v1":
            raise ShadowScanError("unsupported candidate schema")
        object.__setattr__(self, "candidate_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "candidate_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        try:
            fresh = CollectionShadowCandidate(
                market_session=self.market_session,
                symbol=self.symbol,
                series=self.series,
                validated_isin=self.validated_isin,
                financial_instrument_id=self.financial_instrument_id,
                current_close=self.current_close,
                tick_size_rupees=self.tick_size_rupees,
                lookback_sessions=self.lookback_sessions,
                bar_ids=self.bar_ids,
                lookback_return_pct=self.lookback_return_pct,
                positive_session_fraction=self.positive_session_fraction,
                median_daily_traded_value=self.median_daily_traded_value,
                median_daily_volume=self.median_daily_volume,
                median_delivery_percent=self.median_delivery_percent,
                evidence_ids=self.evidence_ids,
                warnings=self.warnings,
                schema_version=self.schema_version,
            )
        except Exception:
            raise ShadowScanError("shadow candidate identity failed") from None
        if self.candidate_id != fresh.candidate_id:
            raise ShadowScanError("shadow candidate identity failed")


@dataclass(frozen=True, slots=True)
class CollectionShadowScanResult:
    market_session: date
    cutoff: datetime
    derived_evidence_id: str
    universe_snapshot_id: str
    liquidity_snapshot_id: str
    tick_size_snapshot_id: str
    historical_price_artifact_ids: tuple[str, ...]
    config_id: str
    candidates: tuple[CollectionShadowCandidate, ...]
    exclusion_counts: tuple[tuple[str, int], ...]
    blockers: tuple[str, ...]
    status: ShadowScanStatus
    mode: str = "RESEARCH_ONLY"
    actionable: bool = False
    schema_version: str = "collection-shadow-scan/v1"
    result_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.market_session) is not date:
            raise ShadowScanError("scan market_session must be a date")
        object.__setattr__(self, "cutoff", _utc(self.cutoff, "scan cutoff"))
        for value, name in (
            (self.derived_evidence_id, "derived_evidence_id"),
            (self.universe_snapshot_id, "universe_snapshot_id"),
            (self.liquidity_snapshot_id, "liquidity_snapshot_id"),
            (self.tick_size_snapshot_id, "tick_size_snapshot_id"),
            (self.config_id, "config_id"),
        ):
            _sha(value, name)
        if (
            type(self.historical_price_artifact_ids) is not tuple
            or not self.historical_price_artifact_ids
            or len(set(self.historical_price_artifact_ids))
            != len(self.historical_price_artifact_ids)
        ):
            raise ShadowScanError("historical artifact IDs must be unique and non-empty")
        for value in self.historical_price_artifact_ids:
            _sha(value, "historical_price_artifact_id")
        if (
            type(self.candidates) is not tuple
            or any(type(value) is not CollectionShadowCandidate for value in self.candidates)
        ):
            raise ShadowScanError("scan candidates must be an exact tuple")
        for value in self.candidates:
            value.verify_content_identity()
            if value.market_session != self.market_session:
                raise ShadowScanError("candidate belongs to another market session")
        expected_candidates = tuple(
            sorted(
                self.candidates,
                key=lambda value: (
                    -value.lookback_return_pct,
                    -value.median_daily_traded_value,
                    value.symbol,
                    value.series,
                ),
            )
        )
        if self.candidates != expected_candidates:
            raise ShadowScanError("scan candidates are not deterministically ranked")
        if (
            type(self.exclusion_counts) is not tuple
            or self.exclusion_counts
            != tuple(sorted(self.exclusion_counts, key=lambda value: value[0]))
            or any(
                type(value) is not tuple
                or len(value) != 2
                or _REASON.fullmatch(value[0]) is None
                or type(value[1]) is not int
                or value[1] <= 0
                for value in self.exclusion_counts
            )
        ):
            raise ShadowScanError("scan exclusion counts are invalid")
        if len({value[0] for value in self.exclusion_counts}) != len(
            self.exclusion_counts
        ):
            raise ShadowScanError("scan exclusion reasons must be unique")
        if (
            type(self.blockers) is not tuple
            or not self.blockers
            or self.blockers != tuple(sorted(set(self.blockers)))
            or any(_REASON.fullmatch(value) is None for value in self.blockers)
        ):
            raise ShadowScanError("scan blockers must be sorted reason codes")
        if type(self.status) is not ShadowScanStatus:
            raise ShadowScanError("scan status must be exact")
        if (self.status is ShadowScanStatus.RANKED) != bool(self.candidates):
            raise ShadowScanError("scan status differs from ranked candidates")
        if self.mode != "RESEARCH_ONLY" or self.actionable:
            raise ShadowScanError("collection scans must remain non-actionable")
        if self.schema_version != "collection-shadow-scan/v1":
            raise ShadowScanError("unsupported scan schema")
        object.__setattr__(self, "result_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "result_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        try:
            fresh = CollectionShadowScanResult(
                market_session=self.market_session,
                cutoff=self.cutoff,
                derived_evidence_id=self.derived_evidence_id,
                universe_snapshot_id=self.universe_snapshot_id,
                liquidity_snapshot_id=self.liquidity_snapshot_id,
                tick_size_snapshot_id=self.tick_size_snapshot_id,
                historical_price_artifact_ids=self.historical_price_artifact_ids,
                config_id=self.config_id,
                candidates=self.candidates,
                exclusion_counts=self.exclusion_counts,
                blockers=self.blockers,
                status=self.status,
                mode=self.mode,
                actionable=self.actionable,
                schema_version=self.schema_version,
            )
        except Exception:
            raise ShadowScanError("shadow scan result identity failed") from None
        if self.result_id != fresh.result_id:
            raise ShadowScanError("shadow scan result identity failed")
