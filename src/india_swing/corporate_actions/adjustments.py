from __future__ import annotations

from dataclasses import dataclass, field, fields
from datetime import date, datetime, timezone
from decimal import Decimal
import re

from india_swing.historical_prices.models import RawNseEodBar
from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness

from .models import (
    CorporateActionSnapshot,
    CorporateActionType,
)


ZERO = Decimal("0")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
ADJUSTED_PRICE_BASIS = "CORPORATE_ACTION_ADJUSTED_AS_OF_CUTOFF"
ADJUSTMENT_POLICY_VERSION = "split-bonus-price-volume-adjustment/v1"


class PriceAdjustmentError(ValueError):
    pass


def _decimal(value: Decimal, name: str, *, positive: bool = False) -> None:
    if type(value) is not Decimal or not value.is_finite():
        raise PriceAdjustmentError(f"{name} must be a finite Decimal")
    if positive and value <= ZERO:
        raise PriceAdjustmentError(f"{name} must be positive")


def _utc(value: datetime, name: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is None:
        raise PriceAdjustmentError(f"{name} must be timezone-aware")
    try:
        offset = value.utcoffset()
    except Exception:
        raise PriceAdjustmentError(f"{name} has invalid timezone behavior") from None
    if offset is None:
        raise PriceAdjustmentError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _sha(value: str, name: str) -> None:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise PriceAdjustmentError(f"{name} must be a lowercase SHA-256")


@dataclass(frozen=True, slots=True)
class StableRawBarBinding:
    """Point-in-time stable-identity evidence for one exact raw bar."""

    market_session: date
    raw_bar_id: str
    stable_instrument_id: str
    stable_listing_id: str
    identity_snapshot_id: str
    knowledge_time: datetime
    binding_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.market_session) is not date:
            raise PriceAdjustmentError("identity binding market_session must be a date")
        for value, name in (
            (self.raw_bar_id, "identity binding raw_bar_id"),
            (self.stable_instrument_id, "identity binding stable_instrument_id"),
            (self.stable_listing_id, "identity binding stable_listing_id"),
            (self.identity_snapshot_id, "identity binding identity_snapshot_id"),
        ):
            _sha(value, name)
        object.__setattr__(
            self,
            "knowledge_time",
            _utc(self.knowledge_time, "identity binding knowledge_time"),
        )
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
            raise PriceAdjustmentError("identity binding content identity failed")


@dataclass(frozen=True, slots=True)
class AdjustedPriceBar:
    market_session: date
    symbol: str
    series: str
    validated_isin: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    traded_value: Decimal
    price_factor: Decimal
    volume_factor: Decimal
    applied_event_ids: tuple[str, ...]
    raw_bar_id: str
    identity_binding_id: str
    identity_snapshot_id: str
    raw_knowledge_time: datetime
    knowledge_time: datetime
    adjustment_snapshot_id: str
    adjustment_policy_version: str = ADJUSTMENT_POLICY_VERSION
    adjusted_bar_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.market_session) is not date:
            raise PriceAdjustmentError("adjusted bar market_session must be a date")
        for value, name in (
            (self.symbol, "symbol"),
            (self.series, "series"),
            (self.validated_isin, "validated_isin"),
        ):
            if type(value) is not str or not value or value != value.strip().upper():
                raise PriceAdjustmentError(f"adjusted bar {name} is invalid")
        for name in (
            "open",
            "high",
            "low",
            "close",
            "volume",
            "traded_value",
            "price_factor",
            "volume_factor",
        ):
            _decimal(getattr(self, name), f"adjusted bar {name}", positive=True)
        if self.high < max(self.open, self.low, self.close):
            raise PriceAdjustmentError("adjusted bar high is inconsistent")
        if self.low > min(self.open, self.high, self.close):
            raise PriceAdjustmentError("adjusted bar low is inconsistent")
        if self.price_factor * self.volume_factor != Decimal("1"):
            raise PriceAdjustmentError("price and volume factors must be reciprocal")
        if not self.low <= self.traded_value / self.volume <= self.high:
            raise PriceAdjustmentError("adjusted turnover implies an invalid price")
        if (
            type(self.applied_event_ids) is not tuple
            or self.applied_event_ids != tuple(sorted(set(self.applied_event_ids)))
        ):
            raise PriceAdjustmentError("applied event IDs must be sorted and unique")
        for value, name in (
            (self.raw_bar_id, "raw_bar_id"),
            (self.identity_binding_id, "identity_binding_id"),
            (self.identity_snapshot_id, "identity_snapshot_id"),
            (self.adjustment_snapshot_id, "adjustment_snapshot_id"),
        ):
            _sha(value, name)
        for value in self.applied_event_ids:
            _sha(value, "applied event ID")
        object.__setattr__(
            self,
            "raw_knowledge_time",
            _utc(self.raw_knowledge_time, "raw_knowledge_time"),
        )
        object.__setattr__(
            self,
            "knowledge_time",
            _utc(self.knowledge_time, "knowledge_time"),
        )
        if self.knowledge_time < self.raw_knowledge_time:
            raise PriceAdjustmentError("adjusted bar cannot predate its raw input")
        if self.adjustment_policy_version != ADJUSTMENT_POLICY_VERSION:
            raise PriceAdjustmentError("unsupported price-adjustment policy")
        object.__setattr__(self, "adjusted_bar_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "adjusted_bar_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.adjusted_bar_id != self._calculated_id():
            raise PriceAdjustmentError("adjusted bar content identity failed")


@dataclass(frozen=True, slots=True)
class CorporateActionAdjustedHistory:
    stable_instrument_id: str
    stable_listing_id: str
    signal_session: date
    cutoff: datetime
    adjustment_knowledge_time: datetime
    corporate_action_snapshot_id: str
    bars: tuple[AdjustedPriceBar, ...]
    price_basis: str = ADJUSTED_PRICE_BASIS
    adjustment_policy_version: str = ADJUSTMENT_POLICY_VERSION
    history_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.stable_instrument_id, "stable_instrument_id"),
            (self.stable_listing_id, "stable_listing_id"),
            (self.corporate_action_snapshot_id, "corporate_action_snapshot_id"),
        ):
            _sha(value, name)
        if type(self.signal_session) is not date:
            raise PriceAdjustmentError("signal_session must be a date")
        object.__setattr__(self, "cutoff", _utc(self.cutoff, "history cutoff"))
        object.__setattr__(
            self,
            "adjustment_knowledge_time",
            _utc(self.adjustment_knowledge_time, "adjustment knowledge time"),
        )
        if self.adjustment_knowledge_time > self.cutoff:
            raise PriceAdjustmentError("adjustment knowledge time exceeds history cutoff")
        if (
            type(self.bars) is not tuple
            or not self.bars
            or any(type(value) is not AdjustedPriceBar for value in self.bars)
        ):
            raise PriceAdjustmentError("adjusted history bars must be a non-empty exact tuple")
        sessions = tuple(value.market_session for value in self.bars)
        if sessions != tuple(sorted(set(sessions))) or sessions[-1] != self.signal_session:
            raise PriceAdjustmentError("adjusted history sessions are invalid")
        for value in self.bars:
            value.verify_content_identity()
            if (
                value.adjustment_snapshot_id != self.corporate_action_snapshot_id
                or value.knowledge_time > self.cutoff
            ):
                raise PriceAdjustmentError("adjusted bar lineage differs from history")
        if self.price_basis != ADJUSTED_PRICE_BASIS:
            raise PriceAdjustmentError("adjusted history has the wrong price basis")
        if self.adjustment_policy_version != ADJUSTMENT_POLICY_VERSION:
            raise PriceAdjustmentError("adjusted history has the wrong policy")
        object.__setattr__(self, "history_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "history_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        for value in self.bars:
            if type(value) is not AdjustedPriceBar:
                raise PriceAdjustmentError("adjusted history graph contains an invalid bar")
            value.verify_content_identity()
        if self.history_id != self._calculated_id():
            raise PriceAdjustmentError("adjusted history content identity failed")


def build_adjusted_price_history(
    *,
    raw_bars: tuple[RawNseEodBar, ...],
    identity_bindings: tuple[StableRawBarBinding, ...],
    stable_instrument_id: str,
    stable_listing_id: str,
    signal_session: date,
    cutoff: datetime,
    corporate_actions: CorporateActionSnapshot,
) -> CorporateActionAdjustedHistory:
    """Build a cutoff-specific view without mutating the raw NSE bars."""

    if (
        type(raw_bars) is not tuple
        or not raw_bars
        or any(type(value) is not RawNseEodBar for value in raw_bars)
    ):
        raise PriceAdjustmentError("raw bars must be a non-empty exact tuple")
    if type(corporate_actions) is not CorporateActionSnapshot:
        raise PriceAdjustmentError("corporate_actions must be exact")
    if type(signal_session) is not date:
        raise PriceAdjustmentError("signal_session must be a date")
    cutoff = _utc(cutoff, "adjustment cutoff")
    corporate_actions.verify_content_identity()
    if (
        not corporate_actions.complete
        or not corporate_actions.actionable
        or corporate_actions.readiness is ReferenceReadiness.COLLECTION_ONLY
    ):
        raise PriceAdjustmentError("corporate-action evidence is not actionable")
    if corporate_actions.cutoff > cutoff:
        raise PriceAdjustmentError("corporate-action snapshot is future-known")
    if (
        corporate_actions.coverage_start > raw_bars[0].market_session
        or corporate_actions.coverage_end < signal_session
    ):
        raise PriceAdjustmentError("corporate-action coverage is incomplete")
    sessions = tuple(value.market_session for value in raw_bars)
    if sessions != tuple(sorted(set(sessions))) or sessions[-1] != signal_session:
        raise PriceAdjustmentError("raw history sessions are invalid")
    if (
        type(identity_bindings) is not tuple
        or len(identity_bindings) != len(raw_bars)
        or any(type(value) is not StableRawBarBinding for value in identity_bindings)
    ):
        raise PriceAdjustmentError("one exact identity binding is required per raw bar")
    for raw, binding in zip(raw_bars, identity_bindings):
        binding.verify_content_identity()
        if (
            binding.market_session != raw.market_session
            or binding.raw_bar_id != raw.bar_id
            or binding.stable_instrument_id != stable_instrument_id
            or binding.stable_listing_id != stable_listing_id
        ):
            raise PriceAdjustmentError("raw bar and stable-identity binding differ")
        if binding.knowledge_time > cutoff:
            raise PriceAdjustmentError("identity binding is future-known")
    for value in raw_bars:
        value.verify_content_identity()
        if value.knowledge_time > cutoff:
            raise PriceAdjustmentError("raw history contains future-known evidence")

    relevant = tuple(
        value
        for value in corporate_actions.active_events
        if value.stable_instrument_id == stable_instrument_id
        and raw_bars[0].market_session < value.effective_session <= signal_session
    )
    for value in relevant:
        if value.stable_listing_id not in (None, stable_listing_id):
            raise PriceAdjustmentError("corporate action belongs to another listing")
        if value.action_type not in {CorporateActionType.SPLIT, CorporateActionType.BONUS}:
            raise PriceAdjustmentError("corporate action requires an unsupported adjustment")
        if value.automatic_raw_price_factor is None:
            raise PriceAdjustmentError("corporate action has no safe automatic factor")

    adjusted: list[AdjustedPriceBar] = []
    for raw, binding in zip(raw_bars, identity_bindings):
        applied = tuple(
            value for value in relevant if raw.market_session < value.effective_session
        )
        price_factor = Decimal("1")
        for value in applied:
            factor = value.automatic_raw_price_factor
            assert factor is not None
            price_factor *= factor
        volume_factor = Decimal("1") / price_factor
        adjusted.append(
            AdjustedPriceBar(
                market_session=raw.market_session,
                symbol=raw.symbol,
                series=raw.series,
                validated_isin=raw.validated_isin,
                open=raw.open * price_factor,
                high=raw.high * price_factor,
                low=raw.low * price_factor,
                close=raw.close * price_factor,
                volume=Decimal(raw.volume) * volume_factor,
                traded_value=raw.traded_value,
                price_factor=price_factor,
                volume_factor=volume_factor,
                applied_event_ids=tuple(sorted(value.event_id for value in applied)),
                raw_bar_id=raw.bar_id,
                identity_binding_id=binding.binding_id,
                identity_snapshot_id=binding.identity_snapshot_id,
                raw_knowledge_time=raw.knowledge_time,
                knowledge_time=max(
                    raw.knowledge_time,
                    binding.knowledge_time,
                    corporate_actions.cutoff,
                ),
                adjustment_snapshot_id=corporate_actions.snapshot_id,
            )
        )
    return CorporateActionAdjustedHistory(
        stable_instrument_id=stable_instrument_id,
        stable_listing_id=stable_listing_id,
        signal_session=signal_session,
        cutoff=cutoff,
        adjustment_knowledge_time=corporate_actions.cutoff,
        corporate_action_snapshot_id=corporate_actions.snapshot_id,
        bars=tuple(adjusted),
    )
