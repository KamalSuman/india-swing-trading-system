from __future__ import annotations

from dataclasses import dataclass, field, fields
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR

from india_swing.data.asof import validate_snapshot
from india_swing.domain.models import (
    DataSnapshot,
    ForecastSummary,
    InstrumentSnapshot,
    ProbabilityStatus,
    SignalFeatures,
    TradeSetup,
)
from india_swing.identity import content_id
from india_swing.reference.calendar import CalendarSnapshot

from .calibration import WalkForwardCalibration


ZERO = Decimal("0")
ONE = Decimal("1")


class DeterministicSwingSignalError(ValueError):
    """Raised when the deterministic signal engine cannot prove an input."""


def _decimal(value: Decimal, name: str, *, positive: bool = False) -> None:
    if type(value) is not Decimal or not value.is_finite():
        raise DeterministicSwingSignalError(f"{name} must be a finite Decimal")
    if positive and value <= ZERO:
        raise DeterministicSwingSignalError(f"{name} must be positive")


def _aware_utc(value: datetime, name: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is None:
        raise DeterministicSwingSignalError(f"{name} must be timezone-aware")
    try:
        offset = value.utcoffset()
    except Exception:
        raise DeterministicSwingSignalError(f"{name} has invalid timezone behavior") from None
    if offset is None:
        raise DeterministicSwingSignalError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _clamp(value: Decimal) -> Decimal:
    return max(ZERO, min(value, ONE))


def _mean(values: tuple[Decimal, ...]) -> Decimal:
    if not values:
        raise DeterministicSwingSignalError("mean requires at least one value")
    return sum(values, ZERO) / Decimal(len(values))


def _median(values: tuple[Decimal, ...]) -> Decimal:
    if not values:
        raise DeterministicSwingSignalError("median requires at least one value")
    ordered = tuple(sorted(values))
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / Decimal("2")


def _tick_floor(value: Decimal, tick: Decimal) -> Decimal:
    return (value / tick).to_integral_value(rounding=ROUND_FLOOR) * tick


def _tick_ceiling(value: Decimal, tick: Decimal) -> Decimal:
    return (value / tick).to_integral_value(rounding=ROUND_CEILING) * tick


@dataclass(frozen=True, slots=True)
class AsOfSwingBar:
    """One adjusted EOD bar bound to evidence known at a decision cutoff."""

    market_session: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    traded_value: Decimal
    available_at: datetime
    evidence_id: str
    content_hash: str
    bar_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.market_session) is not date:
            raise DeterministicSwingSignalError("bar market_session must be a date")
        for name in ("open", "high", "low", "close", "traded_value"):
            _decimal(getattr(self, name), f"bar {name}", positive=True)
        if self.high < max(self.open, self.low, self.close):
            raise DeterministicSwingSignalError("bar high is inconsistent")
        if self.low > min(self.open, self.high, self.close):
            raise DeterministicSwingSignalError("bar low is inconsistent")
        _decimal(self.volume, "bar volume", positive=True)
        if not self.low <= self.traded_value / self.volume <= self.high:
            raise DeterministicSwingSignalError("bar traded value implies an invalid price")
        object.__setattr__(
            self,
            "available_at",
            _aware_utc(self.available_at, "bar available_at"),
        )
        for value, name in (
            (self.evidence_id, "bar evidence_id"),
            (self.content_hash, "bar content_hash"),
        ):
            if type(value) is not str or not value.strip():
                raise DeterministicSwingSignalError(f"{name} is required")
        object.__setattr__(self, "bar_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "bar_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.bar_id != self._calculated_id():
            raise DeterministicSwingSignalError("bar content identity failed")


@dataclass(frozen=True, slots=True)
class InstrumentSwingHistory:
    instrument_id: str
    listing_id: str
    tick_size: Decimal
    tick_available_at: datetime
    tick_evidence_id: str
    tick_content_hash: str
    adjustment_available_at: datetime
    adjustment_evidence_id: str
    adjustment_content_hash: str
    price_basis: str
    bars: tuple[AsOfSwingBar, ...]
    history_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.instrument_id, "history instrument_id"),
            (self.listing_id, "history listing_id"),
        ):
            if type(value) is not str or not value.strip():
                raise DeterministicSwingSignalError(f"{name} is required")
        _decimal(self.tick_size, "history tick_size", positive=True)
        object.__setattr__(
            self,
            "tick_available_at",
            _aware_utc(self.tick_available_at, "history tick_available_at"),
        )
        object.__setattr__(
            self,
            "adjustment_available_at",
            _aware_utc(
                self.adjustment_available_at,
                "history adjustment_available_at",
            ),
        )
        for value, name in (
            (self.tick_evidence_id, "history tick_evidence_id"),
            (self.tick_content_hash, "history tick_content_hash"),
            (self.adjustment_evidence_id, "history adjustment_evidence_id"),
            (self.adjustment_content_hash, "history adjustment_content_hash"),
        ):
            if type(value) is not str or not value.strip():
                raise DeterministicSwingSignalError(f"{name} is required")
        if (
            type(self.bars) is not tuple
            or not self.bars
            or any(type(value) is not AsOfSwingBar for value in self.bars)
        ):
            raise DeterministicSwingSignalError("history bars must be a non-empty exact tuple")
        sessions = tuple(value.market_session for value in self.bars)
        if sessions != tuple(sorted(set(sessions))):
            raise DeterministicSwingSignalError("history sessions must be ordered and unique")
        evidence_ids = tuple(value.evidence_id for value in self.bars)
        if len(set(evidence_ids)) != len(evidence_ids):
            raise DeterministicSwingSignalError("history evidence IDs must be unique")
        if self.tick_evidence_id in evidence_ids:
            raise DeterministicSwingSignalError("tick and bar evidence IDs must differ")
        if (
            self.adjustment_evidence_id in evidence_ids
            or self.adjustment_evidence_id == self.tick_evidence_id
        ):
            raise DeterministicSwingSignalError(
                "adjustment, tick, and bar evidence IDs must differ"
            )
        if self.price_basis != "CORPORATE_ACTION_ADJUSTED_AS_OF_CUTOFF":
            raise DeterministicSwingSignalError(
                "history must use cutoff-bound corporate-action-adjusted prices"
            )
        for value in self.bars:
            value.verify_content_identity()
        object.__setattr__(self, "history_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "instrument_id": self.instrument_id,
                "listing_id": self.listing_id,
                "tick_size": self.tick_size,
                "tick_available_at": self.tick_available_at,
                "tick_evidence_id": self.tick_evidence_id,
                "tick_content_hash": self.tick_content_hash,
                "adjustment_available_at": self.adjustment_available_at,
                "adjustment_evidence_id": self.adjustment_evidence_id,
                "adjustment_content_hash": self.adjustment_content_hash,
                "price_basis": self.price_basis,
                "bars": self.bars,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if any(type(value) is not AsOfSwingBar for value in self.bars):
            raise DeterministicSwingSignalError("history graph contains an invalid bar")
        for value in self.bars:
            value.verify_content_identity()
        if self.history_id != self._calculated_id():
            raise DeterministicSwingSignalError("history content identity failed")


@dataclass(frozen=True, slots=True)
class DeterministicSwingSignalConfig:
    minimum_history_sessions: int = 60
    momentum_lookback_sessions: int = 20
    trend_lookback_sessions: int = 50
    atr_lookback_sessions: int = 14
    volume_lookback_sessions: int = 20
    breakout_lookback_sessions: int = 20
    full_momentum_score: Decimal = Decimal("0.10")
    minimum_daily_traded_value: Decimal = Decimal("10000000")
    entry_atr_buffer: Decimal = Decimal("0.10")
    stop_atr_multiple: Decimal = Decimal("1.50")
    target_net_reward_risk: Decimal = Decimal("2.50")
    base_round_trip_cost_bps: Decimal = Decimal("30")
    maximum_holding_sessions: int = 10
    entry_delay_minutes: int = 5
    entry_expiry_buffer_minutes: int = 15
    policy_version: str = "deterministic-swing-signals/v1"
    config_id: str = field(init=False)

    def __post_init__(self) -> None:
        integer_fields = (
            "minimum_history_sessions",
            "momentum_lookback_sessions",
            "trend_lookback_sessions",
            "atr_lookback_sessions",
            "volume_lookback_sessions",
            "breakout_lookback_sessions",
            "maximum_holding_sessions",
            "entry_delay_minutes",
            "entry_expiry_buffer_minutes",
        )
        for name in integer_fields:
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise DeterministicSwingSignalError(f"{name} must be a positive integer")
        required = max(
            self.momentum_lookback_sessions + 1,
            self.trend_lookback_sessions,
            self.atr_lookback_sessions + 1,
            self.volume_lookback_sessions + 1,
            self.breakout_lookback_sessions + 1,
        )
        if self.minimum_history_sessions < required:
            raise DeterministicSwingSignalError(
                "minimum history cannot be shorter than a feature lookback"
            )
        for name in (
            "full_momentum_score",
            "minimum_daily_traded_value",
            "entry_atr_buffer",
            "stop_atr_multiple",
            "target_net_reward_risk",
            "base_round_trip_cost_bps",
        ):
            _decimal(getattr(self, name), name, positive=True)
        if self.policy_version != "deterministic-swing-signals/v1":
            raise DeterministicSwingSignalError("unsupported signal policy")
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
        if self.config_id != self._calculated_id():
            raise DeterministicSwingSignalError("signal config content identity failed")


@dataclass(frozen=True, slots=True)
class SwingTechnicalMetrics:
    """Point-in-time technical features computed from one exact swing history.

    These are descriptive technical scores, not probabilities. They carry no
    forecast, confidence, or execution authority on their own.
    """

    momentum_return: Decimal
    trend_quality: Decimal
    volume_confirmation: Decimal
    median_traded_value: Decimal
    atr: Decimal
    relative_strength: Decimal
    liquidity_quality: Decimal
    evidence_ids: tuple[str, ...]
    metrics_id: str = field(init=False)

    def __post_init__(self) -> None:
        for name in (
            "momentum_return",
            "trend_quality",
            "volume_confirmation",
            "median_traded_value",
            "atr",
            "relative_strength",
            "liquidity_quality",
        ):
            _decimal(getattr(self, name), f"metrics.{name}")
        if self.atr <= ZERO:
            raise DeterministicSwingSignalError("ATR must be positive")
        if self.median_traded_value < ZERO:
            raise DeterministicSwingSignalError("median_traded_value cannot be negative")
        if (
            type(self.evidence_ids) is not tuple
            or not self.evidence_ids
            or any(type(value) is not str or not value.strip() for value in self.evidence_ids)
        ):
            raise DeterministicSwingSignalError(
                "metrics evidence_ids must be a non-empty exact text tuple"
            )
        if len(set(self.evidence_ids)) != len(self.evidence_ids):
            raise DeterministicSwingSignalError("metrics evidence IDs must be unique")
        object.__setattr__(self, "metrics_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "metrics_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.metrics_id != self._calculated_id():
            raise DeterministicSwingSignalError("technical metrics content identity failed")


def calculate_swing_technical_metrics(
    history: InstrumentSwingHistory,
    config: DeterministicSwingSignalConfig,
) -> SwingTechnicalMetrics:
    """Pure, replay-verifiable technical-feature kernel shared by every caller."""

    if type(history) is not InstrumentSwingHistory:
        raise DeterministicSwingSignalError("history must be exact")
    if type(config) is not DeterministicSwingSignalConfig:
        raise DeterministicSwingSignalError("config must be exact")
    history.verify_content_identity()
    config.verify_content_identity()
    if len(history.bars) < config.minimum_history_sessions:
        raise DeterministicSwingSignalError("history is shorter than the configured minimum")

    bars = history.bars
    current = bars[-1]
    momentum_start = bars[-(config.momentum_lookback_sessions + 1)].close
    momentum = current.close / momentum_start - ONE

    trend_bars = bars[-config.trend_lookback_sessions :]
    positive = sum(
        trend_bars[index].close > trend_bars[index - 1].close
        for index in range(1, len(trend_bars))
    )
    positive_fraction = Decimal(positive) / Decimal(len(trend_bars) - 1)
    moving_average = _mean(tuple(value.close for value in trend_bars))
    above_average = _clamp((current.close / moving_average - Decimal("0.95")) / Decimal("0.10"))
    prior_high = max(
        value.high for value in bars[-(config.breakout_lookback_sessions + 1) : -1]
    )
    breakout_proximity = _clamp(
        (current.close / prior_high - Decimal("0.90")) / Decimal("0.10")
    )
    trend_quality = (
        positive_fraction + above_average + breakout_proximity
    ) / Decimal("3")

    prior_volume = tuple(
        value.volume
        for value in bars[-(config.volume_lookback_sessions + 1) : -1]
    )
    volume_confirmation = _clamp(
        current.volume / _median(prior_volume) / Decimal("2")
    )
    median_traded_value = _median(
        tuple(value.traded_value for value in bars[-config.volume_lookback_sessions :])
    )

    atr_bars = bars[-(config.atr_lookback_sessions + 1) :]
    true_ranges: list[Decimal] = []
    for index in range(1, len(atr_bars)):
        bar = atr_bars[index]
        previous_close = atr_bars[index - 1].close
        true_ranges.append(
            max(
                bar.high - bar.low,
                abs(bar.high - previous_close),
                abs(bar.low - previous_close),
            )
        )
    atr = _mean(tuple(true_ranges))
    if atr <= ZERO:
        raise DeterministicSwingSignalError("ATR must be positive")
    required = max(
        config.momentum_lookback_sessions + 1,
        config.trend_lookback_sessions,
        config.atr_lookback_sessions + 1,
        config.volume_lookback_sessions + 1,
        config.breakout_lookback_sessions + 1,
    )
    relative_strength = _clamp(momentum / config.full_momentum_score)
    liquidity_quality = _clamp(
        median_traded_value / config.minimum_daily_traded_value / Decimal("2")
    )
    return SwingTechnicalMetrics(
        momentum_return=momentum,
        trend_quality=trend_quality,
        volume_confirmation=volume_confirmation,
        median_traded_value=median_traded_value,
        atr=atr,
        relative_strength=relative_strength,
        liquidity_quality=liquidity_quality,
        evidence_ids=tuple(value.evidence_id for value in bars[-required:]),
    )


@dataclass(frozen=True, slots=True)
class SwingTradeLevels:
    """Tick-rounded entry/stop/target construction at one declared cost assumption."""

    entry_low: Decimal
    entry_high: Decimal
    stop: Decimal
    target: Decimal
    estimated_cost_bps: Decimal
    levels_id: str = field(init=False)

    def __post_init__(self) -> None:
        for name in ("entry_low", "entry_high", "target"):
            _decimal(getattr(self, name), f"levels.{name}", positive=True)
        _decimal(self.stop, "levels.stop")
        if self.stop <= ZERO:
            raise DeterministicSwingSignalError("ATR stop is non-positive")
        _decimal(self.estimated_cost_bps, "levels.estimated_cost_bps")
        if self.estimated_cost_bps < ZERO:
            raise DeterministicSwingSignalError("levels.estimated_cost_bps cannot be negative")
        object.__setattr__(self, "levels_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "levels_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.levels_id != self._calculated_id():
            raise DeterministicSwingSignalError("trade levels content identity failed")

    @property
    def cost_per_share(self) -> Decimal:
        return self.entry_high * self.estimated_cost_bps / Decimal("10000")

    @property
    def net_reward_risk(self) -> Decimal:
        cost_per_share = self.cost_per_share
        net_loss_per_share = self.entry_high - self.stop + cost_per_share
        return (self.target - self.entry_high - cost_per_share) / net_loss_per_share


def calculate_swing_trade_levels(
    *,
    current_close: Decimal,
    tick: Decimal,
    atr: Decimal,
    estimated_cost_bps: Decimal,
    config: DeterministicSwingSignalConfig,
) -> SwingTradeLevels:
    """Pure, replay-verifiable tick-aligned level kernel shared by every caller."""

    if type(config) is not DeterministicSwingSignalConfig:
        raise DeterministicSwingSignalError("config must be exact")
    config.verify_content_identity()
    _decimal(current_close, "current_close", positive=True)
    _decimal(tick, "tick", positive=True)
    _decimal(atr, "atr", positive=True)
    _decimal(estimated_cost_bps, "estimated_cost_bps")
    if estimated_cost_bps < ZERO:
        raise DeterministicSwingSignalError("estimated_cost_bps cannot be negative")
    if current_close != _tick_floor(current_close, tick):
        raise DeterministicSwingSignalError("signal close is not tick-aligned")

    entry_low = _tick_floor(current_close, tick)
    entry_high = _tick_ceiling(current_close + config.entry_atr_buffer * atr, tick)
    stop = _tick_floor(entry_low - config.stop_atr_multiple * atr, tick)
    cost_per_share = entry_high * estimated_cost_bps / Decimal("10000")
    net_loss_per_share = entry_high - stop + cost_per_share
    target = _tick_ceiling(
        entry_high + cost_per_share + config.target_net_reward_risk * net_loss_per_share,
        tick,
    )
    return SwingTradeLevels(
        entry_low=entry_low,
        entry_high=entry_high,
        stop=stop,
        target=target,
        estimated_cost_bps=estimated_cost_bps,
    )


@dataclass(frozen=True, slots=True)
class SwingNextEntryWindow:
    """The next eligible executable calendar window and its holding boundary."""

    entry_day: date
    earliest_entry_at: datetime
    entry_expires_at: datetime
    holding_boundary_day: date
    window_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.entry_day) is not date:
            raise DeterministicSwingSignalError("entry window entry_day must be a date")
        if type(self.holding_boundary_day) is not date:
            raise DeterministicSwingSignalError(
                "entry window holding_boundary_day must be a date"
            )
        for value, name in (
            (self.earliest_entry_at, "earliest_entry_at"),
            (self.entry_expires_at, "entry_expires_at"),
        ):
            if type(value) is not datetime or value.tzinfo is None or value.utcoffset() is None:
                raise DeterministicSwingSignalError(
                    f"entry window {name} must be timezone-aware"
                )
        if self.earliest_entry_at >= self.entry_expires_at:
            raise DeterministicSwingSignalError("entry window must open before it expires")
        if self.holding_boundary_day < self.entry_day:
            raise DeterministicSwingSignalError(
                "holding boundary cannot precede the entry day"
            )
        object.__setattr__(self, "window_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "window_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.window_id != self._calculated_id():
            raise DeterministicSwingSignalError("entry window content identity failed")


def calculate_next_entry_window(
    calendar: CalendarSnapshot,
    market_session: date,
    config: DeterministicSwingSignalConfig,
) -> SwingNextEntryWindow:
    """Pure, replay-verifiable next-session entry-window kernel shared by every caller."""

    if type(calendar) is not CalendarSnapshot:
        raise DeterministicSwingSignalError("calendar must be exact")
    calendar.verify_content_identity()
    if type(config) is not DeterministicSwingSignalConfig:
        raise DeterministicSwingSignalError("config must be exact")
    config.verify_content_identity()
    if type(market_session) is not date:
        raise DeterministicSwingSignalError("market_session must be a date")

    entry_day = calendar.next_session(market_session)
    executable = tuple(value for value in entry_day.session_windows if value.is_executable)
    if not executable:
        raise DeterministicSwingSignalError("next session has no executable window")
    entry_window = executable[0]
    earliest_entry_at = entry_window.opens_at + timedelta(minutes=config.entry_delay_minutes)
    entry_expires_at = entry_window.closes_at - timedelta(
        minutes=config.entry_expiry_buffer_minutes
    )
    entry_day.require_same_session_window(earliest_entry_at, entry_expires_at)
    holding_boundary = calendar.advance_sessions(entry_day.day, config.maximum_holding_sessions)
    return SwingNextEntryWindow(
        entry_day=entry_day.day,
        earliest_entry_at=earliest_entry_at,
        entry_expires_at=entry_expires_at,
        holding_boundary_day=holding_boundary.day,
    )


class DeterministicSwingSignalProvider:
    """Point-in-time, explainable signal and trade-level engine.

    It deliberately does not invent win probabilities. Those remain provisional
    until a preregistered walk-forward calibration supplies them.
    """

    version = "deterministic-swing-signals/v1"

    def __init__(
        self,
        *,
        snapshot: DataSnapshot,
        histories: tuple[InstrumentSwingHistory, ...],
        calendar: CalendarSnapshot,
        config: DeterministicSwingSignalConfig | None = None,
        calibration: WalkForwardCalibration | None = None,
    ) -> None:
        if type(snapshot) is not DataSnapshot:
            raise DeterministicSwingSignalError("snapshot must be exact")
        if type(calendar) is not CalendarSnapshot:
            raise DeterministicSwingSignalError("calendar must be exact")
        if config is None:
            config = DeterministicSwingSignalConfig()
        if type(config) is not DeterministicSwingSignalConfig:
            raise DeterministicSwingSignalError("config must be exact")
        if (
            type(histories) is not tuple
            or not histories
            or any(type(value) is not InstrumentSwingHistory for value in histories)
        ):
            raise DeterministicSwingSignalError("histories must be a non-empty exact tuple")
        if histories != tuple(sorted(histories, key=lambda value: value.instrument_id)):
            raise DeterministicSwingSignalError("histories must be instrument-ID ordered")
        if len({value.instrument_id for value in histories}) != len(histories):
            raise DeterministicSwingSignalError("history instrument IDs must be unique")
        snapshot.verify_content_identity()
        calendar.verify_content_identity()
        config.verify_content_identity()
        if snapshot.calendar_version != calendar.version:
            raise DeterministicSwingSignalError("snapshot and calendar versions differ")
        if calendar.cutoff > snapshot.decision_time.astimezone(timezone.utc):
            raise DeterministicSwingSignalError("calendar was unavailable at decision time")

        evidence = {value.evidence_id: value for value in snapshot.evidence}
        if len(evidence) != len(snapshot.evidence):
            raise DeterministicSwingSignalError("snapshot evidence IDs must be unique")
        if calibration is not None:
            if type(calibration) is not WalkForwardCalibration:
                raise DeterministicSwingSignalError("calibration must be exact")
            calibration.verify_content_identity()
            if calibration.signal_config_id != config.config_id:
                raise DeterministicSwingSignalError(
                    "calibration belongs to another signal configuration"
                )
            if calibration.cutoff > snapshot.decision_time.astimezone(timezone.utc):
                raise DeterministicSwingSignalError("calibration is future-known")
            calibration_item = evidence.get(calibration.calibration_id)
            if (
                calibration_item is None
                or calibration_item.content_hash != calibration.calibration_id
                or calibration_item.available_at.astimezone(timezone.utc)
                != calibration.cutoff
            ):
                raise DeterministicSwingSignalError(
                    "calibration evidence binding differs"
                )
        calculated: dict[str, SwingTechnicalMetrics] = {}
        for history in histories:
            history.verify_content_identity()
            if len(history.bars) < config.minimum_history_sessions:
                raise DeterministicSwingSignalError("history is shorter than the configured minimum")
            if history.bars[-1].market_session != snapshot.market_session:
                raise DeterministicSwingSignalError("history does not end at the signal session")
            tick_item = evidence.get(history.tick_evidence_id)
            if (
                tick_item is None
                or tick_item.content_hash != history.tick_content_hash
                or tick_item.available_at.astimezone(timezone.utc)
                != history.tick_available_at
            ):
                raise DeterministicSwingSignalError("tick evidence binding differs")
            if history.tick_available_at > snapshot.decision_time.astimezone(timezone.utc):
                raise DeterministicSwingSignalError("tick size is future-known")
            adjustment_item = evidence.get(history.adjustment_evidence_id)
            if (
                adjustment_item is None
                or adjustment_item.content_hash != history.adjustment_content_hash
                or adjustment_item.available_at.astimezone(timezone.utc)
                != history.adjustment_available_at
            ):
                raise DeterministicSwingSignalError(
                    "corporate-action evidence binding differs"
                )
            if history.adjustment_available_at > snapshot.decision_time.astimezone(timezone.utc):
                raise DeterministicSwingSignalError(
                    "corporate-action adjustment is future-known"
                )
            for bar in history.bars:
                calendar.require_session(bar.market_session)
                if bar.market_session > snapshot.market_session:
                    raise DeterministicSwingSignalError("history contains a future session")
                if bar.available_at > snapshot.decision_time.astimezone(timezone.utc):
                    raise DeterministicSwingSignalError("history contains future-known evidence")
                item = evidence.get(bar.evidence_id)
                if item is None:
                    raise DeterministicSwingSignalError("history evidence is absent from snapshot")
                if (
                    item.content_hash != bar.content_hash
                    or item.available_at.astimezone(timezone.utc) != bar.available_at
                ):
                    raise DeterministicSwingSignalError("history evidence binding differs")
            calculated[history.instrument_id] = calculate_swing_technical_metrics(
                history, config
            )

        try:
            validate_snapshot(snapshot)
        except Exception:
            raise DeterministicSwingSignalError("snapshot contains unavailable evidence") from None

        entry_window = calculate_next_entry_window(calendar, snapshot.market_session, config)

        self.snapshot = snapshot
        self.histories = histories
        self.calendar = calendar
        self.config = config
        self.calibration = calibration
        self._history_by_id = {value.instrument_id: value for value in histories}
        self._calculated = calculated
        self._entry_window = entry_window

    def identity_material(self) -> object:
        return {
            "version": self.version,
            "snapshot_fingerprint": self.snapshot.content_fingerprint,
            "calendar_snapshot_id": self.calendar.snapshot_id,
            "config_id": self.config.config_id,
            "calibration_id": (
                None if self.calibration is None else self.calibration.calibration_id
            ),
            "history_ids": tuple(value.history_id for value in self.histories),
        }

    def _verify_bound_inputs(self) -> None:
        self.snapshot.verify_content_identity()
        self.calendar.verify_content_identity()
        self.config.verify_content_identity()
        if self.calibration is not None:
            self.calibration.verify_content_identity()
        for value in self.histories:
            value.verify_content_identity()

    def generate(
        self,
        instrument: InstrumentSnapshot,
        forecast: ForecastSummary,
        snapshot: DataSnapshot,
    ) -> tuple[SignalFeatures, TradeSetup, tuple[str, ...]]:
        self._verify_bound_inputs()
        if type(instrument) is not InstrumentSnapshot:
            raise DeterministicSwingSignalError("instrument must be exact")
        if type(forecast) is not ForecastSummary:
            raise DeterministicSwingSignalError("forecast must be exact")
        if type(snapshot) is not DataSnapshot:
            raise DeterministicSwingSignalError("snapshot must be exact")
        if snapshot.content_fingerprint != self.snapshot.content_fingerprint:
            raise DeterministicSwingSignalError("provider is bound to another snapshot")
        history = self._history_by_id.get(instrument.instrument_id)
        calculated = self._calculated.get(instrument.instrument_id)
        if history is None or calculated is None:
            raise DeterministicSwingSignalError("instrument history is unavailable")
        if history.listing_id != instrument.listing_id:
            raise DeterministicSwingSignalError("instrument listing differs from history")
        if instrument.price_session != snapshot.market_session:
            raise DeterministicSwingSignalError("instrument price session differs")
        if instrument.last_price != history.bars[-1].close:
            raise DeterministicSwingSignalError("instrument price differs from signal close")
        if instrument.data_available_at.astimezone(timezone.utc) != history.bars[-1].available_at:
            raise DeterministicSwingSignalError("instrument availability differs from signal bar")
        if (
            forecast.instrument_id != instrument.instrument_id
            or forecast.listing_id != instrument.listing_id
            or forecast.universe_snapshot_id != instrument.universe_snapshot_id
            or forecast.data_snapshot_id != snapshot.snapshot_id
            or forecast.data_snapshot_fingerprint != snapshot.content_fingerprint
            or forecast.instrument_fingerprint != instrument.content_fingerprint
            or forecast.as_of != snapshot.decision_time
        ):
            raise DeterministicSwingSignalError("forecast lineage differs from bound inputs")

        config = self.config
        current_close = history.bars[-1].close
        tick = history.tick_size
        estimated_cost_bps = max(
            config.base_round_trip_cost_bps,
            instrument.quoted_spread_bps,
        )
        levels = calculate_swing_trade_levels(
            current_close=current_close,
            tick=tick,
            atr=calculated.atr,
            estimated_cost_bps=estimated_cost_bps,
            config=config,
        )

        common = {
            "instrument_id": instrument.instrument_id,
            "listing_id": instrument.listing_id,
            "universe_snapshot_id": instrument.universe_snapshot_id,
            "data_snapshot_id": snapshot.snapshot_id,
            "data_snapshot_fingerprint": snapshot.content_fingerprint,
            "instrument_fingerprint": instrument.content_fingerprint,
            "provider_version": self.version,
        }
        signals = SignalFeatures(
            relative_strength=calculated.relative_strength,
            trend_quality=calculated.trend_quality,
            volume_confirmation=calculated.volume_confirmation,
            liquidity_quality=calculated.liquidity_quality,
            news_score=ZERO,
            estimated_cost_bps=estimated_cost_bps,
            **common,
        )
        calibration = self.calibration
        setup = TradeSetup(
            symbol=instrument.symbol,
            decision_time=snapshot.decision_time,
            earliest_entry_at=self._entry_window.earliest_entry_at,
            entry_expires_at=self._entry_window.entry_expires_at,
            entry_low=levels.entry_low,
            entry_high=levels.entry_high,
            stop=levels.stop,
            target=levels.target,
            target_probability=(
                ZERO if calibration is None else calibration.target_probability
            ),
            stop_probability=(
                ZERO if calibration is None else calibration.stop_probability
            ),
            expected_time_exit_r=(
                ZERO if calibration is None else calibration.expected_time_exit_r
            ),
            max_holding_sessions=config.maximum_holding_sessions,
            setup_reason=(
                f"{config.momentum_lookback_sessions}-session close return "
                f"{calculated.momentum_return}; trend quality "
                f"{calculated.trend_quality}; volume confirmation "
                f"{calculated.volume_confirmation}"
            ),
            stop_reason=(
                f"stop is {config.stop_atr_multiple} times the "
                f"{config.atr_lookback_sessions}-session ATR below the entry zone"
            ),
            target_reason=(
                f"target preserves at least {config.target_net_reward_risk} net reward/risk "
                f"after a {estimated_cost_bps} bps round-trip cost assumption"
            ),
            cancel_conditions=(
                "do not enter after the declared next-session entry window",
                "do not enter above the entry range",
                "cancel if eligibility, surveillance, liquidity, or evidence changes",
            )
            + (
                ("cancel if the signal is not walk-forward calibrated",)
                if calibration is None
                else ()
            ),
            probability_status=(
                ProbabilityStatus.PROVISIONAL
                if calibration is None
                else ProbabilityStatus.VALIDATED
            ),
            calibration_sample_size=(0 if calibration is None else calibration.sample_size),
            **common,
        )
        evidence_ids = calculated.evidence_ids + (
            history.tick_evidence_id,
            history.adjustment_evidence_id,
        )
        if calibration is not None:
            evidence_ids += (calibration.calibration_id,)
        return signals, setup, evidence_ids
