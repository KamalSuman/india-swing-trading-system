from __future__ import annotations

from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum

from india_swing.data.asof import validate_snapshot
from india_swing.domain.models import DataSnapshot, ForecastSummary, InstrumentSnapshot
from india_swing.identity import content_id
from india_swing.reference.calendar import CalendarSnapshot
from india_swing.signals.deterministic_swing import AsOfSwingBar, InstrumentSwingHistory


ZERO = Decimal("0")
ONE = Decimal("1")
HUNDRED = Decimal("100")


class RegimeEnsembleError(ValueError):
    pass


class MarketRegime(str, Enum):
    TRENDING = "TRENDING"
    RANGE_BOUND = "RANGE_BOUND"
    HIGH_VOLATILITY = "HIGH_VOLATILITY"
    RISK_OFF = "RISK_OFF"


class AlphaSpecialist(str, Enum):
    LIQUIDITY_QUALITY = "LIQUIDITY_QUALITY"
    MOMENTUM_BREAKOUT = "MOMENTUM_BREAKOUT"
    PULLBACK_CONTINUATION = "PULLBACK_CONTINUATION"
    VOLATILITY_CONTRACTION = "VOLATILITY_CONTRACTION"


def _decimal(value: object, name: str, *, positive: bool = False) -> Decimal:
    if type(value) is not Decimal or not value.is_finite():
        raise RegimeEnsembleError(f"{name} must be a finite Decimal")
    if positive and value <= ZERO:
        raise RegimeEnsembleError(f"{name} must be positive")
    return value


def _unit(value: object, name: str) -> Decimal:
    result = _decimal(value, name)
    if not ZERO <= result <= ONE:
        raise RegimeEnsembleError(f"{name} must be between zero and one")
    return result


def _clamp(value: Decimal, lower: Decimal = ZERO, upper: Decimal = ONE) -> Decimal:
    return max(lower, min(value, upper))


def _mean(values: tuple[Decimal, ...]) -> Decimal:
    if not values:
        raise RegimeEnsembleError("mean requires values")
    return sum(values, ZERO) / Decimal(len(values))


def _median(values: tuple[Decimal, ...]) -> Decimal:
    if not values:
        raise RegimeEnsembleError("median requires values")
    ordered = tuple(sorted(values))
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / Decimal("2")


def _utc(value: datetime, name: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is None:
        raise RegimeEnsembleError(f"{name} must be timezone-aware")
    try:
        offset = value.utcoffset()
    except Exception:
        raise RegimeEnsembleError(f"{name} has invalid timezone behavior") from None
    if offset is None:
        raise RegimeEnsembleError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class AlphaRegimeWeighting:
    regime: MarketRegime
    momentum_breakout: Decimal
    pullback_continuation: Decimal
    volatility_contraction: Decimal
    liquidity_quality: Decimal
    weighting_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.regime) is not MarketRegime:
            raise RegimeEnsembleError("weighting regime must be exact")
        values = tuple(
            _unit(getattr(self, name), f"weighting.{name}")
            for name in (
                "momentum_breakout",
                "pullback_continuation",
                "volatility_contraction",
                "liquidity_quality",
            )
        )
        if sum(values, ZERO) != ONE:
            raise RegimeEnsembleError("specialist weights must sum exactly to one")
        object.__setattr__(self, "weighting_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "weighting_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.weighting_id != self._calculated_id():
            raise RegimeEnsembleError("regime weighting content identity failed")

    def weight_for(self, specialist: AlphaSpecialist) -> Decimal:
        mapping = {
            AlphaSpecialist.MOMENTUM_BREAKOUT: self.momentum_breakout,
            AlphaSpecialist.PULLBACK_CONTINUATION: self.pullback_continuation,
            AlphaSpecialist.VOLATILITY_CONTRACTION: self.volatility_contraction,
            AlphaSpecialist.LIQUIDITY_QUALITY: self.liquidity_quality,
        }
        try:
            return mapping[specialist]
        except KeyError:
            raise RegimeEnsembleError("unsupported alpha specialist") from None


def _default_weightings() -> tuple[AlphaRegimeWeighting, ...]:
    return tuple(
        sorted(
            (
                AlphaRegimeWeighting(
                    regime=MarketRegime.TRENDING,
                    momentum_breakout=Decimal("0.45"),
                    pullback_continuation=Decimal("0.25"),
                    volatility_contraction=Decimal("0.20"),
                    liquidity_quality=Decimal("0.10"),
                ),
                AlphaRegimeWeighting(
                    regime=MarketRegime.RANGE_BOUND,
                    momentum_breakout=Decimal("0.20"),
                    pullback_continuation=Decimal("0.35"),
                    volatility_contraction=Decimal("0.30"),
                    liquidity_quality=Decimal("0.15"),
                ),
                AlphaRegimeWeighting(
                    regime=MarketRegime.HIGH_VOLATILITY,
                    momentum_breakout=Decimal("0.15"),
                    pullback_continuation=Decimal("0.15"),
                    volatility_contraction=Decimal("0.25"),
                    liquidity_quality=Decimal("0.45"),
                ),
                AlphaRegimeWeighting(
                    regime=MarketRegime.RISK_OFF,
                    momentum_breakout=Decimal("0.10"),
                    pullback_continuation=Decimal("0.10"),
                    volatility_contraction=Decimal("0.20"),
                    liquidity_quality=Decimal("0.60"),
                ),
            ),
            key=lambda value: value.regime.value,
        )
    )


@dataclass(frozen=True, slots=True)
class RegimeEnsembleConfig:
    minimum_history_sessions: int = 60
    short_momentum_sessions: int = 20
    long_momentum_sessions: int = 50
    trend_sessions: int = 50
    volatility_sessions: int = 20
    volume_sessions: int = 20
    breakout_sessions: int = 20
    contraction_sessions: int = 5
    horizon_sessions: int = 10
    trending_breadth_threshold: Decimal = Decimal("0.60")
    trending_momentum_threshold: Decimal = Decimal("0.02")
    risk_off_breadth_threshold: Decimal = Decimal("0.35")
    high_volatility_threshold: Decimal = Decimal("0.035")
    weightings: tuple[AlphaRegimeWeighting, ...] = field(
        default_factory=_default_weightings
    )
    policy_version: str = "regime-aware-alpha-ensemble/v1"
    config_id: str = field(init=False)

    def __post_init__(self) -> None:
        integer_fields = (
            "minimum_history_sessions",
            "short_momentum_sessions",
            "long_momentum_sessions",
            "trend_sessions",
            "volatility_sessions",
            "volume_sessions",
            "breakout_sessions",
            "contraction_sessions",
            "horizon_sessions",
        )
        for name in integer_fields:
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise RegimeEnsembleError(f"{name} must be a positive integer")
        required = max(
            self.short_momentum_sessions + 1,
            self.long_momentum_sessions + 1,
            self.trend_sessions,
            self.volatility_sessions + 1,
            self.volume_sessions + 1,
            self.breakout_sessions + 1,
            self.contraction_sessions,
        )
        if self.minimum_history_sessions < required:
            raise RegimeEnsembleError(
                "minimum history cannot be shorter than a feature lookback"
            )
        for name in (
            "trending_breadth_threshold",
            "risk_off_breadth_threshold",
        ):
            _unit(getattr(self, name), name)
        _decimal(self.trending_momentum_threshold, "trending_momentum_threshold")
        _decimal(
            self.high_volatility_threshold,
            "high_volatility_threshold",
            positive=True,
        )
        if self.risk_off_breadth_threshold >= self.trending_breadth_threshold:
            raise RegimeEnsembleError(
                "risk-off breadth must be below trending breadth"
            )
        if (
            type(self.weightings) is not tuple
            or any(type(value) is not AlphaRegimeWeighting for value in self.weightings)
            or self.weightings
            != tuple(sorted(self.weightings, key=lambda value: value.regime.value))
            or {value.regime for value in self.weightings} != set(MarketRegime)
        ):
            raise RegimeEnsembleError(
                "config requires one exact weighting for every market regime"
            )
        for value in self.weightings:
            value.verify_content_identity()
        if self.policy_version != "regime-aware-alpha-ensemble/v1":
            raise RegimeEnsembleError("unsupported ensemble policy")
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
        for value in self.weightings:
            value.verify_content_identity()
        if self.config_id != self._calculated_id():
            raise RegimeEnsembleError("ensemble config content identity failed")

    def weighting_for(self, regime: MarketRegime) -> AlphaRegimeWeighting:
        matches = tuple(value for value in self.weightings if value.regime is regime)
        if len(matches) != 1:
            raise RegimeEnsembleError("market regime weighting is unavailable")
        return matches[0]


@dataclass(frozen=True, slots=True)
class AlphaInstrumentMetrics:
    history_id: str
    instrument_id: str
    listing_id: str
    short_momentum: Decimal
    long_momentum: Decimal
    trend_distance: Decimal
    breakout_proximity: Decimal
    pullback_depth: Decimal
    volume_ratio: Decimal
    realized_volatility: Decimal
    atr_fraction: Decimal
    contraction: Decimal
    median_traded_value: Decimal
    metrics_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.history_id, "history_id"),
            (self.instrument_id, "instrument_id"),
            (self.listing_id, "listing_id"),
        ):
            if type(value) is not str or not value:
                raise RegimeEnsembleError(f"metrics {name} is required")
        for name in (
            "short_momentum",
            "long_momentum",
            "trend_distance",
            "pullback_depth",
            "volume_ratio",
            "realized_volatility",
            "atr_fraction",
            "median_traded_value",
        ):
            _decimal(getattr(self, name), f"metrics.{name}")
        for name in ("breakout_proximity", "contraction"):
            _unit(getattr(self, name), f"metrics.{name}")
        if (
            self.realized_volatility < ZERO
            or self.atr_fraction <= ZERO
            or self.median_traded_value <= ZERO
            or self.volume_ratio <= ZERO
        ):
            raise RegimeEnsembleError("metrics contain a non-positive market measure")
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
            raise RegimeEnsembleError("alpha metrics content identity failed")


@dataclass(frozen=True, slots=True)
class AlphaSpecialistScore:
    specialist: AlphaSpecialist
    raw_score: Decimal
    regime_weight: Decimal
    weighted_score: Decimal
    rationale: str
    score_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.specialist) is not AlphaSpecialist:
            raise RegimeEnsembleError("alpha specialist must be exact")
        _unit(self.raw_score, "specialist raw_score")
        _unit(self.regime_weight, "specialist regime_weight")
        if self.weighted_score != self.raw_score * self.regime_weight:
            raise RegimeEnsembleError("specialist weighted score differs")
        if type(self.rationale) is not str or not self.rationale.strip():
            raise RegimeEnsembleError("specialist rationale is required")
        object.__setattr__(self, "score_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "score_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.score_id != self._calculated_id():
            raise RegimeEnsembleError("specialist score content identity failed")


@dataclass(frozen=True, slots=True)
class RegimeEnsembleAssessment:
    config_id: str
    data_snapshot_id: str
    data_snapshot_fingerprint: str
    history_id: str
    instrument_id: str
    listing_id: str
    as_of: datetime
    regime: MarketRegime
    market_breadth: Decimal
    market_median_momentum: Decimal
    market_median_volatility: Decimal
    metrics: AlphaInstrumentMetrics
    specialist_scores: tuple[AlphaSpecialistScore, ...]
    ensemble_score: Decimal
    median_return_pct: Decimal
    downside_return_pct: Decimal
    uncertainty: Decimal
    schema_version: str = "regime-ensemble-assessment/v1"
    assessment_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.config_id, "config_id"),
            (self.data_snapshot_id, "data_snapshot_id"),
            (self.data_snapshot_fingerprint, "data_snapshot_fingerprint"),
            (self.history_id, "history_id"),
            (self.instrument_id, "instrument_id"),
            (self.listing_id, "listing_id"),
        ):
            if type(value) is not str or not value:
                raise RegimeEnsembleError(f"assessment {name} is required")
        object.__setattr__(self, "as_of", _utc(self.as_of, "assessment as_of"))
        if type(self.regime) is not MarketRegime:
            raise RegimeEnsembleError("assessment regime must be exact")
        _unit(self.market_breadth, "assessment market_breadth")
        _decimal(self.market_median_momentum, "assessment market_median_momentum")
        _decimal(self.market_median_volatility, "assessment market_median_volatility")
        if type(self.metrics) is not AlphaInstrumentMetrics:
            raise RegimeEnsembleError("assessment metrics must be exact")
        self.metrics.verify_content_identity()
        if (
            self.metrics.history_id != self.history_id
            or self.metrics.instrument_id != self.instrument_id
            or self.metrics.listing_id != self.listing_id
        ):
            raise RegimeEnsembleError("assessment metrics lineage differs")
        expected_order = tuple(sorted(AlphaSpecialist, key=lambda value: value.value))
        if (
            type(self.specialist_scores) is not tuple
            or any(type(value) is not AlphaSpecialistScore for value in self.specialist_scores)
            or tuple(value.specialist for value in self.specialist_scores) != expected_order
        ):
            raise RegimeEnsembleError(
                "assessment requires one ordered score for every specialist"
            )
        for value in self.specialist_scores:
            value.verify_content_identity()
        if self.ensemble_score != sum(
            (value.weighted_score for value in self.specialist_scores), ZERO
        ):
            raise RegimeEnsembleError("ensemble score differs from specialist sum")
        _unit(self.ensemble_score, "assessment ensemble_score")
        _decimal(self.median_return_pct, "assessment median_return_pct")
        _decimal(self.downside_return_pct, "assessment downside_return_pct")
        if self.downside_return_pct >= ZERO:
            raise RegimeEnsembleError("assessment downside return must be negative")
        _unit(self.uncertainty, "assessment uncertainty")
        if self.schema_version != "regime-ensemble-assessment/v1":
            raise RegimeEnsembleError("unsupported assessment schema")
        object.__setattr__(self, "assessment_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "assessment_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self.metrics.verify_content_identity()
        for value in self.specialist_scores:
            value.verify_content_identity()
        if self.assessment_id != self._calculated_id():
            raise RegimeEnsembleError("ensemble assessment content identity failed")


def _true_range(bar: AsOfSwingBar, previous: AsOfSwingBar) -> Decimal:
    return max(
        bar.high - bar.low,
        abs(bar.high - previous.close),
        abs(bar.low - previous.close),
    )


def calculate_alpha_instrument_metrics(
    history: InstrumentSwingHistory,
    config: RegimeEnsembleConfig,
) -> AlphaInstrumentMetrics:
    if type(history) is not InstrumentSwingHistory:
        raise RegimeEnsembleError("history must be exact")
    if type(config) is not RegimeEnsembleConfig:
        raise RegimeEnsembleError("ensemble config must be exact")
    try:
        history.verify_content_identity()
        config.verify_content_identity()
    except Exception:
        raise RegimeEnsembleError("alpha metric input identity failed") from None
    if len(history.bars) < config.minimum_history_sessions:
        raise RegimeEnsembleError("history is shorter than the configured minimum")
    bars = history.bars
    current = bars[-1]
    short_momentum = (
        current.close / bars[-(config.short_momentum_sessions + 1)].close - ONE
    )
    long_momentum = (
        current.close / bars[-(config.long_momentum_sessions + 1)].close - ONE
    )
    trend_average = _mean(
        tuple(value.close for value in bars[-config.trend_sessions :])
    )
    trend_distance = current.close / trend_average - ONE
    prior_high = max(
        value.high for value in bars[-(config.breakout_sessions + 1) : -1]
    )
    breakout_proximity = _clamp(
        (current.close / prior_high - Decimal("0.90")) / Decimal("0.10")
    )
    pullback_depth = ONE - current.close / prior_high
    previous_volumes = tuple(
        value.volume for value in bars[-(config.volume_sessions + 1) : -1]
    )
    volume_ratio = current.volume / _median(previous_volumes)
    volatility_bars = bars[-(config.volatility_sessions + 1) :]
    returns = tuple(
        volatility_bars[index].close / volatility_bars[index - 1].close - ONE
        for index in range(1, len(volatility_bars))
    )
    realized_volatility = _mean(tuple(value * value for value in returns)).sqrt()
    true_ranges = tuple(
        _true_range(volatility_bars[index], volatility_bars[index - 1])
        for index in range(1, len(volatility_bars))
    )
    atr_fraction = _mean(true_ranges) / current.close
    range_fractions = tuple(
        (value.high - value.low) / value.close
        for value in bars[-config.volatility_sessions :]
    )
    baseline_range = _mean(range_fractions)
    if baseline_range <= ZERO:
        raise RegimeEnsembleError("history has no positive intraday range")
    recent_range = _mean(range_fractions[-config.contraction_sessions :])
    contraction = _clamp(ONE - recent_range / baseline_range)
    median_traded_value = _median(
        tuple(value.traded_value for value in bars[-config.volume_sessions :])
    )
    return AlphaInstrumentMetrics(
        history_id=history.history_id,
        instrument_id=history.instrument_id,
        listing_id=history.listing_id,
        short_momentum=short_momentum,
        long_momentum=long_momentum,
        trend_distance=trend_distance,
        breakout_proximity=breakout_proximity,
        pullback_depth=pullback_depth,
        volume_ratio=volume_ratio,
        realized_volatility=realized_volatility,
        atr_fraction=atr_fraction,
        contraction=contraction,
        median_traded_value=median_traded_value,
    )


def _percentile_ranks(
    values: tuple[tuple[str, Decimal], ...],
    *,
    higher_is_better: bool,
) -> dict[str, Decimal]:
    if not values:
        raise RegimeEnsembleError("rank calculation requires values")
    unique = tuple(sorted({value for _, value in values}))
    if len(unique) == 1:
        raw = {unique[0]: Decimal("0.5")}
    else:
        raw = {
            value: Decimal(index) / Decimal(len(unique) - 1)
            for index, value in enumerate(unique)
        }
    if not higher_is_better:
        raw = {value: ONE - rank for value, rank in raw.items()}
    return {instrument_id: raw[value] for instrument_id, value in values}


def _market_regime(
    metrics: tuple[AlphaInstrumentMetrics, ...],
    config: RegimeEnsembleConfig,
) -> tuple[MarketRegime, Decimal, Decimal, Decimal]:
    breadth = Decimal(sum(value.trend_distance > ZERO for value in metrics)) / Decimal(
        len(metrics)
    )
    median_momentum = _median(tuple(value.short_momentum for value in metrics))
    median_volatility = _median(tuple(value.realized_volatility for value in metrics))
    if median_volatility >= config.high_volatility_threshold:
        regime = MarketRegime.HIGH_VOLATILITY
    elif (
        breadth <= config.risk_off_breadth_threshold
        and median_momentum <= ZERO
    ):
        regime = MarketRegime.RISK_OFF
    elif (
        breadth >= config.trending_breadth_threshold
        and median_momentum >= config.trending_momentum_threshold
    ):
        regime = MarketRegime.TRENDING
    else:
        regime = MarketRegime.RANGE_BOUND
    return regime, breadth, median_momentum, median_volatility


def _pullback_quality(metrics: AlphaInstrumentMetrics) -> Decimal:
    if metrics.trend_distance <= ZERO or metrics.short_momentum <= ZERO:
        return ZERO
    ideal_depth = Decimal("0.04")
    tolerance = Decimal("0.08")
    return _clamp(ONE - abs(metrics.pullback_depth - ideal_depth) / tolerance)


@dataclass(frozen=True, slots=True)
class RegimeCrossSectionScore:
    """One instrument's regime-ensemble scoring facts, free of snapshot lineage.

    This is the same descriptive content as ``RegimeEnsembleAssessment`` minus
    the data-snapshot/as-of binding, so it can be produced by a pure kernel
    that never sees a ``DataSnapshot``, calendar, or evidence.
    """

    config_id: str
    history_id: str
    instrument_id: str
    listing_id: str
    regime: MarketRegime
    market_breadth: Decimal
    market_median_momentum: Decimal
    market_median_volatility: Decimal
    metrics: AlphaInstrumentMetrics
    specialist_scores: tuple[AlphaSpecialistScore, ...]
    ensemble_score: Decimal
    median_return_pct: Decimal
    downside_return_pct: Decimal
    uncertainty: Decimal
    schema_version: str = "regime-cross-section-score/v1"
    cross_section_score_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.config_id, "config_id"),
            (self.history_id, "history_id"),
            (self.instrument_id, "instrument_id"),
            (self.listing_id, "listing_id"),
        ):
            if type(value) is not str or not value:
                raise RegimeEnsembleError(f"cross-section score {name} is required")
        if type(self.regime) is not MarketRegime:
            raise RegimeEnsembleError("cross-section score regime must be exact")
        _unit(self.market_breadth, "cross-section score market_breadth")
        _decimal(self.market_median_momentum, "cross-section score market_median_momentum")
        _decimal(
            self.market_median_volatility, "cross-section score market_median_volatility"
        )
        if type(self.metrics) is not AlphaInstrumentMetrics:
            raise RegimeEnsembleError("cross-section score metrics must be exact")
        self.metrics.verify_content_identity()
        if (
            self.metrics.history_id != self.history_id
            or self.metrics.instrument_id != self.instrument_id
            or self.metrics.listing_id != self.listing_id
        ):
            raise RegimeEnsembleError("cross-section score metrics lineage differs")
        expected_order = tuple(sorted(AlphaSpecialist, key=lambda value: value.value))
        if (
            type(self.specialist_scores) is not tuple
            or any(type(value) is not AlphaSpecialistScore for value in self.specialist_scores)
            or tuple(value.specialist for value in self.specialist_scores) != expected_order
        ):
            raise RegimeEnsembleError(
                "cross-section score requires one ordered score for every specialist"
            )
        for value in self.specialist_scores:
            value.verify_content_identity()
        if self.ensemble_score != sum(
            (value.weighted_score for value in self.specialist_scores), ZERO
        ):
            raise RegimeEnsembleError("cross-section ensemble score differs from specialist sum")
        _unit(self.ensemble_score, "cross-section score ensemble_score")
        _decimal(self.median_return_pct, "cross-section score median_return_pct")
        _decimal(self.downside_return_pct, "cross-section score downside_return_pct")
        if self.downside_return_pct >= ZERO:
            raise RegimeEnsembleError("cross-section score downside return must be negative")
        _unit(self.uncertainty, "cross-section score uncertainty")
        if self.schema_version != "regime-cross-section-score/v1":
            raise RegimeEnsembleError("unsupported cross-section score schema")
        object.__setattr__(self, "cross_section_score_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "cross_section_score_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self.metrics.verify_content_identity()
        for value in self.specialist_scores:
            value.verify_content_identity()
        if self.cross_section_score_id != self._calculated_id():
            raise RegimeEnsembleError("cross-section score content identity failed")


@dataclass(frozen=True, slots=True)
class RegimeCrossSection:
    """One deterministic cross-sectional scoring pass over a set of histories."""

    config_id: str
    regime: MarketRegime
    market_breadth: Decimal
    market_median_momentum: Decimal
    market_median_volatility: Decimal
    scores: tuple[RegimeCrossSectionScore, ...]
    schema_version: str = "regime-cross-section/v1"
    cross_section_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.config_id) is not str or not self.config_id:
            raise RegimeEnsembleError("cross-section config_id is required")
        if type(self.regime) is not MarketRegime:
            raise RegimeEnsembleError("cross-section regime must be exact")
        _unit(self.market_breadth, "cross-section market_breadth")
        _decimal(self.market_median_momentum, "cross-section market_median_momentum")
        _decimal(self.market_median_volatility, "cross-section market_median_volatility")
        if (
            type(self.scores) is not tuple
            or not self.scores
            or any(type(value) is not RegimeCrossSectionScore for value in self.scores)
            or self.scores != tuple(sorted(self.scores, key=lambda value: value.instrument_id))
            or len({value.instrument_id for value in self.scores}) != len(self.scores)
        ):
            raise RegimeEnsembleError(
                "cross-section scores must be a non-empty unique instrument-ordered exact tuple"
            )
        for value in self.scores:
            value.verify_content_identity()
            if (
                value.config_id != self.config_id
                or value.regime is not self.regime
                or value.market_breadth != self.market_breadth
                or value.market_median_momentum != self.market_median_momentum
                or value.market_median_volatility != self.market_median_volatility
            ):
                raise RegimeEnsembleError(
                    "cross-section score binding differs from the cross-section"
                )
        if self.schema_version != "regime-cross-section/v1":
            raise RegimeEnsembleError("unsupported cross-section schema")
        object.__setattr__(self, "cross_section_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "cross_section_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        for value in self.scores:
            value.verify_content_identity()
        if self.cross_section_id != self._calculated_id():
            raise RegimeEnsembleError("cross-section content identity failed")

    def score_for(self, instrument_id: str) -> RegimeCrossSectionScore:
        matches = tuple(value for value in self.scores if value.instrument_id == instrument_id)
        if len(matches) != 1:
            raise RegimeEnsembleError("instrument cross-section score is unavailable")
        return matches[0]


def calculate_regime_cross_section(
    histories: tuple[InstrumentSwingHistory, ...],
    config: RegimeEnsembleConfig,
) -> RegimeCrossSection:
    """Pure, replay-verifiable cross-sectional regime-ensemble scoring kernel.

    This function contains the exact cross-sectional scoring math previously
    inlined in ``RegimeAwareForecastProvider.__init__``: regime detection,
    percentile ranking, per-instrument specialist scoring, ensemble scoring,
    score-implied return, and uncertainty. It never accepts or invents a
    clock, calendar, ``DataSnapshot``, ``EvidenceItem``, universe, readiness,
    or source provenance -- those remain the forecast provider's own
    responsibility, checked before this kernel is ever called.
    """

    if type(config) is not RegimeEnsembleConfig:
        raise RegimeEnsembleError("ensemble config must be exact")
    try:
        config.verify_content_identity()
    except Exception:
        raise RegimeEnsembleError("ensemble input identity failed") from None
    if (
        type(histories) is not tuple
        or not histories
        or any(type(value) is not InstrumentSwingHistory for value in histories)
        or histories != tuple(sorted(histories, key=lambda value: value.instrument_id))
        or len({value.instrument_id for value in histories}) != len(histories)
        or len({value.listing_id for value in histories}) != len(histories)
    ):
        raise RegimeEnsembleError(
            "histories must be a non-empty unique instrument/listing-ordered exact tuple"
        )
    session_tuple: tuple[object, ...] | None = None
    calculated: list[AlphaInstrumentMetrics] = []
    for history in histories:
        try:
            history.verify_content_identity()
        except Exception:
            raise RegimeEnsembleError("history content identity failed") from None
        sessions = tuple(value.market_session for value in history.bars)
        if session_tuple is None:
            session_tuple = sessions
        elif sessions != session_tuple:
            raise RegimeEnsembleError(
                "cross-sectional histories must have identical session coverage"
            )
        calculated.append(calculate_alpha_instrument_metrics(history, config))

    metrics = tuple(calculated)
    regime, breadth, median_momentum, median_volatility = _market_regime(metrics, config)
    short_ranks = _percentile_ranks(
        tuple((value.instrument_id, value.short_momentum) for value in metrics),
        higher_is_better=True,
    )
    long_ranks = _percentile_ranks(
        tuple((value.instrument_id, value.long_momentum) for value in metrics),
        higher_is_better=True,
    )
    liquidity_ranks = _percentile_ranks(
        tuple((value.instrument_id, value.median_traded_value) for value in metrics),
        higher_is_better=True,
    )
    low_volatility_ranks = _percentile_ranks(
        tuple((value.instrument_id, value.realized_volatility) for value in metrics),
        higher_is_better=False,
    )
    weighting = config.weighting_for(regime)
    scores: list[RegimeCrossSectionScore] = []
    for value in metrics:
        momentum = _clamp(
            Decimal("0.35") * short_ranks[value.instrument_id]
            + Decimal("0.25") * long_ranks[value.instrument_id]
            + Decimal("0.25") * value.breakout_proximity
            + Decimal("0.15") * _clamp(value.volume_ratio / Decimal("2"))
        )
        trend_gate = _clamp(value.trend_distance / Decimal("0.10"))
        pullback = _clamp(
            trend_gate
            * (
                Decimal("0.50") * _pullback_quality(value)
                + Decimal("0.25") * short_ranks[value.instrument_id]
                + Decimal("0.25") * _clamp(value.volume_ratio / Decimal("2"))
            )
        )
        contraction = _clamp(
            Decimal("0.40") * value.contraction
            + Decimal("0.25") * value.breakout_proximity
            + Decimal("0.20") * low_volatility_ranks[value.instrument_id]
            + Decimal("0.15") * _clamp(value.volume_ratio / Decimal("2"))
        )
        liquidity = liquidity_ranks[value.instrument_id]
        raw = {
            AlphaSpecialist.MOMENTUM_BREAKOUT: momentum,
            AlphaSpecialist.PULLBACK_CONTINUATION: pullback,
            AlphaSpecialist.VOLATILITY_CONTRACTION: contraction,
            AlphaSpecialist.LIQUIDITY_QUALITY: liquidity,
        }
        rationales = {
            AlphaSpecialist.MOMENTUM_BREAKOUT: (
                f"short rank {short_ranks[value.instrument_id]}; long rank "
                f"{long_ranks[value.instrument_id]}; breakout proximity "
                f"{value.breakout_proximity}; volume ratio {value.volume_ratio}"
            ),
            AlphaSpecialist.PULLBACK_CONTINUATION: (
                f"trend distance {value.trend_distance}; pullback depth "
                f"{value.pullback_depth}; volume ratio {value.volume_ratio}"
            ),
            AlphaSpecialist.VOLATILITY_CONTRACTION: (
                f"contraction {value.contraction}; realized volatility "
                f"{value.realized_volatility}; breakout proximity "
                f"{value.breakout_proximity}"
            ),
            AlphaSpecialist.LIQUIDITY_QUALITY: (
                f"cross-sectional traded-value rank {liquidity}"
            ),
        }
        specialist_scores = tuple(
            AlphaSpecialistScore(
                specialist=specialist,
                raw_score=raw[specialist],
                regime_weight=weighting.weight_for(specialist),
                weighted_score=(raw[specialist] * weighting.weight_for(specialist)),
                rationale=rationales[specialist],
            )
            for specialist in sorted(AlphaSpecialist, key=lambda item: item.value)
        )
        ensemble_score = sum((score.weighted_score for score in specialist_scores), ZERO)
        regime_multiplier = (
            Decimal("0.50")
            if regime is MarketRegime.RISK_OFF
            else Decimal("0.75")
            if regime is MarketRegime.HIGH_VOLATILITY
            else ONE
        )
        momentum_anchor = _clamp(
            value.short_momentum,
            Decimal("-0.10"),
            Decimal("0.10"),
        ) * Decimal("0.25")
        implied_fraction = (
            (ensemble_score - Decimal("0.50")) * Decimal("0.10") + momentum_anchor
        ) * regime_multiplier
        median_return_pct = _clamp(
            implied_fraction * HUNDRED,
            Decimal("-15"),
            Decimal("15"),
        )
        downside_fraction = max(
            Decimal("0.02"),
            value.atr_fraction * Decimal(config.horizon_sessions).sqrt() * Decimal("1.50"),
        )
        downside_return_pct = -min(Decimal("25"), downside_fraction * HUNDRED)
        dispersion = _mean(
            tuple(abs(score.raw_score - ensemble_score) for score in specialist_scores)
        )
        regime_penalty = (
            Decimal("0.15")
            if regime is MarketRegime.RISK_OFF
            else Decimal("0.10")
            if regime is MarketRegime.HIGH_VOLATILITY
            else ZERO
        )
        uncertainty = _clamp(
            Decimal("0.35")
            + Decimal("0.35") * dispersion
            + Decimal("0.20") * _clamp(value.realized_volatility / Decimal("0.05"))
            + regime_penalty
        )
        scores.append(
            RegimeCrossSectionScore(
                config_id=config.config_id,
                history_id=value.history_id,
                instrument_id=value.instrument_id,
                listing_id=value.listing_id,
                regime=regime,
                market_breadth=breadth,
                market_median_momentum=median_momentum,
                market_median_volatility=median_volatility,
                metrics=value,
                specialist_scores=specialist_scores,
                ensemble_score=ensemble_score,
                median_return_pct=median_return_pct,
                downside_return_pct=downside_return_pct,
                uncertainty=uncertainty,
            )
        )
    return RegimeCrossSection(
        config_id=config.config_id,
        regime=regime,
        market_breadth=breadth,
        market_median_momentum=median_momentum,
        market_median_volatility=median_volatility,
        scores=tuple(scores),
    )


class RegimeAwareForecastProvider:
    """Deterministic multi-specialist forecast challenger.

    Output return values are score-implied research forecasts, not calibrated
    probabilities. The downstream signal provider therefore remains provisional
    until the existing walk-forward calibration contract is satisfied.
    """

    def __init__(
        self,
        *,
        snapshot: DataSnapshot,
        histories: tuple[InstrumentSwingHistory, ...],
        calendar: CalendarSnapshot,
        config: RegimeEnsembleConfig | None = None,
    ) -> None:
        if type(snapshot) is not DataSnapshot:
            raise RegimeEnsembleError("snapshot must be exact")
        if type(calendar) is not CalendarSnapshot:
            raise RegimeEnsembleError("calendar must be exact")
        if config is None:
            config = RegimeEnsembleConfig()
        if type(config) is not RegimeEnsembleConfig:
            raise RegimeEnsembleError("ensemble config must be exact")
        if (
            type(histories) is not tuple
            or not histories
            or any(type(value) is not InstrumentSwingHistory for value in histories)
            or histories != tuple(sorted(histories, key=lambda value: value.instrument_id))
            or len({value.instrument_id for value in histories}) != len(histories)
        ):
            raise RegimeEnsembleError(
                "histories must be a non-empty unique instrument-ordered exact tuple"
            )
        try:
            snapshot.verify_content_identity()
            calendar.verify_content_identity()
            config.verify_content_identity()
        except Exception:
            raise RegimeEnsembleError("ensemble input identity failed") from None
        if snapshot.calendar_version != calendar.version:
            raise RegimeEnsembleError("snapshot and calendar versions differ")
        decision_time = _utc(snapshot.decision_time, "snapshot decision_time")
        if calendar.cutoff > decision_time:
            raise RegimeEnsembleError("calendar is future-known")
        evidence = {value.evidence_id: value for value in snapshot.evidence}
        if len(evidence) != len(snapshot.evidence):
            raise RegimeEnsembleError("snapshot evidence IDs must be unique")
        session_tuple: tuple[object, ...] | None = None
        for history in histories:
            try:
                history.verify_content_identity()
            except Exception:
                raise RegimeEnsembleError("history content identity failed") from None
            sessions = tuple(value.market_session for value in history.bars)
            if session_tuple is None:
                session_tuple = sessions
            elif sessions != session_tuple:
                raise RegimeEnsembleError(
                    "cross-sectional histories must have identical session coverage"
                )
            if history.bars[-1].market_session != snapshot.market_session:
                raise RegimeEnsembleError("history does not end at the signal session")
            if len(history.bars) < config.minimum_history_sessions:
                raise RegimeEnsembleError("history is shorter than the configured minimum")
            for bar in history.bars:
                calendar.require_session(bar.market_session)
                if bar.market_session > snapshot.market_session or bar.available_at > decision_time:
                    raise RegimeEnsembleError("history contains future-known evidence")
                item = evidence.get(bar.evidence_id)
                if (
                    item is None
                    or item.content_hash != bar.content_hash
                    or _utc(item.available_at, "bar evidence available_at") != bar.available_at
                ):
                    raise RegimeEnsembleError("history evidence binding differs")
            for evidence_id, content_hash, available_at, name in (
                (
                    history.tick_evidence_id,
                    history.tick_content_hash,
                    history.tick_available_at,
                    "tick",
                ),
                (
                    history.adjustment_evidence_id,
                    history.adjustment_content_hash,
                    history.adjustment_available_at,
                    "adjustment",
                ),
            ):
                item = evidence.get(evidence_id)
                if (
                    item is None
                    or item.content_hash != content_hash
                    or _utc(item.available_at, f"{name} evidence available_at") != available_at
                ):
                    raise RegimeEnsembleError(f"{name} evidence binding differs")
                if available_at > decision_time:
                    raise RegimeEnsembleError(f"{name} evidence is future-known")
        try:
            validate_snapshot(snapshot)
        except Exception:
            raise RegimeEnsembleError("snapshot contains unavailable evidence") from None

        cross_section = calculate_regime_cross_section(histories, config)
        assessments: dict[str, RegimeEnsembleAssessment] = {}
        for score in cross_section.scores:
            assessments[score.instrument_id] = RegimeEnsembleAssessment(
                config_id=config.config_id,
                data_snapshot_id=snapshot.snapshot_id,
                data_snapshot_fingerprint=snapshot.content_fingerprint,
                history_id=score.history_id,
                instrument_id=score.instrument_id,
                listing_id=score.listing_id,
                as_of=snapshot.decision_time,
                regime=score.regime,
                market_breadth=score.market_breadth,
                market_median_momentum=score.market_median_momentum,
                market_median_volatility=score.market_median_volatility,
                metrics=score.metrics,
                specialist_scores=score.specialist_scores,
                ensemble_score=score.ensemble_score,
                median_return_pct=score.median_return_pct,
                downside_return_pct=score.downside_return_pct,
                uncertainty=score.uncertainty,
            )
        self.snapshot = snapshot
        self.histories = histories
        self.calendar = calendar
        self.config = config
        self.regime = cross_section.regime
        self._history_by_id = {value.instrument_id: value for value in histories}
        self._assessments = assessments
        self.model_version = f"{config.policy_version}:{config.config_id}"

    def identity_material(self) -> object:
        return {
            "model_version": self.model_version,
            "snapshot_fingerprint": self.snapshot.content_fingerprint,
            "calendar_snapshot_id": self.calendar.snapshot_id,
            "config_id": self.config.config_id,
            "history_ids": tuple(value.history_id for value in self.histories),
            "regime": self.regime,
        }

    def _verify_bound_inputs(self) -> None:
        try:
            self.snapshot.verify_content_identity()
            self.calendar.verify_content_identity()
            self.config.verify_content_identity()
            for history in self.histories:
                history.verify_content_identity()
            for assessment in self._assessments.values():
                assessment.verify_content_identity()
        except Exception:
            raise RegimeEnsembleError("bound ensemble content identity failed") from None

    def assessment_for(self, instrument_id: str) -> RegimeEnsembleAssessment:
        self._verify_bound_inputs()
        try:
            return self._assessments[instrument_id]
        except (KeyError, TypeError):
            raise RegimeEnsembleError("instrument assessment is unavailable") from None

    def forecast(
        self,
        instrument: InstrumentSnapshot,
        snapshot: DataSnapshot,
    ) -> ForecastSummary:
        self._verify_bound_inputs()
        if type(instrument) is not InstrumentSnapshot:
            raise RegimeEnsembleError("instrument must be exact")
        if type(snapshot) is not DataSnapshot:
            raise RegimeEnsembleError("runtime snapshot must be exact")
        if snapshot.content_fingerprint != self.snapshot.content_fingerprint:
            raise RegimeEnsembleError("forecast provider is bound to another snapshot")
        try:
            instrument.verify_content_identity()
        except Exception:
            raise RegimeEnsembleError("instrument content identity failed") from None
        history = self._history_by_id.get(instrument.instrument_id)
        assessment = self._assessments.get(instrument.instrument_id)
        if history is None or assessment is None:
            raise RegimeEnsembleError("instrument history is unavailable")
        if (
            instrument.listing_id != history.listing_id
            or instrument.universe_snapshot_id != snapshot.universe_snapshot_id
            or instrument.price_session != snapshot.market_session
            or instrument.last_price != history.bars[-1].close
            or _utc(instrument.data_available_at, "instrument data_available_at")
            != history.bars[-1].available_at
        ):
            raise RegimeEnsembleError("instrument differs from ensemble history")
        return ForecastSummary(
            symbol=instrument.symbol,
            as_of=snapshot.decision_time,
            horizon_sessions=self.config.horizon_sessions,
            median_return_pct=assessment.median_return_pct,
            downside_return_pct=assessment.downside_return_pct,
            uncertainty=assessment.uncertainty,
            sample_count=len(assessment.specialist_scores),
            model_version=self.model_version,
            instrument_id=instrument.instrument_id,
            listing_id=instrument.listing_id,
            universe_snapshot_id=instrument.universe_snapshot_id,
            data_snapshot_id=snapshot.snapshot_id,
            data_snapshot_fingerprint=snapshot.content_fingerprint,
            instrument_fingerprint=instrument.content_fingerprint,
        )
