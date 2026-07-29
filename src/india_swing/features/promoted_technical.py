"""Deterministic technical features over verified promoted history inputs.

This is a descriptive collection transformation.  It does not rank the
cross-section, estimate a probability, create a signal, or authorize a trade.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_EVEN, localcontext
from enum import Enum

from india_swing.evaluation.promoted_feature_inputs import (
    PromotedFeatureInputHistory,
    PromotedFeatureInputResult,
    PromotedFeatureInputStatus,
    VerifiedPromotedFeatureInputPanel,
)
from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness


class PromotedTechnicalFeatureError(ValueError):
    """Raised when the promoted technical-feature boundary fails closed."""


PROMOTED_TECHNICAL_FEATURE_SCHEMA_VERSION = "promoted-technical-feature-panel/v1"
PROMOTED_TECHNICAL_FEATURE_POLICY_VERSION = (
    "promoted-technical-feature/descriptive-point-in-time-v1"
)
PROMOTED_TECHNICAL_FEATURE_CONFIG_SCHEMA_VERSION = (
    "promoted-technical-feature-config/v1"
)

_ZERO = Decimal("0")
_ONE = Decimal("1")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_REASON = re.compile(r"[A-Z][A-Z0-9_]{2,127}\Z")

_ERR_TYPE = "promoted technical-feature type is invalid"
_ERR_INPUT = "promoted technical-feature source is invalid"
_ERR_VERIFY = "promoted technical-feature source could not be verified"
_ERR_CUTOFF = "promoted technical-feature cutoff is invalid"
_ERR_FUTURE = "promoted technical-feature contains future-known evidence"
_ERR_CONFIG = "promoted technical-feature configuration is invalid"
_ERR_GRAPH = "promoted technical-feature graph is invalid"
_ERR_DERIVED = "promoted technical-feature derived content is invalid"
_ERR_ID = "promoted technical-feature identifier is invalid"

_COMMON_REASONS = {
    "COLLECTION_ONLY_NO_DECISION_AUTHORITY",
    "NO_CROSS_SECTIONAL_RANKING_AUTHORITY",
    "NO_FORECAST_OR_PROBABILITY_AUTHORITY",
}
_STATUS_SPECIFIC_REASONS = {
    "FEATURE_VECTOR_COMPUTED_COLLECTION_ONLY": {
        "DESCRIPTIVE_FEATURE_VECTOR_COMPUTED",
    },
    "SOURCE_INPUT_BLOCKED": {
        "PROMOTED_FEATURE_INPUT_NOT_ASSEMBLED",
    },
    "INSUFFICIENT_HISTORY_BLOCKED": {
        "CONFIGURED_FEATURE_WARMUP_INCOMPLETE",
    },
    "DEGENERATE_INPUT_BLOCKED": {
        "FEATURE_DENOMINATOR_IS_ZERO",
    },
}


def _utc(value: datetime) -> datetime:
    if type(value) is not datetime:
        raise PromotedTechnicalFeatureError(_ERR_CUTOFF)
    try:
        offset = value.utcoffset()
    except Exception:
        raise PromotedTechnicalFeatureError(_ERR_CUTOFF) from None
    if value.tzinfo is None or offset is None:
        raise PromotedTechnicalFeatureError(_ERR_CUTOFF)
    return value.astimezone(timezone.utc)


def _finite(value: object) -> bool:
    return type(value) is Decimal and value.is_finite()


def _positive_integer(value: object) -> bool:
    return type(value) is int and value > 0


def _mean(values: tuple[Decimal, ...]) -> Decimal:
    if not values:
        raise PromotedTechnicalFeatureError(_ERR_GRAPH)
    return sum(values, _ZERO) / Decimal(len(values))


def _median(values: tuple[Decimal, ...]) -> Decimal:
    if not values:
        raise PromotedTechnicalFeatureError(_ERR_GRAPH)
    ordered = tuple(sorted(values))
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / Decimal("2")


def _reasons(status: "PromotedTechnicalFeatureStatus") -> tuple[str, ...]:
    result = tuple(
        sorted(_COMMON_REASONS | _STATUS_SPECIFIC_REASONS[status.value])
    )
    if any(type(value) is not str or _REASON.fullmatch(value) is None for value in result):
        raise PromotedTechnicalFeatureError(_ERR_GRAPH)
    return result


def _counts(values: tuple[str, ...]) -> tuple[tuple[str, int], ...]:
    totals: dict[str, int] = {}
    for value in values:
        totals[value] = totals.get(value, 0) + 1
    return tuple(sorted(totals.items()))


@dataclass(frozen=True, slots=True)
class PromotedTechnicalFeatureConfig:
    """Immutable lookbacks for descriptive daily technical features.

    Production defaults require 61 ordered sessions: a current observation
    plus 60 prior sessions for the longest return and drawdown windows.
    """

    minimum_history_sessions: int = 61
    short_return_sessions: int = 5
    medium_return_sessions: int = 20
    long_return_sessions: int = 60
    short_trend_sessions: int = 20
    long_trend_sessions: int = 50
    atr_sessions: int = 14
    volatility_sessions: int = 20
    liquidity_sessions: int = 20
    breakout_sessions: int = 20
    drawdown_sessions: int = 60
    contraction_short_sessions: int = 5
    contraction_long_sessions: int = 20
    tick_history_sessions: int = 60
    annualization_sessions: int = 252
    schema_version: str = PROMOTED_TECHNICAL_FEATURE_CONFIG_SCHEMA_VERSION
    policy_version: str = PROMOTED_TECHNICAL_FEATURE_POLICY_VERSION
    config_id: str = field(init=False)

    def __post_init__(self) -> None:
        integer_names = (
            "minimum_history_sessions",
            "short_return_sessions",
            "medium_return_sessions",
            "long_return_sessions",
            "short_trend_sessions",
            "long_trend_sessions",
            "atr_sessions",
            "volatility_sessions",
            "liquidity_sessions",
            "breakout_sessions",
            "drawdown_sessions",
            "contraction_short_sessions",
            "contraction_long_sessions",
            "tick_history_sessions",
            "annualization_sessions",
        )
        if any(not _positive_integer(getattr(self, name)) for name in integer_names):
            raise PromotedTechnicalFeatureError(_ERR_CONFIG)
        if (
            type(self.schema_version) is not str
            or self.schema_version
            != PROMOTED_TECHNICAL_FEATURE_CONFIG_SCHEMA_VERSION
            or type(self.policy_version) is not str
            or self.policy_version != PROMOTED_TECHNICAL_FEATURE_POLICY_VERSION
            or self.short_trend_sessions < 2
            or not (
                self.short_return_sessions
                <= self.medium_return_sessions
                <= self.long_return_sessions
            )
            or self.short_trend_sessions > self.long_trend_sessions
            or self.contraction_short_sessions
            > self.contraction_long_sessions
            or self.minimum_history_sessions < self.required_history_sessions
        ):
            raise PromotedTechnicalFeatureError(_ERR_CONFIG)
        object.__setattr__(self, "config_id", self._calculated_id())

    @property
    def required_history_sessions(self) -> int:
        return max(
            self.short_return_sessions + 1,
            self.medium_return_sessions + 1,
            self.long_return_sessions + 1,
            self.short_trend_sessions,
            self.long_trend_sessions,
            self.atr_sessions + 1,
            self.volatility_sessions + 1,
            self.liquidity_sessions + 1,
            self.breakout_sessions + 1,
            self.drawdown_sessions,
            self.contraction_short_sessions + 1,
            self.contraction_long_sessions + 1,
            self.tick_history_sessions,
        )

    def _identity(self) -> dict[str, object]:
        return {
            value.name: getattr(self, value.name)
            for value in fields(self)
            if value.name != "config_id"
        }

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": PROMOTED_TECHNICAL_FEATURE_CONFIG_SCHEMA_VERSION,
                **self._identity(),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        expected = PromotedTechnicalFeatureConfig(**self._identity())
        if self.config_id != expected.config_id:
            raise PromotedTechnicalFeatureError(_ERR_ID)


class PromotedTechnicalFeatureStatus(str, Enum):
    FEATURE_VECTOR_COMPUTED_COLLECTION_ONLY = (
        "FEATURE_VECTOR_COMPUTED_COLLECTION_ONLY"
    )
    SOURCE_INPUT_BLOCKED = "SOURCE_INPUT_BLOCKED"
    INSUFFICIENT_HISTORY_BLOCKED = "INSUFFICIENT_HISTORY_BLOCKED"
    DEGENERATE_INPUT_BLOCKED = "DEGENERATE_INPUT_BLOCKED"


@dataclass(frozen=True, slots=True)
class PromotedTechnicalFeatureVector:
    source_history_id: str
    config_id: str
    stable_instrument_id: str
    stable_listing_id: str
    signal_session: date
    cutoff: datetime
    knowledge_time: datetime
    input_bar_ids: tuple[str, ...]
    return_short: Decimal
    return_medium: Decimal
    return_long: Decimal
    simple_moving_average_short: Decimal
    simple_moving_average_long: Decimal
    distance_from_short_average: Decimal
    distance_from_long_average: Decimal
    positive_close_fraction_short: Decimal
    average_true_range: Decimal
    average_true_range_fraction: Decimal
    annualized_realized_volatility: Decimal
    prior_breakout_high: Decimal
    prior_breakout_low: Decimal
    breakout_distance: Decimal
    range_position: Decimal
    maximum_drawdown: Decimal
    signal_gap_return: Decimal
    median_prior_volume: Decimal
    signal_volume_ratio: Decimal
    median_prior_traded_value: Decimal
    signal_traded_value_ratio: Decimal
    zero_volume_fraction: Decimal
    range_contraction_ratio: Decimal
    signal_tick_size: Decimal
    signal_tick_fraction: Decimal
    average_true_range_in_ticks: Decimal
    tick_change_count: int
    feature_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            not (
                type(self.source_history_id) is str
                and _SHA256.fullmatch(self.source_history_id)
            )
            or not (
                type(self.config_id) is str and _SHA256.fullmatch(self.config_id)
            )
            or not (
                type(self.stable_instrument_id) is str
                and _SHA256.fullmatch(self.stable_instrument_id)
            )
            or not (
                type(self.stable_listing_id) is str
                and _SHA256.fullmatch(self.stable_listing_id)
            )
            or type(self.signal_session) is not date
            or type(self.input_bar_ids) is not tuple
            or not self.input_bar_ids
            or any(
                type(value) is not str or _SHA256.fullmatch(value) is None
                for value in self.input_bar_ids
            )
            or len(set(self.input_bar_ids)) != len(self.input_bar_ids)
            or type(self.tick_change_count) is not int
            or self.tick_change_count < 0
        ):
            raise PromotedTechnicalFeatureError(_ERR_GRAPH)
        cutoff = _utc(self.cutoff)
        knowledge_time = _utc(self.knowledge_time)
        if knowledge_time > cutoff:
            raise PromotedTechnicalFeatureError(_ERR_FUTURE)
        if cutoff != self.cutoff:
            object.__setattr__(self, "cutoff", cutoff)
        if knowledge_time != self.knowledge_time:
            object.__setattr__(self, "knowledge_time", knowledge_time)
        decimal_names = tuple(
            value.name
            for value in fields(self)
            if value.name
            not in {
                "source_history_id",
                "config_id",
                "stable_instrument_id",
                "stable_listing_id",
                "signal_session",
                "cutoff",
                "knowledge_time",
                "input_bar_ids",
                "tick_change_count",
                "feature_id",
            }
        )
        if any(not _finite(getattr(self, name)) for name in decimal_names):
            raise PromotedTechnicalFeatureError(_ERR_GRAPH)
        if (
            self.simple_moving_average_short <= _ZERO
            or self.simple_moving_average_long <= _ZERO
            or not _ZERO <= self.positive_close_fraction_short <= _ONE
            or self.average_true_range <= _ZERO
            or self.average_true_range_fraction <= _ZERO
            or self.annualized_realized_volatility < _ZERO
            or self.prior_breakout_high <= _ZERO
            or self.prior_breakout_low <= _ZERO
            or self.prior_breakout_high < self.prior_breakout_low
            or self.maximum_drawdown > _ZERO
            or self.median_prior_volume <= _ZERO
            or self.signal_volume_ratio < _ZERO
            or self.median_prior_traded_value <= _ZERO
            or self.signal_traded_value_ratio < _ZERO
            or not _ZERO <= self.zero_volume_fraction <= _ONE
            or self.range_contraction_ratio <= _ZERO
            or self.signal_tick_size <= _ZERO
            or self.signal_tick_fraction <= _ZERO
            or self.average_true_range_in_ticks <= _ZERO
            or self.tick_change_count > len(self.input_bar_ids) - 1
        ):
            raise PromotedTechnicalFeatureError(_ERR_GRAPH)
        object.__setattr__(self, "feature_id", self._calculated_id())

    def _identity(self) -> dict[str, object]:
        return {
            value.name: getattr(self, value.name)
            for value in fields(self)
            if value.name != "feature_id"
        }

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "promoted-technical-feature-vector/v1",
                **self._identity(),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        expected = PromotedTechnicalFeatureVector(**self._identity())
        if self.feature_id != expected.feature_id:
            raise PromotedTechnicalFeatureError(_ERR_ID)


class _DegenerateInput(Exception):
    pass


def _true_ranges(history: PromotedFeatureInputHistory) -> tuple[Decimal, ...]:
    result: list[Decimal] = []
    for index in range(1, len(history.bars)):
        current = history.bars[index]
        previous = history.bars[index - 1]
        result.append(
            max(
                current.adjusted_high - current.adjusted_low,
                abs(current.adjusted_high - previous.adjusted_close),
                abs(current.adjusted_low - previous.adjusted_close),
            )
        )
    return tuple(result)


def _returns(history: PromotedFeatureInputHistory) -> tuple[Decimal, ...]:
    return tuple(
        history.bars[index].adjusted_close
        / history.bars[index - 1].adjusted_close
        - _ONE
        for index in range(1, len(history.bars))
    )


def _maximum_drawdown(closes: tuple[Decimal, ...]) -> Decimal:
    peak = closes[0]
    worst = _ZERO
    for close in closes:
        peak = max(peak, close)
        worst = min(worst, close / peak - _ONE)
    return worst


def _compute_vector_exact(
    history: PromotedFeatureInputHistory,
    config: PromotedTechnicalFeatureConfig,
    cutoff: datetime,
) -> PromotedTechnicalFeatureVector:
    bars = history.bars
    current = bars[-1]
    closes = tuple(value.adjusted_close for value in bars)
    volumes = tuple(value.adjusted_volume for value in bars)
    traded_values = tuple(
        value.adjusted_close * value.adjusted_volume for value in bars
    )
    true_ranges = _true_ranges(history)
    daily_returns = _returns(history)

    short_average = _mean(closes[-config.short_trend_sessions :])
    long_average = _mean(closes[-config.long_trend_sessions :])
    short_closes = closes[-config.short_trend_sessions :]
    positive_fraction = Decimal(
        sum(
            short_closes[index] > short_closes[index - 1]
            for index in range(1, len(short_closes))
        )
    ) / Decimal(len(short_closes) - 1)

    atr = _mean(true_ranges[-config.atr_sessions :])
    volatility_returns = daily_returns[-config.volatility_sessions :]
    volatility_mean = _mean(volatility_returns)
    variance = _mean(
        tuple((value - volatility_mean) ** 2 for value in volatility_returns)
    )
    annualized_volatility = (
        variance.sqrt() * Decimal(config.annualization_sessions).sqrt()
    )

    prior_breakout_bars = bars[-(config.breakout_sessions + 1) : -1]
    prior_high = max(value.adjusted_high for value in prior_breakout_bars)
    prior_low = min(value.adjusted_low for value in prior_breakout_bars)
    prior_range = prior_high - prior_low

    prior_volumes = volumes[-(config.liquidity_sessions + 1) : -1]
    prior_traded_values = traded_values[
        -(config.liquidity_sessions + 1) : -1
    ]
    median_volume = _median(prior_volumes)
    median_traded_value = _median(prior_traded_values)
    long_true_range = _mean(
        true_ranges[-config.contraction_long_sessions :]
    )
    short_true_range = _mean(
        true_ranges[-config.contraction_short_sessions :]
    )
    signal_tick = current.tick_size
    if (
        len(short_closes) < 2
        or atr <= _ZERO
        or prior_range <= _ZERO
        or median_volume <= _ZERO
        or median_traded_value <= _ZERO
        or long_true_range <= _ZERO
        or current.adjusted_close <= _ZERO
        or signal_tick <= _ZERO
    ):
        raise _DegenerateInput

    drawdown_closes = closes[-config.drawdown_sessions :]
    tick_window = bars[-config.tick_history_sessions :]
    tick_change_count = sum(
        tick_window[index].tick_size != tick_window[index - 1].tick_size
        for index in range(1, len(tick_window))
    )
    zero_volume_fraction = Decimal(
        sum(value == _ZERO for value in prior_volumes)
    ) / Decimal(len(prior_volumes))
    vector = PromotedTechnicalFeatureVector(
        source_history_id=history.history_id,
        config_id=config.config_id,
        stable_instrument_id=history.stable_instrument_id,
        stable_listing_id=history.stable_listing_id,
        signal_session=history.signal_session,
        cutoff=cutoff,
        knowledge_time=max(value.knowledge_time for value in bars),
        input_bar_ids=tuple(value.input_bar_id for value in bars),
        return_short=(
            current.adjusted_close
            / bars[-(config.short_return_sessions + 1)].adjusted_close
            - _ONE
        ),
        return_medium=(
            current.adjusted_close
            / bars[-(config.medium_return_sessions + 1)].adjusted_close
            - _ONE
        ),
        return_long=(
            current.adjusted_close
            / bars[-(config.long_return_sessions + 1)].adjusted_close
            - _ONE
        ),
        simple_moving_average_short=short_average,
        simple_moving_average_long=long_average,
        distance_from_short_average=(
            current.adjusted_close / short_average - _ONE
        ),
        distance_from_long_average=(
            current.adjusted_close / long_average - _ONE
        ),
        positive_close_fraction_short=positive_fraction,
        average_true_range=atr,
        average_true_range_fraction=atr / current.adjusted_close,
        annualized_realized_volatility=annualized_volatility,
        prior_breakout_high=prior_high,
        prior_breakout_low=prior_low,
        breakout_distance=current.adjusted_close / prior_high - _ONE,
        range_position=(current.adjusted_close - prior_low) / prior_range,
        maximum_drawdown=_maximum_drawdown(drawdown_closes),
        signal_gap_return=(
            current.adjusted_open / bars[-2].adjusted_close - _ONE
        ),
        median_prior_volume=median_volume,
        signal_volume_ratio=current.adjusted_volume / median_volume,
        median_prior_traded_value=median_traded_value,
        signal_traded_value_ratio=(
            traded_values[-1] / median_traded_value
        ),
        zero_volume_fraction=zero_volume_fraction,
        range_contraction_ratio=short_true_range / long_true_range,
        signal_tick_size=signal_tick,
        signal_tick_fraction=signal_tick / current.adjusted_close,
        average_true_range_in_ticks=atr / signal_tick,
        tick_change_count=tick_change_count,
    )
    vector.verify_content_identity()
    return vector


def _compute_vector(
    history: PromotedFeatureInputHistory,
    config: PromotedTechnicalFeatureConfig,
    cutoff: datetime,
) -> PromotedTechnicalFeatureVector:
    # Decimal division and square root otherwise inherit mutable process-global
    # context. Freeze it so identical evidence produces identical feature IDs
    # in every worker and replay.
    with localcontext() as context:
        context.prec = 28
        context.rounding = ROUND_HALF_EVEN
        return _compute_vector_exact(history, config, cutoff)


@dataclass(frozen=True, slots=True)
class PromotedTechnicalFeatureResult:
    source_result: PromotedFeatureInputResult
    status: PromotedTechnicalFeatureStatus
    required_history_sessions: int
    observed_history_sessions: int
    feature_vector: PromotedTechnicalFeatureVector | None
    reason_codes: tuple[str, ...]
    result_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            type(self.source_result) is not PromotedFeatureInputResult
            or type(self.status) is not PromotedTechnicalFeatureStatus
            or not _positive_integer(self.required_history_sessions)
            or type(self.observed_history_sessions) is not int
            or self.observed_history_sessions < 0
            or (
                self.feature_vector is not None
                and type(self.feature_vector) is not PromotedTechnicalFeatureVector
            )
            or self.reason_codes != _reasons(self.status)
        ):
            raise PromotedTechnicalFeatureError(_ERR_GRAPH)
        try:
            self.source_result.verify_content_identity()
            if self.feature_vector is not None:
                self.feature_vector.verify_content_identity()
        except Exception:
            raise PromotedTechnicalFeatureError(_ERR_GRAPH) from None
        history = self.source_result.input_history
        expected_observed = 0 if history is None else len(history.bars)
        computed = (
            self.status
            is PromotedTechnicalFeatureStatus.FEATURE_VECTOR_COMPUTED_COLLECTION_ONLY
        )
        if self.observed_history_sessions != expected_observed:
            raise PromotedTechnicalFeatureError(_ERR_GRAPH)
        if computed:
            if (
                history is None
                or self.feature_vector is None
                or self.feature_vector.source_history_id != history.history_id
                or self.feature_vector.stable_instrument_id
                != history.stable_instrument_id
                or self.feature_vector.stable_listing_id
                != history.stable_listing_id
                or self.feature_vector.signal_session != history.signal_session
                or self.feature_vector.input_bar_ids
                != tuple(value.input_bar_id for value in history.bars)
                or self.feature_vector.knowledge_time
                != max(value.knowledge_time for value in history.bars)
                or self.feature_vector.cutoff < history.cutoff
                or self.observed_history_sessions
                < self.required_history_sessions
            ):
                raise PromotedTechnicalFeatureError(_ERR_GRAPH)
        else:
            if self.feature_vector is not None:
                raise PromotedTechnicalFeatureError(_ERR_GRAPH)
            source_assembled = (
                self.source_result.status
                is PromotedFeatureInputStatus.INPUT_GRAPH_ASSEMBLED_COLLECTION_ONLY
                and history is not None
            )
            if (
                self.status is PromotedTechnicalFeatureStatus.SOURCE_INPUT_BLOCKED
                and source_assembled
            ) or (
                self.status
                is PromotedTechnicalFeatureStatus.INSUFFICIENT_HISTORY_BLOCKED
                and (
                    not source_assembled
                    or self.observed_history_sessions
                    >= self.required_history_sessions
                )
            ) or (
                self.status
                is PromotedTechnicalFeatureStatus.DEGENERATE_INPUT_BLOCKED
                and (
                    not source_assembled
                    or self.observed_history_sessions
                    < self.required_history_sessions
                )
            ):
                raise PromotedTechnicalFeatureError(_ERR_GRAPH)
        object.__setattr__(self, "result_id", self._calculated_id())

    def _identity(self) -> dict[str, object]:
        return {
            "source_result_id": self.source_result.result_id,
            "status": self.status,
            "required_history_sessions": self.required_history_sessions,
            "observed_history_sessions": self.observed_history_sessions,
            "feature_id": (
                None
                if self.feature_vector is None
                else self.feature_vector.feature_id
            ),
            "reason_codes": self.reason_codes,
        }

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "promoted-technical-feature-result/v1",
                **self._identity(),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        expected = PromotedTechnicalFeatureResult(
            source_result=self.source_result,
            status=self.status,
            required_history_sessions=self.required_history_sessions,
            observed_history_sessions=self.observed_history_sessions,
            feature_vector=self.feature_vector,
            reason_codes=self.reason_codes,
        )
        if self.result_id != expected.result_id:
            raise PromotedTechnicalFeatureError(_ERR_ID)


@dataclass(frozen=True, slots=True)
class _TechnicalFeatureFacts:
    cutoff: datetime
    knowledge_time: datetime
    results: tuple[PromotedTechnicalFeatureResult, ...]
    status_counts: tuple[tuple[str, int], ...]
    computed_history_count: int
    blocked_history_count: int
    resolved_histories_feature_complete: bool
    unassigned_entry_count: int
    readiness: ReferenceReadiness
    actionable: bool
    training_eligible: bool
    feature_eligible: bool
    cross_sectional_ranking_eligible: bool
    alert_eligible: bool
    execution_eligible: bool
    panel_id: str


def _make_result(
    *,
    source_result: PromotedFeatureInputResult,
    status: PromotedTechnicalFeatureStatus,
    required: int,
    vector: PromotedTechnicalFeatureVector | None,
) -> PromotedTechnicalFeatureResult:
    history = source_result.input_history
    return PromotedTechnicalFeatureResult(
        source_result=source_result,
        status=status,
        required_history_sessions=required,
        observed_history_sessions=0 if history is None else len(history.bars),
        feature_vector=vector,
        reason_codes=_reasons(status),
    )


def _panel_identity(
    *,
    source_panel_id: str,
    config_id: str,
    facts: _TechnicalFeatureFacts,
) -> dict[str, object]:
    return {
        "schema_version": PROMOTED_TECHNICAL_FEATURE_SCHEMA_VERSION,
        "policy_version": PROMOTED_TECHNICAL_FEATURE_POLICY_VERSION,
        "source_panel_id": source_panel_id,
        "config_id": config_id,
        "cutoff": facts.cutoff,
        "knowledge_time": facts.knowledge_time,
        "result_ids": tuple(value.result_id for value in facts.results),
        "status_counts": facts.status_counts,
        "computed_history_count": facts.computed_history_count,
        "blocked_history_count": facts.blocked_history_count,
        "resolved_histories_feature_complete": (
            facts.resolved_histories_feature_complete
        ),
        "unassigned_entry_count": facts.unassigned_entry_count,
        "readiness": facts.readiness,
        "actionable": facts.actionable,
        "training_eligible": facts.training_eligible,
        "feature_eligible": facts.feature_eligible,
        "cross_sectional_ranking_eligible": (
            facts.cross_sectional_ranking_eligible
        ),
        "alert_eligible": facts.alert_eligible,
        "execution_eligible": facts.execution_eligible,
    }


def _build_facts(
    source_panel: VerifiedPromotedFeatureInputPanel,
    config: PromotedTechnicalFeatureConfig,
    cutoff: datetime,
) -> _TechnicalFeatureFacts:
    if (
        type(source_panel) is not VerifiedPromotedFeatureInputPanel
        or type(config) is not PromotedTechnicalFeatureConfig
    ):
        raise PromotedTechnicalFeatureError(_ERR_INPUT)
    cutoff = _utc(cutoff)
    try:
        source_panel.verify_content_identity()
        config.verify_content_identity()
    except Exception:
        raise PromotedTechnicalFeatureError(_ERR_VERIFY) from None
    if cutoff < max(source_panel.cutoff, source_panel.knowledge_time):
        raise PromotedTechnicalFeatureError(_ERR_FUTURE)
    if (
        source_panel.readiness is not ReferenceReadiness.COLLECTION_ONLY
        or source_panel.actionable is not False
        or source_panel.training_eligible is not False
        or source_panel.feature_eligible is not False
        or source_panel.cross_sectional_ranking_eligible is not False
        or source_panel.alert_eligible is not False
        or source_panel.execution_eligible is not False
    ):
        raise PromotedTechnicalFeatureError(_ERR_INPUT)

    results: list[PromotedTechnicalFeatureResult] = []
    try:
        for source_result in source_panel.results:
            history = source_result.input_history
            if (
                source_result.status
                is not PromotedFeatureInputStatus.INPUT_GRAPH_ASSEMBLED_COLLECTION_ONLY
                or history is None
            ):
                results.append(
                    _make_result(
                        source_result=source_result,
                        status=PromotedTechnicalFeatureStatus.SOURCE_INPUT_BLOCKED,
                        required=config.minimum_history_sessions,
                        vector=None,
                    )
                )
                continue
            if len(history.bars) < config.minimum_history_sessions:
                results.append(
                    _make_result(
                        source_result=source_result,
                        status=(
                            PromotedTechnicalFeatureStatus
                            .INSUFFICIENT_HISTORY_BLOCKED
                        ),
                        required=config.minimum_history_sessions,
                        vector=None,
                    )
                )
                continue
            try:
                vector = _compute_vector(history, config, cutoff)
            except _DegenerateInput:
                results.append(
                    _make_result(
                        source_result=source_result,
                        status=PromotedTechnicalFeatureStatus.DEGENERATE_INPUT_BLOCKED,
                        required=config.minimum_history_sessions,
                        vector=None,
                    )
                )
                continue
            results.append(
                _make_result(
                    source_result=source_result,
                    status=(
                        PromotedTechnicalFeatureStatus
                        .FEATURE_VECTOR_COMPUTED_COLLECTION_ONLY
                    ),
                    required=config.minimum_history_sessions,
                    vector=vector,
                )
            )
    except PromotedTechnicalFeatureError:
        raise
    except Exception:
        raise PromotedTechnicalFeatureError(_ERR_GRAPH) from None

    result_tuple = tuple(results)
    if len(result_tuple) != len(source_panel.results):
        raise PromotedTechnicalFeatureError(_ERR_GRAPH)
    status_counts = _counts(tuple(value.status.value for value in result_tuple))
    computed_count = sum(
        value.status
        is PromotedTechnicalFeatureStatus.FEATURE_VECTOR_COMPUTED_COLLECTION_ONLY
        for value in result_tuple
    )
    blocked_count = len(result_tuple) - computed_count
    complete = bool(result_tuple) and computed_count == len(result_tuple)
    provisional = _TechnicalFeatureFacts(
        cutoff=cutoff,
        knowledge_time=source_panel.knowledge_time,
        results=result_tuple,
        status_counts=status_counts,
        computed_history_count=computed_count,
        blocked_history_count=blocked_count,
        resolved_histories_feature_complete=complete,
        unassigned_entry_count=source_panel.unassigned_entry_count,
        readiness=ReferenceReadiness.COLLECTION_ONLY,
        actionable=False,
        training_eligible=False,
        feature_eligible=False,
        cross_sectional_ranking_eligible=False,
        alert_eligible=False,
        execution_eligible=False,
        panel_id="",
    )
    panel_id = content_id(
        _panel_identity(
            source_panel_id=source_panel.panel_id,
            config_id=config.config_id,
            facts=provisional,
        ),
        length=64,
    )
    return _TechnicalFeatureFacts(
        cutoff=provisional.cutoff,
        knowledge_time=provisional.knowledge_time,
        results=provisional.results,
        status_counts=provisional.status_counts,
        computed_history_count=provisional.computed_history_count,
        blocked_history_count=provisional.blocked_history_count,
        resolved_histories_feature_complete=(
            provisional.resolved_histories_feature_complete
        ),
        unassigned_entry_count=provisional.unassigned_entry_count,
        readiness=provisional.readiness,
        actionable=provisional.actionable,
        training_eligible=provisional.training_eligible,
        feature_eligible=provisional.feature_eligible,
        cross_sectional_ranking_eligible=(
            provisional.cross_sectional_ranking_eligible
        ),
        alert_eligible=provisional.alert_eligible,
        execution_eligible=provisional.execution_eligible,
        panel_id=panel_id,
    )


@dataclass(frozen=True, slots=True)
class VerifiedPromotedTechnicalFeaturePanel:
    schema_version: str
    policy_version: str
    source_panel: VerifiedPromotedFeatureInputPanel
    config: PromotedTechnicalFeatureConfig
    cutoff: datetime
    knowledge_time: datetime
    results: tuple[PromotedTechnicalFeatureResult, ...]
    status_counts: tuple[tuple[str, int], ...]
    computed_history_count: int
    blocked_history_count: int
    resolved_histories_feature_complete: bool
    unassigned_entry_count: int
    readiness: ReferenceReadiness
    actionable: bool
    training_eligible: bool
    feature_eligible: bool
    cross_sectional_ranking_eligible: bool
    alert_eligible: bool
    execution_eligible: bool
    panel_id: str

    def __post_init__(self) -> None:
        self.verify_content_identity()

    def verify_content_identity(self) -> None:
        if type(self) is not VerifiedPromotedTechnicalFeaturePanel:
            raise PromotedTechnicalFeatureError(_ERR_TYPE)
        if (
            type(self.schema_version) is not str
            or self.schema_version != PROMOTED_TECHNICAL_FEATURE_SCHEMA_VERSION
            or type(self.policy_version) is not str
            or self.policy_version != PROMOTED_TECHNICAL_FEATURE_POLICY_VERSION
            or type(self.source_panel) is not VerifiedPromotedFeatureInputPanel
            or type(self.config) is not PromotedTechnicalFeatureConfig
            or type(self.cutoff) is not datetime
            or type(self.knowledge_time) is not datetime
            or type(self.results) is not tuple
            or any(
                type(value) is not PromotedTechnicalFeatureResult
                for value in self.results
            )
            or type(self.status_counts) is not tuple
            or any(
                type(value) is not tuple
                or len(value) != 2
                or type(value[0]) is not str
                or type(value[1]) is not int
                for value in self.status_counts
            )
            or type(self.computed_history_count) is not int
            or self.computed_history_count < 0
            or type(self.blocked_history_count) is not int
            or self.blocked_history_count < 0
            or type(self.resolved_histories_feature_complete) is not bool
            or type(self.unassigned_entry_count) is not int
            or self.unassigned_entry_count < 0
            or type(self.readiness) is not ReferenceReadiness
            or any(
                type(value) is not bool
                for value in (
                    self.actionable,
                    self.training_eligible,
                    self.feature_eligible,
                    self.cross_sectional_ranking_eligible,
                    self.alert_eligible,
                    self.execution_eligible,
                )
            )
            or type(self.panel_id) is not str
            or _SHA256.fullmatch(self.panel_id) is None
        ):
            raise PromotedTechnicalFeatureError(_ERR_DERIVED)
        try:
            facts = _build_facts(self.source_panel, self.config, self.cutoff)
            comparisons = (
                (self.cutoff, facts.cutoff),
                (self.knowledge_time, facts.knowledge_time),
                (self.results, facts.results),
                (self.status_counts, facts.status_counts),
                (self.computed_history_count, facts.computed_history_count),
                (self.blocked_history_count, facts.blocked_history_count),
                (
                    self.resolved_histories_feature_complete,
                    facts.resolved_histories_feature_complete,
                ),
                (self.unassigned_entry_count, facts.unassigned_entry_count),
                (self.readiness, facts.readiness),
                (self.actionable, facts.actionable),
                (self.training_eligible, facts.training_eligible),
                (self.feature_eligible, facts.feature_eligible),
                (
                    self.cross_sectional_ranking_eligible,
                    facts.cross_sectional_ranking_eligible,
                ),
                (self.alert_eligible, facts.alert_eligible),
                (self.execution_eligible, facts.execution_eligible),
                (self.panel_id, facts.panel_id),
            )
            if any(left != right for left, right in comparisons):
                raise PromotedTechnicalFeatureError(_ERR_DERIVED)
        except PromotedTechnicalFeatureError:
            raise
        except Exception:
            raise PromotedTechnicalFeatureError(_ERR_DERIVED) from None


class PromotedTechnicalFeatureService:
    """Computes descriptive features and nothing beyond that boundary."""

    def materialize(
        self,
        *,
        source_panel: VerifiedPromotedFeatureInputPanel,
        config: PromotedTechnicalFeatureConfig,
        cutoff: datetime,
    ) -> VerifiedPromotedTechnicalFeaturePanel:
        facts = _build_facts(source_panel, config, cutoff)
        return VerifiedPromotedTechnicalFeaturePanel(
            schema_version=PROMOTED_TECHNICAL_FEATURE_SCHEMA_VERSION,
            policy_version=PROMOTED_TECHNICAL_FEATURE_POLICY_VERSION,
            source_panel=source_panel,
            config=config,
            cutoff=facts.cutoff,
            knowledge_time=facts.knowledge_time,
            results=facts.results,
            status_counts=facts.status_counts,
            computed_history_count=facts.computed_history_count,
            blocked_history_count=facts.blocked_history_count,
            resolved_histories_feature_complete=(
                facts.resolved_histories_feature_complete
            ),
            unassigned_entry_count=facts.unassigned_entry_count,
            readiness=facts.readiness,
            actionable=facts.actionable,
            training_eligible=facts.training_eligible,
            feature_eligible=facts.feature_eligible,
            cross_sectional_ranking_eligible=(
                facts.cross_sectional_ranking_eligible
            ),
            alert_eligible=facts.alert_eligible,
            execution_eligible=facts.execution_eligible,
            panel_id=facts.panel_id,
        )
