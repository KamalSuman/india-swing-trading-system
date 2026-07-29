"""Deterministic research-intent generation from promoted cross-sections.

This module is the decision-policy bridge between collection-only opportunity
scores and the existing pessimistic daily execution simulator.  It deliberately
creates research artifacts only: no alert or execution authority is granted.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, fields
from datetime import date
from decimal import (
    Decimal,
    ROUND_CEILING,
    ROUND_FLOOR,
    ROUND_HALF_EVEN,
    localcontext,
)
from enum import Enum

from india_swing.evaluation.engine import EvaluationTradeIntent
from india_swing.execution.simulator import LimitEntryOrder
from india_swing.features.promoted_cross_section import (
    PromotedCrossSectionResult,
    PromotedCrossSectionResultStatus,
    PromotedOpportunityScore,
    VerifiedPromotedCrossSectionPanel,
)
from india_swing.features.promoted_technical import (
    PromotedTechnicalFeatureVector,
)
from india_swing.forecasting.regime_ensemble import MarketRegime
from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness


class PromotedIntentError(ValueError):
    """Raised when research-intent generation cannot remain deterministic."""


PROMOTED_INTENT_POLICY_VERSION = "promoted-research-intent/risk-sized-v1"
PROMOTED_INTENT_CONFIG_SCHEMA_VERSION = "promoted-research-intent-config/v1"
PROMOTED_INTENT_BATCH_SCHEMA_VERSION = "promoted-research-intent-batch/v1"

_ZERO = Decimal("0")
_ONE = Decimal("1")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_REASON = re.compile(r"[A-Z][A-Z0-9_]{2,127}\Z")
_ERR_CONFIG = "promoted intent configuration is invalid"
_ERR_INPUT = "promoted intent source is invalid"
_ERR_GRAPH = "promoted intent graph is invalid"
_ERR_ID = "promoted intent identifier is invalid"


def _finite(value: object) -> bool:
    return type(value) is Decimal and value.is_finite()


def _positive(value: object) -> bool:
    return _finite(value) and value > _ZERO


def _fraction(value: object, *, allow_one: bool = True) -> bool:
    upper = value <= _ONE if _finite(value) else False
    if not allow_one and upper:
        upper = value < _ONE
    return bool(_finite(value) and value > _ZERO and upper)


def _sha(value: object) -> bool:
    return type(value) is str and _SHA256.fullmatch(value) is not None


def _on_tick_floor(value: Decimal, tick_size: Decimal) -> Decimal:
    return (
        (value / tick_size).to_integral_value(rounding=ROUND_FLOOR)
        * tick_size
    )


def _on_tick_ceiling(value: Decimal, tick_size: Decimal) -> Decimal:
    return (
        (value / tick_size).to_integral_value(rounding=ROUND_CEILING)
        * tick_size
    )


def _default_allowed_regimes() -> tuple[MarketRegime, ...]:
    return (
        MarketRegime.RANGE_BOUND,
        MarketRegime.TRENDING,
    )


@dataclass(frozen=True, slots=True)
class PromotedIntentPolicyConfig:
    """Immutable preregistered gates and portfolio construction parameters."""

    maximum_positions: int = 5
    gross_exposure_fraction: Decimal = Decimal("0.95")
    portfolio_risk_fraction: Decimal = Decimal("0.02")
    minimum_ensemble_score: Decimal = Decimal("0.55")
    minimum_median_traded_value: Decimal = Decimal("50000000")
    minimum_signal_traded_value_ratio: Decimal = Decimal("0.50")
    maximum_tick_fraction: Decimal = Decimal("0.0025")
    minimum_average_true_range_ticks: Decimal = Decimal("8")
    maximum_annualized_volatility: Decimal = Decimal("0.75")
    maximum_zero_volume_fraction: Decimal = Decimal("0.10")
    stop_atr_multiple: Decimal = Decimal("1.50")
    minimum_net_reward_risk: Decimal = Decimal("2.50")
    round_trip_cost_buffer_fraction: Decimal = Decimal("0.0020")
    maximum_holding_sessions: int = 10
    maximum_participation: Decimal = Decimal("0.0025")
    allowed_regimes: tuple[MarketRegime, ...] = field(
        default_factory=_default_allowed_regimes
    )
    policy_version: str = PROMOTED_INTENT_POLICY_VERSION
    schema_version: str = PROMOTED_INTENT_CONFIG_SCHEMA_VERSION
    config_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            type(self.maximum_positions) is not int
            or self.maximum_positions <= 0
            or not _fraction(self.gross_exposure_fraction)
            or not _fraction(self.portfolio_risk_fraction, allow_one=False)
            or not _fraction(self.minimum_ensemble_score)
            or not _positive(self.minimum_median_traded_value)
            or not _positive(self.minimum_signal_traded_value_ratio)
            or not _fraction(self.maximum_tick_fraction, allow_one=False)
            or not _positive(self.minimum_average_true_range_ticks)
            or not _positive(self.maximum_annualized_volatility)
            or not _fraction(self.maximum_zero_volume_fraction)
            or not _positive(self.stop_atr_multiple)
            or not _positive(self.minimum_net_reward_risk)
            or self.minimum_net_reward_risk < Decimal("2.5")
            or not _fraction(
                self.round_trip_cost_buffer_fraction,
                allow_one=False,
            )
            or type(self.maximum_holding_sessions) is not int
            or self.maximum_holding_sessions <= 0
            or not _fraction(self.maximum_participation)
            or type(self.allowed_regimes) is not tuple
            or not self.allowed_regimes
            or any(type(value) is not MarketRegime for value in self.allowed_regimes)
            or self.allowed_regimes
            != tuple(sorted(set(self.allowed_regimes), key=lambda value: value.value))
            or MarketRegime.RISK_OFF in self.allowed_regimes
            or MarketRegime.HIGH_VOLATILITY in self.allowed_regimes
            or self.policy_version != PROMOTED_INTENT_POLICY_VERSION
            or self.schema_version != PROMOTED_INTENT_CONFIG_SCHEMA_VERSION
        ):
            raise PromotedIntentError(_ERR_CONFIG)
        object.__setattr__(self, "config_id", self._calculated_id())

    def _identity(self) -> dict[str, object]:
        return {
            value.name: getattr(self, value.name)
            for value in fields(self)
            if value.name != "config_id"
        }

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": PROMOTED_INTENT_CONFIG_SCHEMA_VERSION,
                **self._identity(),
                "rank_boundary_policy": "REJECT_WHOLE_TIE_AT_CAPACITY",
                "position_sizing": "MIN_RISK_BUDGET_AND_EQUAL_SLOT_NOTIONAL",
                "entry_timing": "NEXT_EXPLICIT_SESSION_ONLY",
                "price_basis": "ADJUSTED_SIGNAL_CLOSE",
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        expected = PromotedIntentPolicyConfig(**self._identity())
        if self.config_id != expected.config_id:
            raise PromotedIntentError(_ERR_ID)


class PromotedCandidateDecisionStatus(str, Enum):
    SELECTED_RESEARCH_ONLY = "SELECTED_RESEARCH_ONLY"
    SOURCE_RESULT_BLOCKED = "SOURCE_RESULT_BLOCKED"
    SOURCE_UNIVERSE_INCOMPLETE = "SOURCE_UNIVERSE_INCOMPLETE"
    REGIME_VETOED = "REGIME_VETOED"
    SCORE_BELOW_MINIMUM = "SCORE_BELOW_MINIMUM"
    LIQUIDITY_BELOW_MINIMUM = "LIQUIDITY_BELOW_MINIMUM"
    SIGNAL_LIQUIDITY_WEAK = "SIGNAL_LIQUIDITY_WEAK"
    TICK_FRICTION_TOO_HIGH = "TICK_FRICTION_TOO_HIGH"
    ATR_IN_TICKS_TOO_LOW = "ATR_IN_TICKS_TOO_LOW"
    VOLATILITY_ABOVE_MAXIMUM = "VOLATILITY_ABOVE_MAXIMUM"
    ZERO_VOLUME_HISTORY_TOO_HIGH = "ZERO_VOLUME_HISTORY_TOO_HIGH"
    UNAFFORDABLE_OR_INVALID_RISK = "UNAFFORDABLE_OR_INVALID_RISK"
    BOUNDARY_TIE_REJECTED = "BOUNDARY_TIE_REJECTED"
    OUTSIDE_POSITION_CAPACITY = "OUTSIDE_POSITION_CAPACITY"


@dataclass(frozen=True, slots=True)
class _PreparedCandidate:
    source_result: PromotedCrossSectionResult
    opportunity: PromotedOpportunityScore
    vector: PromotedTechnicalFeatureVector
    universe_snapshot_id: str
    symbol: str
    isin: str
    signal_close: Decimal
    entry_price: Decimal
    stop_price: Decimal
    target_price: Decimal
    quantity: int

    def __post_init__(self) -> None:
        if (
            type(self.source_result) is not PromotedCrossSectionResult
            or type(self.opportunity) is not PromotedOpportunityScore
            or type(self.vector) is not PromotedTechnicalFeatureVector
            or not _sha(self.universe_snapshot_id)
            or not isinstance(self.symbol, str)
            or not self.symbol
            or self.symbol != self.symbol.strip().upper()
            or not isinstance(self.isin, str)
            or len(self.isin) != 12
            or any(
                not _positive(value)
                for value in (
                    self.signal_close,
                    self.entry_price,
                    self.stop_price,
                    self.target_price,
                )
            )
            or not self.stop_price < self.entry_price < self.target_price
            or type(self.quantity) is not int
            or self.quantity <= 0
        ):
            raise PromotedIntentError(_ERR_GRAPH)


def _select_complete_tiers(
    candidates: tuple[_PreparedCandidate, ...],
    maximum_positions: int,
) -> tuple[set[str], set[str]]:
    """Return selected IDs and capacity-boundary tie rejections.

    Stable identifiers are never used to order equal scores.  A tier that
    cannot fit in the remaining slots is rejected in full, and lower tiers are
    not promoted around it.
    """

    if (
        type(candidates) is not tuple
        or type(maximum_positions) is not int
        or maximum_positions <= 0
        or any(type(value) is not _PreparedCandidate for value in candidates)
    ):
        raise PromotedIntentError(_ERR_GRAPH)
    selected: set[str] = set()
    boundary_rejected: set[str] = set()
    tiers = tuple(
        sorted({value.opportunity.rank_tier for value in candidates})
    )
    for tier in tiers:
        members = tuple(
            value
            for value in candidates
            if value.opportunity.rank_tier == tier
        )
        remaining = maximum_positions - len(selected)
        if remaining <= 0:
            break
        if len(members) > remaining:
            boundary_rejected.update(
                value.opportunity.opportunity_id for value in members
            )
            break
        selected.update(value.opportunity.opportunity_id for value in members)
    return selected, boundary_rejected


@dataclass(frozen=True, slots=True)
class PromotedCandidateDecision:
    source_result_id: str
    source_feature_id: str | None
    opportunity_id: str | None
    stable_instrument_id: str | None
    stable_listing_id: str | None
    signal_session: date | None
    ensemble_score: Decimal | None
    rank_tier: int | None
    tie_size: int | None
    status: PromotedCandidateDecisionStatus
    reason_codes: tuple[str, ...]
    selected: bool
    decision_id: str = field(init=False)

    def __post_init__(self) -> None:
        optional_ids = (
            self.source_feature_id,
            self.opportunity_id,
            self.stable_instrument_id,
            self.stable_listing_id,
        )
        if (
            not _sha(self.source_result_id)
            or any(value is not None and not _sha(value) for value in optional_ids)
            or (
                self.signal_session is not None
                and type(self.signal_session) is not date
            )
            or (
                self.ensemble_score is not None
                and not _finite(self.ensemble_score)
            )
            or (
                self.rank_tier is not None
                and (
                    type(self.rank_tier) is not int
                    or self.rank_tier <= 0
                )
            )
            or (
                self.tie_size is not None
                and (
                    type(self.tie_size) is not int
                    or self.tie_size <= 0
                )
            )
            or type(self.status) is not PromotedCandidateDecisionStatus
            or type(self.reason_codes) is not tuple
            or not self.reason_codes
            or self.reason_codes != tuple(sorted(set(self.reason_codes)))
            or any(
                type(value) is not str or _REASON.fullmatch(value) is None
                for value in self.reason_codes
            )
            or type(self.selected) is not bool
            or self.selected
            != (
                self.status
                is PromotedCandidateDecisionStatus.SELECTED_RESEARCH_ONLY
            )
        ):
            raise PromotedIntentError(_ERR_GRAPH)
        has_score = self.opportunity_id is not None
        scored_values = (
            self.source_feature_id,
            self.stable_instrument_id,
            self.stable_listing_id,
            self.signal_session,
            self.ensemble_score,
            self.rank_tier,
            self.tie_size,
        )
        if has_score != all(value is not None for value in scored_values):
            raise PromotedIntentError(_ERR_GRAPH)
        expected_reasons = tuple(
            sorted(
                {
                    self.status.value,
                    "NO_LIVE_ALERT_AUTHORITY",
                    "NO_EXECUTION_AUTHORITY",
                    "SCORE_IS_NOT_A_PROBABILITY",
                }
            )
        )
        if self.reason_codes != expected_reasons:
            raise PromotedIntentError(_ERR_GRAPH)
        object.__setattr__(self, "decision_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "promoted-candidate-decision/v1",
                "source_result_id": self.source_result_id,
                "source_feature_id": self.source_feature_id,
                "opportunity_id": self.opportunity_id,
                "stable_instrument_id": self.stable_instrument_id,
                "stable_listing_id": self.stable_listing_id,
                "signal_session": self.signal_session,
                "ensemble_score": self.ensemble_score,
                "rank_tier": self.rank_tier,
                "tie_size": self.tie_size,
                "status": self.status,
                "reason_codes": self.reason_codes,
                "selected": self.selected,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        expected = PromotedCandidateDecision(
            **{
                value.name: getattr(self, value.name)
                for value in fields(self)
                if value.name != "decision_id"
            }
        )
        if self.decision_id != expected.decision_id:
            raise PromotedIntentError(_ERR_ID)


@dataclass(frozen=True, slots=True)
class PromotedResearchTradeIntent:
    decision_id: str
    source_cross_section_panel_id: str
    source_feature_id: str
    opportunity_id: str
    stable_instrument_id: str
    stable_listing_id: str
    universe_snapshot_id: str
    evaluation_intent: EvaluationTradeIntent
    estimated_cost_buffer: Decimal
    planned_net_reward_risk: Decimal
    actionable: bool = False
    alert_eligible: bool = False
    execution_eligible: bool = False
    research_intent_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            any(
                not _sha(value)
                for value in (
                    self.decision_id,
                    self.source_cross_section_panel_id,
                    self.source_feature_id,
                    self.opportunity_id,
                    self.stable_instrument_id,
                    self.stable_listing_id,
                    self.universe_snapshot_id,
                )
            )
            or type(self.evaluation_intent) is not EvaluationTradeIntent
            or self.evaluation_intent.signal_id != self.decision_id
            or self.evaluation_intent.universe_snapshot_id
            != self.universe_snapshot_id
            or not _positive(self.estimated_cost_buffer)
            or not _positive(self.planned_net_reward_risk)
            or self.planned_net_reward_risk < Decimal("2.5")
            or self.actionable is not False
            or self.alert_eligible is not False
            or self.execution_eligible is not False
        ):
            raise PromotedIntentError(_ERR_GRAPH)
        self.evaluation_intent.verify_content_identity()
        with localcontext() as context:
            context.prec = 28
            context.rounding = ROUND_HALF_EVEN
            entry = self.evaluation_intent.entry_order.limit_price
            reward = (
                self.evaluation_intent.target_price
                - entry
                - self.estimated_cost_buffer
            )
            risk = (
                entry
                - self.evaluation_intent.stop_price
                + self.estimated_cost_buffer
            )
            if (
                risk <= _ZERO
                or reward / risk != self.planned_net_reward_risk
            ):
                raise PromotedIntentError(_ERR_GRAPH)
        object.__setattr__(
            self,
            "research_intent_id",
            self._calculated_id(),
        )

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "promoted-research-trade-intent/v1",
                "decision_id": self.decision_id,
                "source_cross_section_panel_id": (
                    self.source_cross_section_panel_id
                ),
                "source_feature_id": self.source_feature_id,
                "opportunity_id": self.opportunity_id,
                "stable_instrument_id": self.stable_instrument_id,
                "stable_listing_id": self.stable_listing_id,
                "universe_snapshot_id": self.universe_snapshot_id,
                "evaluation_intent_id": self.evaluation_intent.intent_id,
                "estimated_cost_buffer": self.estimated_cost_buffer,
                "planned_net_reward_risk": self.planned_net_reward_risk,
                "actionable": self.actionable,
                "alert_eligible": self.alert_eligible,
                "execution_eligible": self.execution_eligible,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        expected = PromotedResearchTradeIntent(
            **{
                value.name: getattr(self, value.name)
                for value in fields(self)
                if value.name != "research_intent_id"
            }
        )
        if self.research_intent_id != expected.research_intent_id:
            raise PromotedIntentError(_ERR_ID)


@dataclass(frozen=True, slots=True)
class VerifiedPromotedResearchIntentBatch:
    schema_version: str
    policy_version: str
    source_panel_id: str
    config_id: str
    signal_session: date
    entry_session: date
    initial_capital: Decimal
    decisions: tuple[PromotedCandidateDecision, ...]
    intents: tuple[PromotedResearchTradeIntent, ...]
    selected_count: int
    blocked_count: int
    source_universe_complete: bool
    readiness: ReferenceReadiness
    actionable: bool
    alert_eligible: bool
    execution_eligible: bool
    batch_id: str

    def __post_init__(self) -> None:
        if (
            self.schema_version != PROMOTED_INTENT_BATCH_SCHEMA_VERSION
            or self.policy_version != PROMOTED_INTENT_POLICY_VERSION
            or not _sha(self.source_panel_id)
            or not _sha(self.config_id)
            or type(self.signal_session) is not date
            or type(self.entry_session) is not date
            or self.entry_session <= self.signal_session
            or not _positive(self.initial_capital)
            or type(self.decisions) is not tuple
            or not self.decisions
            or any(
                type(value) is not PromotedCandidateDecision
                for value in self.decisions
            )
            or type(self.intents) is not tuple
            or any(
                type(value) is not PromotedResearchTradeIntent
                for value in self.intents
            )
            or type(self.selected_count) is not int
            or type(self.blocked_count) is not int
            or self.selected_count != len(self.intents)
            or self.blocked_count != len(self.decisions) - self.selected_count
            or type(self.source_universe_complete) is not bool
            or self.readiness is not ReferenceReadiness.COLLECTION_ONLY
            or self.actionable is not False
            or self.alert_eligible is not False
            or self.execution_eligible is not False
            or not _sha(self.batch_id)
        ):
            raise PromotedIntentError(_ERR_GRAPH)
        for value in self.decisions:
            value.verify_content_identity()
        for value in self.intents:
            value.verify_content_identity()
            if (
                value.source_cross_section_panel_id != self.source_panel_id
                or value.evaluation_intent.entry_order.signal_session
                != self.signal_session
                or value.evaluation_intent.entry_order.first_eligible_session
                != self.entry_session
            ):
                raise PromotedIntentError(_ERR_GRAPH)
        selected_decision_ids = {
            value.decision_id for value in self.decisions if value.selected
        }
        intent_decision_ids = {value.decision_id for value in self.intents}
        if (
            len(selected_decision_ids) != self.selected_count
            or selected_decision_ids != intent_decision_ids
            or self.batch_id != self._calculated_id()
        ):
            raise PromotedIntentError(_ERR_GRAPH)

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": PROMOTED_INTENT_BATCH_SCHEMA_VERSION,
                "policy_version": self.policy_version,
                "source_panel_id": self.source_panel_id,
                "config_id": self.config_id,
                "signal_session": self.signal_session,
                "entry_session": self.entry_session,
                "initial_capital": self.initial_capital,
                "decision_ids": tuple(
                    value.decision_id for value in self.decisions
                ),
                "research_intent_ids": tuple(
                    value.research_intent_id for value in self.intents
                ),
                "selected_count": self.selected_count,
                "blocked_count": self.blocked_count,
                "source_universe_complete": self.source_universe_complete,
                "readiness": self.readiness,
                "actionable": self.actionable,
                "alert_eligible": self.alert_eligible,
                "execution_eligible": self.execution_eligible,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.batch_id != self._calculated_id():
            raise PromotedIntentError(_ERR_ID)
        for value in self.decisions:
            value.verify_content_identity()
        for value in self.intents:
            value.verify_content_identity()


def _reason_codes(
    status: PromotedCandidateDecisionStatus,
) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                status.value,
                "NO_LIVE_ALERT_AUTHORITY",
                "NO_EXECUTION_AUTHORITY",
                "SCORE_IS_NOT_A_PROBABILITY",
            }
        )
    )


def _decision(
    result: PromotedCrossSectionResult,
    status: PromotedCandidateDecisionStatus,
) -> PromotedCandidateDecision:
    opportunity = result.opportunity_score
    vector = result.source_result.feature_vector
    return PromotedCandidateDecision(
        source_result_id=result.result_id,
        source_feature_id=None if vector is None else vector.feature_id,
        opportunity_id=(
            None if opportunity is None else opportunity.opportunity_id
        ),
        stable_instrument_id=(
            None if opportunity is None else opportunity.stable_instrument_id
        ),
        stable_listing_id=(
            None if opportunity is None else opportunity.stable_listing_id
        ),
        signal_session=None if vector is None else vector.signal_session,
        ensemble_score=(
            None if opportunity is None else opportunity.ensemble_score
        ),
        rank_tier=None if opportunity is None else opportunity.rank_tier,
        tie_size=None if opportunity is None else opportunity.tie_size,
        status=status,
        reason_codes=_reason_codes(status),
        selected=(
            status
            is PromotedCandidateDecisionStatus.SELECTED_RESEARCH_ONLY
        ),
    )


def _source_security(
    vector: PromotedTechnicalFeatureVector,
    result: PromotedCrossSectionResult,
) -> tuple[str, str, str, Decimal]:
    history = result.source_result.source_result.input_history
    adjustment_result = (
        result.source_result.source_result.source_adjustment_result
    )
    if (
        history is None
        or history.signal_session != vector.signal_session
        or not history.bars
        or not adjustment_result.identity_bindings
        or len(adjustment_result.identity_bindings) != len(history.bars)
    ):
        raise PromotedIntentError(_ERR_GRAPH)
    signal_bar = history.bars[-1]
    signal_binding = adjustment_result.identity_bindings[-1]
    source_bar = signal_bar.adjusted_bar.source_bar
    if (
        signal_bar.market_session != vector.signal_session
        or not source_bar.listing_key.startswith("NSE:")
        or signal_binding.market_session != vector.signal_session
        or signal_binding.raw_bar_id != source_bar.bar_id
    ):
        raise PromotedIntentError(_ERR_GRAPH)
    symbol = source_bar.listing_key.removeprefix("NSE:")
    return (
        symbol,
        source_bar.isin,
        signal_binding.identity_snapshot_id,
        signal_bar.adjusted_close,
    )


def _preparation_status(
    *,
    opportunity: PromotedOpportunityScore,
    vector: PromotedTechnicalFeatureVector,
    config: PromotedIntentPolicyConfig,
) -> PromotedCandidateDecisionStatus | None:
    if opportunity.ensemble_score < config.minimum_ensemble_score:
        return PromotedCandidateDecisionStatus.SCORE_BELOW_MINIMUM
    if (
        vector.median_prior_traded_value
        < config.minimum_median_traded_value
    ):
        return PromotedCandidateDecisionStatus.LIQUIDITY_BELOW_MINIMUM
    if (
        vector.signal_traded_value_ratio
        < config.minimum_signal_traded_value_ratio
    ):
        return PromotedCandidateDecisionStatus.SIGNAL_LIQUIDITY_WEAK
    if vector.signal_tick_fraction > config.maximum_tick_fraction:
        return PromotedCandidateDecisionStatus.TICK_FRICTION_TOO_HIGH
    if (
        vector.average_true_range_in_ticks
        < config.minimum_average_true_range_ticks
    ):
        return PromotedCandidateDecisionStatus.ATR_IN_TICKS_TOO_LOW
    if (
        vector.annualized_realized_volatility
        > config.maximum_annualized_volatility
    ):
        return PromotedCandidateDecisionStatus.VOLATILITY_ABOVE_MAXIMUM
    if vector.zero_volume_fraction > config.maximum_zero_volume_fraction:
        return PromotedCandidateDecisionStatus.ZERO_VOLUME_HISTORY_TOO_HIGH
    return None


def _prepare(
    *,
    result: PromotedCrossSectionResult,
    opportunity: PromotedOpportunityScore,
    vector: PromotedTechnicalFeatureVector,
    config: PromotedIntentPolicyConfig,
    initial_capital: Decimal,
) -> _PreparedCandidate | None:
    with localcontext() as context:
        context.prec = 28
        context.rounding = ROUND_HALF_EVEN
        (
            symbol,
            isin,
            universe_snapshot_id,
            signal_close,
        ) = _source_security(vector, result)
        entry_price = _on_tick_floor(signal_close, vector.signal_tick_size)
        stop_price = _on_tick_floor(
            entry_price
            - vector.average_true_range * config.stop_atr_multiple,
            vector.signal_tick_size,
        )
        if stop_price <= _ZERO or stop_price >= entry_price:
            return None
        cost_buffer = (
            entry_price * config.round_trip_cost_buffer_fraction
        )
        risk_per_share = entry_price - stop_price + cost_buffer
        if risk_per_share <= _ZERO:
            return None
        risk_budget = (
            initial_capital
            * config.portfolio_risk_fraction
            / Decimal(config.maximum_positions)
        )
        slot_notional = (
            initial_capital
            * config.gross_exposure_fraction
            / Decimal(config.maximum_positions)
        )
        quantity = min(
            int(risk_budget / risk_per_share),
            int(slot_notional / entry_price),
        )
        if quantity <= 0:
            return None
        target_raw = (
            entry_price
            + cost_buffer
            + config.minimum_net_reward_risk * risk_per_share
        )
        target_price = _on_tick_ceiling(
            target_raw,
            vector.signal_tick_size,
        )
    if target_price <= entry_price:
        return None
    return _PreparedCandidate(
        source_result=result,
        opportunity=opportunity,
        vector=vector,
        universe_snapshot_id=universe_snapshot_id,
        symbol=symbol,
        isin=isin,
        signal_close=signal_close,
        entry_price=entry_price,
        stop_price=stop_price,
        target_price=target_price,
        quantity=quantity,
    )


class PromotedResearchIntentService:
    """Builds deterministic evaluation intents without live-trading authority."""

    def generate(
        self,
        *,
        source_panel: VerifiedPromotedCrossSectionPanel,
        config: PromotedIntentPolicyConfig,
        entry_session: date,
        initial_capital: Decimal,
    ) -> VerifiedPromotedResearchIntentBatch:
        if (
            type(source_panel) is not VerifiedPromotedCrossSectionPanel
            or type(config) is not PromotedIntentPolicyConfig
            or type(entry_session) is not date
            or not _positive(initial_capital)
        ):
            raise PromotedIntentError(_ERR_INPUT)
        try:
            source_panel.verify_content_identity()
            config.verify_content_identity()
        except Exception:
            raise PromotedIntentError(_ERR_INPUT) from None
        signal_session = (
            source_panel.source_panel.source_panel.adjustment_panel
            .signal_session
        )
        if any(
            value.source_result.feature_vector is not None
            and value.source_result.feature_vector.signal_session
            != signal_session
            for value in source_panel.results
        ):
            raise PromotedIntentError(_ERR_GRAPH)
        if entry_session <= signal_session:
            raise PromotedIntentError(_ERR_INPUT)

        statuses: dict[str, PromotedCandidateDecisionStatus] = {}
        prepared: list[_PreparedCandidate] = []
        complete = source_panel.source_universe_cross_section_complete
        regime = (
            None
            if source_panel.regime_evidence is None
            else source_panel.regime_evidence.regime
        )
        for result in source_panel.results:
            if (
                result.status
                is not (
                    PromotedCrossSectionResultStatus
                    .SCORED_RESOLVED_SUBSET_COLLECTION_ONLY
                )
                or result.opportunity_score is None
                or result.source_result.feature_vector is None
            ):
                statuses[result.result_id] = (
                    PromotedCandidateDecisionStatus.SOURCE_RESULT_BLOCKED
                )
                continue
            opportunity = result.opportunity_score
            vector = result.source_result.feature_vector
            if not complete:
                statuses[result.result_id] = (
                    PromotedCandidateDecisionStatus
                    .SOURCE_UNIVERSE_INCOMPLETE
                )
                continue
            if regime not in config.allowed_regimes:
                statuses[result.result_id] = (
                    PromotedCandidateDecisionStatus.REGIME_VETOED
                )
                continue
            status = _preparation_status(
                opportunity=opportunity,
                vector=vector,
                config=config,
            )
            if status is not None:
                statuses[result.result_id] = status
                continue
            candidate = _prepare(
                result=result,
                opportunity=opportunity,
                vector=vector,
                config=config,
                initial_capital=initial_capital,
            )
            if candidate is None:
                statuses[result.result_id] = (
                    PromotedCandidateDecisionStatus
                    .UNAFFORDABLE_OR_INVALID_RISK
                )
                continue
            prepared.append(candidate)

        prepared_tuple = tuple(prepared)
        selected_ids, boundary_ids = _select_complete_tiers(
            prepared_tuple,
            config.maximum_positions,
        )
        for candidate in prepared_tuple:
            opportunity_id = candidate.opportunity.opportunity_id
            if opportunity_id in selected_ids:
                status = (
                    PromotedCandidateDecisionStatus.SELECTED_RESEARCH_ONLY
                )
            elif opportunity_id in boundary_ids:
                status = (
                    PromotedCandidateDecisionStatus.BOUNDARY_TIE_REJECTED
                )
            else:
                status = (
                    PromotedCandidateDecisionStatus
                    .OUTSIDE_POSITION_CAPACITY
                )
            statuses[candidate.source_result.result_id] = status

        decisions = tuple(
            _decision(result, statuses[result.result_id])
            for result in source_panel.results
        )
        decision_by_result = {
            result.result_id: decision
            for result, decision in zip(source_panel.results, decisions)
        }
        intents: list[PromotedResearchTradeIntent] = []
        for candidate in prepared_tuple:
            decision = decision_by_result[candidate.source_result.result_id]
            if not decision.selected:
                continue
            evaluation_intent = EvaluationTradeIntent(
                signal_id=decision.decision_id,
                universe_snapshot_id=candidate.universe_snapshot_id,
                isin=candidate.isin,
                entry_order=LimitEntryOrder(
                    symbol=candidate.symbol,
                    signal_session=signal_session,
                    first_eligible_session=entry_session,
                    expiry_session=entry_session,
                    quantity=candidate.quantity,
                    limit_price=candidate.entry_price,
                    tick_size=candidate.vector.signal_tick_size,
                    maximum_participation=config.maximum_participation,
                ),
                stop_price=candidate.stop_price,
                target_price=candidate.target_price,
                max_holding_sessions=config.maximum_holding_sessions,
            )
            with localcontext() as context:
                context.prec = 28
                context.rounding = ROUND_HALF_EVEN
                cost_buffer = (
                    candidate.entry_price
                    * config.round_trip_cost_buffer_fraction
                )
                net_reward = (
                    candidate.target_price
                    - candidate.entry_price
                    - cost_buffer
                )
                net_risk = (
                    candidate.entry_price
                    - candidate.stop_price
                    + cost_buffer
                )
                planned_net_reward_risk = net_reward / net_risk
            intents.append(
                PromotedResearchTradeIntent(
                    decision_id=decision.decision_id,
                    source_cross_section_panel_id=source_panel.panel_id,
                    source_feature_id=candidate.vector.feature_id,
                    opportunity_id=candidate.opportunity.opportunity_id,
                    stable_instrument_id=(
                        candidate.opportunity.stable_instrument_id
                    ),
                    stable_listing_id=(
                        candidate.opportunity.stable_listing_id
                    ),
                    universe_snapshot_id=candidate.universe_snapshot_id,
                    evaluation_intent=evaluation_intent,
                    estimated_cost_buffer=cost_buffer,
                    planned_net_reward_risk=planned_net_reward_risk,
                )
            )
        intent_tuple = tuple(intents)
        values = {
            "schema_version": PROMOTED_INTENT_BATCH_SCHEMA_VERSION,
            "policy_version": PROMOTED_INTENT_POLICY_VERSION,
            "source_panel_id": source_panel.panel_id,
            "config_id": config.config_id,
            "signal_session": signal_session,
            "entry_session": entry_session,
            "initial_capital": initial_capital,
            "decisions": decisions,
            "intents": intent_tuple,
            "selected_count": len(intent_tuple),
            "blocked_count": len(decisions) - len(intent_tuple),
            "source_universe_complete": complete,
            "readiness": ReferenceReadiness.COLLECTION_ONLY,
            "actionable": False,
            "alert_eligible": False,
            "execution_eligible": False,
        }
        with localcontext() as context:
            context.prec = 28
            context.rounding = ROUND_HALF_EVEN
            batch_id = content_id(
                {
                    "schema": PROMOTED_INTENT_BATCH_SCHEMA_VERSION,
                    "policy_version": values["policy_version"],
                    "source_panel_id": values["source_panel_id"],
                    "config_id": values["config_id"],
                    "signal_session": values["signal_session"],
                    "entry_session": values["entry_session"],
                    "initial_capital": values["initial_capital"],
                    "decision_ids": tuple(
                        value.decision_id for value in decisions
                    ),
                    "research_intent_ids": tuple(
                        value.research_intent_id for value in intent_tuple
                    ),
                    "selected_count": values["selected_count"],
                    "blocked_count": values["blocked_count"],
                    "source_universe_complete": values[
                        "source_universe_complete"
                    ],
                    "readiness": values["readiness"],
                    "actionable": False,
                    "alert_eligible": False,
                    "execution_eligible": False,
                },
                length=64,
            )
        return VerifiedPromotedResearchIntentBatch(
            **values,
            batch_id=batch_id,
        )
