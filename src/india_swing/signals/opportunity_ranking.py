from __future__ import annotations

from dataclasses import dataclass, field, fields
from decimal import Decimal
from enum import Enum

from india_swing.identity import content_id

from .quote_gate import (
    SwingQuoteGateBatch,
    SwingQuoteGateDisposition,
    SwingQuoteGateOutcome,
)


ZERO = Decimal("0")
ONE = Decimal("1")
POLICY_SCHEMA_VERSION = "swing-opportunity-ranking-policy/v1"
COMPONENT_SCHEMA_VERSION = "swing-ranking-component/v1"
OPPORTUNITY_SCHEMA_VERSION = "swing-ranked-opportunity/v1"
BATCH_SCHEMA_VERSION = "swing-opportunity-ranking-batch/v1"


class SwingOpportunityRankingError(ValueError):
    pass


class SwingRankingFactor(str, Enum):
    RELATIVE_STRENGTH = "RELATIVE_STRENGTH"
    TREND_QUALITY = "TREND_QUALITY"
    VOLUME_CONFIRMATION = "VOLUME_CONFIRMATION"
    LIQUIDITY_QUALITY = "LIQUIDITY_QUALITY"
    SPREAD_QUALITY = "SPREAD_QUALITY"
    ENTRY_QUALITY = "ENTRY_QUALITY"


def _finite_decimal(value: Decimal, message: str) -> None:
    if type(value) is not Decimal or not value.is_finite():
        raise SwingOpportunityRankingError(message)


def _unit_decimal(value: Decimal, message: str) -> None:
    _finite_decimal(value, message)
    if value < ZERO or value > ONE:
        raise SwingOpportunityRankingError(message)


def _clamp_unit(value: Decimal) -> Decimal:
    return max(ZERO, min(value, ONE))


@dataclass(frozen=True, slots=True)
class SwingOpportunityRankingPolicy:
    """Explicit provisional research weights; the resulting score is not confidence."""

    relative_strength_weight: Decimal = Decimal("0.25")
    trend_quality_weight: Decimal = Decimal("0.25")
    volume_confirmation_weight: Decimal = Decimal("0.15")
    liquidity_quality_weight: Decimal = Decimal("0.15")
    spread_quality_weight: Decimal = Decimal("0.10")
    entry_quality_weight: Decimal = Decimal("0.10")
    policy_version: str = POLICY_SCHEMA_VERSION
    policy_id: str = field(init=False)

    def __post_init__(self) -> None:
        weights = self.weights
        for _, value in weights:
            _unit_decimal(value, "ranking weights must be finite Decimals in [0, 1]")
        if sum((value for _, value in weights), ZERO) != ONE:
            raise SwingOpportunityRankingError("ranking weights must sum exactly to one")
        if self.policy_version != POLICY_SCHEMA_VERSION:
            raise SwingOpportunityRankingError("unsupported opportunity ranking policy")
        object.__setattr__(self, "policy_id", self._calculated_id())

    @property
    def weights(self) -> tuple[tuple[SwingRankingFactor, Decimal], ...]:
        return (
            (SwingRankingFactor.RELATIVE_STRENGTH, self.relative_strength_weight),
            (SwingRankingFactor.TREND_QUALITY, self.trend_quality_weight),
            (SwingRankingFactor.VOLUME_CONFIRMATION, self.volume_confirmation_weight),
            (SwingRankingFactor.LIQUIDITY_QUALITY, self.liquidity_quality_weight),
            (SwingRankingFactor.SPREAD_QUALITY, self.spread_quality_weight),
            (SwingRankingFactor.ENTRY_QUALITY, self.entry_quality_weight),
        )

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "policy_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.policy_id != self._calculated_id():
            raise SwingOpportunityRankingError("ranking policy content identity failed")


@dataclass(frozen=True, slots=True)
class SwingRankingComponent:
    factor: SwingRankingFactor
    raw_value: Decimal
    weight: Decimal
    contribution: Decimal
    schema_version: str = COMPONENT_SCHEMA_VERSION
    component_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.factor) is not SwingRankingFactor:
            raise SwingOpportunityRankingError("ranking factor must be exact")
        _unit_decimal(self.raw_value, "ranking raw value must be in [0, 1]")
        _unit_decimal(self.weight, "ranking component weight must be in [0, 1]")
        _finite_decimal(
            self.contribution,
            "ranking contribution must be a finite Decimal",
        )
        if self.contribution != self.raw_value * self.weight:
            raise SwingOpportunityRankingError("ranking contribution does not replay")
        if self.schema_version != COMPONENT_SCHEMA_VERSION:
            raise SwingOpportunityRankingError("unsupported ranking component schema")
        object.__setattr__(self, "component_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "component_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.component_id != self._calculated_id():
            raise SwingOpportunityRankingError("ranking component content identity failed")


def _calculate_components(
    outcome: SwingQuoteGateOutcome,
    policy: SwingOpportunityRankingPolicy,
) -> tuple[SwingRankingComponent, ...]:
    if type(outcome) is not SwingQuoteGateOutcome:
        raise SwingOpportunityRankingError("quote gate outcome must be exact")
    outcome.verify_content_identity()
    if outcome.disposition is not SwingQuoteGateDisposition.PASS:
        raise SwingOpportunityRankingError("only quote-gate PASS outcomes can be ranked")
    if type(policy) is not SwingOpportunityRankingPolicy:
        raise SwingOpportunityRankingError("ranking policy must be exact")
    policy.verify_content_identity()
    if outcome.observed_spread_bps is None or outcome.quote.best_ask is None:
        raise SwingOpportunityRankingError("passing quote outcome lacks ranking inputs")

    metrics = outcome.proposal.metrics
    gate_max_spread = outcome.policy.maximum_spread_bps
    spread_quality = _clamp_unit(ONE - outcome.observed_spread_bps / gate_max_spread)
    levels = outcome.proposal.levels
    entry_width = levels.entry_high - levels.entry_low
    if entry_width <= ZERO:
        raise SwingOpportunityRankingError("proposal entry range is not rankable")
    entry_quality = _clamp_unit(
        (levels.entry_high - outcome.quote.best_ask) / entry_width
    )
    raw_values = {
        SwingRankingFactor.RELATIVE_STRENGTH: metrics.relative_strength,
        SwingRankingFactor.TREND_QUALITY: metrics.trend_quality,
        SwingRankingFactor.VOLUME_CONFIRMATION: metrics.volume_confirmation,
        SwingRankingFactor.LIQUIDITY_QUALITY: metrics.liquidity_quality,
        SwingRankingFactor.SPREAD_QUALITY: spread_quality,
        SwingRankingFactor.ENTRY_QUALITY: entry_quality,
    }
    return tuple(
        SwingRankingComponent(
            factor=factor,
            raw_value=raw_values[factor],
            weight=weight,
            contribution=raw_values[factor] * weight,
        )
        for factor, weight in policy.weights
    )


@dataclass(frozen=True, slots=True)
class SwingRankedOpportunity:
    quote_gate_outcome: SwingQuoteGateOutcome
    policy: SwingOpportunityRankingPolicy
    rank: int
    components: tuple[SwingRankingComponent, ...]
    ranking_score: Decimal
    schema_version: str = OPPORTUNITY_SCHEMA_VERSION
    opportunity_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.rank) is not int or self.rank <= 0:
            raise SwingOpportunityRankingError("opportunity rank must be a positive integer")
        if self.schema_version != OPPORTUNITY_SCHEMA_VERSION:
            raise SwingOpportunityRankingError("unsupported ranked opportunity schema")
        self._verify_score()
        object.__setattr__(self, "opportunity_id", self._calculated_id())

    def _verify_score(self) -> None:
        if type(self.quote_gate_outcome) is not SwingQuoteGateOutcome:
            raise SwingOpportunityRankingError("quote_gate_outcome must be exact")
        self.quote_gate_outcome.verify_content_identity()
        if self.quote_gate_outcome.disposition is not SwingQuoteGateDisposition.PASS:
            raise SwingOpportunityRankingError("ranked opportunity must bind a PASS outcome")
        if type(self.policy) is not SwingOpportunityRankingPolicy:
            raise SwingOpportunityRankingError("ranking policy must be exact")
        self.policy.verify_content_identity()
        if type(self.components) is not tuple or any(
            type(value) is not SwingRankingComponent for value in self.components
        ):
            raise SwingOpportunityRankingError("ranking components must be an exact tuple")
        for value in self.components:
            value.verify_content_identity()
        replayed = _calculate_components(self.quote_gate_outcome, self.policy)
        if tuple(value.component_id for value in self.components) != tuple(
            value.component_id for value in replayed
        ):
            raise SwingOpportunityRankingError("ranking components do not replay")
        _unit_decimal(self.ranking_score, "ranking score must be in [0, 1]")
        if self.ranking_score != sum(
            (value.contribution for value in self.components), ZERO
        ):
            raise SwingOpportunityRankingError("ranking score does not replay")

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "opportunity_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._verify_score()
        if self.opportunity_id != self._calculated_id():
            raise SwingOpportunityRankingError("ranked opportunity content identity failed")

    @property
    def symbol(self) -> str:
        return self.quote_gate_outcome.proposal.symbol

    @property
    def research_only(self) -> bool:
        return True

    @property
    def execution_eligible(self) -> bool:
        return False


def _sort_key(
    score: Decimal,
    outcome: SwingQuoteGateOutcome,
) -> tuple[Decimal, Decimal, str, str]:
    if outcome.observed_spread_bps is None:
        raise SwingOpportunityRankingError("passing outcome lacks observed spread")
    proposal = outcome.proposal
    return (
        -score,
        outcome.observed_spread_bps,
        proposal.assembly.stable_instrument_id,
        proposal.assembly.stable_listing_id,
    )


@dataclass(frozen=True, slots=True)
class SwingOpportunityRankingBatch:
    quote_gate_batch: SwingQuoteGateBatch
    policy: SwingOpportunityRankingPolicy
    ranked_opportunities: tuple[SwingRankedOpportunity, ...]
    vetoed_outcomes: tuple[SwingQuoteGateOutcome, ...]
    ranked_subject_count: int
    vetoed_subject_count: int
    schema_version: str = BATCH_SCHEMA_VERSION
    ranking_batch_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value in (self.ranked_subject_count, self.vetoed_subject_count):
            if type(value) is not int or value < 0:
                raise SwingOpportunityRankingError(
                    "ranking subject counts must be non-negative integers"
                )
        if self.schema_version != BATCH_SCHEMA_VERSION:
            raise SwingOpportunityRankingError("unsupported opportunity ranking batch schema")
        self._verify_coverage()
        object.__setattr__(self, "ranking_batch_id", self._calculated_id())

    def _verify_coverage(self) -> None:
        if type(self.quote_gate_batch) is not SwingQuoteGateBatch:
            raise SwingOpportunityRankingError("quote_gate_batch must be exact")
        self.quote_gate_batch.verify_content_identity()
        if type(self.policy) is not SwingOpportunityRankingPolicy:
            raise SwingOpportunityRankingError("ranking policy must be exact")
        self.policy.verify_content_identity()
        if type(self.ranked_opportunities) is not tuple or any(
            type(value) is not SwingRankedOpportunity
            for value in self.ranked_opportunities
        ):
            raise SwingOpportunityRankingError(
                "ranked_opportunities must be an exact tuple"
            )
        if type(self.vetoed_outcomes) is not tuple or any(
            type(value) is not SwingQuoteGateOutcome for value in self.vetoed_outcomes
        ):
            raise SwingOpportunityRankingError("vetoed_outcomes must be an exact tuple")

        passed = tuple(
            value
            for value in self.quote_gate_batch.outcomes
            if value.disposition is SwingQuoteGateDisposition.PASS
        )
        vetoed = tuple(
            value
            for value in self.quote_gate_batch.outcomes
            if value.disposition is SwingQuoteGateDisposition.VETO
        )
        for value in self.ranked_opportunities:
            value.verify_content_identity()
            if value.policy.policy_id != self.policy.policy_id:
                raise SwingOpportunityRankingError(
                    "ranked opportunity policy differs from the batch"
                )
        for value in self.vetoed_outcomes:
            value.verify_content_identity()
        if self.vetoed_outcomes != vetoed:
            raise SwingOpportunityRankingError("quote-gate vetoes were not preserved exactly")

        ranked_ids = tuple(
            value.quote_gate_outcome.outcome_id for value in self.ranked_opportunities
        )
        passed_ids = tuple(value.outcome_id for value in passed)
        if len(set(ranked_ids)) != len(ranked_ids) or set(ranked_ids) != set(passed_ids):
            raise SwingOpportunityRankingError(
                "ranked opportunities do not exactly cover passing outcomes"
            )
        if tuple(value.rank for value in self.ranked_opportunities) != tuple(
            range(1, len(self.ranked_opportunities) + 1)
        ):
            raise SwingOpportunityRankingError("opportunity ranks are not contiguous")
        expected_order = tuple(
            sorted(
                self.ranked_opportunities,
                key=lambda value: _sort_key(
                    value.ranking_score,
                    value.quote_gate_outcome,
                ),
            )
        )
        if self.ranked_opportunities != expected_order:
            raise SwingOpportunityRankingError("ranked opportunities are out of order")
        if (
            self.ranked_subject_count != len(self.ranked_opportunities)
            or self.vetoed_subject_count != len(self.vetoed_outcomes)
            or self.ranked_subject_count + self.vetoed_subject_count
            != len(self.quote_gate_batch.outcomes)
        ):
            raise SwingOpportunityRankingError("ranking subject counts are inconsistent")

    @property
    def research_only(self) -> bool:
        return True

    @property
    def execution_eligible(self) -> bool:
        return False

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "ranking_batch_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._verify_coverage()
        if self.ranking_batch_id != self._calculated_id():
            raise SwingOpportunityRankingError("ranking batch content identity failed")


def assemble_swing_opportunity_ranking_batch(
    *,
    quote_gate_batch: SwingQuoteGateBatch,
    policy: SwingOpportunityRankingPolicy | None = None,
) -> SwingOpportunityRankingBatch:
    """Rank every quote-gate PASS without granting confidence or trade authority."""

    if type(quote_gate_batch) is not SwingQuoteGateBatch:
        raise SwingOpportunityRankingError("quote_gate_batch must be exact")
    quote_gate_batch.verify_content_identity()
    if policy is None:
        policy = SwingOpportunityRankingPolicy()
    if type(policy) is not SwingOpportunityRankingPolicy:
        raise SwingOpportunityRankingError("ranking policy must be exact")
    policy.verify_content_identity()

    scored: list[
        tuple[Decimal, SwingQuoteGateOutcome, tuple[SwingRankingComponent, ...]]
    ] = []
    vetoed: list[SwingQuoteGateOutcome] = []
    for outcome in quote_gate_batch.outcomes:
        if outcome.disposition is SwingQuoteGateDisposition.VETO:
            vetoed.append(outcome)
            continue
        components = _calculate_components(outcome, policy)
        score = sum((value.contribution for value in components), ZERO)
        scored.append((score, outcome, components))

    scored.sort(key=lambda value: _sort_key(value[0], value[1]))
    ranked = tuple(
        SwingRankedOpportunity(
            quote_gate_outcome=outcome,
            policy=policy,
            rank=index,
            components=components,
            ranking_score=score,
        )
        for index, (score, outcome, components) in enumerate(scored, start=1)
    )
    return SwingOpportunityRankingBatch(
        quote_gate_batch=quote_gate_batch,
        policy=policy,
        ranked_opportunities=ranked,
        vetoed_outcomes=tuple(vetoed),
        ranked_subject_count=len(ranked),
        vetoed_subject_count=len(vetoed),
    )
