from __future__ import annotations

from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from decimal import Decimal, ROUND_FLOOR
from enum import Enum

from india_swing.identity import content_id
from india_swing.signals.opportunity_ranking import (
    SwingOpportunityRankingBatch,
    SwingRankedOpportunity,
)
from india_swing.signals.quote_gate import SwingQuoteGateOutcome


ZERO = Decimal("0")
ONE = Decimal("1")
PORTFOLIO_SCHEMA_VERSION = "swing-portfolio-snapshot/v1"
POLICY_SCHEMA_VERSION = "swing-portfolio-sizing-policy/v1"
STATE_SCHEMA_VERSION = "swing-capital-allocation-state/v1"
OUTCOME_SCHEMA_VERSION = "swing-portfolio-sizing-outcome/v1"
BATCH_SCHEMA_VERSION = "swing-portfolio-sizing-batch/v1"


class SwingPortfolioSizingError(ValueError):
    pass


class SwingSizingDisposition(str, Enum):
    SIZED = "SIZED"
    VETO = "VETO"


class SwingSizingReason(str, Enum):
    DAILY_LOSS_HALT = "DAILY_LOSS_HALT"
    PILOT_DRAWDOWN_HALT = "PILOT_DRAWDOWN_HALT"
    MAX_OPEN_POSITIONS_REACHED = "MAX_OPEN_POSITIONS_REACHED"
    MAX_NEW_POSITIONS_PER_RUN_REACHED = "MAX_NEW_POSITIONS_PER_RUN_REACHED"
    PER_TRADE_RISK_TOO_SMALL = "PER_TRADE_RISK_TOO_SMALL"
    TOTAL_OPEN_RISK_EXHAUSTED = "TOTAL_OPEN_RISK_EXHAUSTED"
    POSITION_NOTIONAL_CAP_TOO_SMALL = "POSITION_NOTIONAL_CAP_TOO_SMALL"
    GROSS_EXPOSURE_EXHAUSTED = "GROSS_EXPOSURE_EXHAUSTED"
    CASH_EXHAUSTED = "CASH_EXHAUSTED"
    LIQUIDITY_CAP_TOO_SMALL = "LIQUIDITY_CAP_TOO_SMALL"
    ASK_DEPTH_CAP_TOO_SMALL = "ASK_DEPTH_CAP_TOO_SMALL"
    NET_REWARD_RISK_BELOW_MINIMUM = "NET_REWARD_RISK_BELOW_MINIMUM"


def _decimal(value: Decimal, message: str) -> None:
    if type(value) is not Decimal or not value.is_finite():
        raise SwingPortfolioSizingError(message)


def _nonnegative_decimal(value: Decimal, message: str) -> None:
    _decimal(value, message)
    if value < ZERO:
        raise SwingPortfolioSizingError(message)


def _positive_decimal(value: Decimal, message: str) -> None:
    _decimal(value, message)
    if value <= ZERO:
        raise SwingPortfolioSizingError(message)


def _fraction(value: Decimal) -> None:
    _positive_decimal(value, "sizing fractions must be finite Decimals in (0, 1]")
    if value > ONE:
        raise SwingPortfolioSizingError(
            "sizing fractions must be finite Decimals in (0, 1]"
        )


def _aware_utc(value: datetime) -> datetime:
    if type(value) is not datetime or value.tzinfo is None:
        raise SwingPortfolioSizingError("portfolio as_of must be timezone-aware")
    try:
        offset = value.utcoffset()
    except Exception:
        raise SwingPortfolioSizingError("portfolio as_of has invalid timezone behavior") from None
    if offset is None:
        raise SwingPortfolioSizingError("portfolio as_of must be timezone-aware")
    return value.astimezone(timezone.utc)


def _floor_units(value: Decimal) -> int:
    if value <= ZERO:
        return 0
    return int(value.to_integral_value(rounding=ROUND_FLOOR))


@dataclass(frozen=True, slots=True)
class SwingPortfolioSnapshot:
    capital: Decimal
    cash_available: Decimal
    gross_exposure: Decimal
    open_risk: Decimal
    open_positions: int
    daily_realized_pnl: Decimal
    pilot_realized_pnl: Decimal
    as_of: datetime
    currency: str = "INR"
    schema_version: str = PORTFOLIO_SCHEMA_VERSION
    portfolio_snapshot_id: str = field(init=False)

    def __post_init__(self) -> None:
        _positive_decimal(self.capital, "portfolio capital must be positive")
        for value in (self.cash_available, self.gross_exposure, self.open_risk):
            _nonnegative_decimal(
                value,
                "portfolio cash, exposure, and open risk must be non-negative",
            )
        for value in (self.daily_realized_pnl, self.pilot_realized_pnl):
            _decimal(value, "portfolio realized P&L must be a finite Decimal")
        if type(self.open_positions) is not int or self.open_positions < 0:
            raise SwingPortfolioSizingError(
                "portfolio open_positions must be a non-negative integer"
            )
        if self.cash_available + self.gross_exposure > self.capital:
            raise SwingPortfolioSizingError(
                "portfolio cash and gross exposure exceed capital"
            )
        if self.open_risk > self.capital:
            raise SwingPortfolioSizingError("portfolio open risk exceeds capital")
        object.__setattr__(self, "as_of", _aware_utc(self.as_of))
        if self.currency != "INR":
            raise SwingPortfolioSizingError("portfolio currency must be INR")
        if self.schema_version != PORTFOLIO_SCHEMA_VERSION:
            raise SwingPortfolioSizingError("unsupported swing portfolio schema")
        object.__setattr__(
            self,
            "portfolio_snapshot_id",
            self._calculated_id(),
        )

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "portfolio_snapshot_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.portfolio_snapshot_id != self._calculated_id():
            raise SwingPortfolioSizingError("portfolio snapshot content identity failed")


@dataclass(frozen=True, slots=True)
class SwingPortfolioSizingPolicy:
    per_trade_risk_fraction: Decimal = Decimal("0.005")
    maximum_total_open_risk_fraction: Decimal = Decimal("0.02")
    maximum_position_notional_fraction: Decimal = Decimal("0.25")
    maximum_gross_exposure_fraction: Decimal = Decimal("0.80")
    maximum_daily_turnover_participation: Decimal = Decimal("0.0025")
    maximum_top_ask_participation: Decimal = Decimal("0.20")
    maximum_daily_loss_fraction: Decimal = Decimal("0.01")
    maximum_pilot_drawdown_fraction: Decimal = Decimal("0.02")
    minimum_net_reward_risk: Decimal = Decimal("2.50")
    maximum_open_positions: int = 4
    maximum_new_positions_per_run: int = 1
    policy_version: str = POLICY_SCHEMA_VERSION
    policy_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value in (
            self.per_trade_risk_fraction,
            self.maximum_total_open_risk_fraction,
            self.maximum_position_notional_fraction,
            self.maximum_gross_exposure_fraction,
            self.maximum_daily_turnover_participation,
            self.maximum_top_ask_participation,
            self.maximum_daily_loss_fraction,
            self.maximum_pilot_drawdown_fraction,
        ):
            _fraction(value)
        _positive_decimal(
            self.minimum_net_reward_risk,
            "minimum net reward/risk must be positive",
        )
        for value, name in (
            (self.maximum_open_positions, "maximum_open_positions"),
            (self.maximum_new_positions_per_run, "maximum_new_positions_per_run"),
        ):
            if type(value) is not int or value <= 0:
                raise SwingPortfolioSizingError(f"{name} must be a positive integer")
        if self.maximum_new_positions_per_run > self.maximum_open_positions:
            raise SwingPortfolioSizingError(
                "new positions per run cannot exceed maximum open positions"
            )
        if self.per_trade_risk_fraction > self.maximum_total_open_risk_fraction:
            raise SwingPortfolioSizingError(
                "per-trade risk cannot exceed total open-risk limit"
            )
        if self.maximum_position_notional_fraction > self.maximum_gross_exposure_fraction:
            raise SwingPortfolioSizingError(
                "position notional cannot exceed gross-exposure limit"
            )
        if self.maximum_daily_loss_fraction > self.maximum_pilot_drawdown_fraction:
            raise SwingPortfolioSizingError(
                "daily loss limit cannot exceed pilot drawdown limit"
            )
        if self.policy_version != POLICY_SCHEMA_VERSION:
            raise SwingPortfolioSizingError("unsupported portfolio sizing policy")
        object.__setattr__(self, "policy_id", self._calculated_id())

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
            raise SwingPortfolioSizingError("sizing policy content identity failed")


@dataclass(frozen=True, slots=True)
class SwingCapitalAllocationState:
    cash_available: Decimal
    gross_exposure: Decimal
    open_risk: Decimal
    open_positions: int
    schema_version: str = STATE_SCHEMA_VERSION
    state_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value in (self.cash_available, self.gross_exposure, self.open_risk):
            _nonnegative_decimal(value, "allocation state values must be non-negative")
        if type(self.open_positions) is not int or self.open_positions < 0:
            raise SwingPortfolioSizingError(
                "allocation state open_positions must be non-negative"
            )
        if self.schema_version != STATE_SCHEMA_VERSION:
            raise SwingPortfolioSizingError("unsupported allocation state schema")
        object.__setattr__(self, "state_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "state_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.state_id != self._calculated_id():
            raise SwingPortfolioSizingError("allocation state content identity failed")


def _initial_state(portfolio: SwingPortfolioSnapshot) -> SwingCapitalAllocationState:
    return SwingCapitalAllocationState(
        cash_available=portfolio.cash_available,
        gross_exposure=portfolio.gross_exposure,
        open_risk=portfolio.open_risk,
        open_positions=portfolio.open_positions,
    )


def _top_ask_quantity(outcome: SwingQuoteGateOutcome) -> int:
    best_ask = outcome.quote.best_ask
    if best_ask is None:
        raise SwingPortfolioSizingError("passing outcome has no best ask")
    for level in outcome.quote.depth_sell:
        if level.price == best_ask:
            return level.quantity
    raise SwingPortfolioSizingError("passing outcome has no bound best-ask depth")


def _evaluate_sizing(
    opportunity: SwingRankedOpportunity,
    portfolio: SwingPortfolioSnapshot,
    policy: SwingPortfolioSizingPolicy,
    state_before: SwingCapitalAllocationState,
) -> tuple[
    SwingSizingDisposition,
    tuple[str, ...],
    int,
    Decimal,
    Decimal,
    Decimal,
    SwingCapitalAllocationState,
]:
    outcome = opportunity.quote_gate_outcome
    levels = outcome.quote_adjusted_levels
    if levels is None:
        raise SwingPortfolioSizingError("ranked opportunity lacks quote-adjusted levels")
    cost_per_share = levels.cost_per_share
    loss_per_share = levels.entry_high - levels.stop + cost_per_share
    reward_per_share = levels.target - levels.entry_high - cost_per_share
    if loss_per_share <= ZERO or reward_per_share <= ZERO:
        raise SwingPortfolioSizingError("ranked opportunity has invalid risk/reward")
    net_reward_risk = reward_per_share / loss_per_share

    capital = portfolio.capital
    per_trade_budget = capital * policy.per_trade_risk_fraction
    maximum_open_risk = capital * policy.maximum_total_open_risk_fraction
    maximum_position_notional = capital * policy.maximum_position_notional_fraction
    maximum_gross_exposure = capital * policy.maximum_gross_exposure_fraction
    daily_loss_limit = capital * policy.maximum_daily_loss_fraction
    pilot_drawdown_limit = capital * policy.maximum_pilot_drawdown_fraction
    remaining_open_risk = max(ZERO, maximum_open_risk - state_before.open_risk)
    remaining_gross_exposure = max(
        ZERO,
        maximum_gross_exposure - state_before.gross_exposure,
    )
    historical_liquidity_notional = (
        outcome.proposal.metrics.median_traded_value
        * policy.maximum_daily_turnover_participation
    )
    ask_quantity_cap = _floor_units(
        Decimal(_top_ask_quantity(outcome)) * policy.maximum_top_ask_participation
    )

    risk_quantity = _floor_units(min(per_trade_budget, remaining_open_risk) / loss_per_share)
    position_quantity = _floor_units(maximum_position_notional / levels.entry_high)
    gross_quantity = _floor_units(remaining_gross_exposure / levels.entry_high)
    cash_quantity = _floor_units(
        state_before.cash_available / (levels.entry_high + cost_per_share)
    )
    liquidity_quantity = _floor_units(
        historical_liquidity_notional / levels.entry_high
    )
    quantity = min(
        risk_quantity,
        position_quantity,
        gross_quantity,
        cash_quantity,
        liquidity_quantity,
        ask_quantity_cap,
    )

    reasons: set[str] = set()
    if portfolio.daily_realized_pnl <= -daily_loss_limit:
        reasons.add(SwingSizingReason.DAILY_LOSS_HALT.value)
    if portfolio.pilot_realized_pnl <= -pilot_drawdown_limit:
        reasons.add(SwingSizingReason.PILOT_DRAWDOWN_HALT.value)
    if state_before.open_positions >= policy.maximum_open_positions:
        reasons.add(SwingSizingReason.MAX_OPEN_POSITIONS_REACHED.value)
    if (
        state_before.open_positions - portfolio.open_positions
        >= policy.maximum_new_positions_per_run
    ):
        reasons.add(SwingSizingReason.MAX_NEW_POSITIONS_PER_RUN_REACHED.value)
    if net_reward_risk < policy.minimum_net_reward_risk:
        reasons.add(SwingSizingReason.NET_REWARD_RISK_BELOW_MINIMUM.value)
    if _floor_units(per_trade_budget / loss_per_share) < 1:
        reasons.add(SwingSizingReason.PER_TRADE_RISK_TOO_SMALL.value)
    if _floor_units(remaining_open_risk / loss_per_share) < 1:
        reasons.add(SwingSizingReason.TOTAL_OPEN_RISK_EXHAUSTED.value)
    if position_quantity < 1:
        reasons.add(SwingSizingReason.POSITION_NOTIONAL_CAP_TOO_SMALL.value)
    if gross_quantity < 1:
        reasons.add(SwingSizingReason.GROSS_EXPOSURE_EXHAUSTED.value)
    if cash_quantity < 1:
        reasons.add(SwingSizingReason.CASH_EXHAUSTED.value)
    if liquidity_quantity < 1:
        reasons.add(SwingSizingReason.LIQUIDITY_CAP_TOO_SMALL.value)
    if ask_quantity_cap < 1:
        reasons.add(SwingSizingReason.ASK_DEPTH_CAP_TOO_SMALL.value)

    if reasons or quantity < 1:
        if quantity < 1 and not reasons:
            raise SwingPortfolioSizingError("sizing produced an unexplained zero quantity")
        return (
            SwingSizingDisposition.VETO,
            tuple(sorted(reasons)),
            0,
            ZERO,
            ZERO,
            ZERO,
            state_before,
        )

    entry_notional = levels.entry_high * quantity
    estimated_cost = cost_per_share * quantity
    planned_max_loss = loss_per_share * quantity
    state_after = SwingCapitalAllocationState(
        cash_available=state_before.cash_available - entry_notional - estimated_cost,
        gross_exposure=state_before.gross_exposure + entry_notional,
        open_risk=state_before.open_risk + planned_max_loss,
        open_positions=state_before.open_positions + 1,
    )
    return (
        SwingSizingDisposition.SIZED,
        (),
        quantity,
        entry_notional,
        estimated_cost,
        planned_max_loss,
        state_after,
    )


@dataclass(frozen=True, slots=True)
class SwingPortfolioSizingOutcome:
    opportunity: SwingRankedOpportunity
    portfolio: SwingPortfolioSnapshot
    policy: SwingPortfolioSizingPolicy
    state_before: SwingCapitalAllocationState
    disposition: SwingSizingDisposition
    reason_codes: tuple[str, ...]
    quantity: int
    entry_notional: Decimal
    estimated_round_trip_cost: Decimal
    planned_max_loss: Decimal
    state_after: SwingCapitalAllocationState
    schema_version: str = OUTCOME_SCHEMA_VERSION
    sizing_outcome_id: str = field(init=False)

    def __post_init__(self) -> None:
        if self.schema_version != OUTCOME_SCHEMA_VERSION:
            raise SwingPortfolioSizingError("unsupported sizing outcome schema")
        self._verify()
        object.__setattr__(self, "sizing_outcome_id", self._calculated_id())

    def _verify(self) -> None:
        if type(self.opportunity) is not SwingRankedOpportunity:
            raise SwingPortfolioSizingError("opportunity must be exact")
        self.opportunity.verify_content_identity()
        if type(self.portfolio) is not SwingPortfolioSnapshot:
            raise SwingPortfolioSizingError("portfolio must be exact")
        self.portfolio.verify_content_identity()
        if type(self.policy) is not SwingPortfolioSizingPolicy:
            raise SwingPortfolioSizingError("sizing policy must be exact")
        self.policy.verify_content_identity()
        if type(self.state_before) is not SwingCapitalAllocationState:
            raise SwingPortfolioSizingError("state_before must be exact")
        if type(self.state_after) is not SwingCapitalAllocationState:
            raise SwingPortfolioSizingError("state_after must be exact")
        self.state_before.verify_content_identity()
        self.state_after.verify_content_identity()
        if type(self.disposition) is not SwingSizingDisposition:
            raise SwingPortfolioSizingError("sizing disposition must be exact")
        for value in (
            self.entry_notional,
            self.estimated_round_trip_cost,
            self.planned_max_loss,
        ):
            _nonnegative_decimal(value, "sizing amounts must be non-negative Decimals")
        replayed = _evaluate_sizing(
            self.opportunity,
            self.portfolio,
            self.policy,
            self.state_before,
        )
        (
            disposition,
            reasons,
            quantity,
            notional,
            cost,
            loss,
            state_after,
        ) = replayed
        if disposition is not self.disposition:
            raise SwingPortfolioSizingError("sizing disposition does not replay")
        if type(self.reason_codes) is not tuple or self.reason_codes != reasons:
            raise SwingPortfolioSizingError("sizing reasons do not replay")
        if self.reason_codes != tuple(sorted(set(self.reason_codes))):
            raise SwingPortfolioSizingError("sizing reasons must be sorted and unique")
        if type(self.quantity) is not int or self.quantity != quantity:
            raise SwingPortfolioSizingError("sizing quantity does not replay")
        for actual, expected in (
            (self.entry_notional, notional),
            (self.estimated_round_trip_cost, cost),
            (self.planned_max_loss, loss),
        ):
            if actual != expected:
                raise SwingPortfolioSizingError("sizing amounts do not replay")
        if self.state_after.state_id != state_after.state_id:
            raise SwingPortfolioSizingError("allocation state does not replay")
        if self.disposition is SwingSizingDisposition.SIZED:
            if self.reason_codes or self.quantity <= 0:
                raise SwingPortfolioSizingError("sized outcome is inconsistent")
        elif not self.reason_codes or self.quantity != 0:
            raise SwingPortfolioSizingError("vetoed sizing outcome is inconsistent")

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "sizing_outcome_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._verify()
        if self.sizing_outcome_id != self._calculated_id():
            raise SwingPortfolioSizingError("sizing outcome content identity failed")

    @property
    def sized(self) -> bool:
        return self.disposition is SwingSizingDisposition.SIZED

    @property
    def symbol(self) -> str:
        return self.opportunity.symbol

    @property
    def research_only(self) -> bool:
        return True

    @property
    def execution_eligible(self) -> bool:
        return False


@dataclass(frozen=True, slots=True)
class SwingPortfolioSizingBatch:
    ranking_batch: SwingOpportunityRankingBatch
    portfolio: SwingPortfolioSnapshot
    policy: SwingPortfolioSizingPolicy
    outcomes: tuple[SwingPortfolioSizingOutcome, ...]
    upstream_vetoes: tuple[SwingQuoteGateOutcome, ...]
    sized_subject_count: int
    vetoed_subject_count: int
    final_state: SwingCapitalAllocationState
    schema_version: str = BATCH_SCHEMA_VERSION
    sizing_batch_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value in (self.sized_subject_count, self.vetoed_subject_count):
            if type(value) is not int or value < 0:
                raise SwingPortfolioSizingError(
                    "sizing subject counts must be non-negative integers"
                )
        if self.schema_version != BATCH_SCHEMA_VERSION:
            raise SwingPortfolioSizingError("unsupported portfolio sizing batch schema")
        self._verify_coverage()
        object.__setattr__(self, "sizing_batch_id", self._calculated_id())

    def _verify_coverage(self) -> None:
        if type(self.ranking_batch) is not SwingOpportunityRankingBatch:
            raise SwingPortfolioSizingError("ranking_batch must be exact")
        self.ranking_batch.verify_content_identity()
        if type(self.portfolio) is not SwingPortfolioSnapshot:
            raise SwingPortfolioSizingError("portfolio must be exact")
        self.portfolio.verify_content_identity()
        if type(self.policy) is not SwingPortfolioSizingPolicy:
            raise SwingPortfolioSizingError("sizing policy must be exact")
        self.policy.verify_content_identity()
        if type(self.outcomes) is not tuple or any(
            type(value) is not SwingPortfolioSizingOutcome for value in self.outcomes
        ):
            raise SwingPortfolioSizingError("sizing outcomes must be an exact tuple")
        if type(self.upstream_vetoes) is not tuple or any(
            type(value) is not SwingQuoteGateOutcome for value in self.upstream_vetoes
        ):
            raise SwingPortfolioSizingError("upstream vetoes must be an exact tuple")
        if self.upstream_vetoes != self.ranking_batch.vetoed_outcomes:
            raise SwingPortfolioSizingError("upstream vetoes were not preserved exactly")
        if len(self.outcomes) != len(self.ranking_batch.ranked_opportunities):
            raise SwingPortfolioSizingError(
                "sizing outcomes do not exactly cover ranked opportunities"
            )

        expected_state = _initial_state(self.portfolio)
        for outcome, opportunity in zip(
            self.outcomes,
            self.ranking_batch.ranked_opportunities,
            strict=True,
        ):
            outcome.verify_content_identity()
            if outcome.opportunity.opportunity_id != opportunity.opportunity_id:
                raise SwingPortfolioSizingError(
                    "sizing outcome is not bound to its ranked opportunity"
                )
            if outcome.portfolio.portfolio_snapshot_id != self.portfolio.portfolio_snapshot_id:
                raise SwingPortfolioSizingError("sizing outcome portfolio differs")
            if outcome.policy.policy_id != self.policy.policy_id:
                raise SwingPortfolioSizingError("sizing outcome policy differs")
            if outcome.state_before.state_id != expected_state.state_id:
                raise SwingPortfolioSizingError("allocation state chain is broken")
            expected_state = outcome.state_after
        if type(self.final_state) is not SwingCapitalAllocationState:
            raise SwingPortfolioSizingError("final_state must be exact")
        self.final_state.verify_content_identity()
        if self.final_state.state_id != expected_state.state_id:
            raise SwingPortfolioSizingError("final allocation state does not replay")

        sized = sum(
            1 for value in self.outcomes if value.disposition is SwingSizingDisposition.SIZED
        )
        vetoed = len(self.outcomes) - sized
        if self.sized_subject_count != sized or self.vetoed_subject_count != vetoed:
            raise SwingPortfolioSizingError("sizing subject counts are inconsistent")
        maximum_open_risk = (
            self.portfolio.capital * self.policy.maximum_total_open_risk_fraction
        )
        if self.final_state.open_risk > max(
            self.portfolio.open_risk,
            maximum_open_risk,
        ):
            raise SwingPortfolioSizingError("final open risk exceeds the policy limit")
        maximum_gross_exposure = (
            self.portfolio.capital * self.policy.maximum_gross_exposure_fraction
        )
        if self.final_state.gross_exposure > max(
            self.portfolio.gross_exposure,
            maximum_gross_exposure,
        ):
            raise SwingPortfolioSizingError("final gross exposure exceeds the policy limit")

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
                if item.name != "sizing_batch_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._verify_coverage()
        if self.sizing_batch_id != self._calculated_id():
            raise SwingPortfolioSizingError("sizing batch content identity failed")


def assemble_swing_portfolio_sizing_batch(
    *,
    ranking_batch: SwingOpportunityRankingBatch,
    portfolio: SwingPortfolioSnapshot,
    policy: SwingPortfolioSizingPolicy | None = None,
) -> SwingPortfolioSizingBatch:
    """Allocate ranked research opportunities under exact INR pilot limits."""

    if type(ranking_batch) is not SwingOpportunityRankingBatch:
        raise SwingPortfolioSizingError("ranking_batch must be exact")
    ranking_batch.verify_content_identity()
    if type(portfolio) is not SwingPortfolioSnapshot:
        raise SwingPortfolioSizingError("portfolio must be exact")
    portfolio.verify_content_identity()
    if portfolio.as_of > ranking_batch.quote_gate_batch.evaluated_at:
        raise SwingPortfolioSizingError("portfolio snapshot is future-known")
    if policy is None:
        policy = SwingPortfolioSizingPolicy()
    if type(policy) is not SwingPortfolioSizingPolicy:
        raise SwingPortfolioSizingError("sizing policy must be exact")
    policy.verify_content_identity()

    state = _initial_state(portfolio)
    outcomes: list[SwingPortfolioSizingOutcome] = []
    for opportunity in ranking_batch.ranked_opportunities:
        replayed = _evaluate_sizing(opportunity, portfolio, policy, state)
        disposition, reasons, quantity, notional, cost, loss, state_after = replayed
        outcome = SwingPortfolioSizingOutcome(
            opportunity=opportunity,
            portfolio=portfolio,
            policy=policy,
            state_before=state,
            disposition=disposition,
            reason_codes=reasons,
            quantity=quantity,
            entry_notional=notional,
            estimated_round_trip_cost=cost,
            planned_max_loss=loss,
            state_after=state_after,
        )
        outcomes.append(outcome)
        state = state_after

    sized = sum(
        1 for value in outcomes if value.disposition is SwingSizingDisposition.SIZED
    )
    return SwingPortfolioSizingBatch(
        ranking_batch=ranking_batch,
        portfolio=portfolio,
        policy=policy,
        outcomes=tuple(outcomes),
        upstream_vetoes=ranking_batch.vetoed_outcomes,
        sized_subject_count=sized,
        vetoed_subject_count=len(outcomes) - sized,
        final_state=state,
    )
