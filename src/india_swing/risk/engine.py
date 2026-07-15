from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_FLOOR
from typing import Any

from india_swing.domain.models import (
    Candidate,
    DecisionAction,
    PortfolioState,
    ResearchAssessment,
    ResearchVerdict,
    ProbabilityStatus,
    RiskPolicy,
    TradeDecision,
)
from india_swing.identity import content_id


ZERO = Decimal("0")


def floor_units(value: Decimal) -> int:
    if value <= ZERO:
        return 0
    return int(value.to_integral_value(rounding=ROUND_FLOOR))


@dataclass(frozen=True, slots=True)
class RiskEvaluation:
    approved: bool
    decision: TradeDecision | None
    reasons: tuple[str, ...]


class RiskEngine:
    def __init__(self, policy: RiskPolicy) -> None:
        self.policy = policy

    def evaluate(
        self,
        candidate: Candidate,
        research: ResearchAssessment,
        portfolio: PortfolioState,
        rank: int,
        *,
        identity_context: object | None = None,
        reference_readiness: str = "UNBOUND",
        execution_eligible: bool = False,
    ) -> RiskEvaluation:
        setup = candidate.setup
        instrument = candidate.instrument
        reasons: list[str] = []

        if research.verdict is not ResearchVerdict.APPROVE:
            reasons.append(f"research verdict is {research.verdict.value}")
        if setup.earliest_entry_at <= setup.decision_time:
            reasons.append("entry is not strictly after the decision time")
        if portfolio.open_risk >= self.policy.max_open_risk:
            reasons.append("portfolio open-risk limit is already exhausted")
        if portfolio.open_positions >= self.policy.max_open_positions:
            reasons.append("maximum number of open positions is already reached")
        if portfolio.daily_realized_pnl <= -self.policy.max_daily_loss:
            reasons.append("daily loss halt is active")
        if portfolio.pilot_realized_pnl <= -self.policy.max_pilot_drawdown:
            reasons.append("pilot drawdown halt is active")
        if setup.entry_expires_at is None:
            reasons.append("entry validity window is missing")
        if (
            self.policy.require_validated_probabilities
            and setup.probability_status is not ProbabilityStatus.VALIDATED
        ):
            reasons.append("probability estimate is not validated")
        if (
            self.policy.require_validated_probabilities
            and setup.calibration_sample_size < self.policy.min_calibration_sample_size
        ):
            reasons.append("probability calibration sample is too small")

        cost_bps = max(
            self.policy.estimated_round_trip_cost_bps,
            candidate.signals.estimated_cost_bps,
        )
        cost_per_share = setup.entry_high * cost_bps / Decimal("10000")
        net_loss_per_share = setup.entry_high - setup.stop + cost_per_share
        net_reward_per_share = setup.target - setup.entry_high - cost_per_share

        if net_loss_per_share <= ZERO or net_reward_per_share <= ZERO:
            reasons.append("setup has non-positive net risk or reward")
            net_reward_risk = ZERO
        else:
            net_reward_risk = net_reward_per_share / net_loss_per_share
            if net_reward_risk < self.policy.min_net_reward_risk:
                reasons.append("net reward-to-risk is below the policy minimum")

        time_probability = Decimal("1") - setup.target_probability - setup.stop_probability
        expected_r = (
            setup.target_probability * net_reward_risk
            - setup.stop_probability
            + time_probability * setup.expected_time_exit_r
        )
        if expected_r < self.policy.min_expected_r:
            reasons.append("cost-adjusted expected R is below the policy minimum")

        remaining_open_risk = self.policy.max_open_risk - portfolio.open_risk
        remaining_exposure = self.policy.max_gross_exposure - portfolio.gross_exposure
        remaining_cash = portfolio.capital - portfolio.gross_exposure
        liquidity_notional = (
            instrument.median_daily_traded_value * self.policy.max_turnover_participation
        )
        notional_cap = min(
            self.policy.max_position_notional,
            remaining_exposure,
            remaining_cash,
            liquidity_notional,
        )
        risk_budget = min(self.policy.per_trade_risk, remaining_open_risk)
        quantity = min(
            floor_units(risk_budget / net_loss_per_share) if net_loss_per_share > ZERO else 0,
            floor_units(notional_cap / setup.entry_high) if setup.entry_high > ZERO else 0,
        )
        if quantity < 1:
            reasons.append("risk, exposure, or liquidity caps produce zero quantity")

        if reasons:
            return RiskEvaluation(False, None, tuple(reasons))

        planned_max_loss = net_loss_per_share * quantity
        estimated_cost = cost_per_share * quantity
        decision_material: dict[str, Any] = dict(
            action=DecisionAction.BUY,
            decision_time=setup.decision_time,
            symbol=instrument.symbol,
            quantity=quantity,
            entry_low=setup.entry_low,
            entry_high=setup.entry_high,
            stop=setup.stop,
            target=setup.target,
            planned_max_loss=planned_max_loss,
            estimated_cost=estimated_cost,
            net_reward_risk=net_reward_risk,
            expected_r=expected_r,
            reasons=(setup.setup_reason, setup.stop_reason, setup.target_reason),
            thesis=research.thesis,
            bear_case=research.bear_case,
            cancel_conditions=setup.cancel_conditions,
            metadata=(
                ("rank", str(rank)),
                ("instrument_id", instrument.instrument_id),
                ("listing_id", instrument.listing_id),
                ("instrument_fingerprint", instrument.content_fingerprint),
                ("universe_snapshot_id", instrument.universe_snapshot_id),
                ("risk_policy", self.policy.policy_version),
                ("forecast_model", candidate.forecast.model_version),
                ("research_model", research.model_version),
            ),
            target_probability=setup.target_probability,
            stop_probability=setup.stop_probability,
            probability_status=setup.probability_status,
            calibration_sample_size=setup.calibration_sample_size,
            earliest_entry_at=setup.earliest_entry_at,
            entry_expires_at=setup.entry_expires_at,
            max_holding_sessions=setup.max_holding_sessions,
            order_type="LIMIT",
            reference_readiness=reference_readiness,
            execution_eligible=execution_eligible,
        )
        signal_id = content_id(
            {
                "identity_schema": "trade-signal-v3",
                "pipeline_context": identity_context,
                "candidate": candidate,
                "research": research,
                "portfolio": portfolio,
                "risk_policy": self.policy,
                "rank": rank,
                "final_decision": decision_material,
            }
        )
        decision = TradeDecision(signal_id=signal_id, **decision_material)
        return RiskEvaluation(True, decision, ())
