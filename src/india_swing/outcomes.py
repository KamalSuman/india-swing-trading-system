from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum

from india_swing.domain.models import DecisionAction, TradeDecision, require_aware


ZERO = Decimal("0")


class ExitReason(str, Enum):
    TARGET = "TARGET"
    STOP = "STOP"
    TIME_EXIT = "TIME_EXIT"
    MANUAL = "MANUAL"
    GAP = "GAP"


class ReviewClassification(str, Enum):
    PROFITABLE_OUTCOME = "PROFITABLE_OUTCOME"
    DATA_FAILURE = "DATA_FAILURE"
    EXECUTION_DEVIATION = "EXECUTION_DEVIATION"
    TAIL_OR_GAP_LOSS = "TAIL_OR_GAP_LOSS"
    POST_ENTRY_NEWS_SHOCK = "POST_ENTRY_NEWS_SHOCK"
    MARKET_REGIME_MOVE = "MARKET_REGIME_MOVE"
    SECTOR_MOVE = "SECTOR_MOVE"
    STOP_PLACEMENT_REVIEW = "STOP_PLACEMENT_REVIEW"
    UNRESOLVED_FORECAST_MISS = "UNRESOLVED_FORECAST_MISS"


class Preventability(str, Enum):
    YES = "YES"
    PARTIAL = "PARTIAL"
    NO = "NO"
    UNRESOLVED = "UNRESOLVED"


class ReviewConfidence(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


@dataclass(frozen=True, slots=True)
class TradeOutcome:
    signal_id: str
    symbol: str
    entry_time: datetime
    exit_time: datetime
    actual_entry: Decimal
    actual_exit: Decimal
    quantity: int
    fees_and_taxes: Decimal
    exit_reason: ExitReason
    gap_through_stop: bool = False

    def __post_init__(self) -> None:
        require_aware(self.entry_time, "outcome.entry_time")
        require_aware(self.exit_time, "outcome.exit_time")
        if self.exit_time < self.entry_time:
            raise ValueError("exit_time cannot precede entry_time")
        if self.actual_entry <= ZERO or self.actual_exit <= ZERO:
            raise ValueError("actual prices must be positive")
        if self.quantity <= 0:
            raise ValueError("quantity must be positive")
        if self.fees_and_taxes < ZERO:
            raise ValueError("fees_and_taxes cannot be negative")


@dataclass(frozen=True, slots=True)
class OutcomeEvidence:
    data_integrity_breach: bool = False
    entry_followed_notification: bool = True
    material_post_entry_evidence_ids: tuple[str, ...] = ()
    market_return_pct: Decimal = ZERO
    sector_return_pct: Decimal = ZERO
    target_reached_after_stop_within_horizon: bool = False
    public_catalyst_found: bool = False


@dataclass(frozen=True, slots=True)
class TradeReview:
    signal_id: str
    symbol: str
    net_pnl: Decimal
    realized_r: Decimal
    classification: ReviewClassification
    confidence: ReviewConfidence
    preventability: Preventability
    known_facts: tuple[str, ...]
    uncertainties: tuple[str, ...]
    likely_explanation: str
    action: str
    requires_pipeline_halt: bool


class PostTradeAnalyzer:
    """Evidence-based loss attribution. It never invents a missing catalyst."""

    def __init__(
        self,
        market_shock_threshold_pct: Decimal = Decimal("-2"),
        sector_shock_threshold_pct: Decimal = Decimal("-2"),
        tail_loss_multiple: Decimal = Decimal("1.25"),
    ) -> None:
        self.market_shock_threshold_pct = market_shock_threshold_pct
        self.sector_shock_threshold_pct = sector_shock_threshold_pct
        self.tail_loss_multiple = tail_loss_multiple

    def analyze(
        self,
        decision: TradeDecision,
        outcome: TradeOutcome,
        evidence: OutcomeEvidence,
    ) -> TradeReview:
        if decision.action is not DecisionAction.BUY:
            raise ValueError("only completed BUY decisions can be reviewed")
        if decision.signal_id != outcome.signal_id or decision.symbol != outcome.symbol:
            raise ValueError("outcome does not match the original decision")

        net_pnl = (
            (outcome.actual_exit - outcome.actual_entry) * outcome.quantity
            - outcome.fees_and_taxes
        )
        realized_r = (
            net_pnl / decision.planned_max_loss
            if decision.planned_max_loss > ZERO
            else ZERO
        )
        known_facts = [
            f"net P&L: {net_pnl}",
            f"realized R: {realized_r}",
            f"exit reason: {outcome.exit_reason.value}",
            f"pre-trade stop probability: {decision.stop_probability}",
            f"probability status: {decision.probability_status.value}",
            f"market return during trade: {evidence.market_return_pct}%",
            f"sector return during trade: {evidence.sector_return_pct}%",
        ]
        uncertainties: list[str] = []
        execution_deviations: list[str] = []
        if not evidence.entry_followed_notification:
            execution_deviations.append("manual record says the notification was not followed")
        if outcome.quantity != decision.quantity:
            execution_deviations.append(
                f"executed quantity {outcome.quantity} differs from approved {decision.quantity}"
            )
        if decision.earliest_entry_at is not None and outcome.entry_time < decision.earliest_entry_at:
            execution_deviations.append("entry occurred before the first eligible entry time")
        if decision.entry_expires_at is not None and outcome.entry_time > decision.entry_expires_at:
            execution_deviations.append("entry occurred after the alert expired")
        if (
            decision.entry_low is not None
            and decision.entry_high is not None
            and not (decision.entry_low <= outcome.actual_entry <= decision.entry_high)
        ):
            execution_deviations.append("executed entry price was outside the approved range")
        if execution_deviations:
            known_facts.extend(f"execution deviation: {item}" for item in execution_deviations)

        actual_loss = abs(net_pnl)
        if evidence.data_integrity_breach:
            classification = ReviewClassification.DATA_FAILURE
            confidence = ReviewConfidence.HIGH
            preventability = Preventability.YES
            explanation = "A verified data-integrity breach affected the decision or review."
            action = "Halt affected signals, repair the data path, and replay every impacted run."
            halt = True
        elif execution_deviations:
            classification = ReviewClassification.EXECUTION_DEVIATION
            confidence = ReviewConfidence.HIGH
            preventability = Preventability.YES
            explanation = "The executed trade did not match the timestamped notification."
            action = "Separate this result from model performance and review execution controls."
            halt = False
        elif net_pnl >= ZERO:
            return TradeReview(
                decision.signal_id,
                outcome.symbol,
                net_pnl,
                realized_r,
                ReviewClassification.PROFITABLE_OUTCOME,
                ReviewConfidence.HIGH,
                Preventability.NO,
                tuple(known_facts),
                (),
                "The completed trade was not a loss.",
                "Record the outcome and evaluate it with the same process as losses.",
                False,
            )
        elif outcome.gap_through_stop or (
            decision.planned_max_loss > ZERO
            and actual_loss > decision.planned_max_loss * self.tail_loss_multiple
        ):
            classification = ReviewClassification.TAIL_OR_GAP_LOSS
            confidence = ReviewConfidence.HIGH
            preventability = Preventability.PARTIAL
            explanation = "The realized loss exceeded the planned stop path because of gap/tail behavior."
            action = "Review gap reserves, liquidity gates, and position size; do not retrain from one trade."
            halt = False
        elif evidence.material_post_entry_evidence_ids:
            classification = ReviewClassification.POST_ENTRY_NEWS_SHOCK
            confidence = ReviewConfidence.HIGH
            preventability = Preventability.PARTIAL
            known_facts.append(
                "post-entry evidence: " + ", ".join(evidence.material_post_entry_evidence_ids)
            )
            explanation = "Material public information arrived only after the trade was entered."
            action = "Measure this event class across trades and review event-risk sizing."
            halt = False
        elif evidence.market_return_pct <= self.market_shock_threshold_pct:
            classification = ReviewClassification.MARKET_REGIME_MOVE
            confidence = ReviewConfidence.MEDIUM
            preventability = Preventability.PARTIAL
            explanation = "A broad market decline materially overlapped the holding period."
            action = "Compare the regime gate with contemporaneous breadth before changing it."
            halt = False
        elif evidence.sector_return_pct <= self.sector_shock_threshold_pct:
            classification = ReviewClassification.SECTOR_MOVE
            confidence = ReviewConfidence.MEDIUM
            preventability = Preventability.PARTIAL
            explanation = "A sector-level decline materially overlapped the holding period."
            action = "Review sector-relative filters across a batch of comparable trades."
            halt = False
        elif evidence.target_reached_after_stop_within_horizon:
            classification = ReviewClassification.STOP_PLACEMENT_REVIEW
            confidence = ReviewConfidence.MEDIUM
            preventability = Preventability.PARTIAL
            explanation = "Price reached the target after first hitting the declared stop."
            action = "Review stop placement versus ex-ante volatility; do not remove stops retrospectively."
            halt = False
        else:
            classification = ReviewClassification.UNRESOLVED_FORECAST_MISS
            confidence = ReviewConfidence.LOW
            preventability = Preventability.UNRESOLVED
            explanation = "No verified public catalyst or process breach explains the forecast miss."
            uncertainties.append("A unique causal explanation cannot be established from observed data.")
            if evidence.public_catalyst_found:
                uncertainties.append("A public catalyst existed but was not classified as material evidence.")
            action = "Log the miss and evaluate calibration only across a predeclared batch of outcomes."
            halt = False

        return TradeReview(
            decision.signal_id,
            outcome.symbol,
            net_pnl,
            realized_r,
            classification,
            confidence,
            preventability,
            tuple(known_facts),
            tuple(uncertainties),
            explanation,
            action,
            halt,
        )
