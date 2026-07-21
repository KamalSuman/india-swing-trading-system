from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum

from india_swing.identity import content_id
from india_swing.risk.swing_portfolio import (
    SwingPortfolioSizingBatch,
    SwingPortfolioSizingOutcome,
    SwingSizingDisposition,
)


ZERO = Decimal("0")
RECOMMENDATION_SCHEMA_VERSION = "swing-trade-recommendation/v1"
DECISION_SCHEMA_VERSION = "swing-daily-decision/v1"
NOTIFICATION_SCHEMA_VERSION = "swing-decision-notification/v1"
PACKAGE_SCHEMA_VERSION = "swing-decision-package/v1"
RESEARCH_WARNING = "RESEARCH ONLY — MANUAL REVIEW REQUIRED — DO NOT AUTO-EXECUTE"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class SwingDecisionError(ValueError):
    pass


class SwingDecisionAction(str, Enum):
    BUY = "BUY"
    NO_TRADE = "NO_TRADE"


def _public_text(value: str, name: str) -> None:
    if (
        type(value) is not str
        or not value.strip()
        or len(value.encode("utf-8")) > 128 * 1024
        or any(ord(character) < 32 and character not in "\n\t" for character in value)
    ):
        raise SwingDecisionError(f"{name} must be safe non-empty text")


def _text_tuple(value: tuple[str, ...], name: str, *, allow_empty: bool = False) -> None:
    if type(value) is not tuple or (not allow_empty and not value):
        raise SwingDecisionError(f"{name} must be an exact text tuple")
    for item in value:
        _public_text(item, name)
    if len(value) != len(set(value)):
        raise SwingDecisionError(f"{name} must not contain duplicates")


def _aware_utc(value: datetime, name: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is None:
        raise SwingDecisionError(f"{name} must be timezone-aware")
    try:
        offset = value.utcoffset()
    except Exception:
        raise SwingDecisionError(f"{name} has invalid timezone behavior") from None
    if offset is None:
        raise SwingDecisionError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _rationale(outcome: SwingPortfolioSizingOutcome) -> tuple[str, ...]:
    opportunity = outcome.opportunity
    gate = opportunity.quote_gate_outcome
    proposal = gate.proposal
    levels = gate.quote_adjusted_levels
    if levels is None or gate.observed_spread_bps is None or gate.effective_cost_bps is None:
        raise SwingDecisionError("sized recommendation lacks quote-gate evidence")
    component_lines = tuple(
        (
            f"{component.factor.value}: normalized value {component.raw_value}, "
            f"weight {component.weight}, contribution {component.contribution}."
        )
        for component in opportunity.components
    )
    planned_net_reward = (
        levels.target - levels.entry_high - levels.cost_per_share
    ) * outcome.quantity
    return (
        (
            f"Deterministic cross-sectional rank {opportunity.rank} with score "
            f"{opportunity.ranking_score}; the score is comparative and is not a "
            "probability or confidence estimate."
        ),
        *component_lines,
        (
            f"Quote gate passed at {gate.evaluated_at.isoformat()} with last price "
            f"{gate.quote.last_price}, best ask {gate.quote.best_ask}, observed spread "
            f"{gate.observed_spread_bps} bps, and conservative round-trip cost "
            f"{gate.effective_cost_bps} bps."
        ),
        (
            f"Technical evidence: momentum return {proposal.metrics.momentum_return}, "
            f"trend quality {proposal.metrics.trend_quality}, volume confirmation "
            f"{proposal.metrics.volume_confirmation}, and median traded value INR "
            f"{proposal.metrics.median_traded_value}."
        ),
        (
            f"Portfolio sizing permits {outcome.quantity} shares at entry-high "
            f"notional INR {outcome.entry_notional}, estimated round-trip cost INR "
            f"{outcome.estimated_round_trip_cost}, and planned maximum loss INR "
            f"{outcome.planned_max_loss}."
        ),
        (
            f"Target-side planned net reward is INR {planned_net_reward}; net "
            f"reward/risk is {levels.net_reward_risk}, meeting the deterministic "
            f"minimum {outcome.policy.minimum_net_reward_risk}."
        ),
        (
            f"Entry is valid only from {proposal.entry_window.earliest_entry_at.isoformat()} "
            f"through {proposal.entry_window.entry_expires_at.isoformat()}, with a "
            f"holding boundary of {proposal.entry_window.holding_boundary_day.isoformat()}."
        ),
    )


def _cancellation_conditions(outcome: SwingPortfolioSizingOutcome) -> tuple[str, ...]:
    gate = outcome.opportunity.quote_gate_outcome
    proposal = gate.proposal
    levels = gate.quote_adjusted_levels
    if levels is None:
        raise SwingDecisionError("sized recommendation lacks quote-adjusted levels")
    return (
        (
            f"Do not enter before {proposal.entry_window.earliest_entry_at.isoformat()} "
            f"or after {proposal.entry_window.entry_expires_at.isoformat()}."
        ),
        (
            f"Do not enter outside the quote-adjusted range {levels.entry_low} to "
            f"{levels.entry_high}."
        ),
        (
            f"Re-run the quote gate if the quote is no longer the snapshot evaluated at "
            f"{gate.evaluated_at.isoformat()}, if two-sided depth disappears, or if the "
            f"spread exceeds {gate.policy.maximum_spread_bps} bps."
        ),
        "Do not enter while the security is circuit-locked or the last trade is stale.",
        (
            "Re-run portfolio sizing if cash, open positions, open risk, realized P&L, "
            "or another accepted trade changes before manual entry."
        ),
        (
            f"If entered, treat {levels.stop} as the planned stop and "
            f"{proposal.entry_window.holding_boundary_day.isoformat()} as the maximum "
            "holding boundary; market gaps can exceed the planned loss."
        ),
    )


@dataclass(frozen=True, slots=True)
class SwingTradeRecommendation:
    sizing_outcome: SwingPortfolioSizingOutcome
    rationale: tuple[str, ...]
    cancellation_conditions: tuple[str, ...]
    schema_version: str = RECOMMENDATION_SCHEMA_VERSION
    recommendation_id: str = field(init=False)

    def __post_init__(self) -> None:
        if self.schema_version != RECOMMENDATION_SCHEMA_VERSION:
            raise SwingDecisionError("unsupported trade recommendation schema")
        self._verify()
        object.__setattr__(self, "recommendation_id", self._calculated_id())

    def _verify(self) -> None:
        if type(self.sizing_outcome) is not SwingPortfolioSizingOutcome:
            raise SwingDecisionError("sizing outcome must be exact")
        self.sizing_outcome.verify_content_identity()
        if self.sizing_outcome.disposition is not SwingSizingDisposition.SIZED:
            raise SwingDecisionError("trade recommendation requires a sized outcome")
        _text_tuple(self.rationale, "recommendation rationale")
        _text_tuple(self.cancellation_conditions, "cancellation conditions")
        if self.rationale != _rationale(self.sizing_outcome):
            raise SwingDecisionError("recommendation rationale does not replay")
        if self.cancellation_conditions != _cancellation_conditions(self.sizing_outcome):
            raise SwingDecisionError("cancellation conditions do not replay")

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "recommendation_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._verify()
        if self.recommendation_id != self._calculated_id():
            raise SwingDecisionError("trade recommendation content identity failed")

    @property
    def symbol(self) -> str:
        return self.sizing_outcome.symbol

    @property
    def quantity(self) -> int:
        return self.sizing_outcome.quantity

    @property
    def levels(self):
        levels = self.sizing_outcome.opportunity.quote_gate_outcome.quote_adjusted_levels
        if levels is None:
            raise SwingDecisionError("trade recommendation levels are unavailable")
        return levels

    @property
    def planned_net_reward(self) -> Decimal:
        return (
            self.levels.target - self.levels.entry_high - self.levels.cost_per_share
        ) * self.quantity

    @property
    def research_only(self) -> bool:
        return True

    @property
    def execution_eligible(self) -> bool:
        return False


def _veto_codes(batch: SwingPortfolioSizingBatch) -> tuple[str, ...]:
    proposal_batch = batch.ranking_batch.quote_gate_batch.proposal_batch
    values: set[str] = set()
    for veto in proposal_batch.vetoes:
        values.update(f"UNIVERSE:{code}" for code in veto.reason_codes)
    for veto in batch.upstream_vetoes:
        values.update(f"QUOTE:{veto.proposal.symbol}:{code}" for code in veto.reason_codes)
    for outcome in batch.outcomes:
        if outcome.disposition is SwingSizingDisposition.VETO:
            values.update(f"SIZING:{outcome.symbol}:{code}" for code in outcome.reason_codes)
    return tuple(sorted(values))


@dataclass(frozen=True, slots=True)
class SwingDailyDecision:
    sizing_batch: SwingPortfolioSizingBatch
    action: SwingDecisionAction
    recommendation: SwingTradeRecommendation | None
    veto_reason_codes: tuple[str, ...]
    evaluated_at: datetime
    schema_version: str = DECISION_SCHEMA_VERSION
    decision_id: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "evaluated_at", _aware_utc(self.evaluated_at, "evaluated_at"))
        if self.schema_version != DECISION_SCHEMA_VERSION:
            raise SwingDecisionError("unsupported daily decision schema")
        self._verify()
        object.__setattr__(self, "decision_id", self._calculated_id())

    def _verify(self) -> None:
        if type(self.sizing_batch) is not SwingPortfolioSizingBatch:
            raise SwingDecisionError("sizing batch must be exact")
        self.sizing_batch.verify_content_identity()
        if type(self.action) is not SwingDecisionAction:
            raise SwingDecisionError("decision action must be exact")
        expected_time = _aware_utc(
            self.sizing_batch.ranking_batch.quote_gate_batch.evaluated_at,
            "quote-gate evaluated_at",
        )
        if self.evaluated_at != expected_time:
            raise SwingDecisionError("decision time differs from the quote gate")
        _text_tuple(self.veto_reason_codes, "veto reason codes", allow_empty=True)
        if self.veto_reason_codes != tuple(sorted(set(self.veto_reason_codes))):
            raise SwingDecisionError("veto reason codes must be sorted and unique")
        if self.veto_reason_codes != _veto_codes(self.sizing_batch):
            raise SwingDecisionError("veto reason coverage does not replay")
        sized = tuple(value for value in self.sizing_batch.outcomes if value.sized)
        if len(sized) > 1:
            raise SwingDecisionError("daily decision cannot contain multiple new trades")
        if sized:
            if self.action is not SwingDecisionAction.BUY:
                raise SwingDecisionError("a sized outcome requires a BUY decision")
            if type(self.recommendation) is not SwingTradeRecommendation:
                raise SwingDecisionError("BUY decision requires one recommendation")
            self.recommendation.verify_content_identity()
            if (
                self.recommendation.sizing_outcome.sizing_outcome_id
                != sized[0].sizing_outcome_id
            ):
                raise SwingDecisionError("recommendation differs from the sized outcome")
        else:
            if self.action is not SwingDecisionAction.NO_TRADE or self.recommendation is not None:
                raise SwingDecisionError("no sized outcome requires a NO_TRADE decision")

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "decision_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._verify()
        if self.decision_id != self._calculated_id():
            raise SwingDecisionError("daily decision content identity failed")

    @property
    def research_only(self) -> bool:
        return True

    @property
    def execution_eligible(self) -> bool:
        return False


def assemble_swing_daily_decision(
    *, sizing_batch: SwingPortfolioSizingBatch
) -> SwingDailyDecision:
    if type(sizing_batch) is not SwingPortfolioSizingBatch:
        raise SwingDecisionError("sizing batch must be exact")
    sizing_batch.verify_content_identity()
    sized = tuple(value for value in sizing_batch.outcomes if value.sized)
    if len(sized) > 1:
        raise SwingDecisionError("sizing policy must permit at most one new trade per run")
    recommendation = None
    action = SwingDecisionAction.NO_TRADE
    if sized:
        recommendation = SwingTradeRecommendation(
            sizing_outcome=sized[0],
            rationale=_rationale(sized[0]),
            cancellation_conditions=_cancellation_conditions(sized[0]),
        )
        action = SwingDecisionAction.BUY
    return SwingDailyDecision(
        sizing_batch=sizing_batch,
        action=action,
        recommendation=recommendation,
        veto_reason_codes=_veto_codes(sizing_batch),
        evaluated_at=sizing_batch.ranking_batch.quote_gate_batch.evaluated_at,
    )


def render_swing_decision(decision: SwingDailyDecision) -> str:
    if type(decision) is not SwingDailyDecision:
        raise SwingDecisionError("daily decision must be exact")
    decision.verify_content_identity()
    sizing = decision.sizing_batch
    lines = [
        RESEARCH_WARNING,
        f"Decision: {decision.action.value}",
        f"Decision time: {decision.evaluated_at.isoformat()}",
        f"Portfolio snapshot: {sizing.portfolio.portfolio_snapshot_id}",
        f"Risk policy: {sizing.policy.policy_id}",
    ]
    if decision.action is SwingDecisionAction.BUY:
        recommendation = decision.recommendation
        if recommendation is None:
            raise SwingDecisionError("BUY decision lost its recommendation")
        outcome = recommendation.sizing_outcome
        gate = outcome.opportunity.quote_gate_outcome
        proposal = gate.proposal
        levels = recommendation.levels
        lines.extend(
            (
                f"Symbol: {recommendation.symbol}",
                f"Exchange: {proposal.universe_entry.listing.exchange}",
                f"Cross-sectional rank: {outcome.opportunity.rank}",
                f"Comparative score (not confidence): {outcome.opportunity.ranking_score}",
                f"Quantity: {recommendation.quantity}",
                f"Entry range: INR {levels.entry_low} to INR {levels.entry_high}",
                f"Observed last price: INR {gate.quote.last_price}",
                f"Observed best ask: INR {gate.quote.best_ask}",
                f"Stop: INR {levels.stop}",
                f"Target: INR {levels.target}",
                f"Entry notional at range high: INR {outcome.entry_notional}",
                f"Estimated round-trip cost: INR {outcome.estimated_round_trip_cost}",
                f"Planned maximum loss: INR {outcome.planned_max_loss}",
                f"Planned net reward at target: INR {recommendation.planned_net_reward}",
                f"Net reward/risk: {levels.net_reward_risk}",
                f"Entry window: {proposal.entry_window.earliest_entry_at.isoformat()} to {proposal.entry_window.entry_expires_at.isoformat()}",
                f"Maximum holding boundary: {proposal.entry_window.holding_boundary_day.isoformat()}",
                "Why this trade:",
                *[f"- {value}" for value in recommendation.rationale],
                "Cancel / re-evaluate if:",
                *[f"- {value}" for value in recommendation.cancellation_conditions],
                "Evidence IDs:",
                *[f"- {value}" for value in proposal.evidence_ids],
            )
        )
    else:
        lines.append("No opportunity survived every quote, ranking, and portfolio gate.")
    lines.append("Other veto diagnostics:")
    if decision.veto_reason_codes:
        lines.extend(f"- {value}" for value in decision.veto_reason_codes)
    else:
        lines.append("- NONE")
    lines.extend(
        (
            f"Decision ID: {decision.decision_id}",
            "This package cannot place an order. Revalidate the quote and portfolio immediately before any manual action.",
        )
    )
    message = "\n".join(lines) + "\n"
    _public_text(message, "decision notification message")
    return message


@dataclass(frozen=True, slots=True)
class SwingDecisionNotification:
    decision_id: str
    action: SwingDecisionAction
    evaluated_at: datetime
    message: str
    message_sha256: str
    mode: str = "RESEARCH_ONLY"
    schema_version: str = NOTIFICATION_SCHEMA_VERSION
    notification_id: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "evaluated_at", _aware_utc(self.evaluated_at, "evaluated_at"))
        self._verify()
        object.__setattr__(self, "notification_id", self._calculated_id())

    def _verify(self) -> None:
        if type(self.decision_id) is not str or _SHA256.fullmatch(self.decision_id) is None:
            raise SwingDecisionError("decision_id must be a lowercase SHA-256")
        if type(self.action) is not SwingDecisionAction:
            raise SwingDecisionError("notification action must be exact")
        _public_text(self.message, "notification message")
        if not self.message.startswith(RESEARCH_WARNING + "\n"):
            raise SwingDecisionError("notification research warning is missing")
        if type(self.message_sha256) is not str or _SHA256.fullmatch(self.message_sha256) is None:
            raise SwingDecisionError("message_sha256 must be a lowercase SHA-256")
        if hashlib.sha256(self.message.encode("utf-8")).hexdigest() != self.message_sha256:
            raise SwingDecisionError("notification message hash differs")
        if self.mode != "RESEARCH_ONLY" or self.schema_version != NOTIFICATION_SCHEMA_VERSION:
            raise SwingDecisionError("notification authority boundary is invalid")

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "notification_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._verify()
        if self.notification_id != self._calculated_id():
            raise SwingDecisionError("notification content identity failed")

    @property
    def execution_eligible(self) -> bool:
        return False


def notification_from_swing_decision(
    decision: SwingDailyDecision,
) -> SwingDecisionNotification:
    if type(decision) is not SwingDailyDecision:
        raise SwingDecisionError("daily decision must be exact")
    decision.verify_content_identity()
    message = render_swing_decision(decision)
    return SwingDecisionNotification(
        decision_id=decision.decision_id,
        action=decision.action,
        evaluated_at=decision.evaluated_at,
        message=message,
        message_sha256=hashlib.sha256(message.encode("utf-8")).hexdigest(),
    )


@dataclass(frozen=True, slots=True)
class SwingDecisionPackage:
    decision: SwingDailyDecision
    notification: SwingDecisionNotification
    schema_version: str = PACKAGE_SCHEMA_VERSION
    package_id: str = field(init=False)

    def __post_init__(self) -> None:
        if self.schema_version != PACKAGE_SCHEMA_VERSION:
            raise SwingDecisionError("unsupported decision package schema")
        self._verify()
        object.__setattr__(self, "package_id", self._calculated_id())

    def _verify(self) -> None:
        if type(self.decision) is not SwingDailyDecision:
            raise SwingDecisionError("decision must be exact")
        if type(self.notification) is not SwingDecisionNotification:
            raise SwingDecisionError("notification must be exact")
        self.decision.verify_content_identity()
        self.notification.verify_content_identity()
        expected = notification_from_swing_decision(self.decision)
        if self.notification.notification_id != expected.notification_id:
            raise SwingDecisionError("notification does not replay from the decision")

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "package_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._verify()
        if self.package_id != self._calculated_id():
            raise SwingDecisionError("decision package content identity failed")

    @property
    def research_only(self) -> bool:
        return True

    @property
    def execution_eligible(self) -> bool:
        return False


def package_swing_decision(decision: SwingDailyDecision) -> SwingDecisionPackage:
    notification = notification_from_swing_decision(decision)
    return SwingDecisionPackage(decision=decision, notification=notification)
