"""Pure, deterministic paper decision boundary for the promoted paper path.

Consumes one already-verified ``VerifiedPromotedOperationalAllocationBatch``
and produces exactly one content-addressed ``PAPER_BUY`` or ``NO_TRADE``
decision package with complete deterministic quantity-cap evidence,
rationale, cancellation conditions, veto coverage, and a human-readable
advisory. Performs no I/O, environment, clock, network, broker, filesystem,
GCP, notification, or persistence access. Never changes a price or quantity
retained inside the allocation outcome it wraps, never ranks or re-sizes,
and never selects among more than one ``ALLOCATED`` outcome -- more than
one is an integrity error even if an upstream policy permitted it. Every
decision and package produced here is ``paper_only=True`` and permanently
``notification_eligible=False``/``execution_eligible=False``: ``PAPER_BUY``
is a paper advisory label only and can never become a broker order,
notification, or execution authority.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum

from india_swing.identity import content_id
from india_swing.promoted_operational_allocation import (
    PromotedOperationalAllocationDisposition,
    PromotedOperationalAllocationEvidence,
    PromotedOperationalAllocationOutcome,
    VerifiedPromotedOperationalAllocationBatch,
    build_promoted_operational_allocation_evidence,
)


PAPER_RESEARCH_WARNING = (
    "PAPER RESEARCH ONLY — MANUAL REVIEW REQUIRED — DO NOT AUTO-EXECUTE"
)

PROMOTED_OPERATIONAL_TRADE_RECOMMENDATION_SCHEMA_VERSION = (
    "promoted-operational-trade-recommendation/v1"
)
PROMOTED_OPERATIONAL_DAILY_DECISION_SCHEMA_VERSION = (
    "promoted-operational-daily-decision/v1"
)
PROMOTED_OPERATIONAL_DECISION_PACKAGE_SCHEMA_VERSION = (
    "promoted-operational-decision-package/v1"
)

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAXIMUM_TEXT_BYTES = 128 * 1024


class PromotedOperationalDecisionError(ValueError):
    pass


_ERR_TYPE = "promoted operational decision type is invalid"
_ERR_TEXT = "promoted operational decision text is invalid"
_ERR_AUTHORITY = "promoted operational decision authority flags are invalid"
_ERR_RECOMMENDATION = "promoted operational trade recommendation is invalid"
_ERR_DECISION = "promoted operational daily decision is invalid"
_ERR_SINGULAR = "promoted operational decision cannot contain multiple new trades"
_ERR_PACKAGE = "promoted operational decision package is invalid"


class PromotedOperationalDecisionAction(str, Enum):
    PAPER_BUY = "PAPER_BUY"
    NO_TRADE = "NO_TRADE"


def _public_text(value: str, name: str) -> None:
    if (
        type(value) is not str
        or not value.strip()
        or len(value.encode("utf-8")) > _MAXIMUM_TEXT_BYTES
        or any(ord(character) < 32 and character not in "\n\t" for character in value)
    ):
        raise PromotedOperationalDecisionError(f"{name} must be safe non-empty text")


def _text_tuple(value: tuple[str, ...], name: str, *, allow_empty: bool = False) -> None:
    if type(value) is not tuple or (not allow_empty and not value):
        raise PromotedOperationalDecisionError(f"{name} must be an exact text tuple")
    for item in value:
        _public_text(item, name)
    if len(value) != len(set(value)):
        raise PromotedOperationalDecisionError(f"{name} must not contain duplicates")


def _aware_utc(value: object, name: str) -> datetime:
    if type(value) is not datetime:
        raise PromotedOperationalDecisionError(f"{name} must be timezone-aware")
    try:
        offset = value.utcoffset()
    except Exception:
        raise PromotedOperationalDecisionError(f"{name} has invalid timezone behavior") from None
    if value.tzinfo is None or offset is None:
        raise PromotedOperationalDecisionError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _rationale(
    outcome: PromotedOperationalAllocationOutcome,
    evidence: PromotedOperationalAllocationEvidence,
) -> tuple[str, ...]:
    candidate = outcome.quote_outcome.candidate
    research_intent = candidate.research_intent
    quote_outcome = outcome.quote_outcome
    portfolio = outcome.portfolio_context.portfolio
    policy = outcome.allocation_policy.policy
    return (
        (
            f"Upstream promoted research selected decision {research_intent.decision_id} "
            f"for {candidate.listing_key} on a comparative basis; the research process "
            "does not produce a probability or confidence estimate, and this "
            "recommendation does not add one."
        ),
        (
            f"The quote gate passed at {quote_outcome.evaluated_at.isoformat()} with "
            f"observed spread {quote_outcome.observed_spread_bps} bps and reference "
            f"best ask INR {outcome.reference_entry_price}; freshness, two-sided depth, "
            "spread, and circuit checks were all satisfied at that exact time."
        ),
        (
            f"Quantity ceilings: research {evidence.research_quantity_ceiling}, "
            f"per-trade risk {evidence.per_trade_risk_quantity_ceiling}, total open "
            f"risk {evidence.total_open_risk_quantity_ceiling}, position notional "
            f"{evidence.position_notional_quantity_ceiling}, gross exposure "
            f"{evidence.gross_exposure_quantity_ceiling}, cash "
            f"{evidence.cash_quantity_ceiling}, ask depth "
            f"{evidence.ask_depth_quantity_ceiling}; feasible quantity "
            f"{evidence.feasible_quantity}; binding ceiling(s): "
            f"{', '.join(evidence.binding_ceiling_codes)}."
        ),
        (
            f"Portfolio snapshot {portfolio.portfolio_snapshot_id} and risk policy "
            f"{policy.policy_id}: cash INR {outcome.state_before.cash_available} -> "
            f"INR {outcome.state_after.cash_available}, gross exposure INR "
            f"{outcome.state_before.gross_exposure} -> INR "
            f"{outcome.state_after.gross_exposure}, open risk INR "
            f"{outcome.state_before.open_risk} -> INR {outcome.state_after.open_risk}."
        ),
        (
            f"Operational quantity {outcome.operational_quantity} at entry notional INR "
            f"{outcome.entry_notional}, estimated round-trip cost INR "
            f"{outcome.estimated_round_trip_cost}, planned maximum loss INR "
            f"{outcome.planned_max_loss}, and operational net reward/risk "
            f"{outcome.operational_net_reward_risk}."
        ),
        (
            f"Lineage: research_run {candidate.research_run_id}, research_intent_batch "
            f"{candidate.research_intent_batch_id}, candidate {candidate.candidate_id}, "
            f"quote outcome {quote_outcome.outcome_id}, allocation outcome "
            f"{outcome.allocation_outcome_id}, allocation evidence {evidence.evidence_id}."
        ),
    )


def _cancellation_conditions(
    outcome: PromotedOperationalAllocationOutcome,
) -> tuple[str, ...]:
    quote_outcome = outcome.quote_outcome
    spec = quote_outcome.spec
    candidate = quote_outcome.candidate
    evaluation_intent = candidate.research_intent.evaluation_intent
    entry_order = evaluation_intent.entry_order
    return (
        (
            f"Do not enter before {spec.decision_not_before.isoformat()} or after "
            f"{spec.decision_deadline.isoformat()}."
        ),
        (
            f"Re-run the quote gate if the quote is no longer the snapshot evaluated at "
            f"{quote_outcome.evaluated_at.isoformat()}, if two-sided depth disappears, "
            "or if the spread or circuit state changes."
        ),
        (
            f"Do not enter if the best ask exceeds the research limit INR "
            f"{entry_order.limit_price}, or has reached the stop INR "
            f"{evaluation_intent.stop_price} or target INR "
            f"{evaluation_intent.target_price}."
        ),
        (
            f"Re-run allocation if portfolio artifact "
            f"{outcome.portfolio_context.source_portfolio_artifact_id} or context "
            f"{outcome.portfolio_context.context_id}, or allocation policy "
            f"{outcome.allocation_policy.allocation_policy_id}, changes before manual "
            "entry."
        ),
        f"Do not enter if {candidate.listing_key} is already an open position.",
        (
            f"If entered, treat INR {evaluation_intent.stop_price} as the planned stop "
            f"and {candidate.target_session.isoformat()} plus "
            f"{evaluation_intent.max_holding_sessions} sessions as the maximum holding "
            "boundary; manual delay, slippage, and gap risk can still produce a loss "
            f"larger than the planned INR {outcome.planned_max_loss}."
        ),
    )


@dataclass(frozen=True, slots=True)
class PromotedOperationalTradeRecommendation:
    """One paper trade recommendation bound to exactly one ``ALLOCATED``
    allocation outcome and its exact allocation evidence.

    Never changes a price, quantity, tick, cost buffer, or holding period
    retained inside the allocation outcome. Rationale and cancellation
    conditions are freshly replayed on every verification, never trusted
    from the stored tuple.
    """

    outcome: PromotedOperationalAllocationOutcome
    evidence: PromotedOperationalAllocationEvidence
    rationale: tuple[str, ...]
    cancellation_conditions: tuple[str, ...]
    schema_version: str = PROMOTED_OPERATIONAL_TRADE_RECOMMENDATION_SCHEMA_VERSION
    recommendation_id: str = field(init=False)

    def __post_init__(self) -> None:
        if self.schema_version != PROMOTED_OPERATIONAL_TRADE_RECOMMENDATION_SCHEMA_VERSION:
            raise PromotedOperationalDecisionError(_ERR_RECOMMENDATION)
        self._verify()
        object.__setattr__(self, "recommendation_id", self._calculated_id())

    def _verify(self) -> None:
        if self.schema_version != PROMOTED_OPERATIONAL_TRADE_RECOMMENDATION_SCHEMA_VERSION:
            raise PromotedOperationalDecisionError(_ERR_RECOMMENDATION)
        if type(self.outcome) is not PromotedOperationalAllocationOutcome:
            raise PromotedOperationalDecisionError(_ERR_TYPE)
        self.outcome.verify_content_identity()
        if self.outcome.disposition is not PromotedOperationalAllocationDisposition.ALLOCATED:
            raise PromotedOperationalDecisionError(_ERR_RECOMMENDATION)
        if type(self.evidence) is not PromotedOperationalAllocationEvidence:
            raise PromotedOperationalDecisionError(_ERR_TYPE)
        self.evidence.verify_content_identity()
        if self.evidence.allocation_outcome_id != self.outcome.allocation_outcome_id:
            raise PromotedOperationalDecisionError(_ERR_RECOMMENDATION)
        _text_tuple(self.rationale, "recommendation rationale")
        _text_tuple(self.cancellation_conditions, "cancellation conditions")
        if self.rationale != _rationale(self.outcome, self.evidence):
            raise PromotedOperationalDecisionError(_ERR_RECOMMENDATION)
        if self.cancellation_conditions != _cancellation_conditions(self.outcome):
            raise PromotedOperationalDecisionError(_ERR_RECOMMENDATION)

    def _calculated_id(self) -> str:
        return content_id(
            {
                value.name: getattr(self, value.name)
                for value in fields(self)
                if value.name != "recommendation_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._verify()
        if self.recommendation_id != self._calculated_id():
            raise PromotedOperationalDecisionError(_ERR_RECOMMENDATION)

    @property
    def listing_key(self) -> str:
        return self.outcome.quote_outcome.candidate.listing_key

    @property
    def symbol(self) -> str:
        return self.outcome.quote_outcome.candidate.research_intent.evaluation_intent.entry_order.symbol

    @property
    def isin(self) -> str:
        return self.outcome.quote_outcome.candidate.research_intent.evaluation_intent.isin

    @property
    def quantity(self) -> int:
        return self.outcome.operational_quantity

    @property
    def research_only(self) -> bool:
        return True

    @property
    def execution_eligible(self) -> bool:
        return False


def _veto_codes(batch: VerifiedPromotedOperationalAllocationBatch) -> tuple[str, ...]:
    values: set[str] = set()
    for veto in batch.upstream_quote_vetoes:
        values.update(
            f"QUOTE:{veto.candidate.listing_key}:{code}" for code in veto.reason_codes
        )
    for outcome in batch.allocation_outcomes:
        if outcome.disposition is PromotedOperationalAllocationDisposition.VETO:
            values.update(
                f"ALLOCATION:{outcome.quote_outcome.candidate.listing_key}:{code}"
                for code in outcome.reason_codes
            )
    return tuple(sorted(values))


@dataclass(frozen=True, slots=True)
class PromotedOperationalDailyDecision:
    """One singular ``PAPER_BUY``/``NO_TRADE`` decision for one exact
    promoted operational allocation batch.

    More than one ``ALLOCATED`` outcome is an integrity error even if an
    upstream policy permitted it -- this is checked unconditionally, before
    any action/recommendation shape check.
    """

    allocation_batch: VerifiedPromotedOperationalAllocationBatch
    action: PromotedOperationalDecisionAction
    recommendation: PromotedOperationalTradeRecommendation | None
    veto_reason_codes: tuple[str, ...]
    evaluated_at: datetime
    target_session: date
    paper_only: bool
    notification_eligible: bool
    execution_eligible: bool
    schema_version: str = PROMOTED_OPERATIONAL_DAILY_DECISION_SCHEMA_VERSION
    decision_id: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "evaluated_at", _aware_utc(self.evaluated_at, "decision evaluated_at")
        )
        if self.schema_version != PROMOTED_OPERATIONAL_DAILY_DECISION_SCHEMA_VERSION:
            raise PromotedOperationalDecisionError(_ERR_DECISION)
        self._verify()
        object.__setattr__(self, "decision_id", self._calculated_id())

    def _verify(self) -> None:
        if self.schema_version != PROMOTED_OPERATIONAL_DAILY_DECISION_SCHEMA_VERSION:
            raise PromotedOperationalDecisionError(_ERR_DECISION)
        if type(self.allocation_batch) is not VerifiedPromotedOperationalAllocationBatch:
            raise PromotedOperationalDecisionError(_ERR_TYPE)
        self.allocation_batch.verify_content_identity()
        if type(self.action) is not PromotedOperationalDecisionAction:
            raise PromotedOperationalDecisionError(_ERR_TYPE)
        expected_time = _aware_utc(
            self.allocation_batch.quote_gate_batch.evaluated_at, "quote-gate evaluated_at"
        )
        if self.evaluated_at != expected_time:
            raise PromotedOperationalDecisionError(_ERR_DECISION)
        expected_session = (
            self.allocation_batch.quote_gate_batch.spec.preparation.manifest.target_session
        )
        if type(self.target_session) is not date or self.target_session != expected_session:
            raise PromotedOperationalDecisionError(_ERR_DECISION)
        if (
            self.paper_only is not True
            or self.notification_eligible is not False
            or self.execution_eligible is not False
        ):
            raise PromotedOperationalDecisionError(_ERR_AUTHORITY)
        _text_tuple(self.veto_reason_codes, "veto reason codes", allow_empty=True)
        if self.veto_reason_codes != tuple(sorted(set(self.veto_reason_codes))):
            raise PromotedOperationalDecisionError(_ERR_DECISION)
        if self.veto_reason_codes != _veto_codes(self.allocation_batch):
            raise PromotedOperationalDecisionError(_ERR_DECISION)

        allocated = tuple(
            value for value in self.allocation_batch.allocation_outcomes if value.allocated
        )
        if len(allocated) > 1:
            raise PromotedOperationalDecisionError(_ERR_SINGULAR)
        if allocated:
            if self.action is not PromotedOperationalDecisionAction.PAPER_BUY:
                raise PromotedOperationalDecisionError(_ERR_DECISION)
            if type(self.recommendation) is not PromotedOperationalTradeRecommendation:
                raise PromotedOperationalDecisionError(_ERR_DECISION)
            self.recommendation.verify_content_identity()
            if (
                self.recommendation.outcome.allocation_outcome_id
                != allocated[0].allocation_outcome_id
            ):
                raise PromotedOperationalDecisionError(_ERR_DECISION)
        else:
            if (
                self.action is not PromotedOperationalDecisionAction.NO_TRADE
                or self.recommendation is not None
            ):
                raise PromotedOperationalDecisionError(_ERR_DECISION)

    def _calculated_id(self) -> str:
        return content_id(
            {
                value.name: getattr(self, value.name)
                for value in fields(self)
                if value.name != "decision_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._verify()
        if self.decision_id != self._calculated_id():
            raise PromotedOperationalDecisionError(_ERR_DECISION)

    @property
    def research_only(self) -> bool:
        return True


def assemble_promoted_operational_decision(
    *, allocation_batch: VerifiedPromotedOperationalAllocationBatch
) -> PromotedOperationalDailyDecision:
    """Deterministically produce one singular ``PAPER_BUY``/``NO_TRADE``
    decision from one exact allocation batch. Never selects among more than
    one ``ALLOCATED`` outcome; more than one is an integrity error."""

    if type(allocation_batch) is not VerifiedPromotedOperationalAllocationBatch:
        raise PromotedOperationalDecisionError(_ERR_TYPE)
    allocation_batch.verify_content_identity()

    allocated = tuple(
        value for value in allocation_batch.allocation_outcomes if value.allocated
    )
    if len(allocated) > 1:
        raise PromotedOperationalDecisionError(_ERR_SINGULAR)

    recommendation = None
    action = PromotedOperationalDecisionAction.NO_TRADE
    if allocated:
        outcome = allocated[0]
        evidence = build_promoted_operational_allocation_evidence(outcome)
        recommendation = PromotedOperationalTradeRecommendation(
            outcome=outcome,
            evidence=evidence,
            rationale=_rationale(outcome, evidence),
            cancellation_conditions=_cancellation_conditions(outcome),
        )
        action = PromotedOperationalDecisionAction.PAPER_BUY

    target_session = allocation_batch.quote_gate_batch.spec.preparation.manifest.target_session
    return PromotedOperationalDailyDecision(
        allocation_batch=allocation_batch,
        action=action,
        recommendation=recommendation,
        veto_reason_codes=_veto_codes(allocation_batch),
        evaluated_at=allocation_batch.quote_gate_batch.evaluated_at,
        target_session=target_session,
        paper_only=True,
        notification_eligible=False,
        execution_eligible=False,
    )


def render_promoted_operational_decision(
    decision: PromotedOperationalDailyDecision,
) -> str:
    if type(decision) is not PromotedOperationalDailyDecision:
        raise PromotedOperationalDecisionError(_ERR_TYPE)
    decision.verify_content_identity()
    batch = decision.allocation_batch
    lines = [
        PAPER_RESEARCH_WARNING,
        f"Action: {decision.action.value}",
        f"Target session: {decision.target_session.isoformat()}",
        f"Decision time: {decision.evaluated_at.isoformat()}",
        f"Portfolio context: {batch.portfolio_context.context_id}",
        f"Allocation policy: {batch.allocation_policy.allocation_policy_id}",
    ]
    if decision.action is PromotedOperationalDecisionAction.PAPER_BUY:
        recommendation = decision.recommendation
        if recommendation is None:
            raise PromotedOperationalDecisionError(_ERR_DECISION)
        outcome = recommendation.outcome
        candidate = outcome.quote_outcome.candidate
        evaluation_intent = candidate.research_intent.evaluation_intent
        entry_order = evaluation_intent.entry_order
        lines.extend(
            (
                f"Listing: {candidate.listing_key}",
                f"Symbol: {entry_order.symbol}",
                f"ISIN: {evaluation_intent.isin}",
                f"Quantity: {outcome.operational_quantity}",
                f"Research limit: INR {entry_order.limit_price}",
                f"Reference best ask: INR {outcome.reference_entry_price}",
                f"Stop: INR {evaluation_intent.stop_price}",
                f"Target: INR {evaluation_intent.target_price}",
                f"Tick size: INR {entry_order.tick_size}",
                f"Per-share cost buffer: INR {candidate.research_intent.estimated_cost_buffer}",
                f"Entry notional: INR {outcome.entry_notional}",
                f"Estimated round-trip cost: INR {outcome.estimated_round_trip_cost}",
                f"Planned maximum loss: INR {outcome.planned_max_loss}",
                f"Operational net reward/risk: {outcome.operational_net_reward_risk}",
                (
                    f"Entry decision window: "
                    f"{outcome.quote_outcome.spec.decision_not_before.isoformat()} to "
                    f"{outcome.quote_outcome.spec.decision_deadline.isoformat()}"
                ),
                f"Maximum holding sessions: {evaluation_intent.max_holding_sessions}",
                "Why this trade:",
                *[f"- {value}" for value in recommendation.rationale],
                "Cancel / re-evaluate if:",
                *[f"- {value}" for value in recommendation.cancellation_conditions],
                "Lineage:",
                f"- quote-gate spec: {batch.quote_gate_batch.spec.spec_id}",
                f"- quote-gate batch: {batch.quote_gate_batch.batch_id}",
                f"- allocation batch: {batch.allocation_batch_id}",
                f"- research_run: {candidate.research_run_id}",
                f"- research_intent_batch: {candidate.research_intent_batch_id}",
                f"- candidate: {candidate.candidate_id}",
                f"- quote outcome: {outcome.quote_outcome.outcome_id}",
                f"- allocation outcome: {outcome.allocation_outcome_id}",
                f"- allocation evidence: {recommendation.evidence.evidence_id}",
            )
        )
    else:
        lines.append("No allocated outcome survived every quote and allocation gate.")
    lines.append("Veto diagnostics:")
    if decision.veto_reason_codes:
        lines.extend(f"- {value}" for value in decision.veto_reason_codes)
    else:
        lines.append("- NONE")
    lines.extend(
        (
            f"Decision ID: {decision.decision_id}",
            "This package cannot place an order, notify, or grant execution authority; "
            "revalidate the quote, portfolio, and policy immediately before any manual "
            "action.",
        )
    )
    message = "\n".join(lines) + "\n"
    _public_text(message, "decision advisory text")
    return message


@dataclass(frozen=True, slots=True)
class PromotedOperationalDecisionPackage:
    """Exact, content-addressed, human-readable advisory for one promoted
    operational daily decision.

    ``advisory_text``/``advisory_sha256`` are always replayed from the
    retained decision -- never trusted from supplied prose. Bound to 128
    KiB of safe UTF-8 text.
    """

    decision: PromotedOperationalDailyDecision
    advisory_text: str
    advisory_sha256: str
    paper_only: bool
    notification_eligible: bool
    execution_eligible: bool
    schema_version: str = PROMOTED_OPERATIONAL_DECISION_PACKAGE_SCHEMA_VERSION
    package_id: str = field(init=False)

    def __post_init__(self) -> None:
        if self.schema_version != PROMOTED_OPERATIONAL_DECISION_PACKAGE_SCHEMA_VERSION:
            raise PromotedOperationalDecisionError(_ERR_PACKAGE)
        self._verify()
        object.__setattr__(self, "package_id", self._calculated_id())

    def _verify(self) -> None:
        if self.schema_version != PROMOTED_OPERATIONAL_DECISION_PACKAGE_SCHEMA_VERSION:
            raise PromotedOperationalDecisionError(_ERR_PACKAGE)
        if type(self.decision) is not PromotedOperationalDailyDecision:
            raise PromotedOperationalDecisionError(_ERR_TYPE)
        self.decision.verify_content_identity()
        _public_text(self.advisory_text, "advisory text")
        if not self.advisory_text.startswith(PAPER_RESEARCH_WARNING + "\n"):
            raise PromotedOperationalDecisionError(_ERR_TEXT)
        if type(self.advisory_sha256) is not str or _SHA256.fullmatch(self.advisory_sha256) is None:
            raise PromotedOperationalDecisionError(_ERR_TEXT)
        if hashlib.sha256(self.advisory_text.encode("utf-8")).hexdigest() != self.advisory_sha256:
            raise PromotedOperationalDecisionError(_ERR_TEXT)
        expected_text = render_promoted_operational_decision(self.decision)
        if self.advisory_text != expected_text:
            raise PromotedOperationalDecisionError(_ERR_TEXT)
        if (
            self.paper_only is not True
            or self.notification_eligible is not False
            or self.execution_eligible is not False
        ):
            raise PromotedOperationalDecisionError(_ERR_AUTHORITY)

    def _calculated_id(self) -> str:
        return content_id(
            {
                value.name: getattr(self, value.name)
                for value in fields(self)
                if value.name != "package_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._verify()
        if self.package_id != self._calculated_id():
            raise PromotedOperationalDecisionError(_ERR_PACKAGE)

    @property
    def research_only(self) -> bool:
        return True


def assemble_promoted_operational_decision_package(
    *, allocation_batch: VerifiedPromotedOperationalAllocationBatch
) -> PromotedOperationalDecisionPackage:
    """Build one exact decision and its replayable rendered advisory from
    one exact allocation batch. This package cannot notify, persist, call a
    broker, or place an order."""

    decision = assemble_promoted_operational_decision(allocation_batch=allocation_batch)
    text = render_promoted_operational_decision(decision)
    return PromotedOperationalDecisionPackage(
        decision=decision,
        advisory_text=text,
        advisory_sha256=hashlib.sha256(text.encode("utf-8")).hexdigest(),
        paper_only=True,
        notification_eligible=False,
        execution_eligible=False,
    )
