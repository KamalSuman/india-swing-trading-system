from __future__ import annotations

from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum

from india_swing.identity import content_id
from india_swing.market_data.models import FullQuoteBatch, KiteFullQuote

from .deterministic_swing import SwingTradeLevels, calculate_swing_trade_levels
from .proposal_batch import SwingProposalBatch, SwingTechnicalProposal


ZERO = Decimal("0")
POLICY_SCHEMA_VERSION = "swing-quote-gate-policy/v1"
OUTCOME_SCHEMA_VERSION = "swing-quote-gate-outcome/v1"
BATCH_SCHEMA_VERSION = "swing-quote-gate-batch/v1"


class SwingQuoteGateError(ValueError):
    pass


class SwingQuoteGateDisposition(str, Enum):
    PASS = "PASS"
    VETO = "VETO"


class SwingQuoteGateReason(str, Enum):
    ENTRY_WINDOW_NOT_OPEN = "ENTRY_WINDOW_NOT_OPEN"
    ENTRY_WINDOW_EXPIRED = "ENTRY_WINDOW_EXPIRED"
    QUOTE_OUTSIDE_ENTRY_WINDOW = "QUOTE_OUTSIDE_ENTRY_WINDOW"
    QUOTE_STALE = "QUOTE_STALE"
    LAST_TRADE_TIME_MISSING = "LAST_TRADE_TIME_MISSING"
    LAST_TRADE_OUTSIDE_ENTRY_WINDOW = "LAST_TRADE_OUTSIDE_ENTRY_WINDOW"
    LAST_TRADE_STALE = "LAST_TRADE_STALE"
    TWO_SIDED_DEPTH_MISSING = "TWO_SIDED_DEPTH_MISSING"
    SPREAD_UNAVAILABLE = "SPREAD_UNAVAILABLE"
    SPREAD_ABOVE_POLICY_MAX = "SPREAD_ABOVE_POLICY_MAX"
    CIRCUIT_LOCKED = "CIRCUIT_LOCKED"
    LAST_PRICE_OUTSIDE_ENTRY_RANGE = "LAST_PRICE_OUTSIDE_ENTRY_RANGE"
    BEST_ASK_OUTSIDE_ENTRY_RANGE = "BEST_ASK_OUTSIDE_ENTRY_RANGE"


@dataclass(frozen=True, slots=True)
class SwingQuoteGatePolicy:
    """Explicit, conservative pre-entry quote-gate thresholds (research/paper only)."""

    maximum_batch_collection_seconds: int = 15
    maximum_quote_age_seconds: int = 15
    maximum_last_trade_age_seconds: int = 300
    maximum_spread_bps: Decimal = Decimal("50")
    policy_version: str = POLICY_SCHEMA_VERSION
    policy_id: str = field(init=False)

    def __post_init__(self) -> None:
        for name in (
            "maximum_batch_collection_seconds",
            "maximum_quote_age_seconds",
            "maximum_last_trade_age_seconds",
        ):
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise SwingQuoteGateError(f"{name} must be a positive integer")
        if type(self.maximum_spread_bps) is not Decimal or not self.maximum_spread_bps.is_finite():
            raise SwingQuoteGateError("maximum_spread_bps must be a finite Decimal")
        if self.maximum_spread_bps <= ZERO:
            raise SwingQuoteGateError("maximum_spread_bps must be positive")
        if self.policy_version != POLICY_SCHEMA_VERSION:
            raise SwingQuoteGateError("unsupported quote gate policy version")
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
            raise SwingQuoteGateError("quote gate policy content identity failed")


def _evaluate_quote_gate(
    proposal: SwingTechnicalProposal,
    quote: KiteFullQuote,
    evaluated_at: datetime,
    policy: SwingQuoteGatePolicy,
) -> tuple[
    SwingQuoteGateDisposition,
    tuple[str, ...],
    Decimal | None,
    Decimal | None,
    SwingTradeLevels | None,
]:
    """Pure, replay-verifiable pre-entry quote evaluation shared by every caller."""

    window = proposal.entry_window
    levels = proposal.levels
    reasons: set[str] = set()

    if evaluated_at < window.earliest_entry_at:
        reasons.add(SwingQuoteGateReason.ENTRY_WINDOW_NOT_OPEN.value)
    if evaluated_at > window.entry_expires_at:
        reasons.add(SwingQuoteGateReason.ENTRY_WINDOW_EXPIRED.value)

    if (
        quote.exchange_timestamp < window.earliest_entry_at
        or quote.exchange_timestamp > window.entry_expires_at
    ):
        reasons.add(SwingQuoteGateReason.QUOTE_OUTSIDE_ENTRY_WINDOW.value)
    quote_age_seconds = (evaluated_at - quote.exchange_timestamp).total_seconds()
    if quote_age_seconds < 0 or quote_age_seconds > policy.maximum_quote_age_seconds:
        reasons.add(SwingQuoteGateReason.QUOTE_STALE.value)

    if quote.last_trade_time is None:
        reasons.add(SwingQuoteGateReason.LAST_TRADE_TIME_MISSING.value)
    else:
        if (
            quote.last_trade_time < window.earliest_entry_at
            or quote.last_trade_time > window.entry_expires_at
        ):
            reasons.add(SwingQuoteGateReason.LAST_TRADE_OUTSIDE_ENTRY_WINDOW.value)
        last_trade_age_seconds = (evaluated_at - quote.last_trade_time).total_seconds()
        if (
            last_trade_age_seconds < 0
            or last_trade_age_seconds > policy.maximum_last_trade_age_seconds
        ):
            reasons.add(SwingQuoteGateReason.LAST_TRADE_STALE.value)

    observed_spread_bps = quote.spread_bps
    if not quote.has_two_sided_depth:
        reasons.add(SwingQuoteGateReason.TWO_SIDED_DEPTH_MISSING.value)
    if observed_spread_bps is None:
        reasons.add(SwingQuoteGateReason.SPREAD_UNAVAILABLE.value)
    elif observed_spread_bps > policy.maximum_spread_bps:
        reasons.add(SwingQuoteGateReason.SPREAD_ABOVE_POLICY_MAX.value)

    if quote.at_lower_circuit or quote.at_upper_circuit:
        reasons.add(SwingQuoteGateReason.CIRCUIT_LOCKED.value)

    if not (levels.entry_low <= quote.last_price <= levels.entry_high):
        reasons.add(SwingQuoteGateReason.LAST_PRICE_OUTSIDE_ENTRY_RANGE.value)
    best_ask = quote.best_ask
    if best_ask is not None and not (levels.entry_low <= best_ask <= levels.entry_high):
        reasons.add(SwingQuoteGateReason.BEST_ASK_OUTSIDE_ENTRY_RANGE.value)

    if reasons:
        return (
            SwingQuoteGateDisposition.VETO,
            tuple(sorted(reasons)),
            observed_spread_bps,
            None,
            None,
        )

    effective_cost_bps = max(proposal.config.base_round_trip_cost_bps, observed_spread_bps)
    history = proposal.assembly.signal_materialization.history
    quote_adjusted_levels = calculate_swing_trade_levels(
        current_close=history.bars[-1].close,
        tick=history.tick_size,
        atr=proposal.metrics.atr,
        estimated_cost_bps=effective_cost_bps,
        config=proposal.config,
    )
    return (
        SwingQuoteGateDisposition.PASS,
        (),
        observed_spread_bps,
        effective_cost_bps,
        quote_adjusted_levels,
    )


@dataclass(frozen=True, slots=True)
class SwingQuoteGateOutcome:
    """One deterministic, replayable PASS/VETO outcome for one exact proposal."""

    proposal: SwingTechnicalProposal
    quote: KiteFullQuote
    evaluated_at: datetime
    policy: SwingQuoteGatePolicy
    disposition: SwingQuoteGateDisposition
    reason_codes: tuple[str, ...]
    observed_spread_bps: Decimal | None
    effective_cost_bps: Decimal | None
    quote_adjusted_levels: SwingTradeLevels | None
    schema_version: str = OUTCOME_SCHEMA_VERSION
    outcome_id: str = field(init=False)

    def __post_init__(self) -> None:
        if self.schema_version != OUTCOME_SCHEMA_VERSION:
            raise SwingQuoteGateError("unsupported quote gate outcome schema")
        self._verify()
        object.__setattr__(self, "outcome_id", self._calculated_id())

    def _verify(self) -> None:
        if type(self.proposal) is not SwingTechnicalProposal:
            raise SwingQuoteGateError("proposal must be exact")
        self.proposal.verify_content_identity()
        if type(self.quote) is not KiteFullQuote:
            raise SwingQuoteGateError("quote must be exact")
        self.quote.verify_content_identity()
        expected_key = f"NSE:{self.proposal.symbol}"
        if self.quote.listing_key != expected_key:
            raise SwingQuoteGateError("quote listing key does not match the proposal subject")
        if (
            type(self.evaluated_at) is not datetime
            or self.evaluated_at.tzinfo is None
            or self.evaluated_at.utcoffset() is None
        ):
            raise SwingQuoteGateError("evaluated_at must be timezone-aware")
        if type(self.policy) is not SwingQuoteGatePolicy:
            raise SwingQuoteGateError("policy must be exact")
        self.policy.verify_content_identity()
        if type(self.disposition) is not SwingQuoteGateDisposition:
            raise SwingQuoteGateError("disposition must be exact")

        try:
            replayed = _evaluate_quote_gate(
                self.proposal, self.quote, self.evaluated_at, self.policy
            )
        except SwingQuoteGateError:
            raise
        except Exception:
            raise SwingQuoteGateError("quote gate replay failed") from None
        replayed_disposition, replayed_reasons, replayed_spread, replayed_cost, replayed_levels = (
            replayed
        )
        if replayed_disposition is not self.disposition:
            raise SwingQuoteGateError("outcome disposition does not replay from bound inputs")
        if (
            type(self.reason_codes) is not tuple
            or self.reason_codes != replayed_reasons
        ):
            raise SwingQuoteGateError("outcome reasons do not replay from bound inputs")
        if self.reason_codes != tuple(sorted(set(self.reason_codes))):
            raise SwingQuoteGateError("reason codes must be sorted and unique")
        if self.observed_spread_bps != replayed_spread:
            raise SwingQuoteGateError("observed spread does not replay from bound inputs")

        if self.disposition is SwingQuoteGateDisposition.PASS:
            if self.reason_codes:
                raise SwingQuoteGateError("a PASS outcome cannot carry reason codes")
            if self.effective_cost_bps is None or self.quote_adjusted_levels is None:
                raise SwingQuoteGateError(
                    "a PASS outcome requires effective cost and quote-adjusted levels"
                )
            if type(self.quote_adjusted_levels) is not SwingTradeLevels:
                raise SwingQuoteGateError("quote_adjusted_levels must be exact")
            self.quote_adjusted_levels.verify_content_identity()
            if (
                replayed_levels is None
                or replayed_levels.levels_id != self.quote_adjusted_levels.levels_id
            ):
                raise SwingQuoteGateError(
                    "quote-adjusted levels do not replay from bound inputs"
                )
            if self.effective_cost_bps != replayed_cost:
                raise SwingQuoteGateError("effective cost does not replay from bound inputs")
        else:
            if not self.reason_codes:
                raise SwingQuoteGateError("a VETO outcome requires at least one reason code")
            if self.effective_cost_bps is not None or self.quote_adjusted_levels is not None:
                raise SwingQuoteGateError(
                    "a VETO outcome cannot carry effective cost or quote-adjusted levels"
                )

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "outcome_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._verify()
        if self.outcome_id != self._calculated_id():
            raise SwingQuoteGateError("quote gate outcome content identity failed")

    @property
    def listing_key(self) -> str:
        return f"NSE:{self.proposal.symbol}"

    @property
    def passed(self) -> bool:
        return self.disposition is SwingQuoteGateDisposition.PASS

    @property
    def research_only(self) -> bool:
        return self.proposal.research_only

    @property
    def execution_eligible(self) -> bool:
        return False


@dataclass(frozen=True, slots=True)
class SwingQuoteGateBatch:
    """Exact, content-addressed one-outcome-per-proposal pre-entry quote gate result."""

    proposal_batch: SwingProposalBatch
    quote_batch: FullQuoteBatch
    policy: SwingQuoteGatePolicy
    evaluated_at: datetime
    outcomes: tuple[SwingQuoteGateOutcome, ...]
    pass_count: int
    veto_count: int
    schema_version: str = BATCH_SCHEMA_VERSION
    gate_batch_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.pass_count) is not int or self.pass_count < 0:
            raise SwingQuoteGateError("pass_count must be a non-negative integer")
        if type(self.veto_count) is not int or self.veto_count < 0:
            raise SwingQuoteGateError("veto_count must be a non-negative integer")
        if self.schema_version != BATCH_SCHEMA_VERSION:
            raise SwingQuoteGateError("unsupported quote gate batch schema")
        self._verify_coverage()
        object.__setattr__(self, "gate_batch_id", self._calculated_id())

    def _verify_coverage(self) -> None:
        if type(self.proposal_batch) is not SwingProposalBatch:
            raise SwingQuoteGateError("proposal_batch must be exact")
        self.proposal_batch.verify_content_identity()
        if type(self.quote_batch) is not FullQuoteBatch:
            raise SwingQuoteGateError("quote_batch must be exact")
        self.quote_batch.verify_content_identity()
        if type(self.policy) is not SwingQuoteGatePolicy:
            raise SwingQuoteGateError("policy must be exact")
        self.policy.verify_content_identity()
        if (
            type(self.evaluated_at) is not datetime
            or self.evaluated_at.tzinfo is None
            or self.evaluated_at.utcoffset() is None
        ):
            raise SwingQuoteGateError("evaluated_at must be timezone-aware")

        expected_keys = tuple(
            f"NSE:{value.symbol}" for value in self.proposal_batch.proposals
        )
        if self.quote_batch.requested_keys != expected_keys:
            raise SwingQuoteGateError(
                "quote batch keys do not exactly match the proposal batch"
            )
        if self.quote_batch.observed_at > self.evaluated_at:
            raise SwingQuoteGateError("quote batch was observed after the evaluation time")
        collection_seconds = (
            self.quote_batch.observed_at - self.quote_batch.requested_at
        ).total_seconds()
        if collection_seconds > self.policy.maximum_batch_collection_seconds:
            raise SwingQuoteGateError(
                "quote batch collection duration exceeded the policy maximum"
            )

        if type(self.outcomes) is not tuple or any(
            type(value) is not SwingQuoteGateOutcome for value in self.outcomes
        ):
            raise SwingQuoteGateError("outcomes must be an exact tuple")
        if len(self.outcomes) != len(self.proposal_batch.proposals):
            raise SwingQuoteGateError(
                "outcome coverage does not match the proposal batch"
            )
        for value in self.outcomes:
            value.verify_content_identity()
        for index, outcome in enumerate(self.outcomes):
            proposal = self.proposal_batch.proposals[index]
            quote = self.quote_batch.quotes[index]
            if outcome.proposal.proposal_id != proposal.proposal_id:
                raise SwingQuoteGateError(
                    "outcome is not bound to its proposal batch proposal"
                )
            if outcome.quote != quote:
                raise SwingQuoteGateError("outcome is not bound to its quote batch quote")
            if outcome.evaluated_at != self.evaluated_at:
                raise SwingQuoteGateError("outcome evaluated_at differs from the gate batch")
            if outcome.policy.policy_id != self.policy.policy_id:
                raise SwingQuoteGateError("outcome policy differs from the gate batch")

        expected_pass = sum(
            1 for value in self.outcomes if value.disposition is SwingQuoteGateDisposition.PASS
        )
        expected_veto = len(self.outcomes) - expected_pass
        if self.pass_count != expected_pass or self.veto_count != expected_veto:
            raise SwingQuoteGateError("pass/veto counts are inconsistent")

    @property
    def execution_eligible(self) -> bool:
        return False

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "gate_batch_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._verify_coverage()
        if self.gate_batch_id != self._calculated_id():
            raise SwingQuoteGateError("quote gate batch content identity failed")


def assemble_swing_quote_gate_batch(
    *,
    proposal_batch: SwingProposalBatch,
    quote_batch: FullQuoteBatch,
    evaluated_at: datetime,
    policy: SwingQuoteGatePolicy | None = None,
) -> SwingQuoteGateBatch:
    """Bind one exact SwingProposalBatch to one exact FullQuoteBatch at one exact time.

    Every proposal receives exactly one PASS or VETO outcome. This function
    never lowers the base cost assumption, never invents probabilities, and
    never makes a PASS executable -- it only evaluates already-existing
    proposals and quotes against the declared policy.
    """

    if type(proposal_batch) is not SwingProposalBatch:
        raise SwingQuoteGateError("proposal_batch must be exact")
    proposal_batch.verify_content_identity()
    if type(quote_batch) is not FullQuoteBatch:
        raise SwingQuoteGateError("quote_batch must be exact")
    quote_batch.verify_content_identity()
    if (
        type(evaluated_at) is not datetime
        or evaluated_at.tzinfo is None
        or evaluated_at.utcoffset() is None
    ):
        raise SwingQuoteGateError("evaluated_at must be timezone-aware")
    evaluated_at = evaluated_at.astimezone(timezone.utc)
    if policy is None:
        policy = SwingQuoteGatePolicy()
    if type(policy) is not SwingQuoteGatePolicy:
        raise SwingQuoteGateError("policy must be exact")
    policy.verify_content_identity()

    expected_keys = tuple(f"NSE:{value.symbol}" for value in proposal_batch.proposals)
    if quote_batch.requested_keys != expected_keys:
        raise SwingQuoteGateError("quote batch keys do not exactly match the proposal batch")
    if quote_batch.observed_at > evaluated_at:
        raise SwingQuoteGateError("quote batch was observed after the evaluation time")
    collection_seconds = (quote_batch.observed_at - quote_batch.requested_at).total_seconds()
    if collection_seconds > policy.maximum_batch_collection_seconds:
        raise SwingQuoteGateError(
            "quote batch collection duration exceeded the policy maximum"
        )

    outcomes: list[SwingQuoteGateOutcome] = []
    for proposal, quote in zip(proposal_batch.proposals, quote_batch.quotes, strict=True):
        try:
            disposition, reasons, spread, cost, levels = _evaluate_quote_gate(
                proposal, quote, evaluated_at, policy
            )
        except SwingQuoteGateError:
            raise
        except Exception:
            raise SwingQuoteGateError("quote gate evaluation failed") from None
        outcomes.append(
            SwingQuoteGateOutcome(
                proposal=proposal,
                quote=quote,
                evaluated_at=evaluated_at,
                policy=policy,
                disposition=disposition,
                reason_codes=reasons,
                observed_spread_bps=spread,
                effective_cost_bps=cost,
                quote_adjusted_levels=levels,
            )
        )

    pass_count = sum(
        1 for value in outcomes if value.disposition is SwingQuoteGateDisposition.PASS
    )
    return SwingQuoteGateBatch(
        proposal_batch=proposal_batch,
        quote_batch=quote_batch,
        policy=policy,
        evaluated_at=evaluated_at,
        outcomes=tuple(outcomes),
        pass_count=pass_count,
        veto_count=len(outcomes) - pass_count,
    )
