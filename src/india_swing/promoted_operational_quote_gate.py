"""Pure, injected-data promoted operational quote gate.

Consumes one already-verified ``VerifiedPromotedOperationalPreparation``
plus one already-acquired, exact ``FullQuoteBatch`` and produces one
PASS/VETO outcome per retained candidate, using the same shared
proposal-independent quote-quality evaluator the legacy Swing quote gate
uses, plus promoted-intent-native limit/stop/target/tick checks. It never
acquires a quote, ranks a candidate, sizes a position, produces a BUY
decision, notifies, registers a paper trade, or executes. Every outcome and
batch produced here is ``paper_only=True`` and permanently
``notification_eligible=False``/``execution_eligible=False``.
"""

from __future__ import annotations

from dataclasses import dataclass, field, fields
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from india_swing.domain.models import INDIA_STANDARD_TIME
from india_swing.identity import content_id
from india_swing.market_data.models import FullQuoteBatch, KiteFullQuote
from india_swing.promoted_operational_preparation import (
    PromotedOperationalCandidate,
    VerifiedPromotedOperationalPreparation,
)
from india_swing.signals.quote_gate import SwingQuoteGateDisposition, SwingQuoteGatePolicy
from india_swing.signals.quote_quality import QuoteQualityError, evaluate_quote_quality


PROMOTED_OPERATIONAL_QUOTE_GATE_SPEC_SCHEMA_VERSION = (
    "promoted-operational-quote-gate-spec/v1"
)
PROMOTED_OPERATIONAL_QUOTE_OUTCOME_SCHEMA_VERSION = (
    "promoted-operational-quote-outcome/v1"
)
PROMOTED_OPERATIONAL_QUOTE_GATE_BATCH_SCHEMA_VERSION = (
    "promoted-operational-quote-gate-batch/v1"
)

REASON_BEST_ASK_ABOVE_LIMIT = "BEST_ASK_ABOVE_LIMIT"
REASON_BEST_ASK_AT_OR_BELOW_STOP = "BEST_ASK_AT_OR_BELOW_STOP"
REASON_BEST_ASK_AT_OR_ABOVE_TARGET = "BEST_ASK_AT_OR_ABOVE_TARGET"
REASON_QUOTE_TICK_MISMATCH = "QUOTE_TICK_MISMATCH"


class PromotedOperationalQuoteGateError(ValueError):
    pass


_ERR_TYPE = "promoted operational quote gate type is invalid"
_ERR_WINDOW = "promoted operational quote gate decision window is invalid"
_ERR_AUTHORITY = "promoted operational quote gate authority flags are invalid"
_ERR_COVERAGE = "promoted operational quote gate coverage is invalid"
_ERR_OUTCOME = "promoted operational quote outcome is invalid"
_ERR_REPLAY = "promoted operational quote gate could not replay"


def _require_aware_utc(value: object, message: str) -> datetime:
    if type(value) is not datetime:
        raise PromotedOperationalQuoteGateError(message)
    try:
        offset = value.utcoffset()
    except Exception:
        raise PromotedOperationalQuoteGateError(message) from None
    if value.tzinfo is None or offset is None:
        raise PromotedOperationalQuoteGateError(message)
    return value.astimezone(timezone.utc)


def _require_utc_representation(value: datetime, message: str) -> None:
    """Require ``value`` to be represented with a literal zero UTC offset.

    An aware datetime can represent the same instant under many different
    offsets; only rejecting non-UTC *instants* (via equality) would still
    accept an equivalent-instant, non-UTC *representation*, which would let
    a content ID computed over that representation silently diverge from
    the canonical one. This checks the representation itself.
    """

    if type(value) is not datetime or value.utcoffset() != timedelta(0):
        raise PromotedOperationalQuoteGateError(message)


def _expected_quote_transport_keys(listing_keys: tuple[str, ...]) -> tuple[str, ...]:
    """Sorted quote-acquisition transport order.

    Quote acquisition uses sorted unique listing keys (matching
    ``FullQuoteBatch``'s own canonical-order requirement); this is a
    transport-only canonicalization and never reorders, drops, or mutates
    the preparation's own candidate order, which outcomes always preserve.
    """

    return tuple(sorted(listing_keys))


def _decimal_coefficient_at_exponent(value: Decimal, target_exponent: int) -> int:
    """Return the exact integer coefficient of ``value`` scaled to
    ``target_exponent`` (which must be <= ``value``'s own exponent), using
    only ``Decimal.as_tuple()`` and Python integer arithmetic -- never a
    context-sensitive Decimal operator such as ``%`` or ``/``."""

    sign, digits, exponent = value.as_tuple()
    coefficient = 0
    for digit in digits:
        coefficient = coefficient * 10 + digit
    coefficient *= 10 ** (exponent - target_exponent)
    return -coefficient if sign else coefficient


def _is_exact_tick_multiple(value: Decimal, tick_size: Decimal) -> bool:
    """Whether ``value`` is an exact multiple of ``tick_size``, independent
    of the caller's ambient Decimal context (precision/rounding)."""

    value_exponent = value.as_tuple().exponent
    tick_exponent = tick_size.as_tuple().exponent
    if not isinstance(value_exponent, int) or not isinstance(tick_exponent, int):
        return False
    target_exponent = min(value_exponent, tick_exponent)
    value_int = _decimal_coefficient_at_exponent(value, target_exponent)
    tick_int = _decimal_coefficient_at_exponent(tick_size, target_exponent)
    if tick_int == 0:
        return False
    return value_int % tick_int == 0


@dataclass(frozen=True, slots=True)
class PromotedOperationalQuoteGateSpec:
    """One exact, content-addressed binding of a preparation, decision
    window, and quote-gate policy. Grants no authority: ``paper_only`` is
    always true and both ``notification_eligible``/``execution_eligible``
    are always false."""

    preparation: VerifiedPromotedOperationalPreparation
    decision_not_before: datetime
    decision_deadline: datetime
    policy: SwingQuoteGatePolicy
    paper_only: bool
    notification_eligible: bool
    execution_eligible: bool
    spec_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.preparation) is not VerifiedPromotedOperationalPreparation:
            raise PromotedOperationalQuoteGateError(_ERR_TYPE)
        self.preparation.verify_content_identity()
        if type(self.policy) is not SwingQuoteGatePolicy:
            raise PromotedOperationalQuoteGateError(_ERR_TYPE)
        self.policy.verify_content_identity()

        not_before = _require_aware_utc(self.decision_not_before, _ERR_WINDOW)
        deadline = _require_aware_utc(self.decision_deadline, _ERR_WINDOW)
        if not_before >= deadline:
            raise PromotedOperationalQuoteGateError(_ERR_WINDOW)
        target_session = self.preparation.manifest.target_session
        if (
            not_before.astimezone(INDIA_STANDARD_TIME).date() != target_session
            or deadline.astimezone(INDIA_STANDARD_TIME).date() != target_session
        ):
            raise PromotedOperationalQuoteGateError(_ERR_WINDOW)
        object.__setattr__(self, "decision_not_before", not_before)
        object.__setattr__(self, "decision_deadline", deadline)

        if (
            self.paper_only is not True
            or self.notification_eligible is not False
            or self.execution_eligible is not False
        ):
            raise PromotedOperationalQuoteGateError(_ERR_AUTHORITY)

        object.__setattr__(self, "spec_id", self._calculated_id())

    def _identity(self) -> dict[str, object]:
        return {
            value.name: getattr(self, value.name)
            for value in fields(self)
            if value.name != "spec_id"
        }

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": PROMOTED_OPERATIONAL_QUOTE_GATE_SPEC_SCHEMA_VERSION,
                **self._identity(),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if type(self) is not PromotedOperationalQuoteGateSpec:
            raise PromotedOperationalQuoteGateError(_ERR_TYPE)
        try:
            fresh = PromotedOperationalQuoteGateSpec(
                preparation=self.preparation,
                decision_not_before=self.decision_not_before,
                decision_deadline=self.decision_deadline,
                policy=self.policy,
                paper_only=self.paper_only,
                notification_eligible=self.notification_eligible,
                execution_eligible=self.execution_eligible,
            )
        except PromotedOperationalQuoteGateError:
            raise
        except Exception:
            raise PromotedOperationalQuoteGateError(_ERR_TYPE) from None
        if self.spec_id != fresh.spec_id:
            raise PromotedOperationalQuoteGateError(_ERR_TYPE)


def _evaluate_promoted_candidate_quote(
    candidate: PromotedOperationalCandidate,
    quote: KiteFullQuote,
    spec: PromotedOperationalQuoteGateSpec,
    evaluated_at: datetime,
) -> tuple[SwingQuoteGateDisposition, tuple[str, ...], Decimal | None, Decimal | None]:
    """Pure, replay-verifiable evaluation of one candidate against one quote."""

    entry_order = candidate.research_intent.evaluation_intent.entry_order
    stop_price = candidate.research_intent.evaluation_intent.stop_price
    target_price = candidate.research_intent.evaluation_intent.target_price

    try:
        quality = evaluate_quote_quality(
            quote=quote,
            expected_listing_key=candidate.listing_key,
            decision_not_before=spec.decision_not_before,
            decision_deadline=spec.decision_deadline,
            evaluated_at=evaluated_at,
            maximum_quote_age_seconds=spec.policy.maximum_quote_age_seconds,
            maximum_last_trade_age_seconds=spec.policy.maximum_last_trade_age_seconds,
            maximum_spread_bps=spec.policy.maximum_spread_bps,
        )
    except QuoteQualityError as exc:
        raise PromotedOperationalQuoteGateError(str(exc)) from None

    reasons: set[str] = set(quality.reason_codes)
    observed_spread_bps = quality.observed_spread_bps

    best_ask = quote.best_ask
    if best_ask is not None:
        if best_ask > entry_order.limit_price:
            reasons.add(REASON_BEST_ASK_ABOVE_LIMIT)
        if best_ask <= stop_price:
            reasons.add(REASON_BEST_ASK_AT_OR_BELOW_STOP)
        if best_ask >= target_price:
            reasons.add(REASON_BEST_ASK_AT_OR_ABOVE_TARGET)

    tick_size = entry_order.tick_size
    for value in (quote.last_price, quote.best_bid, best_ask):
        if value is not None and not _is_exact_tick_multiple(value, tick_size):
            reasons.add(REASON_QUOTE_TICK_MISMATCH)
            break

    if reasons:
        return SwingQuoteGateDisposition.VETO, tuple(sorted(reasons)), observed_spread_bps, None
    return SwingQuoteGateDisposition.PASS, (), observed_spread_bps, best_ask


@dataclass(frozen=True, slots=True)
class PromotedOperationalQuoteOutcome:
    """One deterministic, replayable PASS/VETO outcome for one exact
    promoted operational candidate.

    Never changes quantity, limit, stop, target, cost buffer, reward/risk,
    holding period, or any other retained research-intent field.
    """

    candidate: PromotedOperationalCandidate
    quote: KiteFullQuote
    spec: PromotedOperationalQuoteGateSpec
    evaluated_at: datetime
    disposition: SwingQuoteGateDisposition
    reason_codes: tuple[str, ...]
    observed_spread_bps: Decimal | None
    reference_entry_price: Decimal | None
    outcome_id: str = field(init=False)

    def __post_init__(self) -> None:
        normalized_evaluated_at = _require_aware_utc(self.evaluated_at, _ERR_WINDOW)
        object.__setattr__(self, "evaluated_at", normalized_evaluated_at)
        self._verify()
        object.__setattr__(self, "outcome_id", self._calculated_id())

    def _verify(self) -> None:
        if type(self.candidate) is not PromotedOperationalCandidate:
            raise PromotedOperationalQuoteGateError(_ERR_TYPE)
        self.candidate.verify_content_identity()
        if type(self.quote) is not KiteFullQuote:
            raise PromotedOperationalQuoteGateError(_ERR_TYPE)
        self.quote.verify_content_identity()
        if self.quote.listing_key != self.candidate.listing_key:
            raise PromotedOperationalQuoteGateError(_ERR_COVERAGE)
        if type(self.spec) is not PromotedOperationalQuoteGateSpec:
            raise PromotedOperationalQuoteGateError(_ERR_TYPE)
        self.spec.verify_content_identity()

        matching_candidates = [
            value
            for value in self.spec.preparation.candidates
            if value.candidate_id == self.candidate.candidate_id
        ]
        if len(matching_candidates) != 1:
            raise PromotedOperationalQuoteGateError(_ERR_COVERAGE)
        matching_candidate = matching_candidates[0]
        matching_candidate.verify_content_identity()
        if matching_candidate != self.candidate:
            raise PromotedOperationalQuoteGateError(_ERR_COVERAGE)

        evaluated_at = _require_aware_utc(self.evaluated_at, _ERR_WINDOW)
        _require_utc_representation(self.evaluated_at, _ERR_WINDOW)
        if type(self.disposition) is not SwingQuoteGateDisposition:
            raise PromotedOperationalQuoteGateError(_ERR_TYPE)

        try:
            replayed = _evaluate_promoted_candidate_quote(
                self.candidate, self.quote, self.spec, evaluated_at
            )
        except PromotedOperationalQuoteGateError:
            raise
        except Exception:
            raise PromotedOperationalQuoteGateError(_ERR_REPLAY) from None
        replayed_disposition, replayed_reasons, replayed_spread, replayed_reference = replayed

        if replayed_disposition is not self.disposition:
            raise PromotedOperationalQuoteGateError(_ERR_OUTCOME)
        if (
            type(self.reason_codes) is not tuple
            or self.reason_codes != replayed_reasons
            or self.reason_codes != tuple(sorted(set(self.reason_codes)))
        ):
            raise PromotedOperationalQuoteGateError(_ERR_OUTCOME)
        if self.observed_spread_bps != replayed_spread:
            raise PromotedOperationalQuoteGateError(_ERR_OUTCOME)

        if self.disposition is SwingQuoteGateDisposition.PASS:
            if self.reason_codes:
                raise PromotedOperationalQuoteGateError(_ERR_OUTCOME)
            if (
                self.observed_spread_bps is None
                or self.reference_entry_price is None
                or self.reference_entry_price != self.quote.best_ask
                or self.reference_entry_price != replayed_reference
            ):
                raise PromotedOperationalQuoteGateError(_ERR_OUTCOME)
        else:
            if not self.reason_codes:
                raise PromotedOperationalQuoteGateError(_ERR_OUTCOME)
            if self.reference_entry_price is not None:
                raise PromotedOperationalQuoteGateError(_ERR_OUTCOME)

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": PROMOTED_OPERATIONAL_QUOTE_OUTCOME_SCHEMA_VERSION,
                **{
                    value.name: getattr(self, value.name)
                    for value in fields(self)
                    if value.name != "outcome_id"
                },
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._verify()
        if self.outcome_id != self._calculated_id():
            raise PromotedOperationalQuoteGateError(_ERR_OUTCOME)

    @property
    def passed(self) -> bool:
        return self.disposition is SwingQuoteGateDisposition.PASS

    @property
    def execution_eligible(self) -> bool:
        return False


@dataclass(frozen=True, slots=True)
class VerifiedPromotedOperationalQuoteGateBatch:
    """Exact, content-addressed one-outcome-per-candidate promoted
    operational quote-gate result.

    A zero-candidate preparation is valid only with ``quote_batch=None``
    and yields an exact zero-outcome batch. A nonempty preparation
    requires an exact ``FullQuoteBatch`` whose ``requested_keys`` are the
    sorted transport form of the preparation manifest's ``listing_keys``.
    Outcomes still preserve the preparation's candidate order.
    """

    spec: PromotedOperationalQuoteGateSpec
    quote_batch: FullQuoteBatch | None
    evaluated_at: datetime
    outcomes: tuple[PromotedOperationalQuoteOutcome, ...]
    pass_count: int
    veto_count: int
    paper_only: bool
    notification_eligible: bool
    execution_eligible: bool
    batch_id: str = field(init=False)

    def __post_init__(self) -> None:
        normalized_evaluated_at = _require_aware_utc(self.evaluated_at, _ERR_WINDOW)
        object.__setattr__(self, "evaluated_at", normalized_evaluated_at)
        if type(self.pass_count) is not int or self.pass_count < 0:
            raise PromotedOperationalQuoteGateError(_ERR_COVERAGE)
        if type(self.veto_count) is not int or self.veto_count < 0:
            raise PromotedOperationalQuoteGateError(_ERR_COVERAGE)
        if (
            self.paper_only is not True
            or self.notification_eligible is not False
            or self.execution_eligible is not False
        ):
            raise PromotedOperationalQuoteGateError(_ERR_AUTHORITY)
        self._verify_coverage()
        object.__setattr__(self, "batch_id", self._calculated_id())

    def _verify_coverage(self) -> None:
        if type(self.spec) is not PromotedOperationalQuoteGateSpec:
            raise PromotedOperationalQuoteGateError(_ERR_TYPE)
        self.spec.verify_content_identity()
        evaluated_at = _require_aware_utc(self.evaluated_at, _ERR_WINDOW)
        _require_utc_representation(self.evaluated_at, _ERR_WINDOW)

        candidates = self.spec.preparation.candidates
        listing_keys = self.spec.preparation.manifest.listing_keys
        expected_quote_keys = _expected_quote_transport_keys(listing_keys)

        if not candidates:
            if self.quote_batch is not None:
                raise PromotedOperationalQuoteGateError(_ERR_COVERAGE)
        else:
            if type(self.quote_batch) is not FullQuoteBatch:
                raise PromotedOperationalQuoteGateError(_ERR_COVERAGE)
            self.quote_batch.verify_content_identity()
            if self.quote_batch.requested_keys != expected_quote_keys:
                raise PromotedOperationalQuoteGateError(_ERR_COVERAGE)
            if self.quote_batch.observed_at > evaluated_at:
                raise PromotedOperationalQuoteGateError(_ERR_COVERAGE)
            collection_seconds = (
                self.quote_batch.observed_at - self.quote_batch.requested_at
            ).total_seconds()
            if collection_seconds > self.spec.policy.maximum_batch_collection_seconds:
                raise PromotedOperationalQuoteGateError(_ERR_COVERAGE)

        if type(self.outcomes) is not tuple or any(
            type(value) is not PromotedOperationalQuoteOutcome for value in self.outcomes
        ):
            raise PromotedOperationalQuoteGateError(_ERR_COVERAGE)
        if len(self.outcomes) != len(candidates):
            raise PromotedOperationalQuoteGateError(_ERR_COVERAGE)
        for value in self.outcomes:
            value.verify_content_identity()

        quote_by_key = (
            {quote.listing_key: quote for quote in self.quote_batch.quotes}
            if self.quote_batch is not None
            else {}
        )
        for index, outcome in enumerate(self.outcomes):
            candidate = candidates[index]
            if outcome.candidate.candidate_id != candidate.candidate_id:
                raise PromotedOperationalQuoteGateError(_ERR_COVERAGE)
            expected_quote = quote_by_key.get(candidate.listing_key)
            if expected_quote is None or outcome.quote != expected_quote:
                raise PromotedOperationalQuoteGateError(_ERR_COVERAGE)
            if outcome.spec.spec_id != self.spec.spec_id:
                raise PromotedOperationalQuoteGateError(_ERR_COVERAGE)
            if outcome.evaluated_at != evaluated_at:
                raise PromotedOperationalQuoteGateError(_ERR_COVERAGE)

        expected_pass = sum(
            1 for value in self.outcomes if value.disposition is SwingQuoteGateDisposition.PASS
        )
        expected_veto = len(self.outcomes) - expected_pass
        if self.pass_count != expected_pass or self.veto_count != expected_veto:
            raise PromotedOperationalQuoteGateError(_ERR_COVERAGE)

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": PROMOTED_OPERATIONAL_QUOTE_GATE_BATCH_SCHEMA_VERSION,
                **{
                    value.name: getattr(self, value.name)
                    for value in fields(self)
                    if value.name != "batch_id"
                },
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._verify_coverage()
        if self.batch_id != self._calculated_id():
            raise PromotedOperationalQuoteGateError(_ERR_COVERAGE)


def evaluate_promoted_operational_quote_gate(
    *,
    spec: PromotedOperationalQuoteGateSpec,
    quote_batch: FullQuoteBatch | None,
    evaluated_at: datetime,
) -> VerifiedPromotedOperationalQuoteGateBatch:
    """Bind one exact spec to one exact (or absent) quote batch at one exact
    evaluation time.

    Every candidate receives exactly one PASS or VETO outcome. This
    function never fetches or discovers a quote, never ranks or sizes a
    candidate, and never grants notification or execution authority.
    """

    if type(spec) is not PromotedOperationalQuoteGateSpec:
        raise PromotedOperationalQuoteGateError(_ERR_TYPE)
    spec.verify_content_identity()
    evaluated_at = _require_aware_utc(evaluated_at, _ERR_WINDOW)

    candidates = spec.preparation.candidates
    listing_keys = spec.preparation.manifest.listing_keys
    expected_quote_keys = _expected_quote_transport_keys(listing_keys)

    if not candidates:
        if quote_batch is not None:
            raise PromotedOperationalQuoteGateError(_ERR_COVERAGE)
        return VerifiedPromotedOperationalQuoteGateBatch(
            spec=spec,
            quote_batch=None,
            evaluated_at=evaluated_at,
            outcomes=(),
            pass_count=0,
            veto_count=0,
            paper_only=True,
            notification_eligible=False,
            execution_eligible=False,
        )

    if type(quote_batch) is not FullQuoteBatch:
        raise PromotedOperationalQuoteGateError(_ERR_COVERAGE)
    quote_batch.verify_content_identity()
    if quote_batch.requested_keys != expected_quote_keys:
        raise PromotedOperationalQuoteGateError(_ERR_COVERAGE)
    if quote_batch.observed_at > evaluated_at:
        raise PromotedOperationalQuoteGateError(_ERR_COVERAGE)
    collection_seconds = (
        quote_batch.observed_at - quote_batch.requested_at
    ).total_seconds()
    if collection_seconds > spec.policy.maximum_batch_collection_seconds:
        raise PromotedOperationalQuoteGateError(_ERR_COVERAGE)

    quote_by_key = {quote.listing_key: quote for quote in quote_batch.quotes}
    outcomes: list[PromotedOperationalQuoteOutcome] = []
    for candidate in candidates:
        quote = quote_by_key[candidate.listing_key]
        try:
            disposition, reasons, spread, reference_entry_price = (
                _evaluate_promoted_candidate_quote(candidate, quote, spec, evaluated_at)
            )
        except PromotedOperationalQuoteGateError:
            raise
        except Exception:
            raise PromotedOperationalQuoteGateError(_ERR_REPLAY) from None
        outcomes.append(
            PromotedOperationalQuoteOutcome(
                candidate=candidate,
                quote=quote,
                spec=spec,
                evaluated_at=evaluated_at,
                disposition=disposition,
                reason_codes=reasons,
                observed_spread_bps=spread,
                reference_entry_price=reference_entry_price,
            )
        )

    pass_count = sum(
        1 for value in outcomes if value.disposition is SwingQuoteGateDisposition.PASS
    )
    return VerifiedPromotedOperationalQuoteGateBatch(
        spec=spec,
        quote_batch=quote_batch,
        evaluated_at=evaluated_at,
        outcomes=tuple(outcomes),
        pass_count=pass_count,
        veto_count=len(outcomes) - pass_count,
        paper_only=True,
        notification_eligible=False,
        execution_eligible=False,
    )
