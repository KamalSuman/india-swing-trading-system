from __future__ import annotations

import decimal
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from india_swing.domain.models import INDIA_STANDARD_TIME
from india_swing.market_data.models import FullQuoteBatch, KiteDepthLevel, KiteFullQuote
from india_swing.promoted_operational_preparation import (
    PromotedOperationalPreparationService,
)
from india_swing.promoted_operational_quote_gate import (
    PromotedOperationalQuoteGateError,
    PromotedOperationalQuoteGateSpec,
    PromotedOperationalQuoteOutcome,
    VerifiedPromotedOperationalQuoteGateBatch,
    evaluate_promoted_operational_quote_gate,
)
from india_swing.signals.quote_gate import SwingQuoteGateDisposition, SwingQuoteGatePolicy

from tests.test_promoted_operational_preparation import (
    _EMPTY_LINEAGE,
    _NONEMPTY_LINEAGE,
    _build_batch,
    _build_lineage,
    _selected_decision,
    _selected_intent,
)


_DECISION_NOT_BEFORE = datetime(2026, 7, 17, 9, 15, tzinfo=INDIA_STANDARD_TIME)
_DECISION_DEADLINE = datetime(2026, 7, 17, 9, 20, tzinfo=INDIA_STANDARD_TIME)
_EVALUATED_AT = datetime(2026, 7, 17, 9, 17, tzinfo=INDIA_STANDARD_TIME)


def _build_non_alphabetical_lineage():
    """A second, wholly separate in-memory lineage whose two candidates are
    in TCS-then-RELIANCE order -- the reverse of alphabetical -- so tests
    can distinguish sorted quote-transport order from preserved candidate
    (outcome) order."""

    decision_first = _selected_decision(
        source_result_id="1" * 64,
        source_feature_id="e1" * 32,
        opportunity_id="a1" * 32,
        stable_instrument_id="f1" * 32,
        stable_listing_id="a2" * 32,
    )
    decision_second = _selected_decision(
        source_result_id="2" * 64,
        source_feature_id="e2" * 32,
        opportunity_id="a3" * 32,
        stable_instrument_id="f2" * 32,
        stable_listing_id="a4" * 32,
    )
    intent_first = _selected_intent(
        decision=decision_first, symbol="TCS", universe_snapshot_id="5" * 64
    )
    intent_second = _selected_intent(
        decision=decision_second, symbol="RELIANCE", universe_snapshot_id="6" * 64
    )
    batch = _build_batch(
        decisions=(decision_first, decision_second),
        intents=(intent_first, intent_second),
    )
    return _build_lineage(batch)


def _depth(price: str, quantity: int = 10, orders: int = 2) -> KiteDepthLevel:
    return KiteDepthLevel(price=Decimal(price), quantity=quantity, orders=orders)


def _quote(
    *,
    listing_key: str,
    instrument_token: int,
    last_price: Decimal,
    lower_circuit: Decimal,
    upper_circuit: Decimal,
    exchange_timestamp: datetime,
    last_trade_time: datetime | None,
    best_bid: Decimal | None,
    best_ask: Decimal | None,
) -> KiteFullQuote:
    return KiteFullQuote(
        listing_key=listing_key,
        instrument_token=instrument_token,
        exchange_timestamp=exchange_timestamp,
        last_trade_time=last_trade_time,
        last_price=last_price,
        lower_circuit_limit=lower_circuit,
        upper_circuit_limit=upper_circuit,
        depth_buy=(_depth(str(best_bid)),) if best_bid is not None else (),
        depth_sell=(_depth(str(best_ask)),) if best_ask is not None else (),
    )


class PromotedOperationalQuoteGateTests(unittest.TestCase):
    def _preparation(self):
        research_run_manifest, engine_run_manifest, batch = _NONEMPTY_LINEAGE
        return PromotedOperationalPreparationService().prepare(
            research_run_manifest=research_run_manifest,
            engine_run_manifest=engine_run_manifest,
            research_intent_batch=batch,
        )

    def _empty_preparation(self):
        research_run_manifest, engine_run_manifest, batch = _EMPTY_LINEAGE
        return PromotedOperationalPreparationService().prepare(
            research_run_manifest=research_run_manifest,
            engine_run_manifest=engine_run_manifest,
            research_intent_batch=batch,
        )

    def _spec(self, preparation, *, policy=None) -> PromotedOperationalQuoteGateSpec:
        return PromotedOperationalQuoteGateSpec(
            preparation=preparation,
            decision_not_before=_DECISION_NOT_BEFORE,
            decision_deadline=_DECISION_DEADLINE,
            policy=policy or SwingQuoteGatePolicy(),
            paper_only=True,
            notification_eligible=False,
            execution_eligible=False,
        )

    def _happy_quote_for(self, candidate, *, instrument_token: int, evaluated_at=None):
        evaluated_at = evaluated_at or _EVALUATED_AT
        return _quote(
            listing_key=candidate.listing_key,
            instrument_token=instrument_token,
            last_price=Decimal("99.95"),
            lower_circuit=Decimal("90"),
            upper_circuit=Decimal("110"),
            exchange_timestamp=evaluated_at - timedelta(seconds=2),
            last_trade_time=evaluated_at - timedelta(seconds=2),
            best_bid=Decimal("99.90"),
            best_ask=Decimal("99.95"),
        )

    def _happy_quotes(self, preparation, *, evaluated_at=None):
        return tuple(
            self._happy_quote_for(candidate, instrument_token=2000 + index, evaluated_at=evaluated_at)
            for index, candidate in enumerate(preparation.candidates)
        )

    def _happy_quote_batch(self, preparation, *, evaluated_at=None) -> FullQuoteBatch:
        evaluated_at = evaluated_at or _EVALUATED_AT
        return FullQuoteBatch(
            requested_keys=preparation.manifest.listing_keys,
            requested_at=evaluated_at - timedelta(seconds=3),
            observed_at=evaluated_at - timedelta(seconds=1),
            provider_version="kiteconnect/5.2.0",
            quotes=self._happy_quotes(preparation, evaluated_at=evaluated_at),
        )

    def test_non_alphabetical_candidate_order_uses_sorted_quote_transport_and_preserves_outcome_order(
        self,
    ) -> None:
        research_run_manifest, engine_run_manifest, batch = _build_non_alphabetical_lineage()
        preparation = PromotedOperationalPreparationService().prepare(
            research_run_manifest=research_run_manifest,
            engine_run_manifest=engine_run_manifest,
            research_intent_batch=batch,
        )
        # Candidate/manifest order is exactly TCS-then-RELIANCE (unsorted).
        self.assertEqual(
            preparation.manifest.listing_keys, ("NSE:TCS", "NSE:RELIANCE")
        )
        spec = self._spec(preparation)

        sorted_candidates = sorted(preparation.candidates, key=lambda value: value.listing_key)
        quote_batch = FullQuoteBatch(
            requested_keys=tuple(value.listing_key for value in sorted_candidates),
            requested_at=_EVALUATED_AT - timedelta(seconds=3),
            observed_at=_EVALUATED_AT - timedelta(seconds=1),
            provider_version="kiteconnect/5.2.0",
            quotes=tuple(
                self._happy_quote_for(candidate, instrument_token=7000 + index)
                for index, candidate in enumerate(sorted_candidates)
            ),
        )
        self.assertEqual(
            quote_batch.requested_keys, ("NSE:RELIANCE", "NSE:TCS")
        )

        gate_batch = evaluate_promoted_operational_quote_gate(
            spec=spec, quote_batch=quote_batch, evaluated_at=_EVALUATED_AT
        )

        self.assertEqual(gate_batch.pass_count, 2)
        self.assertEqual(gate_batch.veto_count, 0)
        # Outcomes stay in the preparation's own candidate order, never
        # reranked to match the sorted transport order.
        self.assertEqual(
            tuple(outcome.candidate.listing_key for outcome in gate_batch.outcomes),
            ("NSE:TCS", "NSE:RELIANCE"),
        )
        gate_batch.verify_content_identity()

    def test_foreign_candidate_cannot_form_standalone_outcome_for_spec(self) -> None:
        preparation = self._preparation()
        spec = self._spec(preparation)

        (
            foreign_research_run_manifest,
            foreign_engine_run_manifest,
            foreign_batch,
        ) = _build_non_alphabetical_lineage()
        foreign_preparation = PromotedOperationalPreparationService().prepare(
            research_run_manifest=foreign_research_run_manifest,
            engine_run_manifest=foreign_engine_run_manifest,
            research_intent_batch=foreign_batch,
        )
        foreign_candidate = foreign_preparation.candidates[0]
        # The foreign candidate is self-consistent on its own terms.
        foreign_candidate.verify_content_identity()
        foreign_quote = self._happy_quote_for(foreign_candidate, instrument_token=8000)

        with self.assertRaises(PromotedOperationalQuoteGateError):
            PromotedOperationalQuoteOutcome(
                candidate=foreign_candidate,
                quote=foreign_quote,
                spec=spec,
                evaluated_at=_EVALUATED_AT,
                disposition=SwingQuoteGateDisposition.PASS,
                reason_codes=(),
                observed_spread_bps=Decimal("5"),
                reference_entry_price=Decimal("99.95"),
            )

    def test_evaluated_at_is_utc_canonical_for_outcome_and_batch(self) -> None:
        preparation = self._preparation()
        spec = self._spec(preparation)
        quote_batch = self._happy_quote_batch(preparation)

        evaluated_at_ist = _EVALUATED_AT
        evaluated_at_utc = _EVALUATED_AT.astimezone(timezone.utc)

        gate_batch_from_ist = evaluate_promoted_operational_quote_gate(
            spec=spec, quote_batch=quote_batch, evaluated_at=evaluated_at_ist
        )
        gate_batch_from_utc = evaluate_promoted_operational_quote_gate(
            spec=spec, quote_batch=quote_batch, evaluated_at=evaluated_at_utc
        )

        self.assertEqual(gate_batch_from_ist.evaluated_at.utcoffset(), timedelta(0))
        self.assertEqual(gate_batch_from_utc.evaluated_at.utcoffset(), timedelta(0))
        self.assertEqual(gate_batch_from_ist.batch_id, gate_batch_from_utc.batch_id)
        for outcome_a, outcome_b in zip(
            gate_batch_from_ist.outcomes, gate_batch_from_utc.outcomes
        ):
            self.assertEqual(outcome_a.evaluated_at.utcoffset(), timedelta(0))
            self.assertEqual(outcome_a.outcome_id, outcome_b.outcome_id)

        # Tamper each retained evaluated_at to an equivalent non-UTC offset
        # and recompute a self-consistent ID -- still fails, since
        # verify_content_identity independently requires a literal zero
        # UTC offset rather than merely rehashing whatever is retained.
        outcome = gate_batch_from_utc.outcomes[0]
        tampered_outcome_evaluated_at = outcome.evaluated_at.astimezone(
            INDIA_STANDARD_TIME
        )
        self.assertEqual(tampered_outcome_evaluated_at, outcome.evaluated_at)
        object.__setattr__(outcome, "evaluated_at", tampered_outcome_evaluated_at)
        object.__setattr__(outcome, "outcome_id", outcome._calculated_id())
        with self.assertRaises(PromotedOperationalQuoteGateError):
            outcome.verify_content_identity()

        tampered_batch_evaluated_at = gate_batch_from_utc.evaluated_at.astimezone(
            INDIA_STANDARD_TIME
        )
        object.__setattr__(
            gate_batch_from_utc, "evaluated_at", tampered_batch_evaluated_at
        )
        object.__setattr__(
            gate_batch_from_utc, "batch_id", gate_batch_from_utc._calculated_id()
        )
        with self.assertRaises(PromotedOperationalQuoteGateError):
            gate_batch_from_utc.verify_content_identity()

    def test_tick_alignment_is_independent_of_global_decimal_context(self) -> None:
        preparation = self._preparation()
        spec = self._spec(preparation)
        candidate = preparation.candidates[0]
        other_candidate = preparation.candidates[1]
        other_quote = self._happy_quote_for(other_candidate, instrument_token=9000)

        tick_size = candidate.research_intent.evaluation_intent.entry_order.tick_size
        self.assertEqual(tick_size, Decimal("0.05"))

        aligned_quote = self._happy_quote_for(candidate, instrument_token=9001)
        # 99.95 / 0.05 == 1999 -- an integer quotient with far more digits
        # than a precision-1 context allows, which is exactly the case a
        # context-sensitive Decimal '%' can get wrong (or raise on).
        misaligned_quote = replace(aligned_quote, last_price=Decimal("99.97"))

        original_precision = decimal.getcontext().prec
        decimal.getcontext().prec = 1
        try:
            aligned_batch = FullQuoteBatch(
                requested_keys=tuple(sorted(preparation.manifest.listing_keys)),
                requested_at=_EVALUATED_AT - timedelta(seconds=3),
                observed_at=_EVALUATED_AT - timedelta(seconds=1),
                provider_version="kiteconnect/5.2.0",
                quotes=tuple(
                    sorted(
                        (aligned_quote, other_quote), key=lambda value: value.listing_key
                    )
                ),
            )
            aligned_gate_batch = evaluate_promoted_operational_quote_gate(
                spec=spec, quote_batch=aligned_batch, evaluated_at=_EVALUATED_AT
            )
            aligned_outcome = next(
                value
                for value in aligned_gate_batch.outcomes
                if value.candidate.candidate_id == candidate.candidate_id
            )
            self.assertNotIn("QUOTE_TICK_MISMATCH", aligned_outcome.reason_codes)

            misaligned_batch = FullQuoteBatch(
                requested_keys=tuple(sorted(preparation.manifest.listing_keys)),
                requested_at=_EVALUATED_AT - timedelta(seconds=3),
                observed_at=_EVALUATED_AT - timedelta(seconds=1),
                provider_version="kiteconnect/5.2.0",
                quotes=tuple(
                    sorted(
                        (misaligned_quote, other_quote),
                        key=lambda value: value.listing_key,
                    )
                ),
            )
            misaligned_gate_batch = evaluate_promoted_operational_quote_gate(
                spec=spec, quote_batch=misaligned_batch, evaluated_at=_EVALUATED_AT
            )
            misaligned_outcome = next(
                value
                for value in misaligned_gate_batch.outcomes
                if value.candidate.candidate_id == candidate.candidate_id
            )
            self.assertIn("QUOTE_TICK_MISMATCH", misaligned_outcome.reason_codes)
        finally:
            decimal.getcontext().prec = original_precision

    def test_fresh_in_window_limit_compatible_quotes_pass_with_exact_reference_ask(
        self,
    ) -> None:
        preparation = self._preparation()
        spec = self._spec(preparation)
        quote_batch = self._happy_quote_batch(preparation)

        gate_batch = evaluate_promoted_operational_quote_gate(
            spec=spec, quote_batch=quote_batch, evaluated_at=_EVALUATED_AT
        )

        self.assertEqual(gate_batch.pass_count, 2)
        self.assertEqual(gate_batch.veto_count, 0)
        for outcome, candidate in zip(gate_batch.outcomes, preparation.candidates):
            self.assertTrue(outcome.passed)
            self.assertEqual(outcome.reason_codes, ())
            self.assertIsNotNone(outcome.observed_spread_bps)
            self.assertEqual(outcome.reference_entry_price, Decimal("99.95"))
            self.assertEqual(
                candidate.research_intent.evaluation_intent.stop_price, Decimal("90.5")
            )
            self.assertEqual(
                candidate.research_intent.evaluation_intent.entry_order.limit_price,
                Decimal("100.0"),
            )
            self.assertFalse(outcome.execution_eligible)
        self.assertTrue(gate_batch.paper_only)
        self.assertFalse(gate_batch.notification_eligible)
        self.assertFalse(gate_batch.execution_eligible)
        gate_batch.verify_content_identity()

    def test_quote_gate_identity_replay_is_ambient_decimal_context_independent(
        self,
    ) -> None:
        preparation = self._preparation()
        spec = self._spec(preparation)
        quote_batch = self._happy_quote_batch(preparation)
        gate_batch = evaluate_promoted_operational_quote_gate(
            spec=spec, quote_batch=quote_batch, evaluated_at=_EVALUATED_AT
        )
        gate_batch.verify_content_identity()

        original_precision = decimal.getcontext().prec
        decimal.getcontext().prec = 1
        try:
            gate_batch.verify_content_identity()
        finally:
            decimal.getcontext().prec = original_precision
        self.assertEqual(decimal.getcontext().prec, original_precision)
        gate_batch.verify_content_identity()

    def test_quality_and_intent_native_vetoes_accumulate_deterministically(self) -> None:
        preparation = self._preparation()
        spec = self._spec(preparation)
        candidate = preparation.candidates[0]
        other_candidate = preparation.candidates[1]
        base_quote = self._happy_quote_for(candidate, instrument_token=3000)
        other_quote = self._happy_quote_for(other_candidate, instrument_token=3001)

        cases = {
            "SPREAD_ABOVE_POLICY_MAX": replace(base_quote, depth_sell=(_depth("105"),)),
            "TWO_SIDED_DEPTH_MISSING": replace(base_quote, depth_sell=()),
            "CIRCUIT_LOCKED": replace(base_quote, lower_circuit_limit=Decimal("99.95")),
            "BEST_ASK_ABOVE_LIMIT": replace(base_quote, depth_sell=(_depth("100.05"),)),
            "BEST_ASK_AT_OR_BELOW_STOP": replace(
                base_quote,
                depth_buy=(_depth("85.00"),),
                depth_sell=(_depth("90.50"),),
                last_price=Decimal("90.50"),
            ),
            "BEST_ASK_AT_OR_ABOVE_TARGET": replace(
                base_quote,
                depth_sell=(_depth("130.50"),),
                upper_circuit_limit=Decimal("140"),
            ),
            "QUOTE_TICK_MISMATCH": replace(base_quote, last_price=Decimal("99.97")),
        }

        for reason, tampered_quote in cases.items():
            with self.subTest(reason=reason):
                quote_batch = FullQuoteBatch(
                    requested_keys=preparation.manifest.listing_keys,
                    requested_at=_EVALUATED_AT - timedelta(seconds=3),
                    observed_at=_EVALUATED_AT - timedelta(seconds=1),
                    provider_version="kiteconnect/5.2.0",
                    quotes=(tampered_quote, other_quote),
                )
                gate_batch = evaluate_promoted_operational_quote_gate(
                    spec=spec, quote_batch=quote_batch, evaluated_at=_EVALUATED_AT
                )
                outcome = gate_batch.outcomes[0]
                self.assertFalse(outcome.passed)
                self.assertIn(reason, outcome.reason_codes)
                self.assertIsNone(outcome.reference_entry_price)

        # No source intent value is ever changed by evaluation.
        self.assertEqual(
            candidate.research_intent.evaluation_intent.entry_order.limit_price,
            Decimal("100.0"),
        )
        self.assertEqual(
            candidate.research_intent.evaluation_intent.entry_order.quantity, 10
        )

    def test_exact_coverage_window_collection_and_time_integrity_fail_closed(self) -> None:
        preparation = self._preparation()
        spec = self._spec(preparation)
        happy_quotes = self._happy_quotes(preparation)

        # Missing key: only the first candidate's key/quote supplied.
        with self.assertRaises(Exception):
            evaluate_promoted_operational_quote_gate(
                spec=spec,
                quote_batch=FullQuoteBatch(
                    requested_keys=(preparation.manifest.listing_keys[0],),
                    requested_at=_EVALUATED_AT - timedelta(seconds=3),
                    observed_at=_EVALUATED_AT - timedelta(seconds=1),
                    provider_version="kiteconnect/5.2.0",
                    quotes=(happy_quotes[0],),
                ),
                evaluated_at=_EVALUATED_AT,
            )

        # Reordered keys (also unsorted, so FullQuoteBatch itself rejects it).
        with self.assertRaises(Exception):
            FullQuoteBatch(
                requested_keys=tuple(reversed(preparation.manifest.listing_keys)),
                requested_at=_EVALUATED_AT - timedelta(seconds=3),
                observed_at=_EVALUATED_AT - timedelta(seconds=1),
                provider_version="kiteconnect/5.2.0",
                quotes=tuple(reversed(happy_quotes)),
            )

        # Duplicate keys.
        with self.assertRaises(Exception):
            FullQuoteBatch(
                requested_keys=(
                    preparation.manifest.listing_keys[0],
                    preparation.manifest.listing_keys[0],
                ),
                requested_at=_EVALUATED_AT - timedelta(seconds=3),
                observed_at=_EVALUATED_AT - timedelta(seconds=1),
                provider_version="kiteconnect/5.2.0",
                quotes=(happy_quotes[0], happy_quotes[0]),
            )

        # Extra key beyond the preparation's candidates.
        with self.assertRaises(Exception):
            evaluate_promoted_operational_quote_gate(
                spec=spec,
                quote_batch=FullQuoteBatch(
                    requested_keys=preparation.manifest.listing_keys + ("NSE:EXTRA",),
                    requested_at=_EVALUATED_AT - timedelta(seconds=3),
                    observed_at=_EVALUATED_AT - timedelta(seconds=1),
                    provider_version="kiteconnect/5.2.0",
                    quotes=happy_quotes
                    + (
                        _quote(
                            listing_key="NSE:EXTRA",
                            instrument_token=4000,
                            last_price=Decimal("50"),
                            lower_circuit=Decimal("40"),
                            upper_circuit=Decimal("60"),
                            exchange_timestamp=_EVALUATED_AT - timedelta(seconds=2),
                            last_trade_time=_EVALUATED_AT - timedelta(seconds=2),
                            best_bid=Decimal("49.95"),
                            best_ask=Decimal("50.00"),
                        ),
                    ),
                ),
                evaluated_at=_EVALUATED_AT,
            )

        # Mismatched quote key: a batch whose requested_keys were tampered
        # post-construction so its own content identity no longer replays.
        mismatched_batch = self._happy_quote_batch(preparation)
        object.__setattr__(
            mismatched_batch,
            "requested_keys",
            tuple(reversed(preparation.manifest.listing_keys)),
        )
        with self.assertRaises(Exception):
            evaluate_promoted_operational_quote_gate(
                spec=spec, quote_batch=mismatched_batch, evaluated_at=_EVALUATED_AT
            )

        # Non-target-session decision window.
        wrong_session_not_before = datetime(2026, 7, 18, 9, 15, tzinfo=INDIA_STANDARD_TIME)
        wrong_session_deadline = datetime(2026, 7, 18, 9, 20, tzinfo=INDIA_STANDARD_TIME)
        with self.assertRaises(PromotedOperationalQuoteGateError):
            PromotedOperationalQuoteGateSpec(
                preparation=preparation,
                decision_not_before=wrong_session_not_before,
                decision_deadline=wrong_session_deadline,
                policy=SwingQuoteGatePolicy(),
                paper_only=True,
                notification_eligible=False,
                execution_eligible=False,
            )

        # Observed-after-evaluated.
        with self.assertRaises(Exception):
            evaluate_promoted_operational_quote_gate(
                spec=spec,
                quote_batch=FullQuoteBatch(
                    requested_keys=preparation.manifest.listing_keys,
                    requested_at=_EVALUATED_AT - timedelta(seconds=3),
                    observed_at=_EVALUATED_AT + timedelta(seconds=5),
                    provider_version="kiteconnect/5.2.0",
                    quotes=happy_quotes,
                ),
                evaluated_at=_EVALUATED_AT,
            )

        # Excessive collection duration.
        with self.assertRaises(Exception):
            evaluate_promoted_operational_quote_gate(
                spec=spec,
                quote_batch=FullQuoteBatch(
                    requested_keys=preparation.manifest.listing_keys,
                    requested_at=_EVALUATED_AT - timedelta(seconds=30),
                    observed_at=_EVALUATED_AT - timedelta(seconds=1),
                    provider_version="kiteconnect/5.2.0",
                    quotes=happy_quotes,
                ),
                evaluated_at=_EVALUATED_AT,
            )

    def test_zero_candidate_preparation_requires_no_quote_batch(self) -> None:
        preparation = self._empty_preparation()
        spec = self._spec(preparation)

        gate_batch = evaluate_promoted_operational_quote_gate(
            spec=spec, quote_batch=None, evaluated_at=_EVALUATED_AT
        )
        self.assertEqual(gate_batch.outcomes, ())
        self.assertEqual(gate_batch.pass_count, 0)
        self.assertEqual(gate_batch.veto_count, 0)
        self.assertIsNone(gate_batch.quote_batch)
        gate_batch.verify_content_identity()

        # A quote batch supplied for a zero-candidate preparation fails closed.
        nonempty_preparation = self._preparation()
        stray_quote_batch = self._happy_quote_batch(nonempty_preparation)
        with self.assertRaises(PromotedOperationalQuoteGateError):
            evaluate_promoted_operational_quote_gate(
                spec=spec, quote_batch=stray_quote_batch, evaluated_at=_EVALUATED_AT
            )

    def test_direct_construction_and_self_consistent_mutation_cannot_forge_spec_outcomes_or_batch(
        self,
    ) -> None:
        preparation = self._preparation()
        spec = self._spec(preparation)
        quote_batch = self._happy_quote_batch(preparation)
        gate_batch = evaluate_promoted_operational_quote_gate(
            spec=spec, quote_batch=quote_batch, evaluated_at=_EVALUATED_AT
        )
        outcome_a, outcome_b = gate_batch.outcomes

        # Spec: mutate decision_not_before to fall outside the target
        # session and recompute a self-consistent spec_id -- still fails,
        # since verify_content_identity reconstructs a fresh spec from the
        # (now tampered) retained fields and that reconstruction itself
        # re-runs the window/session invariant.
        tampered_spec = PromotedOperationalQuoteGateSpec(
            preparation=spec.preparation,
            decision_not_before=spec.decision_not_before,
            decision_deadline=spec.decision_deadline,
            policy=spec.policy,
            paper_only=spec.paper_only,
            notification_eligible=spec.notification_eligible,
            execution_eligible=spec.execution_eligible,
        )
        object.__setattr__(
            tampered_spec,
            "decision_not_before",
            datetime(2026, 7, 18, 9, 15, tzinfo=INDIA_STANDARD_TIME),
        )
        object.__setattr__(tampered_spec, "spec_id", tampered_spec._calculated_id())
        with self.assertRaises(PromotedOperationalQuoteGateError):
            tampered_spec.verify_content_identity()

        # Outcome: mutate a real PASS outcome into a self-consistent forged
        # VETO and recompute outcome_id -- still fails, since
        # verify_content_identity replays disposition/reasons from the
        # retained (untampered) candidate/quote/spec rather than trusting
        # the tampered fields.
        real_pass_outcome = outcome_a if outcome_a.passed else outcome_b
        tampered_outcome = PromotedOperationalQuoteOutcome(
            candidate=real_pass_outcome.candidate,
            quote=real_pass_outcome.quote,
            spec=real_pass_outcome.spec,
            evaluated_at=real_pass_outcome.evaluated_at,
            disposition=real_pass_outcome.disposition,
            reason_codes=real_pass_outcome.reason_codes,
            observed_spread_bps=real_pass_outcome.observed_spread_bps,
            reference_entry_price=real_pass_outcome.reference_entry_price,
        )
        object.__setattr__(tampered_outcome, "disposition", SwingQuoteGateDisposition.VETO)
        object.__setattr__(tampered_outcome, "reason_codes", ("FORGED",))
        object.__setattr__(tampered_outcome, "reference_entry_price", None)
        object.__setattr__(tampered_outcome, "outcome_id", tampered_outcome._calculated_id())
        with self.assertRaises(PromotedOperationalQuoteGateError):
            tampered_outcome.verify_content_identity()

        # Batch: direct construction rejects missing/reordered/duplicated
        # outcomes and forged pass/veto counts.
        with self.assertRaises(PromotedOperationalQuoteGateError):
            VerifiedPromotedOperationalQuoteGateBatch(
                spec=spec,
                quote_batch=quote_batch,
                evaluated_at=_EVALUATED_AT,
                outcomes=(outcome_a,),
                pass_count=1,
                veto_count=0,
                paper_only=True,
                notification_eligible=False,
                execution_eligible=False,
            )
        with self.assertRaises(PromotedOperationalQuoteGateError):
            VerifiedPromotedOperationalQuoteGateBatch(
                spec=spec,
                quote_batch=quote_batch,
                evaluated_at=_EVALUATED_AT,
                outcomes=(outcome_b, outcome_a),
                pass_count=gate_batch.pass_count,
                veto_count=gate_batch.veto_count,
                paper_only=True,
                notification_eligible=False,
                execution_eligible=False,
            )
        with self.assertRaises(PromotedOperationalQuoteGateError):
            VerifiedPromotedOperationalQuoteGateBatch(
                spec=spec,
                quote_batch=quote_batch,
                evaluated_at=_EVALUATED_AT,
                outcomes=(outcome_a, outcome_a),
                pass_count=2,
                veto_count=0,
                paper_only=True,
                notification_eligible=False,
                execution_eligible=False,
            )
        with self.assertRaises(PromotedOperationalQuoteGateError):
            VerifiedPromotedOperationalQuoteGateBatch(
                spec=spec,
                quote_batch=quote_batch,
                evaluated_at=_EVALUATED_AT,
                outcomes=gate_batch.outcomes,
                pass_count=gate_batch.pass_count + 1,
                veto_count=gate_batch.veto_count,
                paper_only=True,
                notification_eligible=False,
                execution_eligible=False,
            )


if __name__ == "__main__":
    unittest.main()
