from __future__ import annotations

import decimal
import hashlib
import unittest
from dataclasses import fields, replace
from datetime import timedelta
from decimal import Decimal

from india_swing.market_data.models import FullQuoteBatch
from india_swing.promoted_operational_allocation import (
    PromotedOperationalAllocationEvidence,
    assemble_promoted_operational_allocation_batch,
    build_promoted_operational_allocation_evidence,
)
from india_swing.promoted_operational_decision import (
    PAPER_RESEARCH_WARNING,
    PromotedOperationalDailyDecision,
    PromotedOperationalDecisionAction,
    PromotedOperationalDecisionError,
    PromotedOperationalDecisionPackage,
    PromotedOperationalTradeRecommendation,
    assemble_promoted_operational_decision,
    assemble_promoted_operational_decision_package,
    render_promoted_operational_decision,
)
from india_swing.promoted_operational_quote_gate import (
    evaluate_promoted_operational_quote_gate,
)
from india_swing.risk.swing_portfolio import SwingPortfolioSizingPolicy

from tests import test_promoted_operational_allocation as _allocation_tests
from tests.test_promoted_operational_quote_gate import (
    _EVALUATED_AT as _QUOTE_GATE_EVALUATED_AT,
)


# Reuses the allocation test module's own fast in-memory quote-gate-batch
# fixture via its instance helper method -- the same established pattern
# tests/test_promoted_operational_allocation.py itself uses to reuse
# tests/test_promoted_operational_quote_gate.py's fixtures. Constructing
# this instance runs no test method; it only makes its instance helper
# method callable.
_ALLOCATION_FIXTURE = _allocation_tests.PromotedOperationalAllocationTests(
    methodName="test_happy_path_allocates_first_pass_with_quantity_never_above_research_and_exact_state_chain"
)


def _allocation_batch(*, policy=None, preparation=None):
    preparation, quote_gate_batch = _ALLOCATION_FIXTURE._quote_gate_batch(preparation)
    portfolio = _allocation_tests._portfolio(quote_gate_batch)
    portfolio_context = _allocation_tests._portfolio_context(portfolio)
    allocation_policy = (
        policy if policy is not None else _allocation_tests._allocation_policy()
    )
    return preparation, assemble_promoted_operational_allocation_batch(
        quote_gate_batch=quote_gate_batch,
        portfolio_context=portfolio_context,
        allocation_policy=allocation_policy,
    )


def _all_quote_vetoed_allocation_batch():
    preparation = _allocation_tests._QUOTE_GATE_FIXTURE._preparation()
    spec = _allocation_tests._QUOTE_GATE_FIXTURE._spec(preparation)
    happy_quotes = _allocation_tests._QUOTE_GATE_FIXTURE._happy_quotes(preparation)
    vetoed_quotes = tuple(replace(quote, depth_sell=()) for quote in happy_quotes)
    sorted_keys = tuple(sorted(preparation.manifest.listing_keys))
    quote_batch = FullQuoteBatch(
        requested_keys=sorted_keys,
        requested_at=_QUOTE_GATE_EVALUATED_AT - timedelta(seconds=3),
        observed_at=_QUOTE_GATE_EVALUATED_AT - timedelta(seconds=1),
        provider_version="kiteconnect/5.2.0",
        quotes=tuple(sorted(vetoed_quotes, key=lambda value: value.listing_key)),
    )
    quote_gate_batch = evaluate_promoted_operational_quote_gate(
        spec=spec, quote_batch=quote_batch, evaluated_at=_QUOTE_GATE_EVALUATED_AT
    )
    portfolio = _allocation_tests._portfolio(quote_gate_batch)
    portfolio_context = _allocation_tests._portfolio_context(portfolio)
    return assemble_promoted_operational_allocation_batch(
        quote_gate_batch=quote_gate_batch,
        portfolio_context=portfolio_context,
        allocation_policy=_allocation_tests._allocation_policy(),
    )


class PromotedOperationalDecisionTests(unittest.TestCase):
    def test_one_allocated_outcome_produces_singular_paper_buy_with_exact_trade_values_and_authority_flags(
        self,
    ) -> None:
        _, allocation_batch = _allocation_batch()

        package = assemble_promoted_operational_decision_package(
            allocation_batch=allocation_batch
        )
        decision = package.decision
        recommendation = decision.recommendation

        self.assertEqual(decision.action, PromotedOperationalDecisionAction.PAPER_BUY)
        self.assertIsNotNone(recommendation)
        self.assertEqual(recommendation.listing_key, "NSE:RELIANCE")
        self.assertEqual(recommendation.symbol, "RELIANCE")
        self.assertEqual(recommendation.quantity, 2)
        self.assertEqual(recommendation.outcome.reference_entry_price, Decimal("99.95"))
        self.assertEqual(
            decision.evaluated_at, allocation_batch.quote_gate_batch.evaluated_at
        )
        self.assertEqual(
            decision.target_session,
            allocation_batch.quote_gate_batch.spec.preparation.manifest.target_session,
        )
        self.assertTrue(decision.paper_only)
        self.assertFalse(decision.notification_eligible)
        self.assertFalse(decision.execution_eligible)
        self.assertTrue(package.paper_only)
        self.assertFalse(package.notification_eligible)
        self.assertFalse(package.execution_eligible)
        self.assertTrue(recommendation.research_only)
        self.assertFalse(recommendation.execution_eligible)
        self.assertIn(
            "ALLOCATION:NSE:TCS:MAX_NEW_POSITIONS_PER_RUN_REACHED",
            decision.veto_reason_codes,
        )
        package.verify_content_identity()

    def test_allocation_evidence_replays_every_quantity_ceiling_binding_minimum_and_context_independence(
        self,
    ) -> None:
        _, allocation_batch = _allocation_batch()
        outcome = allocation_batch.allocation_outcomes[0]
        self.assertTrue(outcome.allocated)

        evidence = build_promoted_operational_allocation_evidence(outcome)
        self.assertEqual(evidence.allocation_outcome_id, outcome.allocation_outcome_id)
        self.assertEqual(evidence.operational_quantity, outcome.operational_quantity)
        self.assertEqual(evidence.feasible_quantity, outcome.operational_quantity)
        self.assertEqual(evidence.ask_depth_quantity_ceiling, evidence.feasible_quantity)
        self.assertIn("ASK_DEPTH_PARTICIPATION", evidence.binding_ceiling_codes)
        self.assertGreater(evidence.research_quantity_ceiling, evidence.feasible_quantity)
        self.assertNotIn("RESEARCH_QUANTITY", evidence.binding_ceiling_codes)
        self.assertEqual(
            evidence.binding_ceiling_codes, tuple(sorted(set(evidence.binding_ceiling_codes)))
        )
        self.assertEqual(evidence.operational_net_reward_risk, outcome.operational_net_reward_risk)
        evidence.verify_content_identity()

        original_precision = decimal.getcontext().prec
        decimal.getcontext().prec = 1
        try:
            replayed = build_promoted_operational_allocation_evidence(outcome)
        finally:
            decimal.getcontext().prec = original_precision
        self.assertEqual(decimal.getcontext().prec, original_precision)
        self.assertEqual(replayed.evidence_id, evidence.evidence_id)

    def test_advisory_contains_complete_logic_warning_cancellations_and_lineage_without_confidence_claim(
        self,
    ) -> None:
        _, allocation_batch = _allocation_batch()
        package = assemble_promoted_operational_decision_package(
            allocation_batch=allocation_batch
        )
        message = package.advisory_text

        self.assertTrue(message.startswith(PAPER_RESEARCH_WARNING + "\n"))
        for required in (
            "Action: PAPER_BUY",
            "Quantity ceilings:",
            "binding ceiling(s):",
            "Why this trade:",
            "Cancel / re-evaluate if:",
            "Lineage:",
            "Veto diagnostics:",
            "cannot place an order",
            "does not produce a probability or confidence estimate",
        ):
            self.assertIn(required, message)
        self.assertNotIn("probability of", message.lower())
        self.assertNotIn("confidence level", message.lower())
        for immutable_id in (
            package.decision.allocation_batch.quote_gate_batch.spec.spec_id,
            package.decision.allocation_batch.quote_gate_batch.batch_id,
            package.decision.allocation_batch.allocation_batch_id,
        ):
            self.assertIn(immutable_id, message)
        self.assertEqual(message, render_promoted_operational_decision(package.decision))
        recommendation = package.decision.recommendation
        self.assertEqual(len(recommendation.cancellation_conditions), 6)
        self.assertEqual(len(recommendation.rationale), 6)
        package.verify_content_identity()

    def test_quote_and_allocation_vetoes_are_preserved_canonically_and_zero_allocations_produce_no_trade(
        self,
    ) -> None:
        _, allocation_batch = _allocation_batch()
        package = assemble_promoted_operational_decision_package(
            allocation_batch=allocation_batch
        )
        self.assertIn(
            "ALLOCATION:NSE:TCS:MAX_NEW_POSITIONS_PER_RUN_REACHED",
            package.decision.veto_reason_codes,
        )
        self.assertEqual(
            package.decision.veto_reason_codes,
            tuple(sorted(set(package.decision.veto_reason_codes))),
        )

        zero_batch = _all_quote_vetoed_allocation_batch()
        self.assertEqual(zero_batch.allocation_outcomes, ())
        self.assertEqual(len(zero_batch.upstream_quote_vetoes), 2)

        zero_package = assemble_promoted_operational_decision_package(
            allocation_batch=zero_batch
        )
        self.assertEqual(
            zero_package.decision.action, PromotedOperationalDecisionAction.NO_TRADE
        )
        self.assertIsNone(zero_package.decision.recommendation)
        self.assertTrue(
            any(
                code.startswith("QUOTE:")
                for code in zero_package.decision.veto_reason_codes
            )
        )
        self.assertIn(
            "No allocated outcome survived every quote and allocation gate.",
            zero_package.advisory_text,
        )
        zero_package.verify_content_identity()

    def test_multiple_allocations_are_rejected_at_singular_decision_boundary(self) -> None:
        _, allocation_batch = _allocation_batch(
            policy=_allocation_tests._allocation_policy(
                policy=SwingPortfolioSizingPolicy(maximum_new_positions_per_run=2)
            )
        )

        self.assertEqual(allocation_batch.allocated_count, 2)
        with self.assertRaises(PromotedOperationalDecisionError):
            assemble_promoted_operational_decision(allocation_batch=allocation_batch)

    def test_direct_construction_nested_mutation_reordering_missing_veto_and_self_consistent_text_or_id_forgery_fail_closed(
        self,
    ) -> None:
        _, allocation_batch = _allocation_batch()
        package = assemble_promoted_operational_decision_package(
            allocation_batch=allocation_batch
        )
        decision = package.decision
        recommendation = decision.recommendation
        self.assertIsNotNone(recommendation)

        # Direct construction: action/recommendation shape mismatch.
        with self.assertRaises(PromotedOperationalDecisionError):
            PromotedOperationalDailyDecision(
                allocation_batch=allocation_batch,
                action=PromotedOperationalDecisionAction.NO_TRADE,
                recommendation=recommendation,
                veto_reason_codes=decision.veto_reason_codes,
                evaluated_at=decision.evaluated_at,
                target_session=decision.target_session,
                paper_only=True,
                notification_eligible=False,
                execution_eligible=False,
            )

        # Missing veto: drop one real veto reason code.
        with self.assertRaises(PromotedOperationalDecisionError):
            PromotedOperationalDailyDecision(
                allocation_batch=allocation_batch,
                action=decision.action,
                recommendation=recommendation,
                veto_reason_codes=decision.veto_reason_codes[1:],
                evaluated_at=decision.evaluated_at,
                target_session=decision.target_session,
                paper_only=True,
                notification_eligible=False,
                execution_eligible=False,
            )

        # Reordering: a fresh, self-consistent recommendation whose
        # rationale order is tampered and re-hashed still fails, because
        # verify_content_identity replays the rationale order from the
        # retained outcome/evidence rather than trusting the stored tuple.
        tampered_recommendation = PromotedOperationalTradeRecommendation(
            outcome=recommendation.outcome,
            evidence=recommendation.evidence,
            rationale=recommendation.rationale,
            cancellation_conditions=recommendation.cancellation_conditions,
        )
        object.__setattr__(
            tampered_recommendation, "rationale", tuple(reversed(recommendation.rationale))
        )
        object.__setattr__(
            tampered_recommendation,
            "recommendation_id",
            tampered_recommendation._calculated_id(),
        )
        with self.assertRaises(PromotedOperationalDecisionError):
            tampered_recommendation.verify_content_identity()

        # Self-consistent text/id forgery: a package whose advisory text is
        # tampered and whose hash is recomputed to match still fails,
        # because verify_content_identity replays the expected text from
        # the retained decision rather than trusting the supplied hash.
        forged_text = package.advisory_text + "TAMPERED\n"
        forged_sha256 = hashlib.sha256(forged_text.encode("utf-8")).hexdigest()
        with self.assertRaises(PromotedOperationalDecisionError):
            PromotedOperationalDecisionPackage(
                decision=package.decision,
                advisory_text=forged_text,
                advisory_sha256=forged_sha256,
                paper_only=True,
                notification_eligible=False,
                execution_eligible=False,
            )

        # A forged schema version cannot become valid merely by recomputing
        # the object's own content ID. Schema semantics are replayed too.
        for value, id_name in (
            (replace(recommendation), "recommendation_id"),
            (replace(decision), "decision_id"),
            (replace(package), "package_id"),
        ):
            with self.subTest(contract=type(value).__name__):
                object.__setattr__(value, "schema_version", "forged/v999")
                object.__setattr__(value, id_name, value._calculated_id())
                with self.assertRaises(PromotedOperationalDecisionError):
                    value.verify_content_identity()

        # Nested mutation: mutate deep inside the retained outcome's
        # state_after without touching the outer package's own stored ID --
        # the outer ID stays stale, but verification still fails.
        original_package_id = package.package_id
        object.__setattr__(
            recommendation.outcome.state_after, "open_risk", Decimal("99999")
        )
        self.assertEqual(package.package_id, original_package_id)
        with self.assertRaises(Exception):
            package.verify_content_identity()

    def test_deterministic_ids_and_ambient_decimal_context_independence(self) -> None:
        _, allocation_batch_a = _allocation_batch()
        _, allocation_batch_b = _allocation_batch()

        package_a = assemble_promoted_operational_decision_package(
            allocation_batch=allocation_batch_a
        )
        package_b = assemble_promoted_operational_decision_package(
            allocation_batch=allocation_batch_b
        )
        self.assertEqual(package_a.package_id, package_b.package_id)
        self.assertEqual(package_a.decision.decision_id, package_b.decision.decision_id)
        self.assertEqual(
            package_a.decision.recommendation.recommendation_id,
            package_b.decision.recommendation.recommendation_id,
        )

        original_precision = decimal.getcontext().prec
        decimal.getcontext().prec = 1
        try:
            replayed_evidence = build_promoted_operational_allocation_evidence(
                allocation_batch_a.allocation_outcomes[0]
            )
            package_a.verify_content_identity()
        finally:
            decimal.getcontext().prec = original_precision
        self.assertEqual(decimal.getcontext().prec, original_precision)
        self.assertEqual(
            replayed_evidence.evidence_id,
            package_a.decision.recommendation.evidence.evidence_id,
        )
        package_a.verify_content_identity()

    def test_public_contract_has_no_probability_confidence_notification_or_execution_override_fields(
        self,
    ) -> None:
        all_names = {
            item.name
            for contract in (
                PromotedOperationalAllocationEvidence,
                PromotedOperationalTradeRecommendation,
                PromotedOperationalDailyDecision,
                PromotedOperationalDecisionPackage,
            )
            for item in fields(contract)
        }
        self.assertFalse(any("probability" in name for name in all_names))
        self.assertFalse(any("confidence" in name for name in all_names))

        recommendation_field_names = {
            item.name for item in fields(PromotedOperationalTradeRecommendation)
        }
        self.assertNotIn("execution_eligible", recommendation_field_names)
        self.assertNotIn("notification_eligible", recommendation_field_names)
        self.assertNotIn("paper_only", recommendation_field_names)

        for contract in (PromotedOperationalDailyDecision, PromotedOperationalDecisionPackage):
            field_names = {item.name for item in fields(contract)}
            self.assertIn("paper_only", field_names)
            self.assertIn("notification_eligible", field_names)
            self.assertIn("execution_eligible", field_names)


if __name__ == "__main__":
    unittest.main()
