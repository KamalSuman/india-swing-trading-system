from __future__ import annotations

import unittest
from dataclasses import fields, replace
from datetime import timedelta
from decimal import Decimal

from india_swing.market_data.models import FullQuoteBatch
from india_swing.signals.opportunity_ranking import (
    SwingOpportunityRankingBatch,
    SwingOpportunityRankingError,
    SwingOpportunityRankingPolicy,
    SwingRankedOpportunity,
    SwingRankingComponent,
    SwingRankingFactor,
    assemble_swing_opportunity_ranking_batch,
)
from india_swing.signals.quote_gate import (
    SwingQuoteGateDisposition,
    assemble_swing_quote_gate_batch,
)

from tests import test_swing_quote_gate as quote_gate_fixtures


class SwingOpportunityRankingPolicyTests(unittest.TestCase):
    def test_default_policy_is_exact_deterministic_and_sums_to_one(self) -> None:
        first = SwingOpportunityRankingPolicy()
        second = SwingOpportunityRankingPolicy()

        self.assertEqual(first.policy_id, second.policy_id)
        self.assertEqual(
            sum((value for _, value in first.weights), Decimal("0")),
            Decimal("1"),
        )
        self.assertEqual(
            tuple(factor for factor, _ in first.weights),
            tuple(SwingRankingFactor),
        )
        first.verify_content_identity()

    def test_policy_rejects_wrong_nonfinite_negative_or_unbalanced_weights(self) -> None:
        cases = (
            dict(relative_strength_weight=1),
            dict(relative_strength_weight=Decimal("NaN")),
            dict(relative_strength_weight=Decimal("-0.01")),
            dict(relative_strength_weight=Decimal("0.30")),
        )
        for overrides in cases:
            with self.subTest(overrides=overrides):
                with self.assertRaises(SwingOpportunityRankingError):
                    SwingOpportunityRankingPolicy(**overrides)

    def test_policy_detects_post_construction_mutation(self) -> None:
        policy = SwingOpportunityRankingPolicy()
        original_id = policy.policy_id
        object.__setattr__(policy, "trend_quality_weight", Decimal("0.50"))

        self.assertEqual(policy.policy_id, original_id)
        with self.assertRaisesRegex(SwingOpportunityRankingError, "content identity"):
            policy.verify_content_identity()


class SwingOpportunityRankingBatchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = quote_gate_fixtures.SwingQuoteGateBatchTests(
            methodName="test_happy_path_produces_two_pass_outcomes"
        )
        self.fixture.setUp()
        self.quote_batch = self.fixture._happy_batch()
        self.gate_batch = assemble_swing_quote_gate_batch(
            proposal_batch=self.fixture.proposal_batch,
            quote_batch=self.quote_batch,
            evaluated_at=self.fixture.evaluated_at,
        )

    def _rank(self, gate_batch=None, policy=None) -> SwingOpportunityRankingBatch:
        return assemble_swing_opportunity_ranking_batch(
            quote_gate_batch=gate_batch or self.gate_batch,
            policy=policy,
        )

    def _gate_with_first_veto(self):
        quote_a = replace(
            self.fixture._happy_quote(
                self.fixture.proposal_a,
                instrument_token=quote_gate_fixtures.TOKEN_A,
            ),
            depth_sell=(),
        )
        quote_batch = self.fixture._happy_batch(quote_a=quote_a)
        return assemble_swing_quote_gate_batch(
            proposal_batch=self.fixture.proposal_batch,
            quote_batch=quote_batch,
            evaluated_at=self.fixture.evaluated_at,
        )

    def _wide_gate(self, spread_bps: str):
        proposal_batch = self.fixture._wide_proposal_batch()
        proposal_a, proposal_b = proposal_batch.proposals
        quote_a = self.fixture._happy_quote(
            proposal_a,
            instrument_token=quote_gate_fixtures.TOKEN_A,
            spread_bps=spread_bps,
        )
        quote_b = self.fixture._happy_quote(
            proposal_b,
            instrument_token=quote_gate_fixtures.TOKEN_B,
        )
        quote_batch = FullQuoteBatch(
            requested_keys=(f"NSE:{proposal_a.symbol}", f"NSE:{proposal_b.symbol}"),
            requested_at=self.fixture.evaluated_at - timedelta(seconds=3),
            observed_at=self.fixture.evaluated_at - timedelta(seconds=1),
            provider_version="kiteconnect/5.2.0",
            quotes=(quote_a, quote_b),
        )
        return assemble_swing_quote_gate_batch(
            proposal_batch=proposal_batch,
            quote_batch=quote_batch,
            evaluated_at=self.fixture.evaluated_at,
        )

    def test_ranks_every_pass_with_explainable_bounded_components(self) -> None:
        batch = self._rank()

        self.assertEqual(batch.ranked_subject_count, 2)
        self.assertEqual(batch.vetoed_subject_count, 0)
        self.assertEqual(tuple(value.rank for value in batch.ranked_opportunities), (1, 2))
        self.assertGreaterEqual(
            batch.ranked_opportunities[0].ranking_score,
            batch.ranked_opportunities[1].ranking_score,
        )
        for opportunity in batch.ranked_opportunities:
            self.assertEqual(
                tuple(value.factor for value in opportunity.components),
                tuple(SwingRankingFactor),
            )
            self.assertEqual(
                opportunity.ranking_score,
                sum(
                    (value.contribution for value in opportunity.components),
                    Decimal("0"),
                ),
            )
            self.assertTrue(
                all(
                    Decimal("0") <= value.raw_value <= Decimal("1")
                    for value in opportunity.components
                )
            )
            self.assertTrue(opportunity.research_only)
            self.assertFalse(opportunity.execution_eligible)
        self.assertTrue(batch.research_only)
        self.assertFalse(batch.execution_eligible)
        batch.verify_content_identity()

    def test_ranking_is_deterministic_and_ties_use_stable_subject_lineage(self) -> None:
        first = self._rank()
        second = self._rank()

        self.assertEqual(first.ranking_batch_id, second.ranking_batch_id)
        self.assertEqual(
            tuple(value.opportunity_id for value in first.ranked_opportunities),
            tuple(value.opportunity_id for value in second.ranked_opportunities),
        )
        for earlier, later in zip(
            first.ranked_opportunities,
            first.ranked_opportunities[1:],
        ):
            if earlier.ranking_score == later.ranking_score:
                earlier_key = (
                    earlier.quote_gate_outcome.observed_spread_bps,
                    earlier.quote_gate_outcome.proposal.assembly.stable_instrument_id,
                    earlier.quote_gate_outcome.proposal.assembly.stable_listing_id,
                )
                later_key = (
                    later.quote_gate_outcome.observed_spread_bps,
                    later.quote_gate_outcome.proposal.assembly.stable_instrument_id,
                    later.quote_gate_outcome.proposal.assembly.stable_listing_id,
                )
                self.assertLessEqual(earlier_key, later_key)

    def test_wider_passing_ask_changes_only_execution_quality_factors(self) -> None:
        zero_batch = self._rank(self._wide_gate("0"))
        wide_gate = self._wide_gate("40")
        wide_batch = self._rank(wide_gate)

        zero_by_symbol = {value.symbol: value for value in zero_batch.ranked_opportunities}
        wide_by_symbol = {value.symbol: value for value in wide_batch.ranked_opportunities}
        zero_a = zero_by_symbol[self.fixture.proposal_a.symbol]
        wide_a = wide_by_symbol[self.fixture.proposal_a.symbol]

        self.assertLess(wide_a.ranking_score, zero_a.ranking_score)
        self.assertEqual(
            wide_a.quote_gate_outcome.proposal.metrics.metrics_id,
            zero_a.quote_gate_outcome.proposal.metrics.metrics_id,
        )
        changed = tuple(
            component.factor
            for component, original in zip(wide_a.components, zero_a.components)
            if component.raw_value != original.raw_value
        )
        self.assertEqual(
            changed,
            (
                SwingRankingFactor.SPREAD_QUALITY,
                SwingRankingFactor.ENTRY_QUALITY,
            ),
        )

    def test_preserves_every_veto_and_ranks_only_passes(self) -> None:
        gate_batch = self._gate_with_first_veto()
        batch = self._rank(gate_batch)

        expected_vetoes = tuple(
            value
            for value in gate_batch.outcomes
            if value.disposition is SwingQuoteGateDisposition.VETO
        )
        self.assertEqual(batch.ranked_subject_count, 1)
        self.assertEqual(batch.vetoed_subject_count, 1)
        self.assertEqual(batch.vetoed_outcomes, expected_vetoes)
        self.assertEqual(
            batch.ranked_opportunities[0].quote_gate_outcome.disposition,
            SwingQuoteGateDisposition.PASS,
        )

    def test_all_veto_batch_is_valid_and_returns_no_ranked_opportunity(self) -> None:
        quote_a = replace(
            self.quote_batch.quotes[0],
            depth_sell=(),
        )
        quote_b = replace(
            self.quote_batch.quotes[1],
            depth_sell=(),
        )
        quote_batch = replace(self.quote_batch, quotes=(quote_a, quote_b))
        gate_batch = assemble_swing_quote_gate_batch(
            proposal_batch=self.fixture.proposal_batch,
            quote_batch=quote_batch,
            evaluated_at=self.fixture.evaluated_at,
        )

        batch = self._rank(gate_batch)

        self.assertEqual(batch.ranked_opportunities, ())
        self.assertEqual(batch.ranked_subject_count, 0)
        self.assertEqual(batch.vetoed_subject_count, 2)
        self.assertEqual(batch.vetoed_outcomes, gate_batch.outcomes)

    def test_direct_construction_rejects_missing_duplicate_reordered_or_veto_loss(self) -> None:
        batch = self._rank()
        first, second = batch.ranked_opportunities

        with self.assertRaises(SwingOpportunityRankingError):
            replace(batch, ranked_opportunities=(first,), ranked_subject_count=1)
        with self.assertRaises(SwingOpportunityRankingError):
            replace(batch, ranked_opportunities=(first, first))
        with self.assertRaises(SwingOpportunityRankingError):
            replace(batch, ranked_opportunities=(second, first))

        veto_batch = self._rank(self._gate_with_first_veto())
        with self.assertRaises(SwingOpportunityRankingError):
            replace(veto_batch, vetoed_outcomes=(), vetoed_subject_count=0)

    def test_direct_construction_rejects_forged_rank_component_or_score(self) -> None:
        batch = self._rank()
        first = batch.ranked_opportunities[0]
        component = first.components[0]

        with self.assertRaises(SwingOpportunityRankingError):
            replace(first, rank=0)
        with self.assertRaises(SwingOpportunityRankingError):
            replace(first, ranking_score=Decimal("0"))
        with self.assertRaises(SwingOpportunityRankingError):
            replace(component, contribution=component.contribution + Decimal("0.01"))

    def test_nested_mutation_is_detected_without_changing_outer_batch_id(self) -> None:
        batch = self._rank()
        original_id = batch.ranking_batch_id
        object.__setattr__(
            batch.ranked_opportunities[0].components[0],
            "raw_value",
            Decimal("0"),
        )

        self.assertEqual(batch.ranking_batch_id, original_id)
        with self.assertRaises(Exception):
            batch.verify_content_identity()

    def test_public_contract_has_no_confidence_probability_or_execution_override(self) -> None:
        names = {
            item.name
            for contract in (
                SwingRankingComponent,
                SwingRankedOpportunity,
                SwingOpportunityRankingBatch,
            )
            for item in fields(contract)
        }

        self.assertFalse(any("confidence" in name for name in names))
        self.assertFalse(any("probability" in name for name in names))
        self.assertNotIn("execution_eligible", names)

    def test_rejects_wrong_exact_input_types(self) -> None:
        with self.assertRaises(SwingOpportunityRankingError):
            assemble_swing_opportunity_ranking_batch(quote_gate_batch=object())
        with self.assertRaises(SwingOpportunityRankingError):
            assemble_swing_opportunity_ranking_batch(
                quote_gate_batch=self.gate_batch,
                policy=object(),
            )


if __name__ == "__main__":
    unittest.main()
