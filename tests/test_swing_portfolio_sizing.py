from __future__ import annotations

import unittest
from dataclasses import fields, replace
from datetime import timedelta
from decimal import Decimal

from india_swing.market_data.models import FullQuoteBatch
from india_swing.risk.swing_portfolio import (
    SwingCapitalAllocationState,
    SwingPortfolioSizingBatch,
    SwingPortfolioSizingError,
    SwingPortfolioSizingOutcome,
    SwingPortfolioSizingPolicy,
    SwingPortfolioSnapshot,
    SwingSizingDisposition,
    SwingSizingReason,
    assemble_swing_portfolio_sizing_batch,
)
from india_swing.signals.opportunity_ranking import (
    assemble_swing_opportunity_ranking_batch,
)
from india_swing.signals.quote_gate import assemble_swing_quote_gate_batch

from tests import test_swing_opportunity_ranking as ranking_fixtures


D = Decimal


class SwingPortfolioSizingPolicyTests(unittest.TestCase):
    def test_default_policy_is_deterministic_and_maps_to_one_lakh_limits(self) -> None:
        first = SwingPortfolioSizingPolicy()
        second = SwingPortfolioSizingPolicy()

        self.assertEqual(first.policy_id, second.policy_id)
        self.assertEqual(D("100000") * first.per_trade_risk_fraction, D("500.000"))
        self.assertEqual(
            D("100000") * first.maximum_total_open_risk_fraction,
            D("2000.00"),
        )
        self.assertEqual(first.minimum_net_reward_risk, D("2.50"))
        first.verify_content_identity()

    def test_policy_rejects_wrong_nonfinite_out_of_range_or_inconsistent_values(self) -> None:
        cases = (
            dict(per_trade_risk_fraction=1),
            dict(per_trade_risk_fraction=D("NaN")),
            dict(per_trade_risk_fraction=D("0")),
            dict(per_trade_risk_fraction=D("1.01")),
            dict(per_trade_risk_fraction=D("0.03")),
            dict(maximum_position_notional_fraction=D("0.90")),
            dict(maximum_daily_loss_fraction=D("0.03")),
            dict(minimum_net_reward_risk=D("0")),
            dict(maximum_open_positions=True),
            dict(maximum_open_positions=0),
            dict(maximum_new_positions_per_run=0),
            dict(maximum_open_positions=1, maximum_new_positions_per_run=2),
        )
        for overrides in cases:
            with self.subTest(overrides=overrides):
                with self.assertRaises(SwingPortfolioSizingError):
                    SwingPortfolioSizingPolicy(**overrides)

    def test_policy_detects_post_construction_mutation(self) -> None:
        policy = SwingPortfolioSizingPolicy()
        original_id = policy.policy_id
        object.__setattr__(policy, "per_trade_risk_fraction", D("0.02"))

        self.assertEqual(policy.policy_id, original_id)
        with self.assertRaisesRegex(SwingPortfolioSizingError, "content identity"):
            policy.verify_content_identity()


class SwingPortfolioSnapshotTests(unittest.TestCase):
    def _snapshot(self, **overrides) -> SwingPortfolioSnapshot:
        values = dict(
            capital=D("100000"),
            cash_available=D("80000"),
            gross_exposure=D("20000"),
            open_risk=D("500"),
            open_positions=1,
            daily_realized_pnl=D("0"),
            pilot_realized_pnl=D("0"),
            as_of=None,
        )
        fixture = ranking_fixtures.SwingOpportunityRankingBatchTests(
            methodName="test_ranks_every_pass_with_explainable_bounded_components"
        )
        fixture.setUp()
        values["as_of"] = fixture.fixture.evaluated_at - timedelta(seconds=1)
        values.update(overrides)
        return SwingPortfolioSnapshot(**values)

    def test_snapshot_is_deterministic_content_addressed_and_utc_normalized(self) -> None:
        first = self._snapshot()
        second = self._snapshot()

        self.assertEqual(first.portfolio_snapshot_id, second.portfolio_snapshot_id)
        self.assertEqual(first.as_of.utcoffset(), timedelta(0))
        first.verify_content_identity()

    def test_snapshot_rejects_invalid_types_balances_and_currency(self) -> None:
        cases = (
            dict(capital=100000),
            dict(cash_available=D("-1")),
            dict(cash_available=D("90000"), gross_exposure=D("20000")),
            dict(open_risk=D("100001")),
            dict(open_positions=True),
            dict(currency="USD"),
        )
        for overrides in cases:
            with self.subTest(overrides=overrides):
                with self.assertRaises(SwingPortfolioSizingError):
                    self._snapshot(**overrides)


class SwingPortfolioSizingBatchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.ranking_fixture = ranking_fixtures.SwingOpportunityRankingBatchTests(
            methodName="test_ranks_every_pass_with_explainable_bounded_components"
        )
        self.ranking_fixture.setUp()
        self.quote_fixture = self.ranking_fixture.fixture
        self.ranking_batch = self.ranking_fixture._rank()
        self.portfolio = self._portfolio()

    def _portfolio(self, **overrides) -> SwingPortfolioSnapshot:
        values = dict(
            capital=D("100000"),
            cash_available=D("100000"),
            gross_exposure=D("0"),
            open_risk=D("0"),
            open_positions=0,
            daily_realized_pnl=D("0"),
            pilot_realized_pnl=D("0"),
            as_of=self.ranking_batch.quote_gate_batch.evaluated_at
            - timedelta(seconds=1),
        )
        values.update(overrides)
        return SwingPortfolioSnapshot(**values)

    def _size(self, *, ranking_batch=None, portfolio=None, policy=None):
        return assemble_swing_portfolio_sizing_batch(
            ranking_batch=ranking_batch or self.ranking_batch,
            portfolio=portfolio or self.portfolio,
            policy=policy,
        )

    def _ranking_with_depth_quantity(self, quantity: int):
        quotes = tuple(
            replace(
                quote,
                depth_buy=tuple(replace(level, quantity=quantity) for level in quote.depth_buy),
                depth_sell=tuple(
                    replace(level, quantity=quantity) for level in quote.depth_sell
                ),
            )
            for quote in self.ranking_fixture.quote_batch.quotes
        )
        quote_batch = FullQuoteBatch(
            requested_keys=self.ranking_fixture.quote_batch.requested_keys,
            requested_at=self.ranking_fixture.quote_batch.requested_at,
            observed_at=self.ranking_fixture.quote_batch.observed_at,
            provider_version=self.ranking_fixture.quote_batch.provider_version,
            quotes=quotes,
        )
        gate_batch = assemble_swing_quote_gate_batch(
            proposal_batch=self.quote_fixture.proposal_batch,
            quote_batch=quote_batch,
            evaluated_at=self.quote_fixture.evaluated_at,
        )
        return assemble_swing_opportunity_ranking_batch(quote_gate_batch=gate_batch)

    def test_happy_path_allocates_in_rank_order_under_every_portfolio_limit(self) -> None:
        batch = self._size()

        self.assertEqual(len(batch.outcomes), 2)
        self.assertEqual(
            tuple(value.disposition for value in batch.outcomes),
            (SwingSizingDisposition.SIZED, SwingSizingDisposition.VETO),
        )
        self.assertEqual(tuple(value.quantity for value in batch.outcomes), (2, 0))
        self.assertEqual(batch.sized_subject_count, 1)
        self.assertEqual(batch.vetoed_subject_count, 1)
        self.assertEqual(batch.outcomes[0].state_before.cash_available, D("100000"))
        self.assertEqual(
            batch.outcomes[1].state_before.state_id,
            batch.outcomes[0].state_after.state_id,
        )
        self.assertEqual(batch.final_state.state_id, batch.outcomes[-1].state_after.state_id)
        for outcome in tuple(value for value in batch.outcomes if value.sized):
            self.assertLessEqual(outcome.planned_max_loss, D("500"))
            self.assertTrue(outcome.research_only)
            self.assertFalse(outcome.execution_eligible)
        self.assertLessEqual(batch.final_state.open_risk, D("2000"))
        self.assertLessEqual(batch.final_state.gross_exposure, D("80000"))
        self.assertGreaterEqual(batch.final_state.cash_available, D("0"))
        self.assertTrue(batch.research_only)
        self.assertFalse(batch.execution_eligible)
        batch.verify_content_identity()

    def test_deep_quotes_prove_per_trade_and_total_open_risk_caps(self) -> None:
        ranking_batch = self._ranking_with_depth_quantity(10000)
        policy = replace(
            SwingPortfolioSizingPolicy(),
            maximum_position_notional_fraction=D("1"),
            maximum_gross_exposure_fraction=D("1"),
            maximum_daily_turnover_participation=D("1"),
            maximum_top_ask_participation=D("1"),
            maximum_new_positions_per_run=2,
        )
        batch = self._size(ranking_batch=ranking_batch, policy=policy)

        self.assertTrue(all(value.sized for value in batch.outcomes))
        for outcome in batch.outcomes:
            loss_per_share = outcome.planned_max_loss / outcome.quantity
            self.assertLessEqual(outcome.planned_max_loss, D("500"))
            self.assertGreater(
                loss_per_share * (outcome.quantity + 1),
                D("500"),
            )
        self.assertLessEqual(batch.final_state.open_risk, D("2000"))

    def test_default_allows_only_one_new_position_per_run(self) -> None:
        batch = self._size()

        self.assertTrue(batch.outcomes[0].sized)
        self.assertFalse(batch.outcomes[1].sized)
        self.assertIn(
            SwingSizingReason.MAX_NEW_POSITIONS_PER_RUN_REACHED.value,
            batch.outcomes[1].reason_codes,
        )
        self.assertEqual(batch.final_state.open_positions, 1)

    def test_shallow_ask_depth_vetoes_instead_of_rounding_up(self) -> None:
        batch = self._size(
            policy=replace(
                SwingPortfolioSizingPolicy(),
                maximum_top_ask_participation=D("0.05"),
            )
        )

        self.assertTrue(all(not value.sized for value in batch.outcomes))
        self.assertTrue(
            all(
                SwingSizingReason.ASK_DEPTH_CAP_TOO_SMALL.value in value.reason_codes
                for value in batch.outcomes
            )
        )
        self.assertEqual(batch.final_state.state_id, batch.outcomes[0].state_before.state_id)

    def test_exhausted_risk_cash_positions_and_liquidity_each_veto(self) -> None:
        cases = (
            (
                self._portfolio(open_risk=D("2000")),
                SwingPortfolioSizingPolicy(),
                SwingSizingReason.TOTAL_OPEN_RISK_EXHAUSTED.value,
            ),
            (
                self._portfolio(cash_available=D("0")),
                SwingPortfolioSizingPolicy(),
                SwingSizingReason.CASH_EXHAUSTED.value,
            ),
            (
                self._portfolio(open_positions=4),
                SwingPortfolioSizingPolicy(),
                SwingSizingReason.MAX_OPEN_POSITIONS_REACHED.value,
            ),
            (
                self.portfolio,
                replace(
                    SwingPortfolioSizingPolicy(),
                    maximum_daily_turnover_participation=D("0.000000000001"),
                ),
                SwingSizingReason.LIQUIDITY_CAP_TOO_SMALL.value,
            ),
        )
        for portfolio, policy, reason in cases:
            with self.subTest(reason=reason):
                batch = self._size(portfolio=portfolio, policy=policy)
                self.assertTrue(all(value.disposition is SwingSizingDisposition.VETO for value in batch.outcomes))
                self.assertTrue(all(reason in value.reason_codes for value in batch.outcomes))
                self.assertEqual(batch.final_state.state_id, batch.outcomes[0].state_before.state_id)

    def test_preexisting_limit_breach_is_preserved_and_vetoed_not_hidden(self) -> None:
        portfolio = self._portfolio(
            cash_available=D("10000"),
            gross_exposure=D("90000"),
            open_risk=D("3000"),
        )
        batch = self._size(portfolio=portfolio)

        for outcome in batch.outcomes:
            self.assertEqual(outcome.disposition, SwingSizingDisposition.VETO)
            self.assertIn(
                SwingSizingReason.TOTAL_OPEN_RISK_EXHAUSTED.value,
                outcome.reason_codes,
            )
            self.assertIn(
                SwingSizingReason.GROSS_EXPOSURE_EXHAUSTED.value,
                outcome.reason_codes,
            )
        self.assertEqual(batch.final_state.open_risk, D("3000"))
        self.assertEqual(batch.final_state.gross_exposure, D("90000"))

    def test_loss_halts_fire_at_exact_policy_boundaries(self) -> None:
        portfolio = self._portfolio(
            daily_realized_pnl=D("-1000"),
            pilot_realized_pnl=D("-2000"),
        )
        batch = self._size(portfolio=portfolio)

        for outcome in batch.outcomes:
            self.assertEqual(outcome.disposition, SwingSizingDisposition.VETO)
            self.assertIn(SwingSizingReason.DAILY_LOSS_HALT.value, outcome.reason_codes)
            self.assertIn(SwingSizingReason.PILOT_DRAWDOWN_HALT.value, outcome.reason_codes)

    def test_preserves_upstream_quote_vetoes_exactly(self) -> None:
        gate_batch = self.ranking_fixture._gate_with_first_veto()
        ranking_batch = self.ranking_fixture._rank(gate_batch)
        batch = self._size(ranking_batch=ranking_batch)

        self.assertEqual(batch.upstream_vetoes, ranking_batch.vetoed_outcomes)
        self.assertEqual(len(batch.outcomes), 1)

    def test_direct_construction_rejects_coverage_counts_amounts_and_state_forgery(self) -> None:
        batch = self._size()
        first, second = batch.outcomes

        with self.assertRaises(SwingPortfolioSizingError):
            replace(batch, outcomes=(first,), sized_subject_count=1)
        with self.assertRaises(SwingPortfolioSizingError):
            replace(batch, outcomes=(first, first))
        with self.assertRaises(SwingPortfolioSizingError):
            replace(batch, outcomes=(second, first))
        with self.assertRaises(SwingPortfolioSizingError):
            replace(batch, sized_subject_count=99)
        with self.assertRaises(SwingPortfolioSizingError):
            replace(first, quantity=first.quantity + 1)
        with self.assertRaises(SwingPortfolioSizingError):
            replace(first, planned_max_loss=D("-1"))
        with self.assertRaises(SwingPortfolioSizingError):
            replace(batch, final_state=first.state_before)

    def test_nested_mutation_is_detected_without_changing_outer_batch_id(self) -> None:
        batch = self._size()
        original_id = batch.sizing_batch_id
        object.__setattr__(batch.outcomes[0].state_after, "open_risk", D("99999"))

        self.assertEqual(batch.sizing_batch_id, original_id)
        with self.assertRaises(Exception):
            batch.verify_content_identity()

    def test_rejects_future_known_portfolio_and_wrong_exact_input_types(self) -> None:
        future_portfolio = self._portfolio(
            as_of=self.ranking_batch.quote_gate_batch.evaluated_at
            + timedelta(microseconds=1)
        )
        with self.assertRaisesRegex(SwingPortfolioSizingError, "future-known"):
            self._size(portfolio=future_portfolio)
        with self.assertRaises(SwingPortfolioSizingError):
            assemble_swing_portfolio_sizing_batch(
                ranking_batch=object(),
                portfolio=self.portfolio,
            )
        with self.assertRaises(SwingPortfolioSizingError):
            assemble_swing_portfolio_sizing_batch(
                ranking_batch=self.ranking_batch,
                portfolio=object(),
            )
        with self.assertRaises(SwingPortfolioSizingError):
            self._size(policy=object())

    def test_public_contract_has_no_confidence_probability_or_execution_override(self) -> None:
        names = {
            item.name
            for contract in (
                SwingPortfolioSnapshot,
                SwingPortfolioSizingPolicy,
                SwingCapitalAllocationState,
                SwingPortfolioSizingOutcome,
                SwingPortfolioSizingBatch,
            )
            for item in fields(contract)
        }

        self.assertFalse(any("confidence" in name for name in names))
        self.assertFalse(any("probability" in name for name in names))
        self.assertNotIn("execution_eligible", names)


if __name__ == "__main__":
    unittest.main()
