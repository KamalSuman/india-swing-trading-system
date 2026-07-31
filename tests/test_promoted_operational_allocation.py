from __future__ import annotations

import decimal
import unittest
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

from india_swing.market_data.models import FullQuoteBatch
from india_swing.promoted_operational_allocation import (
    PromotedOperationalAllocationDisposition,
    PromotedOperationalAllocationError,
    PromotedOperationalAllocationOutcome,
    PromotedOperationalAllocationPolicy,
    PromotedOperationalAllocationState,
    PromotedOperationalPortfolioContext,
    VerifiedPromotedOperationalAllocationBatch,
    _evaluate_allocation,
    assemble_promoted_operational_allocation_batch,
)
from india_swing.promoted_operational_quote_gate import (
    evaluate_promoted_operational_quote_gate,
)
from india_swing.risk.swing_portfolio import SwingPortfolioSizingPolicy, SwingPortfolioSnapshot

from tests.test_promoted_operational_quote_gate import (
    PromotedOperationalQuoteGateTests,
    _EVALUATED_AT as _QUOTE_GATE_EVALUATED_AT,
)


_SOURCE_PORTFOLIO_ARTIFACT_ID = "7" * 64

# Reuses the quote-gate test module's own fast in-memory preparation/quote
# fixtures via its instance helper methods -- the same established pattern
# tests/test_swing_portfolio_sizing.py itself uses to reuse another test
# module's fixture-building test case. Constructing this instance runs no
# test method; it only makes its instance helper methods callable.
_QUOTE_GATE_FIXTURE = PromotedOperationalQuoteGateTests(
    methodName="test_fresh_in_window_limit_compatible_quotes_pass_with_exact_reference_ask"
)


def _portfolio(quote_gate_batch, **overrides) -> SwingPortfolioSnapshot:
    values = dict(
        capital=Decimal("100000"),
        cash_available=Decimal("100000"),
        gross_exposure=Decimal("0"),
        open_risk=Decimal("0"),
        open_positions=0,
        daily_realized_pnl=Decimal("0"),
        pilot_realized_pnl=Decimal("0"),
        as_of=quote_gate_batch.evaluated_at - timedelta(seconds=1),
    )
    values.update(overrides)
    return SwingPortfolioSnapshot(**values)


def _portfolio_context(
    portfolio: SwingPortfolioSnapshot, open_listing_keys=()
) -> PromotedOperationalPortfolioContext:
    return PromotedOperationalPortfolioContext(
        portfolio=portfolio,
        source_portfolio_artifact_id=_SOURCE_PORTFOLIO_ARTIFACT_ID,
        open_listing_keys=tuple(open_listing_keys),
    )


def _allocation_policy(**overrides) -> PromotedOperationalAllocationPolicy:
    values = dict(
        policy=SwingPortfolioSizingPolicy(),
        maximum_portfolio_age_seconds=300,
        paper_only=True,
        notification_eligible=False,
        execution_eligible=False,
    )
    values.update(overrides)
    return PromotedOperationalAllocationPolicy(**values)


class PromotedOperationalAllocationTests(unittest.TestCase):
    def _quote_gate_batch(self, preparation=None):
        preparation = preparation or _QUOTE_GATE_FIXTURE._preparation()
        spec = _QUOTE_GATE_FIXTURE._spec(preparation)
        quote_batch = _QUOTE_GATE_FIXTURE._happy_quote_batch(preparation)
        return preparation, evaluate_promoted_operational_quote_gate(
            spec=spec, quote_batch=quote_batch, evaluated_at=_QUOTE_GATE_EVALUATED_AT
        )

    def test_happy_path_allocates_first_pass_with_quantity_never_above_research_and_exact_state_chain(
        self,
    ) -> None:
        preparation, quote_gate_batch = self._quote_gate_batch()
        portfolio = _portfolio(quote_gate_batch)
        portfolio_context = _portfolio_context(portfolio)
        allocation_policy = _allocation_policy()

        allocation_batch = assemble_promoted_operational_allocation_batch(
            quote_gate_batch=quote_gate_batch,
            portfolio_context=portfolio_context,
            allocation_policy=allocation_policy,
        )

        first_outcome = allocation_batch.allocation_outcomes[0]
        self.assertTrue(first_outcome.allocated)
        self.assertEqual(first_outcome.reason_codes, ())
        research_quantity = (
            first_outcome.quote_outcome.candidate.research_intent.evaluation_intent
            .entry_order.quantity
        )
        # Bound by the top-ask depth cap (10 * 0.20 == 2), strictly below
        # the retained research quantity of 10 -- proving quantity is
        # never invented above research and can genuinely be tighter.
        self.assertLess(first_outcome.operational_quantity, research_quantity)
        self.assertEqual(first_outcome.operational_quantity, 2)
        self.assertLessEqual(first_outcome.operational_quantity, research_quantity)
        self.assertEqual(first_outcome.reference_entry_price, Decimal("99.95"))
        self.assertEqual(first_outcome.entry_notional, Decimal("99.95") * 2)
        self.assertEqual(first_outcome.estimated_round_trip_cost, Decimal("0.5") * 2)
        self.assertEqual(first_outcome.planned_max_loss, Decimal("9.95") * 2)
        self.assertEqual(
            first_outcome.state_before.state_id, allocation_batch.initial_state.state_id
        )
        self.assertEqual(first_outcome.state_after.open_listing_keys, ("NSE:RELIANCE",))
        self.assertEqual(
            first_outcome.state_after.cash_available,
            Decimal("100000")
            - first_outcome.entry_notional
            - first_outcome.estimated_round_trip_cost,
        )
        self.assertEqual(first_outcome.state_after.gross_exposure, first_outcome.entry_notional)
        self.assertEqual(first_outcome.state_after.open_risk, first_outcome.planned_max_loss)
        self.assertFalse(first_outcome.execution_eligible)
        self.assertTrue(allocation_batch.paper_only)
        self.assertFalse(allocation_batch.notification_eligible)
        self.assertFalse(allocation_batch.execution_eligible)
        allocation_batch.verify_content_identity()

    def test_candidate_order_is_preserved_and_maximum_new_positions_vetoes_later_passes_without_reranking(
        self,
    ) -> None:
        preparation, quote_gate_batch = self._quote_gate_batch()
        portfolio = _portfolio(quote_gate_batch)
        portfolio_context = _portfolio_context(portfolio)
        # Default policy.maximum_new_positions_per_run == 1.
        allocation_policy = _allocation_policy()

        allocation_batch = assemble_promoted_operational_allocation_batch(
            quote_gate_batch=quote_gate_batch,
            portfolio_context=portfolio_context,
            allocation_policy=allocation_policy,
        )

        self.assertEqual(len(allocation_batch.allocation_outcomes), 2)
        self.assertEqual(
            tuple(
                value.quote_outcome.candidate.listing_key
                for value in allocation_batch.allocation_outcomes
            ),
            ("NSE:RELIANCE", "NSE:TCS"),
        )
        first, second = allocation_batch.allocation_outcomes
        self.assertTrue(first.allocated)
        self.assertFalse(second.allocated)
        self.assertIn("MAX_NEW_POSITIONS_PER_RUN_REACHED", second.reason_codes)
        self.assertEqual(second.operational_quantity, 0)
        self.assertEqual(second.entry_notional, Decimal("0"))
        self.assertEqual(second.state_after.state_id, second.state_before.state_id)
        self.assertEqual(allocation_batch.allocated_count, 1)
        self.assertEqual(allocation_batch.veto_count, 1)
        allocation_batch.verify_content_identity()

    def test_risk_and_wealth_guards_accumulate_canonical_reasons(self) -> None:
        preparation, quote_gate_batch = self._quote_gate_batch()
        reliance_listing_key = preparation.candidates[0].listing_key

        cases = {
            "DUPLICATE_OPEN_LISTING": dict(
                portfolio_overrides=dict(open_positions=1),
                open_listing_keys=(reliance_listing_key,),
            ),
            "DAILY_LOSS_HALT": dict(
                portfolio_overrides=dict(daily_realized_pnl=Decimal("-1000")),
            ),
            "PILOT_DRAWDOWN_HALT": dict(
                portfolio_overrides=dict(pilot_realized_pnl=Decimal("-2000")),
            ),
            "MAX_OPEN_POSITIONS_REACHED": dict(
                portfolio_overrides=dict(open_positions=4),
                open_listing_keys=("NSE:AAA1", "NSE:AAA2", "NSE:AAA3", "NSE:AAA4"),
            ),
            "RESEARCH_LIQUIDITY_POLICY_TOO_WIDE": dict(
                policy_overrides=dict(
                    maximum_daily_turnover_participation=Decimal("0.0001")
                ),
            ),
            "NET_REWARD_RISK_BELOW_MINIMUM": dict(
                policy_overrides=dict(minimum_net_reward_risk=Decimal("10")),
            ),
            "PER_TRADE_RISK_TOO_SMALL": dict(
                policy_overrides=dict(per_trade_risk_fraction=Decimal("0.00001")),
            ),
            "TOTAL_OPEN_RISK_EXHAUSTED": dict(
                portfolio_overrides=dict(open_risk=Decimal("2000")),
            ),
            "POSITION_NOTIONAL_CAP_TOO_SMALL": dict(
                policy_overrides=dict(
                    maximum_position_notional_fraction=Decimal("0.00001")
                ),
            ),
            "GROSS_EXPOSURE_EXHAUSTED": dict(
                portfolio_overrides=dict(
                    gross_exposure=Decimal("80000"), cash_available=Decimal("20000")
                ),
            ),
            "CASH_EXHAUSTED": dict(
                portfolio_overrides=dict(cash_available=Decimal("10")),
            ),
            "ASK_DEPTH_CAP_TOO_SMALL": dict(
                policy_overrides=dict(maximum_top_ask_participation=Decimal("0.001")),
            ),
        }

        for reason, overrides in cases.items():
            with self.subTest(reason=reason):
                portfolio = _portfolio(
                    quote_gate_batch, **overrides.get("portfolio_overrides", {})
                )
                portfolio_context = _portfolio_context(
                    portfolio, overrides.get("open_listing_keys", ())
                )
                allocation_policy = _allocation_policy(
                    policy=SwingPortfolioSizingPolicy(
                        **overrides.get("policy_overrides", {})
                    )
                )
                allocation_batch = assemble_promoted_operational_allocation_batch(
                    quote_gate_batch=quote_gate_batch,
                    portfolio_context=portfolio_context,
                    allocation_policy=allocation_policy,
                )
                outcome = allocation_batch.allocation_outcomes[0]
                self.assertIn(reason, outcome.reason_codes)

        # Genuine accumulation: two independent triggers fire together.
        combined_portfolio = _portfolio(
            quote_gate_batch,
            daily_realized_pnl=Decimal("-1000"),
            pilot_realized_pnl=Decimal("-2000"),
        )
        combined_batch = assemble_promoted_operational_allocation_batch(
            quote_gate_batch=quote_gate_batch,
            portfolio_context=_portfolio_context(combined_portfolio),
            allocation_policy=_allocation_policy(),
        )
        combined_outcome = combined_batch.allocation_outcomes[0]
        self.assertIn("DAILY_LOSS_HALT", combined_outcome.reason_codes)
        self.assertIn("PILOT_DRAWDOWN_HALT", combined_outcome.reason_codes)
        self.assertEqual(
            combined_outcome.reason_codes,
            tuple(sorted(set(combined_outcome.reason_codes))),
        )

    def test_allocation_policy_safe_defaults_are_exact(self) -> None:
        policy = PromotedOperationalAllocationPolicy(policy=SwingPortfolioSizingPolicy())

        self.assertEqual(policy.maximum_portfolio_age_seconds, 300)
        self.assertTrue(policy.paper_only)
        self.assertFalse(policy.notification_eligible)
        self.assertFalse(policy.execution_eligible)
        policy.verify_content_identity()

    def test_nested_snapshot_and_policy_self_consistent_tampering_fails_semantic_replay(
        self,
    ) -> None:
        preparation, quote_gate_batch = self._quote_gate_batch()

        # A self-consistently rehashed invalid SwingPortfolioSnapshot
        # (cash_available + gross_exposure > capital) nested inside a
        # self-consistently rehashed context still fails, because the
        # promoted wrapper independently reconstructs the exact parent
        # snapshot type rather than trusting its hash-only
        # verify_content_identity.
        portfolio = _portfolio(quote_gate_batch)
        context = _portfolio_context(portfolio)
        object.__setattr__(portfolio, "cash_available", portfolio.capital)
        object.__setattr__(portfolio, "gross_exposure", portfolio.capital)
        object.__setattr__(portfolio, "portfolio_snapshot_id", portfolio._calculated_id())
        object.__setattr__(context, "context_id", context._calculated_id())

        with self.assertRaises(PromotedOperationalAllocationError):
            context.verify_content_identity()
        with self.assertRaises(PromotedOperationalAllocationError):
            PromotedOperationalPortfolioContext(
                portfolio=portfolio,
                source_portfolio_artifact_id=context.source_portfolio_artifact_id,
                open_listing_keys=context.open_listing_keys,
            )

        # A self-consistently rehashed invalid/widened
        # SwingPortfolioSizingPolicy (per_trade_risk_fraction=2) nested
        # inside a self-consistently rehashed allocation policy still
        # fails for the same reason.
        sizing_policy = SwingPortfolioSizingPolicy()
        allocation_policy = _allocation_policy(policy=sizing_policy)
        object.__setattr__(sizing_policy, "per_trade_risk_fraction", Decimal("2"))
        object.__setattr__(sizing_policy, "policy_id", sizing_policy._calculated_id())
        object.__setattr__(
            allocation_policy,
            "allocation_policy_id",
            allocation_policy._calculated_id(),
        )

        with self.assertRaises(PromotedOperationalAllocationError):
            allocation_policy.verify_content_identity()
        with self.assertRaises(PromotedOperationalAllocationError):
            PromotedOperationalAllocationPolicy(
                policy=sizing_policy,
                maximum_portfolio_age_seconds=allocation_policy.maximum_portfolio_age_seconds,
                paper_only=allocation_policy.paper_only,
                notification_eligible=allocation_policy.notification_eligible,
                execution_eligible=allocation_policy.execution_eligible,
            )

    def test_future_or_stale_portfolio_and_context_shape_fail_closed(self) -> None:
        preparation, quote_gate_batch = self._quote_gate_batch()
        allocation_policy = _allocation_policy()

        future_portfolio = _portfolio(
            quote_gate_batch, as_of=quote_gate_batch.evaluated_at + timedelta(seconds=5)
        )
        with self.assertRaises(PromotedOperationalAllocationError):
            assemble_promoted_operational_allocation_batch(
                quote_gate_batch=quote_gate_batch,
                portfolio_context=_portfolio_context(future_portfolio),
                allocation_policy=allocation_policy,
            )

        stale_portfolio = _portfolio(
            quote_gate_batch,
            as_of=quote_gate_batch.evaluated_at - timedelta(seconds=301),
        )
        with self.assertRaises(PromotedOperationalAllocationError):
            assemble_promoted_operational_allocation_batch(
                quote_gate_batch=quote_gate_batch,
                portfolio_context=_portfolio_context(stale_portfolio),
                allocation_policy=allocation_policy,
            )

        valid_portfolio = _portfolio(quote_gate_batch)

        with self.assertRaises(PromotedOperationalAllocationError):
            PromotedOperationalPortfolioContext(
                portfolio=valid_portfolio,
                source_portfolio_artifact_id="not-hex",
                open_listing_keys=(),
            )

        with self.assertRaises(PromotedOperationalAllocationError):
            PromotedOperationalPortfolioContext(
                portfolio=_portfolio(quote_gate_batch, open_positions=2),
                source_portfolio_artifact_id=_SOURCE_PORTFOLIO_ARTIFACT_ID,
                open_listing_keys=("NSE:TCS", "NSE:RELIANCE"),
            )

        with self.assertRaises(PromotedOperationalAllocationError):
            PromotedOperationalPortfolioContext(
                portfolio=_portfolio(quote_gate_batch, open_positions=2),
                source_portfolio_artifact_id=_SOURCE_PORTFOLIO_ARTIFACT_ID,
                open_listing_keys=("NSE:RELIANCE", "NSE:RELIANCE"),
            )

        with self.assertRaises(PromotedOperationalAllocationError):
            PromotedOperationalPortfolioContext(
                portfolio=_portfolio(quote_gate_batch, open_positions=1),
                source_portfolio_artifact_id=_SOURCE_PORTFOLIO_ARTIFACT_ID,
                open_listing_keys=("nse:reliance",),
            )

        with self.assertRaises(PromotedOperationalAllocationError):
            PromotedOperationalPortfolioContext(
                portfolio=valid_portfolio,
                source_portfolio_artifact_id=_SOURCE_PORTFOLIO_ARTIFACT_ID,
                open_listing_keys=("NSE:RELIANCE",),
            )

    def test_quote_vetoes_are_preserved_and_zero_pass_batch_has_unchanged_state(
        self,
    ) -> None:
        preparation = _QUOTE_GATE_FIXTURE._preparation()
        spec = _QUOTE_GATE_FIXTURE._spec(preparation)
        happy_quotes = _QUOTE_GATE_FIXTURE._happy_quotes(preparation)
        vetoed_quotes = tuple(replace(quote, depth_sell=()) for quote in happy_quotes)
        sorted_keys = tuple(sorted(preparation.manifest.listing_keys))
        quote_batch = FullQuoteBatch(
            requested_keys=sorted_keys,
            requested_at=_QUOTE_GATE_EVALUATED_AT - timedelta(seconds=3),
            observed_at=_QUOTE_GATE_EVALUATED_AT - timedelta(seconds=1),
            provider_version="kiteconnect/5.2.0",
            quotes=tuple(
                sorted(vetoed_quotes, key=lambda value: value.listing_key)
            ),
        )
        quote_gate_batch = evaluate_promoted_operational_quote_gate(
            spec=spec, quote_batch=quote_batch, evaluated_at=_QUOTE_GATE_EVALUATED_AT
        )
        self.assertEqual(quote_gate_batch.pass_count, 0)
        self.assertEqual(quote_gate_batch.veto_count, 2)

        portfolio = _portfolio(quote_gate_batch)
        portfolio_context = _portfolio_context(portfolio)
        allocation_policy = _allocation_policy()

        allocation_batch = assemble_promoted_operational_allocation_batch(
            quote_gate_batch=quote_gate_batch,
            portfolio_context=portfolio_context,
            allocation_policy=allocation_policy,
        )

        self.assertEqual(allocation_batch.allocation_outcomes, ())
        self.assertEqual(allocation_batch.allocated_count, 0)
        self.assertEqual(allocation_batch.veto_count, 0)
        self.assertEqual(
            allocation_batch.upstream_quote_vetoes, quote_gate_batch.outcomes
        )
        self.assertEqual(
            allocation_batch.final_state.state_id, allocation_batch.initial_state.state_id
        )
        allocation_batch.verify_content_identity()

    def test_direct_construction_reordering_state_chain_count_and_self_consistent_mutation_fail_closed(
        self,
    ) -> None:
        preparation, quote_gate_batch = self._quote_gate_batch()
        portfolio = _portfolio(quote_gate_batch)
        portfolio_context = _portfolio_context(portfolio)
        allocation_policy = _allocation_policy()
        allocation_batch = assemble_promoted_operational_allocation_batch(
            quote_gate_batch=quote_gate_batch,
            portfolio_context=portfolio_context,
            allocation_policy=allocation_policy,
        )
        outcome_a, outcome_b = allocation_batch.allocation_outcomes
        self.assertTrue(outcome_a.allocated)
        self.assertFalse(outcome_b.allocated)

        with self.assertRaises(PromotedOperationalAllocationError):
            VerifiedPromotedOperationalAllocationBatch(
                quote_gate_batch=quote_gate_batch,
                portfolio_context=portfolio_context,
                allocation_policy=allocation_policy,
                allocation_outcomes=(outcome_b, outcome_a),
                upstream_quote_vetoes=allocation_batch.upstream_quote_vetoes,
                initial_state=allocation_batch.initial_state,
                final_state=allocation_batch.final_state,
                allocated_count=allocation_batch.allocated_count,
                veto_count=allocation_batch.veto_count,
                paper_only=True,
                notification_eligible=False,
                execution_eligible=False,
            )

        with self.assertRaises(PromotedOperationalAllocationError):
            VerifiedPromotedOperationalAllocationBatch(
                quote_gate_batch=quote_gate_batch,
                portfolio_context=portfolio_context,
                allocation_policy=allocation_policy,
                allocation_outcomes=(outcome_a,),
                upstream_quote_vetoes=allocation_batch.upstream_quote_vetoes,
                initial_state=allocation_batch.initial_state,
                final_state=allocation_batch.final_state,
                allocated_count=1,
                veto_count=0,
                paper_only=True,
                notification_eligible=False,
                execution_eligible=False,
            )

        with self.assertRaises(PromotedOperationalAllocationError):
            VerifiedPromotedOperationalAllocationBatch(
                quote_gate_batch=quote_gate_batch,
                portfolio_context=portfolio_context,
                allocation_policy=allocation_policy,
                allocation_outcomes=(outcome_a, outcome_a),
                upstream_quote_vetoes=allocation_batch.upstream_quote_vetoes,
                initial_state=allocation_batch.initial_state,
                final_state=allocation_batch.final_state,
                allocated_count=allocation_batch.allocated_count,
                veto_count=allocation_batch.veto_count,
                paper_only=True,
                notification_eligible=False,
                execution_eligible=False,
            )

        with self.assertRaises(PromotedOperationalAllocationError):
            VerifiedPromotedOperationalAllocationBatch(
                quote_gate_batch=quote_gate_batch,
                portfolio_context=portfolio_context,
                allocation_policy=allocation_policy,
                allocation_outcomes=allocation_batch.allocation_outcomes,
                upstream_quote_vetoes=allocation_batch.upstream_quote_vetoes,
                initial_state=allocation_batch.initial_state,
                final_state=allocation_batch.final_state,
                allocated_count=allocation_batch.allocated_count + 1,
                veto_count=allocation_batch.veto_count,
                paper_only=True,
                notification_eligible=False,
                execution_eligible=False,
            )

        # Self-consistent tampering: portfolio context.
        tampered_context = PromotedOperationalPortfolioContext(
            portfolio=portfolio_context.portfolio,
            source_portfolio_artifact_id=portfolio_context.source_portfolio_artifact_id,
            open_listing_keys=portfolio_context.open_listing_keys,
        )
        object.__setattr__(tampered_context, "open_listing_keys", ("NSE:FORGED",))
        object.__setattr__(
            tampered_context, "context_id", tampered_context._calculated_id()
        )
        with self.assertRaises(PromotedOperationalAllocationError):
            tampered_context.verify_content_identity()

        # Self-consistent tampering: allocation policy.
        tampered_policy = PromotedOperationalAllocationPolicy(
            policy=allocation_policy.policy,
            maximum_portfolio_age_seconds=allocation_policy.maximum_portfolio_age_seconds,
            paper_only=allocation_policy.paper_only,
            notification_eligible=allocation_policy.notification_eligible,
            execution_eligible=allocation_policy.execution_eligible,
        )
        object.__setattr__(tampered_policy, "maximum_portfolio_age_seconds", -5)
        object.__setattr__(
            tampered_policy, "allocation_policy_id", tampered_policy._calculated_id()
        )
        with self.assertRaises(PromotedOperationalAllocationError):
            tampered_policy.verify_content_identity()

        # Self-consistent tampering: allocation state.
        tampered_state = PromotedOperationalAllocationState(
            cash_available=allocation_batch.initial_state.cash_available,
            gross_exposure=allocation_batch.initial_state.gross_exposure,
            open_risk=allocation_batch.initial_state.open_risk,
            open_listing_keys=allocation_batch.initial_state.open_listing_keys,
        )
        object.__setattr__(tampered_state, "cash_available", Decimal("-1"))
        object.__setattr__(tampered_state, "state_id", tampered_state._calculated_id())
        with self.assertRaises(PromotedOperationalAllocationError):
            tampered_state.verify_content_identity()

        # Self-consistent tampering: allocation outcome (forge the real VETO
        # into a self-consistent ALLOCATED with a recomputed ID).
        tampered_outcome = PromotedOperationalAllocationOutcome(
            quote_outcome=outcome_b.quote_outcome,
            portfolio_context=outcome_b.portfolio_context,
            allocation_policy=outcome_b.allocation_policy,
            state_before=outcome_b.state_before,
            state_after=outcome_b.state_after,
            disposition=outcome_b.disposition,
            reason_codes=outcome_b.reason_codes,
            operational_quantity=outcome_b.operational_quantity,
            reference_entry_price=outcome_b.reference_entry_price,
            entry_notional=outcome_b.entry_notional,
            estimated_round_trip_cost=outcome_b.estimated_round_trip_cost,
            planned_max_loss=outcome_b.planned_max_loss,
            operational_net_reward_risk=outcome_b.operational_net_reward_risk,
        )
        object.__setattr__(
            tampered_outcome,
            "disposition",
            PromotedOperationalAllocationDisposition.ALLOCATED,
        )
        object.__setattr__(tampered_outcome, "reason_codes", ())
        object.__setattr__(tampered_outcome, "operational_quantity", 5)
        object.__setattr__(
            tampered_outcome,
            "allocation_outcome_id",
            tampered_outcome._calculated_id(),
        )
        with self.assertRaises(PromotedOperationalAllocationError):
            tampered_outcome.verify_content_identity()

    def test_decimal_results_are_independent_of_global_context_and_restore_context_in_finally(
        self,
    ) -> None:
        # Exercise both the private allocation calculation and the complete
        # public builder. Quote midpoint/spread replay is now context-isolated,
        # so the entire pure boundary must remain stable under the caller's
        # aggressively restricted Decimal precision.
        preparation, quote_gate_batch = self._quote_gate_batch()
        portfolio = _portfolio(quote_gate_batch)
        portfolio_context = _portfolio_context(portfolio)
        allocation_policy = _allocation_policy()
        quote_outcome = quote_gate_batch.outcomes[0]
        initial_state = PromotedOperationalAllocationState(
            cash_available=portfolio.cash_available,
            gross_exposure=portfolio.gross_exposure,
            open_risk=portfolio.open_risk,
            open_listing_keys=portfolio_context.open_listing_keys,
        )

        baseline = _evaluate_allocation(
            quote_outcome, portfolio_context, allocation_policy, initial_state
        )
        baseline_batch = assemble_promoted_operational_allocation_batch(
            quote_gate_batch=quote_gate_batch,
            portfolio_context=portfolio_context,
            allocation_policy=allocation_policy,
        )

        original_precision = decimal.getcontext().prec
        decimal.getcontext().prec = 1
        try:
            restricted = _evaluate_allocation(
                quote_outcome, portfolio_context, allocation_policy, initial_state
            )
            restricted_batch = assemble_promoted_operational_allocation_batch(
                quote_gate_batch=quote_gate_batch,
                portfolio_context=portfolio_context,
                allocation_policy=allocation_policy,
            )
            restricted_batch.verify_content_identity()
        finally:
            decimal.getcontext().prec = original_precision

        self.assertEqual(decimal.getcontext().prec, original_precision)
        self.assertEqual(
            restricted_batch.allocation_batch_id,
            baseline_batch.allocation_batch_id,
        )
        (
            baseline_disposition,
            baseline_reasons,
            baseline_quantity,
            baseline_notional,
            baseline_cost,
            baseline_loss,
            baseline_net_reward_risk,
            baseline_state_after,
        ) = baseline
        (
            restricted_disposition,
            restricted_reasons,
            restricted_quantity,
            restricted_notional,
            restricted_cost,
            restricted_loss,
            restricted_net_reward_risk,
            restricted_state_after,
        ) = restricted
        self.assertEqual(restricted_disposition, baseline_disposition)
        self.assertEqual(restricted_reasons, baseline_reasons)
        self.assertEqual(restricted_quantity, baseline_quantity)
        self.assertEqual(restricted_notional, baseline_notional)
        self.assertEqual(restricted_cost, baseline_cost)
        self.assertEqual(restricted_loss, baseline_loss)
        self.assertEqual(restricted_net_reward_risk, baseline_net_reward_risk)
        self.assertEqual(restricted_state_after.state_id, baseline_state_after.state_id)


if __name__ == "__main__":
    unittest.main()
