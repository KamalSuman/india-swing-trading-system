from __future__ import annotations

import tempfile
import unittest
from copy import deepcopy
from dataclasses import fields, replace
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from india_swing.recommendations import (
    RESEARCH_WARNING,
    LocalSwingDecisionOutbox,
    SwingDailyDecision,
    SwingDecisionAction,
    SwingDecisionError,
    SwingDecisionNotification,
    SwingDecisionOutboxError,
    SwingDecisionPackage,
    SwingTradeRecommendation,
    assemble_swing_daily_decision,
    build_swing_decision_package,
    notification_from_swing_decision,
    render_swing_decision,
)
from india_swing.risk.swing_portfolio import (
    SwingPortfolioSizingPolicy,
    SwingPortfolioSnapshot,
    SwingSizingReason,
)

from tests import test_swing_opportunity_ranking as ranking_fixtures


D = Decimal


class SwingDecisionPackageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        fixture = ranking_fixtures.SwingOpportunityRankingBatchTests(
            methodName="test_ranks_every_pass_with_explainable_bounded_components"
        )
        fixture.setUp()
        cls.ranking_fixture = fixture
        cls.quote_fixture = fixture.fixture
        cls.proposal_batch = cls.quote_fixture.proposal_batch
        cls.quote_batch = fixture.quote_batch
        cls.evaluated_at = cls.quote_fixture.evaluated_at
        cls.portfolio = SwingPortfolioSnapshot(
            capital=D("100000"),
            cash_available=D("100000"),
            gross_exposure=D("0"),
            open_risk=D("0"),
            open_positions=0,
            daily_realized_pnl=D("0"),
            pilot_realized_pnl=D("0"),
            as_of=cls.evaluated_at - timedelta(seconds=1),
        )
        cls.base_package = build_swing_decision_package(
            proposal_batch=cls.proposal_batch,
            quote_batch=cls.quote_batch,
            portfolio=cls.portfolio,
            evaluated_at=cls.evaluated_at,
        )

    def _portfolio(self, **overrides) -> SwingPortfolioSnapshot:
        values = dict(
            capital=D("100000"),
            cash_available=D("100000"),
            gross_exposure=D("0"),
            open_risk=D("0"),
            open_positions=0,
            daily_realized_pnl=D("0"),
            pilot_realized_pnl=D("0"),
            as_of=self.evaluated_at - timedelta(seconds=1),
        )
        values.update(overrides)
        return SwingPortfolioSnapshot(**values)

    def _build(self, *, fresh: bool = False, **overrides) -> SwingDecisionPackage:
        if not fresh and not overrides:
            return self.base_package
        values = dict(
            proposal_batch=self.proposal_batch,
            quote_batch=self.quote_batch,
            portfolio=self.portfolio,
            evaluated_at=self.evaluated_at,
        )
        values.update(overrides)
        return build_swing_decision_package(**values)

    def test_complete_happy_path_produces_one_manual_buy_package(self) -> None:
        package = self._build()
        decision = package.decision
        recommendation = decision.recommendation

        self.assertEqual(decision.action, SwingDecisionAction.BUY)
        self.assertIsNotNone(recommendation)
        self.assertEqual(decision.sizing_batch.sized_subject_count, 1)
        self.assertEqual(decision.sizing_batch.vetoed_subject_count, 1)
        self.assertEqual(recommendation.quantity, 2)
        self.assertEqual(
            recommendation.symbol,
            decision.sizing_batch.ranking_batch.ranked_opportunities[0].symbol,
        )
        self.assertLessEqual(
            recommendation.sizing_outcome.planned_max_loss,
            D("500"),
        )
        self.assertGreater(recommendation.planned_net_reward, D("0"))
        self.assertIn(
            SwingSizingReason.MAX_NEW_POSITIONS_PER_RUN_REACHED.value,
            decision.sizing_batch.outcomes[1].reason_codes,
        )
        self.assertTrue(decision.research_only)
        self.assertFalse(decision.execution_eligible)
        self.assertTrue(package.research_only)
        self.assertFalse(package.execution_eligible)
        package.verify_content_identity()

    def test_notification_contains_levels_sizing_logic_lineage_and_cancellations(self) -> None:
        package = self._build()
        message = package.notification.message
        recommendation = package.decision.recommendation

        self.assertTrue(message.startswith(RESEARCH_WARNING + "\n"))
        for required in (
            "Decision: BUY",
            "Comparative score (not confidence):",
            "Entry range: INR",
            "Planned maximum loss: INR",
            "Planned net reward at target: INR",
            "Why this trade:",
            "Cancel / re-evaluate if:",
            "Evidence IDs:",
            "cannot place an order",
        ):
            self.assertIn(required, message)
        self.assertEqual(
            package.notification.message,
            render_swing_decision(package.decision),
        )
        self.assertGreaterEqual(len(recommendation.rationale), 10)
        self.assertEqual(len(recommendation.cancellation_conditions), 6)
        package.notification.verify_content_identity()

    def test_build_is_deterministic_for_identical_immutable_inputs(self) -> None:
        first = self._build()
        second = self._build(fresh=True)

        self.assertEqual(first.package_id, second.package_id)
        self.assertEqual(first.decision.decision_id, second.decision.decision_id)
        self.assertEqual(
            first.notification.notification_id,
            second.notification.notification_id,
        )

    def test_zero_cash_produces_auditable_no_trade_with_sizing_reasons(self) -> None:
        package = self._build(portfolio=self._portfolio(cash_available=D("0")))

        self.assertEqual(package.decision.action, SwingDecisionAction.NO_TRADE)
        self.assertIsNone(package.decision.recommendation)
        self.assertEqual(package.decision.sizing_batch.sized_subject_count, 0)
        self.assertTrue(
            any(
                code.endswith(":" + SwingSizingReason.CASH_EXHAUSTED.value)
                for code in package.decision.veto_reason_codes
            )
        )
        self.assertIn("Decision: NO_TRADE", package.notification.message)
        self.assertIn("No opportunity survived", package.notification.message)

    def test_quote_veto_is_preserved_while_another_subject_can_be_selected(self) -> None:
        quote = replace(self.quote_batch.quotes[0], depth_sell=())
        quote_batch = replace(
            self.quote_batch,
            quotes=(quote, self.quote_batch.quotes[1]),
        )
        package = self._build(quote_batch=quote_batch)

        self.assertEqual(package.decision.action, SwingDecisionAction.BUY)
        self.assertEqual(
            package.decision.sizing_batch.upstream_vetoes,
            package.decision.sizing_batch.ranking_batch.vetoed_outcomes,
        )
        self.assertTrue(
            any(code.startswith("QUOTE:") for code in package.decision.veto_reason_codes)
        )

    def test_all_quote_vetoes_produce_no_trade_without_losing_coverage(self) -> None:
        quotes = tuple(replace(quote, depth_sell=()) for quote in self.quote_batch.quotes)
        package = self._build(quote_batch=replace(self.quote_batch, quotes=quotes))

        self.assertEqual(package.decision.action, SwingDecisionAction.NO_TRADE)
        self.assertEqual(package.decision.sizing_batch.outcomes, ())
        self.assertEqual(len(package.decision.sizing_batch.upstream_vetoes), 2)
        self.assertTrue(
            any(code.startswith("QUOTE:") for code in package.decision.veto_reason_codes)
        )

    def test_multi_trade_sizing_policy_is_rejected_at_single_decision_boundary(self) -> None:
        policy = replace(
            SwingPortfolioSizingPolicy(),
            maximum_new_positions_per_run=2,
        )
        with self.assertRaises(SwingDecisionError):
            self._build(sizing_policy=policy)

    def test_future_portfolio_and_wrong_exact_inputs_fail_closed(self) -> None:
        with self.assertRaises(SwingDecisionError):
            self._build(
                portfolio=self._portfolio(
                    as_of=self.evaluated_at + timedelta(microseconds=1)
                )
            )
        for key in ("proposal_batch", "quote_batch", "portfolio", "evaluated_at"):
            with self.subTest(key=key):
                with self.assertRaises(SwingDecisionError):
                    self._build(**{key: object()})

    def test_direct_construction_cannot_forge_action_rationale_message_or_lineage(self) -> None:
        package = self._build()
        decision = package.decision
        recommendation = decision.recommendation
        self.assertIsNotNone(recommendation)

        with self.assertRaises(SwingDecisionError):
            replace(decision, action=SwingDecisionAction.NO_TRADE, recommendation=None)
        with self.assertRaises(SwingDecisionError):
            replace(recommendation, rationale=("Looks good",))
        with self.assertRaises(SwingDecisionError):
            replace(decision, veto_reason_codes=())
        with self.assertRaises(SwingDecisionError):
            replace(package.notification, message=package.notification.message + "changed")
        with self.assertRaises(SwingDecisionError):
            replace(package, notification=notification_from_swing_decision(
                self._build(portfolio=self._portfolio(cash_available=D("0"))).decision
            ))
        forged_notification = deepcopy(package.notification)
        object.__setattr__(
            forged_notification,
            "message",
            forged_notification.message + "changed",
        )
        object.__setattr__(
            forged_notification,
            "notification_id",
            forged_notification._calculated_id(),
        )
        with self.assertRaisesRegex(SwingDecisionError, "message hash"):
            forged_notification.verify_content_identity()

    def test_nested_mutation_is_detected_without_changing_outer_package_id(self) -> None:
        package = self._build(fresh=True)
        original_id = package.package_id
        object.__setattr__(
            package.decision.sizing_batch.outcomes[0].state_after,
            "open_risk",
            D("99999"),
        )

        self.assertEqual(package.package_id, original_id)
        with self.assertRaises(Exception):
            package.verify_content_identity()

    def test_public_contract_has_no_probability_or_execution_override_fields(self) -> None:
        names = {
            item.name
            for contract in (
                SwingTradeRecommendation,
                SwingDailyDecision,
                SwingDecisionNotification,
                SwingDecisionPackage,
            )
            for item in fields(contract)
        }

        self.assertFalse(any("probability" in name for name in names))
        self.assertFalse(any("confidence" in name for name in names))
        self.assertNotIn("execution_eligible", names)


class LocalSwingDecisionOutboxTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        fixture = ranking_fixtures.SwingOpportunityRankingBatchTests(
            methodName="test_ranks_every_pass_with_explainable_bounded_components"
        )
        fixture.setUp()
        quote_fixture = fixture.fixture
        portfolio = SwingPortfolioSnapshot(
            capital=D("100000"),
            cash_available=D("100000"),
            gross_exposure=D("0"),
            open_risk=D("0"),
            open_positions=0,
            daily_realized_pnl=D("0"),
            pilot_realized_pnl=D("0"),
            as_of=quote_fixture.evaluated_at - timedelta(seconds=1),
        )
        cls.package = build_swing_decision_package(
            proposal_batch=quote_fixture.proposal_batch,
            quote_batch=fixture.quote_batch,
            portfolio=portfolio,
            evaluated_at=quote_fixture.evaluated_at,
        )

    def test_create_once_put_get_and_idempotent_replay(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = LocalSwingDecisionOutbox(Path(directory))

            first = store.put(self.package)
            second = store.put(self.package)
            loaded = store.get(self.package.decision.decision_id)

            self.assertEqual(first, second)
            self.assertEqual(first, loaded)
            self.assertEqual(first.notification_id, self.package.notification.notification_id)
            self.assertEqual(
                tuple(store.notifications_root.glob("*.json")),
                (store.path_for(self.package.decision.decision_id),),
            )

    def test_tampered_existing_content_is_rejected_on_read_and_republish(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = LocalSwingDecisionOutbox(Path(directory))
            store.put(self.package)
            path = store.path_for(self.package.decision.decision_id)
            path.write_bytes(path.read_bytes().replace(b"Decision: BUY", b"Decision: BAD"))

            with self.assertRaises(SwingDecisionOutboxError):
                store.get(self.package.decision.decision_id)
            with self.assertRaises(SwingDecisionOutboxError):
                store.put(self.package)

    def test_outbox_rejects_path_traversal_wrong_types_and_mutated_package(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = LocalSwingDecisionOutbox(Path(directory))
            for value in ("../escape", "A" * 64, "0" * 63, object()):
                with self.subTest(value=value):
                    with self.assertRaises(SwingDecisionOutboxError):
                        store.path_for(value)
            with self.assertRaises(SwingDecisionOutboxError):
                store.put(object())
            mutated = deepcopy(self.package)
            object.__setattr__(mutated.notification, "message_sha256", "0" * 64)
            with self.assertRaises(SwingDecisionOutboxError):
                store.put(mutated)

    def test_outbox_rejects_duplicate_key_json(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = LocalSwingDecisionOutbox(Path(directory))
            path = store.path_for(self.package.decision.decision_id)
            path.parent.mkdir(parents=True)
            path.write_bytes(
                b'{"codec_schema_version":"x","codec_schema_version":"x","notification":{}}'
            )

            with self.assertRaisesRegex(SwingDecisionOutboxError, "duplicate keys"):
                store.get(self.package.decision.decision_id)


if __name__ == "__main__":
    unittest.main()
