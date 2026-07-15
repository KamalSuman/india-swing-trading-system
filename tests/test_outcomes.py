from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from india_swing.domain.models import DecisionAction, ProbabilityStatus, TradeDecision
from india_swing.outcomes import (
    ExitReason,
    OutcomeEvidence,
    PostTradeAnalyzer,
    ReviewClassification,
    TradeOutcome,
)


IST = timezone(timedelta(hours=5, minutes=30))


def decision() -> TradeDecision:
    return TradeDecision(
        action=DecisionAction.BUY,
        signal_id="signal-1",
        decision_time=datetime(2026, 7, 15, 17, tzinfo=IST),
        symbol="DEMO",
        quantity=10,
        entry_low=Decimal("99"),
        entry_high=Decimal("100"),
        stop=Decimal("95"),
        target=Decimal("113"),
        planned_max_loss=Decimal("50"),
        estimated_cost=Decimal("2"),
        net_reward_risk=Decimal("2.5"),
        expected_r=Decimal("0.5"),
        reasons=("demo",),
        target_probability=Decimal("0.45"),
        stop_probability=Decimal("0.35"),
        probability_status=ProbabilityStatus.PROVISIONAL,
        earliest_entry_at=datetime(2026, 7, 16, 9, 15, tzinfo=IST),
        entry_expires_at=datetime(2026, 7, 16, 15, 15, tzinfo=IST),
        max_holding_sessions=8,
        order_type="LIMIT",
    )


def losing_outcome(**overrides) -> TradeOutcome:
    values = {
        "signal_id": "signal-1",
        "symbol": "DEMO",
        "entry_time": datetime(2026, 7, 16, 9, 20, tzinfo=IST),
        "exit_time": datetime(2026, 7, 18, 10, tzinfo=IST),
        "actual_entry": Decimal("100"),
        "actual_exit": Decimal("95"),
        "quantity": 10,
        "fees_and_taxes": Decimal("2"),
        "exit_reason": ExitReason.STOP,
    }
    values.update(overrides)
    return TradeOutcome(**values)


class PostTradeAnalyzerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.analyzer = PostTradeAnalyzer()

    def test_data_failure_requires_halt(self) -> None:
        review = self.analyzer.analyze(
            decision(), losing_outcome(), OutcomeEvidence(data_integrity_breach=True)
        )
        self.assertEqual(review.classification, ReviewClassification.DATA_FAILURE)
        self.assertTrue(review.requires_pipeline_halt)

    def test_data_integrity_breach_takes_precedence_over_profit(self) -> None:
        profitable_outcome = losing_outcome(
            actual_exit=Decimal("114"),
            exit_reason=ExitReason.TARGET,
        )

        review = self.analyzer.analyze(
            decision(),
            profitable_outcome,
            OutcomeEvidence(data_integrity_breach=True),
        )

        self.assertGreater(review.net_pnl, Decimal("0"))
        self.assertEqual(review.classification, ReviewClassification.DATA_FAILURE)
        self.assertTrue(review.requires_pipeline_halt)

    def test_material_post_entry_news_is_evidence_backed(self) -> None:
        review = self.analyzer.analyze(
            decision(),
            losing_outcome(),
            OutcomeEvidence(material_post_entry_evidence_ids=("nse-event-1",)),
        )
        self.assertEqual(review.classification, ReviewClassification.POST_ENTRY_NEWS_SHOCK)
        self.assertIn("nse-event-1", " ".join(review.known_facts))

    def test_execution_must_match_quantity_time_and_entry_range(self) -> None:
        cases = (
            ("quantity", {"quantity": 9}),
            (
                "too early",
                {"entry_time": datetime(2026, 7, 16, 9, 14, tzinfo=IST)},
            ),
            (
                "expired",
                {"entry_time": datetime(2026, 7, 16, 15, 16, tzinfo=IST)},
            ),
            ("outside price range", {"actual_entry": Decimal("101")}),
        )
        for label, overrides in cases:
            with self.subTest(deviation=label):
                review = self.analyzer.analyze(
                    decision(),
                    losing_outcome(**overrides),
                    OutcomeEvidence(),
                )
                self.assertEqual(
                    review.classification,
                    ReviewClassification.EXECUTION_DEVIATION,
                )
                self.assertIn("execution deviation", " ".join(review.known_facts))

    def test_profitable_execution_deviation_is_not_counted_as_model_profit(self) -> None:
        review = self.analyzer.analyze(
            decision(),
            losing_outcome(actual_exit=Decimal("114"), quantity=9),
            OutcomeEvidence(),
        )
        self.assertGreater(review.net_pnl, Decimal("0"))
        self.assertEqual(review.classification, ReviewClassification.EXECUTION_DEVIATION)

    def test_gap_loss_is_not_disguised_as_planned_stop(self) -> None:
        review = self.analyzer.analyze(
            decision(),
            losing_outcome(actual_exit=Decimal("90"), gap_through_stop=True),
            OutcomeEvidence(),
        )
        self.assertEqual(review.classification, ReviewClassification.TAIL_OR_GAP_LOSS)
        self.assertLess(review.realized_r, Decimal("-1"))

    def test_unknown_cause_stays_unresolved(self) -> None:
        review = self.analyzer.analyze(decision(), losing_outcome(), OutcomeEvidence())
        self.assertEqual(review.classification, ReviewClassification.UNRESOLVED_FORECAST_MISS)
        self.assertTrue(review.uncertainties)
        self.assertIn("cannot be established", review.uncertainties[0])

    def test_profitable_trade_is_also_reviewed(self) -> None:
        outcome = losing_outcome(actual_exit=Decimal("114"), exit_reason=ExitReason.TARGET)
        review = self.analyzer.analyze(decision(), outcome, OutcomeEvidence())
        self.assertEqual(review.classification, ReviewClassification.PROFITABLE_OUTCOME)
        self.assertGreater(review.net_pnl, Decimal("0"))


if __name__ == "__main__":
    unittest.main()
