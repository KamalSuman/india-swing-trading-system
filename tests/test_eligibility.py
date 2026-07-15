from __future__ import annotations

import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from india_swing.domain.models import (
    Board,
    DataSnapshot,
    InstrumentSnapshot,
    MarketCapBucket,
    RiskPolicy,
    Surveillance,
)
from india_swing.universe.eligibility import evaluate_eligibility


IST = timezone(timedelta(hours=5, minutes=30))
MARKET_SESSION = date(2026, 7, 15)
DECISION_TIME = datetime(2026, 7, 15, 17, 0, tzinfo=IST)


class EligibilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = RiskPolicy()
        self.snapshot = DataSnapshot(
            snapshot_id="snapshot-1",
            decision_time=DECISION_TIME,
            market_session=MARKET_SESSION,
            evidence=(),
            session_finalized_at=DECISION_TIME - timedelta(minutes=30),
            universe_snapshot_id="universe-1",
            calendar_version="calendar-1",
            trial_id="trial-1",
            model_bundle_id="bundle-1",
            data_content_hash="data-hash-1",
            source_revision="source-1",
            execution_policy_version="execution-1",
            cost_schedule_version="cost-1",
        )

    def instrument(
        self,
        *,
        board: Board = Board.MAIN,
        market_cap_bucket: MarketCapBucket = MarketCapBucket.SMALL,
        surveillance: Surveillance = Surveillance.NONE,
        price_session: date = MARKET_SESSION,
        data_available_at: datetime | None = None,
    ) -> InstrumentSnapshot:
        return InstrumentSnapshot(
            symbol="TEST",
            board=board,
            market_cap_bucket=market_cap_bucket,
            active=True,
            suspended=False,
            surveillance=surveillance,
            last_price=Decimal("100"),
            median_daily_traded_value=Decimal("10000000"),
            quoted_spread_bps=Decimal("20"),
            lower_circuit_locked=False,
            history_sessions=250,
            price_session=price_session,
            data_available_at=data_available_at or DECISION_TIME - timedelta(minutes=10),
        )

    def test_small_and_micro_main_board_instruments_can_be_actionable(self) -> None:
        for bucket in (MarketCapBucket.SMALL, MarketCapBucket.MICRO):
            with self.subTest(market_cap_bucket=bucket):
                result = evaluate_eligibility(
                    self.instrument(market_cap_bucket=bucket),
                    self.policy,
                    self.snapshot,
                )
                self.assertTrue(result.actionable)
                self.assertFalse(result.watch_only)
                self.assertEqual((), result.reasons)

    def test_sme_instrument_is_watch_only_and_not_actionable(self) -> None:
        result = evaluate_eligibility(
            self.instrument(board=Board.SME),
            self.policy,
            self.snapshot,
        )
        self.assertFalse(result.actionable)
        self.assertTrue(result.watch_only)
        self.assertIn("SME instruments are watch-only in the pilot", result.reasons)

    def test_banned_surveillance_categories_are_not_actionable(self) -> None:
        for category in (
            Surveillance.ASM,
            Surveillance.GSM,
            Surveillance.TRADE_TO_TRADE,
        ):
            with self.subTest(surveillance=category):
                result = evaluate_eligibility(
                    self.instrument(surveillance=category),
                    self.policy,
                    self.snapshot,
                )
                self.assertFalse(result.actionable)
                self.assertIn(
                    f"surveillance category {category.value} is blocked",
                    result.reasons,
                )

    def test_stale_price_session_is_not_actionable(self) -> None:
        result = evaluate_eligibility(
            self.instrument(price_session=MARKET_SESSION - timedelta(days=1)),
            self.policy,
            self.snapshot,
        )
        self.assertFalse(result.actionable)
        self.assertIn("price data is not from the required market session", result.reasons)

    def test_market_data_available_after_cutoff_is_not_actionable(self) -> None:
        result = evaluate_eligibility(
            self.instrument(data_available_at=DECISION_TIME + timedelta(seconds=1)),
            self.policy,
            self.snapshot,
        )
        self.assertFalse(result.actionable)
        self.assertIn("market data was unavailable at the decision cutoff", result.reasons)

    def test_market_cap_bucket_is_not_an_eligibility_cutoff(self) -> None:
        for bucket in MarketCapBucket:
            with self.subTest(market_cap_bucket=bucket):
                result = evaluate_eligibility(
                    self.instrument(market_cap_bucket=bucket),
                    self.policy,
                    self.snapshot,
                )
                self.assertTrue(result.actionable)
                self.assertEqual((), result.reasons)


if __name__ == "__main__":
    unittest.main()
