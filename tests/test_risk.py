from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_FLOOR
from datetime import timezone

from india_swing.domain.models import (
    Board,
    Candidate,
    DecisionAction,
    ForecastSummary,
    InstrumentSnapshot,
    MarketCapBucket,
    PortfolioState,
    ProbabilityStatus,
    ResearchAssessment,
    ResearchVerdict,
    RiskPolicy,
    SignalFeatures,
    Surveillance,
    TradeSetup,
)
from india_swing.risk.engine import RiskEngine


D = Decimal
IST = timezone(timedelta(hours=5, minutes=30))


class RiskEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        decision_time = datetime(2026, 7, 15, 16, 0, tzinfo=IST)
        instrument = InstrumentSnapshot(
            instrument_id="instrument-reliance",
            listing_id="listing-reliance",
            universe_snapshot_id="universe-1",
            exchange="NSE",
            segment="CM",
            symbol="RELIANCE",
            board=Board.MAIN,
            market_cap_bucket=MarketCapBucket.LARGE,
            active=True,
            suspended=False,
            surveillance=Surveillance.NONE,
            last_price=D("98"),
            median_daily_traded_value=D("10000000"),
            quoted_spread_bps=D("8"),
            lower_circuit_locked=False,
            history_sessions=500,
            price_session=decision_time.date(),
            data_available_at=decision_time,
        )
        forecast = ForecastSummary(
            symbol="RELIANCE",
            as_of=decision_time,
            horizon_sessions=5,
            median_return_pct=D("4.5"),
            downside_return_pct=D("-2.0"),
            uncertainty=D("0.25"),
            sample_count=100,
            model_version="kronos-small@pilot",
            instrument_id="instrument-reliance",
            listing_id="listing-reliance",
            universe_snapshot_id="universe-1",
            data_snapshot_id="snapshot-1",
            data_snapshot_fingerprint="snapshot-fingerprint-1",
            instrument_fingerprint=instrument.content_fingerprint,
        )
        signals = SignalFeatures(
            relative_strength=D("0.85"),
            trend_quality=D("0.80"),
            volume_confirmation=D("0.75"),
            liquidity_quality=D("0.95"),
            news_score=D("0.30"),
            estimated_cost_bps=D("80"),
            instrument_id="instrument-reliance",
            listing_id="listing-reliance",
            universe_snapshot_id="universe-1",
            data_snapshot_id="snapshot-1",
            data_snapshot_fingerprint="snapshot-fingerprint-1",
            instrument_fingerprint=instrument.content_fingerprint,
            provider_version="signal-test-v1",
        )
        setup = TradeSetup(
            symbol="RELIANCE",
            decision_time=decision_time,
            earliest_entry_at=decision_time + timedelta(hours=17, minutes=15),
            entry_low=D("95"),
            entry_high=D("100"),
            stop=D("90"),
            target=D("128.5"),
            target_probability=D("0.50"),
            stop_probability=D("0.20"),
            expected_time_exit_r=D("0.20"),
            max_holding_sessions=5,
            setup_reason="trend continuation",
            stop_reason="structure invalidation",
            target_reason="cost-adjusted 2.5R objective",
            cancel_conditions=("gap above entry range", "liquidity deterioration"),
            entry_expires_at=decision_time + timedelta(days=1, hours=23),
            instrument_id="instrument-reliance",
            listing_id="listing-reliance",
            universe_snapshot_id="universe-1",
            data_snapshot_id="snapshot-1",
            data_snapshot_fingerprint="snapshot-fingerprint-1",
            instrument_fingerprint=instrument.content_fingerprint,
            provider_version="signal-test-v1",
        )
        self.candidate = Candidate(
            instrument=instrument,
            forecast=forecast,
            signals=signals,
            setup=setup,
            evidence_ids=("price-1", "forecast-1"),
        )
        self.research = ResearchAssessment(
            symbol="RELIANCE",
            verdict=ResearchVerdict.APPROVE,
            thesis="Momentum and participation support the setup.",
            bear_case="A broad-market reversal could invalidate the breakout.",
            risks=("overnight gap",),
            evidence_ids=("price-1", "forecast-1"),
            model_version="tradingagents@01477f9",
            instrument_id="instrument-reliance",
            listing_id="listing-reliance",
            universe_snapshot_id="universe-1",
            data_snapshot_id="snapshot-1",
            data_snapshot_fingerprint="snapshot-fingerprint-1",
            instrument_fingerprint=instrument.content_fingerprint,
        )
        self.portfolio = PortfolioState(
            capital=D("100000"),
            open_risk=D("0"),
            gross_exposure=D("0"),
        )
        self.policy = RiskPolicy(
            estimated_round_trip_cost_bps=D("100"),
            require_validated_probabilities=False,
        )

    def evaluate(
        self,
        *,
        candidate: Candidate | None = None,
        research: ResearchAssessment | None = None,
        portfolio: PortfolioState | None = None,
        policy: RiskPolicy | None = None,
        rank: int = 1,
    ):
        return RiskEngine(policy or self.policy).evaluate(
            candidate or self.candidate,
            research or self.research,
            portfolio or self.portfolio,
            rank,
        )

    def test_sizing_uses_conservative_entry_high(self) -> None:
        evaluation = self.evaluate()

        self.assertTrue(evaluation.approved)
        decision = evaluation.decision
        self.assertIsNotNone(decision)
        assert decision is not None

        # At entry_high, the 100 bps round-trip estimate makes net loss/share
        # 100 - 90 + 1 = 11, so the Rs 250 budget permits floor(250 / 11) = 22.
        self.assertEqual(decision.quantity, 22)
        self.assertEqual(decision.planned_max_loss, D("242"))

        less_conservative_quantity = int(
            (self.policy.per_trade_risk / D("6")).to_integral_value(rounding=ROUND_FLOOR)
        )
        self.assertEqual(less_conservative_quantity, 41)
        self.assertNotEqual(decision.quantity, less_conservative_quantity)

    def test_position_notional_cannot_exceed_remaining_cash(self) -> None:
        tiny_account = PortfolioState(D("150"), D("0"), D("0"))

        evaluation = self.evaluate(portfolio=tiny_account)

        self.assertTrue(evaluation.approved)
        assert evaluation.decision is not None
        self.assertEqual(evaluation.decision.quantity, 1)
        self.assertLessEqual(
            evaluation.decision.quantity * self.candidate.setup.entry_high,
            tiny_account.capital,
        )

    def test_account_without_enough_cash_is_rejected(self) -> None:
        unaffordable = PortfolioState(D("99"), D("0"), D("0"))

        evaluation = self.evaluate(portfolio=unaffordable)

        self.assertFalse(evaluation.approved)
        self.assertIn("risk, exposure, or liquidity caps produce zero quantity", evaluation.reasons)

    def test_default_policy_rejects_provisional_probabilities(self) -> None:
        policy = RiskPolicy(estimated_round_trip_cost_bps=D("100"))

        evaluation = self.evaluate(policy=policy)

        self.assertFalse(evaluation.approved)
        self.assertIn("probability estimate is not validated", evaluation.reasons)

    def test_validated_probabilities_with_sufficient_calibration_can_pass(self) -> None:
        validated_setup = replace(
            self.candidate.setup,
            probability_status=ProbabilityStatus.VALIDATED,
            calibration_sample_size=100,
        )
        candidate = replace(self.candidate, setup=validated_setup)
        policy = RiskPolicy(estimated_round_trip_cost_bps=D("100"))

        evaluation = self.evaluate(candidate=candidate, policy=policy)

        self.assertTrue(evaluation.approved)

    def test_portfolio_halts_block_new_positions(self) -> None:
        cases = (
            (
                "position count",
                replace(self.portfolio, open_positions=2),
                "maximum number of open positions is already reached",
            ),
            (
                "daily loss",
                replace(self.portfolio, daily_realized_pnl=D("-750")),
                "daily loss halt is active",
            ),
            (
                "pilot drawdown",
                replace(self.portfolio, pilot_realized_pnl=D("-1500")),
                "pilot drawdown halt is active",
            ),
        )
        for label, portfolio, reason in cases:
            with self.subTest(halt=label):
                evaluation = self.evaluate(portfolio=portfolio)
                self.assertFalse(evaluation.approved)
                self.assertIn(reason, evaluation.reasons)

    def test_net_reward_risk_is_exactly_2_5_after_costs(self) -> None:
        evaluation = self.evaluate()

        self.assertTrue(evaluation.approved)
        decision = evaluation.decision
        self.assertIsNotNone(decision)
        assert decision is not None

        gross_reward_risk = (D("128.5") - D("100")) / (D("100") - D("90"))
        self.assertEqual(gross_reward_risk, D("2.85"))
        self.assertEqual(decision.estimated_cost / decision.quantity, D("1"))
        self.assertEqual(decision.net_reward_risk, D("2.5"))

    def test_liquidity_notional_and_remaining_open_risk_caps_bind_quantity(self) -> None:
        liquidity_instrument = replace(
            self.candidate.instrument,
            median_daily_traded_value=D("400000"),
        )
        liquidity_candidate = replace(
            self.candidate,
            instrument=liquidity_instrument,
            forecast=replace(
                self.candidate.forecast,
                instrument_fingerprint=liquidity_instrument.content_fingerprint,
            ),
            signals=replace(
                self.candidate.signals,
                instrument_fingerprint=liquidity_instrument.content_fingerprint,
            ),
            setup=replace(
                self.candidate.setup,
                instrument_fingerprint=liquidity_instrument.content_fingerprint,
            ),
        )
        notional_policy = replace(self.policy, max_position_notional=D("1200"))
        open_risk_portfolio = replace(self.portfolio, open_risk=D("450"))

        cases = (
            ("liquidity", self.evaluate(candidate=liquidity_candidate), 10),
            ("notional", self.evaluate(policy=notional_policy), 12),
            ("remaining_open_risk", self.evaluate(portfolio=open_risk_portfolio), 4),
        )
        for label, evaluation, expected_quantity in cases:
            with self.subTest(cap=label):
                self.assertTrue(evaluation.approved)
                self.assertIsNotNone(evaluation.decision)
                assert evaluation.decision is not None
                self.assertEqual(evaluation.decision.quantity, expected_quantity)

        assert cases[2][1].decision is not None
        self.assertLessEqual(
            open_risk_portfolio.open_risk + cases[2][1].decision.planned_max_loss,
            self.policy.max_open_risk,
        )

    def test_research_veto_rejects_candidate(self) -> None:
        veto = replace(self.research, verdict=ResearchVerdict.VETO)

        evaluation = self.evaluate(research=veto)

        self.assertFalse(evaluation.approved)
        self.assertIsNone(evaluation.decision)
        self.assertEqual(evaluation.reasons, ("research verdict is VETO",))

    def test_cost_adjusted_expected_r_below_minimum_is_rejected(self) -> None:
        low_expectancy_setup = replace(
            self.candidate.setup,
            target_probability=D("0.20"),
            stop_probability=D("0.60"),
            expected_time_exit_r=D("0.20"),
        )
        candidate = replace(self.candidate, setup=low_expectancy_setup)

        evaluation = self.evaluate(candidate=candidate)

        self.assertFalse(evaluation.approved)
        self.assertIsNone(evaluation.decision)
        self.assertEqual(
            evaluation.reasons,
            ("cost-adjusted expected R is below the policy minimum",),
        )

    def test_approved_buy_contains_auditable_metadata(self) -> None:
        evaluation = self.evaluate(rank=3)

        self.assertTrue(evaluation.approved)
        self.assertEqual(evaluation.reasons, ())
        decision = evaluation.decision
        self.assertIsNotNone(decision)
        assert decision is not None

        self.assertIs(decision.action, DecisionAction.BUY)
        self.assertEqual(decision.symbol, "RELIANCE")
        self.assertEqual(len(decision.signal_id), 20)
        self.assertEqual(decision.decision_time, self.candidate.setup.decision_time)
        self.assertEqual(decision.thesis, self.research.thesis)
        self.assertEqual(decision.bear_case, self.research.bear_case)
        self.assertEqual(decision.cancel_conditions, self.candidate.setup.cancel_conditions)
        self.assertEqual(
            decision.reasons,
            (
                "trend continuation",
                "structure invalidation",
                "cost-adjusted 2.5R objective",
            ),
        )
        self.assertEqual(
            dict(decision.metadata),
            {
                "rank": "3",
                "instrument_id": "instrument-reliance",
                "instrument_fingerprint": self.candidate.instrument.content_fingerprint,
                "listing_id": "listing-reliance",
                "universe_snapshot_id": "universe-1",
                "risk_policy": "pilot-v1",
                "forecast_model": "kronos-small@pilot",
                "research_model": "tradingagents@01477f9",
            },
        )


if __name__ == "__main__":
    unittest.main()
