from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

from india_swing.domain.models import (
    Board,
    DataSnapshot,
    EvidenceItem,
    ForecastSummary,
    InstrumentSnapshot,
    MarketCapBucket,
    ProbabilityStatus,
    Surveillance,
)
from india_swing.reference.calendar import (
    CalendarDay,
    CalendarDayKind,
    CalendarSnapshot,
    SessionWindow,
    SessionWindowPhase,
)
from india_swing.reference.models import ExternalRecordRef, ReferenceReadiness
from india_swing.signals.deterministic_swing import (
    AsOfSwingBar,
    DeterministicSwingSignalConfig,
    DeterministicSwingSignalError,
    DeterministicSwingSignalProvider,
    InstrumentSwingHistory,
    SwingNextEntryWindow,
    SwingTechnicalMetrics,
    SwingTradeLevels,
    calculate_next_entry_window,
    calculate_swing_technical_metrics,
    calculate_swing_trade_levels,
)
from india_swing.signals.calibration import (
    CalibrationObservation,
    CalibrationOutcome,
    CalibrationPartition,
    WalkForwardCalibrationPlan,
    build_walk_forward_calibration,
)


IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc
START = date(2026, 1, 1)
SIGNAL_SESSION = date(2026, 3, 1)
DECISION_TIME = datetime(2026, 3, 1, 17, 0, tzinfo=IST)
SOURCE_ID = "a" * 64


def D(value: str) -> Decimal:
    return Decimal(value)


def calendar() -> CalendarSnapshot:
    days = []
    for offset in range(80):
        day = START + timedelta(days=offset)
        days.append(
            CalendarDay(
                day=day,
                kind=CalendarDayKind.REGULAR,
                reference=ExternalRecordRef(
                    event_time=datetime.combine(day, time(0), tzinfo=IST),
                    knowledge_time=datetime(2025, 12, 1, 12, 0, tzinfo=IST),
                    source="TEST",
                    content_hash=f"{day.toordinal():064x}",
                    source_snapshot_id=SOURCE_ID,
                ),
                session_windows=(
                    SessionWindow(
                        opens_at=datetime.combine(day, time(9, 15), tzinfo=IST),
                        closes_at=datetime.combine(day, time(15, 30), tzinfo=IST),
                        phase=SessionWindowPhase.LIVE_CONTINUOUS,
                    ),
                ),
            )
        )
    return CalendarSnapshot.create(
        exchange="NSE",
        segment="CM",
        cutoff=datetime(2026, 3, 1, 16, 30, tzinfo=IST),
        coverage_start=days[0].day,
        coverage_end=days[-1].day,
        days=tuple(days),
        source_snapshot_ids=(SOURCE_ID,),
        readiness=ReferenceReadiness.SYNTHETIC_TEST,
    )


def bars() -> tuple[AsOfSwingBar, ...]:
    values = []
    for offset in range(60):
        session = START + timedelta(days=offset)
        close = D("100") + D("0.50") * offset
        volume = 1000 + offset * 10
        values.append(
            AsOfSwingBar(
                market_session=session,
                open=close - D("0.20"),
                high=close + D("1.00"),
                low=close - D("1.00"),
                close=close,
                volume=Decimal(volume),
                traded_value=close * volume,
                available_at=datetime.combine(session, time(16, 0), tzinfo=IST),
                evidence_id=f"bar-{offset:03d}",
                content_hash=f"hash-bar-{offset:03d}",
            )
        )
    return tuple(values)


def history(*, history_bars: tuple[AsOfSwingBar, ...] | None = None) -> InstrumentSwingHistory:
    return InstrumentSwingHistory(
        instrument_id="instrument-test",
        listing_id="listing-test",
        tick_size=D("0.05"),
        tick_available_at=datetime(2026, 1, 1, 10, 0, tzinfo=IST),
        tick_evidence_id="tick-test",
        tick_content_hash="hash-tick-test",
        adjustment_available_at=datetime(2026, 1, 1, 10, 1, tzinfo=IST),
        adjustment_evidence_id="adjustment-test",
        adjustment_content_hash="hash-adjustment-test",
        price_basis="CORPORATE_ACTION_ADJUSTED_AS_OF_CUTOFF",
        bars=history_bars or bars(),
    )


def evidence_for(value: InstrumentSwingHistory) -> tuple[EvidenceItem, ...]:
    result = [
        EvidenceItem(
            evidence_id=bar.evidence_id,
            source="TEST_BAR",
            published_at=bar.available_at - timedelta(minutes=1),
            available_at=bar.available_at,
            content_hash=bar.content_hash,
        )
        for bar in value.bars
    ]
    result.append(
        EvidenceItem(
            evidence_id=value.tick_evidence_id,
            source="TEST_TICK",
            published_at=value.tick_available_at - timedelta(minutes=1),
            available_at=value.tick_available_at,
            content_hash=value.tick_content_hash,
        )
    )
    result.append(
        EvidenceItem(
            evidence_id=value.adjustment_evidence_id,
            source="TEST_CORPORATE_ACTION_ADJUSTMENT",
            published_at=value.adjustment_available_at - timedelta(minutes=1),
            available_at=value.adjustment_available_at,
            content_hash=value.adjustment_content_hash,
        )
    )
    return tuple(result)


def snapshot(
    value: InstrumentSwingHistory,
    *,
    evidence: tuple[EvidenceItem, ...] | None = None,
) -> DataSnapshot:
    current_calendar = calendar()
    return DataSnapshot(
        snapshot_id="snapshot-test",
        decision_time=DECISION_TIME,
        market_session=SIGNAL_SESSION,
        evidence=evidence if evidence is not None else evidence_for(value),
        session_finalized_at=datetime(2026, 3, 1, 16, 30, tzinfo=IST),
        universe_snapshot_id="universe-test",
        calendar_version=current_calendar.version,
        trial_id="trial-test",
        model_bundle_id="models-test",
        data_content_hash="data-test",
        source_revision="source-test",
        execution_policy_version="execution-test",
        cost_schedule_version="cost-test",
    )


def instrument(value: InstrumentSwingHistory, current_snapshot: DataSnapshot) -> InstrumentSnapshot:
    return InstrumentSnapshot(
        instrument_id=value.instrument_id,
        listing_id=value.listing_id,
        universe_snapshot_id=current_snapshot.universe_snapshot_id,
        exchange="NSE",
        segment="CM",
        symbol="TEST",
        board=Board.MAIN,
        market_cap_bucket=MarketCapBucket.SMALL,
        active=True,
        suspended=False,
        surveillance=Surveillance.NONE,
        last_price=value.bars[-1].close,
        median_daily_traded_value=D("20000000"),
        quoted_spread_bps=D("40"),
        lower_circuit_locked=False,
        history_sessions=len(value.bars),
        price_session=current_snapshot.market_session,
        data_available_at=value.bars[-1].available_at,
    )


def forecast(value: InstrumentSnapshot, current_snapshot: DataSnapshot) -> ForecastSummary:
    return ForecastSummary(
        symbol=value.symbol,
        as_of=current_snapshot.decision_time,
        horizon_sessions=8,
        median_return_pct=D("4"),
        downside_return_pct=D("-2"),
        uncertainty=D("0.30"),
        sample_count=100,
        model_version="test-forecast/v1",
        instrument_id=value.instrument_id,
        listing_id=value.listing_id,
        universe_snapshot_id=value.universe_snapshot_id,
        data_snapshot_id=current_snapshot.snapshot_id,
        data_snapshot_fingerprint=current_snapshot.content_fingerprint,
        instrument_fingerprint=value.content_fingerprint,
    )


def validated_calibration(config_id: str):
    trial_id = "e" * 64
    registered_at = datetime(2025, 1, 1, 0, 0, tzinfo=UTC)
    calibration_cutoff = datetime(2026, 2, 28, 12, 0, tzinfo=UTC)
    calibration_plan = WalkForwardCalibrationPlan(
        registered_at=registered_at,
        signal_config_id=config_id,
        source_trial_ids=(trial_id,),
    )
    values = tuple(
        CalibrationObservation(
            signal_config_id=config_id,
            source_trial_id=trial_id,
            source_result_id="f" * 64,
            source_completion_event_id="9" * 64,
            source_trade_id=f"{3000 + index:064x}",
            signal_id=f"{4000 + index:064x}",
            signal_session=date(2026, 1, 2),
            resolved_session=date(2026, 1, 10),
            known_at=datetime(2026, 1, 11, 0, 0, tzinfo=UTC)
            + timedelta(minutes=index),
            partition=CalibrationPartition.TEST,
            outcome=(
                CalibrationOutcome.TARGET
                if index < 60
                else CalibrationOutcome.STOP
                if index < 90
                else CalibrationOutcome.TIME
            ),
            realized_time_exit_r=D("0.10") if index >= 90 else None,
        )
        for index in range(100)
    )
    return build_walk_forward_calibration(
        plan=calibration_plan,
        observations=values,
        cutoff=calibration_cutoff,
    )


class DeterministicSwingSignalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.calendar = calendar()
        self.history = history()
        self.snapshot = snapshot(self.history)
        self.instrument = instrument(self.history, self.snapshot)
        self.forecast = forecast(self.instrument, self.snapshot)

    def provider(
        self,
        *,
        history_value: InstrumentSwingHistory | None = None,
        snapshot_value: DataSnapshot | None = None,
    ) -> DeterministicSwingSignalProvider:
        history_value = history_value or self.history
        snapshot_value = snapshot_value or self.snapshot
        return DeterministicSwingSignalProvider(
            snapshot=snapshot_value,
            histories=(history_value,),
            calendar=self.calendar,
        )

    def test_builds_explainable_next_session_tick_aligned_setup(self) -> None:
        signals, setup, evidence_ids = self.provider().generate(
            self.instrument,
            self.forecast,
            self.snapshot,
        )

        self.assertGreater(signals.relative_strength, D("0"))
        self.assertGreater(signals.trend_quality, D("0"))
        self.assertGreater(signals.volume_confirmation, D("0"))
        self.assertEqual(signals.news_score, D("0"))
        self.assertEqual(signals.estimated_cost_bps, D("40"))
        self.assertEqual(setup.earliest_entry_at, datetime(2026, 3, 2, 9, 20, tzinfo=IST))
        self.assertEqual(setup.entry_expires_at, datetime(2026, 3, 2, 15, 15, tzinfo=IST))
        for value in (setup.entry_low, setup.entry_high, setup.stop, setup.target):
            self.assertEqual(value % self.history.tick_size, D("0"))
        self.assertLess(setup.stop, setup.entry_low)
        self.assertLess(setup.entry_high, setup.target)
        self.assertIn("20-session close return", setup.setup_reason)
        self.assertIn("ATR", setup.stop_reason)
        self.assertIn("net reward/risk", setup.target_reason)
        self.assertEqual(evidence_ids[-2], self.history.tick_evidence_id)
        self.assertEqual(evidence_ids[-1], self.history.adjustment_evidence_id)
        self.assertEqual(len(evidence_ids), 52)

    def test_target_preserves_configured_net_reward_risk_after_cost(self) -> None:
        signals, setup, _ = self.provider().generate(
            self.instrument,
            self.forecast,
            self.snapshot,
        )
        cost = setup.entry_high * signals.estimated_cost_bps / D("10000")
        net_loss = setup.entry_high - setup.stop + cost
        net_reward = setup.target - setup.entry_high - cost

        self.assertGreaterEqual(net_reward / net_loss, D("2.5"))

    def test_probabilities_remain_provisional_until_calibrated(self) -> None:
        _, setup, _ = self.provider().generate(
            self.instrument,
            self.forecast,
            self.snapshot,
        )

        self.assertIs(setup.probability_status, ProbabilityStatus.PROVISIONAL)
        self.assertEqual(setup.calibration_sample_size, 0)
        self.assertEqual(setup.target_probability, D("0"))
        self.assertEqual(setup.stop_probability, D("0"))
        self.assertIn("not walk-forward calibrated", setup.cancel_conditions[-1])

    def test_bound_walk_forward_calibration_upgrades_probability_status(self) -> None:
        config = DeterministicSwingSignalConfig()
        calibration = validated_calibration(config.config_id)
        calibration_item = EvidenceItem(
            evidence_id=calibration.calibration_id,
            source="TEST_CALIBRATION",
            published_at=calibration.cutoff,
            available_at=calibration.cutoff,
            content_hash=calibration.calibration_id,
        )
        calibrated_snapshot = snapshot(
            self.history,
            evidence=evidence_for(self.history) + (calibration_item,),
        )
        calibrated_instrument = instrument(self.history, calibrated_snapshot)
        calibrated_forecast = forecast(calibrated_instrument, calibrated_snapshot)
        provider = DeterministicSwingSignalProvider(
            snapshot=calibrated_snapshot,
            histories=(self.history,),
            calendar=self.calendar,
            config=config,
            calibration=calibration,
        )

        _, setup, evidence_ids = provider.generate(
            calibrated_instrument,
            calibrated_forecast,
            calibrated_snapshot,
        )

        self.assertIs(setup.probability_status, ProbabilityStatus.VALIDATED)
        self.assertEqual(setup.calibration_sample_size, 100)
        self.assertEqual(setup.target_probability, calibration.target_probability)
        self.assertEqual(setup.stop_probability, calibration.stop_probability)
        self.assertEqual(setup.expected_time_exit_r, D("0.10"))
        self.assertEqual(evidence_ids[-1], calibration.calibration_id)
        self.assertNotIn("not walk-forward calibrated", setup.cancel_conditions)

    def test_rejects_calibration_absent_from_decision_snapshot(self) -> None:
        config = DeterministicSwingSignalConfig()
        calibration = validated_calibration(config.config_id)

        with self.assertRaisesRegex(
            DeterministicSwingSignalError,
            "calibration evidence binding differs",
        ):
            DeterministicSwingSignalProvider(
                snapshot=self.snapshot,
                histories=(self.history,),
                calendar=self.calendar,
                config=config,
                calibration=calibration,
            )

    def test_same_bound_inputs_are_deterministic(self) -> None:
        provider = self.provider()
        first = provider.generate(self.instrument, self.forecast, self.snapshot)
        second = provider.generate(self.instrument, self.forecast, self.snapshot)
        self.assertEqual(first, second)

    def test_rejects_future_known_bar_even_if_snapshot_lists_it(self) -> None:
        changed_bar = replace(
            self.history.bars[-1],
            available_at=DECISION_TIME + timedelta(minutes=1),
        )
        changed_history = history(history_bars=self.history.bars[:-1] + (changed_bar,))
        changed_snapshot = snapshot(changed_history)

        with self.assertRaisesRegex(
            DeterministicSwingSignalError,
            "future-known evidence",
        ):
            self.provider(
                history_value=changed_history,
                snapshot_value=changed_snapshot,
            )

    def test_rejects_bar_content_not_bound_to_snapshot_evidence(self) -> None:
        changed_evidence = list(evidence_for(self.history))
        changed_evidence[-3] = replace(changed_evidence[-3], content_hash="wrong-hash")
        changed_snapshot = snapshot(self.history, evidence=tuple(changed_evidence))

        with self.assertRaisesRegex(
            DeterministicSwingSignalError,
            "history evidence binding differs",
        ):
            self.provider(snapshot_value=changed_snapshot)

    def test_rejects_future_known_tick_size(self) -> None:
        changed_history = replace(
            self.history,
            tick_available_at=DECISION_TIME + timedelta(minutes=1),
        )
        changed_snapshot = snapshot(changed_history)

        with self.assertRaisesRegex(DeterministicSwingSignalError, "tick size is future-known"):
            self.provider(
                history_value=changed_history,
                snapshot_value=changed_snapshot,
            )

    def test_rejects_raw_unadjusted_price_history(self) -> None:
        with self.assertRaisesRegex(
            DeterministicSwingSignalError,
            "corporate-action-adjusted prices",
        ):
            replace(self.history, price_basis="RAW_UNADJUSTED")

    def test_rejects_future_known_corporate_action_adjustment(self) -> None:
        changed_history = replace(
            self.history,
            adjustment_available_at=DECISION_TIME + timedelta(minutes=1),
        )
        changed_snapshot = snapshot(changed_history)

        with self.assertRaisesRegex(
            DeterministicSwingSignalError,
            "adjustment is future-known",
        ):
            self.provider(
                history_value=changed_history,
                snapshot_value=changed_snapshot,
            )

    def test_rejects_history_mutated_after_provider_construction(self) -> None:
        provider = self.provider()
        object.__setattr__(self.history.bars[-1], "close", D("999"))

        with self.assertRaisesRegex(DeterministicSwingSignalError, "bar content identity failed"):
            provider.generate(self.instrument, self.forecast, self.snapshot)

    def test_rejects_a_different_runtime_snapshot(self) -> None:
        provider = self.provider()
        other = replace(self.snapshot, source_revision="other-source")

        with self.assertRaisesRegex(DeterministicSwingSignalError, "another snapshot"):
            provider.generate(self.instrument, self.forecast, other)

    def test_rejects_history_below_the_configured_minimum(self) -> None:
        short = history(history_bars=self.history.bars[-59:])
        short_snapshot = snapshot(short)

        with self.assertRaisesRegex(DeterministicSwingSignalError, "shorter"):
            DeterministicSwingSignalProvider(
                snapshot=short_snapshot,
                histories=(short,),
                calendar=self.calendar,
            )

    def test_rejects_instrument_price_that_differs_from_signal_close(self) -> None:
        changed = replace(self.instrument, last_price=self.instrument.last_price + D("0.05"))

        with self.assertRaisesRegex(DeterministicSwingSignalError, "price differs"):
            self.provider().generate(changed, forecast(changed, self.snapshot), self.snapshot)

    def test_config_rejects_lookback_longer_than_minimum_history(self) -> None:
        with self.assertRaisesRegex(DeterministicSwingSignalError, "minimum history"):
            DeterministicSwingSignalConfig(
                minimum_history_sessions=20,
                trend_lookback_sessions=50,
            )

    def test_identity_material_reflects_bound_inputs(self) -> None:
        provider = self.provider()

        material = provider.identity_material()

        self.assertEqual(material["version"], DeterministicSwingSignalProvider.version)
        self.assertEqual(material["snapshot_fingerprint"], self.snapshot.content_fingerprint)
        self.assertEqual(material["calendar_snapshot_id"], self.calendar.snapshot_id)
        self.assertIsNone(material["calibration_id"])
        self.assertEqual(material["history_ids"], (self.history.history_id,))


class SwingTechnicalMetricsAndLevelsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.calendar = calendar()
        self.history = history()
        self.config = DeterministicSwingSignalConfig()

    def test_metrics_are_deterministic_content_addressed_and_replayable(self) -> None:
        first = calculate_swing_technical_metrics(self.history, self.config)
        second = calculate_swing_technical_metrics(self.history, self.config)

        self.assertEqual(first, second)
        self.assertEqual(first.metrics_id, second.metrics_id)
        first.verify_content_identity()
        first.verify_content_identity()

    def test_metrics_reject_wrong_types(self) -> None:
        with self.assertRaisesRegex(DeterministicSwingSignalError, "history must be exact"):
            calculate_swing_technical_metrics(object(), self.config)
        with self.assertRaisesRegex(DeterministicSwingSignalError, "config must be exact"):
            calculate_swing_technical_metrics(self.history, object())

    def test_metrics_enforce_the_configured_minimum_history(self) -> None:
        short = history(history_bars=self.history.bars[-59:])

        with self.assertRaisesRegex(DeterministicSwingSignalError, "shorter"):
            calculate_swing_technical_metrics(short, self.config)

    def test_metrics_detect_post_construction_mutation(self) -> None:
        metrics = calculate_swing_technical_metrics(self.history, self.config)
        original_id = metrics.metrics_id
        object.__setattr__(metrics, "atr", metrics.atr + Decimal("1"))

        self.assertEqual(metrics.metrics_id, original_id)
        with self.assertRaisesRegex(DeterministicSwingSignalError, "content identity"):
            metrics.verify_content_identity()

    def test_levels_are_deterministic_content_addressed_and_tick_aligned(self) -> None:
        metrics = calculate_swing_technical_metrics(self.history, self.config)
        current_close = self.history.bars[-1].close
        tick = self.history.tick_size

        first = calculate_swing_trade_levels(
            current_close=current_close,
            tick=tick,
            atr=metrics.atr,
            estimated_cost_bps=Decimal("40"),
            config=self.config,
        )
        second = calculate_swing_trade_levels(
            current_close=current_close,
            tick=tick,
            atr=metrics.atr,
            estimated_cost_bps=Decimal("40"),
            config=self.config,
        )

        self.assertEqual(first.levels_id, second.levels_id)
        for value in (first.entry_low, first.entry_high, first.stop, first.target):
            self.assertEqual(value % tick, Decimal("0"))
        self.assertLess(first.stop, first.entry_low)
        self.assertLess(first.entry_high, first.target)
        first.verify_content_identity()

    def test_levels_preserve_configured_net_reward_risk_after_cost(self) -> None:
        metrics = calculate_swing_technical_metrics(self.history, self.config)
        levels = calculate_swing_trade_levels(
            current_close=self.history.bars[-1].close,
            tick=self.history.tick_size,
            atr=metrics.atr,
            estimated_cost_bps=self.config.base_round_trip_cost_bps,
            config=self.config,
        )

        self.assertGreaterEqual(levels.net_reward_risk, self.config.target_net_reward_risk)

    def test_levels_reject_a_non_tick_aligned_close(self) -> None:
        metrics = calculate_swing_technical_metrics(self.history, self.config)

        with self.assertRaisesRegex(DeterministicSwingSignalError, "tick-aligned"):
            calculate_swing_trade_levels(
                current_close=self.history.bars[-1].close + Decimal("0.001"),
                tick=self.history.tick_size,
                atr=metrics.atr,
                estimated_cost_bps=Decimal("40"),
                config=self.config,
            )

    def test_levels_reject_a_negative_estimated_cost(self) -> None:
        metrics = calculate_swing_technical_metrics(self.history, self.config)

        with self.assertRaisesRegex(DeterministicSwingSignalError, "cannot be negative"):
            calculate_swing_trade_levels(
                current_close=self.history.bars[-1].close,
                tick=self.history.tick_size,
                atr=metrics.atr,
                estimated_cost_bps=Decimal("-1"),
                config=self.config,
            )

    def test_levels_detect_post_construction_mutation(self) -> None:
        metrics = calculate_swing_technical_metrics(self.history, self.config)
        levels = calculate_swing_trade_levels(
            current_close=self.history.bars[-1].close,
            tick=self.history.tick_size,
            atr=metrics.atr,
            estimated_cost_bps=Decimal("40"),
            config=self.config,
        )
        original_id = levels.levels_id
        object.__setattr__(levels, "target", levels.target + levels.entry_high)

        self.assertEqual(levels.levels_id, original_id)
        with self.assertRaisesRegex(DeterministicSwingSignalError, "content identity"):
            levels.verify_content_identity()

    def test_entry_window_is_deterministic_and_matches_provider_output(self) -> None:
        first = calculate_next_entry_window(self.calendar, SIGNAL_SESSION, self.config)
        second = calculate_next_entry_window(self.calendar, SIGNAL_SESSION, self.config)

        self.assertEqual(first.window_id, second.window_id)
        self.assertEqual(first.earliest_entry_at, datetime(2026, 3, 2, 9, 20, tzinfo=IST))
        self.assertEqual(first.entry_expires_at, datetime(2026, 3, 2, 15, 15, tzinfo=IST))
        self.assertEqual(first.entry_day, date(2026, 3, 2))
        first.verify_content_identity()

    def test_entry_window_detects_post_construction_mutation(self) -> None:
        window = calculate_next_entry_window(self.calendar, SIGNAL_SESSION, self.config)
        original_id = window.window_id
        object.__setattr__(
            window,
            "holding_boundary_day",
            window.holding_boundary_day + timedelta(days=1),
        )

        self.assertEqual(window.window_id, original_id)
        with self.assertRaisesRegex(DeterministicSwingSignalError, "content identity"):
            window.verify_content_identity()


if __name__ == "__main__":
    unittest.main()
