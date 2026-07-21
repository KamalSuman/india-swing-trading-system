from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from india_swing.signals.calibration import (
    CalibrationError,
    CalibrationObservation,
    CalibrationOutcome,
    CalibrationPartition,
    WalkForwardCalibrationPlan,
    build_walk_forward_calibration,
    observations_from_evaluation_comparison,
)
from india_swing.evaluation.lifecycle import (
    TrialLifecycleEvent,
    TrialLifecycleEventType,
)
from tests.test_deterministic_baselines import (
    evaluate_run,
    split_plan,
    strategy_config,
)


UTC = timezone.utc
CONFIG_ID = "a" * 64
TRIAL_ID = "b" * 64
REGISTERED_AT = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
CUTOFF = datetime(2026, 1, 12, 0, 0, tzinfo=UTC)


def D(value: str) -> Decimal:
    return Decimal(value)


def plan(
    *,
    registered_at: datetime = REGISTERED_AT,
    source_trial_ids: tuple[str, ...] = (TRIAL_ID,),
) -> WalkForwardCalibrationPlan:
    return WalkForwardCalibrationPlan(
        registered_at=registered_at,
        signal_config_id=CONFIG_ID,
        source_trial_ids=source_trial_ids,
        minimum_sample_size=100,
        adverse_stop_prior_trades=10,
    )


def observations(count: int = 100) -> tuple[CalibrationObservation, ...]:
    values = []
    for index in range(count):
        if index < 60:
            outcome = CalibrationOutcome.TARGET
            time_r = None
        elif index < 90:
            outcome = CalibrationOutcome.STOP
            time_r = None
        else:
            outcome = CalibrationOutcome.TIME
            time_r = D("0.20")
        values.append(
            CalibrationObservation(
                signal_config_id=CONFIG_ID,
                source_trial_id=TRIAL_ID,
                source_result_id="c" * 64,
                source_completion_event_id="d" * 64,
                source_trade_id=f"{1000 + index:064x}",
                signal_id=f"{2000 + index:064x}",
                signal_session=date(2026, 1, 2),
                resolved_session=date(2026, 1, 3),
                known_at=datetime(2026, 1, 4, 0, 0, tzinfo=UTC)
                + timedelta(minutes=index),
                partition=CalibrationPartition.TEST,
                outcome=outcome,
                realized_time_exit_r=time_r,
            )
        )
    return tuple(values)


class SignalCalibrationTests(unittest.TestCase):
    @staticmethod
    def completed_baseline():
        run = evaluate_run()
        comparison = run.comparison
        completion = TrialLifecycleEvent(
            trial_id=comparison.trial_id,
            sequence=1,
            previous_event_id=None,
            event_type=TrialLifecycleEventType.TRIAL_COMPLETED,
            occurred_at=datetime(2026, 10, 1, 0, 0, tzinfo=UTC),
            actor_id="test-calibration",
            reason="Bind the completed comparison for calibration extraction.",
            metrics=tuple(
                sorted(
                    comparison.strategy_base.metrics
                    + comparison.comparison_metrics
                )
            ),
            passed=comparison.passed,
            evaluation_result_id=comparison.comparison_id,
        )
        return run, completion

    def test_extracts_only_engine_proven_test_partition_outcomes(self) -> None:
        run, completion = self.completed_baseline()
        extracted = observations_from_evaluation_comparison(
            signal_config_id=strategy_config().strategy_id,
            comparison=run.comparison,
            strategy_batch=run.strategy_batch,
            split_plan=split_plan(),
            completion=completion,
        )

        self.assertEqual(len(extracted), len(run.comparison.strategy_base.trades))
        self.assertTrue(extracted)
        self.assertTrue(
            all(value.partition is CalibrationPartition.TEST for value in extracted)
        )
        self.assertEqual(
            {value.source_result_id for value in extracted},
            {run.comparison.comparison_id},
        )

    def test_extractor_rejects_completion_for_another_result(self) -> None:
        run, completion = self.completed_baseline()
        wrong = replace(completion, evaluation_result_id="9" * 64)

        with self.assertRaisesRegex(CalibrationError, "does not bind"):
            observations_from_evaluation_comparison(
                signal_config_id=strategy_config().strategy_id,
                comparison=run.comparison,
                strategy_batch=run.strategy_batch,
                split_plan=split_plan(),
                completion=wrong,
            )

    def test_builds_conservative_test_only_calibration(self) -> None:
        result = build_walk_forward_calibration(
            plan=plan(),
            observations=observations(),
            cutoff=CUTOFF,
        )

        self.assertEqual(result.sample_size, 100)
        self.assertEqual(result.target_count, 60)
        self.assertEqual(result.stop_count, 30)
        self.assertEqual(result.time_count, 10)
        self.assertEqual(result.target_probability, D("60") / D("110"))
        self.assertEqual(result.stop_probability, D("40") / D("110"))
        self.assertLess(
            abs(
                D("1")
                - result.target_probability
                - result.stop_probability
                - D("10") / D("110")
            ),
            D("0.000000000000000000000000001"),
        )
        self.assertEqual(result.expected_time_exit_r, D("0.20"))
        result.verify_content_identity()

    def test_rejects_sample_below_preregistered_minimum(self) -> None:
        with self.assertRaisesRegex(CalibrationError, "smaller"):
            build_walk_forward_calibration(
                plan=plan(),
                observations=observations(99),
                cutoff=CUTOFF,
            )

    def test_rejects_training_or_validation_outcomes(self) -> None:
        for partition in (CalibrationPartition.TRAIN, CalibrationPartition.VALIDATION):
            changed = (replace(observations()[0], partition=partition),) + observations()[1:]
            with self.subTest(partition=partition):
                with self.assertRaisesRegex(CalibrationError, "test-partition"):
                    build_walk_forward_calibration(
                        plan=plan(),
                        observations=changed,
                        cutoff=CUTOFF,
                    )

    def test_rejects_outcome_known_after_calibration_cutoff(self) -> None:
        values = observations()
        changed = values[:-1] + (
            replace(values[-1], known_at=CUTOFF + timedelta(seconds=1)),
        )

        with self.assertRaisesRegex(CalibrationError, "future-known"):
            build_walk_forward_calibration(
                plan=plan(),
                observations=changed,
                cutoff=CUTOFF,
            )

    def test_rejects_plan_registered_after_outcome_was_known(self) -> None:
        late_plan = plan(registered_at=datetime(2026, 1, 5, 0, 0, tzinfo=UTC))

        with self.assertRaisesRegex(CalibrationError, "after an outcome"):
            build_walk_forward_calibration(
                plan=late_plan,
                observations=observations(),
                cutoff=CUTOFF,
            )

    def test_rejects_missing_preregistered_source_trial(self) -> None:
        missing_trial = "d" * 64
        two_trial_plan = plan(source_trial_ids=tuple(sorted((TRIAL_ID, missing_trial))))

        with self.assertRaisesRegex(CalibrationError, "every preregistered"):
            build_walk_forward_calibration(
                plan=two_trial_plan,
                observations=observations(),
                cutoff=CUTOFF,
            )

    def test_rejects_duplicate_trade_or_signal(self) -> None:
        values = observations()
        duplicate_trade = values[:-1] + (
            replace(values[-1], source_trade_id=values[0].source_trade_id),
        )
        with self.assertRaisesRegex(CalibrationError, "trade cannot be counted twice"):
            build_walk_forward_calibration(
                plan=plan(),
                observations=duplicate_trade,
                cutoff=CUTOFF,
            )

        duplicate_signal = values[:-1] + (
            replace(values[-1], signal_id=values[0].signal_id),
        )
        with self.assertRaisesRegex(CalibrationError, "signal cannot be counted twice"):
            build_walk_forward_calibration(
                plan=plan(),
                observations=duplicate_signal,
                cutoff=CUTOFF,
            )

    def test_rejects_multiple_results_selected_from_one_trial(self) -> None:
        values = observations()
        changed = values[:-1] + (
            replace(
                values[-1],
                source_result_id="e" * 64,
                source_completion_event_id="f" * 64,
            ),
        )

        with self.assertRaisesRegex(CalibrationError, "one completed evaluation"):
            build_walk_forward_calibration(
                plan=plan(),
                observations=changed,
                cutoff=CUTOFF,
            )

    def test_rejects_outcome_claimed_known_before_resolution(self) -> None:
        with self.assertRaisesRegex(CalibrationError, "before its resolved session"):
            replace(
                observations()[0],
                resolved_session=date(2026, 1, 6),
                known_at=datetime(2026, 1, 5, 0, 0, tzinfo=UTC),
            )

    def test_detects_calibration_mutation(self) -> None:
        result = build_walk_forward_calibration(
            plan=plan(),
            observations=observations(),
            cutoff=CUTOFF,
        )
        object.__setattr__(result, "target_probability", D("0.99"))

        with self.assertRaisesRegex(CalibrationError, "content identity"):
            result.verify_content_identity()

    def test_plan_cannot_lower_minimum_below_100(self) -> None:
        with self.assertRaisesRegex(CalibrationError, "below 100"):
            WalkForwardCalibrationPlan(
                registered_at=REGISTERED_AT,
                signal_config_id=CONFIG_ID,
                source_trial_ids=(TRIAL_ID,),
                minimum_sample_size=99,
            )


if __name__ == "__main__":
    unittest.main()
