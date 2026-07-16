from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import date, timedelta

from india_swing.evaluation import (
    EvaluationPlanError,
    EvaluationPlanIntegrityError,
    PurgedWalkForwardPlan,
    SplitMethod,
    WalkForwardFold,
    build_expanding_purged_walk_forward_plan,
)


def sessions(count: int) -> tuple[date, ...]:
    start = date(2025, 1, 1)
    return tuple(start + timedelta(days=index) for index in range(count))


class PurgedWalkForwardTests(unittest.TestCase):
    def plan(self) -> PurgedWalkForwardPlan:
        return build_expanding_purged_walk_forward_plan(
            calendar_version="synthetic-calendar-v1",
            ordered_sessions=sessions(80),
            initial_training_sessions=20,
            validation_sessions=5,
            test_sessions=5,
            step_sessions=5,
        )

    def test_training_labels_do_not_overlap_validation_window(self) -> None:
        calendar = sessions(50)
        fold = WalkForwardFold(
            training_sessions=calendar[:20],
            validation_sessions=calendar[25:30],
            test_sessions=calendar[40:45],
        )

        with self.assertRaisesRegex(EvaluationPlanError, "training labels overlap"):
            PurgedWalkForwardPlan(
                calendar_version="synthetic-calendar-v1",
                ordered_sessions=calendar,
                label_horizon_sessions=10,
                embargo_sessions=10,
                folds=(fold,),
            )

    def test_ten_session_embargo_is_enforced(self) -> None:
        plan = self.plan()
        with self.assertRaisesRegex(EvaluationPlanError, "embargo"):
            replace(plan, embargo_sessions=9)

    def test_random_cross_validation_is_rejected_for_time_series_trial(self) -> None:
        plan = self.plan()
        with self.assertRaisesRegex(EvaluationPlanError, "purged walk-forward"):
            replace(plan, split_method="RANDOM_K_FOLD")

    def test_partitions_count_explicit_sessions_not_calendar_days(self) -> None:
        calendar = tuple(
            value
            for value in sessions(120)
            if value.weekday() < 5 and value != date(2025, 1, 27)
        )
        plan = build_expanding_purged_walk_forward_plan(
            calendar_version="holiday-aware-calendar-v1",
            ordered_sessions=calendar,
            initial_training_sessions=20,
            validation_sessions=5,
            test_sessions=5,
            step_sessions=5,
        )
        first = plan.folds[0]

        self.assertEqual(len(first.training_sessions), 20)
        self.assertEqual(len(first.validation_sessions), 5)
        self.assertEqual(len(first.test_sessions), 5)
        self.assertEqual(
            calendar.index(first.validation_sessions[0])
            - calendar.index(first.training_sessions[-1])
            - 1,
            10,
        )

    def test_test_sessions_never_repeat_across_folds(self) -> None:
        plan = self.plan()
        observed = [value for fold in plan.folds for value in fold.test_sessions]
        self.assertEqual(len(observed), len(set(observed)))

    def test_insufficient_calendar_coverage_fails_closed(self) -> None:
        with self.assertRaisesRegex(EvaluationPlanError, "insufficient"):
            build_expanding_purged_walk_forward_plan(
                calendar_version="short-calendar-v1",
                ordered_sessions=sessions(49),
                initial_training_sessions=20,
                validation_sessions=5,
                test_sessions=5,
                step_sessions=5,
            )

    def test_nested_fold_mutation_invalidates_plan_identity(self) -> None:
        plan = self.plan()
        object.__setattr__(plan.folds[0], "training_sessions", sessions(19))

        with self.assertRaisesRegex(EvaluationPlanIntegrityError, "fold"):
            plan.verify_content_identity()

    def test_equivalent_plan_builds_are_content_identical(self) -> None:
        first = self.plan()
        second = self.plan()
        self.assertEqual(first, second)
        self.assertEqual(first.plan_id, second.plan_id)
        self.assertIs(first.split_method, SplitMethod.PURGED_WALK_FORWARD)


if __name__ == "__main__":
    unittest.main()
