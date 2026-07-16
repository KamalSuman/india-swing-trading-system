from __future__ import annotations

from datetime import date

from .models import (
    MINIMUM_SWING_LABEL_HORIZON_SESSIONS,
    EvaluationPlanError,
    PurgedWalkForwardPlan,
    WalkForwardFold,
)


def build_expanding_purged_walk_forward_plan(
    *,
    calendar_version: str,
    ordered_sessions: tuple[date, ...],
    initial_training_sessions: int,
    validation_sessions: int,
    test_sessions: int,
    step_sessions: int,
    label_horizon_sessions: int = MINIMUM_SWING_LABEL_HORIZON_SESSIONS,
    embargo_sessions: int = MINIMUM_SWING_LABEL_HORIZON_SESSIONS,
) -> PurgedWalkForwardPlan:
    """Build expanding chronological folds using trading-session positions only."""

    for value, name in (
        (initial_training_sessions, "initial_training_sessions"),
        (validation_sessions, "validation_sessions"),
        (test_sessions, "test_sessions"),
        (step_sessions, "step_sessions"),
    ):
        if type(value) is not int or value <= 0:
            raise EvaluationPlanError(f"{name} must be a positive integer")
    if step_sessions < test_sessions:
        raise EvaluationPlanError(
            "step_sessions must cover the test window so holdout sessions do not repeat"
        )
    if type(ordered_sessions) is not tuple:
        raise EvaluationPlanError("ordered_sessions must be an immutable tuple")

    folds: list[WalkForwardFold] = []
    offset = 0
    while True:
        training_end = initial_training_sessions + offset
        validation_start = training_end + embargo_sessions
        validation_end = validation_start + validation_sessions
        test_start = validation_end + embargo_sessions
        test_end = test_start + test_sessions
        if test_end > len(ordered_sessions):
            break
        folds.append(
            WalkForwardFold(
                training_sessions=ordered_sessions[:training_end],
                validation_sessions=ordered_sessions[validation_start:validation_end],
                test_sessions=ordered_sessions[test_start:test_end],
            )
        )
        offset += step_sessions
    if not folds:
        raise EvaluationPlanError(
            "calendar coverage is insufficient for one fully purged fold"
        )
    return PurgedWalkForwardPlan(
        calendar_version=calendar_version,
        ordered_sessions=ordered_sessions,
        label_horizon_sessions=label_horizon_sessions,
        embargo_sessions=embargo_sessions,
        folds=tuple(folds),
    )
