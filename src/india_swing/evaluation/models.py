from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum

from india_swing.identity import content_id


EVALUATION_SPLIT_SCHEMA_VERSION = "purged-walk-forward-plan/v1"
MINIMUM_SWING_LABEL_HORIZON_SESSIONS = 10


class EvaluationPlanError(ValueError):
    pass


class EvaluationPlanIntegrityError(EvaluationPlanError):
    pass


class SplitMethod(str, Enum):
    PURGED_WALK_FORWARD = "PURGED_WALK_FORWARD"


def _session_tuple(values: tuple[date, ...], name: str) -> None:
    if (
        type(values) is not tuple
        or not values
        or any(type(value) is not date for value in values)
        or values != tuple(sorted(set(values)))
    ):
        raise EvaluationPlanError(f"{name} must be sorted, unique trading sessions")


@dataclass(frozen=True, slots=True)
class WalkForwardFold:
    training_sessions: tuple[date, ...]
    validation_sessions: tuple[date, ...]
    test_sessions: tuple[date, ...]
    fold_id: str = field(init=False)

    def __post_init__(self) -> None:
        for values, name in (
            (self.training_sessions, "training_sessions"),
            (self.validation_sessions, "validation_sessions"),
            (self.test_sessions, "test_sessions"),
        ):
            _session_tuple(values, name)
        if not (
            self.training_sessions[-1] < self.validation_sessions[0]
            and self.validation_sessions[-1] < self.test_sessions[0]
        ):
            raise EvaluationPlanError(
                "walk-forward partitions must be strictly chronological"
            )
        if (
            set(self.training_sessions) & set(self.validation_sessions)
            or set(self.training_sessions) & set(self.test_sessions)
            or set(self.validation_sessions) & set(self.test_sessions)
        ):
            raise EvaluationPlanError("walk-forward partitions cannot overlap")
        object.__setattr__(self, "fold_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema_version": EVALUATION_SPLIT_SCHEMA_VERSION,
                "training_sessions": self.training_sessions,
                "validation_sessions": self.validation_sessions,
                "test_sessions": self.test_sessions,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.fold_id != self._calculated_id():
            raise EvaluationPlanIntegrityError("walk-forward fold identity failed")


@dataclass(frozen=True, slots=True)
class PurgedWalkForwardPlan:
    calendar_version: str
    ordered_sessions: tuple[date, ...]
    label_horizon_sessions: int
    embargo_sessions: int
    folds: tuple[WalkForwardFold, ...]
    split_method: SplitMethod = SplitMethod.PURGED_WALK_FORWARD
    schema_version: str = EVALUATION_SPLIT_SCHEMA_VERSION
    plan_id: str = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.calendar_version, str) or not self.calendar_version.strip():
            raise EvaluationPlanError("calendar_version is required")
        _session_tuple(self.ordered_sessions, "ordered_sessions")
        if (
            type(self.label_horizon_sessions) is not int
            or self.label_horizon_sessions
            < MINIMUM_SWING_LABEL_HORIZON_SESSIONS
        ):
            raise EvaluationPlanError(
                "swing label horizon must be at least ten trading sessions"
            )
        if (
            type(self.embargo_sessions) is not int
            or self.embargo_sessions < self.label_horizon_sessions
        ):
            raise EvaluationPlanError(
                "embargo must be at least the maximum forward-label horizon"
            )
        if self.split_method is not SplitMethod.PURGED_WALK_FORWARD:
            raise EvaluationPlanError("time-series trials require purged walk-forward splits")
        if self.schema_version != EVALUATION_SPLIT_SCHEMA_VERSION:
            raise EvaluationPlanError("unsupported evaluation split schema")
        if (
            type(self.folds) is not tuple
            or not self.folds
            or any(type(value) is not WalkForwardFold for value in self.folds)
        ):
            raise EvaluationPlanError("folds must be a non-empty immutable exact tuple")

        session_index = {value: index for index, value in enumerate(self.ordered_sessions)}
        seen_fold_ids: set[str] = set()
        seen_test_sessions: set[date] = set()
        previous: WalkForwardFold | None = None
        for fold in self.folds:
            fold.verify_content_identity()
            if fold.fold_id in seen_fold_ids:
                raise EvaluationPlanError("walk-forward folds must be unique")
            seen_fold_ids.add(fold.fold_id)
            all_partition_sessions = (
                fold.training_sessions
                + fold.validation_sessions
                + fold.test_sessions
            )
            if any(value not in session_index for value in all_partition_sessions):
                raise EvaluationPlanError("fold contains a session outside its calendar")
            for values, name in (
                (fold.training_sessions, "training"),
                (fold.validation_sessions, "validation"),
                (fold.test_sessions, "test"),
            ):
                positions = tuple(session_index[value] for value in values)
                if positions != tuple(range(positions[0], positions[-1] + 1)):
                    raise EvaluationPlanError(
                        f"{name} partition must be contiguous in trading-session time"
                    )
            training_end = session_index[fold.training_sessions[-1]]
            validation_start = session_index[fold.validation_sessions[0]]
            validation_end = session_index[fold.validation_sessions[-1]]
            test_start = session_index[fold.test_sessions[0]]
            if validation_start - training_end - 1 < self.embargo_sessions:
                raise EvaluationPlanError(
                    "training labels overlap validation or its required embargo"
                )
            if test_start - validation_end - 1 < self.embargo_sessions:
                raise EvaluationPlanError(
                    "validation labels overlap test or its required embargo"
                )
            if seen_test_sessions & set(fold.test_sessions):
                raise EvaluationPlanError("test sessions cannot repeat across folds")
            seen_test_sessions.update(fold.test_sessions)
            if previous is not None:
                if (
                    not set(previous.training_sessions).issubset(fold.training_sessions)
                    or previous.test_sessions[-1] >= fold.test_sessions[0]
                ):
                    raise EvaluationPlanError(
                        "folds must use expanding training data and advancing tests"
                    )
            previous = fold
        object.__setattr__(self, "plan_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema_version": self.schema_version,
                "calendar_version": self.calendar_version,
                "ordered_sessions": self.ordered_sessions,
                "label_horizon_sessions": self.label_horizon_sessions,
                "embargo_sessions": self.embargo_sessions,
                "folds": self.folds,
                "split_method": self.split_method,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        for fold in self.folds:
            fold.verify_content_identity()
        if self.plan_id != self._calculated_id():
            raise EvaluationPlanIntegrityError("walk-forward plan identity failed")
