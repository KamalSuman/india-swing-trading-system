from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum

from india_swing.identity import content_id


TRIAL_LIFECYCLE_EVENT_SCHEMA_VERSION = "research-trial-lifecycle-event/v2"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class TrialLifecycleError(ValueError):
    pass


class TrialLifecycleIntegrityError(TrialLifecycleError):
    pass


class TrialLifecycleEventType(str, Enum):
    TRIAL_STARTED = "TRIAL_STARTED"
    HOLDOUT_UNSEALED = "HOLDOUT_UNSEALED"
    HOLDOUT_FEATURES_ACCESSED = "HOLDOUT_FEATURES_ACCESSED"
    HOLDOUT_LABELS_ACCESSED = "HOLDOUT_LABELS_ACCESSED"
    HOLDOUT_RESULTS_ACCESSED = "HOLDOUT_RESULTS_ACCESSED"
    TRIAL_COMPLETED = "TRIAL_COMPLETED"
    TRIAL_FAILED = "TRIAL_FAILED"
    TRIAL_ABORTED = "TRIAL_ABORTED"
    TRIAL_INVALIDATED = "TRIAL_INVALIDATED"


HOLDOUT_EVENT_TYPES = frozenset(
    {
        TrialLifecycleEventType.HOLDOUT_UNSEALED,
        TrialLifecycleEventType.HOLDOUT_FEATURES_ACCESSED,
        TrialLifecycleEventType.HOLDOUT_LABELS_ACCESSED,
        TrialLifecycleEventType.HOLDOUT_RESULTS_ACCESSED,
    }
)
HOLDOUT_ACCESS_EVENT_TYPES = HOLDOUT_EVENT_TYPES - {
    TrialLifecycleEventType.HOLDOUT_UNSEALED
}
TERMINAL_OUTCOME_EVENT_TYPES = frozenset(
    {
        TrialLifecycleEventType.TRIAL_COMPLETED,
        TrialLifecycleEventType.TRIAL_FAILED,
        TrialLifecycleEventType.TRIAL_ABORTED,
    }
)


def _sha(value: str, name: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise TrialLifecycleError(f"{name} must be a full lowercase SHA-256")


def _text(value: str, name: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or any(ord(character) < 32 for character in value)
    ):
        raise TrialLifecycleError(f"{name} must be canonical non-empty text")


@dataclass(frozen=True, slots=True)
class TrialLifecycleEvent:
    trial_id: str
    sequence: int
    previous_event_id: str | None
    event_type: TrialLifecycleEventType
    occurred_at: datetime
    actor_id: str
    reason: str
    holdout_id: str | None = None
    metrics: tuple[tuple[str, Decimal], ...] = ()
    passed: bool | None = None
    evaluation_result_id: str | None = None
    schema_version: str = TRIAL_LIFECYCLE_EVENT_SCHEMA_VERSION
    event_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha(self.trial_id, "trial_id")
        if type(self.sequence) is not int or self.sequence <= 0:
            raise TrialLifecycleError("sequence must be a positive integer")
        if self.sequence == 1:
            if self.previous_event_id is not None:
                raise TrialLifecycleError("first event cannot have a predecessor")
        else:
            if self.previous_event_id is None:
                raise TrialLifecycleError("later events require a predecessor")
            _sha(self.previous_event_id, "previous_event_id")
        if type(self.event_type) is not TrialLifecycleEventType:
            raise TrialLifecycleError("event_type must be exact")
        if not isinstance(self.occurred_at, datetime):
            raise TrialLifecycleError("occurred_at must be a datetime")
        if self.occurred_at.tzinfo is None or self.occurred_at.utcoffset() is None:
            raise TrialLifecycleError("occurred_at must be timezone-aware")
        object.__setattr__(
            self,
            "occurred_at",
            self.occurred_at.astimezone(timezone.utc),
        )
        _text(self.actor_id, "actor_id")
        _text(self.reason, "reason")
        if self.event_type in HOLDOUT_EVENT_TYPES:
            if self.holdout_id is None:
                raise TrialLifecycleError("holdout event requires holdout_id")
            _sha(self.holdout_id, "holdout_id")
        elif self.holdout_id is not None:
            raise TrialLifecycleError("non-holdout event cannot carry holdout_id")
        if (
            type(self.metrics) is not tuple
            or tuple(name for name, _ in self.metrics)
            != tuple(sorted({name for name, _ in self.metrics}))
        ):
            raise TrialLifecycleError("metrics must be a sorted unique tuple")
        for name, value in self.metrics:
            _text(name, "metric name")
            if type(value) is not Decimal or not value.is_finite():
                raise TrialLifecycleError("metric values must be finite Decimals")
        if self.event_type is TrialLifecycleEventType.TRIAL_COMPLETED:
            if (
                not self.metrics
                or type(self.passed) is not bool
                or self.evaluation_result_id is None
            ):
                raise TrialLifecycleError(
                    "completed trial requires a generated result, metrics, and pass result"
                )
            _sha(self.evaluation_result_id, "evaluation_result_id")
        elif self.metrics or self.passed is not None or self.evaluation_result_id is not None:
            raise TrialLifecycleError(
                "only a completed trial can carry a generated evaluation result"
            )
        if self.schema_version != TRIAL_LIFECYCLE_EVENT_SCHEMA_VERSION:
            raise TrialLifecycleError("unsupported trial-lifecycle schema")
        object.__setattr__(self, "event_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema_version": self.schema_version,
                "trial_id": self.trial_id,
                "sequence": self.sequence,
                "previous_event_id": self.previous_event_id,
                "event_type": self.event_type,
                "occurred_at": self.occurred_at,
                "actor_id": self.actor_id,
                "reason": self.reason,
                "holdout_id": self.holdout_id,
                "metrics": self.metrics,
                "passed": self.passed,
                "evaluation_result_id": self.evaluation_result_id,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.event_id != self._calculated_id():
            raise TrialLifecycleIntegrityError(
                "trial lifecycle event content identity failed"
            )
