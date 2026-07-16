from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from dataclasses import fields
from datetime import datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from .engine import TrialEvaluationResult

from .lifecycle import (
    HOLDOUT_ACCESS_EVENT_TYPES,
    HOLDOUT_EVENT_TYPES,
    TERMINAL_OUTCOME_EVENT_TYPES,
    TRIAL_LIFECYCLE_EVENT_SCHEMA_VERSION,
    TrialLifecycleError,
    TrialLifecycleEvent,
    TrialLifecycleEventType,
    TrialLifecycleIntegrityError,
)
from .trial_store import LocalTrialRegistry, TrialNotRegistered
from .trials import TrialRegistration, TrialStage


TRIAL_LIFECYCLE_STORE_SCHEMA_VERSION = "local-trial-lifecycle-store/v2"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_EVENT_FILENAME = re.compile(r"([0-9]{20})-([0-9a-f]{64})\.json\Z")
_MAX_EVENT_BYTES = 4 * 1024 * 1024


class TrialLifecycleConflict(TrialLifecycleError):
    pass


def _is_link_like(path: Path) -> bool:
    try:
        status = os.lstat(path)
    except OSError:
        return path.is_symlink()
    reparse_attribute = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return path.is_symlink() or bool(
        getattr(status, "st_file_attributes", 0) & reparse_attribute
    )


def _json_value(value: object) -> object:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    if value is None or isinstance(value, (str, int, bool)):
        return value
    raise TypeError(f"unsupported trial-lifecycle value: {type(value).__name__}")


def encode_trial_lifecycle_event(event: TrialLifecycleEvent) -> bytes:
    if type(event) is not TrialLifecycleEvent:
        raise TypeError("event must be an exact TrialLifecycleEvent")
    event.verify_content_identity()
    value = {
        "store_schema_version": TRIAL_LIFECYCLE_STORE_SCHEMA_VERSION,
        "event": {
            item.name: _json_value(getattr(event, item.name))
            for item in fields(TrialLifecycleEvent)
        },
    }
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise TrialLifecycleIntegrityError(
                "trial lifecycle event contains a duplicate JSON key"
            )
        result[key] = value
    return result


def decode_trial_lifecycle_event(payload: bytes) -> TrialLifecycleEvent:
    try:
        value = json.loads(
            payload.decode("utf-8", errors="strict"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        if (
            type(value) is not dict
            or set(value) != {"store_schema_version", "event"}
            or value["store_schema_version"]
            != TRIAL_LIFECYCLE_STORE_SCHEMA_VERSION
            or type(value["event"]) is not dict
        ):
            raise ValueError
        raw = value["event"]
        if set(raw) != {item.name for item in fields(TrialLifecycleEvent)}:
            raise ValueError
        if type(raw["metrics"]) is not list or any(
            type(item) is not list
            or len(item) != 2
            or type(item[0]) is not str
            or type(item[1]) is not str
            for item in raw["metrics"]
        ):
            raise ValueError
        stored_event_id = raw["event_id"]
        event = TrialLifecycleEvent(
            trial_id=raw["trial_id"],
            sequence=raw["sequence"],
            previous_event_id=raw["previous_event_id"],
            event_type=TrialLifecycleEventType(raw["event_type"]),
            occurred_at=datetime.fromisoformat(raw["occurred_at"]),
            actor_id=raw["actor_id"],
            reason=raw["reason"],
            holdout_id=raw["holdout_id"],
            metrics=tuple(
                (name, Decimal(metric_value)) for name, metric_value in raw["metrics"]
            ),
            passed=raw["passed"],
            evaluation_result_id=raw["evaluation_result_id"],
            schema_version=raw["schema_version"],
        )
        if event.event_id != stored_event_id:
            raise TrialLifecycleIntegrityError(
                "stored lifecycle event ID does not match content"
            )
        event.verify_content_identity()
        return event
    except TrialLifecycleIntegrityError:
        raise
    except (
        InvalidOperation,
        KeyError,
        TypeError,
        ValueError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:
        raise TrialLifecycleIntegrityError(
            "stored trial lifecycle event is invalid"
        ) from exc


def _validate_history(
    registration: TrialRegistration,
    events: tuple[TrialLifecycleEvent, ...],
) -> None:
    if not events:
        return
    expected_metrics = {registration.primary_metric, *registration.secondary_metrics}
    started = False
    unsealed = False
    holdout_results_accessed = False
    terminal = False
    invalidated = False
    previous: TrialLifecycleEvent | None = None
    for expected_sequence, event in enumerate(events, start=1):
        event.verify_content_identity()
        if event.trial_id != registration.trial_id:
            raise TrialLifecycleIntegrityError("event belongs to another trial")
        if event.sequence != expected_sequence:
            raise TrialLifecycleIntegrityError("trial event sequence contains a gap")
        if event.previous_event_id != (
            None if previous is None else previous.event_id
        ):
            raise TrialLifecycleIntegrityError("trial event predecessor chain is broken")
        if previous is not None and event.occurred_at < previous.occurred_at:
            raise TrialLifecycleIntegrityError("trial event time moved backwards")
        if event.occurred_at < registration.registered_at:
            raise TrialLifecycleIntegrityError(
                "trial event predates its registration"
            )
        if invalidated:
            raise TrialLifecycleConflict("no event may follow invalidation")
        if terminal and event.event_type is not TrialLifecycleEventType.TRIAL_INVALIDATED:
            raise TrialLifecycleConflict(
                "only invalidation may follow a terminal trial outcome"
            )
        if event.event_type is TrialLifecycleEventType.TRIAL_STARTED:
            if started or expected_sequence != 1:
                raise TrialLifecycleConflict("trial can start exactly once as its first event")
            started = True
        else:
            if not started:
                raise TrialLifecycleConflict("trial must start before other lifecycle events")
            if event.event_type is TrialLifecycleEventType.HOLDOUT_UNSEALED:
                if unsealed:
                    raise TrialLifecycleConflict("holdout can be unsealed only once")
                unsealed = True
            elif event.event_type in HOLDOUT_ACCESS_EVENT_TYPES and not unsealed:
                raise TrialLifecycleConflict("holdout access requires a prior unseal event")
            if (
                event.event_type
                is TrialLifecycleEventType.HOLDOUT_RESULTS_ACCESSED
            ):
                holdout_results_accessed = True
            if event.event_type in HOLDOUT_EVENT_TYPES:
                if (
                    not registration.holdout_sealed
                    or event.holdout_id != registration.holdout_id
                ):
                    raise TrialLifecycleConflict(
                        "holdout event does not match the sealed registration"
                    )
            if event.event_type is TrialLifecycleEventType.TRIAL_COMPLETED:
                if (
                    registration.stage is TrialStage.CONFIRMATORY
                    and not holdout_results_accessed
                ):
                    raise TrialLifecycleConflict(
                        "completed confirmatory trial requires audited holdout results access"
                    )
                if not expected_metrics.issubset({name for name, _ in event.metrics}):
                    raise TrialLifecycleConflict(
                        "completed outcome omits a registered evaluation metric"
                    )
                terminal = True
            elif event.event_type in {
                TrialLifecycleEventType.TRIAL_FAILED,
                TrialLifecycleEventType.TRIAL_ABORTED,
            }:
                terminal = True
            elif event.event_type is TrialLifecycleEventType.TRIAL_INVALIDATED:
                invalidated = True
        previous = event


class LocalTrialLifecycleStore:
    """Create-once per-trial event chains bound to immutable registrations."""

    def __init__(self, root: Path, registry: LocalTrialRegistry) -> None:
        self.root = Path(root)
        if type(registry) is not LocalTrialRegistry:
            raise TypeError("registry must be an exact LocalTrialRegistry")
        self.registry = registry

    @property
    def events_root(self) -> Path:
        return self.root / "events"

    def _trial_dir(self, trial_id: str) -> Path:
        if not isinstance(trial_id, str) or _SHA256.fullmatch(trial_id) is None:
            raise TrialLifecycleError("trial_id must be a full lowercase SHA-256")
        return self.events_root / trial_id

    def list_events(self, trial_id: str) -> tuple[TrialLifecycleEvent, ...]:
        registration = self.registry.require_registered(trial_id)
        trial_dir = self._trial_dir(trial_id)
        if not trial_dir.exists():
            return ()
        if not trial_dir.is_dir() or _is_link_like(trial_dir):
            raise TrialLifecycleIntegrityError(
                "trial event path must be a regular directory"
            )
        paths = tuple(sorted(trial_dir.iterdir(), key=lambda value: value.name))
        events: list[TrialLifecycleEvent] = []
        for path in paths:
            match = _EVENT_FILENAME.fullmatch(path.name)
            if match is None or not path.is_file() or _is_link_like(path):
                raise TrialLifecycleIntegrityError("trial event file set is invalid")
            try:
                payload = read_stable_regular_file(
                    path,
                    maximum_bytes=_MAX_EVENT_BYTES,
                )
            except FileSafetyError as exc:
                raise TrialLifecycleIntegrityError(
                    "trial event could not be read safely"
                ) from exc
            event = decode_trial_lifecycle_event(payload)
            if (
                int(match.group(1)) != event.sequence
                or match.group(2) != event.event_id
            ):
                raise TrialLifecycleIntegrityError("trial event filename identity mismatch")
            events.append(event)
        result = tuple(events)
        _validate_history(registration, result)
        return result

    def append(
        self,
        *,
        trial_id: str,
        event_type: TrialLifecycleEventType,
        occurred_at: datetime,
        actor_id: str,
        reason: str,
        holdout_id: str | None = None,
        metrics: tuple[tuple[str, Decimal], ...] = (),
        passed: bool | None = None,
        evaluation_result: TrialEvaluationResult | None = None,
    ) -> TrialLifecycleEvent:
        registration = self.registry.require_registered(trial_id)
        evaluation_result_id: str | None = None
        if event_type is TrialLifecycleEventType.TRIAL_COMPLETED:
            if type(evaluation_result) is not TrialEvaluationResult:
                raise TrialLifecycleConflict(
                    "trial completion requires an engine-generated evaluation result"
                )
            if metrics or passed is not None:
                raise TrialLifecycleConflict(
                    "caller-provided completion metrics are forbidden"
                )
            evaluation_result.verify_content_identity()
            if evaluation_result.trial_id != registration.trial_id:
                raise TrialLifecycleConflict("evaluation result belongs to another trial")
            if (
                evaluation_result.split_plan_id != registration.split_plan_id
                or evaluation_result.execution_policy_id
                != registration.execution_policy_hash
                or evaluation_result.cost_schedule_id
                != registration.cost_schedule_hash
                or evaluation_result.pass_thresholds != registration.pass_thresholds
            ):
                raise TrialLifecycleConflict(
                    "evaluation result does not match registered policies or thresholds"
                )
            metrics = evaluation_result.metrics
            passed = evaluation_result.passed
            evaluation_result_id = evaluation_result.result_id
        elif evaluation_result is not None:
            raise TrialLifecycleConflict(
                "only trial completion can carry an evaluation result"
            )
        self.events_root.mkdir(parents=True, exist_ok=True)
        if _is_link_like(self.events_root):
            raise TrialLifecycleIntegrityError("trial-events root cannot be a link")
        trial_dir = self._trial_dir(trial_id)
        trial_dir.mkdir(exist_ok=True)
        if _is_link_like(trial_dir):
            raise TrialLifecycleIntegrityError("trial event path cannot be a link")
        lock = self.events_root / ".trial-events.lock"
        try:
            with advisory_file_lock(lock):
                existing = self.list_events(trial_id)
                if not existing and event_type is TrialLifecycleEventType.TRIAL_STARTED:
                    self._reject_contaminated_confirmatory_successor(registration)
                event = TrialLifecycleEvent(
                    trial_id=trial_id,
                    sequence=len(existing) + 1,
                    previous_event_id=(existing[-1].event_id if existing else None),
                    event_type=event_type,
                    occurred_at=occurred_at,
                    actor_id=actor_id,
                    reason=reason,
                    holdout_id=holdout_id,
                    metrics=metrics,
                    passed=passed,
                    evaluation_result_id=evaluation_result_id,
                )
                proposed = existing + (event,)
                _validate_history(registration, proposed)
                payload = encode_trial_lifecycle_event(event)
                target = trial_dir / f"{event.sequence:020d}-{event.event_id}.json"
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".trial-event-",
                    suffix=".tmp",
                    dir=trial_dir,
                )
                temporary = Path(temporary_name)
                try:
                    with os.fdopen(descriptor, "wb") as handle:
                        handle.write(payload)
                        handle.flush()
                        os.fsync(handle.fileno())
                    os.link(temporary, target)
                finally:
                    temporary.unlink(missing_ok=True)
        except (FileLockUnavailable, FileSafetyError) as exc:
            raise TrialLifecycleConflict("trial lifecycle store is unavailable") from exc
        return event

    def _reject_contaminated_confirmatory_successor(
        self,
        registration: TrialRegistration,
    ) -> None:
        if registration.parent_trial_id is None:
            return
        try:
            parent = self.registry.get(registration.parent_trial_id)
        except TrialNotRegistered as exc:
            raise TrialLifecycleConflict("successor parent is not registered") from exc
        parent_events = self.list_events(parent.trial_id)
        parent_unsealed = any(
            value.event_type is TrialLifecycleEventType.HOLDOUT_UNSEALED
            for value in parent_events
        )
        if (
            parent_unsealed
            and registration.stage is TrialStage.CONFIRMATORY
            and registration.holdout_id == parent.holdout_id
        ):
            raise TrialLifecycleConflict(
                "confirmatory successor cannot reuse its parent's unsealed holdout"
            )

    def outcomes(self, trial_id: str) -> tuple[TrialLifecycleEvent, ...]:
        return tuple(
            event
            for event in self.list_events(trial_id)
            if event.event_type in TERMINAL_OUTCOME_EVENT_TYPES
            or event.event_type is TrialLifecycleEventType.TRIAL_INVALIDATED
        )
