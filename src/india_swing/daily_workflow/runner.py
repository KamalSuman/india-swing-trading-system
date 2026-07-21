from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Protocol

from .models import (
    DailyPaperWorkflowError,
    DailyPaperWorkflowEventStatus,
    DailyPaperWorkflowOutput,
    DailyPaperWorkflowOutputStatus,
    DailyPaperWorkflowSpec,
    DailyPaperWorkflowTerminal,
)
from .store import LocalDailyPaperWorkflowStore


class DailyPaperWorkflowExecutionError(DailyPaperWorkflowError):
    pass


class DailyPaperWorkflowRejected(DailyPaperWorkflowExecutionError):
    def __init__(self, reason_code: str = "WORKFLOW_REJECTED") -> None:
        self.reason_code = reason_code
        super().__init__("daily paper workflow was rejected safely")


class DailyPaperWorkflowRetryExhausted(DailyPaperWorkflowExecutionError):
    pass


class DailyPaperWorkflowWorker(Protocol):
    def execute(self, spec: DailyPaperWorkflowSpec) -> DailyPaperWorkflowOutput: ...


def _clock_value(clock: Callable[[], datetime]) -> datetime:
    value = clock()
    if type(value) is not datetime or value.tzinfo is None or value.utcoffset() is None:
        raise DailyPaperWorkflowExecutionError("workflow clock returned an invalid time")
    return value


def _attempt_end_time(
    clock: Callable[[], datetime], started_at: datetime
) -> datetime:
    try:
        value = _clock_value(clock)
    except Exception:
        return started_at
    return max(value, started_at)


def run_daily_paper_workflow(
    *,
    spec: DailyPaperWorkflowSpec,
    worker: DailyPaperWorkflowWorker,
    store: LocalDailyPaperWorkflowStore,
    clock: Callable[[], datetime],
) -> DailyPaperWorkflowTerminal:
    """Run one bounded attempt, publishing the terminal record last.

    The worker owns domain idempotency. This runner owns attempt accounting:
    every start and sanitized terminal attempt status is append-only, and no
    more than the spec's explicit maximum attempts can be consumed.
    """

    if type(spec) is not DailyPaperWorkflowSpec:
        raise DailyPaperWorkflowExecutionError("workflow spec must be exact")
    if type(store) is not LocalDailyPaperWorkflowStore:
        raise DailyPaperWorkflowExecutionError("workflow store must be exact")
    if not callable(getattr(worker, "execute", None)):
        raise DailyPaperWorkflowExecutionError("workflow worker is invalid")
    if not callable(clock):
        raise DailyPaperWorkflowExecutionError("workflow clock is required")
    spec.verify_content_identity()
    stored_spec = store.put_spec(spec)
    if stored_spec != spec:
        raise DailyPaperWorkflowExecutionError("stored workflow spec differs")

    terminal = store.get_terminal(spec.workflow_id)
    if terminal is not None:
        terminal.verify_content_identity()
        if terminal.workflow_id != spec.workflow_id:
            raise DailyPaperWorkflowExecutionError("stored workflow terminal differs")
        events = store.list_events(spec.workflow_id)
        if not events:
            raise DailyPaperWorkflowExecutionError("workflow terminal lacks an attempt")
        expected_status = (
            DailyPaperWorkflowEventStatus.COMPLETED
            if terminal.output.status is DailyPaperWorkflowOutputStatus.COMPLETE
            else DailyPaperWorkflowEventStatus.REJECTED
        )
        if events[-1].status is DailyPaperWorkflowEventStatus.STARTED:
            store.append_event(
                workflow_id=spec.workflow_id,
                status=expected_status,
                occurred_at=_clock_value(clock),
                reason_code=(
                    None
                    if expected_status is DailyPaperWorkflowEventStatus.COMPLETED
                    else "NO_ACTIVE_POSITIONS"
                ),
                terminal_id=(
                    terminal.terminal_id
                    if expected_status is DailyPaperWorkflowEventStatus.COMPLETED
                    else None
                ),
            )
        elif (
            events[-1].status is not expected_status
            or (
                expected_status is DailyPaperWorkflowEventStatus.COMPLETED
                and events[-1].terminal_id != terminal.terminal_id
            )
            or (
                expected_status is DailyPaperWorkflowEventStatus.REJECTED
                and events[-1].reason_code != "NO_ACTIVE_POSITIONS"
            )
        ):
            raise DailyPaperWorkflowExecutionError(
                "workflow terminal attempt status differs"
            )
        return terminal

    events = store.list_events(spec.workflow_id)
    attempts = sum(
        value.status is DailyPaperWorkflowEventStatus.STARTED for value in events
    )
    if attempts >= spec.maximum_attempts:
        raise DailyPaperWorkflowRetryExhausted(
            "daily paper workflow retry budget is exhausted"
        )

    started_at = _clock_value(clock)
    store.append_event(
        workflow_id=spec.workflow_id,
        status=DailyPaperWorkflowEventStatus.STARTED,
        occurred_at=started_at,
    )
    try:
        output = worker.execute(spec)
        if type(output) is not DailyPaperWorkflowOutput:
            raise DailyPaperWorkflowExecutionError(
                "workflow worker returned an invalid output"
            )
        output.verify_content_identity()
        completed_at = _clock_value(clock)
        terminal = store.put_terminal(
            DailyPaperWorkflowTerminal(
                workflow_id=spec.workflow_id,
                output=output,
                started_at=started_at,
                completed_at=completed_at,
            )
        )
        store.append_event(
            workflow_id=spec.workflow_id,
            status=(
                DailyPaperWorkflowEventStatus.COMPLETED
                if output.status is DailyPaperWorkflowOutputStatus.COMPLETE
                else DailyPaperWorkflowEventStatus.REJECTED
            ),
            occurred_at=completed_at,
            reason_code=(
                None
                if output.status is DailyPaperWorkflowOutputStatus.COMPLETE
                else "NO_ACTIVE_POSITIONS"
            ),
            terminal_id=(
                terminal.terminal_id
                if output.status is DailyPaperWorkflowOutputStatus.COMPLETE
                else None
            ),
        )
        return terminal
    except DailyPaperWorkflowRejected as exc:
        store.append_event(
            workflow_id=spec.workflow_id,
            status=DailyPaperWorkflowEventStatus.REJECTED,
            occurred_at=_attempt_end_time(clock, started_at),
            reason_code=exc.reason_code,
        )
        raise
    except Exception:
        store.append_event(
            workflow_id=spec.workflow_id,
            status=DailyPaperWorkflowEventStatus.FAILED,
            occurred_at=_attempt_end_time(clock, started_at),
            reason_code="WORKFLOW_EXECUTION_FAILED",
        )
        raise DailyPaperWorkflowExecutionError(
            "daily paper workflow execution failed safely"
        ) from None
