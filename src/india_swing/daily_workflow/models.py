from __future__ import annotations

import re
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum

from india_swing.identity import content_id
from india_swing.paper_outcomes import validate_paper_outcome_state_bucket


WORKFLOW_SPEC_SCHEMA = "daily-paper-workflow-spec/v1"
WORKFLOW_OUTPUT_SCHEMA = "daily-paper-workflow-output/v1"
WORKFLOW_EVENT_SCHEMA = "daily-paper-workflow-event/v1"
WORKFLOW_TERMINAL_SCHEMA = "daily-paper-workflow-terminal/v1"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,63}\Z")


class DailyPaperWorkflowError(RuntimeError):
    pass


def _sha(value: object, name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise DailyPaperWorkflowError(f"{name} must be a lowercase SHA-256")
    return value


def _utc(value: object, name: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is None:
        raise DailyPaperWorkflowError(f"{name} must be timezone-aware")
    try:
        offset = value.utcoffset()
    except Exception:
        raise DailyPaperWorkflowError(f"{name} has invalid timezone behavior") from None
    if offset is None:
        raise DailyPaperWorkflowError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _positive_decimal(value: object, name: str) -> Decimal:
    if type(value) is not Decimal or not value.is_finite() or value <= 0:
        raise DailyPaperWorkflowError(f"{name} must be a positive finite Decimal")
    return value


class DailyPaperWorkflowOutputStatus(str, Enum):
    COMPLETE = "COMPLETE"
    NO_ACTIVE_POSITIONS = "NO_ACTIVE_POSITIONS"


class DailyPaperWorkflowEventStatus(str, Enum):
    STARTED = "STARTED"
    COMPLETED = "COMPLETED"
    REJECTED = "REJECTED"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class PublishedManifestPin:
    object_name: str
    generation: int
    sha256: str
    pin_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            type(self.object_name) is not str
            or not self.object_name
            or self.object_name.startswith("/")
            or "\\" in self.object_name
            or any(part in {"", ".", ".."} for part in self.object_name.split("/"))
        ):
            raise DailyPaperWorkflowError("manifest object name is invalid")
        if type(self.generation) is not int or type(self.generation) is bool or self.generation <= 0:
            raise DailyPaperWorkflowError("manifest generation is invalid")
        _sha(self.sha256, "manifest_sha256")
        object.__setattr__(self, "pin_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "object_name": self.object_name,
                "generation": self.generation,
                "sha256": self.sha256,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.pin_id != self._calculated_id():
            raise DailyPaperWorkflowError("manifest pin identity failed")


@dataclass(frozen=True, slots=True)
class DailyPaperWorkflowSpec:
    run_id: str
    derived_evidence_id: str
    state_bucket: str
    daily_loss_limit: Decimal = Decimal("1000")
    cumulative_loss_limit: Decimal = Decimal("2000")
    maximum_attempts: int = 3
    mode: str = "PAPER_ONLY"
    schema_version: str = WORKFLOW_SPEC_SCHEMA
    workflow_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha(self.run_id, "run_id")
        _sha(self.derived_evidence_id, "derived_evidence_id")
        try:
            bucket = validate_paper_outcome_state_bucket(self.state_bucket)
        except Exception:
            raise DailyPaperWorkflowError("workflow state bucket is invalid") from None
        object.__setattr__(self, "state_bucket", bucket)
        _positive_decimal(self.daily_loss_limit, "daily_loss_limit")
        _positive_decimal(self.cumulative_loss_limit, "cumulative_loss_limit")
        if (
            type(self.maximum_attempts) is not int
            or type(self.maximum_attempts) is bool
            or not 1 <= self.maximum_attempts <= 10
        ):
            raise DailyPaperWorkflowError("maximum_attempts is invalid")
        if self.mode != "PAPER_ONLY" or self.schema_version != WORKFLOW_SPEC_SCHEMA:
            raise DailyPaperWorkflowError("workflow authority boundary is invalid")
        object.__setattr__(self, "workflow_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "workflow_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.workflow_id != self._calculated_id():
            raise DailyPaperWorkflowError("workflow spec identity failed")


@dataclass(frozen=True, slots=True)
class DailyPaperWorkflowOutput:
    status: DailyPaperWorkflowOutputStatus
    preparation_id: str | None
    batch_id: str | None
    state_id: str | None
    outcome_manifest_pins: tuple[PublishedManifestPin, ...]
    portfolio_manifest_pin: PublishedManifestPin | None
    telegram_receipt_id: str
    schema_version: str = WORKFLOW_OUTPUT_SCHEMA
    output_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.status) is not DailyPaperWorkflowOutputStatus:
            raise DailyPaperWorkflowError("workflow output status must be exact")
        for value, name in (
            (self.preparation_id, "preparation_id"),
            (self.batch_id, "batch_id"),
            (self.state_id, "state_id"),
        ):
            if value is not None:
                _sha(value, name)
        if (
            type(self.outcome_manifest_pins) is not tuple
            or any(type(value) is not PublishedManifestPin for value in self.outcome_manifest_pins)
        ):
            raise DailyPaperWorkflowError("outcome manifest pins are invalid")
        for value in self.outcome_manifest_pins:
            value.verify_content_identity()
        if tuple(value.pin_id for value in self.outcome_manifest_pins) != tuple(
            sorted({value.pin_id for value in self.outcome_manifest_pins})
        ):
            raise DailyPaperWorkflowError("outcome manifest pins must be unique and ordered")
        if self.portfolio_manifest_pin is not None:
            if type(self.portfolio_manifest_pin) is not PublishedManifestPin:
                raise DailyPaperWorkflowError("portfolio manifest pin is invalid")
            self.portfolio_manifest_pin.verify_content_identity()
        _sha(self.telegram_receipt_id, "telegram_receipt_id")
        if self.status is DailyPaperWorkflowOutputStatus.COMPLETE:
            if (
                self.preparation_id is None
                or self.batch_id is None
                or self.state_id is None
                or not self.outcome_manifest_pins
                or self.portfolio_manifest_pin is None
            ):
                raise DailyPaperWorkflowError("complete workflow output lacks lineage")
        elif any(
            value is not None
            for value in (
                self.preparation_id,
                self.batch_id,
                self.state_id,
                self.portfolio_manifest_pin,
            )
        ) or self.outcome_manifest_pins:
            raise DailyPaperWorkflowError("no-position workflow output implies portfolio work")
        if self.schema_version != WORKFLOW_OUTPUT_SCHEMA:
            raise DailyPaperWorkflowError("workflow output schema is unsupported")
        object.__setattr__(self, "output_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "output_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.output_id != self._calculated_id():
            raise DailyPaperWorkflowError("workflow output identity failed")


@dataclass(frozen=True, slots=True)
class DailyPaperWorkflowTerminal:
    workflow_id: str
    output: DailyPaperWorkflowOutput
    started_at: datetime
    completed_at: datetime
    schema_version: str = WORKFLOW_TERMINAL_SCHEMA
    terminal_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha(self.workflow_id, "workflow_id")
        if type(self.output) is not DailyPaperWorkflowOutput:
            raise DailyPaperWorkflowError("workflow terminal output must be exact")
        self.output.verify_content_identity()
        object.__setattr__(self, "started_at", _utc(self.started_at, "started_at"))
        object.__setattr__(self, "completed_at", _utc(self.completed_at, "completed_at"))
        if self.completed_at < self.started_at:
            raise DailyPaperWorkflowError("workflow terminal time moved backwards")
        if self.schema_version != WORKFLOW_TERMINAL_SCHEMA:
            raise DailyPaperWorkflowError("workflow terminal schema is unsupported")
        object.__setattr__(self, "terminal_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "terminal_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.terminal_id != self._calculated_id():
            raise DailyPaperWorkflowError("workflow terminal identity failed")


@dataclass(frozen=True, slots=True)
class DailyPaperWorkflowEvent:
    workflow_id: str
    sequence: int
    previous_event_id: str | None
    status: DailyPaperWorkflowEventStatus
    occurred_at: datetime
    reason_code: str | None = None
    terminal_id: str | None = None
    schema_version: str = WORKFLOW_EVENT_SCHEMA
    event_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha(self.workflow_id, "workflow_id")
        if type(self.sequence) is not int or type(self.sequence) is bool or self.sequence <= 0:
            raise DailyPaperWorkflowError("workflow event sequence is invalid")
        if self.previous_event_id is not None:
            _sha(self.previous_event_id, "previous_event_id")
        if type(self.status) is not DailyPaperWorkflowEventStatus:
            raise DailyPaperWorkflowError("workflow event status must be exact")
        object.__setattr__(self, "occurred_at", _utc(self.occurred_at, "occurred_at"))
        if self.reason_code is not None and (
            type(self.reason_code) is not str or _CODE.fullmatch(self.reason_code) is None
        ):
            raise DailyPaperWorkflowError("workflow reason code is invalid")
        if self.terminal_id is not None:
            _sha(self.terminal_id, "terminal_id")
        if self.status is DailyPaperWorkflowEventStatus.STARTED:
            if self.reason_code is not None or self.terminal_id is not None:
                raise DailyPaperWorkflowError("started workflow event has terminal fields")
        elif self.status is DailyPaperWorkflowEventStatus.COMPLETED:
            if self.reason_code is not None or self.terminal_id is None:
                raise DailyPaperWorkflowError("completed workflow event lacks terminal")
        elif self.reason_code is None or self.terminal_id is not None:
            raise DailyPaperWorkflowError("failed workflow event lacks a reason")
        if self.schema_version != WORKFLOW_EVENT_SCHEMA:
            raise DailyPaperWorkflowError("workflow event schema is unsupported")
        object.__setattr__(self, "event_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "event_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.event_id != self._calculated_id():
            raise DailyPaperWorkflowError("workflow event identity failed")
