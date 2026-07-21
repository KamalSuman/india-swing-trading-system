from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)

from .models import (
    DailyPaperWorkflowError,
    DailyPaperWorkflowEvent,
    DailyPaperWorkflowEventStatus,
    DailyPaperWorkflowOutput,
    DailyPaperWorkflowOutputStatus,
    DailyPaperWorkflowSpec,
    DailyPaperWorkflowTerminal,
    PublishedManifestPin,
)


_CODEC = "daily-paper-workflow-json/v1"
_MAXIMUM_BYTES = 2 * 1024 * 1024
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_EVENT_NAME = re.compile(r"([0-9]{20})-([0-9a-f]{64})\.json\Z")


def _is_link_like(path: Path) -> bool:
    try:
        status = os.lstat(path)
    except OSError:
        return path.is_symlink()
    return path.is_symlink() or bool(
        getattr(status, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise DailyPaperWorkflowError("workflow JSON contains duplicate keys")
        result[key] = value
    return result


def _loads(payload: bytes) -> dict[str, object]:
    try:
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
    except DailyPaperWorkflowError:
        raise
    except Exception:
        raise DailyPaperWorkflowError("stored workflow JSON is invalid") from None
    if type(value) is not dict:
        raise DailyPaperWorkflowError("stored workflow envelope is invalid")
    return value


def _dumps(kind: str, value: dict[str, object]) -> bytes:
    payload = (
        json.dumps(
            {"codec_schema_version": _CODEC, "kind": kind, "value": value},
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    if len(payload) > _MAXIMUM_BYTES:
        raise DailyPaperWorkflowError("workflow artifact is too large")
    return payload


def _envelope(payload: bytes, kind: str) -> dict[str, object]:
    value = _loads(payload)
    if (
        set(value) != {"codec_schema_version", "kind", "value"}
        or value["codec_schema_version"] != _CODEC
        or value["kind"] != kind
        or type(value["value"]) is not dict
    ):
        raise DailyPaperWorkflowError("stored workflow envelope is invalid")
    return value["value"]


def _pin_data(value: PublishedManifestPin) -> dict[str, object]:
    value.verify_content_identity()
    return {
        "generation": value.generation,
        "object_name": value.object_name,
        "pin_id": value.pin_id,
        "sha256": value.sha256,
    }


def _pin(value: object) -> PublishedManifestPin:
    if type(value) is not dict or set(value) != {
        "generation", "object_name", "pin_id", "sha256"
    }:
        raise DailyPaperWorkflowError("stored manifest pin is invalid")
    pin = PublishedManifestPin(
        object_name=value["object_name"],
        generation=value["generation"],
        sha256=value["sha256"],
    )
    if pin.pin_id != value["pin_id"]:
        raise DailyPaperWorkflowError("stored manifest pin identity differs")
    return pin


def encode_workflow_spec(value: DailyPaperWorkflowSpec) -> bytes:
    if type(value) is not DailyPaperWorkflowSpec:
        raise DailyPaperWorkflowError("workflow spec must be exact")
    value.verify_content_identity()
    return _dumps(
        "SPEC",
        {
            "cumulative_loss_limit": str(value.cumulative_loss_limit),
            "daily_loss_limit": str(value.daily_loss_limit),
            "derived_evidence_id": value.derived_evidence_id,
            "maximum_attempts": value.maximum_attempts,
            "mode": value.mode,
            "run_id": value.run_id,
            "schema_version": value.schema_version,
            "state_bucket": value.state_bucket,
            "workflow_id": value.workflow_id,
        },
    )


def decode_workflow_spec(payload: bytes) -> DailyPaperWorkflowSpec:
    raw = _envelope(payload, "SPEC")
    expected = {
        "cumulative_loss_limit", "daily_loss_limit", "derived_evidence_id",
        "maximum_attempts", "mode", "run_id", "schema_version", "state_bucket",
        "workflow_id",
    }
    if set(raw) != expected:
        raise DailyPaperWorkflowError("stored workflow spec fields are invalid")
    try:
        value = DailyPaperWorkflowSpec(
            run_id=raw["run_id"],
            derived_evidence_id=raw["derived_evidence_id"],
            state_bucket=raw["state_bucket"],
            daily_loss_limit=Decimal(raw["daily_loss_limit"]),
            cumulative_loss_limit=Decimal(raw["cumulative_loss_limit"]),
            maximum_attempts=raw["maximum_attempts"],
            mode=raw["mode"],
            schema_version=raw["schema_version"],
        )
    except DailyPaperWorkflowError:
        raise
    except Exception:
        raise DailyPaperWorkflowError("stored workflow spec is invalid") from None
    if value.workflow_id != raw["workflow_id"] or encode_workflow_spec(value) != payload:
        raise DailyPaperWorkflowError("stored workflow spec identity differs")
    return value


def _output_data(value: DailyPaperWorkflowOutput) -> dict[str, object]:
    value.verify_content_identity()
    return {
        "batch_id": value.batch_id,
        "outcome_manifest_pins": [_pin_data(item) for item in value.outcome_manifest_pins],
        "output_id": value.output_id,
        "portfolio_manifest_pin": (
            None if value.portfolio_manifest_pin is None else _pin_data(value.portfolio_manifest_pin)
        ),
        "preparation_id": value.preparation_id,
        "schema_version": value.schema_version,
        "state_id": value.state_id,
        "status": value.status.value,
        "telegram_receipt_id": value.telegram_receipt_id,
    }


def _output(raw: object) -> DailyPaperWorkflowOutput:
    expected = {
        "batch_id", "outcome_manifest_pins", "output_id", "portfolio_manifest_pin",
        "preparation_id", "schema_version", "state_id", "status",
        "telegram_receipt_id",
    }
    if type(raw) is not dict or set(raw) != expected or type(raw["outcome_manifest_pins"]) is not list:
        raise DailyPaperWorkflowError("stored workflow output is invalid")
    value = DailyPaperWorkflowOutput(
        status=DailyPaperWorkflowOutputStatus(raw["status"]),
        preparation_id=raw["preparation_id"],
        batch_id=raw["batch_id"],
        state_id=raw["state_id"],
        outcome_manifest_pins=tuple(_pin(item) for item in raw["outcome_manifest_pins"]),
        portfolio_manifest_pin=(
            None if raw["portfolio_manifest_pin"] is None else _pin(raw["portfolio_manifest_pin"])
        ),
        telegram_receipt_id=raw["telegram_receipt_id"],
        schema_version=raw["schema_version"],
    )
    if value.output_id != raw["output_id"]:
        raise DailyPaperWorkflowError("stored workflow output identity differs")
    return value


def encode_workflow_terminal(value: DailyPaperWorkflowTerminal) -> bytes:
    if type(value) is not DailyPaperWorkflowTerminal:
        raise DailyPaperWorkflowError("workflow terminal must be exact")
    value.verify_content_identity()
    return _dumps(
        "TERMINAL",
        {
            "completed_at": value.completed_at.isoformat(),
            "output": _output_data(value.output),
            "schema_version": value.schema_version,
            "started_at": value.started_at.isoformat(),
            "terminal_id": value.terminal_id,
            "workflow_id": value.workflow_id,
        },
    )


def decode_workflow_terminal(payload: bytes) -> DailyPaperWorkflowTerminal:
    raw = _envelope(payload, "TERMINAL")
    expected = {"completed_at", "output", "schema_version", "started_at", "terminal_id", "workflow_id"}
    if set(raw) != expected:
        raise DailyPaperWorkflowError("stored workflow terminal fields are invalid")
    try:
        value = DailyPaperWorkflowTerminal(
            workflow_id=raw["workflow_id"],
            output=_output(raw["output"]),
            started_at=datetime.fromisoformat(raw["started_at"]),
            completed_at=datetime.fromisoformat(raw["completed_at"]),
            schema_version=raw["schema_version"],
        )
    except DailyPaperWorkflowError:
        raise
    except Exception:
        raise DailyPaperWorkflowError("stored workflow terminal is invalid") from None
    if value.terminal_id != raw["terminal_id"] or encode_workflow_terminal(value) != payload:
        raise DailyPaperWorkflowError("stored workflow terminal identity differs")
    return value


def encode_workflow_event(value: DailyPaperWorkflowEvent) -> bytes:
    if type(value) is not DailyPaperWorkflowEvent:
        raise DailyPaperWorkflowError("workflow event must be exact")
    value.verify_content_identity()
    return _dumps(
        "EVENT",
        {
            "event_id": value.event_id,
            "occurred_at": value.occurred_at.isoformat(),
            "previous_event_id": value.previous_event_id,
            "reason_code": value.reason_code,
            "schema_version": value.schema_version,
            "sequence": value.sequence,
            "status": value.status.value,
            "terminal_id": value.terminal_id,
            "workflow_id": value.workflow_id,
        },
    )


def decode_workflow_event(payload: bytes) -> DailyPaperWorkflowEvent:
    raw = _envelope(payload, "EVENT")
    expected = {
        "event_id", "occurred_at", "previous_event_id", "reason_code", "schema_version",
        "sequence", "status", "terminal_id", "workflow_id",
    }
    if set(raw) != expected:
        raise DailyPaperWorkflowError("stored workflow event fields are invalid")
    try:
        value = DailyPaperWorkflowEvent(
            workflow_id=raw["workflow_id"],
            sequence=raw["sequence"],
            previous_event_id=raw["previous_event_id"],
            status=DailyPaperWorkflowEventStatus(raw["status"]),
            occurred_at=datetime.fromisoformat(raw["occurred_at"]),
            reason_code=raw["reason_code"],
            terminal_id=raw["terminal_id"],
            schema_version=raw["schema_version"],
        )
    except DailyPaperWorkflowError:
        raise
    except Exception:
        raise DailyPaperWorkflowError("stored workflow event is invalid") from None
    if value.event_id != raw["event_id"] or encode_workflow_event(value) != payload:
        raise DailyPaperWorkflowError("stored workflow event identity differs")
    return value


class LocalDailyPaperWorkflowStore:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def _path(self, kind: str, workflow_id: str) -> Path:
        if type(workflow_id) is not str or _SHA256.fullmatch(workflow_id) is None:
            raise DailyPaperWorkflowError("workflow ID is invalid")
        return self.root / kind / f"{workflow_id}.json"

    def _verify_root(self, *, create: bool = False) -> None:
        try:
            if create:
                self.root.mkdir(parents=True, exist_ok=True)
            if self.root.exists() and (
                not self.root.is_dir() or _is_link_like(self.root)
            ):
                raise DailyPaperWorkflowError("workflow store root is unsafe")
        except DailyPaperWorkflowError:
            raise
        except OSError:
            raise DailyPaperWorkflowError("workflow store is unavailable") from None

    def spec_path(self, workflow_id: str) -> Path:
        return self._path("specifications", workflow_id)

    def terminal_path(self, workflow_id: str) -> Path:
        return self._path("terminals", workflow_id)

    def events_root(self, workflow_id: str) -> Path:
        self._path("events", workflow_id)
        return self.root / "events" / workflow_id

    @staticmethod
    def _create_once(target: Path, payload: bytes) -> None:
        descriptor, name = tempfile.mkstemp(prefix=".daily-workflow-", suffix=".tmp", dir=target.parent)
        temporary = Path(name)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.link(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)

    def _put(self, target: Path, payload: bytes, decode) -> object:
        try:
            self._verify_root(create=True)
            target.parent.mkdir(parents=True, exist_ok=True)
            if _is_link_like(target.parent):
                raise DailyPaperWorkflowError("workflow store root is unsafe")
            with advisory_file_lock(self.root / ".daily-workflow.lock"):
                if target.exists():
                    stored = decode(read_stable_regular_file(target, maximum_bytes=_MAXIMUM_BYTES))
                    stored_payload = (
                        encode_workflow_spec(stored)
                        if type(stored) is DailyPaperWorkflowSpec
                        else encode_workflow_terminal(stored)
                    )
                    if stored_payload != payload:
                        raise DailyPaperWorkflowError("workflow artifact already differs")
                    return stored
                self._create_once(target, payload)
        except DailyPaperWorkflowError:
            raise
        except (FileLockUnavailable, FileSafetyError, OSError):
            raise DailyPaperWorkflowError("workflow store is unavailable") from None
        return decode(read_stable_regular_file(target, maximum_bytes=_MAXIMUM_BYTES))

    def put_spec(self, value: DailyPaperWorkflowSpec) -> DailyPaperWorkflowSpec:
        return self._put(self.spec_path(value.workflow_id), encode_workflow_spec(value), decode_workflow_spec)

    def get_spec(self, workflow_id: str) -> DailyPaperWorkflowSpec:
        self._verify_root()
        path = self.spec_path(workflow_id)
        if not path.is_file() or _is_link_like(path):
            raise DailyPaperWorkflowError("workflow spec was not found safely")
        return decode_workflow_spec(read_stable_regular_file(path, maximum_bytes=_MAXIMUM_BYTES))

    def put_terminal(self, value: DailyPaperWorkflowTerminal) -> DailyPaperWorkflowTerminal:
        return self._put(
            self.terminal_path(value.workflow_id),
            encode_workflow_terminal(value),
            decode_workflow_terminal,
        )

    def get_terminal(self, workflow_id: str) -> DailyPaperWorkflowTerminal | None:
        self._verify_root()
        path = self.terminal_path(workflow_id)
        if not path.exists():
            return None
        if not path.is_file() or _is_link_like(path):
            raise DailyPaperWorkflowError("workflow terminal path is unsafe")
        return decode_workflow_terminal(read_stable_regular_file(path, maximum_bytes=_MAXIMUM_BYTES))

    def list_events(self, workflow_id: str) -> tuple[DailyPaperWorkflowEvent, ...]:
        self._verify_root()
        directory = self.events_root(workflow_id)
        if not directory.exists():
            return ()
        if not directory.is_dir() or _is_link_like(directory):
            raise DailyPaperWorkflowError("workflow event set is unsafe")
        events = []
        for path in sorted(directory.iterdir(), key=lambda value: value.name):
            match = _EVENT_NAME.fullmatch(path.name)
            if match is None or not path.is_file() or _is_link_like(path):
                raise DailyPaperWorkflowError("workflow event file set is invalid")
            event = decode_workflow_event(read_stable_regular_file(path, maximum_bytes=_MAXIMUM_BYTES))
            if event.sequence != int(match.group(1)) or event.event_id != match.group(2):
                raise DailyPaperWorkflowError("workflow event path differs")
            events.append(event)
        previous = None
        for sequence, event in enumerate(events, 1):
            if (
                event.workflow_id != workflow_id
                or event.sequence != sequence
                or event.previous_event_id != (None if previous is None else previous.event_id)
                or (previous is not None and event.occurred_at < previous.occurred_at)
            ):
                raise DailyPaperWorkflowError("workflow event chain is broken")
            if sequence % 2 == 1:
                if event.status is not DailyPaperWorkflowEventStatus.STARTED:
                    raise DailyPaperWorkflowError("workflow attempt chain is broken")
            elif event.status is DailyPaperWorkflowEventStatus.STARTED:
                raise DailyPaperWorkflowError("workflow attempt chain is broken")
            previous = event
        return tuple(events)

    def append_event(
        self,
        *,
        workflow_id: str,
        status: DailyPaperWorkflowEventStatus,
        occurred_at: datetime,
        reason_code: str | None = None,
        terminal_id: str | None = None,
    ) -> DailyPaperWorkflowEvent:
        directory = self.events_root(workflow_id)
        try:
            self._verify_root(create=True)
            directory.mkdir(parents=True, exist_ok=True)
            if _is_link_like(directory):
                raise DailyPaperWorkflowError("workflow event store is unsafe")
            with advisory_file_lock(self.root / ".daily-workflow.lock"):
                existing = self.list_events(workflow_id)
                if (
                    (len(existing) % 2 == 0)
                    != (status is DailyPaperWorkflowEventStatus.STARTED)
                ):
                    raise DailyPaperWorkflowError("workflow attempt transition is invalid")
                value = DailyPaperWorkflowEvent(
                    workflow_id=workflow_id,
                    sequence=len(existing) + 1,
                    previous_event_id=None if not existing else existing[-1].event_id,
                    status=status,
                    occurred_at=occurred_at,
                    reason_code=reason_code,
                    terminal_id=terminal_id,
                )
                target = directory / f"{value.sequence:020d}-{value.event_id}.json"
                self._create_once(target, encode_workflow_event(value))
        except DailyPaperWorkflowError:
            raise
        except (FileLockUnavailable, FileSafetyError, OSError):
            raise DailyPaperWorkflowError("workflow event store is unavailable") from None
        return self.list_events(workflow_id)[-1]
