from __future__ import annotations

import hashlib
import json
import re
from datetime import date, time

from .models import (
    CALENDAR_DECLARATION_SCHEMA_VERSION,
    CalendarDeclaredDayKind,
    CalendarEvent,
    CalendarEventType,
    CalendarSourceArtifactIntegrityError,
    CalendarSourceLocator,
    CalendarWeekday,
    CalendarWindowPhase,
    DeclaredSessionWindow,
    ParsedCalendarDeclaration,
)


MAXIMUM_CALENDAR_SOURCE_BYTES = 50 * 1024 * 1024
MAXIMUM_CALENDAR_DECLARATION_BYTES = 2 * 1024 * 1024
MAXIMUM_CALENDAR_EVENTS = 20_000

_CLOCK_TIME = re.compile(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]\Z")
_ROOT_KEYS = {
    "schema_version",
    "exchange",
    "segment",
    "claimed_authority",
    "claimed_document_id",
    "claimed_issue_date",
    "claimed_source_url",
    "source_filename",
    "source_media_type",
    "source_byte_count",
    "source_sha256",
    "events",
}
_LOCATOR_KEYS = {"page", "section", "record"}
_WINDOW_KEYS = {"phase", "opens", "closes"}
_EVENT_KEYS = {
    CalendarEventType.BASE_WEEKLY_SCHEDULE: {
        "event_type",
        "effective_from",
        "effective_to_exclusive",
        "weekdays",
        "windows",
        "supersedes_event_ids",
        "source_locator",
        "reason",
    },
    CalendarEventType.DATE_CLOSED: {
        "event_type",
        "date",
        "day_kind",
        "supersedes_event_ids",
        "source_locator",
        "reason",
    },
    CalendarEventType.DATE_SESSION_REPLACED: {
        "event_type",
        "date",
        "day_kind",
        "windows",
        "supersedes_event_ids",
        "source_locator",
        "reason",
    },
    CalendarEventType.NON_EXECUTABLE_ACTIVITY: {
        "event_type",
        "date",
        "windows",
        "supersedes_event_ids",
        "source_locator",
        "reason",
    },
}


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _reject_float(_: str) -> object:
    raise CalendarSourceArtifactIntegrityError(
        "calendar declaration cannot contain floating-point numbers"
    )


def _reject_constant(_: str) -> object:
    raise CalendarSourceArtifactIntegrityError(
        "calendar declaration cannot contain non-finite numbers"
    )


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise CalendarSourceArtifactIntegrityError(
                "calendar declaration contains a duplicate JSON key"
            )
        value[key] = item
    return value


def decode_strict_json(payload: bytes, *, label: str) -> object:
    if type(payload) is not bytes or not payload:
        raise CalendarSourceArtifactIntegrityError(f"{label} must be non-empty bytes")
    if payload.startswith(b"\xef\xbb\xbf"):
        raise CalendarSourceArtifactIntegrityError(f"{label} cannot contain a UTF-8 BOM")
    try:
        text = payload.decode("utf-8", errors="strict")
        return json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_float=_reject_float,
            parse_constant=_reject_constant,
        )
    except CalendarSourceArtifactIntegrityError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CalendarSourceArtifactIntegrityError(f"{label} is not strict UTF-8 JSON") from exc


def _expect_object(value: object, keys: set[str], label: str) -> dict[str, object]:
    if type(value) is not dict or set(value) != keys:
        raise CalendarSourceArtifactIntegrityError(f"{label} schema mismatch")
    return value


def _text(value: object, label: str) -> str:
    if type(value) is not str:
        raise CalendarSourceArtifactIntegrityError(f"{label} must be text")
    return value


def _date(value: object, label: str) -> date:
    raw = _text(value, label)
    try:
        parsed = date.fromisoformat(raw)
    except ValueError as exc:
        raise CalendarSourceArtifactIntegrityError(f"{label} must be an ISO date") from exc
    if parsed.isoformat() != raw:
        raise CalendarSourceArtifactIntegrityError(f"{label} must be a canonical ISO date")
    return parsed


def _time(value: object, label: str) -> time:
    raw = _text(value, label)
    if _CLOCK_TIME.fullmatch(raw) is None:
        raise CalendarSourceArtifactIntegrityError(f"{label} must be HH:MM:SS")
    return time.fromisoformat(raw)


def _source_locator(value: object) -> CalendarSourceLocator:
    raw = _expect_object(value, _LOCATOR_KEYS, "source_locator")
    page = raw["page"]
    if page is not None and type(page) is not int:
        raise CalendarSourceArtifactIntegrityError(
            "source_locator.page must be an integer or null"
        )
    record = raw["record"]
    if record is not None and type(record) is not str:
        raise CalendarSourceArtifactIntegrityError(
            "source_locator.record must be text or null"
        )
    try:
        return CalendarSourceLocator(
            page=page,
            section=_text(raw["section"], "source_locator.section"),
            record=record,
        )
    except (TypeError, ValueError) as exc:
        raise CalendarSourceArtifactIntegrityError("invalid source locator") from exc


def _windows(value: object) -> tuple[DeclaredSessionWindow, ...]:
    if type(value) is not list:
        raise CalendarSourceArtifactIntegrityError("event windows must be a JSON array")
    parsed: list[DeclaredSessionWindow] = []
    for item in value:
        raw = _expect_object(item, _WINDOW_KEYS, "session window")
        try:
            parsed.append(
                DeclaredSessionWindow(
                    opens=_time(raw["opens"], "window.opens"),
                    closes=_time(raw["closes"], "window.closes"),
                    phase=CalendarWindowPhase(raw["phase"]),
                )
            )
        except (TypeError, ValueError) as exc:
            raise CalendarSourceArtifactIntegrityError("invalid declared session window") from exc
    return tuple(parsed)


def _supersedes(value: object) -> tuple[str, ...]:
    if type(value) is not list or any(type(item) is not str for item in value):
        raise CalendarSourceArtifactIntegrityError(
            "supersedes_event_ids must be a JSON string array"
        )
    return tuple(value)


def _parse_event(
    value: object,
    *,
    source_sha256: str,
    claimed_document_id: str,
) -> CalendarEvent:
    if type(value) is not dict or type(value.get("event_type")) is not str:
        raise CalendarSourceArtifactIntegrityError("calendar event must name its type")
    try:
        event_type = CalendarEventType(value["event_type"])
    except ValueError as exc:
        raise CalendarSourceArtifactIntegrityError("unsupported calendar event type") from exc
    raw = _expect_object(value, _EVENT_KEYS[event_type], "calendar event")
    common = {
        "event_type": event_type,
        "source_sha256": source_sha256,
        "claimed_document_id": claimed_document_id,
        "source_locator": _source_locator(raw["source_locator"]),
        "reason": _text(raw["reason"], "event.reason"),
        "supersedes_event_ids": _supersedes(raw["supersedes_event_ids"]),
    }
    try:
        if event_type is CalendarEventType.BASE_WEEKLY_SCHEDULE:
            weekdays = raw["weekdays"]
            if type(weekdays) is not list:
                raise CalendarSourceArtifactIntegrityError(
                    "base schedule weekdays must be a JSON array"
                )
            return CalendarEvent(
                **common,
                effective_from=_date(raw["effective_from"], "effective_from"),
                effective_to_exclusive=_date(
                    raw["effective_to_exclusive"],
                    "effective_to_exclusive",
                ),
                weekdays=tuple(CalendarWeekday(value) for value in weekdays),
                windows=_windows(raw["windows"]),
            )
        if event_type is CalendarEventType.DATE_CLOSED:
            return CalendarEvent(
                **common,
                effective_date=_date(raw["date"], "event.date"),
                day_kind=CalendarDeclaredDayKind(raw["day_kind"]),
            )
        if event_type is CalendarEventType.DATE_SESSION_REPLACED:
            return CalendarEvent(
                **common,
                effective_date=_date(raw["date"], "event.date"),
                day_kind=CalendarDeclaredDayKind(raw["day_kind"]),
                windows=_windows(raw["windows"]),
            )
        return CalendarEvent(
            **common,
            effective_date=_date(raw["date"], "event.date"),
            windows=_windows(raw["windows"]),
        )
    except CalendarSourceArtifactIntegrityError:
        raise
    except (TypeError, ValueError) as exc:
        raise CalendarSourceArtifactIntegrityError("invalid calendar event") from exc


def _validate_pdf(source_bytes: bytes, source_filename: str) -> None:
    if not source_filename.lower().endswith(".pdf"):
        raise CalendarSourceArtifactIntegrityError(
            "application/pdf calendar sources require a .pdf filename"
        )
    if not source_bytes.startswith(b"%PDF-") or b"%%EOF" not in source_bytes[-2_048:]:
        raise CalendarSourceArtifactIntegrityError(
            "calendar source does not satisfy the pinned PDF envelope"
        )


class CalendarDeclarationParser:
    """Strict parser for a manual declaration bound to exact official-source bytes."""

    maximum_source_bytes = MAXIMUM_CALENDAR_SOURCE_BYTES
    maximum_declaration_bytes = MAXIMUM_CALENDAR_DECLARATION_BYTES

    def parse_bytes(
        self,
        declaration_bytes: bytes,
        *,
        source_bytes: bytes,
        source_filename: str,
        declaration_filename: str,
    ) -> ParsedCalendarDeclaration:
        if type(source_bytes) is not bytes or not source_bytes:
            raise CalendarSourceArtifactIntegrityError("calendar source must be non-empty bytes")
        if len(source_bytes) > self.maximum_source_bytes:
            raise CalendarSourceArtifactIntegrityError("calendar source exceeds the size limit")
        if type(declaration_bytes) is not bytes or not declaration_bytes:
            raise CalendarSourceArtifactIntegrityError(
                "calendar declaration must be non-empty bytes"
            )
        if len(declaration_bytes) > self.maximum_declaration_bytes:
            raise CalendarSourceArtifactIntegrityError(
                "calendar declaration exceeds the size limit"
            )
        if not isinstance(source_filename, str) or not source_filename:
            raise CalendarSourceArtifactIntegrityError("source filename is required")
        if (
            not isinstance(declaration_filename, str)
            or not declaration_filename.lower().endswith(".json")
        ):
            raise CalendarSourceArtifactIntegrityError(
                "calendar declaration requires a .json filename"
            )

        root = _expect_object(
            decode_strict_json(declaration_bytes, label="calendar declaration"),
            _ROOT_KEYS,
            "calendar declaration",
        )
        if root["schema_version"] != CALENDAR_DECLARATION_SCHEMA_VERSION:
            raise CalendarSourceArtifactIntegrityError(
                "unsupported calendar declaration schema"
            )
        if (root["exchange"], root["segment"], root["claimed_authority"]) != (
            "NSE",
            "CM",
            "NSE",
        ):
            raise CalendarSourceArtifactIntegrityError(
                "calendar declaration must be pinned to NSE CM"
            )
        if root["source_filename"] != source_filename:
            raise CalendarSourceArtifactIntegrityError(
                "calendar declaration is bound to another source filename"
            )
        if root["source_media_type"] != "application/pdf":
            raise CalendarSourceArtifactIntegrityError(
                "calendar source foundation currently accepts only PDF sources"
            )
        if type(root["source_byte_count"]) is not int:
            raise CalendarSourceArtifactIntegrityError(
                "source_byte_count must be an integer"
            )
        source_sha256 = _sha256(source_bytes)
        if (
            root["source_byte_count"] != len(source_bytes)
            or root["source_sha256"] != source_sha256
        ):
            raise CalendarSourceArtifactIntegrityError(
                "calendar declaration does not bind the exact source bytes"
            )
        _validate_pdf(source_bytes, source_filename)

        claimed_document_id = _text(
            root["claimed_document_id"],
            "claimed_document_id",
        )
        events_value = root["events"]
        if (
            type(events_value) is not list
            or not events_value
            or len(events_value) > MAXIMUM_CALENDAR_EVENTS
        ):
            raise CalendarSourceArtifactIntegrityError(
                "calendar declaration events must be a bounded non-empty array"
            )
        parsed_events = tuple(
            sorted(
                (
                    _parse_event(
                        event,
                        source_sha256=source_sha256,
                        claimed_document_id=claimed_document_id,
                    )
                    for event in events_value
                ),
                key=lambda event: event.event_id,
            )
        )
        claimed_source_url = root["claimed_source_url"]
        if claimed_source_url is not None and type(claimed_source_url) is not str:
            raise CalendarSourceArtifactIntegrityError(
                "claimed_source_url must be text or null"
            )
        try:
            return ParsedCalendarDeclaration(
                exchange="NSE",
                segment="CM",
                claimed_authority="NSE",
                claimed_document_id=claimed_document_id,
                claimed_issue_date=_date(
                    root["claimed_issue_date"],
                    "claimed_issue_date",
                ),
                claimed_source_url=claimed_source_url,
                source_filename=source_filename,
                source_media_type="application/pdf",
                source_byte_count=len(source_bytes),
                source_sha256=source_sha256,
                events=parsed_events,
            )
        except (TypeError, ValueError) as exc:
            raise CalendarSourceArtifactIntegrityError(
                "calendar declaration violates the pinned contract"
            ) from exc

