from __future__ import annotations

import json

from .models import (
    CALENDAR_EVENT_SCHEMA_VERSION,
    CALENDAR_NORMALIZED_CODEC_VERSION,
    CalendarEvent,
    CalendarEventType,
    ParsedCalendarDeclaration,
)


def _locator(event: CalendarEvent) -> dict[str, object]:
    return {
        "page": event.source_locator.page,
        "record": event.source_locator.record,
        "section": event.source_locator.section,
    }


def _windows(event: CalendarEvent) -> list[dict[str, str]]:
    return [
        {
            "closes": window.closes.isoformat(timespec="seconds"),
            "opens": window.opens.isoformat(timespec="seconds"),
            "phase": window.phase.value,
        }
        for window in event.windows
    ]


def _event(event: CalendarEvent) -> dict[str, object]:
    value: dict[str, object] = {
        "event_id": event.event_id,
        "event_schema_version": event.schema_version,
        "event_type": event.event_type.value,
        "reason": event.reason,
        "source_locator": _locator(event),
        "supersedes_event_ids": list(event.supersedes_event_ids),
    }
    if event.event_type is CalendarEventType.BASE_WEEKLY_SCHEDULE:
        assert event.effective_from is not None
        assert event.effective_to_exclusive is not None
        value.update(
            {
                "effective_from": event.effective_from.isoformat(),
                "effective_to_exclusive": event.effective_to_exclusive.isoformat(),
                "weekdays": [weekday.value for weekday in event.weekdays],
                "windows": _windows(event),
            }
        )
    elif event.event_type is CalendarEventType.DATE_CLOSED:
        assert event.effective_date is not None
        assert event.day_kind is not None
        value.update(
            {
                "date": event.effective_date.isoformat(),
                "day_kind": event.day_kind.value,
            }
        )
    elif event.event_type is CalendarEventType.DATE_SESSION_REPLACED:
        assert event.effective_date is not None
        assert event.day_kind is not None
        value.update(
            {
                "date": event.effective_date.isoformat(),
                "day_kind": event.day_kind.value,
                "windows": _windows(event),
            }
        )
    else:
        assert event.effective_date is not None
        value.update(
            {
                "date": event.effective_date.isoformat(),
                "windows": _windows(event),
            }
        )
    return value


def encode_calendar_declaration(declaration: ParsedCalendarDeclaration) -> bytes:
    if type(declaration) is not ParsedCalendarDeclaration:
        raise TypeError("calendar codec requires an exact parsed declaration")
    declaration.verify_content_identity()
    value = {
        "claimed_authority": declaration.claimed_authority,
        "claimed_document_id": declaration.claimed_document_id,
        "claimed_issue_date": declaration.claimed_issue_date.isoformat(),
        "claimed_source_url": declaration.claimed_source_url,
        "codec_version": CALENDAR_NORMALIZED_CODEC_VERSION,
        "declaration_schema_version": declaration.schema_version,
        "event_count": len(declaration.events),
        "event_schema_version": CALENDAR_EVENT_SCHEMA_VERSION,
        "events": [_event(event) for event in declaration.events],
        "exchange": declaration.exchange,
        "segment": declaration.segment,
        "source": {
            "byte_count": declaration.source_byte_count,
            "filename": declaration.source_filename,
            "media_type": declaration.source_media_type,
            "sha256": declaration.source_sha256,
        },
    }
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")

