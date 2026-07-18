from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone
from enum import Enum
from pathlib import Path, PurePath

from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference_data.models import AcquisitionMode


CALENDAR_SOURCE_DATASET = "nse-cm-calendar-source"
CALENDAR_SOURCE_ARTIFACT_SCHEMA_VERSION = "calendar-source-artifact/v1"
CALENDAR_DECLARATION_SCHEMA_VERSION = "nse-cm-calendar-declaration/v1"
CALENDAR_EVENT_SCHEMA_VERSION = "nse-cm-calendar-event/v1"
CALENDAR_DECLARATION_PARSER_VERSION = "nse-cm-calendar-declaration-parser/v1"
CALENDAR_NORMALIZED_CODEC_VERSION = "nse-cm-calendar-declaration-json/v1"
CALENDAR_EVENT_POLICY_VERSION = "nse-cm-calendar-events-collection-only/v1"
CALENDAR_PUBLICATION_TIME_STATUS = "LOCALLY_OBSERVED_ONLY"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_DOCUMENT_ID = re.compile(r"[A-Z0-9][A-Z0-9._-]{2,127}\Z")


class CalendarSourceArtifactError(RuntimeError):
    pass


class CalendarSourceArtifactIntegrityError(CalendarSourceArtifactError):
    pass


class CalendarSourceArtifactConflict(CalendarSourceArtifactError):
    pass


class CalendarSourceArtifactNotFound(CalendarSourceArtifactError):
    pass


class CalendarEventType(str, Enum):
    BASE_WEEKLY_SCHEDULE = "BASE_WEEKLY_SCHEDULE"
    DATE_CLOSED = "DATE_CLOSED"
    DATE_SESSION_REPLACED = "DATE_SESSION_REPLACED"
    NON_EXECUTABLE_ACTIVITY = "NON_EXECUTABLE_ACTIVITY"


class CalendarDeclaredDayKind(str, Enum):
    SPECIAL = "SPECIAL"
    HOLIDAY = "HOLIDAY"
    WEEKEND = "WEEKEND"
    UNSCHEDULED_CLOSURE = "UNSCHEDULED_CLOSURE"


class CalendarWindowPhase(str, Enum):
    LIVE_CONTINUOUS = "LIVE_CONTINUOUS"
    PRE_OPEN = "PRE_OPEN"
    CALL_AUCTION = "CALL_AUCTION"
    CLOSING_AUCTION = "CLOSING_AUCTION"
    MOCK_TEST = "MOCK_TEST"


class CalendarWeekday(str, Enum):
    MON = "MON"
    TUE = "TUE"
    WED = "WED"
    THU = "THU"
    FRI = "FRI"
    SAT = "SAT"
    SUN = "SUN"


_WEEKDAY_ORDER = tuple(CalendarWeekday)


def _require_sha256(value: str, field_name: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a full lowercase SHA-256")


def _require_safe_basename(value: str, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or PurePath(value).name != value
        or "/" in value
        or "\\" in value
    ):
        raise ValueError(f"{field_name} must be a safe basename")


def _require_canonical_text(
    value: str,
    field_name: str,
    *,
    maximum_length: int,
) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > maximum_length
        or any(ord(character) < 32 for character in value)
    ):
        raise ValueError(f"{field_name} must be canonical non-empty text")


def _utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class CalendarSourceLocator:
    page: int | None
    section: str
    record: str | None

    def __post_init__(self) -> None:
        if self.page is not None and (
            type(self.page) is not int or self.page <= 0 or self.page > 100_000
        ):
            raise ValueError("source locator page must be a positive integer or null")
        _require_canonical_text(
            self.section,
            "source locator section",
            maximum_length=300,
        )
        if self.record is not None:
            _require_canonical_text(
                self.record,
                "source locator record",
                maximum_length=500,
            )


@dataclass(frozen=True, slots=True)
class DeclaredSessionWindow:
    opens: time
    closes: time
    phase: CalendarWindowPhase

    def __post_init__(self) -> None:
        if type(self.opens) is not time or type(self.closes) is not time:
            raise TypeError("declared window times must be exact time values")
        if self.opens.tzinfo is not None or self.closes.tzinfo is not None:
            raise ValueError("declared window times must be naive India-local wall times")
        if self.opens.microsecond or self.closes.microsecond:
            raise ValueError("declared window times cannot contain microseconds")
        if self.opens >= self.closes:
            raise ValueError("declared window must satisfy opens < closes")
        if type(self.phase) is not CalendarWindowPhase:
            raise TypeError("declared window phase must be an exact CalendarWindowPhase")

    @property
    def executable(self) -> bool:
        return self.phase is CalendarWindowPhase.LIVE_CONTINUOUS


@dataclass(frozen=True, slots=True)
class CalendarEvent:
    event_type: CalendarEventType
    source_sha256: str
    claimed_document_id: str
    source_locator: CalendarSourceLocator
    reason: str
    effective_date: date | None = None
    effective_from: date | None = None
    effective_to_exclusive: date | None = None
    weekdays: tuple[CalendarWeekday, ...] = ()
    day_kind: CalendarDeclaredDayKind | None = None
    windows: tuple[DeclaredSessionWindow, ...] = ()
    supersedes_event_ids: tuple[str, ...] = ()
    schema_version: str = CALENDAR_EVENT_SCHEMA_VERSION
    event_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.event_type) is not CalendarEventType:
            raise TypeError("event_type must be an exact CalendarEventType")
        _require_sha256(self.source_sha256, "event source_sha256")
        if (
            not isinstance(self.claimed_document_id, str)
            or _DOCUMENT_ID.fullmatch(self.claimed_document_id) is None
        ):
            raise ValueError("claimed_document_id must be canonical uppercase text")
        if type(self.source_locator) is not CalendarSourceLocator:
            raise TypeError("source_locator must be an exact CalendarSourceLocator")
        _require_canonical_text(self.reason, "event reason", maximum_length=500)
        if type(self.weekdays) is not tuple or any(
            type(value) is not CalendarWeekday for value in self.weekdays
        ):
            raise TypeError("weekdays must be an immutable exact weekday tuple")
        expected_weekdays = tuple(
            value for value in _WEEKDAY_ORDER if value in self.weekdays
        )
        if self.weekdays != expected_weekdays:
            raise ValueError("weekdays must be unique and in canonical Monday-Sunday order")
        if type(self.windows) is not tuple or any(
            type(value) is not DeclaredSessionWindow for value in self.windows
        ):
            raise TypeError("windows must be an immutable exact window tuple")
        previous: DeclaredSessionWindow | None = None
        for window in self.windows:
            if previous is not None and window.opens < previous.closes:
                raise ValueError("declared windows must be sorted and non-overlapping")
            previous = window
        if type(self.supersedes_event_ids) is not tuple:
            raise TypeError("supersedes_event_ids must be an immutable tuple")
        for value in self.supersedes_event_ids:
            _require_sha256(value, "supersedes_event_id")
        if tuple(sorted(set(self.supersedes_event_ids))) != self.supersedes_event_ids:
            raise ValueError("supersedes_event_ids must be unique and sorted")
        if self.day_kind is not None and type(self.day_kind) is not CalendarDeclaredDayKind:
            raise TypeError("day_kind must be an exact CalendarDeclaredDayKind or null")
        if self.schema_version != CALENDAR_EVENT_SCHEMA_VERSION:
            raise ValueError("unsupported calendar-event schema version")

        if self.event_type is CalendarEventType.BASE_WEEKLY_SCHEDULE:
            if (
                self.effective_date is not None
                or type(self.effective_from) is not date
                or type(self.effective_to_exclusive) is not date
                or self.effective_to_exclusive <= self.effective_from
                or not self.weekdays
                or self.day_kind is not None
                or not self.windows
                or not any(window.executable for window in self.windows)
                or self.supersedes_event_ids
            ):
                raise ValueError("invalid BASE_WEEKLY_SCHEDULE shape")
        elif self.event_type is CalendarEventType.DATE_CLOSED:
            if (
                type(self.effective_date) is not date
                or self.effective_from is not None
                or self.effective_to_exclusive is not None
                or self.weekdays
                or self.day_kind
                not in {
                    CalendarDeclaredDayKind.HOLIDAY,
                    CalendarDeclaredDayKind.WEEKEND,
                    CalendarDeclaredDayKind.UNSCHEDULED_CLOSURE,
                }
                or self.windows
                or not self.supersedes_event_ids
            ):
                raise ValueError("invalid DATE_CLOSED shape")
        elif self.event_type is CalendarEventType.DATE_SESSION_REPLACED:
            if (
                type(self.effective_date) is not date
                or self.effective_from is not None
                or self.effective_to_exclusive is not None
                or self.weekdays
                or self.day_kind is not CalendarDeclaredDayKind.SPECIAL
                or not self.windows
                or not any(window.executable for window in self.windows)
                or not self.supersedes_event_ids
            ):
                raise ValueError("invalid DATE_SESSION_REPLACED shape")
        elif self.event_type is CalendarEventType.NON_EXECUTABLE_ACTIVITY:
            if (
                type(self.effective_date) is not date
                or self.effective_from is not None
                or self.effective_to_exclusive is not None
                or self.weekdays
                or self.day_kind is not None
                or not self.windows
                or any(window.executable for window in self.windows)
            ):
                raise ValueError("invalid NON_EXECUTABLE_ACTIVITY shape")

        object.__setattr__(self, "event_id", self._calculated_event_id())

    def _calculated_event_id(self) -> str:
        return content_id(
            {
                "schema_version": self.schema_version,
                "event_type": self.event_type,
                "source_sha256": self.source_sha256,
                "claimed_document_id": self.claimed_document_id,
                "source_locator": self.source_locator,
                "reason": self.reason,
                "effective_date": self.effective_date,
                "effective_from": self.effective_from,
                "effective_to_exclusive": self.effective_to_exclusive,
                "weekdays": self.weekdays,
                "day_kind": self.day_kind,
                "windows": self.windows,
                "supersedes_event_ids": self.supersedes_event_ids,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if type(self.source_locator) is not CalendarSourceLocator or any(
            type(window) is not DeclaredSessionWindow for window in self.windows
        ):
            raise CalendarSourceArtifactIntegrityError(
                "calendar event graph contains an unaudited type"
            )
        if self.event_id != self._calculated_event_id():
            raise CalendarSourceArtifactIntegrityError(
                "calendar event content identity verification failed"
            )


@dataclass(frozen=True, slots=True)
class ParsedCalendarDeclaration:
    exchange: str
    segment: str
    claimed_authority: str
    claimed_document_id: str
    claimed_issue_date: date
    claimed_source_url: str | None
    source_filename: str
    source_media_type: str
    source_byte_count: int
    source_sha256: str
    events: tuple[CalendarEvent, ...]
    schema_version: str = CALENDAR_DECLARATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (self.exchange, self.segment, self.claimed_authority) != (
            "NSE",
            "CM",
            "NSE",
        ):
            raise ValueError("calendar declaration must be pinned to NSE CM")
        if (
            not isinstance(self.claimed_document_id, str)
            or _DOCUMENT_ID.fullmatch(self.claimed_document_id) is None
        ):
            raise ValueError("claimed_document_id must be canonical uppercase text")
        if type(self.claimed_issue_date) is not date:
            raise TypeError("claimed_issue_date must be a date")
        if self.claimed_source_url is not None and (
            not isinstance(self.claimed_source_url, str)
            or not self.claimed_source_url.startswith("https://")
            or self.claimed_source_url != self.claimed_source_url.strip()
            or len(self.claimed_source_url) > 2_048
        ):
            raise ValueError("claimed_source_url must be null or a canonical HTTPS URL")
        _require_safe_basename(self.source_filename, "source_filename")
        if self.source_media_type != "application/pdf":
            raise ValueError("calendar source foundation currently accepts only PDF sources")
        if type(self.source_byte_count) is not int or self.source_byte_count <= 0:
            raise ValueError("source_byte_count must be a positive integer")
        _require_sha256(self.source_sha256, "source_sha256")
        if type(self.events) is not tuple or not self.events or any(
            type(event) is not CalendarEvent for event in self.events
        ):
            raise TypeError("events must be a non-empty immutable exact tuple")
        if tuple(sorted(self.events, key=lambda event: event.event_id)) != self.events:
            raise ValueError("calendar events must be sorted by content-derived event ID")
        if len({event.event_id for event in self.events}) != len(self.events):
            raise ValueError("calendar declaration contains duplicate events")
        for event in self.events:
            event.verify_content_identity()
            if (
                event.source_sha256 != self.source_sha256
                or event.claimed_document_id != self.claimed_document_id
            ):
                raise ValueError("calendar event lineage disagrees with its declaration")
        if self.schema_version != CALENDAR_DECLARATION_SCHEMA_VERSION:
            raise ValueError("unsupported calendar-declaration schema version")

    def verify_content_identity(self) -> None:
        if any(type(event) is not CalendarEvent for event in self.events):
            raise CalendarSourceArtifactIntegrityError(
                "calendar declaration graph contains an unaudited type"
            )
        for event in self.events:
            event.verify_content_identity()

    @property
    def event_ids(self) -> tuple[str, ...]:
        return tuple(event.event_id for event in self.events)


@dataclass(frozen=True, slots=True)
class CalendarSourceArtifactManifest:
    schema_version: str
    manifest_id: str
    artifact_id: str
    dataset: str
    exchange: str
    segment: str
    claimed_authority: str
    acquisition_mode: AcquisitionMode
    readiness: ReferenceReadiness
    actionable: bool
    publication_time_status: str
    first_seen_at: datetime
    validated_at: datetime
    original_source_filename: str
    original_declaration_filename: str
    claimed_document_id: str
    claimed_issue_date: date
    claimed_source_url: str | None
    source_media_type: str
    source_byte_count: int
    source_sha256: str
    declaration_byte_count: int
    declaration_sha256: str
    normalized_byte_count: int
    normalized_sha256: str
    event_count: int
    event_ids: tuple[str, ...]
    parser_version: str
    declaration_schema_version: str
    event_schema_version: str
    event_policy_version: str
    normalized_codec_version: str
    raw_filename: str
    declaration_filename: str
    normalized_filename: str

    def __post_init__(self) -> None:
        for value, name in (
            (self.manifest_id, "manifest_id"),
            (self.artifact_id, "artifact_id"),
            (self.source_sha256, "source_sha256"),
            (self.declaration_sha256, "declaration_sha256"),
            (self.normalized_sha256, "normalized_sha256"),
        ):
            _require_sha256(value, name)
        if (
            self.schema_version != CALENDAR_SOURCE_ARTIFACT_SCHEMA_VERSION
            or self.dataset != CALENDAR_SOURCE_DATASET
            or (self.exchange, self.segment, self.claimed_authority)
            != ("NSE", "CM", "NSE")
        ):
            raise ValueError("calendar source manifest scope or schema is unsupported")
        if self.acquisition_mode is not AcquisitionMode.UNVERIFIED_MANUAL_FILE:
            raise ValueError("calendar source acquisition must remain unverified manual")
        if (
            self.readiness is not ReferenceReadiness.COLLECTION_ONLY
            or type(self.actionable) is not bool
            or self.actionable
        ):
            raise ValueError("calendar source artifacts must remain collection-only")
        if self.publication_time_status != CALENDAR_PUBLICATION_TIME_STATUS:
            raise ValueError("calendar source publication-time status is unsupported")
        object.__setattr__(self, "first_seen_at", _utc(self.first_seen_at, "first_seen_at"))
        object.__setattr__(self, "validated_at", _utc(self.validated_at, "validated_at"))
        if self.validated_at < self.first_seen_at:
            raise ValueError("calendar source validation cannot precede first observation")
        _require_safe_basename(
            self.original_source_filename,
            "original_source_filename",
        )
        _require_safe_basename(
            self.original_declaration_filename,
            "original_declaration_filename",
        )
        if (
            not isinstance(self.claimed_document_id, str)
            or _DOCUMENT_ID.fullmatch(self.claimed_document_id) is None
        ):
            raise ValueError("claimed_document_id must be canonical uppercase text")
        if type(self.claimed_issue_date) is not date:
            raise TypeError("claimed_issue_date must be a date")
        if self.claimed_source_url is not None and (
            not isinstance(self.claimed_source_url, str)
            or not self.claimed_source_url.startswith("https://")
            or self.claimed_source_url != self.claimed_source_url.strip()
            or len(self.claimed_source_url) > 2_048
        ):
            raise ValueError("claimed_source_url must be null or HTTPS")
        if self.source_media_type != "application/pdf":
            raise ValueError("calendar source media type must be application/pdf")
        for value, name in (
            (self.source_byte_count, "source_byte_count"),
            (self.declaration_byte_count, "declaration_byte_count"),
            (self.normalized_byte_count, "normalized_byte_count"),
            (self.event_count, "event_count"),
        ):
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if type(self.event_ids) is not tuple or len(self.event_ids) != self.event_count:
            raise ValueError("event_ids must exactly cover event_count")
        for event_id in self.event_ids:
            _require_sha256(event_id, "event_id")
        if tuple(sorted(set(self.event_ids))) != self.event_ids:
            raise ValueError("manifest event IDs must be unique and sorted")
        if (
            self.parser_version != CALENDAR_DECLARATION_PARSER_VERSION
            or self.declaration_schema_version != CALENDAR_DECLARATION_SCHEMA_VERSION
            or self.event_schema_version != CALENDAR_EVENT_SCHEMA_VERSION
            or self.event_policy_version != CALENDAR_EVENT_POLICY_VERSION
            or self.normalized_codec_version != CALENDAR_NORMALIZED_CODEC_VERSION
        ):
            raise ValueError("calendar source manifest contract version is unsupported")
        for value, name in (
            (self.raw_filename, "raw_filename"),
            (self.declaration_filename, "declaration_filename"),
            (self.normalized_filename, "normalized_filename"),
        ):
            _require_safe_basename(value, name)

    @property
    def knowledge_time(self) -> datetime:
        """Earliest locally supportable time for the validated declaration."""

        return self.validated_at


@dataclass(frozen=True, slots=True)
class StoredCalendarSourceArtifact:
    path: Path
    manifest: CalendarSourceArtifactManifest
    parsed: ParsedCalendarDeclaration
    source_bytes: bytes
    declaration_bytes: bytes
    normalized_bytes: bytes

    def __post_init__(self) -> None:
        if not isinstance(self.path, Path):
            raise TypeError("stored calendar source path must be a Path")
        if type(self.manifest) is not CalendarSourceArtifactManifest:
            raise TypeError("stored calendar source manifest must be exact")
        if type(self.parsed) is not ParsedCalendarDeclaration:
            raise TypeError("stored calendar declaration must be exact")
        if any(
            type(value) is not bytes
            for value in (
                self.source_bytes,
                self.declaration_bytes,
                self.normalized_bytes,
            )
        ):
            raise TypeError("stored calendar source payloads must be exact bytes")

    @property
    def knowledge_time(self) -> datetime:
        return self.manifest.knowledge_time
