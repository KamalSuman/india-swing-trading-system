from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from enum import Enum

from india_swing.identity import content_id

from .models import ExternalRecordRef, ReferenceReadiness


INDIA_STANDARD_TIME = timezone(timedelta(hours=5, minutes=30))
CALENDAR_SCHEMA_VERSION = "reference-calendar/v4"


class CalendarIntegrityError(ValueError):
    pass


class CalendarCoverageError(CalendarIntegrityError):
    pass


class NotTradingSessionError(CalendarIntegrityError):
    pass


class OutsideSessionWindowError(CalendarIntegrityError):
    pass


class CalendarDayKind(str, Enum):
    REGULAR = "REGULAR"
    SPECIAL = "SPECIAL"
    HOLIDAY = "HOLIDAY"
    WEEKEND = "WEEKEND"
    UNSCHEDULED_CLOSURE = "UNSCHEDULED_CLOSURE"


class SessionWindowPhase(str, Enum):
    LIVE_CONTINUOUS = "LIVE_CONTINUOUS"
    PRE_OPEN = "PRE_OPEN"
    CALL_AUCTION = "CALL_AUCTION"
    CLOSING_AUCTION = "CLOSING_AUCTION"
    MOCK_TEST = "MOCK_TEST"


_TRADING_KINDS = frozenset((CalendarDayKind.REGULAR, CalendarDayKind.SPECIAL))


def _require_date(value: date, field_name: str) -> None:
    if type(value) is not date:
        raise TypeError(f"{field_name} must be a date")


def _require_aware(value: datetime, field_name: str) -> None:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


def _require_ist_session_time(value: datetime, day: date, field_name: str) -> None:
    _require_aware(value, field_name)
    if value.utcoffset() != INDIA_STANDARD_TIME.utcoffset(None):
        raise ValueError(f"{field_name} must use the Asia/Kolkata offset")
    if value.astimezone(INDIA_STANDARD_TIME).date() != day:
        raise ValueError(f"{field_name} must belong to the calendar day in India")


@dataclass(frozen=True, slots=True)
class SessionWindow:
    opens_at: datetime
    closes_at: datetime
    phase: SessionWindowPhase

    def __post_init__(self) -> None:
        _require_aware(self.opens_at, "session_window.opens_at")
        _require_aware(self.closes_at, "session_window.closes_at")
        if not isinstance(self.phase, SessionWindowPhase):
            raise TypeError("session_window.phase must be a SessionWindowPhase")
        if self.opens_at.utcoffset() != INDIA_STANDARD_TIME.utcoffset(None):
            raise ValueError("session_window.opens_at must use the Asia/Kolkata offset")
        if self.closes_at.utcoffset() != INDIA_STANDARD_TIME.utcoffset(None):
            raise ValueError("session_window.closes_at must use the Asia/Kolkata offset")
        open_day = self.opens_at.astimezone(INDIA_STANDARD_TIME).date()
        close_day = self.closes_at.astimezone(INDIA_STANDARD_TIME).date()
        if open_day != close_day:
            raise CalendarIntegrityError(
                "session window open and close must belong to the same India date"
            )
        if self.opens_at >= self.closes_at:
            raise CalendarIntegrityError("session window must satisfy open < close")

    @property
    def is_executable(self) -> bool:
        return self.phase is SessionWindowPhase.LIVE_CONTINUOUS


@dataclass(frozen=True, slots=True)
class CalendarDay:
    day: date
    kind: CalendarDayKind
    reference: ExternalRecordRef
    session_windows: tuple[SessionWindow, ...] = ()
    # Exchange schedule evidence and provider/report finality are different
    # facts.  Official holiday and session circulars can establish the former
    # without establishing when a particular EOD dataset became final.  The
    # optional value remains for synthetic decision fixtures and a future,
    # separately sourced finality overlay; calendar materializers must not
    # invent it from a session close.
    data_ready_at: datetime | None = None

    def __post_init__(self) -> None:
        _require_date(self.day, "calendar_day.day")
        if not isinstance(self.kind, CalendarDayKind):
            raise TypeError("calendar_day.kind must be a CalendarDayKind")
        if type(self.reference) is not ExternalRecordRef:
            raise TypeError("calendar_day.reference must be an exact ExternalRecordRef")
        if self.reference.event_time.astimezone(INDIA_STANDARD_TIME).date() != self.day:
            raise CalendarIntegrityError(
                "calendar reference event_time must belong to its exchange date"
            )

        if type(self.session_windows) is not tuple:
            raise TypeError("calendar_day.session_windows must be an immutable tuple")
        for window in self.session_windows:
            if type(window) is not SessionWindow:
                raise TypeError(
                    "calendar_day.session_windows must contain exact SessionWindow values"
                )

        if self.is_session:
            if not self.session_windows:
                raise CalendarIntegrityError(
                    "trading sessions require at least one window"
                )
            if not any(window.is_executable for window in self.session_windows):
                raise CalendarIntegrityError(
                    "trading sessions require an executable live-continuous window"
                )
            previous: SessionWindow | None = None
            for window in self.session_windows:
                _require_ist_session_time(
                    window.opens_at,
                    self.day,
                    "calendar_day.session_windows.opens_at",
                )
                _require_ist_session_time(
                    window.closes_at,
                    self.day,
                    "calendar_day.session_windows.closes_at",
                )
                if previous is not None and window.opens_at < previous.closes_at:
                    raise CalendarIntegrityError(
                        "session windows must be sorted and non-overlapping"
                    )
                previous = window
            if self.data_ready_at is not None:
                _require_ist_session_time(
                    self.data_ready_at,
                    self.day,
                    "calendar_day.data_ready_at",
                )
                if self.session_windows[-1].closes_at >= self.data_ready_at:
                    raise CalendarIntegrityError(
                        "data-ready time must be after the final session-window close"
                    )
        elif self.session_windows or self.data_ready_at is not None:
            raise CalendarIntegrityError(
                "closed calendar dates cannot carry session windows or a data-ready time"
            )

    @property
    def is_session(self) -> bool:
        return self.kind in _TRADING_KINDS

    def session_window_containing(
        self,
        value: datetime,
        *,
        include_close: bool = False,
        executable_only: bool = True,
    ) -> SessionWindow | None:
        """Return the real trading window containing ``value``, if any."""

        if not self.is_session:
            return None
        _require_ist_session_time(value, self.day, "session-window lookup time")
        for window in self.session_windows:
            if executable_only and not window.is_executable:
                continue
            if window.opens_at <= value and (
                value <= window.closes_at if include_close else value < window.closes_at
            ):
                return window
        return None

    def require_same_session_window(
        self,
        entry_at: datetime,
        expiry_at: datetime,
    ) -> SessionWindow:
        """Require ``open <= entry < expiry <= close`` in one real window."""

        if not self.is_session:
            raise NotTradingSessionError(f"{self.day.isoformat()} is not a trading session")
        _require_ist_session_time(entry_at, self.day, "entry_at")
        _require_ist_session_time(expiry_at, self.day, "expiry_at")
        if entry_at >= expiry_at:
            raise OutsideSessionWindowError("entry_at must precede expiry_at")
        entry_window = self.session_window_containing(entry_at)
        if entry_window is not None and expiry_at <= entry_window.closes_at:
            return entry_window
        raise OutsideSessionWindowError(
            "entry and expiry must lie inside the same session window"
        )


@dataclass(frozen=True, slots=True)
class CalendarSnapshot:
    exchange: str
    segment: str
    cutoff: datetime
    coverage_start: date
    coverage_end: date
    days: tuple[CalendarDay, ...]
    source_snapshot_ids: tuple[str, ...]
    readiness: ReferenceReadiness
    schema_version: str = CALENDAR_SCHEMA_VERSION
    snapshot_id: str = field(init=False)
    version: str = field(init=False)

    @classmethod
    def create(
        cls,
        *,
        exchange: str,
        segment: str,
        cutoff: datetime,
        coverage_start: date,
        coverage_end: date,
        days: tuple[CalendarDay, ...],
        source_snapshot_ids: tuple[str, ...],
        readiness: ReferenceReadiness,
        schema_version: str = CALENDAR_SCHEMA_VERSION,
    ) -> CalendarSnapshot:
        return cls(
            exchange=exchange,
            segment=segment,
            cutoff=cutoff,
            coverage_start=coverage_start,
            coverage_end=coverage_end,
            days=days,
            source_snapshot_ids=source_snapshot_ids,
            readiness=readiness,
            schema_version=schema_version,
        )

    def __post_init__(self) -> None:
        if not isinstance(self.exchange, str) or not self.exchange.strip():
            raise ValueError("calendar exchange is required")
        if self.exchange != self.exchange.strip().upper():
            raise ValueError("calendar exchange must be normalized uppercase text")
        if not isinstance(self.segment, str) or not self.segment.strip():
            raise ValueError("calendar segment is required")
        if self.segment != self.segment.strip().upper():
            raise ValueError("calendar segment must be normalized uppercase text")
        _require_aware(self.cutoff, "calendar.cutoff")
        object.__setattr__(self, "cutoff", self.cutoff.astimezone(timezone.utc))
        _require_date(self.coverage_start, "calendar.coverage_start")
        _require_date(self.coverage_end, "calendar.coverage_end")
        if self.coverage_end < self.coverage_start:
            raise CalendarCoverageError("calendar coverage_end precedes coverage_start")
        if not isinstance(self.readiness, ReferenceReadiness):
            raise TypeError("calendar readiness must be a ReferenceReadiness")
        if self.readiness is ReferenceReadiness.POINT_IN_TIME_VERIFIED:
            raise CalendarIntegrityError(
                "point-in-time calendar verification is unavailable until the official artifact importer exists"
            )
        if self.schema_version != CALENDAR_SCHEMA_VERSION:
            raise CalendarIntegrityError("unsupported calendar schema version")
        if type(self.days) is not tuple:
            raise TypeError("calendar days must be an immutable tuple")
        if type(self.source_snapshot_ids) is not tuple:
            raise TypeError("calendar source_snapshot_ids must be an immutable tuple")

        expected_count = (self.coverage_end - self.coverage_start).days + 1
        if len(self.days) != expected_count:
            raise CalendarCoverageError(
                "calendar must contain one explicit CalendarDay for every covered date"
            )
        for index, calendar_day in enumerate(self.days):
            if type(calendar_day) is not CalendarDay:
                raise TypeError("calendar days must contain exact CalendarDay values")
            expected_day = self.coverage_start + timedelta(days=index)
            if calendar_day.day != expected_day:
                raise CalendarCoverageError(
                    "calendar days must be unique, ordered, and contiguous"
                )
            if calendar_day.reference.knowledge_time > self.cutoff:
                raise CalendarIntegrityError(
                    "calendar contains a reference record known after its cutoff"
                )

        if not self.source_snapshot_ids:
            raise CalendarIntegrityError("calendar source_snapshot_ids are required")
        if tuple(sorted(set(self.source_snapshot_ids))) != self.source_snapshot_ids:
            raise CalendarIntegrityError(
                "calendar source_snapshot_ids must be unique and sorted"
            )
        referenced_snapshot_ids = tuple(
            sorted({day.reference.source_snapshot_id for day in self.days})
        )
        if self.source_snapshot_ids != referenced_snapshot_ids:
            raise CalendarIntegrityError(
                "calendar source_snapshot_ids do not match its day references"
            )

        snapshot_id = self._calculated_snapshot_id()
        object.__setattr__(self, "snapshot_id", snapshot_id)
        object.__setattr__(
            self,
            "version",
            f"{self.schema_version}@sha256:{snapshot_id}",
        )

    def _calculated_snapshot_id(self) -> str:
        material = {
            "schema_version": self.schema_version,
            "exchange": self.exchange,
            "segment": self.segment,
            "cutoff": self.cutoff,
            "coverage_start": self.coverage_start,
            "coverage_end": self.coverage_end,
            "days": self.days,
            "source_snapshot_ids": self.source_snapshot_ids,
            "readiness": self.readiness,
        }
        return content_id(material, length=64)

    def verify_content_identity(self) -> None:
        """Detect any mutation or deserialization bypass after construction."""

        if any(
            type(calendar_day) is not CalendarDay
            or type(calendar_day.reference) is not ExternalRecordRef
            or any(
                type(window) is not SessionWindow
                for window in calendar_day.session_windows
            )
            for calendar_day in self.days
        ):
            raise CalendarIntegrityError(
                "calendar reference graph must contain only exact audited types"
            )
        expected_id = self._calculated_snapshot_id()
        expected_version = f"{self.schema_version}@sha256:{expected_id}"
        if self.snapshot_id != expected_id or self.version != expected_version:
            raise CalendarIntegrityError("calendar content identity verification failed")

    def day(self, value: date) -> CalendarDay:
        _require_date(value, "calendar lookup date")
        if value < self.coverage_start or value > self.coverage_end:
            raise CalendarCoverageError("calendar lookup falls outside declared coverage")
        return self.days[(value - self.coverage_start).days]

    def require_session(self, value: date) -> CalendarDay:
        calendar_day = self.day(value)
        if not calendar_day.is_session:
            raise NotTradingSessionError(f"{value.isoformat()} is not a trading session")
        return calendar_day

    def next_session(self, after: date) -> CalendarDay:
        self.day(after)
        offset = (after - self.coverage_start).days + 1
        for calendar_day in self.days[offset:]:
            if calendar_day.is_session:
                return calendar_day
        raise CalendarCoverageError("no next trading session exists within calendar coverage")

    def previous_session(self, before: date) -> CalendarDay:
        """Return the preceding declared session without inferring missing dates."""

        self.day(before)
        offset = (before - self.coverage_start).days
        for calendar_day in reversed(self.days[:offset]):
            if calendar_day.is_session:
                return calendar_day
        raise CalendarCoverageError(
            "no previous trading session exists within calendar coverage"
        )

    def advance_sessions(self, start: date, sessions: int) -> CalendarDay:
        if type(sessions) is not int or sessions < 0:
            raise ValueError("sessions must be a non-negative integer")
        current = self.require_session(start)
        for _ in range(sessions):
            current = self.next_session(current.day)
        return current
