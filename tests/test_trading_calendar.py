from __future__ import annotations

import unittest
from datetime import date, datetime, time, timedelta, timezone

from india_swing.reference.calendar import (
    CalendarCoverageError,
    CalendarDay,
    CalendarDayKind,
    CalendarIntegrityError,
    CalendarSnapshot,
    OutsideSessionWindowError,
    SessionWindow,
    SessionWindowPhase,
)
from india_swing.reference.models import ExternalRecordRef, ReferenceReadiness


IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc
SOURCE_SNAPSHOT_ID = "a" * 64
CUTOFF = datetime(2026, 7, 16, 18, 0, tzinfo=IST)
COVERAGE_START = date(2026, 7, 17)
COVERAGE_END = date(2026, 7, 22)


def reference(
    day: date,
    *,
    knowledge_time: datetime = datetime(2026, 7, 1, 12, 0, tzinfo=IST),
    content_hash: str | None = None,
) -> ExternalRecordRef:
    return ExternalRecordRef(
        event_time=datetime.combine(day, time(0), tzinfo=IST),
        knowledge_time=knowledge_time,
        source="NSE_TEST_FIXTURE",
        content_hash=content_hash or f"{day.toordinal():064x}",
        source_snapshot_id=SOURCE_SNAPSHOT_ID,
    )


def session_day(
    day: date,
    *,
    kind: CalendarDayKind = CalendarDayKind.REGULAR,
    record: ExternalRecordRef | None = None,
    tz: timezone = IST,
) -> CalendarDay:
    return CalendarDay(
        day=day,
        kind=kind,
        reference=record or reference(day),
        session_windows=(
            SessionWindow(
                opens_at=datetime.combine(day, time(9, 15), tzinfo=tz),
                closes_at=datetime.combine(day, time(15, 30), tzinfo=tz),
                phase=SessionWindowPhase.LIVE_CONTINUOUS,
            ),
        ),
        data_ready_at=datetime.combine(day, time(16, 0), tzinfo=tz),
    )


def closed_day(day: date, kind: CalendarDayKind) -> CalendarDay:
    return CalendarDay(day=day, kind=kind, reference=reference(day))


def calendar_days() -> tuple[CalendarDay, ...]:
    return (
        session_day(date(2026, 7, 17)),
        closed_day(date(2026, 7, 18), CalendarDayKind.WEEKEND),
        closed_day(date(2026, 7, 19), CalendarDayKind.WEEKEND),
        closed_day(date(2026, 7, 20), CalendarDayKind.HOLIDAY),
        session_day(date(2026, 7, 21)),
        session_day(date(2026, 7, 22), kind=CalendarDayKind.SPECIAL),
    )


def snapshot(
    *,
    days: tuple[CalendarDay, ...] | None = None,
    cutoff: datetime = CUTOFF,
    readiness: ReferenceReadiness = ReferenceReadiness.SYNTHETIC_TEST,
) -> CalendarSnapshot:
    return CalendarSnapshot.create(
        exchange="NSE",
        segment="CM",
        cutoff=cutoff,
        coverage_start=COVERAGE_START,
        coverage_end=COVERAGE_END,
        days=days if days is not None else calendar_days(),
        source_snapshot_ids=(SOURCE_SNAPSHOT_ID,),
        readiness=readiness,
    )


class TradingCalendarTests(unittest.TestCase):
    def test_verified_label_is_locked_until_an_official_importer_exists(self) -> None:
        with self.assertRaisesRegex(CalendarIntegrityError, "importer"):
            snapshot(readiness=ReferenceReadiness.POINT_IN_TIME_VERIFIED)

    def test_external_reference_times_must_be_aware(self) -> None:
        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            ExternalRecordRef(
                event_time=datetime(2026, 7, 1, 12, 0),
                knowledge_time=datetime(2026, 7, 1, 12, 1, tzinfo=IST),
                source="NSE_TEST_FIXTURE",
                content_hash="b" * 64,
                source_snapshot_id=SOURCE_SNAPSHOT_ID,
            )

    def test_coverage_requires_one_explicit_ordered_day_per_date(self) -> None:
        missing_saturday = calendar_days()[:1] + calendar_days()[2:]

        with self.assertRaisesRegex(CalendarCoverageError, "every covered date"):
            snapshot(days=missing_saturday)

        disordered = list(calendar_days())
        disordered[1], disordered[2] = disordered[2], disordered[1]
        with self.assertRaisesRegex(CalendarCoverageError, "ordered, and contiguous"):
            snapshot(days=tuple(disordered))

    def test_next_and_advance_sessions_skip_weekends_and_holidays(self) -> None:
        trading_calendar = snapshot()

        self.assertEqual(
            trading_calendar.next_session(date(2026, 7, 17)).day,
            date(2026, 7, 21),
        )
        self.assertEqual(
            trading_calendar.next_session(date(2026, 7, 18)).day,
            date(2026, 7, 21),
        )
        self.assertEqual(
            trading_calendar.advance_sessions(date(2026, 7, 17), 1).day,
            date(2026, 7, 21),
        )
        self.assertEqual(
            trading_calendar.advance_sessions(date(2026, 7, 17), 2).day,
            date(2026, 7, 22),
        )
        self.assertEqual(
            trading_calendar.previous_session(date(2026, 7, 21)).day,
            date(2026, 7, 17),
        )
        self.assertEqual(
            trading_calendar.previous_session(date(2026, 7, 20)).day,
            date(2026, 7, 17),
        )

    def test_session_arithmetic_fails_closed_outside_coverage(self) -> None:
        trading_calendar = snapshot()

        with self.assertRaises(CalendarCoverageError):
            trading_calendar.next_session(date(2026, 7, 16))
        with self.assertRaises(CalendarCoverageError):
            trading_calendar.next_session(COVERAGE_END)
        with self.assertRaises(CalendarCoverageError):
            trading_calendar.previous_session(COVERAGE_START)
        with self.assertRaises(CalendarCoverageError):
            trading_calendar.previous_session(date(2026, 7, 23))
        with self.assertRaises(CalendarCoverageError):
            trading_calendar.advance_sessions(date(2026, 7, 22), 1)

    def test_reference_vintage_known_after_cutoff_is_rejected(self) -> None:
        late_reference = reference(
            COVERAGE_START,
            knowledge_time=CUTOFF + timedelta(microseconds=1),
        )
        days = (session_day(COVERAGE_START, record=late_reference),) + calendar_days()[1:]

        with self.assertRaisesRegex(CalendarIntegrityError, "known after"):
            snapshot(days=days)

    def test_reference_event_date_must_match_calendar_date(self) -> None:
        wrong_event = reference(date(2026, 7, 18))

        with self.assertRaisesRegex(CalendarIntegrityError, "event_time"):
            session_day(COVERAGE_START, record=wrong_event)

    def test_session_times_must_use_india_offset(self) -> None:
        with self.assertRaisesRegex(ValueError, "Asia/Kolkata"):
            session_day(COVERAGE_START, tz=UTC)

    def test_trading_days_require_immutable_nonempty_windows(self) -> None:
        with self.assertRaisesRegex(TypeError, "immutable tuple"):
            CalendarDay(
                day=COVERAGE_START,
                kind=CalendarDayKind.REGULAR,
                reference=reference(COVERAGE_START),
                session_windows=[],  # type: ignore[arg-type]
                data_ready_at=datetime.combine(COVERAGE_START, time(16), tzinfo=IST),
            )

        with self.assertRaisesRegex(CalendarIntegrityError, "at least one window"):
            CalendarDay(
                day=COVERAGE_START,
                kind=CalendarDayKind.REGULAR,
                reference=reference(COVERAGE_START),
                data_ready_at=datetime.combine(COVERAGE_START, time(16), tzinfo=IST),
            )

    def test_pre_open_and_mock_windows_are_not_executable(self) -> None:
        pre_open = SessionWindow(
            datetime.combine(COVERAGE_START, time(9), tzinfo=IST),
            datetime.combine(COVERAGE_START, time(9, 15), tzinfo=IST),
            SessionWindowPhase.PRE_OPEN,
        )
        live = SessionWindow(
            datetime.combine(COVERAGE_START, time(9, 15), tzinfo=IST),
            datetime.combine(COVERAGE_START, time(15, 30), tzinfo=IST),
            SessionWindowPhase.LIVE_CONTINUOUS,
        )
        mock = SessionWindow(
            datetime.combine(COVERAGE_START, time(15, 30), tzinfo=IST),
            datetime.combine(COVERAGE_START, time(15, 45), tzinfo=IST),
            SessionWindowPhase.MOCK_TEST,
        )
        trading_day = CalendarDay(
            day=COVERAGE_START,
            kind=CalendarDayKind.SPECIAL,
            reference=reference(COVERAGE_START),
            session_windows=(pre_open, live, mock),
            data_ready_at=datetime.combine(COVERAGE_START, time(16), tzinfo=IST),
        )

        self.assertFalse(pre_open.is_executable)
        self.assertTrue(live.is_executable)
        self.assertFalse(mock.is_executable)
        self.assertIsNone(
            trading_day.session_window_containing(
                datetime.combine(COVERAGE_START, time(9, 5), tzinfo=IST)
            )
        )
        self.assertIs(
            trading_day.session_window_containing(
                datetime.combine(COVERAGE_START, time(9, 5), tzinfo=IST),
                executable_only=False,
            ),
            pre_open,
        )
        self.assertIsNone(
            trading_day.session_window_containing(
                datetime.combine(COVERAGE_START, time(15, 35), tzinfo=IST)
            )
        )

    def test_trading_day_rejects_only_non_executable_windows(self) -> None:
        with self.assertRaisesRegex(
            CalendarIntegrityError,
            "executable live-continuous window",
        ):
            CalendarDay(
                day=COVERAGE_START,
                kind=CalendarDayKind.SPECIAL,
                reference=reference(COVERAGE_START),
                session_windows=(
                    SessionWindow(
                        datetime.combine(COVERAGE_START, time(9), tzinfo=IST),
                        datetime.combine(COVERAGE_START, time(9, 15), tzinfo=IST),
                        SessionWindowPhase.PRE_OPEN,
                    ),
                    SessionWindow(
                        datetime.combine(COVERAGE_START, time(15, 30), tzinfo=IST),
                        datetime.combine(COVERAGE_START, time(15, 45), tzinfo=IST),
                        SessionWindowPhase.MOCK_TEST,
                    ),
                ),
                data_ready_at=datetime.combine(COVERAGE_START, time(16), tzinfo=IST),
            )

    def test_session_window_subclass_cannot_override_executable_phase(self) -> None:
        class PretendLiveWindow(SessionWindow):
            @property
            def is_executable(self) -> bool:
                return True

        forged_pre_open = PretendLiveWindow(
            datetime.combine(COVERAGE_START, time(9), tzinfo=IST),
            datetime.combine(COVERAGE_START, time(9, 15), tzinfo=IST),
            SessionWindowPhase.PRE_OPEN,
        )

        with self.assertRaisesRegex(TypeError, "exact SessionWindow"):
            CalendarDay(
                day=COVERAGE_START,
                kind=CalendarDayKind.REGULAR,
                reference=reference(COVERAGE_START),
                session_windows=(forged_pre_open,),
                data_ready_at=datetime.combine(COVERAGE_START, time(16), tzinfo=IST),
            )

    def test_session_windows_must_be_sorted_and_non_overlapping(self) -> None:
        windows = (
            SessionWindow(
                datetime.combine(COVERAGE_START, time(13), tzinfo=IST),
                datetime.combine(COVERAGE_START, time(15, 30), tzinfo=IST),
                SessionWindowPhase.LIVE_CONTINUOUS,
            ),
            SessionWindow(
                datetime.combine(COVERAGE_START, time(9, 15), tzinfo=IST),
                datetime.combine(COVERAGE_START, time(13, 30), tzinfo=IST),
                SessionWindowPhase.LIVE_CONTINUOUS,
            ),
        )
        with self.assertRaisesRegex(CalendarIntegrityError, "sorted and non-overlapping"):
            CalendarDay(
                day=COVERAGE_START,
                kind=CalendarDayKind.SPECIAL,
                reference=reference(COVERAGE_START),
                session_windows=windows,
                data_ready_at=datetime.combine(COVERAGE_START, time(16), tzinfo=IST),
            )

    def test_data_ready_time_must_follow_final_window(self) -> None:
        with self.assertRaisesRegex(CalendarIntegrityError, "after the final"):
            CalendarDay(
                day=COVERAGE_START,
                kind=CalendarDayKind.REGULAR,
                reference=reference(COVERAGE_START),
                session_windows=(
                    SessionWindow(
                        datetime.combine(COVERAGE_START, time(9, 15), tzinfo=IST),
                        datetime.combine(COVERAGE_START, time(15, 30), tzinfo=IST),
                        SessionWindowPhase.LIVE_CONTINUOUS,
                    ),
                ),
                data_ready_at=datetime.combine(COVERAGE_START, time(15, 30), tzinfo=IST),
            )

    def test_split_session_gap_is_not_a_trading_window(self) -> None:
        split_day = CalendarDay(
            day=COVERAGE_START,
            kind=CalendarDayKind.SPECIAL,
            reference=reference(COVERAGE_START),
            session_windows=(
                SessionWindow(
                    datetime.combine(COVERAGE_START, time(9), tzinfo=IST),
                    datetime.combine(COVERAGE_START, time(11, 30), tzinfo=IST),
                    SessionWindowPhase.LIVE_CONTINUOUS,
                ),
                SessionWindow(
                    datetime.combine(COVERAGE_START, time(13), tzinfo=IST),
                    datetime.combine(COVERAGE_START, time(15), tzinfo=IST),
                    SessionWindowPhase.LIVE_CONTINUOUS,
                ),
            ),
            data_ready_at=datetime.combine(COVERAGE_START, time(15, 30), tzinfo=IST),
        )

        first = split_day.require_same_session_window(
            datetime.combine(COVERAGE_START, time(10), tzinfo=IST),
            datetime.combine(COVERAGE_START, time(11, 30), tzinfo=IST),
        )
        self.assertEqual(first, split_day.session_windows[0])
        self.assertIsNone(
            split_day.session_window_containing(
                datetime.combine(COVERAGE_START, time(12), tzinfo=IST)
            )
        )
        with self.assertRaisesRegex(OutsideSessionWindowError, "same session window"):
            split_day.require_same_session_window(
                datetime.combine(COVERAGE_START, time(11), tzinfo=IST),
                datetime.combine(COVERAGE_START, time(13, 30), tzinfo=IST),
            )

    def test_closed_dates_cannot_carry_session_times(self) -> None:
        with self.assertRaisesRegex(CalendarIntegrityError, "cannot carry"):
            CalendarDay(
                day=COVERAGE_START,
                kind=CalendarDayKind.UNSCHEDULED_CLOSURE,
                reference=reference(COVERAGE_START),
                session_windows=(
                    SessionWindow(
                        datetime.combine(COVERAGE_START, time(9, 15), tzinfo=IST),
                        datetime.combine(COVERAGE_START, time(15, 30), tzinfo=IST),
                        SessionWindowPhase.LIVE_CONTINUOUS,
                    ),
                ),
            )

    def test_snapshot_id_and_version_are_deterministic_and_content_derived(self) -> None:
        first = snapshot()
        second = snapshot()

        self.assertEqual(first.snapshot_id, second.snapshot_id)
        self.assertEqual(first.version, second.version)
        self.assertEqual(len(first.snapshot_id), 64)
        self.assertEqual(
            first.version,
            f"{first.schema_version}@sha256:{first.snapshot_id}",
        )

        changed = snapshot(readiness=ReferenceReadiness.COLLECTION_ONLY)
        self.assertNotEqual(first.snapshot_id, changed.snapshot_id)
        self.assertNotEqual(first.version, changed.version)


if __name__ == "__main__":
    unittest.main()
