from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import datetime

from india_swing.data.asof import DataIntegrityError, validate_candidate
from india_swing.demo import IST, build_demo
from india_swing.domain.models import Candidate, RunStatus
from india_swing.reference.calendar import (
    CalendarDayKind,
    CalendarSnapshot,
    SessionWindow,
    SessionWindowPhase,
)


class CalendarExecutionTimingTests(unittest.TestCase):
    @staticmethod
    def _calendar_with_next_day_windows(reference_context, windows):
        days = list(reference_context.calendar.days)
        original = reference_context.calendar.day(datetime(2026, 7, 16).date())
        days[(original.day - reference_context.calendar.coverage_start).days] = replace(
            original,
            kind=CalendarDayKind.SPECIAL,
            session_windows=windows,
            data_ready_at=datetime(2026, 7, 16, 20, 0, tzinfo=IST),
        )
        return CalendarSnapshot.create(
            exchange=reference_context.calendar.exchange,
            segment=reference_context.calendar.segment,
            cutoff=reference_context.calendar.cutoff,
            coverage_start=reference_context.calendar.coverage_start,
            coverage_end=reference_context.calendar.coverage_end,
            days=tuple(days),
            source_snapshot_ids=reference_context.calendar.source_snapshot_ids,
            readiness=reference_context.calendar.readiness,
        )

    def test_entry_window_must_match_actual_special_session_hours(self) -> None:
        pipeline, snapshot, instruments, _, reference_context = build_demo()
        special_calendar = self._calendar_with_next_day_windows(
            reference_context,
            (
                SessionWindow(
                    datetime(2026, 7, 16, 18, 0, tzinfo=IST),
                    datetime(2026, 7, 16, 19, 0, tzinfo=IST),
                    SessionWindowPhase.LIVE_CONTINUOUS,
                ),
            ),
        )
        instrument = instruments[0]
        forecast = pipeline.forecast_provider.forecast(instrument, snapshot)
        signals, setup, evidence_ids = pipeline.signal_provider.generate(
            instrument, forecast, snapshot
        )
        candidate = Candidate(instrument, forecast, signals, setup, evidence_ids)

        with self.assertRaisesRegex(DataIntegrityError, "trading calendar"):
            validate_candidate(candidate, snapshot, special_calendar)

    def test_split_session_break_cannot_be_used_as_an_entry_window(self) -> None:
        pipeline, snapshot, instruments, _, reference_context = build_demo()
        split_calendar = self._calendar_with_next_day_windows(
            reference_context,
            (
                SessionWindow(
                    datetime(2026, 7, 16, 9, 0, tzinfo=IST),
                    datetime(2026, 7, 16, 11, 0, tzinfo=IST),
                    SessionWindowPhase.LIVE_CONTINUOUS,
                ),
                SessionWindow(
                    datetime(2026, 7, 16, 13, 0, tzinfo=IST),
                    datetime(2026, 7, 16, 15, 0, tzinfo=IST),
                    SessionWindowPhase.LIVE_CONTINUOUS,
                ),
            ),
        )
        instrument = instruments[0]
        forecast = pipeline.forecast_provider.forecast(instrument, snapshot)
        signals, setup, evidence_ids = pipeline.signal_provider.generate(
            instrument, forecast, snapshot
        )
        setup = replace(
            setup,
            earliest_entry_at=datetime(2026, 7, 16, 12, 0, tzinfo=IST),
            entry_expires_at=datetime(2026, 7, 16, 13, 30, tzinfo=IST),
        )
        candidate = Candidate(instrument, forecast, signals, setup, evidence_ids)

        with self.assertRaisesRegex(DataIntegrityError, "trading calendar"):
            validate_candidate(candidate, snapshot, split_calendar)

    def test_weekend_cannot_be_an_entry_session(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
        signals, setup, evidence_ids = pipeline.signal_provider.values["DEMO-SMALL"]
        pipeline.signal_provider.values["DEMO-SMALL"] = (
            signals,
            replace(
                setup,
                earliest_entry_at=datetime(2026, 7, 18, 9, 20, tzinfo=IST),
                entry_expires_at=datetime(2026, 7, 18, 15, 15, tzinfo=IST),
            ),
            evidence_ids,
        )

        result = pipeline.run(snapshot, instruments, portfolio, reference_context)

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertEqual(result.failure_stage, "candidate_integrity")

    def test_entry_must_use_next_declared_session(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
        signals, setup, evidence_ids = pipeline.signal_provider.values["DEMO-SMALL"]
        pipeline.signal_provider.values["DEMO-SMALL"] = (
            signals,
            replace(
                setup,
                earliest_entry_at=datetime(2026, 7, 17, 9, 20, tzinfo=IST),
                entry_expires_at=datetime(2026, 7, 17, 15, 15, tzinfo=IST),
            ),
            evidence_ids,
        )

        result = pipeline.run(snapshot, instruments, portfolio, reference_context)

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertEqual(result.failure_stage, "candidate_integrity")

    def test_holding_horizon_must_fit_explicit_calendar_coverage(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
        signals, setup, evidence_ids = pipeline.signal_provider.values["DEMO-SMALL"]
        pipeline.signal_provider.values["DEMO-SMALL"] = (
            signals,
            replace(setup, max_holding_sessions=20),
            evidence_ids,
        )

        result = pipeline.run(snapshot, instruments, portfolio, reference_context)

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertEqual(result.failure_stage, "candidate_integrity")


if __name__ == "__main__":
    unittest.main()
