from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from india_swing.calendar_data.artifact_store import LocalCalendarSourceArtifactStore
from india_swing.calendar_data.materialization import (
    CalendarMaterializationIntegrityError,
    materialize_collection_calendar,
)
from india_swing.calendar_data.materialization_codec import (
    encode_calendar_materialization,
)
from india_swing.calendar_data.models import CALENDAR_DECLARATION_SCHEMA_VERSION
from india_swing.calendar_evidence import build_observed_market_date_artifact
from india_swing.reference.calendar import CalendarDayKind, SessionWindowPhase
from india_swing.reference.models import ReferenceReadiness
from tests.test_calendar_evidence import stored_bundle


UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
KNOWN = datetime(2026, 7, 15, 13, 0, tzinfo=UTC)
CUTOFF = datetime(2026, 7, 15, 15, 30, tzinfo=UTC)
START = date(2026, 7, 17)
END = date(2026, 7, 19)


def _window(phase: str, opens: str, closes: str) -> dict[str, str]:
    return {"phase": phase, "opens": opens, "closes": closes}


def _locator(record: str) -> dict[str, object]:
    return {"page": 1, "section": "CM schedule", "record": record}


def _base_event(
    *,
    effective_from: str = "2026-01-01",
    effective_to_exclusive: str = "2027-01-01",
) -> dict[str, object]:
    return {
        "event_type": "BASE_WEEKLY_SCHEDULE",
        "effective_from": effective_from,
        "effective_to_exclusive": effective_to_exclusive,
        "weekdays": ["MON", "TUE", "WED", "THU", "FRI"],
        "windows": [_window("LIVE_CONTINUOUS", "09:15:00", "15:30:00")],
        "supersedes_event_ids": [],
        "source_locator": _locator("regular"),
        "reason": "Regular capital-market schedule",
    }


def _closed(day: date, predecessor: str, kind: str = "HOLIDAY") -> dict[str, object]:
    return {
        "event_type": "DATE_CLOSED",
        "date": day.isoformat(),
        "day_kind": kind,
        "supersedes_event_ids": [predecessor],
        "source_locator": _locator(f"closed-{day.isoformat()}"),
        "reason": "Explicit dated closure",
    }


def _special(day: date, predecessor: str) -> dict[str, object]:
    return {
        "event_type": "DATE_SESSION_REPLACED",
        "date": day.isoformat(),
        "day_kind": "SPECIAL",
        "windows": [
            _window("PRE_OPEN", "17:45:00", "18:00:00"),
            _window("LIVE_CONTINUOUS", "18:00:00", "19:00:00"),
        ],
        "supersedes_event_ids": [predecessor],
        "source_locator": _locator(f"special-{day.isoformat()}"),
        "reason": "Exact replacement special-session schedule",
    }


def _mock(day: date) -> dict[str, object]:
    return {
        "event_type": "NON_EXECUTABLE_ACTIVITY",
        "date": day.isoformat(),
        "windows": [_window("MOCK_TEST", "11:00:00", "12:00:00")],
        "supersedes_event_ids": [],
        "source_locator": _locator(f"mock-{day.isoformat()}"),
        "reason": "Non-executable mock activity",
    }


class CalendarMaterializationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.sequence = 0

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def import_source(
        self,
        document_id: str,
        events: list[dict[str, object]],
        *,
        validated_at: datetime = KNOWN,
    ):
        self.sequence += 1
        source_name = f"{document_id}-{self.sequence}.pdf"
        declaration_name = f"{document_id}-{self.sequence}.events.json"
        source_bytes = (
            f"%PDF-1.7\n{document_id}-{self.sequence}\n%%EOF\n".encode("ascii")
        )
        input_root = self.root / "inputs"
        input_root.mkdir(exist_ok=True)
        source_path = input_root / source_name
        declaration_path = input_root / declaration_name
        source_path.write_bytes(source_bytes)
        declaration = {
            "schema_version": CALENDAR_DECLARATION_SCHEMA_VERSION,
            "exchange": "NSE",
            "segment": "CM",
            "claimed_authority": "NSE",
            "claimed_document_id": document_id,
            "claimed_issue_date": "2026-01-01",
            "claimed_source_url": f"https://example.invalid/{source_name}",
            "source_filename": source_name,
            "source_media_type": "application/pdf",
            "source_byte_count": len(source_bytes),
            "source_sha256": hashlib.sha256(source_bytes).hexdigest(),
            "events": events,
        }
        declaration_path.write_text(
            json.dumps(declaration, separators=(",", ":")),
            encoding="utf-8",
        )
        values = iter((validated_at - timedelta(seconds=1), validated_at))
        return LocalCalendarSourceArtifactStore(
            self.root / "archive",
            clock=lambda: next(values),
        ).import_source(source_path, declaration_path)

    def sources(self):
        base = self.import_source("CMTR-BASE-2026", [_base_event()])
        base_id = base.parsed.events[0].event_id
        holiday = self.import_source("CMTR-HOLIDAY-2026", [_closed(START, base_id)])
        special = self.import_source(
            "CMTR-SPECIAL-2026",
            [_special(START + timedelta(days=1), base_id)],
        )
        mock = self.import_source(
            "CMTR-MOCK-2026",
            [_mock(START + timedelta(days=2))],
        )
        return (base, holiday, special, mock)

    def test_materializes_explicit_schedule_graph_without_inventing_finality(self) -> None:
        result = materialize_collection_calendar(
            sources=self.sources(),
            coverage_start=START,
            coverage_end=END,
            cutoff=CUTOFF,
        )

        self.assertEqual(
            tuple(value.kind for value in result.calendar_snapshot.days),
            (
                CalendarDayKind.HOLIDAY,
                CalendarDayKind.SPECIAL,
                CalendarDayKind.WEEKEND,
            ),
        )
        self.assertEqual(
            tuple(
                value.phase
                for value in result.calendar_snapshot.days[1].session_windows
            ),
            (SessionWindowPhase.PRE_OPEN, SessionWindowPhase.LIVE_CONTINUOUS),
        )
        self.assertTrue(
            all(value.data_ready_at is None for value in result.calendar_snapshot.days)
        )
        self.assertFalse(result.calendar_snapshot.days[2].is_session)
        self.assertFalse(result.calendar_snapshot.days[2].session_windows)
        self.assertEqual(
            len(result.day_resolutions[2].non_executable_event_ids),
            1,
        )
        self.assertEqual(len(result.source_manifests), 4)
        self.assertIs(result.readiness, ReferenceReadiness.COLLECTION_ONLY)
        self.assertFalse(result.actionable)

    def test_source_order_and_equivalent_timezone_cutoff_have_identical_bytes(self) -> None:
        sources = self.sources()
        first = materialize_collection_calendar(
            sources=sources,
            coverage_start=START,
            coverage_end=END,
            cutoff=CUTOFF,
        )
        second = materialize_collection_calendar(
            sources=tuple(reversed(sources)),
            coverage_start=START,
            coverage_end=END,
            cutoff=CUTOFF.astimezone(IST),
        )

        self.assertEqual(first.materialization_id, second.materialization_id)
        self.assertEqual(
            encode_calendar_materialization(first),
            encode_calendar_materialization(second),
        )

    def test_requires_sealed_sources_known_by_cutoff(self) -> None:
        base = self.import_source("CMTR-BASE-CUTOFF", [_base_event()])
        with self.assertRaisesRegex(
            CalendarMaterializationIntegrityError,
            "validated by",
        ):
            materialize_collection_calendar(
                sources=(base,),
                coverage_start=START,
                coverage_end=START,
                cutoff=KNOWN - timedelta(microseconds=1),
            )

        substituted = replace(base, normalized_bytes=b"{}")
        with self.assertRaisesRegex(
            CalendarMaterializationIntegrityError,
            "sealed provenance",
        ):
            materialize_collection_calendar(
                sources=(substituted,),
                coverage_start=START,
                coverage_end=START,
                cutoff=CUTOFF,
            )

    def test_missing_or_overlapping_base_and_unknown_override_fail_closed(self) -> None:
        base = self.import_source("CMTR-BASE-A", [_base_event()])
        overlapping = self.import_source("CMTR-BASE-B", [_base_event()])
        with self.assertRaisesRegex(
            CalendarMaterializationIntegrityError,
            "exactly one",
        ):
            materialize_collection_calendar(
                sources=(base, overlapping),
                coverage_start=START,
                coverage_end=START,
                cutoff=CUTOFF,
            )

        unknown = self.import_source(
            "CMTR-UNKNOWN-OVERRIDE",
            [_closed(START, "f" * 64)],
        )
        with self.assertRaisesRegex(
            CalendarMaterializationIntegrityError,
            "unknown event",
        ):
            materialize_collection_calendar(
                sources=(base, unknown),
                coverage_start=START,
                coverage_end=START,
                cutoff=CUTOFF,
            )

    def test_competing_overrides_are_rejected(self) -> None:
        base = self.import_source("CMTR-BASE-GRAPH", [_base_event()])
        base_id = base.parsed.events[0].event_id
        first = self.import_source("CMTR-CLOSE-A", [_closed(START, base_id)])
        second = self.import_source(
            "CMTR-CLOSE-B",
            [_closed(START, base_id, "UNSCHEDULED_CLOSURE")],
        )
        with self.assertRaisesRegex(
            CalendarMaterializationIntegrityError,
            "competing",
        ):
            materialize_collection_calendar(
                sources=(base, first, second),
                coverage_start=START,
                coverage_end=START,
                cutoff=CUTOFF,
            )


    def test_explicit_holiday_to_special_session_chain_is_supported(self) -> None:
        base = self.import_source("CMTR-BASE-AMEND", [_base_event()])
        base_id = base.parsed.events[0].event_id
        holiday = self.import_source(
            "CMTR-HOLIDAY-AMEND",
            [_closed(START, base_id)],
        )
        replacement = self.import_source(
            "CMTR-SPECIAL-AMEND",
            [_special(START, holiday.parsed.events[0].event_id)],
        )

        result = materialize_collection_calendar(
            sources=(base, holiday, replacement),
            coverage_start=START,
            coverage_end=START,
            cutoff=CUTOFF,
        )

        self.assertIs(result.calendar_snapshot.days[0].kind, CalendarDayKind.SPECIAL)
        self.assertEqual(len(result.day_resolutions[0].state_chain_event_ids), 3)

    def test_positive_traded_date_cannot_resolve_closed_or_arrive_after_cutoff(self) -> None:
        base = self.import_source("CMTR-BASE-EVIDENCE", [_base_event()])
        base_id = base.parsed.events[0].event_id
        closed = self.import_source("CMTR-CLOSE-EVIDENCE", [_closed(START, base_id)])
        evidence_time = datetime(2026, 7, 17, 15, 0, tzinfo=UTC)
        bundle = stored_bundle(
            (START,),
            storage_root=self.root / "daily",
            first_seen=evidence_time - timedelta(seconds=1),
            validated=evidence_time,
        )
        evidence = build_observed_market_date_artifact(
            bundle,
            cutoff=evidence_time,
        )
        with self.assertRaisesRegex(
            CalendarMaterializationIntegrityError,
            "contradicts",
        ):
            materialize_collection_calendar(
                sources=(base, closed),
                coverage_start=START,
                coverage_end=START,
                cutoff=evidence_time,
                observed_date_artifacts=(evidence,),
            )
        with self.assertRaisesRegex(
            CalendarMaterializationIntegrityError,
            "not known",
        ):
            materialize_collection_calendar(
                sources=(base,),
                coverage_start=START,
                coverage_end=START,
                cutoff=evidence_time - timedelta(microseconds=1),
                observed_date_artifacts=(evidence,),
            )

    def test_codec_detects_nested_resolution_mutation(self) -> None:
        result = materialize_collection_calendar(
            sources=self.sources(),
            coverage_start=START,
            coverage_end=END,
            cutoff=CUTOFF,
        )
        object.__setattr__(
            result.day_resolutions[0],
            "source_snapshot_id",
            "f" * 64,
        )
        with self.assertRaises(CalendarMaterializationIntegrityError):
            encode_calendar_materialization(result)


if __name__ == "__main__":
    unittest.main()
