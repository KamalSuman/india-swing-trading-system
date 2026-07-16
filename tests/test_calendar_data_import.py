from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

from india_swing.calendar_data import (
    CALENDAR_DECLARATION_SCHEMA_VERSION,
    CalendarDeclarationParser,
    CalendarEventType,
    CalendarSourceArtifactConflict,
    CalendarSourceArtifactIntegrityError,
    LocalCalendarSourceArtifactStore,
    encode_calendar_declaration,
    verify_stored_calendar_source_provenance,
)
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference_data.models import AcquisitionMode


SOURCE_NAME = "CMTR-CALENDAR-2026.pdf"
DECLARATION_NAME = "CMTR-CALENDAR-2026.events.json"
SOURCE_BYTES = b"%PDF-1.7\n1 0 obj\n<< /Type /Catalog >>\nendobj\n%%EOF\n"
UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
FIRST_SEEN_IST = datetime(2026, 7, 15, 20, 0, tzinfo=IST)
VALIDATED_IST = FIRST_SEEN_IST + timedelta(seconds=3)
FIRST_SEEN_UTC = FIRST_SEEN_IST.astimezone(UTC)
VALIDATED_UTC = VALIDATED_IST.astimezone(UTC)


def window(phase: str, opens: str, closes: str) -> dict[str, str]:
    return {"phase": phase, "opens": opens, "closes": closes}


def locator(record: str) -> dict[str, object]:
    return {"page": 2, "section": "Capital market schedule", "record": record}


def calendar_events() -> list[dict[str, object]]:
    return [
        {
            "event_type": "BASE_WEEKLY_SCHEDULE",
            "effective_from": "2026-01-01",
            "effective_to_exclusive": "2027-01-01",
            "weekdays": ["MON", "TUE", "WED", "THU", "FRI"],
            "windows": [window("LIVE_CONTINUOUS", "09:15:00", "15:30:00")],
            "supersedes_event_ids": [],
            "source_locator": locator("Regular schedule"),
            "reason": "Versioned regular capital-market schedule",
        },
        {
            "event_type": "DATE_CLOSED",
            "date": "2026-01-26",
            "day_kind": "HOLIDAY",
            "supersedes_event_ids": ["a" * 64],
            "source_locator": locator("Republic Day"),
            "reason": "Annual trading holiday",
        },
        {
            "event_type": "DATE_SESSION_REPLACED",
            "date": "2026-11-08",
            "day_kind": "SPECIAL",
            "windows": [
                window("PRE_OPEN", "17:45:00", "18:00:00"),
                window("LIVE_CONTINUOUS", "18:00:00", "19:00:00"),
            ],
            "supersedes_event_ids": ["b" * 64],
            "source_locator": locator("Special live session"),
            "reason": "Exact replacement schedule for a special live session",
        },
        {
            "event_type": "NON_EXECUTABLE_ACTIVITY",
            "date": "2026-08-01",
            "windows": [window("MOCK_TEST", "11:00:00", "12:00:00")],
            "supersedes_event_ids": [],
            "source_locator": locator("Mock test"),
            "reason": "Mock activity is recorded but cannot open a trading session",
        },
    ]


def declaration_value(
    *,
    events: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    return {
        "schema_version": CALENDAR_DECLARATION_SCHEMA_VERSION,
        "exchange": "NSE",
        "segment": "CM",
        "claimed_authority": "NSE",
        "claimed_document_id": "CMTR-CALENDAR-2026",
        "claimed_issue_date": "2025-12-15",
        "claimed_source_url": "https://example.invalid/CMTR-CALENDAR-2026.pdf",
        "source_filename": SOURCE_NAME,
        "source_media_type": "application/pdf",
        "source_byte_count": len(SOURCE_BYTES),
        "source_sha256": hashlib.sha256(SOURCE_BYTES).hexdigest(),
        "events": events if events is not None else calendar_events(),
    }


def declaration_bytes(
    *,
    events: list[dict[str, object]] | None = None,
    value: dict[str, object] | None = None,
) -> bytes:
    return json.dumps(
        value if value is not None else declaration_value(events=events),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def clock_sequence(*values: datetime):
    iterator = iter(values)
    return lambda: next(iterator)


class CalendarDeclarationParserTests(unittest.TestCase):
    def setUp(self) -> None:
        self.parser = CalendarDeclarationParser()

    def parse(self, payload: bytes | None = None, source: bytes = SOURCE_BYTES):
        return self.parser.parse_bytes(
            payload or declaration_bytes(),
            source_bytes=source,
            source_filename=SOURCE_NAME,
            declaration_filename=DECLARATION_NAME,
        )

    def test_parses_all_four_event_types_and_derives_stable_ids(self) -> None:
        first = self.parse()
        second = self.parse()

        self.assertEqual(first, second)
        self.assertEqual(len(first.events), 4)
        self.assertEqual(
            {event.event_type for event in first.events},
            set(CalendarEventType),
        )
        self.assertEqual(first.event_ids, tuple(sorted(first.event_ids)))
        self.assertTrue(all(len(event_id) == 64 for event_id in first.event_ids))
        self.assertEqual(
            encode_calendar_declaration(first),
            encode_calendar_declaration(second),
        )

    def test_declaration_order_does_not_change_normalized_events(self) -> None:
        forward = self.parse(declaration_bytes(events=calendar_events()))
        reverse = self.parse(declaration_bytes(events=list(reversed(calendar_events()))))

        self.assertEqual(forward.events, reverse.events)
        self.assertEqual(
            encode_calendar_declaration(forward),
            encode_calendar_declaration(reverse),
        )

    def test_unknown_duplicate_or_caller_knowledge_fields_fail_strictly(self) -> None:
        extra = declaration_value()
        extra["knowledge_time"] = "2020-01-01T00:00:00Z"
        with self.assertRaisesRegex(CalendarSourceArtifactIntegrityError, "schema"):
            self.parse(declaration_bytes(value=extra))

        valid = declaration_bytes().decode("utf-8")
        duplicate = valid.replace(
            '{"claimed_authority"',
            '{"exchange":"NSE","claimed_authority"',
            1,
        ).encode("utf-8")
        with self.assertRaisesRegex(CalendarSourceArtifactIntegrityError, "duplicate"):
            self.parse(duplicate)

        event_extra = calendar_events()
        event_extra[0]["priority"] = 1
        with self.assertRaisesRegex(CalendarSourceArtifactIntegrityError, "schema"):
            self.parse(declaration_bytes(events=event_extra))

    def test_declaration_is_bound_to_exact_pdf_bytes_and_filename(self) -> None:
        wrong_hash = declaration_value()
        wrong_hash["source_sha256"] = "f" * 64
        with self.assertRaisesRegex(CalendarSourceArtifactIntegrityError, "exact source"):
            self.parse(declaration_bytes(value=wrong_hash))

        invalid_pdf = b"not a pdf"
        invalid_pdf_declaration = declaration_value()
        invalid_pdf_declaration["source_byte_count"] = len(invalid_pdf)
        invalid_pdf_declaration["source_sha256"] = hashlib.sha256(
            invalid_pdf
        ).hexdigest()
        with self.assertRaisesRegex(CalendarSourceArtifactIntegrityError, "PDF envelope"):
            self.parse(
                declaration_bytes(value=invalid_pdf_declaration),
                source=invalid_pdf,
            )

        with self.assertRaisesRegex(CalendarSourceArtifactIntegrityError, "another source"):
            self.parser.parse_bytes(
                declaration_bytes(),
                source_bytes=SOURCE_BYTES,
                source_filename="another.pdf",
                declaration_filename=DECLARATION_NAME,
            )

    def test_override_and_executability_shapes_fail_closed(self) -> None:
        cases: list[list[dict[str, object]]] = []

        no_supersedes = calendar_events()
        no_supersedes[1]["supersedes_event_ids"] = []
        cases.append(no_supersedes)

        base_override = calendar_events()
        base_override[0]["supersedes_event_ids"] = ["a" * 64]
        cases.append(base_override)

        live_mock = calendar_events()
        live_mock[3]["windows"] = [
            window("LIVE_CONTINUOUS", "11:00:00", "12:00:00")
        ]
        cases.append(live_mock)

        unsorted_supersedes = calendar_events()
        unsorted_supersedes[1]["supersedes_event_ids"] = ["b" * 64, "a" * 64]
        cases.append(unsorted_supersedes)

        for events in cases:
            with self.subTest(), self.assertRaises(CalendarSourceArtifactIntegrityError):
                self.parse(declaration_bytes(events=events))

    def test_nested_event_mutation_is_detected_before_encoding(self) -> None:
        parsed = self.parse()
        object.__setattr__(parsed.events[0], "reason", "mutated after validation")

        with self.assertRaisesRegex(
            CalendarSourceArtifactIntegrityError,
            "content identity",
        ):
            encode_calendar_declaration(parsed)


class LocalCalendarSourceArtifactStoreTests(unittest.TestCase):
    def write_inputs(self, root: Path) -> tuple[Path, Path, bytes]:
        source = root / SOURCE_NAME
        declaration = root / DECLARATION_NAME
        payload = declaration_bytes()
        source.write_bytes(SOURCE_BYTES)
        declaration.write_bytes(payload)
        return source, declaration, payload

    def store(self, root: Path) -> LocalCalendarSourceArtifactStore:
        return LocalCalendarSourceArtifactStore(
            root / "archive",
            clock=clock_sequence(FIRST_SEEN_IST, VALIDATED_IST),
        )

    def test_import_archives_exact_inputs_and_local_utc_knowledge(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source, declaration, declaration_payload = self.write_inputs(root)
            store = self.store(root)

            stored = store.import_source(source, declaration)
            loaded = store.get(stored.manifest.artifact_id)

            self.assertEqual(stored, loaded)
            self.assertEqual(stored.source_bytes, SOURCE_BYTES)
            self.assertEqual(stored.declaration_bytes, declaration_payload)
            self.assertEqual(stored.manifest.first_seen_at, FIRST_SEEN_UTC)
            self.assertEqual(stored.knowledge_time, VALIDATED_UTC)
            self.assertEqual(stored.knowledge_time.utcoffset(), timedelta(0))
            self.assertIs(
                stored.manifest.acquisition_mode,
                AcquisitionMode.UNVERIFIED_MANUAL_FILE,
            )
            self.assertIs(stored.manifest.readiness, ReferenceReadiness.COLLECTION_ONLY)
            self.assertFalse(stored.manifest.actionable)
            self.assertEqual(stored.manifest.event_count, 4)
            self.assertEqual(
                {path.name for path in stored.path.iterdir()},
                {"manifest.json", "source.bin", "declaration.json", "normalized.json"},
            )
            verify_stored_calendar_source_provenance(stored)

    def test_future_claimed_issue_date_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / SOURCE_NAME
            declaration = root / DECLARATION_NAME
            value = declaration_value()
            value["claimed_issue_date"] = "2026-07-16"
            source.write_bytes(SOURCE_BYTES)
            declaration.write_bytes(declaration_bytes(value=value))

            with self.assertRaisesRegex(
                CalendarSourceArtifactIntegrityError,
                "after local observation",
            ):
                self.store(root).import_source(source, declaration)

    def test_reimport_is_idempotent_and_preserves_first_availability(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source, declaration, _ = self.write_inputs(root)
            store = LocalCalendarSourceArtifactStore(
                root / "archive",
                clock=clock_sequence(
                    FIRST_SEEN_IST,
                    VALIDATED_IST,
                    FIRST_SEEN_IST + timedelta(days=1),
                    VALIDATED_IST + timedelta(days=1),
                ),
            )

            first = store.import_source(source, declaration)
            second = store.import_source(source, declaration)

            self.assertEqual(first.path, second.path)
            self.assertEqual(first.manifest, second.manifest)
            self.assertEqual(second.manifest.first_seen_at, FIRST_SEEN_UTC)

    def test_manual_manifest_cannot_be_promoted_or_made_actionable(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source, declaration, _ = self.write_inputs(root)
            stored = self.store(root).import_source(source, declaration)

            with self.assertRaisesRegex(ValueError, "collection-only"):
                replace(
                    stored.manifest,
                    readiness=ReferenceReadiness.POINT_IN_TIME_VERIFIED,
                )
            with self.assertRaisesRegex(ValueError, "collection-only"):
                replace(stored.manifest, actionable=True)

    def test_raw_declaration_normalized_manifest_and_extra_tampering_fail(self) -> None:
        mutations = (
            lambda path: (path / "source.bin").write_bytes(b"tampered"),
            lambda path: (path / "declaration.json").write_bytes(b"{}"),
            lambda path: (path / "normalized.json").write_bytes(b"{}"),
            self._tamper_manifest,
            lambda path: (path / "extra.txt").write_text("extra", encoding="utf-8"),
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temp_dir:
                root = Path(temp_dir)
                source, declaration, _ = self.write_inputs(root)
                store = self.store(root)
                stored = store.import_source(source, declaration)
                mutation(stored.path)

                with self.assertRaises(CalendarSourceArtifactIntegrityError):
                    store.get(stored.manifest.artifact_id)

    @staticmethod
    def _tamper_manifest(path: Path) -> None:
        manifest_path = path / "manifest.json"
        value = json.loads(manifest_path.read_text(encoding="utf-8"))
        value["validated_at"] = "2020-01-01T00:00:00+00:00"
        manifest_path.write_text(json.dumps(value), encoding="utf-8")

    def test_provenance_rejects_an_in_memory_payload_substitution(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source, declaration, _ = self.write_inputs(root)
            stored = self.store(root).import_source(source, declaration)
            substituted = replace(stored, normalized_bytes=b"{}")

            with self.assertRaisesRegex(
                CalendarSourceArtifactIntegrityError,
                "memory graph",
            ):
                verify_stored_calendar_source_provenance(substituted)

    def test_duplicate_availability_partition_is_ambiguous(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source, declaration, _ = self.write_inputs(root)
            store = self.store(root)
            stored = store.import_source(source, declaration)
            duplicate = stored.path.parent.parent / "2099-01-01" / stored.path.name
            duplicate.parent.mkdir()
            shutil.copytree(stored.path, duplicate)

            with self.assertRaises(CalendarSourceArtifactConflict):
                store.get(stored.manifest.artifact_id)


if __name__ == "__main__":
    unittest.main()
