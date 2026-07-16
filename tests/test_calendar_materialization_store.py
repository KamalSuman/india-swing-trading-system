from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from india_swing.calendar_data.artifact_store import (
    LocalCalendarSourceArtifactStore,
)
from india_swing.calendar_data.materialization import (
    materialize_collection_calendar,
)
from india_swing.calendar_data.materialization_store import (
    CalendarMaterializationStoreConflict,
    CalendarMaterializationStoreIntegrityError,
    LocalCalendarMaterializationStore,
)
from india_swing.calendar_data.models import (
    CALENDAR_DECLARATION_SCHEMA_VERSION,
)
from india_swing.calendar_evidence import build_observed_market_date_artifact
from tests.test_calendar_evidence import stored_bundle


UTC = timezone.utc
START = date(2026, 7, 17)
END = date(2026, 7, 19)
SOURCE_KNOWN = datetime(2026, 7, 15, 13, 0, tzinfo=UTC)
EVIDENCE_KNOWN = datetime(2026, 7, 17, 14, 35, tzinfo=UTC)
EVIDENCE_CUTOFF = datetime(2026, 7, 17, 15, 0, tzinfo=UTC)
MATERIALIZATION_CUTOFF = datetime(2026, 7, 17, 16, 0, tzinfo=UTC)


class CalendarMaterializationStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.calendar_root = self.root / "calendar"
        self.daily_root = self.root / "daily"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def source(self):
        source_name = "CMTR-BASE-2026.pdf"
        declaration_name = "CMTR-BASE-2026.events.json"
        source_bytes = b"%PDF-1.7\nCMTR-BASE-2026\n%%EOF\n"
        inputs = self.root / "inputs"
        inputs.mkdir(exist_ok=True)
        source_path = inputs / source_name
        declaration_path = inputs / declaration_name
        source_path.write_bytes(source_bytes)
        declaration_path.write_text(
            json.dumps(
                {
                    "schema_version": CALENDAR_DECLARATION_SCHEMA_VERSION,
                    "exchange": "NSE",
                    "segment": "CM",
                    "claimed_authority": "NSE",
                    "claimed_document_id": "CMTR-BASE-2026",
                    "claimed_issue_date": "2026-01-01",
                    "claimed_source_url": "https://example.invalid/CMTR-BASE-2026.pdf",
                    "source_filename": source_name,
                    "source_media_type": "application/pdf",
                    "source_byte_count": len(source_bytes),
                    "source_sha256": hashlib.sha256(source_bytes).hexdigest(),
                    "events": [
                        {
                            "event_type": "BASE_WEEKLY_SCHEDULE",
                            "effective_from": "2026-01-01",
                            "effective_to_exclusive": "2027-01-01",
                            "weekdays": ["MON", "TUE", "WED", "THU", "FRI"],
                            "windows": [
                                {
                                    "phase": "LIVE_CONTINUOUS",
                                    "opens": "09:15:00",
                                    "closes": "15:30:00",
                                }
                            ],
                            "supersedes_event_ids": [],
                            "source_locator": {
                                "page": 1,
                                "section": "CM schedule",
                                "record": "regular",
                            },
                            "reason": "Regular capital-market schedule",
                        }
                    ],
                },
                separators=(",", ":"),
            ),
            encoding="utf-8",
        )
        times = iter((SOURCE_KNOWN - timedelta(seconds=1), SOURCE_KNOWN))
        return LocalCalendarSourceArtifactStore(
            self.calendar_root,
            clock=lambda: next(times),
        ).import_source(source_path, declaration_path)

    def materialization(self, *, with_evidence: bool = True):
        source = self.source()
        evidence = ()
        if with_evidence:
            bundle = stored_bundle(
                (START,),
                storage_root=self.daily_root,
                first_seen=EVIDENCE_KNOWN - timedelta(seconds=1),
                validated=EVIDENCE_KNOWN,
            )
            evidence = (
                build_observed_market_date_artifact(
                    bundle,
                    cutoff=EVIDENCE_CUTOFF,
                ),
            )
        return materialize_collection_calendar(
            sources=(source,),
            coverage_start=START,
            coverage_end=END,
            cutoff=MATERIALIZATION_CUTOFF,
            observed_date_artifacts=evidence,
        )

    def store(self) -> LocalCalendarMaterializationStore:
        return LocalCalendarMaterializationStore(
            self.calendar_root,
            self.daily_root / "archive",
        )

    def test_round_trip_replays_sources_and_preserves_evidence_cutoff(self) -> None:
        materialization = self.materialization()
        stored = self.store().put(materialization)
        loaded = self.store().get(materialization.materialization_id)

        self.assertEqual(stored, loaded)
        self.assertEqual(loaded.materialization, materialization)
        binding = loaded.manifest.observed_evidence_bindings[0]
        self.assertEqual(binding.knowledge_time, EVIDENCE_KNOWN)
        self.assertEqual(binding.cutoff, EVIDENCE_CUTOFF)
        self.assertGreater(binding.cutoff, binding.knowledge_time)
        self.assertFalse(loaded.manifest.actionable)

    def test_payload_tampering_is_rejected(self) -> None:
        stored = self.store().put(self.materialization(with_evidence=False))
        (stored.path / "materialization.json").write_bytes(b"{}")

        with self.assertRaisesRegex(
            CalendarMaterializationStoreIntegrityError,
            "bytes fail",
        ):
            self.store().get(stored.manifest.artifact_id)

    def test_manifest_tampering_is_rejected(self) -> None:
        stored = self.store().put(self.materialization(with_evidence=False))
        path = stored.path / "manifest.json"
        value = json.loads(path.read_text(encoding="utf-8"))
        value["session_count"] += 1
        path.write_text(
            json.dumps(value, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        with self.assertRaises(CalendarMaterializationStoreIntegrityError):
            self.store().get(stored.manifest.artifact_id)

    def test_duplicate_partition_is_ambiguous(self) -> None:
        stored = self.store().put(self.materialization(with_evidence=False))
        duplicate_parent = stored.path.parent.parent / "2099-01-01"
        duplicate_parent.mkdir()
        shutil.copytree(stored.path, duplicate_parent / stored.path.name)

        with self.assertRaises(CalendarMaterializationStoreConflict):
            self.store().get(stored.manifest.artifact_id)

    def test_in_memory_resolution_substitution_is_rejected(self) -> None:
        materialization = self.materialization(with_evidence=False)
        object.__setattr__(
            materialization.day_resolutions[0],
            "source_snapshot_id",
            "f" * 64,
        )

        with self.assertRaises(CalendarMaterializationStoreIntegrityError):
            self.store().put(materialization)


if __name__ == "__main__":
    unittest.main()
