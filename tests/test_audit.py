from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from india_swing.audit import (
    AuditExistsError,
    AuditIntegrityError,
    AuditReader,
    AuditWriter,
    InvalidAuditRunId,
)


class AuditTests(unittest.TestCase):
    def test_write_read_round_trip_verifies_hash(self) -> None:
        payload = {
            "run_date": date(2026, 7, 15),
            "decision_time": datetime(2026, 7, 15, 17, 0, tzinfo=timezone.utc),
            "expected_r": Decimal("0.42"),
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            path = AuditWriter().write(output_dir, "run_2026-07-15", payload)
            envelope = AuditReader().read(output_dir, "run_2026-07-15")

        self.assertEqual(path.name, "run_2026-07-15.json")
        self.assertEqual(envelope["schema_version"], "audit-v1")
        self.assertEqual(envelope["payload"]["run_date"], "2026-07-15")
        self.assertEqual(envelope["payload"]["expected_r"], "0.42")
        self.assertEqual(len(envelope["audit_hash"]), 64)

    def test_path_traversal_run_ids_are_rejected_for_read_and_write(self) -> None:
        invalid_run_ids = (
            "",
            ".",
            "..",
            "../escape",
            "..\\escape",
            "nested/escape",
            "/absolute",
            "C:\\escape",
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            for run_id in invalid_run_ids:
                with self.subTest(run_id=run_id):
                    with self.assertRaises(InvalidAuditRunId):
                        AuditWriter().write(output_dir, run_id, {})
                    with self.assertRaises(InvalidAuditRunId):
                        AuditReader().read(output_dir, run_id)
            self.assertEqual(list(output_dir.iterdir()), [])

    def test_reader_detects_payload_tampering(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            path = AuditWriter().write(output_dir, "run-1", {"decision": "BUY"})
            envelope = json.loads(path.read_text(encoding="utf-8"))
            envelope["payload"]["decision"] = "NO_TRADE"
            path.write_text(json.dumps(envelope), encoding="utf-8")

            with self.assertRaisesRegex(AuditIntegrityError, "hash verification failed"):
                AuditReader().read(output_dir, "run-1")

    def test_existing_record_is_never_overwritten(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            path = AuditWriter().write(output_dir, "run-1", {"decision": "BUY"})
            original = path.read_bytes()

            with self.assertRaises(AuditExistsError):
                AuditWriter().write(output_dir, "run-1", {"decision": "NO_TRADE"})

            self.assertEqual(path.read_bytes(), original)
            self.assertEqual(AuditReader().read(output_dir, "run-1")["payload"], {"decision": "BUY"})

    def test_failed_atomic_publish_leaves_no_partial_record(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            with patch("india_swing.audit.os.link", side_effect=OSError("publish failed")):
                with self.assertRaisesRegex(OSError, "publish failed"):
                    AuditWriter().write(output_dir, "run-1", {"decision": "BUY"})

            self.assertEqual(list(output_dir.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
