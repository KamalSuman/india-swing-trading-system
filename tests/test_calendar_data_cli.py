from __future__ import annotations

import hashlib
import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from india_swing.calendar_data.cli import main
from india_swing.calendar_data.config import CALENDAR_DATA_ROOT_ENV
from india_swing.calendar_data.models import CALENDAR_DECLARATION_SCHEMA_VERSION


class CalendarDataCliTests(unittest.TestCase):
    def test_source_import_then_materialize(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / "CMTR-BASE-2026.pdf"
            declaration = root / "CMTR-BASE-2026.events.json"
            source_bytes = b"%PDF-1.7\nCMTR-BASE-2026\n%%EOF\n"
            source.write_bytes(source_bytes)
            declaration.write_text(
                json.dumps(
                    {
                        "schema_version": CALENDAR_DECLARATION_SCHEMA_VERSION,
                        "exchange": "NSE",
                        "segment": "CM",
                        "claimed_authority": "NSE",
                        "claimed_document_id": "CMTR-BASE-2026",
                        "claimed_issue_date": "2026-01-01",
                        "claimed_source_url": "https://example.invalid/base.pdf",
                        "source_filename": source.name,
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
            environment = {
                CALENDAR_DATA_ROOT_ENV: str(root / "archive"),
                "INDIA_SWING_DAILY_REPORTS_ROOT": str(root / "daily"),
            }
            with patch.dict(os.environ, environment, clear=False):
                output = io.StringIO()
                with redirect_stdout(output):
                    self.assertEqual(
                        main(
                            [
                                "source-import",
                                "--source-pdf",
                                str(source),
                                "--declaration",
                                str(declaration),
                            ]
                        ),
                        0,
                    )
                source_response = json.loads(output.getvalue())

                output = io.StringIO()
                with redirect_stdout(output):
                    self.assertEqual(
                        main(
                            [
                                "materialize",
                                "--source-id",
                                source_response["artifact_id"],
                                "--coverage-start",
                                "2026-07-17",
                                "--coverage-end",
                                "2026-07-19",
                                "--cutoff",
                                "2027-01-01T16:00:00+00:00",
                            ]
                        ),
                        0,
                    )
                response = json.loads(output.getvalue())

            self.assertEqual(response["day_count"], 3)
            self.assertEqual(response["session_count"], 1)
            self.assertEqual(response["readiness"], "COLLECTION_ONLY")
            self.assertFalse(response["actionable"])

    def test_errors_are_sanitized(self) -> None:
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            self.assertEqual(main(["materialize"]), 2)
        value = json.loads(stderr.getvalue())
        self.assertEqual(value["status"], "FAILED")
        self.assertEqual(value["error_type"], "CalendarDataArgumentError")
        self.assertNotIn("usage:", stderr.getvalue().lower())


if __name__ == "__main__":
    unittest.main()
