from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from india_swing.identity_registry import build_identity_adjudication_queue
from india_swing.market_data.backfill import (
    HistoricalBackfillIssueCode,
    UpstoxIsinInstrumentResolver,
    build_historical_backfill_plan,
)
from india_swing.market_data.backfill_blockers import (
    REPORT_FILENAME,
    HistoricalBackfillBlockerAction,
    HistoricalBackfillBlockerIntegrityError,
    LocalHistoricalBackfillBlockerReportStore,
    build_historical_backfill_blocker_report,
)
from tests.test_historical_backfill import (
    DAY_ONE,
    DAY_TWO,
    DAY_ZERO,
    REQUESTED_AT,
    calendar,
    registry,
    security_master_sources,
)
from tests.test_identity_registry import security_row


GENERATED_AT = datetime(2026, 7, 23, 16, 0, tzinfo=timezone.utc)


def blocker_report(root: Path):
    invalid = security_row(
        TckrSymb="NOISIN",
        FinInstrmId="7000",
        ISIN="NOTANISIN",
    )
    identity = registry(root, [invalid], [invalid])
    value = build_historical_backfill_plan(
        registry=identity,
        security_master_sources=security_master_sources(root, identity),
        calendar=calendar(),
        resolver=UpstoxIsinInstrumentResolver(),
        coverage_start=DAY_ONE,
        coverage_end=DAY_TWO,
        requested_at=REQUESTED_AT,
    )
    queue = build_identity_adjudication_queue(identity)
    return build_historical_backfill_blocker_report(
        plan=value,
        registry=identity,
        adjudication_queue=queue,
        generated_at=GENERATED_AT,
    )


class HistoricalBackfillBlockerReportTests(unittest.TestCase):
    def test_report_routes_only_blocking_issues_to_existing_cases(self) -> None:
        unsupported = security_row(
            TckrSymb="BLOCKLANE",
            FinInstrmId="8001",
            ISIN="INE002A01018",
            SctySrs="BL",
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            identity = registry(
                Path(temp_dir),
                [security_row(), unsupported],
                [security_row(), unsupported],
            )
            value = build_historical_backfill_plan(
                registry=identity,
                security_master_sources=security_master_sources(
                    Path(temp_dir), identity
                ),
                calendar=calendar(DAY_ZERO, DAY_TWO),
                resolver=UpstoxIsinInstrumentResolver(),
                coverage_start=DAY_ZERO,
                coverage_end=DAY_TWO,
                requested_at=REQUESTED_AT,
            )
            report = build_historical_backfill_blocker_report(
                plan=value,
                registry=identity,
                adjudication_queue=build_identity_adjudication_queue(identity),
                generated_at=GENERATED_AT,
            )

        self.assertEqual(report.record_count, 1)
        self.assertEqual(
            report.entries[0].issue_code,
            HistoricalBackfillIssueCode.MISSING_SECURITY_MASTER_VINTAGE,
        )
        self.assertEqual(report.entries[0].affected_dates, (DAY_ZERO,))
        self.assertEqual(report.entries[0].observation_ids, ())
        self.assertEqual(report.entries[0].candidate_ids, ())
        self.assertIn(
            HistoricalBackfillBlockerAction.SUPPLY_DATED_SECURITY_MASTER,
            report.entries[0].actions,
        )
        self.assertFalse(report.actionable)
        self.assertFalse(report.evidence_satisfied)

    def test_unvalidated_identifier_reuses_sealed_adjudication_cases(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            report = blocker_report(Path(temp_dir))

        self.assertEqual(report.record_count, 2)
        self.assertEqual(report.candidate_count, 2)
        self.assertEqual(report.adjudication_case_count, 2)
        self.assertEqual(
            {value.issue_code for value in report.entries},
            {HistoricalBackfillIssueCode.UNVALIDATED_IDENTIFIER},
        )
        self.assertTrue(
            all(value.candidate_ids for value in report.entries)
        )
        self.assertTrue(
            all(value.adjudication_case_ids for value in report.entries)
        )

    def test_store_round_trip_is_canonical_and_tamper_evident(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            report = blocker_report(root / "inputs")
            store = LocalHistoricalBackfillBlockerReportStore(root / "reports")
            stored = store.put(report)
            self.assertEqual(store.get(stored.report_id), report)
            path = (
                store.dataset_root
                / report.report_id
                / REPORT_FILENAME
            )
            path.write_bytes(path.read_bytes() + b" ")
            with self.assertRaises(HistoricalBackfillBlockerIntegrityError):
                store.get(report.report_id)

    def test_nested_entry_tampering_is_detected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            report = blocker_report(Path(temp_dir))
        object.__setattr__(
            report.entries[0],
            "issue_id",
            "0" * 64,
        )

        with self.assertRaises(HistoricalBackfillBlockerIntegrityError):
            report.verify_content_identity()


if __name__ == "__main__":
    unittest.main()
