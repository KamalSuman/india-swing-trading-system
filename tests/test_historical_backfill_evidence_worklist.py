from __future__ import annotations

import csv
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from india_swing.identity_registry import (
    IdentityAdjudicationRequirement,
    build_identity_adjudication_queue,
)
from india_swing.market_data.backfill import (
    HistoricalBackfillIssueCode,
    UpstoxIsinInstrumentResolver,
    build_historical_backfill_plan,
)
from india_swing.market_data.backfill_blockers import (
    LocalHistoricalBackfillBlockerReportStore,
    build_historical_backfill_blocker_report,
)
from india_swing.market_data.backfill_cli import main as backfill_main
from india_swing.market_data.backfill_evidence_worklist import (
    PACKAGE_FILENAME,
    WORKLIST_COLUMNS,
    WORKLIST_FILENAME,
    HistoricalBackfillEvidenceDocumentNeed,
    HistoricalBackfillEvidenceWorklistIntegrityError,
    LocalHistoricalBackfillEvidenceWorkPackageStore,
    build_historical_backfill_evidence_work_package,
    decode_historical_backfill_evidence_work_package,
    encode_historical_backfill_evidence_work_package,
    encode_historical_backfill_evidence_worklist_csv,
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


UTC = timezone.utc
BLOCKER_GENERATED_AT = datetime(2026, 7, 23, 16, 0, tzinfo=UTC)
PACKAGE_GENERATED_AT = datetime(2026, 7, 23, 16, 5, tzinfo=UTC)


def evidence_context(root: Path, *, include_missing_day: bool = False):
    invalid = security_row(
        TckrSymb="NOISIN",
        FinInstrmId="7000",
        ISIN="NOTANISIN",
    )
    identity = registry(root, [invalid], [invalid])
    selected_calendar = (
        calendar(DAY_ZERO, DAY_TWO)
        if include_missing_day
        else calendar()
    )
    plan = build_historical_backfill_plan(
        registry=identity,
        security_master_sources=security_master_sources(root, identity),
        calendar=selected_calendar,
        resolver=UpstoxIsinInstrumentResolver(),
        coverage_start=(
            DAY_ZERO if include_missing_day else DAY_ONE
        ),
        coverage_end=DAY_TWO,
        requested_at=REQUESTED_AT,
    )
    queue = build_identity_adjudication_queue(identity)
    report = build_historical_backfill_blocker_report(
        plan=plan,
        registry=identity,
        adjudication_queue=queue,
        generated_at=BLOCKER_GENERATED_AT,
    )
    package = build_historical_backfill_evidence_work_package(
        blocker_report=report,
        registry=identity,
        adjudication_queue=queue,
        generated_at=PACKAGE_GENERATED_AT,
    )
    return identity, queue, report, package


class HistoricalBackfillEvidenceWorklistTests(unittest.TestCase):
    def test_package_groups_blockers_by_exact_adjudication_case(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            identity, queue, report, package = evidence_context(
                Path(temp_dir)
            )

        self.assertEqual(package.blocker_report_id, report.report_id)
        self.assertEqual(package.identity_registry_id, identity.registry_id)
        self.assertEqual(package.adjudication_queue_id, queue.queue_id)
        self.assertEqual(package.candidate_count, 2)
        self.assertEqual(package.observation_count, 2)
        self.assertEqual(package.operational_requests, ())
        self.assertEqual(
            {value.adjudication_case_id for value in package.case_requests},
            {value.case_id for value in queue.cases},
        )
        self.assertTrue(
            all(
                IdentityAdjudicationRequirement.VALIDATED_IDENTIFIER
                in value.requirements
                for value in package.case_requests
            )
        )
        self.assertTrue(
            all(
                HistoricalBackfillEvidenceDocumentNeed.NSE_ADJACENT_DATED_SECURITY_MASTER
                in value.document_needs
                and HistoricalBackfillEvidenceDocumentNeed.NSE_LISTING_CIRCULAR_PDF
                in value.document_needs
                for value in package.case_requests
            )
        )
        self.assertFalse(package.actionable)
        self.assertFalse(package.evidence_satisfied)

    def test_case_includes_all_observations_but_marks_direct_blockers(self) -> None:
        duplicate_series = [
            security_row(),
            security_row(
                TckrSymb="INFYALT",
                FinInstrmId="2000",
            ),
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            identity = registry(
                Path(temp_dir),
                duplicate_series,
                duplicate_series,
            )
            plan = build_historical_backfill_plan(
                registry=identity,
                security_master_sources=security_master_sources(
                    Path(temp_dir), identity
                ),
                calendar=calendar(),
                resolver=UpstoxIsinInstrumentResolver(),
                coverage_start=DAY_ONE,
                coverage_end=DAY_TWO,
                requested_at=REQUESTED_AT,
            )
            queue = build_identity_adjudication_queue(identity)
            report = build_historical_backfill_blocker_report(
                plan=plan,
                registry=identity,
                adjudication_queue=queue,
                generated_at=BLOCKER_GENERATED_AT,
            )
            package = build_historical_backfill_evidence_work_package(
                blocker_report=report,
                registry=identity,
                adjudication_queue=queue,
                generated_at=PACKAGE_GENERATED_AT,
            )

        self.assertEqual(package.candidate_count, 1)
        request = package.case_requests[0]
        self.assertEqual(len(request.observations), 4)
        self.assertTrue(all(value.directly_blocked for value in request.observations))
        self.assertEqual(
            {value.issue_code for value in report.entries},
            {HistoricalBackfillIssueCode.CONFLICTING_IDENTITY},
        )

    def test_operational_missing_vintage_is_not_forced_into_fake_case(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            _, _, _, package = evidence_context(
                Path(temp_dir),
                include_missing_day=True,
            )

        self.assertEqual(len(package.operational_requests), 1)
        request = package.operational_requests[0]
        self.assertEqual(
            request.issue_code,
            HistoricalBackfillIssueCode.MISSING_SECURITY_MASTER_VINTAGE,
        )
        self.assertEqual(request.affected_dates, (DAY_ZERO,))
        self.assertIn(
            HistoricalBackfillEvidenceDocumentNeed.NSE_DATED_SECURITY_MASTER,
            request.document_needs,
        )

    def test_json_and_csv_are_canonical_and_complete(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            _, _, _, package = evidence_context(Path(temp_dir))

        encoded = encode_historical_backfill_evidence_work_package(package)
        self.assertEqual(
            decode_historical_backfill_evidence_work_package(encoded),
            package,
        )
        csv_bytes = encode_historical_backfill_evidence_worklist_csv(
            package
        )
        rows = list(
            csv.DictReader(
                io.StringIO(csv_bytes.decode("utf-8"), newline="")
            )
        )
        self.assertEqual(tuple(rows[0]), WORKLIST_COLUMNS)
        self.assertEqual(
            len(rows),
            package.requirement_pair_count,
        )
        self.assertTrue(
            all(
                value["evidence_collected"] == "false"
                and value["review_completed"] == "false"
                for value in rows
            )
        )

    def test_store_detects_json_and_csv_tampering(self) -> None:
        for filename in (PACKAGE_FILENAME, WORKLIST_FILENAME):
            with self.subTest(filename=filename):
                with tempfile.TemporaryDirectory() as temp_dir:
                    root = Path(temp_dir)
                    _, _, _, package = evidence_context(root / "inputs")
                    store = LocalHistoricalBackfillEvidenceWorkPackageStore(
                        root / "work"
                    )
                    stored = store.put(package)
                    self.assertEqual(
                        store.get(stored.package_id),
                        package,
                    )
                    path = store.path_for(package.package_id) / filename
                    path.write_bytes(path.read_bytes() + b" ")
                    with self.assertRaises(
                        HistoricalBackfillEvidenceWorklistIntegrityError
                    ):
                        store.get(package.package_id)

    def test_nested_observation_tampering_is_detected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            _, _, _, package = evidence_context(Path(temp_dir))
        object.__setattr__(
            package.case_requests[0].observations[0],
            "ticker_symbol",
            "FORGED",
        )

        with self.assertRaises(
            HistoricalBackfillEvidenceWorklistIntegrityError
        ):
            package.verify_content_identity()

    def test_cli_generates_credential_free_worklist_from_exact_report(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            identity, queue, report, _ = evidence_context(root / "inputs")
            market_root = root / "market"
            LocalHistoricalBackfillBlockerReportStore(market_root).put(report)
            identity_store = SimpleNamespace(
                root=root / "identity",
                get=lambda _: SimpleNamespace(registry=identity),
            )
            queue_store = SimpleNamespace(get=lambda _: queue)
            output = io.StringIO()
            with (
                patch.dict(
                    "os.environ",
                    {
                        "INDIA_SWING_MARKET_DATA_ROOT": str(market_root),
                        "INDIA_SWING_IDENTITY_REGISTRY_ROOT": str(
                            root / "identity"
                        ),
                        "INDIA_SWING_REFERENCE_DATA_ROOT": str(
                            root / "reference"
                        ),
                    },
                    clear=False,
                ),
                patch(
                    "india_swing.market_data.backfill_cli.LocalIdentityRegistryStore",
                    return_value=identity_store,
                ),
                patch(
                    "india_swing.market_data.backfill_cli.LocalIdentityAdjudicationQueueStore",
                    return_value=queue_store,
                ),
                patch(
                    "india_swing.market_data.backfill_cli.UpstoxCredentials.from_env",
                    side_effect=AssertionError(
                        "credentials must not be read"
                    ),
                ),
                redirect_stdout(output),
            ):
                exit_code = backfill_main(
                    [
                        "evidence-worklist",
                        "--blocker-report-id",
                        report.report_id,
                    ]
                )

            payload = json.loads(output.getvalue())
            self.assertEqual(exit_code, 0)
            self.assertEqual(
                payload["status"],
                "BACKFILL_EVIDENCE_WORKLIST_READY",
            )
            self.assertEqual(payload["candidate_count"], 2)
            self.assertFalse(payload["actionable"])
            self.assertFalse(payload["evidence_satisfied"])
            self.assertTrue(Path(payload["csv_path"]).is_file())


if __name__ == "__main__":
    unittest.main()
