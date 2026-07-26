from __future__ import annotations

import json
import os
import tempfile
import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from india_swing.market_data.backfill import (
    HistoricalBackfillRunner,
    LocalHistoricalBackfillProgressStore,
)
from india_swing.market_data.backfill_gaps import (
    HistoricalBackfillGapClassification,
    HistoricalBackfillSessionGapEvidence,
    LocalHistoricalBackfillSessionGapStore,
)
from india_swing.market_data.collection import historical_dataset_name
from india_swing.market_data.dataset_admission import (
    MAXIMUM_DATASET_ADMISSION_REPORT_BYTES,
    REPORT_FILENAME,
    HistoricalDatasetAdmissionDisposition,
    HistoricalDatasetAdmissionError,
    HistoricalDatasetAdmissionIntegrityError,
    HistoricalDatasetAdmissionReport,
    LocalHistoricalDatasetAdmissionReportStore,
    build_historical_dataset_admission_report,
    decode_historical_dataset_admission_report,
    encode_historical_dataset_admission_report,
)
from india_swing.market_data.gap_adjudication import (
    build_historical_gap_adjudication_report,
)
from india_swing.market_data.reconciliation import (
    HISTORICAL_RECONCILIATION_POLICY_VERSION,
    HISTORICAL_RECONCILIATION_SCHEMA_VERSION,
    HistoricalCandleDifference,
    HistoricalCandleReconciliationReport,
    HistoricalCandleReconciliationRow,
    HistoricalReconciliationStatus,
    reconcile_historical_batch,
)
from india_swing.market_data.snapshot_store import LocalMarketSnapshotStore
from tests.test_historical_backfill import (
    DAY_ONE,
    DAY_TWO,
    DAY_ZERO,
    REQUESTED_AT,
    RUN_CLOCK,
    calendar,
    plan,
    two_session_body,
)
from tests.test_historical_backfill_gaps import gap_evidence
from tests.test_historical_reconciliation import RECONCILED_AT, nse_artifact
from tests.test_identity_registry import security_row
from tests.test_upstox_market_data import (
    FakeTransport,
    adapter as upstox_adapter,
    candle_row,
    response,
    success_body,
)


UTC = timezone.utc
ASSESSED_AT = datetime(2026, 7, 23, 10, 0, tzinfo=UTC)


def single_request_plan(root: Path, **overrides):
    values = dict(
        first_rows=[security_row()],
        second_rows=[security_row()],
        selected_calendar=calendar(DAY_ONE, DAY_ONE),
        coverage_start=DAY_ONE,
        coverage_end=DAY_ONE,
    )
    values.update(overrides)
    return plan(root, **values)


def matching_candle_body() -> bytes:
    return success_body(
        [
            candle_row(
                DAY_ONE,
                open_value=1600.0,
                high=1620.0,
                low=1590.0,
                close=1610.0,
                volume=100,
            )
        ]
    )


def run_single_request(
    value,
    root: Path,
    *,
    body: bytes | None = None,
    clock=lambda: RUN_CLOCK,
):
    snapshot_store = LocalMarketSnapshotStore(root / "snapshots")
    progress_store = LocalHistoricalBackfillProgressStore(root / "progress")
    transport = FakeTransport(response(body if body is not None else matching_candle_body()))
    connector = upstox_adapter(transport)
    runner = HistoricalBackfillRunner(
        connector, snapshot_store, progress_store, clock=clock
    )
    progress = runner.run(value)
    return progress, snapshot_store


def admitted_fixture(root: Path):
    """A fully admitted single-request plan: completion + snapshot + passing reconciliation."""

    value = single_request_plan(root / "inputs")
    progress, snapshot_store = run_single_request(value, root)
    completion = progress.completions[0]
    stored = snapshot_store.get(
        historical_dataset_name(value.provider), completion.snapshot_id
    )
    batch = stored.normalized_payload
    artifact = nse_artifact(root / "nse")
    report = reconcile_historical_batch(batch, (artifact,), reconciled_at=RECONCILED_AT)
    return value, progress, stored, report


def build_admitted_report(root: Path, **overrides):
    value, progress, stored, report = admitted_fixture(root)
    values = dict(
        plan=value,
        progress=progress,
        snapshots=(stored,),
        reconciliations=(report,),
        gaps=(),
        gap_adjudication=None,
        assessed_at=ASSESSED_AT,
    )
    values.update(overrides)
    return build_historical_dataset_admission_report(**values)


def blocking_issue_admitted_report(root: Path) -> HistoricalDatasetAdmissionReport:
    """A fully admitted request whose plan also carries one real blocking issue."""

    value = single_request_plan(
        root / "inputs",
        selected_calendar=calendar(DAY_ZERO, DAY_ONE),
        coverage_start=DAY_ZERO,
        coverage_end=DAY_ONE,
    )
    progress, snapshot_store = run_single_request(value, root)
    completion = progress.completions[0]
    stored = snapshot_store.get(
        historical_dataset_name(value.provider), completion.snapshot_id
    )
    artifact = nse_artifact(root / "nse")
    report = reconcile_historical_batch(
        stored.normalized_payload, (artifact,), reconciled_at=RECONCILED_AT
    )
    return build_historical_dataset_admission_report(
        plan=value,
        progress=progress,
        snapshots=(stored,),
        reconciliations=(report,),
        gaps=(),
        gap_adjudication=None,
        assessed_at=ASSESSED_AT,
    )


def run_multi_request_plan(value, root: Path, *, clock=lambda: RUN_CLOCK):
    snapshot_store = LocalMarketSnapshotStore(root / "snapshots")
    progress_store = LocalHistoricalBackfillProgressStore(root / "progress")
    transport = FakeTransport(*(response(two_session_body()) for _ in value.requests))
    connector = upstox_adapter(transport)
    runner = HistoricalBackfillRunner(
        connector, snapshot_store, progress_store, clock=clock
    )
    progress = runner.run(value)
    return progress, snapshot_store


def _match_row(
    session: date, *, artifact_id: str = "a" * 64, bar_id: str = "b" * 64
) -> HistoricalCandleReconciliationRow:
    return HistoricalCandleReconciliationRow(
        session=session,
        nse_artifact_id=artifact_id,
        nse_bar_id=bar_id,
        status=HistoricalReconciliationStatus.MATCH,
        differences=(),
    )


def _mismatch_row(
    session: date, *, artifact_id: str = "a" * 64, bar_id: str = "b" * 64
) -> HistoricalCandleReconciliationRow:
    return HistoricalCandleReconciliationRow(
        session=session,
        nse_artifact_id=artifact_id,
        nse_bar_id=bar_id,
        status=HistoricalReconciliationStatus.MISMATCH,
        differences=(
            HistoricalCandleDifference(
                field_name="close", provider_value="1600.0", nse_value="1610.0"
            ),
        ),
    )


def _missing_row(
    session: date, *, artifact_id: str = "a" * 64
) -> HistoricalCandleReconciliationRow:
    return HistoricalCandleReconciliationRow(
        session=session,
        nse_artifact_id=artifact_id,
        nse_bar_id=None,
        status=HistoricalReconciliationStatus.MISSING_NSE_BAR,
        differences=(),
    )


def _synthetic_report(
    rows: list[HistoricalCandleReconciliationRow],
    *,
    historical_batch_id: str = "c" * 64,
    historical_request_id: str = "d" * 64,
    provider: str = "UPSTOX",
    provider_version: str = "upstox-test-connector/v1",
    listing_key: str = "NSE:INFY",
    security_series: str = "EQ",
    isin: str = "INE009A01021",
    reconciled_at: datetime = RECONCILED_AT,
    passed: bool | None = None,
    actionable: bool = False,
    schema_version: str | None = None,
    policy_version: str | None = None,
) -> HistoricalCandleReconciliationReport:
    if passed is None:
        passed = all(
            row.status is HistoricalReconciliationStatus.MATCH for row in rows
        )
    return HistoricalCandleReconciliationReport(
        historical_batch_id=historical_batch_id,
        historical_request_id=historical_request_id,
        provider=provider,
        provider_version=provider_version,
        listing_key=listing_key,
        security_series=security_series,
        isin=isin,
        reconciled_at=reconciled_at,
        rows=tuple(rows),
        passed=passed,
        actionable=actionable,
        schema_version=schema_version or HISTORICAL_RECONCILIATION_SCHEMA_VERSION,
        policy_version=policy_version or HISTORICAL_RECONCILIATION_POLICY_VERSION,
    )


class AdmissionDispositionTests(unittest.TestCase):
    def test_complete_request_is_admitted_only_when_everything_matches(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            value, progress, stored, report = admitted_fixture(Path(temp_dir))
            admission = build_historical_dataset_admission_report(
                plan=value,
                progress=progress,
                snapshots=(stored,),
                reconciliations=(report,),
                gaps=(),
                gap_adjudication=None,
                assessed_at=ASSESSED_AT,
            )

        self.assertEqual(len(admission.entries), 1)
        entry = admission.entries[0]
        self.assertEqual(
            entry.disposition, HistoricalDatasetAdmissionDisposition.ADMITTED
        )
        self.assertEqual(entry.snapshot_id, stored.manifest.snapshot_id)
        self.assertEqual(entry.historical_batch_id, stored.normalized_payload.batch_id)
        self.assertEqual(entry.reconciliation_report_id, report.report_id)
        self.assertIsNone(entry.gap_evidence_id)
        self.assertIsNone(entry.gap_adjudication_entry_id)
        self.assertTrue(admission.safe_requests_complete)
        self.assertTrue(admission.coverage_complete)
        self.assertEqual(admission.admitted_request_ids, (entry.request_id,))
        self.assertEqual(admission.admitted_snapshot_ids, (stored.manifest.snapshot_id,))
        self.assertEqual(
            admission.admitted_batch_ids, (stored.normalized_payload.batch_id,)
        )
        self.assertEqual(admission.admitted_request_count, 1)
        self.assertEqual(admission.blocked_request_count, 0)
        self.assertEqual(admission.total_request_count, 1)
        self.assertIs(admission.collection_only, True)
        self.assertIs(admission.actionable, False)
        self.assertIs(admission.training_eligible, False)

    def test_missing_completion_is_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value = single_request_plan(root / "inputs")
            progress_store = LocalHistoricalBackfillProgressStore(root / "progress")
            progress = progress_store.save(
                _empty_progress(value, "upstox-test-connector/v1")
            )
            admission = build_historical_dataset_admission_report(
                plan=value,
                progress=progress,
                snapshots=(),
                reconciliations=(),
                gaps=(),
                gap_adjudication=None,
                assessed_at=ASSESSED_AT,
            )

        entry = admission.entries[0]
        self.assertEqual(
            entry.disposition,
            HistoricalDatasetAdmissionDisposition.MISSING_COMPLETION,
        )
        self.assertIsNone(entry.snapshot_id)
        self.assertIsNone(entry.historical_batch_id)
        self.assertFalse(admission.safe_requests_complete)
        self.assertFalse(admission.coverage_complete)

    def test_missing_reconciliation_is_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value, progress, stored, _ = admitted_fixture(root)
            admission = build_historical_dataset_admission_report(
                plan=value,
                progress=progress,
                snapshots=(stored,),
                reconciliations=(),
                gaps=(),
                gap_adjudication=None,
                assessed_at=ASSESSED_AT,
            )

        entry = admission.entries[0]
        self.assertEqual(
            entry.disposition,
            HistoricalDatasetAdmissionDisposition.RECONCILIATION_MISSING_OR_FAILED,
        )
        self.assertIsNone(entry.reconciliation_report_id)
        self.assertIsNotNone(entry.snapshot_id)
        self.assertFalse(admission.safe_requests_complete)
        self.assertFalse(admission.coverage_complete)

    def test_mismatched_reconciliation_is_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value = single_request_plan(root / "inputs")
            progress, snapshot_store = run_single_request(
                value,
                root,
                body=success_body(
                    [
                        candle_row(
                            DAY_ONE,
                            open_value=1600.0,
                            high=1620.0,
                            low=1590.0,
                            close=1608.0,
                            volume=99,
                        )
                    ]
                ),
            )
            completion = progress.completions[0]
            stored = snapshot_store.get(
                historical_dataset_name(value.provider), completion.snapshot_id
            )
            artifact = nse_artifact(root / "nse")
            report = reconcile_historical_batch(
                stored.normalized_payload, (artifact,), reconciled_at=RECONCILED_AT
            )
            self.assertFalse(report.passed)
            admission = build_historical_dataset_admission_report(
                plan=value,
                progress=progress,
                snapshots=(stored,),
                reconciliations=(report,),
                gaps=(),
                gap_adjudication=None,
                assessed_at=ASSESSED_AT,
            )

        entry = admission.entries[0]
        self.assertEqual(
            entry.disposition,
            HistoricalDatasetAdmissionDisposition.RECONCILIATION_MISSING_OR_FAILED,
        )
        self.assertEqual(entry.reconciliation_report_id, report.report_id)
        self.assertFalse(admission.safe_requests_complete)

    def test_missing_nse_bar_reconciliation_is_blocked(self) -> None:
        # A genuine completed request whose real identity (TCS) is simply
        # absent from the pinned NSE artifact (which only carries INFY/FUND
        # bars), producing an honest MISSING_NSE_BAR row for the exact
        # completed request -- not a lineage mismatch.
        from tests.test_identity_registry import tcs_row

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value = single_request_plan(
                root / "inputs", first_rows=[tcs_row()], second_rows=[tcs_row()]
            )
            progress, snapshot_store = run_single_request(value, root)
            completion = progress.completions[0]
            stored = snapshot_store.get(
                historical_dataset_name(value.provider), completion.snapshot_id
            )
            artifact = nse_artifact(root / "nse")
            report = reconcile_historical_batch(
                stored.normalized_payload, (artifact,), reconciled_at=RECONCILED_AT
            )
            self.assertFalse(report.passed)

            admission = build_historical_dataset_admission_report(
                plan=value,
                progress=progress,
                snapshots=(stored,),
                reconciliations=(report,),
                gaps=(),
                gap_adjudication=None,
                assessed_at=ASSESSED_AT,
            )

        entry = admission.entries[0]
        self.assertEqual(
            entry.disposition,
            HistoricalDatasetAdmissionDisposition.RECONCILIATION_MISSING_OR_FAILED,
        )
        self.assertEqual(entry.reconciliation_report_id, report.report_id)
        self.assertFalse(admission.safe_requests_complete)

    def test_gap_without_adjudication_is_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value = single_request_plan(root / "inputs")
            request = value.requests[0]
            gap = HistoricalBackfillSessionGapEvidence(
                plan_id=value.plan_id,
                request_id=request.request_id,
                provider=value.provider,
                provider_version="upstox-test-connector/v1",
                provider_instrument_id=request.binding.provider_instrument_id,
                listing_key=request.binding.listing_key,
                security_series=request.binding.security_series,
                isin=request.binding.isin,
                session=request.sessions[0],
                response_observed_at=REQUESTED_AT + timedelta(hours=1),
                normalized_response_sha256="c" * 64,
            )
            progress_store = LocalHistoricalBackfillProgressStore(root / "progress")
            progress = progress_store.save(
                _empty_progress(value, "upstox-test-connector/v1")
            )
            admission = build_historical_dataset_admission_report(
                plan=value,
                progress=progress,
                snapshots=(),
                reconciliations=(),
                gaps=(gap,),
                gap_adjudication=None,
                assessed_at=ASSESSED_AT,
            )

        entry = admission.entries[0]
        self.assertEqual(
            entry.disposition,
            HistoricalDatasetAdmissionDisposition.UNRESOLVED_GAP_WITHOUT_ADJUDICATION,
        )
        self.assertEqual(entry.gap_evidence_id, gap.evidence_id)
        self.assertIsNone(entry.gap_adjudication_entry_id)
        self.assertFalse(admission.safe_requests_complete)
        self.assertFalse(admission.coverage_complete)

    def _adjudicated_gap_admission(
        self,
        root: Path,
        *,
        first_rows,
        second_rows,
        artifact_builder,
    ):
        value = single_request_plan(
            root / "inputs", first_rows=first_rows, second_rows=second_rows
        )
        request = value.requests[0]
        gap = HistoricalBackfillSessionGapEvidence(
            plan_id=value.plan_id,
            request_id=request.request_id,
            provider=value.provider,
            provider_version="upstox-test-connector/v1",
            provider_instrument_id=request.binding.provider_instrument_id,
            listing_key=request.binding.listing_key,
            security_series=request.binding.security_series,
            isin=request.binding.isin,
            session=request.sessions[0],
            response_observed_at=REQUESTED_AT + timedelta(hours=1),
            normalized_response_sha256="c" * 64,
        )
        progress_store = LocalHistoricalBackfillProgressStore(root / "progress")
        progress = progress_store.save(
            _empty_progress(value, "upstox-test-connector/v1")
        )
        artifact = artifact_builder(root / "nse")
        adjudication = build_historical_gap_adjudication_report(
            gaps=(gap,),
            nse_artifacts=(artifact,),
            adjudicated_at=ASSESSED_AT,
        )
        admission = build_historical_dataset_admission_report(
            plan=value,
            progress=progress,
            snapshots=(),
            reconciliations=(),
            gaps=(gap,),
            gap_adjudication=adjudication,
            assessed_at=ASSESSED_AT,
        )
        return admission

    def test_each_adjudication_status_maps_to_a_distinct_blocked_disposition(
        self,
    ) -> None:
        from tests.test_historical_gap_adjudication import conflicted_artifact
        from tests.test_identity_registry import tcs_row

        cases = {
            "exact": (
                [security_row()],
                [security_row()],
                nse_artifact,
                HistoricalDatasetAdmissionDisposition.EXACT_NSE_BAR_PENDING_SOURCE_SELECTION_REVIEW,
            ),
            "absent": (
                [tcs_row()],
                [tcs_row()],
                nse_artifact,
                HistoricalDatasetAdmissionDisposition.NSE_TRADED_BAR_ABSENT,
            ),
            "conflict": (
                [security_row()],
                [security_row()],
                conflicted_artifact,
                HistoricalDatasetAdmissionDisposition.RELATED_NSE_IDENTITY_CONFLICT,
            ),
        }
        for name, (
            first_rows,
            second_rows,
            artifact_builder,
            expected_disposition,
        ) in cases.items():
            with self.subTest(case=name):
                with tempfile.TemporaryDirectory() as temp_dir:
                    admission = self._adjudicated_gap_admission(
                        Path(temp_dir),
                        first_rows=first_rows,
                        second_rows=second_rows,
                        artifact_builder=artifact_builder,
                    )

                entry = admission.entries[0]
                self.assertEqual(entry.disposition, expected_disposition)
                self.assertIsNotNone(entry.gap_adjudication_entry_id)
                self.assertIs(entry.actionable, False)
                self.assertIs(entry.training_eligible, False)
                self.assertFalse(admission.safe_requests_complete)

    def test_blocking_plan_issue_makes_coverage_incomplete_even_when_admitted(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value = single_request_plan(
                root / "inputs",
                selected_calendar=calendar(DAY_ZERO, DAY_ONE),
                coverage_start=DAY_ZERO,
                coverage_end=DAY_ONE,
            )
            self.assertTrue(value.has_blocking_issues)
            progress, snapshot_store = run_single_request(value, root)
            completion = progress.completions[0]
            stored = snapshot_store.get(
                historical_dataset_name(value.provider), completion.snapshot_id
            )
            artifact = nse_artifact(root / "nse")
            report = reconcile_historical_batch(
                stored.normalized_payload, (artifact,), reconciled_at=RECONCILED_AT
            )
            admission = build_historical_dataset_admission_report(
                plan=value,
                progress=progress,
                snapshots=(stored,),
                reconciliations=(report,),
                gaps=(),
                gap_adjudication=None,
                assessed_at=ASSESSED_AT,
            )

        self.assertTrue(admission.safe_requests_complete)
        self.assertFalse(admission.coverage_complete)

    def test_non_blocking_issue_still_allows_coverage_complete(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            deleted_alias = security_row(
                TckrSymb="OLDINFY",
                FinInstrmId="2000",
                DelFlg="Y",
                SctyStsNrmlMkt="3",
                ElgbltyNrmlMkt="0",
            )
            value = single_request_plan(
                root / "inputs",
                first_rows=[deleted_alias, security_row()],
                second_rows=[deleted_alias, security_row()],
            )
            self.assertFalse(value.has_blocking_issues)
            self.assertTrue(value.has_coverage_issues)
            self.assertEqual(value.safe_request_count, 1)
            progress, snapshot_store = run_single_request(value, root)
            completion = progress.completions[0]
            stored = snapshot_store.get(
                historical_dataset_name(value.provider), completion.snapshot_id
            )
            artifact = nse_artifact(root / "nse")
            report = reconcile_historical_batch(
                stored.normalized_payload, (artifact,), reconciled_at=RECONCILED_AT
            )
            admission = build_historical_dataset_admission_report(
                plan=value,
                progress=progress,
                snapshots=(stored,),
                reconciliations=(report,),
                gaps=(),
                gap_adjudication=None,
                assessed_at=ASSESSED_AT,
            )

        self.assertTrue(admission.safe_requests_complete)
        self.assertTrue(admission.coverage_complete)

    def test_zero_safe_requests_plan_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            suspended = security_row(SctyStsNrmlMkt="1", ElgbltyNrmlMkt="0")
            value = single_request_plan(
                root / "inputs",
                first_rows=[suspended],
                second_rows=[suspended],
            )
            self.assertEqual(value.requests, ())
            progress_store = LocalHistoricalBackfillProgressStore(root / "progress")
            progress = progress_store.save(
                _empty_progress(value, "upstox-test-connector/v1")
            )

            with self.assertRaises(HistoricalDatasetAdmissionError):
                build_historical_dataset_admission_report(
                    plan=value,
                    progress=progress,
                    snapshots=(),
                    reconciliations=(),
                    gaps=(),
                    gap_adjudication=None,
                    assessed_at=ASSESSED_AT,
                )


def _empty_progress(value, connector_version: str):
    from india_swing.market_data.backfill import HistoricalBackfillProgress

    return HistoricalBackfillProgress(
        plan_id=value.plan_id,
        provider=value.provider,
        connector_version=connector_version,
        completions=(),
        updated_at=RUN_CLOCK,
    )


class CoverageValidationTests(unittest.TestCase):
    def test_extra_unused_snapshot_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value, progress, stored, report = admitted_fixture(root)
            other_value = single_request_plan(root / "other-inputs")
            other_progress, other_store = run_single_request(other_value, root / "other")
            other_completion = other_progress.completions[0]
            other_stored = other_store.get(
                historical_dataset_name(other_value.provider),
                other_completion.snapshot_id,
            )
            with self.assertRaises(HistoricalDatasetAdmissionError):
                build_historical_dataset_admission_report(
                    plan=value,
                    progress=progress,
                    snapshots=(stored, other_stored),
                    reconciliations=(report,),
                    gaps=(),
                    gap_adjudication=None,
                    assessed_at=ASSESSED_AT,
                )

    def test_duplicate_snapshot_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value, progress, stored, report = admitted_fixture(root)
            with self.assertRaises(HistoricalDatasetAdmissionError):
                build_historical_dataset_admission_report(
                    plan=value,
                    progress=progress,
                    snapshots=(stored, stored),
                    reconciliations=(report,),
                    gaps=(),
                    gap_adjudication=None,
                    assessed_at=ASSESSED_AT,
                )

    def test_duplicate_reconciliation_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value, progress, stored, report = admitted_fixture(root)
            with self.assertRaises(HistoricalDatasetAdmissionError):
                build_historical_dataset_admission_report(
                    plan=value,
                    progress=progress,
                    snapshots=(stored,),
                    reconciliations=(report, report),
                    gaps=(),
                    gap_adjudication=None,
                    assessed_at=ASSESSED_AT,
                )

    def test_completion_and_gap_for_the_same_request_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value, progress, stored, report = admitted_fixture(root)
            request = value.requests[0]
            gap = HistoricalBackfillSessionGapEvidence(
                plan_id=value.plan_id,
                request_id=request.request_id,
                provider=value.provider,
                provider_version=progress.connector_version,
                provider_instrument_id=request.binding.provider_instrument_id,
                listing_key=request.binding.listing_key,
                security_series=request.binding.security_series,
                isin=request.binding.isin,
                session=request.sessions[0],
                response_observed_at=REQUESTED_AT + timedelta(hours=1),
                normalized_response_sha256="c" * 64,
            )
            with self.assertRaises(HistoricalDatasetAdmissionError):
                build_historical_dataset_admission_report(
                    plan=value,
                    progress=progress,
                    snapshots=(stored,),
                    reconciliations=(report,),
                    gaps=(gap,),
                    gap_adjudication=None,
                    assessed_at=ASSESSED_AT,
                )

    def test_gap_referencing_a_foreign_plan_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value, progress, stored, report = admitted_fixture(root)
            foreign_gap = gap_evidence()
            with self.assertRaises(HistoricalDatasetAdmissionError):
                build_historical_dataset_admission_report(
                    plan=value,
                    progress=progress,
                    snapshots=(),
                    reconciliations=(),
                    gaps=(foreign_gap,),
                    gap_adjudication=None,
                    assessed_at=ASSESSED_AT,
                )

    def test_assessed_at_before_batch_observation_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value, progress, stored, report = admitted_fixture(root)
            with self.assertRaises(HistoricalDatasetAdmissionError):
                build_historical_dataset_admission_report(
                    plan=value,
                    progress=progress,
                    snapshots=(stored,),
                    reconciliations=(report,),
                    gaps=(),
                    gap_adjudication=None,
                    assessed_at=stored.normalized_payload.observed_at
                    - timedelta(seconds=1),
                )

    def test_input_tuple_ordering_does_not_affect_the_result(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value, progress, stored, report = admitted_fixture(root)
            forward = build_historical_dataset_admission_report(
                plan=value,
                progress=progress,
                snapshots=(stored,),
                reconciliations=(report,),
                gaps=(),
                gap_adjudication=None,
                assessed_at=ASSESSED_AT,
            )
            backward = build_historical_dataset_admission_report(
                plan=value,
                progress=progress,
                snapshots=tuple(reversed((stored,))),
                reconciliations=tuple(reversed((report,))),
                gaps=(),
                gap_adjudication=None,
                assessed_at=ASSESSED_AT,
            )

        self.assertEqual(forward.report_id, backward.report_id)


class SnapshotTamperTests(unittest.TestCase):
    def _fixture(self, root: Path):
        return admitted_fixture(root)

    def test_manifest_boundary_tampering_is_rejected(self) -> None:
        mutations = {
            "snapshot_id": "0" * 64,
            "dataset": "wrong-dataset",
            "selection_key": "0" * 64,
            "provider": "ZERODHA_KITE",
            "provider_version": "wrong-version/v1",
            "observed_at": datetime(2099, 1, 1, tzinfo=UTC),
            "record_count": 999,
            "payload_filename": "wrong.json",
            "payload_sha256": "0" * 64,
        }
        for field_name, bad_value in mutations.items():
            with self.subTest(field=field_name):
                with tempfile.TemporaryDirectory() as temp_dir:
                    root = Path(temp_dir)
                    value, progress, stored, report = self._fixture(root)
                    tampered_manifest = replace(
                        stored.manifest, **{field_name: bad_value}
                    )
                    tampered = replace(stored, manifest=tampered_manifest)

                    # Some fields (e.g. snapshot_id/selection_key) are caught by
                    # the earlier completion-matching lookup rather than the
                    # deeper _verify_snapshot cross-checks; either failure mode
                    # is a correct fail-closed rejection.
                    with self.assertRaises(HistoricalDatasetAdmissionError):
                        build_historical_dataset_admission_report(
                            plan=value,
                            progress=progress,
                            snapshots=(tampered,),
                            reconciliations=(report,),
                            gaps=(),
                            gap_adjudication=None,
                            assessed_at=ASSESSED_AT,
                        )

    def test_payload_bytes_tampering_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value, progress, stored, report = self._fixture(root)
            tampered = replace(stored, payload_bytes=stored.payload_bytes + b"x")

            with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
                build_historical_dataset_admission_report(
                    plan=value,
                    progress=progress,
                    snapshots=(tampered,),
                    reconciliations=(report,),
                    gaps=(),
                    gap_adjudication=None,
                    assessed_at=ASSESSED_AT,
                )

    def test_normalized_batch_swap_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value, progress, stored, report = self._fixture(root)
            other_value = single_request_plan(root / "other-inputs")
            other_progress, other_store = run_single_request(
                other_value,
                root / "other",
                body=success_body(
                    [
                        candle_row(
                            DAY_ONE,
                            open_value=1500.0,
                            high=1520.0,
                            low=1490.0,
                            close=1510.0,
                            volume=50,
                        )
                    ]
                ),
            )
            other_completion = other_progress.completions[0]
            other_stored = other_store.get(
                historical_dataset_name(other_value.provider),
                other_completion.snapshot_id,
            )
            tampered = replace(
                stored, normalized_payload=other_stored.normalized_payload
            )

            with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
                build_historical_dataset_admission_report(
                    plan=value,
                    progress=progress,
                    snapshots=(tampered,),
                    reconciliations=(report,),
                    gaps=(),
                    gap_adjudication=None,
                    assessed_at=ASSESSED_AT,
                )


class ReportInvariantRevalidationTests(unittest.TestCase):
    def test_forged_fixed_flags_are_rejected_even_with_recomputed_hash(self) -> None:
        mutations = {
            "collection_only": False,
            "actionable": True,
            "training_eligible": True,
            "safe_requests_complete": (lambda report: not report.safe_requests_complete),
        }
        for field_name, bad_value in mutations.items():
            with self.subTest(field=field_name):
                with tempfile.TemporaryDirectory() as temp_dir:
                    admission = build_admitted_report(Path(temp_dir))
                value = bad_value(admission) if callable(bad_value) else bad_value
                object.__setattr__(admission, field_name, value)
                object.__setattr__(admission, "report_id", admission._calculated_id())

                with self.assertRaises(ValueError):
                    admission.verify_content_identity()

    def test_coverage_complete_true_without_safe_requests_complete_is_rejected(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value, progress, stored, _ = admitted_fixture(root)
            admission = build_historical_dataset_admission_report(
                plan=value,
                progress=progress,
                snapshots=(stored,),
                reconciliations=(),
                gaps=(),
                gap_adjudication=None,
                assessed_at=ASSESSED_AT,
            )
        self.assertFalse(admission.safe_requests_complete)
        object.__setattr__(admission, "coverage_complete", True)
        object.__setattr__(admission, "report_id", admission._calculated_id())

        with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
            admission.verify_content_identity()

    def test_forged_entry_disposition_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            admission = build_admitted_report(Path(temp_dir))
        entry = admission.entries[0]
        object.__setattr__(
            entry,
            "disposition",
            HistoricalDatasetAdmissionDisposition.MISSING_COMPLETION,
        )
        object.__setattr__(entry, "entry_id", entry._calculated_id())
        object.__setattr__(admission, "report_id", admission._calculated_id())

        with self.assertRaises(ValueError):
            admission.verify_content_identity()

    def test_forged_plan_issue_ids_ordering_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value = single_request_plan(
                root / "inputs",
                selected_calendar=calendar(DAY_ZERO, DAY_ONE),
                coverage_start=DAY_ZERO,
                coverage_end=DAY_ONE,
            )
            progress, snapshot_store = run_single_request(value, root)
            completion = progress.completions[0]
            stored = snapshot_store.get(
                historical_dataset_name(value.provider), completion.snapshot_id
            )
            artifact = nse_artifact(root / "nse")
            report = reconcile_historical_batch(
                stored.normalized_payload, (artifact,), reconciled_at=RECONCILED_AT
            )
            admission = build_historical_dataset_admission_report(
                plan=value,
                progress=progress,
                snapshots=(stored,),
                reconciliations=(report,),
                gaps=(),
                gap_adjudication=None,
                assessed_at=ASSESSED_AT,
            )
        self.assertGreaterEqual(len(admission.plan_issue_ids), 1)
        object.__setattr__(
            admission, "plan_issue_ids", tuple(reversed(admission.plan_issue_ids)) * 1
        )
        # a single-element tuple reversed is unchanged; force real disorder instead
        object.__setattr__(
            admission,
            "plan_issue_ids",
            admission.plan_issue_ids + ("0" * 64,),
        )
        object.__setattr__(admission, "report_id", admission._calculated_id())

        with self.assertRaises(ValueError):
            admission.verify_content_identity()

    def test_forged_adjudication_linkage_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            admission = build_admitted_report(Path(temp_dir))
        object.__setattr__(admission, "gap_adjudication_report_id", "0" * 64)
        object.__setattr__(admission, "report_id", admission._calculated_id())

        with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
            admission.verify_content_identity()

    def test_forged_request_coverage_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            admission = build_admitted_report(Path(temp_dir))
        object.__setattr__(admission, "entries", ())
        with self.assertRaises(TypeError):
            admission.verify_content_identity()


class OrderingAndIdentityTests(unittest.TestCase):
    def test_equivalent_timezone_instants_produce_identical_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            utc_admission = build_admitted_report(root / "first", assessed_at=ASSESSED_AT)
            ist = timezone(timedelta(hours=5, minutes=30))
            ist_admission = build_admitted_report(
                root / "second", assessed_at=ASSESSED_AT.astimezone(ist)
            )
        self.assertEqual(utc_admission.report_id, ist_admission.report_id)

    def test_changed_assessed_at_changes_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            first = build_admitted_report(root / "first", assessed_at=ASSESSED_AT)
            second = build_admitted_report(
                root / "second", assessed_at=ASSESSED_AT + timedelta(seconds=1)
            )
        self.assertNotEqual(first.report_id, second.report_id)


class CodecTests(unittest.TestCase):
    def test_exact_canonical_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            admission = build_admitted_report(Path(temp_dir))
        payload = encode_historical_dataset_admission_report(admission)
        decoded = decode_historical_dataset_admission_report(payload)
        self.assertEqual(decoded, admission)
        self.assertEqual(encode_historical_dataset_admission_report(decoded), payload)

    def test_non_bytes_input_is_rejected(self) -> None:
        with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
            decode_historical_dataset_admission_report("not-bytes")  # type: ignore[arg-type]

    def test_empty_bytes_is_rejected(self) -> None:
        with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
            decode_historical_dataset_admission_report(b"")

    def test_oversized_payload_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            admission = build_admitted_report(Path(temp_dir))
        payload = encode_historical_dataset_admission_report(admission)
        with patch(
            "india_swing.market_data.dataset_admission."
            "MAXIMUM_DATASET_ADMISSION_REPORT_BYTES",
            len(payload) - 1,
        ):
            with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
                decode_historical_dataset_admission_report(payload)

    def test_invalid_utf8_is_rejected(self) -> None:
        with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
            decode_historical_dataset_admission_report(b"\xff\xfe\xfd")

    def test_float_and_nan_infinity_tokens_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            admission = build_admitted_report(Path(temp_dir))
        payload = encode_historical_dataset_admission_report(admission)
        text = payload.decode("utf-8")
        self.assertIn('"actionable":false', text)
        for replacement in ('"actionable":0.5', '"actionable":NaN', '"actionable":Infinity'):
            with self.subTest(replacement=replacement):
                corrupted = text.replace('"actionable":false', replacement).encode(
                    "utf-8"
                )
                with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
                    decode_historical_dataset_admission_report(corrupted)

    def test_duplicate_keys_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            admission = build_admitted_report(Path(temp_dir))
        original = json.loads(encode_historical_dataset_admission_report(admission))
        pairs = ",".join(
            f"{json.dumps(key)}:{json.dumps(value)}" for key, value in original.items()
        )
        duplicated = (
            "{" + pairs + f',"plan_id":{json.dumps(original["plan_id"])}' + "}"
        ).encode("utf-8")

        with self.assertRaises(HistoricalDatasetAdmissionError):
            decode_historical_dataset_admission_report(duplicated)

    def test_missing_and_unknown_keys_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            admission = build_admitted_report(Path(temp_dir))
        base = json.loads(encode_historical_dataset_admission_report(admission))

        missing = dict(base)
        del missing["plan_id"]
        with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
            decode_historical_dataset_admission_report(
                json.dumps(missing).encode("utf-8")
            )

        extra = dict(base)
        extra["unexpected"] = "x"
        with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
            decode_historical_dataset_admission_report(
                json.dumps(extra).encode("utf-8")
            )

    def test_stale_claimed_report_id_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            admission = build_admitted_report(Path(temp_dir))
        original = json.loads(encode_historical_dataset_admission_report(admission))
        original["report_id"] = "0" * 64
        with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
            decode_historical_dataset_admission_report(
                json.dumps(original).encode("utf-8")
            )

    def test_unknown_schema_policy_codec_versions_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            admission = build_admitted_report(Path(temp_dir))
        base = json.loads(encode_historical_dataset_admission_report(admission))
        for key in ("schema_version", "policy_version", "codec_version"):
            with self.subTest(key=key):
                corrupted = dict(base)
                corrupted[key] = "unknown/v0"
                with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
                    decode_historical_dataset_admission_report(
                        json.dumps(corrupted).encode("utf-8")
                    )

    def test_noncanonical_but_equivalent_bytes_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            admission = build_admitted_report(Path(temp_dir))
        canonical = encode_historical_dataset_admission_report(admission)
        value = json.loads(canonical)
        noncanonical = json.dumps(value, indent=2).encode("utf-8")
        self.assertNotEqual(noncanonical, canonical)

        with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
            decode_historical_dataset_admission_report(noncanonical)


class LocalHistoricalDatasetAdmissionReportStoreTests(unittest.TestCase):
    def test_round_trip_and_idempotent_put(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            admission = build_admitted_report(root / "inputs")
            store = LocalHistoricalDatasetAdmissionReportStore(root / "reports")
            stored = store.put(admission)
            again = store.put(admission)
            loaded = store.get(admission.report_id)

        self.assertEqual(stored, admission)
        self.assertEqual(again, admission)
        self.assertEqual(loaded, admission)

    def test_conflicting_content_at_the_same_path_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            admission = build_admitted_report(root / "inputs")
            store = LocalHistoricalDatasetAdmissionReportStore(root / "reports")
            store.put(admission)
            path = store.dataset_root / admission.report_id / REPORT_FILENAME
            corrupted = json.loads(path.read_bytes())
            corrupted["assessed_at"] = (
                admission.assessed_at + timedelta(seconds=1)
            ).isoformat()
            path.write_bytes(
                (json.dumps(corrupted, sort_keys=True, separators=(",", ":")) + "\n").encode(
                    "utf-8"
                )
            )

            with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
                store.put(admission)
            with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
                store.get(admission.report_id)

    def test_not_found_and_invalid_ids_are_distinct(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalHistoricalDatasetAdmissionReportStore(Path(temp_dir))
            with self.assertRaises(ValueError):
                store.get("not-a-hash")
            with self.assertRaises(HistoricalDatasetAdmissionError):
                store.get("a" * 64)

    def test_unexpected_children_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            admission = build_admitted_report(root / "inputs")
            store = LocalHistoricalDatasetAdmissionReportStore(root / "reports")
            store.put(admission)
            (store.dataset_root / admission.report_id / "unexpected.txt").write_text(
                "x", encoding="utf-8"
            )

            with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
                store.get(admission.report_id)

    def test_no_latest_list_find_or_select_operation_exists(self) -> None:
        store = LocalHistoricalDatasetAdmissionReportStore(Path("unused"))
        for banned in ("latest", "list", "list_all", "find", "select"):
            self.assertFalse(hasattr(store, banned))

    def test_symlink_report_directory_is_rejected_when_platform_allows_links(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            admission = build_admitted_report(root / "inputs")
            store = LocalHistoricalDatasetAdmissionReportStore(root / "reports")
            store.put(admission)
            real_dir = store.dataset_root / admission.report_id
            link_dir = store.dataset_root / "linked"
            try:
                os.symlink(real_dir, link_dir, target_is_directory=True)
            except OSError as exc:
                self.skipTest(f"platform does not permit symlinks: {type(exc).__name__}")

            with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
                store._read_path(link_dir)

    def test_atomic_publish_cleans_up_on_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            admission = build_admitted_report(root / "inputs")
            store = LocalHistoricalDatasetAdmissionReportStore(root / "reports")

            with patch(
                "india_swing.market_data.dataset_admission.os.replace",
                side_effect=OSError("publish failed"),
            ):
                with self.assertRaisesRegex(OSError, "publish failed"):
                    store.put(admission)

            leftovers = [
                path
                for path in store.dataset_root.glob("*")
                if path.is_dir() and path.name != admission.report_id
            ]
        self.assertEqual(leftovers, [])


class ForgedCoverageRegressionTests(unittest.TestCase):
    def test_coverage_complete_true_to_false_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            admission = build_admitted_report(Path(temp_dir))
        self.assertEqual(admission.blocking_plan_issue_ids, ())
        self.assertTrue(admission.coverage_complete)
        valid_payload = encode_historical_dataset_admission_report(admission)

        object.__setattr__(admission, "coverage_complete", False)
        object.__setattr__(admission, "report_id", admission._calculated_id())
        with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
            admission.verify_content_identity()

        corrupted = json.loads(valid_payload)
        corrupted["coverage_complete"] = False
        corrupted["report_id"] = admission.report_id
        with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
            decode_historical_dataset_admission_report(
                json.dumps(corrupted, sort_keys=True, separators=(",", ":")).encode(
                    "utf-8"
                )
            )

    def test_coverage_complete_false_to_true_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            admission = blocking_issue_admitted_report(Path(temp_dir))
        self.assertTrue(admission.safe_requests_complete)
        self.assertTrue(admission.blocking_plan_issue_ids)
        self.assertFalse(admission.coverage_complete)
        valid_payload = encode_historical_dataset_admission_report(admission)

        object.__setattr__(admission, "coverage_complete", True)
        object.__setattr__(admission, "report_id", admission._calculated_id())
        with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
            admission.verify_content_identity()

        corrupted = json.loads(valid_payload)
        corrupted["coverage_complete"] = True
        corrupted["report_id"] = admission.report_id
        with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
            decode_historical_dataset_admission_report(
                json.dumps(corrupted, sort_keys=True, separators=(",", ":")).encode(
                    "utf-8"
                )
            )

    def test_builder_emits_exact_blocking_subset_and_retains_all_plan_issues(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            deleted_alias = security_row(
                TckrSymb="OLDINFY",
                FinInstrmId="2000",
                DelFlg="Y",
                SctyStsNrmlMkt="3",
                ElgbltyNrmlMkt="0",
            )
            value = single_request_plan(
                root / "inputs",
                first_rows=[deleted_alias, security_row()],
                second_rows=[deleted_alias, security_row()],
                selected_calendar=calendar(DAY_ZERO, DAY_ONE),
                coverage_start=DAY_ZERO,
                coverage_end=DAY_ONE,
            )
            self.assertEqual(len(value.issues), 2)
            blocking_ids = {
                issue.issue_id for issue in value.issues if issue.blocks_collection
            }
            non_blocking_ids = {
                issue.issue_id
                for issue in value.issues
                if not issue.blocks_collection
            }
            self.assertEqual(len(blocking_ids), 1)
            self.assertEqual(len(non_blocking_ids), 1)
            progress, snapshot_store = run_single_request(value, root)
            completion = progress.completions[0]
            stored = snapshot_store.get(
                historical_dataset_name(value.provider), completion.snapshot_id
            )
            artifact = nse_artifact(root / "nse")
            report = reconcile_historical_batch(
                stored.normalized_payload, (artifact,), reconciled_at=RECONCILED_AT
            )
            admission = build_historical_dataset_admission_report(
                plan=value,
                progress=progress,
                snapshots=(stored,),
                reconciliations=(report,),
                gaps=(),
                gap_adjudication=None,
                assessed_at=ASSESSED_AT,
            )

        self.assertEqual(set(admission.plan_issue_ids), blocking_ids | non_blocking_ids)
        self.assertEqual(set(admission.blocking_plan_issue_ids), blocking_ids)
        self.assertTrue(admission.safe_requests_complete)
        self.assertFalse(admission.coverage_complete)


class BlockingPlanIssueIdsCodecTests(unittest.TestCase):
    def test_missing_key_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            admission = build_admitted_report(Path(temp_dir))
        base = json.loads(encode_historical_dataset_admission_report(admission))
        del base["blocking_plan_issue_ids"]
        with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
            decode_historical_dataset_admission_report(
                json.dumps(base).encode("utf-8")
            )

    def test_extra_key_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            admission = build_admitted_report(Path(temp_dir))
        base = json.loads(encode_historical_dataset_admission_report(admission))
        base["unexpected_extra"] = []
        with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
            decode_historical_dataset_admission_report(
                json.dumps(base).encode("utf-8")
            )

    def test_wrong_type_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            admission = build_admitted_report(Path(temp_dir))
        base = json.loads(encode_historical_dataset_admission_report(admission))
        base["blocking_plan_issue_ids"] = "not-a-list"
        with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
            decode_historical_dataset_admission_report(
                json.dumps(base).encode("utf-8")
            )

    def test_malformed_entry_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            admission = blocking_issue_admitted_report(Path(temp_dir))
        object.__setattr__(
            admission, "blocking_plan_issue_ids", ("not-a-sha256",)
        )
        with self.assertRaises(ValueError):
            admission.verify_content_identity()

    def test_non_subset_value_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            admission = blocking_issue_admitted_report(Path(temp_dir))
        self.assertTrue(admission.blocking_plan_issue_ids)
        foreign_issue_id = "f" * 64
        self.assertNotIn(foreign_issue_id, admission.plan_issue_ids)
        object.__setattr__(
            admission, "blocking_plan_issue_ids", (foreign_issue_id,)
        )
        object.__setattr__(admission, "report_id", admission._calculated_id())
        with self.assertRaises(ValueError):
            admission.verify_content_identity()

    def test_duplicate_value_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            admission = blocking_issue_admitted_report(Path(temp_dir))
        self.assertEqual(len(admission.blocking_plan_issue_ids), 1)
        duplicated = admission.blocking_plan_issue_ids * 2
        object.__setattr__(admission, "blocking_plan_issue_ids", duplicated)
        object.__setattr__(admission, "report_id", admission._calculated_id())
        with self.assertRaises(ValueError):
            admission.verify_content_identity()

    def test_reordered_value_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            admission = blocking_issue_admitted_report(Path(temp_dir))
        real_id = admission.blocking_plan_issue_ids[0]
        earlier_id = "0" * 64
        self.assertLess(earlier_id, real_id)
        object.__setattr__(
            admission, "blocking_plan_issue_ids", (real_id, earlier_id)
        )
        object.__setattr__(admission, "report_id", admission._calculated_id())
        with self.assertRaises(ValueError):
            admission.verify_content_identity()


class ForgedReconciliationRegressionTests(unittest.TestCase):
    def _assert_forged_reconciliation_rejected(
        self, root: Path, forged_report: HistoricalCandleReconciliationReport
    ) -> None:
        value, progress, stored, _ = admitted_fixture(root)
        with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
            build_historical_dataset_admission_report(
                plan=value,
                progress=progress,
                snapshots=(stored,),
                reconciliations=(forged_report,),
                gaps=(),
                gap_adjudication=None,
                assessed_at=ASSESSED_AT,
            )

    def test_mismatch_row_kept_passing_is_rejected(self) -> None:
        base = _synthetic_report([_match_row(DAY_ONE)])
        object.__setattr__(base, "rows", (_mismatch_row(DAY_ONE),))
        object.__setattr__(base, "report_id", base._calculated_id())
        with tempfile.TemporaryDirectory() as temp_dir:
            self._assert_forged_reconciliation_rejected(Path(temp_dir), base)

    def test_missing_bar_kept_passing_is_rejected(self) -> None:
        base = _synthetic_report([_match_row(DAY_ONE)])
        object.__setattr__(base, "rows", (_missing_row(DAY_ONE),))
        object.__setattr__(base, "report_id", base._calculated_id())
        with tempfile.TemporaryDirectory() as temp_dir:
            self._assert_forged_reconciliation_rejected(Path(temp_dir), base)

    def test_match_rows_kept_failing_is_rejected(self) -> None:
        base = _synthetic_report([_match_row(DAY_ONE)])
        self.assertTrue(base.passed)
        object.__setattr__(base, "passed", False)
        object.__setattr__(base, "report_id", base._calculated_id())
        with tempfile.TemporaryDirectory() as temp_dir:
            self._assert_forged_reconciliation_rejected(Path(temp_dir), base)

    def test_wrong_historical_request_id_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value, progress, stored, report = admitted_fixture(root)
            forged = _synthetic_report(
                [
                    _match_row(
                        DAY_ONE,
                        artifact_id=report.rows[0].nse_artifact_id,
                        bar_id=report.rows[0].nse_bar_id,
                    )
                ],
                historical_batch_id=report.historical_batch_id,
                historical_request_id="e" * 64,
                provider=report.provider,
                provider_version=report.provider_version,
                listing_key=report.listing_key,
                security_series=report.security_series,
                isin=report.isin,
                reconciled_at=report.reconciled_at,
            )
            with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
                build_historical_dataset_admission_report(
                    plan=value,
                    progress=progress,
                    snapshots=(stored,),
                    reconciliations=(forged,),
                    gaps=(),
                    gap_adjudication=None,
                    assessed_at=ASSESSED_AT,
                )

    def test_invalid_actionable_flag_is_rejected(self) -> None:
        base = _synthetic_report([_match_row(DAY_ONE)])
        object.__setattr__(base, "actionable", True)
        object.__setattr__(base, "report_id", base._calculated_id())
        with tempfile.TemporaryDirectory() as temp_dir:
            self._assert_forged_reconciliation_rejected(Path(temp_dir), base)

    def test_invalid_schema_version_is_rejected(self) -> None:
        base = _synthetic_report([_match_row(DAY_ONE)])
        object.__setattr__(base, "schema_version", "unsupported/v0")
        object.__setattr__(base, "report_id", base._calculated_id())
        with tempfile.TemporaryDirectory() as temp_dir:
            self._assert_forged_reconciliation_rejected(Path(temp_dir), base)

    def test_invalid_policy_version_is_rejected(self) -> None:
        base = _synthetic_report([_match_row(DAY_ONE)])
        object.__setattr__(base, "policy_version", "unsupported/v0")
        object.__setattr__(base, "report_id", base._calculated_id())
        with tempfile.TemporaryDirectory() as temp_dir:
            self._assert_forged_reconciliation_rejected(Path(temp_dir), base)

    def test_reordered_rows_is_rejected(self) -> None:
        base = _synthetic_report([_match_row(DAY_ONE), _match_row(DAY_TWO)])
        object.__setattr__(base, "rows", tuple(reversed(base.rows)))
        object.__setattr__(base, "report_id", base._calculated_id())
        with tempfile.TemporaryDirectory() as temp_dir:
            self._assert_forged_reconciliation_rejected(Path(temp_dir), base)

    def test_duplicate_rows_is_rejected(self) -> None:
        row = _match_row(DAY_ONE)
        base = _synthetic_report([row])
        object.__setattr__(base, "rows", (row, row))
        object.__setattr__(base, "report_id", base._calculated_id())
        with tempfile.TemporaryDirectory() as temp_dir:
            self._assert_forged_reconciliation_rejected(Path(temp_dir), base)

    def test_row_shape_mutated_and_rehashed_is_rejected(self) -> None:
        valid_row = _match_row(DAY_ONE)
        object.__setattr__(valid_row, "nse_bar_id", None)
        object.__setattr__(valid_row, "row_id", valid_row._calculated_id())
        base = _synthetic_report([valid_row])
        base.verify_content_identity()
        with tempfile.TemporaryDirectory() as temp_dir:
            self._assert_forged_reconciliation_rejected(Path(temp_dir), base)

    def test_genuine_mismatch_report_remains_blocked_not_an_error(self) -> None:
        # Regression guard: a real, constructor-valid failing reconciliation
        # must still be tolerated as blocked evidence, not rejected outright.
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value = single_request_plan(root / "inputs")
            progress, snapshot_store = run_single_request(
                value,
                root,
                body=success_body(
                    [
                        candle_row(
                            DAY_ONE,
                            open_value=1600.0,
                            high=1620.0,
                            low=1590.0,
                            close=1608.0,
                            volume=99,
                        )
                    ]
                ),
            )
            completion = progress.completions[0]
            stored = snapshot_store.get(
                historical_dataset_name(value.provider), completion.snapshot_id
            )
            artifact = nse_artifact(root / "nse")
            report = reconcile_historical_batch(
                stored.normalized_payload, (artifact,), reconciled_at=RECONCILED_AT
            )
            self.assertFalse(report.passed)
            admission = build_historical_dataset_admission_report(
                plan=value,
                progress=progress,
                snapshots=(stored,),
                reconciliations=(report,),
                gaps=(),
                gap_adjudication=None,
                assessed_at=ASSESSED_AT,
            )
        entry = admission.entries[0]
        self.assertEqual(
            entry.disposition,
            HistoricalDatasetAdmissionDisposition.RECONCILIATION_MISSING_OR_FAILED,
        )
        self.assertEqual(entry.reconciliation_report_id, report.report_id)


class ForgedPlanRegressionTests(unittest.TestCase):
    def _assert_plan_rejected(self, root: Path, value) -> None:
        progress_store = LocalHistoricalBackfillProgressStore(root / "progress")
        progress = progress_store.save(
            _empty_progress(value, "upstox-test-connector/v1")
        )
        with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
            build_historical_dataset_admission_report(
                plan=value,
                progress=progress,
                snapshots=(),
                reconciliations=(),
                gaps=(),
                gap_adjudication=None,
                assessed_at=ASSESSED_AT,
            )

    def test_collection_only_false_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value = single_request_plan(root / "inputs")
            object.__setattr__(value, "collection_only", False)
            object.__setattr__(value, "plan_id", value._calculated_id())
            self._assert_plan_rejected(root, value)

    def test_wrong_schema_version_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value = single_request_plan(root / "inputs")
            object.__setattr__(value, "schema_version", "unsupported/v0")
            object.__setattr__(value, "plan_id", value._calculated_id())
            self._assert_plan_rejected(root, value)

    def test_malformed_issue_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value = single_request_plan(
                root / "inputs",
                selected_calendar=calendar(DAY_ZERO, DAY_ONE),
                coverage_start=DAY_ZERO,
                coverage_end=DAY_ONE,
            )
            self.assertEqual(len(value.issues), 1)
            issue = value.issues[0]
            object.__setattr__(issue, "observation_ids", ("z" * 64, "a" * 64))
            object.__setattr__(issue, "issue_id", issue._calculated_id())
            object.__setattr__(value, "issues", (issue,))
            object.__setattr__(value, "plan_id", value._calculated_id())
            self._assert_plan_rejected(root, value)

    def test_duplicate_issue_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value = single_request_plan(
                root / "inputs",
                selected_calendar=calendar(DAY_ZERO, DAY_ONE),
                coverage_start=DAY_ZERO,
                coverage_end=DAY_ONE,
            )
            self.assertEqual(len(value.issues), 1)
            object.__setattr__(value, "issues", value.issues * 2)
            object.__setattr__(value, "plan_id", value._calculated_id())
            self._assert_plan_rejected(root, value)

    def test_reordered_issues_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            deleted_alias = security_row(
                TckrSymb="OLDINFY",
                FinInstrmId="2000",
                DelFlg="Y",
                SctyStsNrmlMkt="3",
                ElgbltyNrmlMkt="0",
            )
            value = single_request_plan(
                root / "inputs",
                first_rows=[deleted_alias, security_row()],
                second_rows=[deleted_alias, security_row()],
                selected_calendar=calendar(DAY_ZERO, DAY_ONE),
                coverage_start=DAY_ZERO,
                coverage_end=DAY_ONE,
            )
            self.assertEqual(len(value.issues), 2)
            object.__setattr__(value, "issues", tuple(reversed(value.issues)))
            object.__setattr__(value, "plan_id", value._calculated_id())
            self._assert_plan_rejected(root, value)


class ForgedProgressRegressionTests(unittest.TestCase):
    def test_duplicate_completions_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value, progress, stored, report = admitted_fixture(root)
            completion = progress.completions[0]
            object.__setattr__(progress, "completions", (completion, completion))
            object.__setattr__(progress, "progress_id", progress._calculated_id())
            with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
                build_historical_dataset_admission_report(
                    plan=value,
                    progress=progress,
                    snapshots=(),
                    reconciliations=(),
                    gaps=(),
                    gap_adjudication=None,
                    assessed_at=ASSESSED_AT,
                )

    def test_reordered_completions_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value = plan(root / "inputs")
            progress, _ = run_multi_request_plan(value, root)
            self.assertEqual(len(progress.completions), 2)
            object.__setattr__(
                progress, "completions", tuple(reversed(progress.completions))
            )
            object.__setattr__(progress, "progress_id", progress._calculated_id())
            with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
                build_historical_dataset_admission_report(
                    plan=value,
                    progress=progress,
                    snapshots=(),
                    reconciliations=(),
                    gaps=(),
                    gap_adjudication=None,
                    assessed_at=ASSESSED_AT,
                )

    def test_wrong_schema_version_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value, progress, stored, report = admitted_fixture(root)
            object.__setattr__(progress, "schema_version", "unsupported/v0")
            object.__setattr__(progress, "progress_id", progress._calculated_id())
            with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
                build_historical_dataset_admission_report(
                    plan=value,
                    progress=progress,
                    snapshots=(),
                    reconciliations=(),
                    gaps=(),
                    gap_adjudication=None,
                    assessed_at=ASSESSED_AT,
                )

    def test_wrong_provider_shape_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value, progress, stored, report = admitted_fixture(root)
            object.__setattr__(progress, "provider", "not-a-provider")
            object.__setattr__(progress, "progress_id", progress._calculated_id())
            with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
                build_historical_dataset_admission_report(
                    plan=value,
                    progress=progress,
                    snapshots=(),
                    reconciliations=(),
                    gaps=(),
                    gap_adjudication=None,
                    assessed_at=ASSESSED_AT,
                )

    def test_wrong_connector_version_shape_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value, progress, stored, report = admitted_fixture(root)
            object.__setattr__(progress, "connector_version", "")
            object.__setattr__(progress, "progress_id", progress._calculated_id())
            with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
                build_historical_dataset_admission_report(
                    plan=value,
                    progress=progress,
                    snapshots=(),
                    reconciliations=(),
                    gaps=(),
                    gap_adjudication=None,
                    assessed_at=ASSESSED_AT,
                )

    def test_completion_after_updated_at_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value, progress, stored, report = admitted_fixture(root)
            completion = progress.completions[0]
            object.__setattr__(
                completion,
                "completed_at",
                progress.updated_at + timedelta(hours=1),
            )
            object.__setattr__(completion, "completion_id", completion._calculated_id())
            object.__setattr__(progress, "completions", (completion,))
            object.__setattr__(progress, "progress_id", progress._calculated_id())
            with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
                build_historical_dataset_admission_report(
                    plan=value,
                    progress=progress,
                    snapshots=(),
                    reconciliations=(),
                    gaps=(),
                    gap_adjudication=None,
                    assessed_at=ASSESSED_AT,
                )

    def test_completion_with_malformed_snapshot_id_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value, progress, stored, report = admitted_fixture(root)
            completion = progress.completions[0]
            object.__setattr__(completion, "snapshot_id", "not-a-sha256")
            object.__setattr__(completion, "completion_id", completion._calculated_id())
            object.__setattr__(progress, "completions", (completion,))
            object.__setattr__(progress, "progress_id", progress._calculated_id())
            with self.assertRaises(HistoricalDatasetAdmissionIntegrityError):
                build_historical_dataset_admission_report(
                    plan=value,
                    progress=progress,
                    snapshots=(),
                    reconciliations=(),
                    gaps=(),
                    gap_adjudication=None,
                    assessed_at=ASSESSED_AT,
                )


if __name__ == "__main__":
    unittest.main()
