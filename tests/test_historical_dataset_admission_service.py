from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

from india_swing.market_data.backfill import (
    HistoricalBackfillProgress,
    HistoricalBackfillRunner,
    LocalHistoricalBackfillProgressStore,
)
from india_swing.market_data.backfill_gaps import (
    HistoricalBackfillSessionGapEvidence,
    LocalHistoricalBackfillSessionGapStore,
)
from india_swing.market_data.collection import (
    HistoricalReconciliationCollector,
    historical_dataset_name,
)
from india_swing.market_data.dataset_admission import (
    HistoricalDatasetAdmissionDisposition,
    HistoricalDatasetAdmissionError,
    LocalHistoricalDatasetAdmissionReportStore,
)
from india_swing.market_data.dataset_admission_service import (
    HistoricalDatasetAdmissionService,
    HistoricalDatasetAdmissionServiceError,
    HistoricalDatasetAdmissionServiceIntegrityError,
)
from india_swing.market_data.gap_adjudication import (
    HistoricalGapAdjudicationError,
    LocalHistoricalGapAdjudicationReportStore,
    build_historical_gap_adjudication_report,
)
from india_swing.market_data.reconciliation import (
    HISTORICAL_RECONCILIATION_DATASET,
    HISTORICAL_RECONCILIATION_PROVIDER,
    reconcile_historical_batch,
)
from india_swing.market_data.snapshot_store import (
    LocalMarketSnapshotStore,
    MarketSnapshotNotFound,
    StoredMarketSnapshot,
)
from tests.test_historical_backfill import REQUESTED_AT, RUN_CLOCK
from tests.test_historical_dataset_admission import (
    ASSESSED_AT,
    matching_candle_body,
    single_request_plan,
)
from tests.test_historical_reconciliation import RECONCILED_AT, nse_artifact
from tests.test_identity_registry import tcs_row
from tests.test_upstox_market_data import FakeTransport, adapter as upstox_adapter, response


def _make_stores(root: Path) -> dict[str, object]:
    return dict(
        progress_store=LocalHistoricalBackfillProgressStore(root),
        snapshot_store=LocalMarketSnapshotStore(root),
        gap_store=LocalHistoricalBackfillSessionGapStore(root),
        gap_adjudication_store=LocalHistoricalGapAdjudicationReportStore(root),
        admission_store=LocalHistoricalDatasetAdmissionReportStore(root),
    )


def _empty_progress(
    value, connector_version: str = "upstox-test-connector/v1"
) -> HistoricalBackfillProgress:
    return HistoricalBackfillProgress(
        plan_id=value.plan_id,
        provider=value.provider,
        connector_version=connector_version,
        completions=(),
        updated_at=RUN_CLOCK,
    )


def _complete_single_request(
    value,
    snapshot_store: LocalMarketSnapshotStore,
    progress_store: LocalHistoricalBackfillProgressStore,
    *,
    body: bytes | None = None,
    clock=lambda: RUN_CLOCK,
):
    transport = FakeTransport(response(body if body is not None else matching_candle_body()))
    connector = upstox_adapter(transport)
    runner = HistoricalBackfillRunner(
        connector, snapshot_store, progress_store, clock=clock
    )
    progress = runner.run(value)
    completion = progress.completions[0]
    stored = snapshot_store.get(
        historical_dataset_name(value.provider), completion.snapshot_id
    )
    return progress, stored


def _gap_for(value, request, *, response_observed_at=None):
    return HistoricalBackfillSessionGapEvidence(
        plan_id=value.plan_id,
        request_id=request.request_id,
        provider=value.provider,
        provider_version="upstox-test-connector/v1",
        provider_instrument_id=request.binding.provider_instrument_id,
        listing_key=request.binding.listing_key,
        security_series=request.binding.security_series,
        isin=request.binding.isin,
        session=request.sessions[0],
        response_observed_at=response_observed_at or (REQUESTED_AT + timedelta(hours=1)),
        normalized_response_sha256="c" * 64,
    )


def _build_reconciled_fixture(root: Path):
    """A fully admitted single-request fixture, plus its genuine stored reconciliation."""

    stores = _make_stores(root)
    value = single_request_plan(root / "inputs")
    progress, stored = _complete_single_request(
        value, stores["snapshot_store"], stores["progress_store"]
    )
    artifact = nse_artifact(root / "nse")
    report = reconcile_historical_batch(
        stored.normalized_payload, (artifact,), reconciled_at=RECONCILED_AT
    )
    stored_reconciliation = HistoricalReconciliationCollector(
        stores["snapshot_store"]
    ).collect(report)
    return stores, value, progress, stored_reconciliation, report


class _ProxySnapshotStore:
    """Delegates provider-snapshot reads to a real store; injects a forged result
    for one exact reconciliation snapshot ID.

    Models an untrusted store dependency: the service explicitly supports an
    injected snapshot_store, so a caller-controlled result -- not only what a
    real LocalMarketSnapshotStore would ever return -- must be independently
    verified before its inner report is trusted.
    """

    def __init__(self, real_store, forged_by_id: dict[str, StoredMarketSnapshot]):
        self._real_store = real_store
        self._forged_by_id = forged_by_id

    def get(self, dataset: str, snapshot_id: str) -> StoredMarketSnapshot:
        forged = self._forged_by_id.get(snapshot_id)
        if forged is not None:
            return forged
        return self._real_store.get(dataset, snapshot_id)


class HistoricalDatasetAdmissionServiceHappyPathTests(unittest.TestCase):
    def test_one_request_completed_and_reconciled_admits_and_persists(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stores = _make_stores(root)
            value = single_request_plan(root / "inputs")
            progress, stored = _complete_single_request(
                value, stores["snapshot_store"], stores["progress_store"]
            )
            artifact = nse_artifact(root / "nse")
            report = reconcile_historical_batch(
                stored.normalized_payload, (artifact,), reconciled_at=RECONCILED_AT
            )
            stored_reconciliation = HistoricalReconciliationCollector(
                stores["snapshot_store"]
            ).collect(report)

            service = HistoricalDatasetAdmissionService(**stores)
            result = service.run(
                plan=value,
                expected_plan_id=value.plan_id,
                expected_progress_id=progress.progress_id,
                reconciliation_snapshot_ids=(
                    stored_reconciliation.manifest.snapshot_id,
                ),
                expected_gap_evidence_ids=(),
                gap_adjudication_report_id=None,
                assessed_at=ASSESSED_AT,
            )

            persisted = stores["admission_store"].get(result.report.report_id)

        self.assertTrue(result.report.coverage_complete)
        self.assertEqual(len(result.report.entries), 1)
        self.assertEqual(
            result.report.entries[0].disposition,
            HistoricalDatasetAdmissionDisposition.ADMITTED,
        )
        self.assertEqual(dict(result.disposition_counts), {"ADMITTED": 1})
        self.assertEqual(persisted, result.report)

    def test_missing_reconciliation_persists_blocked_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stores = _make_stores(root)
            value = single_request_plan(root / "inputs")
            progress, _ = _complete_single_request(
                value, stores["snapshot_store"], stores["progress_store"]
            )

            service = HistoricalDatasetAdmissionService(**stores)
            result = service.run(
                plan=value,
                expected_plan_id=value.plan_id,
                expected_progress_id=progress.progress_id,
                reconciliation_snapshot_ids=(),
                expected_gap_evidence_ids=(),
                gap_adjudication_report_id=None,
                assessed_at=ASSESSED_AT,
            )

            persisted = stores["admission_store"].get(result.report.report_id)

        self.assertFalse(result.report.coverage_complete)
        entry = result.report.entries[0]
        self.assertEqual(
            entry.disposition,
            HistoricalDatasetAdmissionDisposition.RECONCILIATION_MISSING_OR_FAILED,
        )
        self.assertIs(result.report.collection_only, True)
        self.assertIs(result.report.actionable, False)
        self.assertIs(result.report.training_eligible, False)
        self.assertEqual(persisted, result.report)

    def test_adjudication_report_id_is_loaded_and_wired(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stores = _make_stores(root)
            value = single_request_plan(root / "inputs")
            progress = stores["progress_store"].save(_empty_progress(value))
            request = value.requests[0]
            gap = _gap_for(value, request)
            stores["gap_store"].put(gap)
            artifact = nse_artifact(root / "nse")
            adjudication = build_historical_gap_adjudication_report(
                gaps=(gap,), nse_artifacts=(artifact,), adjudicated_at=ASSESSED_AT
            )
            stores["gap_adjudication_store"].put(adjudication)

            service = HistoricalDatasetAdmissionService(**stores)
            result = service.run(
                plan=value,
                expected_plan_id=value.plan_id,
                expected_progress_id=progress.progress_id,
                reconciliation_snapshot_ids=(),
                expected_gap_evidence_ids=(gap.evidence_id,),
                gap_adjudication_report_id=adjudication.report_id,
                assessed_at=ASSESSED_AT,
            )

        self.assertEqual(result.report.gap_adjudication_report_id, adjudication.report_id)
        entry = result.report.entries[0]
        self.assertIsNotNone(entry.gap_adjudication_entry_id)
        self.assertFalse(result.report.safe_requests_complete)


class HistoricalDatasetAdmissionServiceRejectionTests(unittest.TestCase):
    def _reconciled_fixture(self, root: Path):
        stores = _make_stores(root)
        value = single_request_plan(root / "inputs")
        progress, stored = _complete_single_request(
            value, stores["snapshot_store"], stores["progress_store"]
        )
        artifact = nse_artifact(root / "nse")
        report = reconcile_historical_batch(
            stored.normalized_payload, (artifact,), reconciled_at=RECONCILED_AT
        )
        stored_reconciliation = HistoricalReconciliationCollector(
            stores["snapshot_store"]
        ).collect(report)
        return stores, value, progress, stored_reconciliation, report

    def test_malformed_reconciliation_snapshot_id_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stores, value, progress, _, _ = self._reconciled_fixture(root)
            service = HistoricalDatasetAdmissionService(**stores)
            with self.assertRaises(HistoricalDatasetAdmissionServiceError):
                service.run(
                    plan=value,
                    expected_plan_id=value.plan_id,
                    expected_progress_id=progress.progress_id,
                    reconciliation_snapshot_ids=("not-a-sha256",),
                    expected_gap_evidence_ids=(),
                    gap_adjudication_report_id=None,
                    assessed_at=ASSESSED_AT,
                )

    def test_duplicate_reconciliation_snapshot_id_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stores, value, progress, stored_reconciliation, _ = self._reconciled_fixture(
                root
            )
            service = HistoricalDatasetAdmissionService(**stores)
            with self.assertRaises(HistoricalDatasetAdmissionServiceError):
                service.run(
                    plan=value,
                    expected_plan_id=value.plan_id,
                    expected_progress_id=progress.progress_id,
                    reconciliation_snapshot_ids=(
                        stored_reconciliation.manifest.snapshot_id,
                        stored_reconciliation.manifest.snapshot_id,
                    ),
                    expected_gap_evidence_ids=(),
                    gap_adjudication_report_id=None,
                    assessed_at=ASSESSED_AT,
                )

    def test_duplicate_expected_gap_evidence_id_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stores, value, progress, _, _ = self._reconciled_fixture(root)
            service = HistoricalDatasetAdmissionService(**stores)
            with self.assertRaises(HistoricalDatasetAdmissionServiceError):
                service.run(
                    plan=value,
                    expected_plan_id=value.plan_id,
                    expected_progress_id=progress.progress_id,
                    reconciliation_snapshot_ids=(),
                    expected_gap_evidence_ids=("a" * 64, "a" * 64),
                    gap_adjudication_report_id=None,
                    assessed_at=ASSESSED_AT,
                )

    def test_expected_plan_mismatch_is_rejected_before_downstream_reads(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stores = _make_stores(root)
            value = single_request_plan(root / "inputs")
            # deliberately never save progress: if the plan check ran after
            # any downstream read, this would instead fail with a different
            # (progress-not-found) message.
            service = HistoricalDatasetAdmissionService(**stores)
            with self.assertRaisesRegex(
                HistoricalDatasetAdmissionServiceError, "expected_plan_id"
            ):
                service.run(
                    plan=value,
                    expected_plan_id="0" * 64,
                    expected_progress_id="1" * 64,
                    reconciliation_snapshot_ids=(),
                    expected_gap_evidence_ids=(),
                    gap_adjudication_report_id=None,
                    assessed_at=ASSESSED_AT,
                )

    def test_missing_progress_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stores = _make_stores(root)
            value = single_request_plan(root / "inputs")
            service = HistoricalDatasetAdmissionService(**stores)
            with self.assertRaises(HistoricalDatasetAdmissionServiceError):
                service.run(
                    plan=value,
                    expected_plan_id=value.plan_id,
                    expected_progress_id="0" * 64,
                    reconciliation_snapshot_ids=(),
                    expected_gap_evidence_ids=(),
                    gap_adjudication_report_id=None,
                    assessed_at=ASSESSED_AT,
                )

    def test_stale_expected_progress_id_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stores, value, progress, _, _ = self._reconciled_fixture(root)
            service = HistoricalDatasetAdmissionService(**stores)
            with self.assertRaises(HistoricalDatasetAdmissionServiceError):
                service.run(
                    plan=value,
                    expected_plan_id=value.plan_id,
                    expected_progress_id="0" * 64,
                    reconciliation_snapshot_ids=(),
                    expected_gap_evidence_ids=(),
                    gap_adjudication_report_id=None,
                    assessed_at=ASSESSED_AT,
                )

    def test_missing_provider_snapshot_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stores, value, progress, _, _ = self._reconciled_fixture(root)
            service = HistoricalDatasetAdmissionService(
                progress_store=stores["progress_store"],
                snapshot_store=LocalMarketSnapshotStore(root / "empty-snapshots"),
                gap_store=stores["gap_store"],
                gap_adjudication_store=stores["gap_adjudication_store"],
                admission_store=stores["admission_store"],
            )
            with self.assertRaises(MarketSnapshotNotFound):
                service.run(
                    plan=value,
                    expected_plan_id=value.plan_id,
                    expected_progress_id=progress.progress_id,
                    reconciliation_snapshot_ids=(),
                    expected_gap_evidence_ids=(),
                    gap_adjudication_report_id=None,
                    assessed_at=ASSESSED_AT,
                )

    def test_missing_reconciliation_snapshot_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stores, value, progress, _, _ = self._reconciled_fixture(root)
            service = HistoricalDatasetAdmissionService(**stores)
            with self.assertRaises(MarketSnapshotNotFound):
                service.run(
                    plan=value,
                    expected_plan_id=value.plan_id,
                    expected_progress_id=progress.progress_id,
                    reconciliation_snapshot_ids=("f" * 64,),
                    expected_gap_evidence_ids=(),
                    gap_adjudication_report_id=None,
                    assessed_at=ASSESSED_AT,
                )

    def test_wrong_reconciliation_payload_type_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stores, value, progress, _, _ = self._reconciled_fixture(root)
            completion = progress.completions[0]
            stored_batch = stores["snapshot_store"].get(
                historical_dataset_name(value.provider), completion.snapshot_id
            )
            stored_wrong = stores["snapshot_store"].put(
                dataset=HISTORICAL_RECONCILIATION_DATASET,
                selection_key="a" * 64,
                provider=HISTORICAL_RECONCILIATION_PROVIDER,
                provider_version="provider-nse-exact-raw-ohlcv/v1",
                observed_at=stored_batch.manifest.observed_at,
                normalized_payload=stored_batch.normalized_payload,
            )
            service = HistoricalDatasetAdmissionService(**stores)
            with self.assertRaises(HistoricalDatasetAdmissionServiceIntegrityError):
                service.run(
                    plan=value,
                    expected_plan_id=value.plan_id,
                    expected_progress_id=progress.progress_id,
                    reconciliation_snapshot_ids=(stored_wrong.manifest.snapshot_id,),
                    expected_gap_evidence_ids=(),
                    gap_adjudication_report_id=None,
                    assessed_at=ASSESSED_AT,
                )

    def test_wrong_reconciliation_stored_identity_is_rejected(self) -> None:
        mutations = {
            "selection_key": "0" * 64,
            "provider": "WRONG_PROVIDER",
            "provider_version": "wrong-version",
        }
        for field_name, bad_value in mutations.items():
            with self.subTest(field=field_name):
                with tempfile.TemporaryDirectory() as temp_dir:
                    root = Path(temp_dir)
                    stores, value, progress, _, report = self._reconciled_fixture(root)
                    kwargs = dict(
                        dataset=HISTORICAL_RECONCILIATION_DATASET,
                        selection_key=report.historical_batch_id,
                        provider=HISTORICAL_RECONCILIATION_PROVIDER,
                        provider_version=report.policy_version,
                        observed_at=report.reconciled_at,
                        normalized_payload=report,
                    )
                    kwargs[field_name] = bad_value
                    stored_wrong = stores["snapshot_store"].put(**kwargs)
                    service = HistoricalDatasetAdmissionService(**stores)
                    with self.assertRaises(
                        HistoricalDatasetAdmissionServiceIntegrityError
                    ):
                        service.run(
                            plan=value,
                            expected_plan_id=value.plan_id,
                            expected_progress_id=progress.progress_id,
                            reconciliation_snapshot_ids=(
                                stored_wrong.manifest.snapshot_id,
                            ),
                            expected_gap_evidence_ids=(),
                            gap_adjudication_report_id=None,
                            assessed_at=ASSESSED_AT,
                        )

    def test_omitted_expected_gap_evidence_id_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stores = _make_stores(root)
            value = single_request_plan(root / "inputs")
            progress = stores["progress_store"].save(_empty_progress(value))
            gap = _gap_for(value, value.requests[0])
            stores["gap_store"].put(gap)
            service = HistoricalDatasetAdmissionService(**stores)
            with self.assertRaises(HistoricalDatasetAdmissionServiceError):
                service.run(
                    plan=value,
                    expected_plan_id=value.plan_id,
                    expected_progress_id=progress.progress_id,
                    reconciliation_snapshot_ids=(),
                    expected_gap_evidence_ids=(),
                    gap_adjudication_report_id=None,
                    assessed_at=ASSESSED_AT,
                )

    def test_unknown_expected_gap_evidence_id_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stores = _make_stores(root)
            value = single_request_plan(root / "inputs")
            progress = stores["progress_store"].save(_empty_progress(value))
            service = HistoricalDatasetAdmissionService(**stores)
            with self.assertRaises(HistoricalDatasetAdmissionServiceError):
                service.run(
                    plan=value,
                    expected_plan_id=value.plan_id,
                    expected_progress_id=progress.progress_id,
                    reconciliation_snapshot_ids=(),
                    expected_gap_evidence_ids=("d" * 64,),
                    gap_adjudication_report_id=None,
                    assessed_at=ASSESSED_AT,
                )

    def test_wrong_plan_gap_evidence_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stores = _make_stores(root)
            value = single_request_plan(root / "inputs")
            progress = stores["progress_store"].save(_empty_progress(value))
            our_gap = _gap_for(value, value.requests[0])
            stores["gap_store"].put(our_gap)

            foreign_value = single_request_plan(
                root / "foreign-inputs",
                first_rows=[tcs_row()],
                second_rows=[tcs_row()],
            )
            self.assertNotEqual(foreign_value.plan_id, value.plan_id)
            foreign_gap = _gap_for(foreign_value, foreign_value.requests[0])
            stores["gap_store"].put(foreign_gap)

            service = HistoricalDatasetAdmissionService(**stores)
            with self.assertRaises(HistoricalDatasetAdmissionServiceError):
                service.run(
                    plan=value,
                    expected_plan_id=value.plan_id,
                    expected_progress_id=progress.progress_id,
                    reconciliation_snapshot_ids=(),
                    expected_gap_evidence_ids=(foreign_gap.evidence_id,),
                    gap_adjudication_report_id=None,
                    assessed_at=ASSESSED_AT,
                )

    def test_missing_adjudication_report_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stores = _make_stores(root)
            value = single_request_plan(root / "inputs")
            progress = stores["progress_store"].save(_empty_progress(value))
            gap = _gap_for(value, value.requests[0])
            stores["gap_store"].put(gap)
            service = HistoricalDatasetAdmissionService(**stores)
            with self.assertRaises(HistoricalGapAdjudicationError):
                service.run(
                    plan=value,
                    expected_plan_id=value.plan_id,
                    expected_progress_id=progress.progress_id,
                    reconciliation_snapshot_ids=(),
                    expected_gap_evidence_ids=(gap.evidence_id,),
                    gap_adjudication_report_id="f" * 64,
                    assessed_at=ASSESSED_AT,
                )

    def test_wrong_plan_adjudication_report_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stores = _make_stores(root)

            foreign_value = single_request_plan(
                root / "foreign-inputs",
                first_rows=[tcs_row()],
                second_rows=[tcs_row()],
            )
            stores["progress_store"].save(_empty_progress(foreign_value))
            foreign_gap = _gap_for(foreign_value, foreign_value.requests[0])
            stores["gap_store"].put(foreign_gap)
            foreign_artifact = nse_artifact(root / "foreign-nse")
            foreign_adjudication = build_historical_gap_adjudication_report(
                gaps=(foreign_gap,),
                nse_artifacts=(foreign_artifact,),
                adjudicated_at=ASSESSED_AT,
            )
            stores["gap_adjudication_store"].put(foreign_adjudication)

            value = single_request_plan(root / "inputs")
            self.assertNotEqual(value.plan_id, foreign_value.plan_id)
            progress = stores["progress_store"].save(_empty_progress(value))
            gap = _gap_for(value, value.requests[0])
            stores["gap_store"].put(gap)

            service = HistoricalDatasetAdmissionService(**stores)
            with self.assertRaises(HistoricalDatasetAdmissionError):
                service.run(
                    plan=value,
                    expected_plan_id=value.plan_id,
                    expected_progress_id=progress.progress_id,
                    reconciliation_snapshot_ids=(),
                    expected_gap_evidence_ids=(gap.evidence_id,),
                    gap_adjudication_report_id=foreign_adjudication.report_id,
                    assessed_at=ASSESSED_AT,
                )


def _forge_stored_reconciliation(
    stored: StoredMarketSnapshot, *, payload_bytes: bytes | None = None, **manifest_overrides
) -> StoredMarketSnapshot:
    manifest = (
        replace(stored.manifest, **manifest_overrides)
        if manifest_overrides
        else stored.manifest
    )
    kwargs: dict[str, object] = {"manifest": manifest}
    if payload_bytes is not None:
        kwargs["payload_bytes"] = payload_bytes
    return replace(stored, **kwargs)


class ForgedReconciliationSnapshotEnvelopeRegressionTests(unittest.TestCase):
    def _service_with_proxy(self, stores, proxy) -> HistoricalDatasetAdmissionService:
        return HistoricalDatasetAdmissionService(
            progress_store=stores["progress_store"],
            snapshot_store=proxy,
            gap_store=stores["gap_store"],
            gap_adjudication_store=stores["gap_adjudication_store"],
            admission_store=stores["admission_store"],
        )

    def test_exact_codex_regression_forged_envelope_is_rejected_and_not_persisted(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stores, value, progress, stored_reconciliation, _ = (
                _build_reconciled_fixture(root)
            )
            forged_id = "f" * 64
            forged = _forge_stored_reconciliation(
                stored_reconciliation,
                snapshot_id=forged_id,
                observed_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
                payload_bytes=b"tampered",
            )
            proxy = _ProxySnapshotStore(stores["snapshot_store"], {forged_id: forged})
            service = self._service_with_proxy(stores, proxy)

            with self.assertRaises(HistoricalDatasetAdmissionServiceIntegrityError):
                service.run(
                    plan=value,
                    expected_plan_id=value.plan_id,
                    expected_progress_id=progress.progress_id,
                    reconciliation_snapshot_ids=(forged_id,),
                    expected_gap_evidence_ids=(),
                    gap_adjudication_report_id=None,
                    assessed_at=ASSESSED_AT,
                )

            self.assertFalse(stores["admission_store"].dataset_root.exists())

    def test_isolated_envelope_field_mutations_are_rejected(self) -> None:
        mutations = {
            "schema_version": "wrong-schema/v0",
            "codec_version": "wrong-codec/v0",
            "payload_filename": "wrong.json",
            "dataset": "wrong-dataset",
            "selection_key": "0" * 64,
            "provider": "WRONG_PROVIDER",
            "provider_version": "wrong-version",
            "observed_at": datetime(2099, 1, 1, tzinfo=timezone.utc),
            "record_count": 999,
            "payload_sha256": "0" * 64,
        }
        for field_name, bad_value in mutations.items():
            with self.subTest(field=field_name):
                with tempfile.TemporaryDirectory() as temp_dir:
                    root = Path(temp_dir)
                    stores, value, progress, stored_reconciliation, _ = (
                        _build_reconciled_fixture(root)
                    )
                    forged = _forge_stored_reconciliation(
                        stored_reconciliation, **{field_name: bad_value}
                    )
                    proxy = _ProxySnapshotStore(
                        stores["snapshot_store"],
                        {stored_reconciliation.manifest.snapshot_id: forged},
                    )
                    service = self._service_with_proxy(stores, proxy)

                    with self.assertRaises(
                        HistoricalDatasetAdmissionServiceIntegrityError
                    ):
                        service.run(
                            plan=value,
                            expected_plan_id=value.plan_id,
                            expected_progress_id=progress.progress_id,
                            reconciliation_snapshot_ids=(
                                stored_reconciliation.manifest.snapshot_id,
                            ),
                            expected_gap_evidence_ids=(),
                            gap_adjudication_report_id=None,
                            assessed_at=ASSESSED_AT,
                        )

    def test_snapshot_id_request_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stores, value, progress, stored_reconciliation, _ = (
                _build_reconciled_fixture(root)
            )
            forged = _forge_stored_reconciliation(
                stored_reconciliation, snapshot_id="e" * 64
            )
            proxy = _ProxySnapshotStore(
                stores["snapshot_store"],
                {stored_reconciliation.manifest.snapshot_id: forged},
            )
            service = self._service_with_proxy(stores, proxy)

            with self.assertRaises(HistoricalDatasetAdmissionServiceIntegrityError):
                service.run(
                    plan=value,
                    expected_plan_id=value.plan_id,
                    expected_progress_id=progress.progress_id,
                    reconciliation_snapshot_ids=(
                        stored_reconciliation.manifest.snapshot_id,
                    ),
                    expected_gap_evidence_ids=(),
                    gap_adjudication_report_id=None,
                    assessed_at=ASSESSED_AT,
                )

    def test_recomputed_snapshot_identity_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stores, value, progress, stored_reconciliation, _ = (
                _build_reconciled_fixture(root)
            )
            forged_id = "1" * 64
            self.assertNotEqual(forged_id, stored_reconciliation.manifest.snapshot_id)
            forged = _forge_stored_reconciliation(
                stored_reconciliation, snapshot_id=forged_id
            )
            proxy = _ProxySnapshotStore(stores["snapshot_store"], {forged_id: forged})
            service = self._service_with_proxy(stores, proxy)

            with self.assertRaises(HistoricalDatasetAdmissionServiceIntegrityError):
                service.run(
                    plan=value,
                    expected_plan_id=value.plan_id,
                    expected_progress_id=progress.progress_id,
                    reconciliation_snapshot_ids=(forged_id,),
                    expected_gap_evidence_ids=(),
                    gap_adjudication_report_id=None,
                    assessed_at=ASSESSED_AT,
                )

    def test_payload_bytes_tampering_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stores, value, progress, stored_reconciliation, _ = (
                _build_reconciled_fixture(root)
            )
            forged = _forge_stored_reconciliation(
                stored_reconciliation,
                payload_bytes=stored_reconciliation.payload_bytes + b"x",
            )
            proxy = _ProxySnapshotStore(
                stores["snapshot_store"],
                {stored_reconciliation.manifest.snapshot_id: forged},
            )
            service = self._service_with_proxy(stores, proxy)

            with self.assertRaises(HistoricalDatasetAdmissionServiceIntegrityError):
                service.run(
                    plan=value,
                    expected_plan_id=value.plan_id,
                    expected_progress_id=progress.progress_id,
                    reconciliation_snapshot_ids=(
                        stored_reconciliation.manifest.snapshot_id,
                    ),
                    expected_gap_evidence_ids=(),
                    gap_adjudication_report_id=None,
                    assessed_at=ASSESSED_AT,
                )

    def test_non_exact_stored_snapshot_type_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stores, value, progress, stored_reconciliation, _ = (
                _build_reconciled_fixture(root)
            )
            proxy = _ProxySnapshotStore(
                stores["snapshot_store"],
                {stored_reconciliation.manifest.snapshot_id: object()},
            )
            service = self._service_with_proxy(stores, proxy)

            with self.assertRaises(HistoricalDatasetAdmissionServiceIntegrityError):
                service.run(
                    plan=value,
                    expected_plan_id=value.plan_id,
                    expected_progress_id=progress.progress_id,
                    reconciliation_snapshot_ids=(
                        stored_reconciliation.manifest.snapshot_id,
                    ),
                    expected_gap_evidence_ids=(),
                    gap_adjudication_report_id=None,
                    assessed_at=ASSESSED_AT,
                )

    def test_non_exact_manifest_and_payload_bytes_types_are_rejected(self) -> None:
        for field_name, bad_value in (
            ("manifest", object()),
            ("payload_bytes", bytearray(b"not-exact-bytes")),
        ):
            with self.subTest(field=field_name):
                with tempfile.TemporaryDirectory() as temp_dir:
                    root = Path(temp_dir)
                    stores, value, progress, stored_reconciliation, _ = (
                        _build_reconciled_fixture(root)
                    )
                    forged = replace(
                        stored_reconciliation,
                        **{field_name: bad_value},
                    )
                    proxy = _ProxySnapshotStore(
                        stores["snapshot_store"],
                        {stored_reconciliation.manifest.snapshot_id: forged},
                    )
                    service = self._service_with_proxy(stores, proxy)

                    with self.assertRaises(
                        HistoricalDatasetAdmissionServiceIntegrityError
                    ):
                        service.run(
                            plan=value,
                            expected_plan_id=value.plan_id,
                            expected_progress_id=progress.progress_id,
                            reconciliation_snapshot_ids=(
                                stored_reconciliation.manifest.snapshot_id,
                            ),
                            expected_gap_evidence_ids=(),
                            gap_adjudication_report_id=None,
                            assessed_at=ASSESSED_AT,
                        )

    def test_non_exact_reconciliation_payload_type_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stores, value, progress, stored_reconciliation, _ = (
                _build_reconciled_fixture(root)
            )
            completion = progress.completions[0]
            stored_batch = stores["snapshot_store"].get(
                historical_dataset_name(value.provider), completion.snapshot_id
            )
            forged = replace(
                stored_reconciliation, normalized_payload=stored_batch.normalized_payload
            )
            proxy = _ProxySnapshotStore(
                stores["snapshot_store"],
                {stored_reconciliation.manifest.snapshot_id: forged},
            )
            service = self._service_with_proxy(stores, proxy)

            with self.assertRaises(HistoricalDatasetAdmissionServiceIntegrityError):
                service.run(
                    plan=value,
                    expected_plan_id=value.plan_id,
                    expected_progress_id=progress.progress_id,
                    reconciliation_snapshot_ids=(
                        stored_reconciliation.manifest.snapshot_id,
                    ),
                    expected_gap_evidence_ids=(),
                    gap_adjudication_report_id=None,
                    assessed_at=ASSESSED_AT,
                )

    def test_valid_injected_store_result_still_admits_exactly_once(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stores, value, progress, stored_reconciliation, _ = (
                _build_reconciled_fixture(root)
            )
            # No forged entries: every read delegates to the real store, proving
            # the tightened verifier does not reject a genuine injected result.
            proxy = _ProxySnapshotStore(stores["snapshot_store"], {})
            service = self._service_with_proxy(stores, proxy)

            result = service.run(
                plan=value,
                expected_plan_id=value.plan_id,
                expected_progress_id=progress.progress_id,
                reconciliation_snapshot_ids=(
                    stored_reconciliation.manifest.snapshot_id,
                ),
                expected_gap_evidence_ids=(),
                gap_adjudication_report_id=None,
                assessed_at=ASSESSED_AT,
            )

        self.assertTrue(result.report.coverage_complete)
        self.assertEqual(dict(result.disposition_counts), {"ADMITTED": 1})


if __name__ == "__main__":
    unittest.main()
