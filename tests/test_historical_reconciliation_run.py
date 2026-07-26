from __future__ import annotations

import hashlib
import inspect
import json
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

from india_swing.daily_reports.artifact_store import LocalDailyBundleArtifactStore
from india_swing.daily_reports.parser import NSE_DAILY_BUNDLE_FILENAME
from india_swing.historical_prices import materialize_nse_eod_session
from india_swing.market_data import reconciliation_run
from india_swing.market_data.backfill import (
    HistoricalBackfillCompletion,
    HistoricalBackfillProgress,
    HistoricalBackfillRunner,
    LocalHistoricalBackfillProgressStore,
)
from india_swing.market_data.codec import encode_market_payload
from india_swing.market_data.collection import (
    HistoricalReconciliationCollector,
    historical_dataset_name,
)
from india_swing.market_data.reconciliation import (
    HISTORICAL_RECONCILIATION_DATASET,
    HistoricalCandleReconciliationReport,
)
from india_swing.market_data.reconciliation_run import (
    HISTORICAL_RECONCILIATION_INDEX_CODEC_VERSION,
    HISTORICAL_RECONCILIATION_INDEX_DATASET,
    HISTORICAL_RECONCILIATION_INDEX_POLICY_VERSION,
    HISTORICAL_RECONCILIATION_INDEX_SCHEMA_VERSION,
    INDEX_FILENAME,
    MAXIMUM_RECONCILIATION_INDEX_BYTES,
    MAXIMUM_RECONCILIATIONS_PER_RUN,
    HistoricalBulkReconciliationError,
    HistoricalBulkReconciliationIntegrityError,
    HistoricalBulkReconciliationService,
    HistoricalReconciliationIndex,
    HistoricalReconciliationIndexEntry,
    LocalHistoricalReconciliationIndexStore,
    decode_historical_reconciliation_index,
    encode_historical_reconciliation_index,
    reconciliation_index_snapshot_ids,
)
from india_swing.market_data.snapshot_store import LocalMarketSnapshotStore
from tests.test_historical_backfill import DAY_ONE, RUN_CLOCK
from tests.test_historical_backfill_pilot import (
    CUTOFF,
    FIRST_SEEN,
    OBSERVED_AT,
    RECONCILED_AT,
    VALIDATED,
    FakePilotConnector,
    _bundle_bytes,
    _clock,
    _with_market_session,
    nse_artifact,
    pilot_plan,
)


UTC = timezone.utc
EXTRA_SESSION = DAY_ONE - timedelta(days=14)


def alternate_nse_artifact(root: Path, cutoff: datetime):
    """A genuinely distinct artifact for the same session (different cutoff)."""

    source = root / "source" / NSE_DAILY_BUNDLE_FILENAME
    source.parent.mkdir(parents=True)
    source.write_bytes(_bundle_bytes())
    bundle = LocalDailyBundleArtifactStore(
        root / "daily",
        clock=_clock(FIRST_SEEN, VALIDATED),
    ).import_bundle(source)
    return materialize_nse_eod_session(
        bundle,
        market_session=DAY_ONE,
        cutoff=cutoff,
    )


class CountingReconciliationCollector(HistoricalReconciliationCollector):
    """A real collector that records every report it was actually asked to seal."""

    def __init__(self, store: LocalMarketSnapshotStore) -> None:
        super().__init__(store)
        self.reports: list[HistoricalCandleReconciliationReport] = []

    def collect(self, report):
        self.reports.append(report)
        return super().collect(report)


class ForgingReconciliationCollector(HistoricalReconciliationCollector):
    """A collector whose genuinely persisted envelope is tampered on return."""

    def __init__(self, store: LocalMarketSnapshotStore, forge) -> None:
        super().__init__(store)
        self._forge = forge

    def collect(self, report):
        return self._forge(super().collect(report))


class ProxySnapshotStore:
    """An injected store that returns forged envelopes for exact snapshot IDs."""

    def __init__(
        self,
        real: LocalMarketSnapshotStore,
        forged: dict[str, object] | None = None,
    ) -> None:
        self._real = real
        self._forged = dict(forged or {})

    def get(self, dataset: str, snapshot_id: str):
        if snapshot_id in self._forged:
            return self._forged[snapshot_id]
        return self._real.get(dataset, snapshot_id)


def forge_stored(stored, *, payload_bytes=None, **manifest_overrides):
    manifest = (
        replace(stored.manifest, **manifest_overrides)
        if manifest_overrides
        else stored.manifest
    )
    return replace(
        stored,
        manifest=manifest,
        payload_bytes=(
            stored.payload_bytes if payload_bytes is None else payload_bytes
        ),
    )


class BulkReconciliationFixture(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.plan = pilot_plan(self.root / "inputs")
        self.artifact = nse_artifact(self.root / "nse")
        self.snapshot_store = LocalMarketSnapshotStore(self.root / "snapshots")
        self.progress_store = LocalHistoricalBackfillProgressStore(
            self.root / "progress"
        )
        self.index_store = LocalHistoricalReconciliationIndexStore(
            self.root / "indexes"
        )
        self.collector = CountingReconciliationCollector(self.snapshot_store)
        self.connector = FakePilotConnector()
        self.progress = self._collect(self.connector)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _collect(self, connector) -> HistoricalBackfillProgress:
        runner = HistoricalBackfillRunner(
            connector,
            self.snapshot_store,
            self.progress_store,
            clock=lambda: RUN_CLOCK,
        )
        return runner.run(self.plan, maximum_requests=len(self.plan.requests))

    def service(
        self,
        *,
        snapshot_store=None,
        collector=None,
        progress_store=None,
        index_store=None,
    ) -> HistoricalBulkReconciliationService:
        return HistoricalBulkReconciliationService(
            progress_store=progress_store or self.progress_store,
            snapshot_store=snapshot_store or self.snapshot_store,
            reconciliation_collector=collector or self.collector,
            index_store=index_store or self.index_store,
        )

    def run_index(self, **overrides):
        values = dict(
            plan=self.plan,
            expected_plan_id=self.plan.plan_id,
            expected_progress_id=self.progress.progress_id,
            nse_artifacts=(self.artifact,),
            maximum_requests=len(self.plan.requests),
            reconciled_at=RECONCILED_AT,
        )
        values.update(overrides)
        service = values.pop("service", None) or self.service()
        return service.run(**values)


class BulkReconciliationRunTests(BulkReconciliationFixture):
    def test_capped_run_then_explicit_resume_completes_without_reprocessing(
        self,
    ) -> None:
        first = self.run_index(maximum_requests=1)

        self.assertIsInstance(first, HistoricalReconciliationIndex)
        self.assertFalse(first.complete)
        self.assertEqual(first.indexed_count, 1)
        self.assertEqual(first.remaining_count, 1)
        self.assertEqual(first.total_completion_count, 2)
        self.assertIsNone(first.prior_index_id)
        self.assertEqual(
            first.entries[0].request_id, self.progress.completions[0].request_id
        )
        self.assertEqual(
            first.entries[0].provider_snapshot_id,
            self.progress.completions[0].snapshot_id,
        )
        self.assertEqual(len(self.collector.reports), 1)
        first.verify_content_identity()

        stored_report = self.snapshot_store.get(
            HISTORICAL_RECONCILIATION_DATASET,
            first.entries[0].reconciliation_snapshot_id,
        )
        self.assertIsInstance(
            stored_report.normalized_payload, HistoricalCandleReconciliationReport
        )
        self.assertEqual(
            stored_report.normalized_payload.report_id,
            first.entries[0].reconciliation_report_id,
        )

        second = self.run_index(
            maximum_requests=MAXIMUM_RECONCILIATIONS_PER_RUN,
            prior_index_id=first.index_id,
        )

        self.assertTrue(second.complete)
        self.assertEqual(second.indexed_count, 2)
        self.assertEqual(second.remaining_count, 0)
        self.assertEqual(second.prior_index_id, first.index_id)
        self.assertEqual(second.entries[:1], first.entries)
        self.assertEqual(len(self.collector.reports), 2)
        self.assertEqual(
            tuple(value.request_id for value in second.entries),
            tuple(value.request_id for value in self.progress.completions),
        )
        second.verify_content_identity()
        self.assertEqual(self.index_store.get(second.index_id), second)

    def test_index_is_never_actionable_or_training_eligible(self) -> None:
        index = self.run_index()

        self.assertTrue(index.collection_only)
        self.assertFalse(index.actionable)
        self.assertFalse(index.training_eligible)
        self.assertEqual(
            index.schema_version, HISTORICAL_RECONCILIATION_INDEX_SCHEMA_VERSION
        )
        self.assertEqual(
            index.policy_version, HISTORICAL_RECONCILIATION_INDEX_POLICY_VERSION
        )

    def test_failed_reconciliation_is_indexed_counted_and_retained(self) -> None:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        self.snapshot_store = LocalMarketSnapshotStore(root / "snapshots")
        self.progress_store = LocalHistoricalBackfillProgressStore(
            root / "progress"
        )
        self.collector = CountingReconciliationCollector(self.snapshot_store)
        mismatching = FakePilotConnector(
            close_by_listing_key={"NSE:INFY": "1608.00"}
        )
        self.progress = self._collect(mismatching)

        index = self.run_index()

        self.assertTrue(index.complete)
        self.assertEqual(index.indexed_count, 2)
        self.assertEqual(index.passed_count, 1)
        self.assertEqual(index.failed_count, 1)
        failed = [value for value in index.entries if not value.passed]
        self.assertEqual(len(failed), 1)
        stored = self.snapshot_store.get(
            HISTORICAL_RECONCILIATION_DATASET,
            failed[0].reconciliation_snapshot_id,
        )
        self.assertFalse(stored.normalized_payload.passed)
        self.assertFalse(stored.normalized_payload.actionable)
        index.verify_content_identity()

    def test_identical_retry_is_byte_identical_and_idempotent(self) -> None:
        first = self.run_index(maximum_requests=1)
        second = self.run_index(maximum_requests=1)

        self.assertEqual(first.index_id, second.index_id)
        self.assertEqual(
            encode_historical_reconciliation_index(first),
            encode_historical_reconciliation_index(second),
        )
        self.assertEqual(
            first.entries[0].reconciliation_snapshot_id,
            second.entries[0].reconciliation_snapshot_id,
        )
        self.assertEqual(len(self.collector.reports), 2)

    def test_runner_never_uses_listing_latest_or_selection_key_lookup(self) -> None:
        source = inspect.getsource(reconciliation_run)

        for forbidden in (
            "find_by_selection_key",
            "latest_at_or_before",
            "load_unresolved",
            ".glob(",
        ):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, source)

        public = {
            name
            for name in dir(LocalHistoricalReconciliationIndexStore)
            if not name.startswith("_")
        }
        self.assertEqual(public, {"put", "get", "dataset_root"})

    def test_zero_remaining_completions_is_rejected_rather_than_resealed(
        self,
    ) -> None:
        complete = self.run_index()
        self.assertTrue(complete.complete)

        with self.assertRaisesRegex(
            HistoricalBulkReconciliationError, "already complete"
        ):
            self.run_index(prior_index_id=complete.index_id)


class BulkReconciliationInputRejectionTests(BulkReconciliationFixture):
    def test_malformed_identifiers_are_rejected(self) -> None:
        for override in (
            {"expected_plan_id": "not-a-sha256"},
            {"expected_plan_id": "A" * 64},
            {"expected_progress_id": "short"},
            {"expected_progress_id": None},
            {"prior_index_id": "not-a-sha256"},
        ):
            with self.subTest(override=override):
                with self.assertRaises(HistoricalBulkReconciliationError):
                    self.run_index(**override)
        self.assertEqual(self.collector.reports, [])

    def test_wrong_plan_type_and_identity_are_rejected(self) -> None:
        with self.assertRaises(HistoricalBulkReconciliationError):
            self.run_index(plan="not-a-plan")

        object.__setattr__(self.plan, "plan_id", "0" * 64)
        with self.assertRaises(HistoricalBulkReconciliationError):
            self.run_index(expected_plan_id="0" * 64)

    def test_plan_id_must_equal_expected_plan_id(self) -> None:
        with self.assertRaisesRegex(
            HistoricalBulkReconciliationError, "expected_plan_id"
        ):
            self.run_index(expected_plan_id="1" * 64)

    def test_maximum_requests_bad_values_are_rejected(self) -> None:
        for bad in (
            True,
            False,
            0,
            -1,
            MAXIMUM_RECONCILIATIONS_PER_RUN + 1,
            "1",
            1.0,
            None,
        ):
            with self.subTest(bad=bad):
                with self.assertRaises(HistoricalBulkReconciliationError):
                    self.run_index(maximum_requests=bad)
        self.assertEqual(self.collector.reports, [])

    def test_reconciled_at_must_be_an_aware_datetime(self) -> None:
        for bad in (datetime(2026, 7, 22, 10, 0), "2026-07-22T10:00:00+00:00", None):
            with self.subTest(bad=bad):
                with self.assertRaises(HistoricalBulkReconciliationError):
                    self.run_index(reconciled_at=bad)

    def test_reconciled_at_before_evidence_fails_closed(self) -> None:
        with self.assertRaises(HistoricalBulkReconciliationError):
            self.run_index(reconciled_at=OBSERVED_AT - timedelta(days=1))

    def test_missing_progress_is_rejected(self) -> None:
        empty_store = LocalHistoricalBackfillProgressStore(self.root / "empty")

        with self.assertRaisesRegex(
            HistoricalBulkReconciliationError, "no backfill progress"
        ):
            self.run_index(service=self.service(progress_store=empty_store))

    def test_wrong_expected_progress_id_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            HistoricalBulkReconciliationError, "expected_progress_id"
        ):
            self.run_index(expected_progress_id="2" * 64)

    def test_progress_provider_mismatch_is_rejected(self) -> None:
        forged_store = LocalHistoricalBackfillProgressStore(self.root / "forged")
        forged = HistoricalBackfillProgress(
            plan_id=self.plan.plan_id,
            provider="ZERODHA_KITE",
            connector_version=self.progress.connector_version,
            completions=self.progress.completions,
            updated_at=self.progress.updated_at,
        )
        forged_store.save(forged)

        with self.assertRaisesRegex(
            HistoricalBulkReconciliationError, "lineage does not match the plan"
        ):
            self.run_index(
                expected_progress_id=forged.progress_id,
                service=self.service(progress_store=forged_store),
            )

    def test_progress_without_completions_is_rejected(self) -> None:
        empty_store = LocalHistoricalBackfillProgressStore(self.root / "no-work")
        empty = HistoricalBackfillProgress(
            plan_id=self.plan.plan_id,
            provider=self.plan.provider,
            connector_version=self.progress.connector_version,
            completions=(),
            updated_at=self.progress.updated_at,
        )
        empty_store.save(empty)

        with self.assertRaisesRegex(
            HistoricalBulkReconciliationError, "no completion to reconcile"
        ):
            self.run_index(
                expected_progress_id=empty.progress_id,
                service=self.service(progress_store=empty_store),
            )

    def test_completion_outside_the_plan_is_rejected(self) -> None:
        foreign_store = LocalHistoricalBackfillProgressStore(self.root / "foreign")
        foreign = HistoricalBackfillProgress(
            plan_id=self.plan.plan_id,
            provider=self.plan.provider,
            connector_version=self.progress.connector_version,
            completions=(
                HistoricalBackfillCompletion(
                    request_id="9" * 64,
                    snapshot_id="8" * 64,
                    completed_at=RUN_CLOCK,
                    recovered_existing=False,
                ),
            ),
            updated_at=self.progress.updated_at,
        )
        foreign_store.save(foreign)

        with self.assertRaisesRegex(
            HistoricalBulkReconciliationError, "outside the plan"
        ):
            self.run_index(
                expected_progress_id=foreign.progress_id,
                service=self.service(progress_store=foreign_store),
            )


class BulkReconciliationArtifactRejectionTests(BulkReconciliationFixture):
    def test_wrong_artifact_container_types_are_rejected(self) -> None:
        for bad in ((), [self.artifact], ("not-an-artifact",), self.artifact):
            with self.subTest(bad=type(bad).__name__):
                with self.assertRaises(HistoricalBulkReconciliationError):
                    self.run_index(nse_artifacts=bad)
        self.assertEqual(self.collector.reports, [])

    def test_duplicate_artifact_ids_and_sessions_are_rejected(self) -> None:
        with self.assertRaisesRegex(
            HistoricalBulkReconciliationError, "unique artifact IDs"
        ):
            self.run_index(nse_artifacts=(self.artifact, self.artifact))

        duplicate_session = alternate_nse_artifact(
            self.root / "alternate", CUTOFF + timedelta(hours=1)
        )
        self.assertNotEqual(duplicate_session.artifact_id, self.artifact.artifact_id)
        self.assertEqual(duplicate_session.market_session, self.artifact.market_session)
        with self.assertRaisesRegex(
            HistoricalBulkReconciliationError, "session-unique"
        ):
            self.run_index(nse_artifacts=(self.artifact, duplicate_session))

    def test_missing_and_extra_sessions_are_rejected(self) -> None:
        misplaced = _with_market_session(self.artifact, EXTRA_SESSION)

        with self.assertRaisesRegex(
            HistoricalBulkReconciliationError, "exactly cover"
        ):
            self.run_index(nse_artifacts=(misplaced,))
        with self.assertRaisesRegex(
            HistoricalBulkReconciliationError, "exactly cover"
        ):
            self.run_index(nse_artifacts=(self.artifact, misplaced))
        self.assertEqual(self.collector.reports, [])

    def test_tampered_artifact_identity_is_detected(self) -> None:
        object.__setattr__(self.artifact, "artifact_id", "0" * 64)

        with self.assertRaises(HistoricalBulkReconciliationIntegrityError):
            self.run_index()
        self.assertEqual(self.collector.reports, [])

    def test_changed_artifact_set_between_resumes_is_rejected(self) -> None:
        first = self.run_index(maximum_requests=1)
        changed = alternate_nse_artifact(
            self.root / "changed", CUTOFF + timedelta(hours=2)
        )

        with self.assertRaisesRegex(
            HistoricalBulkReconciliationError, "current evidence set"
        ):
            self.run_index(
                nse_artifacts=(changed,), prior_index_id=first.index_id
            )


class BulkReconciliationPriorIndexTests(BulkReconciliationFixture):
    def _prior(self, **overrides) -> HistoricalReconciliationIndex:
        first = self.run_index(maximum_requests=1)
        if not overrides:
            return first
        return HistoricalReconciliationIndex(
            plan_id=overrides.get("plan_id", first.plan_id),
            progress_id=overrides.get("progress_id", first.progress_id),
            provider=overrides.get("provider", first.provider),
            connector_version=overrides.get(
                "connector_version", first.connector_version
            ),
            nse_artifact_ids=overrides.get(
                "nse_artifact_ids", first.nse_artifact_ids
            ),
            prior_index_id=overrides.get("prior_index_id", first.prior_index_id),
            entries=overrides.get("entries", first.entries),
            total_completion_count=overrides.get(
                "total_completion_count", first.total_completion_count
            ),
            updated_at=overrides.get("updated_at", first.updated_at),
            complete=overrides.get("complete", first.complete),
        )

    def test_unknown_prior_index_id_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            HistoricalBulkReconciliationError, "was not found"
        ):
            self.run_index(prior_index_id="3" * 64)

    def test_regressing_reconciled_at_is_rejected(self) -> None:
        first = self.run_index(maximum_requests=1)

        with self.assertRaisesRegex(
            HistoricalBulkReconciliationError, "cannot predate"
        ):
            self.run_index(
                prior_index_id=first.index_id,
                reconciled_at=RECONCILED_AT - timedelta(seconds=1),
            )

    def test_prior_index_lineage_mismatch_is_rejected(self) -> None:
        for override in (
            {"plan_id": "4" * 64},
            {"progress_id": "5" * 64},
            {"connector_version": "wrong-connector/v9"},
            {"nse_artifact_ids": ("6" * 64,)},
            {"total_completion_count": 3, "complete": False},
        ):
            with self.subTest(override=override):
                forged = self.index_store.put(self._prior(**override))
                with self.assertRaisesRegex(
                    HistoricalBulkReconciliationError, "current evidence set"
                ):
                    self.run_index(prior_index_id=forged.index_id)

    def test_prior_entries_must_be_an_exact_progress_prefix(self) -> None:
        first = self.run_index(maximum_requests=1)
        genuine = first.entries[0]

        for mutated in (
            replace(genuine, request_id="7" * 64),
            replace(genuine, provider_snapshot_id="7" * 64),
        ):
            with self.subTest(mutated=mutated.entry_id):
                forged = self.index_store.put(self._prior(entries=(mutated,)))
                with self.assertRaisesRegex(
                    HistoricalBulkReconciliationError, "progress prefix"
                ):
                    self.run_index(prior_index_id=forged.index_id)

    def test_reordered_prior_entries_are_not_a_prefix(self) -> None:
        complete = self.run_index()
        reordered = HistoricalReconciliationIndex(
            plan_id=complete.plan_id,
            progress_id=complete.progress_id,
            provider=complete.provider,
            connector_version=complete.connector_version,
            nse_artifact_ids=complete.nse_artifact_ids,
            prior_index_id=None,
            entries=tuple(reversed(complete.entries)),
            total_completion_count=3,
            updated_at=complete.updated_at,
            complete=False,
        )
        stored = self.index_store.put(reordered)

        with self.assertRaises(HistoricalBulkReconciliationError):
            self.run_index(prior_index_id=stored.index_id)

    def test_prior_index_returned_under_the_wrong_id_is_rejected(self) -> None:
        first = self.run_index(maximum_requests=1)

        class SwappingIndexStore:
            def __init__(self, real, swapped):
                self._real = real
                self._swapped = swapped

            def get(self, index_id):
                return self._swapped

            def put(self, index):
                return self._real.put(index)

        swapped = self.service(
            index_store=SwappingIndexStore(self.index_store, first)
        )
        with self.assertRaisesRegex(
            HistoricalBulkReconciliationIntegrityError, "requested ID"
        ):
            self.run_index(prior_index_id="8" * 64, service=swapped)


class ForgedProviderSnapshotTests(BulkReconciliationFixture):
    def _completion(self):
        return self.progress.completions[0]

    def _stored(self):
        return self.snapshot_store.get(
            historical_dataset_name(self.plan.provider),
            self._completion().snapshot_id,
        )

    def _run_with(self, forged):
        proxy = ProxySnapshotStore(
            self.snapshot_store, {self._completion().snapshot_id: forged}
        )
        return self.run_index(
            maximum_requests=1, service=self.service(snapshot_store=proxy)
        )

    def test_non_exact_stored_snapshot_type_is_rejected(self) -> None:
        with self.assertRaises(HistoricalBulkReconciliationIntegrityError):
            self._run_with(object())

    def test_wrong_payload_type_is_rejected(self) -> None:
        genuine = self.run_index(maximum_requests=1)
        reconciliation = self.snapshot_store.get(
            HISTORICAL_RECONCILIATION_DATASET,
            genuine.entries[0].reconciliation_snapshot_id,
        )

        with self.assertRaises(HistoricalBulkReconciliationIntegrityError):
            self._run_with(reconciliation)

    def test_every_identity_bound_manifest_field_is_checked(self) -> None:
        stored = self._stored()
        cases = {
            "schema_version": "market-snapshot-v1",
            "codec_version": "market-data-json/v0",
            "payload_filename": "other.json",
            "dataset": "wrong-dataset",
            "selection_key": "9" * 64,
            "provider": "ZERODHA_KITE",
            "provider_version": "wrong-connector/v9",
            "observed_at": OBSERVED_AT + timedelta(seconds=1),
            "record_count": stored.manifest.record_count + 1,
            "payload_sha256": "a" * 64,
        }
        for field_name, wrong_value in cases.items():
            with self.subTest(field_name=field_name):
                with self.assertRaises(
                    HistoricalBulkReconciliationIntegrityError
                ):
                    self._run_with(
                        forge_stored(stored, **{field_name: wrong_value})
                    )

    def test_tampered_payload_bytes_are_rejected(self) -> None:
        stored = self._stored()

        with self.assertRaises(HistoricalBulkReconciliationIntegrityError):
            self._run_with(
                forge_stored(stored, payload_bytes=stored.payload_bytes + b" ")
            )

    def test_requested_snapshot_id_mismatch_is_rejected(self) -> None:
        stored = self._stored()

        with self.assertRaises(HistoricalBulkReconciliationIntegrityError):
            self._run_with(forge_stored(stored, snapshot_id="b" * 64))

    def test_recomputed_snapshot_identity_is_the_only_remaining_check(self) -> None:
        stored = self._stored()
        completion = self._completion()
        forged_id = "f" * 64
        forged = forge_stored(stored, snapshot_id=forged_id)
        self.assertEqual(forged.manifest.dataset, stored.manifest.dataset)
        self.assertEqual(
            forged.manifest.payload_sha256,
            hashlib.sha256(forged.payload_bytes).hexdigest(),
        )

        rebound_store = LocalHistoricalBackfillProgressStore(self.root / "rebound")
        rebound = HistoricalBackfillProgress(
            plan_id=self.plan.plan_id,
            provider=self.plan.provider,
            connector_version=self.progress.connector_version,
            completions=(
                HistoricalBackfillCompletion(
                    request_id=completion.request_id,
                    snapshot_id=forged_id,
                    completed_at=completion.completed_at,
                    recovered_existing=completion.recovered_existing,
                ),
            ),
            updated_at=self.progress.updated_at,
        )
        rebound_store.save(rebound)
        proxy = ProxySnapshotStore(self.snapshot_store, {forged_id: forged})

        with self.assertRaises(HistoricalBulkReconciliationIntegrityError):
            self.run_index(
                maximum_requests=1,
                expected_progress_id=rebound.progress_id,
                service=self.service(
                    snapshot_store=proxy, progress_store=rebound_store
                ),
            )

    def test_missing_provider_snapshot_is_rejected(self) -> None:
        missing_store = LocalMarketSnapshotStore(self.root / "no-snapshots")
        proxy = ProxySnapshotStore(missing_store)

        with self.assertRaises(HistoricalBulkReconciliationError):
            self.run_index(
                maximum_requests=1, service=self.service(snapshot_store=proxy)
            )


class ForgedReconciliationSnapshotTests(BulkReconciliationFixture):
    def _run_with(self, forge):
        collector = ForgingReconciliationCollector(self.snapshot_store, forge)
        return self.run_index(
            maximum_requests=1, service=self.service(collector=collector)
        )

    def test_valid_collector_still_admits_one_entry(self) -> None:
        index = self._run_with(lambda stored: stored)

        self.assertEqual(index.indexed_count, 1)
        index.verify_content_identity()

    def test_non_exact_type_and_payload_are_rejected(self) -> None:
        with self.assertRaises(HistoricalBulkReconciliationIntegrityError):
            self._run_with(lambda stored: object())

        provider_snapshot = self.snapshot_store.get(
            historical_dataset_name(self.plan.provider),
            self.progress.completions[0].snapshot_id,
        )
        with self.assertRaises(HistoricalBulkReconciliationIntegrityError):
            self._run_with(lambda stored: provider_snapshot)

    def test_every_identity_bound_manifest_field_is_checked(self) -> None:
        cases = {
            "schema_version": "market-snapshot-v1",
            "codec_version": "market-data-json/v0",
            "payload_filename": "other.json",
            "dataset": "wrong-dataset",
            "selection_key": "9" * 64,
            "provider": "WRONG_PROVIDER",
            "provider_version": "wrong-policy/v1",
            "observed_at": RECONCILED_AT + timedelta(seconds=1),
            "payload_sha256": "a" * 64,
            "snapshot_id": "f" * 64,
        }
        for field_name, wrong_value in cases.items():
            with self.subTest(field_name=field_name):
                with self.assertRaises(
                    HistoricalBulkReconciliationIntegrityError
                ):
                    self._run_with(
                        lambda stored, name=field_name, value=wrong_value: (
                            forge_stored(stored, **{name: value})
                        )
                    )

    def test_record_count_and_payload_bytes_are_checked(self) -> None:
        with self.assertRaises(HistoricalBulkReconciliationIntegrityError):
            self._run_with(
                lambda stored: forge_stored(
                    stored, record_count=stored.manifest.record_count + 1
                )
            )
        with self.assertRaises(HistoricalBulkReconciliationIntegrityError):
            self._run_with(
                lambda stored: forge_stored(
                    stored, payload_bytes=stored.payload_bytes + b" "
                )
            )

    def test_no_index_is_persisted_when_an_envelope_is_forged(self) -> None:
        with self.assertRaises(HistoricalBulkReconciliationIntegrityError):
            self._run_with(lambda stored: forge_stored(stored, snapshot_id="c" * 64))

        self.assertFalse(self.index_store.dataset_root.exists())


class ForgedPriorEntryEvidenceTests(BulkReconciliationFixture):
    """A prior index is untrusted input, not a shortcut around verification."""

    def _stored_index_ids(self) -> set[str]:
        root = self.index_store.dataset_root
        if not root.exists():
            return set()
        return {value.name for value in root.iterdir() if value.is_dir()}

    def _prior_index(self, template, entry) -> HistoricalReconciliationIndex:
        return self.index_store.put(
            HistoricalReconciliationIndex(
                plan_id=template.plan_id,
                progress_id=template.progress_id,
                provider=template.provider,
                connector_version=template.connector_version,
                nse_artifact_ids=template.nse_artifact_ids,
                prior_index_id=None,
                entries=(entry,),
                total_completion_count=template.total_completion_count,
                updated_at=template.updated_at,
                complete=False,
            )
        )

    def _rehashed_prior(self, first, **entry_overrides):
        """Re-hash a genuine prior entry after mutating its claimed evidence."""

        original = first.entries[0]
        values = dict(
            request_id=original.request_id,
            provider_snapshot_id=original.provider_snapshot_id,
            historical_batch_id=original.historical_batch_id,
            reconciliation_report_id=original.reconciliation_report_id,
            reconciliation_snapshot_id=original.reconciliation_snapshot_id,
            reconciled_at=original.reconciled_at,
            passed=original.passed,
        )
        values.update(entry_overrides)
        return self._prior_index(
            first, HistoricalReconciliationIndexEntry(**values)
        )

    def test_rehashed_prior_evidence_is_rejected_before_any_write(self) -> None:
        first = self.run_index(maximum_requests=1)
        original = first.entries[0]
        forged = self._rehashed_prior(
            first,
            historical_batch_id="c" * 64,
            reconciliation_report_id="a" * 64,
            reconciliation_snapshot_id="b" * 64,
            passed=not original.passed,
        )

        forged.verify_content_identity()
        self.assertEqual(forged.entries[0].request_id, original.request_id)
        self.assertEqual(
            forged.entries[0].provider_snapshot_id, original.provider_snapshot_id
        )
        before_indexes = self._stored_index_ids()
        before_reports = len(self.collector.reports)

        with self.assertRaises(HistoricalBulkReconciliationError):
            self.run_index(prior_index_id=forged.index_id)

        self.assertEqual(self._stored_index_ids(), before_indexes)
        self.assertEqual(len(self.collector.reports), before_reports)

    def test_each_isolated_prior_evidence_mutation_fails_closed(self) -> None:
        first = self.run_index(maximum_requests=1)
        original = first.entries[0]

        for override in (
            {"historical_batch_id": "c" * 64},
            {"reconciliation_report_id": "a" * 64},
            {"reconciliation_snapshot_id": "b" * 64},
            {"reconciled_at": original.reconciled_at - timedelta(seconds=1)},
            {"passed": not original.passed},
        ):
            with self.subTest(override=tuple(override)):
                forged = self._rehashed_prior(first, **override)
                self.assertEqual(
                    forged.entries[0].request_id, original.request_id
                )
                self.assertEqual(
                    forged.entries[0].provider_snapshot_id,
                    original.provider_snapshot_id,
                )
                before_indexes = self._stored_index_ids()
                before_reports = len(self.collector.reports)

                with self.assertRaises(HistoricalBulkReconciliationError):
                    self.run_index(prior_index_id=forged.index_id)

                self.assertEqual(self._stored_index_ids(), before_indexes)
                self.assertEqual(len(self.collector.reports), before_reports)

    def test_prior_entry_pointing_at_another_completions_evidence_is_rejected(
        self,
    ) -> None:
        complete = self.run_index()
        first_entry, other = complete.entries[0], complete.entries[1]
        forged = self._prior_index(
            complete,
            HistoricalReconciliationIndexEntry(
                request_id=first_entry.request_id,
                provider_snapshot_id=first_entry.provider_snapshot_id,
                historical_batch_id=other.historical_batch_id,
                reconciliation_report_id=other.reconciliation_report_id,
                reconciliation_snapshot_id=other.reconciliation_snapshot_id,
                reconciled_at=other.reconciled_at,
                passed=other.passed,
            ),
        )

        with self.assertRaisesRegex(
            HistoricalBulkReconciliationError, "pinned provider batch"
        ):
            self.run_index(prior_index_id=forged.index_id)

    def test_forged_prior_provider_snapshot_envelope_is_rejected(self) -> None:
        first = self.run_index(maximum_requests=1)
        prior_snapshot_id = first.entries[0].provider_snapshot_id
        stored = self.snapshot_store.get(
            historical_dataset_name(self.plan.provider), prior_snapshot_id
        )

        for forged in (
            object(),
            forge_stored(stored, provider_version="wrong-connector/v9"),
            forge_stored(stored, payload_bytes=stored.payload_bytes + b" "),
        ):
            with self.subTest(forged=type(forged).__name__):
                proxy = ProxySnapshotStore(
                    self.snapshot_store, {prior_snapshot_id: forged}
                )
                before_reports = len(self.collector.reports)
                with self.assertRaises(
                    HistoricalBulkReconciliationIntegrityError
                ):
                    self.run_index(
                        prior_index_id=first.index_id,
                        service=self.service(snapshot_store=proxy),
                    )
                self.assertEqual(len(self.collector.reports), before_reports)

    def test_forged_prior_reconciliation_snapshot_envelope_is_rejected(
        self,
    ) -> None:
        first = self.run_index(maximum_requests=1)
        reconciliation_snapshot_id = first.entries[0].reconciliation_snapshot_id
        stored = self.snapshot_store.get(
            HISTORICAL_RECONCILIATION_DATASET, reconciliation_snapshot_id
        )

        for forged in (
            object(),
            forge_stored(stored, provider_version="wrong-policy/v1"),
            forge_stored(stored, snapshot_id="d" * 64),
            forge_stored(stored, payload_bytes=stored.payload_bytes + b" "),
        ):
            with self.subTest(forged=type(forged).__name__):
                proxy = ProxySnapshotStore(
                    self.snapshot_store, {reconciliation_snapshot_id: forged}
                )
                before_reports = len(self.collector.reports)
                with self.assertRaises(
                    HistoricalBulkReconciliationIntegrityError
                ):
                    self.run_index(
                        prior_index_id=first.index_id,
                        service=self.service(snapshot_store=proxy),
                    )
                self.assertEqual(len(self.collector.reports), before_reports)

    def test_prior_entries_are_verified_but_never_reconciled_again(self) -> None:
        first = self.run_index(maximum_requests=1)
        entry = first.entries[0]
        before = self.snapshot_store.get(
            HISTORICAL_RECONCILIATION_DATASET, entry.reconciliation_snapshot_id
        )
        before_reports = len(self.collector.reports)

        second = self.run_index(
            maximum_requests=MAXIMUM_RECONCILIATIONS_PER_RUN,
            prior_index_id=first.index_id,
        )

        self.assertEqual(len(self.collector.reports) - before_reports, 1)
        self.assertEqual(
            self.collector.reports[-1].historical_request_id,
            self.progress.completions[1].request_id,
        )
        self.assertEqual(second.entries[0], entry)
        after = self.snapshot_store.get(
            HISTORICAL_RECONCILIATION_DATASET, entry.reconciliation_snapshot_id
        )
        self.assertEqual(after.payload_bytes, before.payload_bytes)
        self.assertEqual(after.manifest, before.manifest)


class ReconciliationIndexModelTests(BulkReconciliationFixture):
    def setUp(self) -> None:
        super().setUp()
        self.index = self.run_index()

    def test_entry_rejects_bad_identifiers_and_types(self) -> None:
        entry = self.index.entries[0]
        for override in (
            {"request_id": "short"},
            {"provider_snapshot_id": "A" * 64},
            {"historical_batch_id": 1},
            {"reconciliation_report_id": None},
            {"reconciliation_snapshot_id": "not-a-sha256"},
            {"passed": 1},
            {"reconciled_at": datetime(2026, 7, 22, 10, 0)},
            {"reconciled_at": "2026-07-22T10:00:00+00:00"},
        ):
            with self.subTest(override=override):
                with self.assertRaises((TypeError, ValueError)):
                    replace(entry, **override)

    def test_entry_tampering_is_detected(self) -> None:
        entry = self.index.entries[0]
        object.__setattr__(entry, "passed", not entry.passed)

        with self.assertRaises(HistoricalBulkReconciliationIntegrityError):
            entry.verify_content_identity()

    def test_index_rejects_duplicate_entry_bindings(self) -> None:
        entry = self.index.entries[0]

        with self.assertRaisesRegex(ValueError, "unique"):
            replace(self.index, entries=(entry, entry), complete=True)

    def test_index_requires_sorted_unique_artifact_ids(self) -> None:
        with self.assertRaises(ValueError):
            replace(self.index, nse_artifact_ids=("b" * 64, "a" * 64))
        with self.assertRaises(ValueError):
            replace(self.index, nse_artifact_ids=("a" * 64, "a" * 64))
        with self.assertRaises(TypeError):
            replace(self.index, nse_artifact_ids=())

    def test_complete_flag_is_derived_not_caller_controlled(self) -> None:
        with self.assertRaisesRegex(ValueError, "complete flag"):
            replace(self.index, complete=False)
        with self.assertRaisesRegex(ValueError, "complete flag"):
            replace(self.index, complete=1)
        with self.assertRaisesRegex(ValueError, "complete flag"):
            replace(self.index, total_completion_count=3)

    def test_entries_cannot_exceed_total_completion_count(self) -> None:
        with self.assertRaises(ValueError):
            replace(self.index, total_completion_count=1)

    def test_total_completion_count_must_be_a_positive_exact_integer(self) -> None:
        for bad in (0, -1, True, "2", 2.0):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    replace(self.index, total_completion_count=bad)

    def test_updated_at_cannot_predate_an_entry(self) -> None:
        with self.assertRaisesRegex(ValueError, "predate"):
            replace(self.index, updated_at=RECONCILED_AT - timedelta(seconds=1))

    def test_fixed_safety_flags_and_versions_are_pinned(self) -> None:
        for override in (
            {"collection_only": False},
            {"actionable": True},
            {"training_eligible": True},
            {"schema_version": "wrong/v0"},
            {"policy_version": "wrong/v0"},
            {"codec_version": "wrong/v0"},
        ):
            with self.subTest(override=override):
                with self.assertRaises(ValueError):
                    replace(self.index, **override)

    def test_forged_index_id_and_nested_entry_are_detected(self) -> None:
        original = self.index.index_id
        object.__setattr__(self.index.entries[0], "request_id", "0" * 64)
        with self.assertRaises(HistoricalBulkReconciliationIntegrityError):
            self.index.verify_content_identity()
        self.assertEqual(self.index.index_id, original)

        object.__setattr__(self.index, "index_id", "0" * 64)
        with self.assertRaises(HistoricalBulkReconciliationIntegrityError):
            self.index.verify_content_identity()

    def test_snapshot_id_helper_requires_matching_lineage(self) -> None:
        self.assertEqual(
            reconciliation_index_snapshot_ids(
                self.index,
                expected_plan_id=self.index.plan_id,
                expected_progress_id=self.index.progress_id,
            ),
            tuple(
                value.reconciliation_snapshot_id for value in self.index.entries
            ),
        )
        with self.assertRaises(HistoricalBulkReconciliationError):
            reconciliation_index_snapshot_ids(
                self.index,
                expected_plan_id="1" * 64,
                expected_progress_id=self.index.progress_id,
            )
        with self.assertRaises(HistoricalBulkReconciliationError):
            reconciliation_index_snapshot_ids(
                self.index,
                expected_plan_id=self.index.plan_id,
                expected_progress_id="1" * 64,
            )
        with self.assertRaises(HistoricalBulkReconciliationError):
            reconciliation_index_snapshot_ids(
                "not-an-index",
                expected_plan_id=self.index.plan_id,
                expected_progress_id=self.index.progress_id,
            )


class ReconciliationIndexCodecTests(BulkReconciliationFixture):
    def setUp(self) -> None:
        super().setUp()
        self.index = self.run_index()
        self.payload = encode_historical_reconciliation_index(self.index)

    def test_round_trip_is_deterministic(self) -> None:
        decoded = decode_historical_reconciliation_index(self.payload)

        self.assertEqual(decoded, self.index)
        self.assertEqual(decoded.index_id, self.index.index_id)
        self.assertEqual(
            encode_historical_reconciliation_index(decoded), self.payload
        )

    def test_encode_rejects_a_foreign_object(self) -> None:
        with self.assertRaises(TypeError):
            encode_historical_reconciliation_index("not-an-index")

    def test_missing_and_extra_root_keys_are_rejected(self) -> None:
        root = json.loads(self.payload)
        for key in tuple(root):
            with self.subTest(missing=key):
                mutated = {name: item for name, item in root.items() if name != key}
                with self.assertRaises(
                    HistoricalBulkReconciliationIntegrityError
                ):
                    decode_historical_reconciliation_index(self._dump(mutated))
        with self.assertRaises(HistoricalBulkReconciliationIntegrityError):
            decode_historical_reconciliation_index(
                self._dump({**root, "extra": 1})
            )

    def test_missing_and_extra_entry_keys_are_rejected(self) -> None:
        root = json.loads(self.payload)
        for key in tuple(root["entries"][0]):
            with self.subTest(missing=key):
                mutated = json.loads(self.payload)
                del mutated["entries"][0][key]
                with self.assertRaises(
                    HistoricalBulkReconciliationIntegrityError
                ):
                    decode_historical_reconciliation_index(self._dump(mutated))
        mutated = json.loads(self.payload)
        mutated["entries"][0]["extra"] = 1
        with self.assertRaises(HistoricalBulkReconciliationIntegrityError):
            decode_historical_reconciliation_index(self._dump(mutated))

    def test_duplicate_json_keys_are_rejected(self) -> None:
        text = self.payload.decode("utf-8").rstrip("\n")
        duplicated = text[:-1] + ',"complete":false}\n'

        with self.assertRaisesRegex(
            HistoricalBulkReconciliationIntegrityError, "duplicate JSON keys"
        ):
            decode_historical_reconciliation_index(duplicated.encode("utf-8"))

    def test_floats_and_json_constants_are_rejected(self) -> None:
        root = json.loads(self.payload)
        float_payload = self._dump({**root, "total_completion_count": 2.0})
        constant_payload = self._dump(
            {**root, "total_completion_count": float("nan")}, allow_nan=True
        )

        for payload in (float_payload, constant_payload):
            with self.subTest(payload=payload[:40]):
                with self.assertRaises(
                    HistoricalBulkReconciliationIntegrityError
                ):
                    decode_historical_reconciliation_index(payload)

    def test_noncanonical_encoding_is_rejected(self) -> None:
        root = json.loads(self.payload)
        indented = (json.dumps(root, indent=2, sort_keys=True) + "\n").encode(
            "utf-8"
        )
        reversed_root = {name: root[name] for name in reversed(list(root))}
        unsorted = (
            json.dumps(reversed_root, separators=(",", ":"), sort_keys=False)
            + "\n"
        ).encode("utf-8")

        for payload in (indented, unsorted):
            with self.subTest(payload=payload[:40]):
                with self.assertRaises(
                    HistoricalBulkReconciliationIntegrityError
                ):
                    decode_historical_reconciliation_index(payload)

    def test_stale_entry_and_index_identifiers_are_rejected(self) -> None:
        stale_entry = json.loads(self.payload)
        stale_entry["entries"][0]["entry_id"] = "0" * 64
        stale_index = json.loads(self.payload)
        stale_index["index_id"] = "0" * 64

        for mutated in (stale_entry, stale_index):
            with self.subTest(mutated=mutated["index_id"]):
                with self.assertRaises(
                    HistoricalBulkReconciliationIntegrityError
                ):
                    decode_historical_reconciliation_index(self._dump(mutated))

    def test_invalid_container_types_are_rejected(self) -> None:
        for key, value in (
            ("entries", {}),
            ("nse_artifact_ids", "not-a-list"),
            ("passed", None),
        ):
            with self.subTest(key=key):
                mutated = json.loads(self.payload)
                if key == "passed":
                    mutated["entries"][0][key] = value
                else:
                    mutated[key] = value
                with self.assertRaises(
                    HistoricalBulkReconciliationIntegrityError
                ):
                    decode_historical_reconciliation_index(self._dump(mutated))

    def test_malformed_empty_and_oversized_payloads_are_rejected(self) -> None:
        for payload in (
            b"",
            b"{",
            b"\xff\xfe",
            b"[]",
            bytes(MAXIMUM_RECONCILIATION_INDEX_BYTES + 1),
        ):
            with self.subTest(length=len(payload)):
                with self.assertRaises(
                    HistoricalBulkReconciliationIntegrityError
                ):
                    decode_historical_reconciliation_index(payload)
        with self.assertRaises(HistoricalBulkReconciliationIntegrityError):
            decode_historical_reconciliation_index("not-bytes")

    def test_decoder_messages_are_static_and_sanitized(self) -> None:
        secret = self.index.entries[0].reconciliation_snapshot_id
        mutated = json.loads(self.payload)
        mutated["index_id"] = "0" * 64

        with self.assertRaises(
            HistoricalBulkReconciliationIntegrityError
        ) as caught:
            decode_historical_reconciliation_index(self._dump(mutated))

        self.assertEqual(
            str(caught.exception),
            "stored historical reconciliation index is invalid",
        )
        self.assertNotIn(secret, str(caught.exception))

    @staticmethod
    def _dump(value: dict, *, allow_nan: bool = False) -> bytes:
        return (
            json.dumps(
                value,
                ensure_ascii=False,
                allow_nan=allow_nan,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8")


class ReconciliationIndexStoreTests(BulkReconciliationFixture):
    def setUp(self) -> None:
        super().setUp()
        self.index = self.run_index()
        self.target = self.index_store.dataset_root / self.index.index_id

    def test_exact_get_round_trips_and_is_idempotent(self) -> None:
        self.assertEqual(self.index_store.get(self.index.index_id), self.index)
        self.assertEqual(self.index_store.put(self.index), self.index)
        self.assertEqual(
            (self.target / INDEX_FILENAME).read_bytes(),
            encode_historical_reconciliation_index(self.index),
        )
        self.assertEqual(
            self.index_store.dataset_root.name,
            HISTORICAL_RECONCILIATION_INDEX_DATASET,
        )

    def test_put_rejects_a_foreign_object(self) -> None:
        with self.assertRaises(TypeError):
            self.index_store.put("not-an-index")

    def test_unknown_and_malformed_ids_are_rejected(self) -> None:
        for bad in ("0" * 64, "not-a-sha256", "../escape", "", None):
            with self.subTest(bad=bad):
                with self.assertRaises(HistoricalBulkReconciliationError):
                    self.index_store.get(bad)

    def test_collision_with_different_content_is_rejected(self) -> None:
        partial = HistoricalReconciliationIndex(
            plan_id=self.index.plan_id,
            progress_id=self.index.progress_id,
            provider=self.index.provider,
            connector_version=self.index.connector_version,
            nse_artifact_ids=self.index.nse_artifact_ids,
            prior_index_id=None,
            entries=self.index.entries[:1],
            total_completion_count=self.index.total_completion_count,
            updated_at=self.index.updated_at,
            complete=False,
        )
        colliding = self.index_store.dataset_root / partial.index_id
        colliding.mkdir()
        (colliding / INDEX_FILENAME).write_bytes(
            encode_historical_reconciliation_index(self.index)
        )

        with self.assertRaises(HistoricalBulkReconciliationIntegrityError):
            self.index_store.put(partial)
        with self.assertRaises(HistoricalBulkReconciliationIntegrityError):
            self.index_store.get(partial.index_id)

    def test_tampered_stored_bytes_are_detected(self) -> None:
        (self.target / INDEX_FILENAME).write_bytes(b"{}\n")

        with self.assertRaises(HistoricalBulkReconciliationIntegrityError):
            self.index_store.get(self.index.index_id)

    def test_unexpected_and_nonregular_entries_are_rejected(self) -> None:
        extra = self.target / "extra.json"
        extra.write_bytes(b"{}\n")
        with self.assertRaises(HistoricalBulkReconciliationIntegrityError):
            self.index_store.get(self.index.index_id)
        extra.unlink()

        (self.target / INDEX_FILENAME).unlink()
        (self.target / INDEX_FILENAME).mkdir()
        with self.assertRaises(HistoricalBulkReconciliationIntegrityError):
            self.index_store.get(self.index.index_id)

    def test_index_path_that_is_a_file_is_rejected(self) -> None:
        other = self.index_store.dataset_root / ("1" * 64)
        other.write_bytes(b"{}\n")

        with self.assertRaises(HistoricalBulkReconciliationIntegrityError):
            self.index_store.get("1" * 64)

    def test_symlinked_index_directory_is_rejected(self) -> None:
        link = self.index_store.dataset_root / ("2" * 64)
        try:
            link.symlink_to(self.target, target_is_directory=True)
        except (OSError, NotImplementedError):
            self.skipTest("platform does not support creating symbolic links")

        with self.assertRaises(HistoricalBulkReconciliationIntegrityError):
            self.index_store.get("2" * 64)

    def test_symlinked_index_file_is_rejected(self) -> None:
        genuine = self.target / INDEX_FILENAME
        payload = genuine.read_bytes()
        elsewhere = self.root / "elsewhere.json"
        elsewhere.write_bytes(payload)
        genuine.unlink()
        try:
            genuine.symlink_to(elsewhere)
        except (OSError, NotImplementedError):
            self.skipTest("platform does not support creating symbolic links")

        with self.assertRaises(HistoricalBulkReconciliationIntegrityError):
            self.index_store.get(self.index.index_id)

    def test_oversized_stored_payload_is_rejected(self) -> None:
        (self.target / INDEX_FILENAME).write_bytes(
            bytes(MAXIMUM_RECONCILIATION_INDEX_BYTES + 1)
        )

        with self.assertRaises(HistoricalBulkReconciliationIntegrityError):
            self.index_store.get(self.index.index_id)

    def test_store_exposes_no_listing_latest_or_find_operation(self) -> None:
        for name in ("list", "latest", "find", "find_by_selection_key", "all"):
            with self.subTest(name=name):
                self.assertFalse(hasattr(self.index_store, name))


class PersistedReportEvidenceTests(BulkReconciliationFixture):
    def test_reconciliation_snapshots_match_their_index_entries(self) -> None:
        index = self.run_index()

        for entry in index.entries:
            stored = self.snapshot_store.get(
                HISTORICAL_RECONCILIATION_DATASET,
                entry.reconciliation_snapshot_id,
            )
            report = stored.normalized_payload
            report.verify_content_identity()
            self.assertEqual(report.report_id, entry.reconciliation_report_id)
            self.assertEqual(
                report.historical_batch_id, entry.historical_batch_id
            )
            self.assertEqual(report.reconciled_at, entry.reconciled_at)
            self.assertEqual(report.passed, entry.passed)
            self.assertEqual(
                stored.payload_bytes, encode_market_payload(report)
            )
            self.assertEqual(
                stored.manifest.payload_sha256,
                hashlib.sha256(stored.payload_bytes).hexdigest(),
            )


class ReconciliationIndexEntryConstructionTests(unittest.TestCase):
    def test_entry_identity_is_content_addressed(self) -> None:
        values = dict(
            request_id="a" * 64,
            provider_snapshot_id="b" * 64,
            historical_batch_id="c" * 64,
            reconciliation_report_id="d" * 64,
            reconciliation_snapshot_id="e" * 64,
            reconciled_at=datetime(2026, 7, 22, 10, 0, tzinfo=UTC),
            passed=True,
        )
        first = HistoricalReconciliationIndexEntry(**values)
        second = HistoricalReconciliationIndexEntry(**values)
        different = HistoricalReconciliationIndexEntry(**{**values, "passed": False})

        self.assertEqual(first.entry_id, second.entry_id)
        self.assertNotEqual(first.entry_id, different.entry_id)
        first.verify_content_identity()

    def test_naive_reconciled_at_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            HistoricalReconciliationIndexEntry(
                request_id="a" * 64,
                provider_snapshot_id="b" * 64,
                historical_batch_id="c" * 64,
                reconciliation_report_id="d" * 64,
                reconciliation_snapshot_id="e" * 64,
                reconciled_at=datetime(2026, 7, 22, 10, 0),
                passed=True,
            )


class ReconciliationIndexCodecVersionTests(unittest.TestCase):
    def test_fixed_version_constants_are_stable(self) -> None:
        self.assertEqual(
            HISTORICAL_RECONCILIATION_INDEX_SCHEMA_VERSION,
            "historical-reconciliation-index/v1",
        )
        self.assertEqual(
            HISTORICAL_RECONCILIATION_INDEX_POLICY_VERSION,
            "historical-reconciliation-index-policy/v1",
        )
        self.assertEqual(
            HISTORICAL_RECONCILIATION_INDEX_CODEC_VERSION,
            "historical-reconciliation-index-json/v1",
        )
        self.assertEqual(MAXIMUM_RECONCILIATIONS_PER_RUN, 500)
        self.assertEqual(
            MAXIMUM_RECONCILIATION_INDEX_BYTES, 32 * 1024 * 1024
        )


if __name__ == "__main__":
    unittest.main()
