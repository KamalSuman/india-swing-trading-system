from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from india_swing.daily_pipeline import (
    DailyPipelineIntegrityError,
    DailyPipelineRunConflict,
    LocalDailyPipelineRunStore,
    run_daily_pipeline,
)
from india_swing.daily_reports.artifact_store import LocalDailyBundleArtifactStore
from india_swing.historical_prices.artifact_store import LocalHistoricalPriceArtifactStore
from india_swing.identity_registry.adjudication_store import LocalIdentityAdjudicationQueueStore
from india_swing.identity_registry.artifact_store import LocalIdentityRegistryStore
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference_data.artifact_store import LocalReferenceArtifactStore

from tests.test_reconciliation import (
    BUNDLE_FIRST_SEEN,
    BUNDLE_VALIDATED,
    CUTOFF,
    MASTER_FIRST_SEEN,
    MASTER_VALIDATED,
    SESSION,
    _bundle_bytes,
    _calendar,
    _clock,
    _master_bytes,
)


class DailyPipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.reference_root = self.root / "reference"
        self.daily_root = self.root / "daily"
        self.history_root = self.root / "history"
        self.identity_root = self.root / "identity"
        self.pipeline_root = self.root / "pipeline"
        self.input_root = self.root / "input"
        self.input_root.mkdir()
        self.master_file = self.input_root / "NSE_CM_security_15072026.csv.gz"
        self.bundle_file = self.input_root / "Reports-Daily-Multiple (1).zip"
        self.master_file.write_bytes(_master_bytes())
        self.bundle_file.write_bytes(_bundle_bytes())

        self.reference_store = LocalReferenceArtifactStore(
            self.reference_root,
            clock=_clock(MASTER_FIRST_SEEN, MASTER_VALIDATED),
        )
        self.daily_store = LocalDailyBundleArtifactStore(
            self.daily_root,
            clock=_clock(BUNDLE_FIRST_SEEN, BUNDLE_VALIDATED),
        )
        self.historical_store = LocalHistoricalPriceArtifactStore(
            self.history_root,
            self.daily_root,
        )
        self.identity_store = LocalIdentityRegistryStore(
            self.identity_root,
            self.reference_root,
        )
        self.adjudication_store = LocalIdentityAdjudicationQueueStore(
            self.identity_root,
            self.identity_store,
        )
        self.run_store = LocalDailyPipelineRunStore(self.pipeline_root)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _run(self, *, previous_run_id: str | None = None):
        return run_daily_pipeline(
            market_session=SESSION,
            cutoff=CUTOFF,
            calendar_materialization_id="8" * 64,
            calendar=_calendar(),
            security_master_file=self.master_file,
            daily_bundle_file=self.bundle_file,
            previous_run_id=previous_run_id,
            reference_store=self.reference_store,
            daily_store=self.daily_store,
            historical_store=self.historical_store,
            identity_store=self.identity_store,
            adjudication_store=self.adjudication_store,
            run_store=self.run_store,
        )

    def test_bootstrap_run_persists_complete_collection_only_lineage(self) -> None:
        run = self._run()

        self.assertEqual(run.market_session, SESSION)
        self.assertEqual(run.observed_dates, (SESSION,))
        self.assertEqual(len(run.security_master_artifact_ids), 1)
        self.assertEqual(len(run.daily_bundle_artifact_ids), 1)
        self.assertGreater(run.bar_count, 0)
        self.assertEqual(run.adjudication_case_count, run.identity_candidate_count)
        self.assertIn("NO_PREVIOUS_DAILY_RUN", run.completeness_issues)
        self.assertIn("IDENTITY_ADJUDICATION_REQUIRED", run.completeness_issues)
        self.assertIs(run.readiness, ReferenceReadiness.COLLECTION_ONLY)
        self.assertFalse(run.actionable)
        self.assertFalse(run.stable_identity_assigned)
        self.assertEqual(self.run_store.get(run.run_id), run)
        self.assertEqual(self.run_store.publish(run), run)
        self.assertEqual(self.run_store.list_runs(), (run,))

    def test_wrong_predecessor_and_tampered_run_fail_closed(self) -> None:
        run = self._run()
        with self.assertRaisesRegex(
            DailyPipelineIntegrityError,
            "preceding session",
        ):
            self._run(previous_run_id=run.run_id)

        path = self.run_store.path_for(run.run_id)
        value = json.loads(path.read_text(encoding="utf-8"))
        value["run"]["bar_count"] += 1
        path.write_text(json.dumps(value), encoding="utf-8")
        with self.assertRaises(DailyPipelineRunConflict):
            self.run_store.get(run.run_id)


if __name__ == "__main__":
    unittest.main()
