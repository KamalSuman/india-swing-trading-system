from __future__ import annotations

import json
import unittest

from india_swing.daily_pipeline.derived_evidence import (
    DailyDerivedEvidenceIntegrityError,
    daily_run_chain,
    materialize_daily_derived_evidence,
    validate_daily_derived_evidence,
)
from india_swing.daily_pipeline.derived_evidence_store import (
    LocalDailyDerivedEvidenceStore,
)
from india_swing.liquidity import LocalLiquiditySnapshotStore
from india_swing.tick_sizes import LocalTickSizeSnapshotStore
from india_swing.universe import LocalCollectionUniverseSnapshotStore

from tests.test_daily_pipeline import DailyPipelineTests


class DailyDerivedEvidenceTests(DailyPipelineTests):
    def setUp(self) -> None:
        super().setUp()
        self.tick_root = self.root / "ticks"
        self.liquidity_root = self.root / "liquidity"
        self.universe_root = self.root / "universe"
        self.derived_store = LocalDailyDerivedEvidenceStore(self.pipeline_root)

    def test_materializes_replayable_evidence_for_one_sealed_run(self) -> None:
        run = self._run()
        value = materialize_daily_derived_evidence(
            runs=daily_run_chain(run, run_store=self.run_store),
            reference_store=self.reference_store,
            historical_store=self.historical_store,
            tick_store=LocalTickSizeSnapshotStore(self.tick_root, self.reference_root),
            liquidity_store=LocalLiquiditySnapshotStore(
                self.liquidity_root,
                self.history_root,
                self.daily_root,
            ),
            universe_store=LocalCollectionUniverseSnapshotStore(
                self.universe_root,
                self.reference_root,
            ),
        )

        self.assertEqual(value.run_id, run.run_id)
        self.assertEqual(value.historical_price_artifact_ids, (run.historical_price_artifact_id,))
        self.assertEqual(value.minimum_history_sessions, 120)
        self.assertIn("DERIVED_FROM_COLLECTION_ONLY_RUN", value.reason_codes)
        value.verify_content_identity()
        self.assertEqual(
            validate_daily_derived_evidence(
                value,
                run=run,
                run_store=self.run_store,
            ),
            (run,),
        )
        self.assertEqual(self.derived_store.publish(value), value)
        self.assertEqual(self.derived_store.get(value.evidence_id), value)

    def test_tampered_derived_evidence_is_rejected(self) -> None:
        run = self._run()
        value = materialize_daily_derived_evidence(
            runs=(run,),
            reference_store=self.reference_store,
            historical_store=self.historical_store,
            tick_store=LocalTickSizeSnapshotStore(self.tick_root, self.reference_root),
            liquidity_store=LocalLiquiditySnapshotStore(
                self.liquidity_root,
                self.history_root,
                self.daily_root,
            ),
            universe_store=LocalCollectionUniverseSnapshotStore(
                self.universe_root,
                self.reference_root,
            ),
        )
        self.derived_store.publish(value)
        path = self.derived_store.path_for(value.evidence_id)
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["evidence"]["minimum_history_sessions"] = 1
        path.write_text(json.dumps(payload), encoding="utf-8")

        with self.assertRaises(DailyDerivedEvidenceIntegrityError):
            self.derived_store.get(value.evidence_id)


if __name__ == "__main__":
    unittest.main()
