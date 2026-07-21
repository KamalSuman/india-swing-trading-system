from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import timedelta

from india_swing.calendar_data import materialize_collection_calendar
from india_swing.calendar_data.artifact_store import LocalCalendarSourceArtifactStore
from india_swing.calendar_data.materialization_store import LocalCalendarMaterializationStore
from india_swing.calendar_data.models import CALENDAR_DECLARATION_SCHEMA_VERSION
from india_swing.daily_pipeline import run_daily_pipeline
from india_swing.daily_pipeline.derived_evidence import (
    daily_run_chain,
    materialize_daily_derived_evidence,
)
from india_swing.daily_pipeline.derived_evidence_store import LocalDailyDerivedEvidenceStore
from india_swing.liquidity import LocalLiquiditySnapshotStore
from india_swing.paper_outcomes import (
    LocalPaperPortfolioBatchStore,
    LocalPaperPortfolioPreparationStore,
    LocalPaperPortfolioStateStore,
    PaperPortfolioPipelineBridgeError,
    prepare_paper_portfolio_from_daily_pipeline,
)
from india_swing.paper_trades import LocalPaperTradeLedger
from india_swing.tick_sizes import LocalTickSizeSnapshotStore
from india_swing.universe import LocalCollectionUniverseSnapshotStore
from tests.test_daily_pipeline import DailyPipelineTests
from tests.test_paper_outcomes import _registration
from tests.test_reconciliation import CUTOFF, SESSION, _calendar


class PaperPortfolioPipelineBridgeTests(DailyPipelineTests):
    def _calendar_store(self):
        root = self.root / "calendar_data"
        inputs = self.root / "calendar_inputs"
        inputs.mkdir()
        source_bytes = b"%PDF-1.7\nNSE CM TEST CALENDAR\n%%EOF\n"
        source_path = inputs / "CM-2026.pdf"
        declaration_path = inputs / "CM-2026.events.json"
        source_path.write_bytes(source_bytes)
        declaration_path.write_text(
            json.dumps(
                {
                    "schema_version": CALENDAR_DECLARATION_SCHEMA_VERSION,
                    "exchange": "NSE",
                    "segment": "CM",
                    "claimed_authority": "NSE",
                    "claimed_document_id": "CM-2026",
                    "claimed_issue_date": "2026-01-01",
                    "claimed_source_url": "https://example.invalid/CM-2026.pdf",
                    "source_filename": "CM-2026.pdf",
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
        times = iter((CUTOFF - timedelta(seconds=2), CUTOFF - timedelta(seconds=1)))
        source = LocalCalendarSourceArtifactStore(
            root, clock=lambda: next(times)
        ).import_source(source_path, declaration_path)
        materialization = materialize_collection_calendar(
            sources=(source,),
            coverage_start=SESSION,
            coverage_end=SESSION + timedelta(days=1),
            cutoff=CUTOFF,
            observed_date_artifacts=(),
        )
        store = LocalCalendarMaterializationStore(root, self.daily_root)
        return store, store.put(materialization)

    def test_exact_derived_bundle_prepares_the_active_portfolio(self) -> None:
        calendar_store, stored_calendar = self._calendar_store()
        run = run_daily_pipeline(
            market_session=SESSION,
            cutoff=CUTOFF,
            calendar_materialization_id=stored_calendar.materialization.materialization_id,
            calendar=stored_calendar.materialization.calendar_snapshot,
            security_master_file=self.master_file,
            daily_bundle_file=self.bundle_file,
            previous_run_id=None,
            reference_store=self.reference_store,
            daily_store=self.daily_store,
            historical_store=self.historical_store,
            identity_store=self.identity_store,
            adjudication_store=self.adjudication_store,
            run_store=self.run_store,
        )
        tick_store = LocalTickSizeSnapshotStore(
            self.root / "tick_sizes", self.reference_root
        )
        derived = materialize_daily_derived_evidence(
            runs=daily_run_chain(run, run_store=self.run_store),
            reference_store=self.reference_store,
            historical_store=self.historical_store,
            tick_store=tick_store,
            liquidity_store=LocalLiquiditySnapshotStore(
                self.root / "liquidity", self.history_root, self.daily_root
            ),
            universe_store=LocalCollectionUniverseSnapshotStore(
                self.root / "universe", self.reference_root
            ),
            minimum_history_sessions=1,
        )
        derived_store = LocalDailyDerivedEvidenceStore(self.pipeline_root)
        derived_store.publish(derived)
        registration = replace(
            _registration(),
            decision_time=CUTOFF - timedelta(hours=2),
            earliest_entry_at=CUTOFF + timedelta(hours=12),
            entry_expires_at=CUTOFF + timedelta(days=2),
        )
        ledger = LocalPaperTradeLedger(self.root / "state" / "paper")
        ledger.register_value(registration)
        preparation_store = LocalPaperPortfolioPreparationStore(
            self.root / "state" / "preparations"
        )
        batch_store = LocalPaperPortfolioBatchStore(
            self.root / "state" / "batches"
        )

        result = prepare_paper_portfolio_from_daily_pipeline(
            run_id=run.run_id,
            derived_evidence_id=derived.evidence_id,
            ledger=ledger,
            run_store=self.run_store,
            derived_store=derived_store,
            calendar_store=calendar_store,
            tick_store=tick_store,
            historical_store=self.historical_store,
            reference_store=self.reference_store,
            portfolio_store=LocalPaperPortfolioStateStore(
                self.root / "state" / "portfolio"
            ),
            preparation_store=preparation_store,
            batch_store=batch_store,
        )

        self.assertEqual(result.run_id, run.run_id)
        self.assertEqual(result.derived_evidence_id, derived.evidence_id)
        self.assertEqual(
            result.preparation.historical_artifact_ids,
            derived.historical_price_artifact_ids,
        )
        self.assertEqual(
            result.preparation.listings[0].tick_snapshot_id,
            derived.tick_size_snapshot_id,
        )
        self.assertLessEqual(
            tick_store.get(result.preparation.listings[0].tick_snapshot_id).knowledge_time,
            registration.decision_time,
        )
        self.assertEqual(result.preparation.listings[0].series, "EQ")
        self.assertEqual(result.preparation.listings[0].validated_isin, "INE009A01021")
        self.assertEqual(result.batch.outcome_jobs[0].registration_id, registration.registration_id)
        self.assertEqual(
            preparation_store.get(result.preparation.preparation_id),
            result.preparation,
        )
        self.assertEqual(batch_store.get(result.batch.batch_id), result.batch)

        early_registration = replace(
            _registration(),
            decision_time=CUTOFF - timedelta(hours=4),
            earliest_entry_at=CUTOFF + timedelta(hours=12),
            entry_expires_at=CUTOFF + timedelta(days=2),
        )
        early_ledger = LocalPaperTradeLedger(self.root / "early_state" / "paper")
        early_ledger.register_value(early_registration)
        early_preparations = LocalPaperPortfolioPreparationStore(
            self.root / "early_state" / "preparations"
        )
        with self.assertRaisesRegex(
            PaperPortfolioPipelineBridgeError, "decision-time tick"
        ):
            prepare_paper_portfolio_from_daily_pipeline(
                run_id=run.run_id,
                derived_evidence_id=derived.evidence_id,
                ledger=early_ledger,
                run_store=self.run_store,
                derived_store=derived_store,
                calendar_store=calendar_store,
                tick_store=tick_store,
                historical_store=self.historical_store,
                reference_store=self.reference_store,
                portfolio_store=LocalPaperPortfolioStateStore(
                    self.root / "early_state" / "portfolio"
                ),
                preparation_store=early_preparations,
                batch_store=LocalPaperPortfolioBatchStore(
                    self.root / "early_state" / "batches"
                ),
            )
        self.assertFalse(early_preparations.specifications_root.exists())


if __name__ == "__main__":
    import unittest

    unittest.main()
