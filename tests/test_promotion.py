from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from india_swing.daily_pipeline import DailyPipelineRun, LocalDailyPipelineRunStore
from india_swing.daily_pipeline.config import DAILY_PIPELINE_ROOT_ENV
from india_swing.promotion import (
    ALERT_REQUIREMENTS,
    BACKTEST_REQUIREMENTS,
    PROMOTION_ROOT_ENV,
    LocalPromotionDecisionStore,
    PromotionCapability,
    PromotionEvidence,
    PromotionIntegrityError,
    PromotionStage,
    PromotionStoreConflict,
    decode_promotion_decision,
    encode_promotion_decision,
    evaluate_promotion,
    promotion_evidence_from_daily_run,
)
from india_swing.promotion.cli import main as promotion_main
from india_swing.reference import ReferenceReadiness


IST = timezone(timedelta(hours=5, minutes=30))
HISTORY_START = date(2020, 1, 1)
MARKET_SESSION = date(2026, 7, 16)
CUTOFF = datetime(2026, 7, 16, 17, 0, tzinfo=IST)


def verified(capability: PromotionCapability) -> PromotionEvidence:
    index = list(PromotionCapability).index(capability) + 1
    return PromotionEvidence(
        capability=capability,
        cutoff=CUTOFF,
        coverage_start=HISTORY_START,
        coverage_end=MARKET_SESSION,
        source_snapshot_ids=(f"{index:064x}",),
        readiness=ReferenceReadiness.POINT_IN_TIME_VERIFIED,
        complete=True,
        actionable=True,
        reason_codes=(),
    )


def all_verified() -> tuple[PromotionEvidence, ...]:
    return tuple(
        verified(value)
        for value in sorted(ALERT_REQUIREMENTS, key=lambda item: item.value)
    )


def daily_run() -> DailyPipelineRun:
    return DailyPipelineRun(
        market_session=MARKET_SESSION,
        cutoff=CUTOFF,
        calendar_materialization_id="1" * 64,
        calendar_snapshot_id="2" * 64,
        previous_run_id="3" * 64,
        security_master_artifact_ids=("4" * 64,),
        daily_bundle_artifact_ids=("5" * 64,),
        current_security_master_artifact_id="4" * 64,
        current_daily_bundle_artifact_id="5" * 64,
        observed_date_artifact_id="6" * 64,
        observed_dates=(MARKET_SESSION,),
        historical_price_artifact_id="7" * 64,
        historical_price_manifest_id="8" * 64,
        bar_count=3439,
        reconciliation_snapshot_id="9" * 64,
        reconciliation_global_reason_codes=(
            "CALENDAR_NOT_POINT_IN_TIME_VERIFIED",
            "EFFECTIVE_REG1_STATE_MISSING",
        ),
        retained_row_count=100,
        main_scope_count=70,
        sme_scope_count=10,
        unsupported_series_count=20,
        unresolved_count=30,
        traded_row_count=60,
        orphan_report_key_count=5,
        identity_registry_id="a" * 64,
        identity_registry_manifest_id="b" * 64,
        identity_observation_count=200,
        identity_candidate_count=2,
        identity_transition_count=5,
        identity_conflict_count=1,
        adjudication_queue_id="c" * 64,
        adjudication_case_count=2,
        adjudication_requirement_counts=(("OFFICIAL_LISTING_STATUS", 2),),
        completeness_issues=("COLLECTION_ONLY_INPUTS", "VERIFIED_LANDING_LINEAGE_UNAVAILABLE"),
        landing_input_lineage=None,
    )


class PromotionGateTests(unittest.TestCase):
    def test_complete_verified_evidence_reaches_alert_stage(self) -> None:
        result = evaluate_promotion(
            market_session=MARKET_SESSION,
            history_start=HISTORY_START,
            decision_cutoff=CUTOFF,
            evidence=all_verified(),
        )

        self.assertEqual(result.achieved_stage, PromotionStage.ALERT_ELIGIBLE)
        self.assertTrue(result.research_eligible)
        self.assertTrue(result.backtest_eligible)
        self.assertTrue(result.alert_eligible)
        result.verify_content_identity()

    def test_research_can_pass_while_backtest_and_alert_remain_blocked(self) -> None:
        research_capabilities = {
            PromotionCapability.CALENDAR,
            PromotionCapability.STABLE_IDENTITY,
            PromotionCapability.UNIVERSE,
            PromotionCapability.RAW_PRICES,
        }
        result = evaluate_promotion(
            market_session=MARKET_SESSION,
            history_start=HISTORY_START,
            decision_cutoff=CUTOFF,
            evidence=tuple(
                verified(value)
                for value in sorted(
                    research_capabilities,
                    key=lambda item: item.value,
                )
            ),
        )

        self.assertEqual(result.achieved_stage, PromotionStage.RESEARCH_ELIGIBLE)
        self.assertFalse(result.backtest_eligible)
        self.assertIn("MISSING_CORPORATE_ACTIONS", result.backtest_blockers)
        self.assertIn("MISSING_MODEL_VALIDATION", result.alert_blockers)

    def test_collection_only_real_archive_fails_closed_with_exact_reasons(self) -> None:
        calendar = PromotionEvidence(
            capability=PromotionCapability.CALENDAR,
            cutoff=CUTOFF,
            coverage_start=date(2026, 1, 1),
            coverage_end=date(2026, 7, 31),
            source_snapshot_ids=("1" * 64,),
            readiness=ReferenceReadiness.COLLECTION_ONLY,
            complete=False,
            actionable=False,
            reason_codes=("SOURCE_PROVENANCE_UNVERIFIED",),
        )
        result = evaluate_promotion(
            market_session=MARKET_SESSION,
            history_start=HISTORY_START,
            decision_cutoff=CUTOFF,
            evidence=(calendar,),
        )

        self.assertEqual(result.achieved_stage, PromotionStage.COLLECTION_ONLY)
        self.assertIn("CALENDAR_COLLECTION_ONLY", result.research_blockers)
        self.assertIn("CALENDAR_INCOMPLETE", result.research_blockers)
        self.assertIn("CALENDAR_NOT_ACTIONABLE", result.research_blockers)
        self.assertIn("CALENDAR_COVERAGE_GAP", result.research_blockers)
        self.assertIn(
            "CALENDAR_SOURCE_PROVENANCE_UNVERIFIED",
            result.research_blockers,
        )
        self.assertIn("MISSING_STABLE_IDENTITY", result.research_blockers)

    def test_future_knowledge_and_partial_history_are_independent_blockers(self) -> None:
        future_prices = replace(
            verified(PromotionCapability.RAW_PRICES),
            cutoff=CUTOFF + timedelta(minutes=1),
            coverage_start=date(2025, 1, 1),
        )
        evidence = tuple(
            future_prices if value.capability is PromotionCapability.RAW_PRICES else value
            for value in all_verified()
        )
        result = evaluate_promotion(
            market_session=MARKET_SESSION,
            history_start=HISTORY_START,
            decision_cutoff=CUTOFF,
            evidence=evidence,
        )

        self.assertIn("RAW_PRICES_FUTURE_KNOWLEDGE", result.research_blockers)
        self.assertIn("RAW_PRICES_COVERAGE_GAP", result.research_blockers)

    def test_synthetic_evidence_never_silently_promotes_real_work(self) -> None:
        calendar = replace(
            verified(PromotionCapability.CALENDAR),
            readiness=ReferenceReadiness.SYNTHETIC_TEST,
        )
        evidence = tuple(
            calendar if value.capability is PromotionCapability.CALENDAR else value
            for value in all_verified()
        )
        result = evaluate_promotion(
            market_session=MARKET_SESSION,
            history_start=HISTORY_START,
            decision_cutoff=CUTOFF,
            evidence=evidence,
        )

        self.assertIn("CALENDAR_SYNTHETIC_ONLY", result.research_blockers)
        self.assertEqual(result.achieved_stage, PromotionStage.COLLECTION_ONLY)

    def test_backtest_requirements_are_a_strict_subset_of_alert_requirements(self) -> None:
        self.assertTrue(BACKTEST_REQUIREMENTS < ALERT_REQUIREMENTS)

    def test_content_mutation_is_detected(self) -> None:
        result = evaluate_promotion(
            market_session=MARKET_SESSION,
            history_start=HISTORY_START,
            decision_cutoff=CUTOFF,
            evidence=all_verified(),
        )
        object.__setattr__(result, "market_session", date(2026, 7, 15))

        with self.assertRaises(PromotionIntegrityError):
            result.verify_content_identity()

    def test_daily_run_adapter_reports_collection_evidence_without_upgrading(self) -> None:
        evidence = promotion_evidence_from_daily_run(daily_run())

        self.assertEqual(len(evidence), 8)
        self.assertEqual(
            {value.capability for value in evidence},
            {
                PromotionCapability.CALENDAR,
                PromotionCapability.STABLE_IDENTITY,
                PromotionCapability.UNIVERSE,
                PromotionCapability.RAW_PRICES,
                PromotionCapability.LIQUIDITY,
                PromotionCapability.SURVEILLANCE,
                PromotionCapability.EXPLICIT_NONTRADING,
                PromotionCapability.RECONCILIATION,
            },
        )
        self.assertTrue(all(not value.actionable for value in evidence))
        decision = evaluate_promotion(
            market_session=MARKET_SESSION,
            history_start=HISTORY_START,
            decision_cutoff=CUTOFF,
            evidence=evidence,
        )
        self.assertEqual(decision.achieved_stage, PromotionStage.COLLECTION_ONLY)
        self.assertIn("MISSING_CORPORATE_ACTIONS", decision.backtest_blockers)
        self.assertIn("MISSING_TICK_SIZES", decision.backtest_blockers)


class PromotionPersistenceTests(unittest.TestCase):
    def decision(self):
        return evaluate_promotion(
            market_session=MARKET_SESSION,
            history_start=HISTORY_START,
            decision_cutoff=CUTOFF,
            evidence=promotion_evidence_from_daily_run(daily_run()),
        )

    def test_codec_and_store_round_trip_idempotently(self) -> None:
        decision = self.decision()
        self.assertEqual(
            decode_promotion_decision(encode_promotion_decision(decision)),
            decision,
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalPromotionDecisionStore(Path(temp_dir))
            first = store.put(decision)
            second = store.put(decision)

            self.assertEqual(first, decision)
            self.assertEqual(second, decision)
            self.assertEqual(store.list_decisions(), (decision,))

    def test_tampered_stored_decision_is_rejected(self) -> None:
        decision = self.decision()
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalPromotionDecisionStore(Path(temp_dir))
            store.put(decision)
            path = store.path_for(decision.decision_id)
            value = json.loads(path.read_text(encoding="utf-8"))
            value["decision"]["history_start"] = "2021-01-01"
            path.write_text(json.dumps(value), encoding="utf-8")

            with self.assertRaises(PromotionStoreConflict):
                store.get(decision.decision_id)

    def test_cli_evaluates_shows_and_lists_without_echoing_bad_arguments(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            daily_root = root / "daily"
            promotion_root = root / "promotion"
            run = LocalDailyPipelineRunStore(daily_root).publish(daily_run())
            stdout = io.StringIO()
            with patch.dict(
                os.environ,
                {
                    DAILY_PIPELINE_ROOT_ENV: str(daily_root),
                    PROMOTION_ROOT_ENV: str(promotion_root),
                },
                clear=True,
            ), patch("sys.stdout", stdout):
                exit_code = promotion_main(
                    [
                        "evaluate-daily-run",
                        "--run-id",
                        run.run_id,
                        "--history-start",
                        HISTORY_START.isoformat(),
                    ]
                )
                response = json.loads(stdout.getvalue())
                stdout.seek(0)
                stdout.truncate(0)
                show_code = promotion_main(
                    ["show", "--decision-id", response["decision_id"]]
                )
                shown = json.loads(stdout.getvalue())
                stdout.seek(0)
                stdout.truncate(0)
                list_code = promotion_main(["list"])
                listed = json.loads(stdout.getvalue())

            self.assertEqual((exit_code, show_code, list_code), (0, 0, 0))
            self.assertEqual(response["achieved_stage"], "COLLECTION_ONLY")
            self.assertFalse(response["research_eligible"])
            self.assertEqual(shown["decision_id"], response["decision_id"])
            self.assertEqual(len(listed["decisions"]), 1)

            stderr = io.StringIO()
            with patch("sys.stderr", stderr):
                bad_code = promotion_main(
                    ["show", "--decision-id", "access_token=distinct-secret"]
                )
            self.assertEqual(bad_code, 2)
            self.assertNotIn("distinct-secret", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
