from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from india_swing.daily_pipeline import DailyPipelineRun, LocalDailyPipelineRunStore
from india_swing.daily_pipeline.config import DAILY_PIPELINE_ROOT_ENV
from india_swing.market_data.config import MARKET_DATA_ROOT_ENV
from india_swing.promotion import (
    ALERT_REQUIREMENTS,
    BACKTEST_REQUIREMENTS,
    PROMOTION_ROOT_ENV,
    HistoricalCorpusPromotionError,
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
    promotion_evidence_from_historical_corpus,
)
from india_swing.promotion.cli import PromotionArgumentError
from india_swing.promotion.cli import main as promotion_main
from india_swing.promotion.cli import parser as promotion_parser
from india_swing.reference import ReferenceReadiness
from tests.test_historical_evaluation_corpus import (
    BUILT_AT as CORPUS_BUILT_AT,
    SESSION_ONE as CORPUS_SESSION_ONE,
    SESSION_TWO as CORPUS_SESSION_TWO,
    _fabricated_bar as corpus_bar,
    _fabricated_index as corpus_index,
    _fabricated_partition as corpus_partition,
    build_service as build_corpus_service,
    build_two_symbol_fixture,
)


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


# --- historical-corpus-to-promotion bridge -----------------------------------


def _historical_corpus_fixture(root: Path):
    """A real, fully admitted, two-symbol/two-session, complete corpus."""

    fixture = build_two_symbol_fixture(root)
    service = build_corpus_service(fixture)
    index = service.build(
        admission_report_id=fixture["admission_report"].report_id,
        reconciliation_index_id=fixture["reconciliation_index"].index_id,
        built_at=CORPUS_BUILT_AT,
    )
    _stored_index, partitions = fixture["corpus_store"].get(index.corpus_id)
    return fixture, index, partitions


class HistoricalCorpusPromotionAdapterHappyPathTests(unittest.TestCase):
    def test_complete_corpus_yields_exactly_raw_prices_and_reconciliation(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            _fixture, index, partitions = _historical_corpus_fixture(Path(temp_dir))
        evidence = promotion_evidence_from_historical_corpus(index, partitions)

        self.assertEqual(len(evidence), 2)
        self.assertEqual(
            tuple(value.capability for value in evidence),
            (PromotionCapability.RAW_PRICES, PromotionCapability.RECONCILIATION),
        )
        self.assertTrue(index.safe_requests_complete)
        self.assertTrue(index.coverage_complete)
        self.assertEqual(index.blocked_entry_ids, ())
        for value in evidence:
            value.verify_content_identity()
            self.assertIs(value.readiness, ReferenceReadiness.COLLECTION_ONLY)
            self.assertFalse(value.actionable)
            self.assertTrue(value.complete)
            self.assertEqual(value.coverage_start, index.partition_sessions[0])
            self.assertEqual(value.coverage_end, index.partition_sessions[-1])
            self.assertEqual(value.cutoff, index.built_at)
            self.assertIn(index.corpus_id, value.source_snapshot_ids)
            self.assertIn(
                "PROVENANCE_NOT_POINT_IN_TIME_VERIFIED", value.reason_codes
            )
        raw_prices, reconciliation = evidence
        self.assertIn(index.admission_report_id, raw_prices.source_snapshot_ids)
        self.assertIn(
            index.reconciliation_index_id, reconciliation.source_snapshot_ids
        )


class HistoricalCorpusPromotionAdapterPartialStateTests(unittest.TestCase):
    def test_coverage_incomplete_alone_sets_complete_false_and_its_reason(
        self,
    ) -> None:
        partition = corpus_partition((corpus_bar(),))
        index = corpus_index(
            (partition,),
            safe_requests_complete=True,
            coverage_complete=False,
            blocked_entry_ids=(),
        )
        raw_prices, reconciliation = promotion_evidence_from_historical_corpus(
            index, (partition,)
        )
        for value in (raw_prices, reconciliation):
            self.assertFalse(value.complete)
            self.assertEqual(
                set(value.reason_codes),
                {"PROVENANCE_NOT_POINT_IN_TIME_VERIFIED", "COVERAGE_INCOMPLETE"},
            )

    def test_blocked_entries_alone_sets_complete_false_and_its_reason(self) -> None:
        partition = corpus_partition((corpus_bar(),))
        index = corpus_index(
            (partition,),
            all_entry_ids=("b" * 64, "c" * 64),
            admitted_entry_ids=("b" * 64,),
            blocked_entry_ids=("c" * 64,),
            disposition_counts=(("ADMITTED", 1), ("MISSING_COMPLETION", 1)),
            safe_requests_complete=True,
            coverage_complete=True,
        )
        raw_prices, reconciliation = promotion_evidence_from_historical_corpus(
            index, (partition,)
        )
        for value in (raw_prices, reconciliation):
            self.assertFalse(value.complete)
            self.assertEqual(
                set(value.reason_codes),
                {
                    "PROVENANCE_NOT_POINT_IN_TIME_VERIFIED",
                    "BLOCKED_ENTRIES_PRESENT",
                },
            )

    def test_no_reason_is_suppressed_when_every_blocker_applies_at_once(
        self,
    ) -> None:
        partition = corpus_partition((corpus_bar(),))
        index = corpus_index(
            (partition,),
            all_entry_ids=("b" * 64, "c" * 64),
            admitted_entry_ids=("b" * 64,),
            blocked_entry_ids=("c" * 64,),
            disposition_counts=(("ADMITTED", 1), ("MISSING_COMPLETION", 1)),
            safe_requests_complete=False,
            coverage_complete=False,
        )
        raw_prices, reconciliation = promotion_evidence_from_historical_corpus(
            index, (partition,)
        )
        expected = {
            "PROVENANCE_NOT_POINT_IN_TIME_VERIFIED",
            "SAFE_REQUESTS_INCOMPLETE",
            "COVERAGE_INCOMPLETE",
            "BLOCKED_ENTRIES_PRESENT",
        }
        for value in (raw_prices, reconciliation):
            self.assertFalse(value.complete)
            self.assertEqual(set(value.reason_codes), expected)


class HistoricalCorpusPromotionAdapterIntegrityTests(unittest.TestCase):
    def test_wrong_type_index_is_rejected(self) -> None:
        partition = corpus_partition((corpus_bar(),))
        with self.assertRaises(HistoricalCorpusPromotionError):
            promotion_evidence_from_historical_corpus(object(), (partition,))  # type: ignore[arg-type]

    def test_wrong_type_partitions_is_rejected(self) -> None:
        partition = corpus_partition((corpus_bar(),))
        index = corpus_index((partition,))
        with self.assertRaises(HistoricalCorpusPromotionError):
            promotion_evidence_from_historical_corpus(index, [partition])  # type: ignore[arg-type]

    def test_empty_partition_set_is_rejected(self) -> None:
        partition = corpus_partition((corpus_bar(),))
        index = corpus_index((partition,))
        with self.assertRaises(HistoricalCorpusPromotionError):
            promotion_evidence_from_historical_corpus(index, ())

    def test_misaligned_partitions_are_rejected(self) -> None:
        first = corpus_bar()
        second = corpus_bar(
            session=CORPUS_SESSION_TWO,
            request_id="c" * 64,
            binding_id="d" * 64,
            provider_snapshot_id="e" * 64,
            reconciliation_snapshot_id="f" * 64,
        )
        partition_one = corpus_partition((first,), session=CORPUS_SESSION_ONE)
        partition_two = corpus_partition((second,), session=CORPUS_SESSION_TWO)
        index = corpus_index(
            (partition_one, partition_two),
            all_entry_ids=("b" * 64, "c" * 64),
            admitted_entry_ids=("b" * 64, "c" * 64),
            disposition_counts=(("ADMITTED", 2),),
        )
        # Reordered relative to index.partition_ids.
        with self.assertRaises(HistoricalCorpusPromotionError):
            promotion_evidence_from_historical_corpus(
                index, (partition_two, partition_one)
            )

    def test_duplicate_partitions_are_rejected(self) -> None:
        partition = corpus_partition((corpus_bar(),))
        index = corpus_index((partition,))
        with self.assertRaises(HistoricalCorpusPromotionError):
            promotion_evidence_from_historical_corpus(index, (partition, partition))

    def test_tampered_partition_content_is_rejected(self) -> None:
        partition = corpus_partition((corpus_bar(),))
        index = corpus_index((partition,))
        object.__setattr__(partition, "market_session", partition.market_session)
        object.__setattr__(partition, "collection_only", False)
        with self.assertRaises(HistoricalCorpusPromotionError):
            promotion_evidence_from_historical_corpus(index, (partition,))

    def test_tampered_index_identity_is_rejected(self) -> None:
        partition = corpus_partition((corpus_bar(),))
        index = corpus_index((partition,))
        object.__setattr__(index, "coverage_complete", not index.coverage_complete)
        with self.assertRaises(HistoricalCorpusPromotionError):
            promotion_evidence_from_historical_corpus(index, (partition,))

    def test_empty_admitted_set_returns_no_evidence(self) -> None:
        partition = corpus_partition((corpus_bar(),))
        index = corpus_index(
            (partition,),
            all_entry_ids=("b" * 64,),
            admitted_entry_ids=(),
            blocked_entry_ids=("b" * 64,),
            disposition_counts=(("MISSING_COMPLETION", 1),),
            safe_requests_complete=False,
            coverage_complete=False,
        )
        with self.assertRaises(HistoricalCorpusPromotionError):
            promotion_evidence_from_historical_corpus(index, (partition,))


class HistoricalCorpusPromotionDecisionIntegrationTests(unittest.TestCase):
    def test_achieved_stage_remains_collection_only_with_exact_blockers(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            _fixture, index, partitions = _historical_corpus_fixture(Path(temp_dir))
        evidence = promotion_evidence_from_historical_corpus(index, partitions)
        decision = evaluate_promotion(
            market_session=index.partition_sessions[-1],
            history_start=index.partition_sessions[0],
            decision_cutoff=index.built_at,
            evidence=evidence,
        )

        self.assertEqual(decision.achieved_stage, PromotionStage.COLLECTION_ONLY)
        self.assertIn("RAW_PRICES_COLLECTION_ONLY", decision.research_blockers)
        self.assertIn("RAW_PRICES_NOT_ACTIONABLE", decision.research_blockers)
        self.assertIn("RECONCILIATION_COLLECTION_ONLY", decision.backtest_blockers)
        self.assertIn("RECONCILIATION_NOT_ACTIONABLE", decision.backtest_blockers)
        self.assertIn("MISSING_CALENDAR", decision.research_blockers)
        self.assertIn("MISSING_STABLE_IDENTITY", decision.research_blockers)
        self.assertIn("MISSING_UNIVERSE", decision.research_blockers)
        self.assertIn("MISSING_CORPORATE_ACTIONS", decision.backtest_blockers)
        self.assertIn("MISSING_LIQUIDITY", decision.backtest_blockers)
        self.assertIn("MISSING_SURVEILLANCE", decision.backtest_blockers)
        self.assertIn("MISSING_TICK_SIZES", decision.backtest_blockers)
        self.assertIn("MISSING_EXPLICIT_NONTRADING", decision.backtest_blockers)
        self.assertIn("MISSING_MODEL_VALIDATION", decision.alert_blockers)
        self.assertIn("MISSING_RISK_POLICY", decision.alert_blockers)
        self.assertIn("MISSING_SHADOW_OPERATIONS", decision.alert_blockers)
        decision.verify_content_identity()


class HistoricalCorpusPromotionCliTests(unittest.TestCase):
    def test_no_listing_or_latest_flag_exists(self) -> None:
        store = LocalPromotionDecisionStore(Path("unused"))
        for banned in ("latest", "list_corpora", "find", "select"):
            self.assertFalse(hasattr(store, banned))
        # This CLI's SanitizedArgumentParser converts an unrecognized flag
        # (such as a would-be --latest) into the same sanitized argument
        # error as any other bad input, rather than argparse's SystemExit.
        with self.assertRaises(PromotionArgumentError):
            promotion_parser().parse_args(
                [
                    "evaluate-historical-corpus",
                    "--corpus-id",
                    "a" * 64,
                    "--history-start",
                    "2020-01-01",
                    "--latest",
                ]
            )

    def test_round_trip_builds_evaluates_and_shows_the_same_decision(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            _fixture, index, _partitions = _historical_corpus_fixture(root)
            environment = {
                MARKET_DATA_ROOT_ENV: str(root / "market"),
                PROMOTION_ROOT_ENV: str(root / "promotion"),
            }
            stdout = io.StringIO()
            with patch.dict(os.environ, environment, clear=True), patch(
                "sys.stdout", stdout
            ):
                exit_code = promotion_main(
                    [
                        "evaluate-historical-corpus",
                        "--corpus-id",
                        index.corpus_id,
                        "--history-start",
                        index.partition_sessions[0].isoformat(),
                    ]
                )
                response = json.loads(stdout.getvalue())
                stdout.seek(0)
                stdout.truncate(0)
                show_code = promotion_main(
                    ["show", "--decision-id", response["decision_id"]]
                )
                shown = json.loads(stdout.getvalue())

            self.assertEqual((exit_code, show_code), (0, 0))
            self.assertEqual(response["achieved_stage"], "COLLECTION_ONLY")
            self.assertFalse(response["research_eligible"])
            self.assertIn("RAW_PRICES", response["evidence_capabilities"])
            self.assertIn("RECONCILIATION", response["evidence_capabilities"])
            self.assertEqual(shown["decision_id"], response["decision_id"])
            self.assertEqual(
                shown["research_blockers"], response["research_blockers"]
            )
            self.assertEqual(
                shown["backtest_blockers"], response["backtest_blockers"]
            )
            self.assertEqual(shown["alert_blockers"], response["alert_blockers"])

    def test_history_start_after_first_corpus_session_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            _fixture, index, _partitions = _historical_corpus_fixture(root)
            environment = {
                MARKET_DATA_ROOT_ENV: str(root / "market"),
                PROMOTION_ROOT_ENV: str(root / "promotion"),
            }
            after_first_session = (
                index.partition_sessions[0] + timedelta(days=1)
            ).isoformat()
            with patch.dict(os.environ, environment, clear=True):
                exit_code = promotion_main(
                    [
                        "evaluate-historical-corpus",
                        "--corpus-id",
                        index.corpus_id,
                        "--history-start",
                        after_first_session,
                    ]
                )
            self.assertEqual(exit_code, 2)

    def test_invalid_and_missing_corpus_ids_fail_closed_with_exit_code_two(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            environment = {
                MARKET_DATA_ROOT_ENV: str(root / "market"),
                PROMOTION_ROOT_ENV: str(root / "promotion"),
            }
            with patch.dict(os.environ, environment, clear=True):
                missing_code = promotion_main(
                    [
                        "evaluate-historical-corpus",
                        "--corpus-id",
                        "f" * 64,
                        "--history-start",
                        "2020-01-01",
                    ]
                )
                stderr = io.StringIO()
                with patch("sys.stderr", stderr):
                    invalid_code = promotion_main(
                        [
                            "evaluate-historical-corpus",
                            "--corpus-id",
                            "access_token=distinct-secret",
                            "--history-start",
                            "2020-01-01",
                        ]
                    )
            self.assertEqual(missing_code, 2)
            self.assertEqual(invalid_code, 2)
            self.assertNotIn("distinct-secret", stderr.getvalue())

    def test_hostile_corpus_store_exception_text_does_not_surface(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            environment = {
                MARKET_DATA_ROOT_ENV: str(root / "market"),
                PROMOTION_ROOT_ENV: str(root / "promotion"),
            }
            secret = "leaked-secret-token-9f8e7d /var/secret/corpus.json"
            hostile_store = MagicMock()
            hostile_store.get.side_effect = RuntimeError(secret)
            stderr = io.StringIO()
            with patch.dict(os.environ, environment, clear=True), patch(
                "india_swing.promotion.cli.LocalHistoricalEvaluationCorpusStore",
                return_value=hostile_store,
            ), patch("sys.stderr", stderr):
                exit_code = promotion_main(
                    [
                        "evaluate-historical-corpus",
                        "--corpus-id",
                        "a" * 64,
                        "--history-start",
                        "2020-01-01",
                    ]
                )
            self.assertEqual(exit_code, 2)
            payload = json.loads(stderr.getvalue())
            self.assertEqual(set(payload), {"status", "error_type"})
            self.assertEqual(payload["error_type"], "HistoricalCorpusPromotionError")
            self.assertNotIn("leaked-secret-token-9f8e7d", stderr.getvalue())
            self.assertNotIn("/var/secret", stderr.getvalue())
            self.assertNotIn("RuntimeError", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
