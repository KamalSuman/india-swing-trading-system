from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from india_swing.evaluation import (
    LocalDeterministicComparisonRunStore,
    LocalGeneratedIntentBatchStore,
    LocalRegimeEnsembleEvaluationReportStore,
    LocalTrialEvaluationComparisonStore,
    LocalTrialEvaluationResultStore,
    LocalTrialRegistry,
    REGIME_ENSEMBLE_REPORT_CAVEATS,
    RegimeEnsembleEvaluationReport,
    RegimeEnsembleReportError,
    RegimeEnsembleReportStoreConflict,
    RegimeEnsembleReportStoreNotFound,
    build_regime_ensemble_evaluation_report,
    decode_regime_ensemble_evaluation_report,
    encode_regime_ensemble_evaluation_report,
)

from tests.test_regime_ensemble_run import STRONG, WEAK, _evaluate


class RegimeEnsembleEvaluationReportTests(unittest.TestCase):
    def test_report_is_deterministic_and_content_addressed(self) -> None:
        run, _ = _evaluate(profiles=(STRONG, WEAK), num_folds=1)
        first = build_regime_ensemble_evaluation_report(run)
        second = build_regime_ensemble_evaluation_report(run)
        self.assertEqual(first.report_id, second.report_id)
        self.assertEqual(first, second)

    def test_report_contains_exact_base_stressed_fold_and_count_evidence(self) -> None:
        run, intent_run = _evaluate(profiles=(STRONG, WEAK), num_folds=1)
        comparison = run.comparison_run.comparison
        report = build_regime_ensemble_evaluation_report(run)

        self.assertEqual(report.fold_count, len(intent_run.fold_results))
        self.assertEqual(
            report.proposal_count,
            sum(len(result.proposal_batch.proposals) for result in intent_run.fold_results),
        )
        self.assertEqual(report.strategy_decision_count, len(intent_run.generated_batch.decisions))
        self.assertEqual(report.selected_intent_count, len(intent_run.generated_batch.intents))
        self.assertEqual(report.strategy_base_metrics, comparison.strategy_base.metrics)
        self.assertEqual(report.benchmark_base_metrics, comparison.benchmark_base.metrics)
        self.assertEqual(report.comparison_metrics, comparison.comparison_metrics)
        self.assertIsNotNone(report.strategy_stressed_metrics)
        self.assertIsNotNone(report.benchmark_stressed_metrics)
        self.assertEqual(report.strategy_stressed_metrics, comparison.strategy_stressed.metrics)
        self.assertEqual(
            report.fold_summary_ids,
            tuple(item.summary_id for item in run.comparison_run.fold_summaries),
        )
        self.assertEqual(report.passed, comparison.passed)
        self.assertEqual(
            report.outperformed,
            all(item.outperformed for item in run.comparison_run.fold_summaries),
        )
        self.assertEqual(report.caveats, REGIME_ENSEMBLE_REPORT_CAVEATS)
        self.assertTrue(len(report.caveats) > 0)
        self.assertFalse(report.execution_eligible)
        self.assertFalse(report.promotion_eligible)

    def test_report_detects_mutation(self) -> None:
        run, _ = _evaluate(profiles=(STRONG,), num_folds=1)
        report = build_regime_ensemble_evaluation_report(run)
        report.verify_content_identity()
        untouched_id = report.report_id

        object.__setattr__(report, "passed", not report.passed)

        with self.assertRaises(RegimeEnsembleReportError):
            report.verify_content_identity()
        self.assertEqual(report.report_id, untouched_id)

    def test_build_rejects_wrong_type(self) -> None:
        with self.assertRaisesRegex(RegimeEnsembleReportError, "must be exact"):
            build_regime_ensemble_evaluation_report("not-a-run")


def _real_report(run) -> RegimeEnsembleEvaluationReport:
    return build_regime_ensemble_evaluation_report(run)


class RegimeEnsembleEvaluationReportCodecTests(unittest.TestCase):
    def setUp(self) -> None:
        run, _ = _evaluate(profiles=(STRONG,), num_folds=1)
        self.report = _real_report(run)

    def test_round_trip_is_exact(self) -> None:
        payload = encode_regime_ensemble_evaluation_report(self.report)
        decoded = decode_regime_ensemble_evaluation_report(payload)
        self.assertEqual(decoded, self.report)

    def _raw(self) -> dict:
        payload = encode_regime_ensemble_evaluation_report(self.report)
        return json.loads(payload.decode("utf-8"))

    def _dump(self, raw: dict) -> bytes:
        return (json.dumps(raw, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")

    def test_rejects_unknown_key(self) -> None:
        raw = self._raw()
        raw["report"]["unexpected_field"] = "x"
        with self.assertRaises(RegimeEnsembleReportStoreConflict):
            decode_regime_ensemble_evaluation_report(self._dump(raw))

    def test_rejects_missing_key(self) -> None:
        raw = self._raw()
        del raw["report"]["passed"]
        with self.assertRaises(RegimeEnsembleReportStoreConflict):
            decode_regime_ensemble_evaluation_report(self._dump(raw))

    def test_rejects_duplicate_key(self) -> None:
        payload = encode_regime_ensemble_evaluation_report(self.report)
        text = payload.decode("utf-8")
        duplicated = text.replace('"passed":', '"passed":false,"passed":', 1)
        with self.assertRaises(RegimeEnsembleReportStoreConflict):
            decode_regime_ensemble_evaluation_report(duplicated.encode("utf-8"))

    def test_rejects_float_values(self) -> None:
        text = encode_regime_ensemble_evaluation_report(self.report).decode("utf-8")
        text = text.replace('"fold_count":1', '"fold_count":1.5', 1)
        with self.assertRaises(RegimeEnsembleReportStoreConflict):
            decode_regime_ensemble_evaluation_report(text.encode("utf-8"))

    def test_rejects_invalid_readiness_enum(self) -> None:
        raw = self._raw()
        raw["report"]["dataset_readiness"] = "NOT_A_READINESS"
        with self.assertRaises(RegimeEnsembleReportStoreConflict):
            decode_regime_ensemble_evaluation_report(self._dump(raw))

    def test_rejects_invalid_metrics_shape(self) -> None:
        raw = self._raw()
        raw["report"]["strategy_base_metrics"] = [["net_return", "0.1"]]
        with self.assertRaises(RegimeEnsembleReportStoreConflict):
            decode_regime_ensemble_evaluation_report(self._dump(raw))

    def test_rejects_invalid_counts(self) -> None:
        raw = self._raw()
        raw["report"]["fold_count"] = "one"
        with self.assertRaises(RegimeEnsembleReportStoreConflict):
            decode_regime_ensemble_evaluation_report(self._dump(raw))

    def test_rejects_invalid_ids(self) -> None:
        raw = self._raw()
        raw["report"]["trial_id"] = "not-a-sha256"
        with self.assertRaises(RegimeEnsembleReportStoreConflict):
            decode_regime_ensemble_evaluation_report(self._dump(raw))

    def test_rejects_invalid_schema(self) -> None:
        raw = self._raw()
        raw["store_schema_version"] = "unsupported/v0"
        with self.assertRaises(RegimeEnsembleReportStoreConflict):
            decode_regime_ensemble_evaluation_report(self._dump(raw))

    def test_rejects_malformed_utf8(self) -> None:
        with self.assertRaises(RegimeEnsembleReportStoreConflict):
            decode_regime_ensemble_evaluation_report(b"\xff\xfe\x00not utf-8")

    def test_rejects_recomputed_content_mismatch(self) -> None:
        raw = self._raw()
        raw["report"]["passed"] = not raw["report"]["passed"]
        with self.assertRaises(RegimeEnsembleReportStoreConflict):
            decode_regime_ensemble_evaluation_report(self._dump(raw))


class LocalRegimeEnsembleEvaluationReportStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.registry = LocalTrialRegistry(self.root / "trials")
        self.result_store = LocalTrialEvaluationResultStore(self.root / "evidence", self.registry)
        self.comparison_store = LocalTrialEvaluationComparisonStore(
            self.root / "evidence", self.registry, self.result_store
        )
        self.batch_store = LocalGeneratedIntentBatchStore(self.root / "evidence", self.registry)
        self.run_store = LocalDeterministicComparisonRunStore(
            self.batch_store, self.comparison_store
        )
        self.report_store = LocalRegimeEnsembleEvaluationReportStore(self.run_store)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _registered_run(self, *, profiles=(STRONG,)):
        run, _ = _evaluate(profiles=profiles, num_folds=1)
        self.registry.register(run.registration)
        return run

    def test_publish_persists_deterministic_run_before_the_report(self) -> None:
        run = self._registered_run()
        report = self.report_store.publish(run)

        persisted_run = self.run_store.get(run.registration.trial_id)
        self.assertEqual(persisted_run, run.comparison_run)
        self.assertEqual(report, build_regime_ensemble_evaluation_report(run))

    def test_publish_is_idempotent_for_identical_report(self) -> None:
        run = self._registered_run()
        first = self.report_store.publish(run)
        second = self.report_store.publish(run)
        self.assertEqual(first, second)

    def test_publish_conflicts_on_a_different_report_for_the_same_trial(self) -> None:
        run = self._registered_run()
        self.report_store.publish(run)
        real_report = build_regime_ensemble_evaluation_report(run)

        forged_report = RegimeEnsembleEvaluationReport(
            trial_id=real_report.trial_id,
            evaluation_run_id="9" * 64,
            intent_run_id=real_report.intent_run_id,
            deterministic_run_id=real_report.deterministic_run_id,
            comparison_id=real_report.comparison_id,
            config_id=real_report.config_id,
            benchmark_id=real_report.benchmark_id,
            split_plan_id=real_report.split_plan_id,
            dataset_id=real_report.dataset_id,
            dataset_readiness=real_report.dataset_readiness,
            fold_count=real_report.fold_count,
            proposal_count=real_report.proposal_count,
            strategy_decision_count=real_report.strategy_decision_count,
            selected_intent_count=real_report.selected_intent_count,
            strategy_trade_count=real_report.strategy_trade_count,
            benchmark_trade_count=real_report.benchmark_trade_count,
            primary_metric=real_report.primary_metric,
            strategy_base_metrics=real_report.strategy_base_metrics,
            benchmark_base_metrics=real_report.benchmark_base_metrics,
            comparison_metrics=real_report.comparison_metrics,
            strategy_stressed_metrics=real_report.strategy_stressed_metrics,
            benchmark_stressed_metrics=real_report.benchmark_stressed_metrics,
            fold_summary_ids=real_report.fold_summary_ids,
            passed=real_report.passed,
            outperformed=real_report.outperformed,
        )
        path = self.report_store._path(run.registration.trial_id)
        path.write_bytes(encode_regime_ensemble_evaluation_report(forged_report))

        with self.assertRaises(RegimeEnsembleReportStoreConflict):
            self.report_store.publish(run)

    def test_get_rejects_directory_at_report_path(self) -> None:
        run = self._registered_run()
        self.report_store.publish(run)
        path = self.report_store._path(run.registration.trial_id)
        path.unlink()
        path.mkdir()

        with self.assertRaises(RegimeEnsembleReportStoreConflict):
            self.report_store.get(run.registration.trial_id)

    def test_get_rejects_symlinked_report_path(self) -> None:
        run = self._registered_run()
        self.report_store.publish(run)
        path = self.report_store._path(run.registration.trial_id)
        real_target = path.with_name(path.name + ".real")
        real_target.write_bytes(path.read_bytes())
        path.unlink()
        try:
            path.symlink_to(real_target)
        except OSError:
            self.skipTest("symlink creation is not permitted in this environment")

        with self.assertRaises(RegimeEnsembleReportStoreConflict):
            self.report_store.get(run.registration.trial_id)

    def test_get_returns_a_stable_report_across_reads(self) -> None:
        run = self._registered_run()
        self.report_store.publish(run)
        first = self.report_store.get(run.registration.trial_id)
        second = self.report_store.get(run.registration.trial_id)
        self.assertEqual(first, second)

    def test_get_requires_a_persisted_deterministic_run(self) -> None:
        run = self._registered_run()
        with self.assertRaises(Exception):
            self.report_store.get(run.registration.trial_id)

    def test_get_raises_not_found_when_only_the_deterministic_run_is_persisted(self) -> None:
        run = self._registered_run()
        self.run_store.publish(run.comparison_run)
        with self.assertRaises(RegimeEnsembleReportStoreNotFound):
            self.report_store.get(run.registration.trial_id)

    def test_get_detects_tampered_report_bytes(self) -> None:
        run = self._registered_run()
        self.report_store.publish(run)
        path = self.report_store._path(run.registration.trial_id)
        raw = json.loads(path.read_bytes().decode("utf-8"))
        raw["report"]["passed"] = not raw["report"]["passed"]
        path.write_bytes(
            (json.dumps(raw, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
        )

        with self.assertRaises(RegimeEnsembleReportStoreConflict):
            self.report_store.get(run.registration.trial_id)

    def test_get_rejects_oversized_report_file(self) -> None:
        run = self._registered_run()
        self.report_store.publish(run)
        path = self.report_store._path(run.registration.trial_id)
        path.write_bytes(path.read_bytes()[:-1] + b" " * (600 * 1024) + b"\n")

        with self.assertRaises(RegimeEnsembleReportStoreConflict):
            self.report_store.get(run.registration.trial_id)

    def test_require_persisted_matches_built_report(self) -> None:
        run = self._registered_run()
        self.report_store.publish(run)
        stored = self.report_store.require_persisted(run)
        self.assertEqual(stored, build_regime_ensemble_evaluation_report(run))

    def test_store_has_no_list_or_latest_selection_api(self) -> None:
        self.assertFalse(hasattr(self.report_store, "list"))
        self.assertFalse(hasattr(self.report_store, "latest"))


if __name__ == "__main__":
    unittest.main()
