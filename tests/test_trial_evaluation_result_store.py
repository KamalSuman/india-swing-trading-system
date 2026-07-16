from __future__ import annotations

import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from india_swing.evaluation import (
    LocalTrialEvaluationResultStore,
    LocalTrialEvaluationComparisonStore,
    LocalTrialLifecycleStore,
    LocalTrialRegistry,
    TrialEvaluationEngine,
    TrialEvaluationComparisonResult,
    TrialEvaluationError,
    TrialEvaluationResultNotFound,
    TrialLifecycleConflict,
    TrialLifecycleEventType,
    decode_trial_evaluation_result,
    encode_trial_evaluation_result,
)
from india_swing.execution import zerodha_nse_delivery_schedule_2026
from tests.test_trial_evaluation_engine import (
    dataset,
    intent,
    policy,
    registration,
    split_plan,
)


class TrialEvaluationResultStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "trials"
        self.registry = LocalTrialRegistry(self.root)
        self.registration = registration()
        self.registry.register(self.registration)
        self.store = LocalTrialEvaluationResultStore(self.root, self.registry)
        self.comparison_store = LocalTrialEvaluationComparisonStore(
            self.root, self.registry, self.store
        )
        self.result = TrialEvaluationEngine().evaluate(
            registration=self.registration,
            split_plan=split_plan(),
            dataset=dataset(),
            intents=(intent(),),
            execution_policy=policy(),
            cost_schedule=zerodha_nse_delivery_schedule_2026(),
            initial_capital=Decimal("100000"),
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_result_codec_round_trips_complete_evidence(self) -> None:
        encoded = encode_trial_evaluation_result(self.result)
        decoded = decode_trial_evaluation_result(encoded)

        self.assertEqual(decoded, self.result)
        self.assertEqual(decoded.trades, self.result.trades)
        self.assertEqual(decoded.charges, self.result.charges)
        self.assertEqual(decoded.equity_curve, self.result.equity_curve)

    def test_publish_is_create_once_and_idempotent(self) -> None:
        first = self.store.publish(self.result)
        path = (
            self.root
            / "results"
            / self.registration.trial_id
            / f"{self.result.result_id}.json"
        )
        original = path.read_bytes()
        second = self.store.publish(self.result)

        self.assertEqual(first, second)
        self.assertEqual(path.read_bytes(), original)
        self.assertEqual(
            self.store.results_for_trial(self.registration.trial_id),
            (self.result,),
        )

    def test_tampered_stored_metric_is_rejected(self) -> None:
        self.store.publish(self.result)
        path = (
            self.root
            / "results"
            / self.registration.trial_id
            / f"{self.result.result_id}.json"
        )
        payload = json.loads(path.read_text(encoding="utf-8"))
        for metric in payload["result"]["metrics"]:
            if metric[0] == "net_profit":
                metric[1] = "999999"
        path.write_text(json.dumps(payload), encoding="utf-8")

        with self.assertRaisesRegex(TrialEvaluationError, "metrics were not generated"):
            self.store.get(self.registration.trial_id, self.result.result_id)

    def test_missing_result_is_explicit(self) -> None:
        with self.assertRaises(TrialEvaluationResultNotFound):
            self.store.get(self.registration.trial_id, "f" * 64)

    def test_lifecycle_completion_requires_result_to_be_persisted_first(self) -> None:
        lifecycle = LocalTrialLifecycleStore(
            self.root, self.registry, self.comparison_store
        )
        comparison = TrialEvaluationComparisonResult(
            trial_id=self.registration.trial_id,
            strategy_id=self.registration.model_bundle_id,
            benchmark_id=self.registration.benchmark_id,
            primary_metric=self.registration.primary_metric,
            base_slippage_bps=self.registration.base_slippage_bps,
            stressed_slippage_bps=None,
            strategy_base=self.result,
            benchmark_base=self.result,
            strategy_stressed=None,
            benchmark_stressed=None,
        )
        lifecycle.append(
            trial_id=self.registration.trial_id,
            event_type=TrialLifecycleEventType.TRIAL_STARTED,
            occurred_at=self.registration.registered_at,
            actor_id="evaluation-runner",
            reason="Start synthetic evaluation.",
        )

        with self.assertRaisesRegex(TrialLifecycleConflict, "persisted"):
            lifecycle.append(
                trial_id=self.registration.trial_id,
                event_type=TrialLifecycleEventType.TRIAL_COMPLETED,
                occurred_at=self.registration.registered_at,
                actor_id="evaluation-runner",
                reason="Attempt completion before publishing evidence.",
                evaluation_comparison=comparison,
            )

    def test_comparison_store_persists_references_to_full_results(self) -> None:
        comparison = TrialEvaluationComparisonResult(
            trial_id=self.registration.trial_id,
            strategy_id=self.registration.model_bundle_id,
            benchmark_id=self.registration.benchmark_id,
            primary_metric=self.registration.primary_metric,
            base_slippage_bps=self.registration.base_slippage_bps,
            stressed_slippage_bps=None,
            strategy_base=self.result,
            benchmark_base=self.result,
            strategy_stressed=None,
            benchmark_stressed=None,
        )

        published = self.comparison_store.publish(comparison)

        self.assertEqual(published, comparison)
        self.assertEqual(
            self.comparison_store.get(
                comparison.trial_id, comparison.comparison_id
            ),
            comparison,
        )

    def test_tampered_comparison_pass_flag_is_rejected(self) -> None:
        comparison = TrialEvaluationComparisonResult(
            trial_id=self.registration.trial_id,
            strategy_id=self.registration.model_bundle_id,
            benchmark_id=self.registration.benchmark_id,
            primary_metric=self.registration.primary_metric,
            base_slippage_bps=self.registration.base_slippage_bps,
            stressed_slippage_bps=None,
            strategy_base=self.result,
            benchmark_base=self.result,
            strategy_stressed=None,
            benchmark_stressed=None,
        )
        self.comparison_store.publish(comparison)
        path = (
            self.root
            / "comparisons"
            / comparison.trial_id
            / f"{comparison.comparison_id}.json"
        )
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["comparison"]["passed"] = not comparison.passed
        path.write_text(json.dumps(payload), encoding="utf-8")

        with self.assertRaisesRegex(TrialEvaluationError, "does not match"):
            self.comparison_store.get(comparison.trial_id, comparison.comparison_id)


if __name__ == "__main__":
    unittest.main()
