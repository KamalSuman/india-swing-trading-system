from __future__ import annotations

import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from india_swing.evaluation import (
    LocalTrialEvaluationResultStore,
    LocalTrialLifecycleStore,
    LocalTrialRegistry,
    TrialEvaluationEngine,
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
        lifecycle = LocalTrialLifecycleStore(self.root, self.registry, self.store)
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
                evaluation_result=self.result,
            )


if __name__ == "__main__":
    unittest.main()
