from __future__ import annotations

import json
import tempfile
import unittest
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from india_swing.evaluation import (
    LocalTrialLifecycleStore,
    LocalTrialRegistry,
    TrialLifecycleConflict,
    TrialLifecycleEventType,
    TrialLifecycleIntegrityError,
    TrialNotRegistered,
    TrialStage,
    decode_trial_lifecycle_event,
    encode_trial_lifecycle_event,
)
from tests.test_trial_registry import digest, registration


class TrialLifecycleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "trials"
        self.registry = LocalTrialRegistry(self.root)
        self.store = LocalTrialLifecycleStore(self.root, self.registry)
        self.registration = registration()
        self.registry.register(self.registration)
        self.started_at = self.registration.registered_at + timedelta(seconds=1)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def start(self, value=None):
        value = value or self.registration
        return self.store.append(
            trial_id=value.trial_id,
            event_type=TrialLifecycleEventType.TRIAL_STARTED,
            occurred_at=value.registered_at + timedelta(seconds=1),
            actor_id="evaluation-runner",
            reason="Begin the preregistered evaluation.",
        )

    def unseal(self, *, seconds: int = 2):
        return self.store.append(
            trial_id=self.registration.trial_id,
            event_type=TrialLifecycleEventType.HOLDOUT_UNSEALED,
            occurred_at=self.registration.registered_at + timedelta(seconds=seconds),
            actor_id="evaluation-runner",
            reason="Begin the single preregistered holdout evaluation.",
            holdout_id=self.registration.holdout_id,
        )

    def complete(self, *, passed: bool, seconds: int = 4):
        return self.store.append(
            trial_id=self.registration.trial_id,
            event_type=TrialLifecycleEventType.TRIAL_COMPLETED,
            occurred_at=self.registration.registered_at + timedelta(seconds=seconds),
            actor_id="evaluation-runner",
            reason="Persist the preregistered terminal metrics.",
            metrics=(
                ("max_drawdown", Decimal("-0.25")),
                ("net_cagr", Decimal("0.04")),
                ("turnover", Decimal("2.50")),
            ),
            passed=passed,
        )

    def access_results(self, *, seconds: int = 3):
        return self.store.append(
            trial_id=self.registration.trial_id,
            event_type=TrialLifecycleEventType.HOLDOUT_RESULTS_ACCESSED,
            occurred_at=self.registration.registered_at + timedelta(seconds=seconds),
            actor_id="evaluation-runner",
            reason="Read the preregistered terminal holdout metrics.",
            holdout_id=self.registration.holdout_id,
        )

    def test_evaluation_event_requires_registered_trial(self) -> None:
        with self.assertRaises(TrialNotRegistered):
            self.store.append(
                trial_id=digest("f"),
                event_type=TrialLifecycleEventType.TRIAL_STARTED,
                occurred_at=self.started_at,
                actor_id="evaluation-runner",
                reason="This must not start.",
            )

    def test_holdout_labels_are_inaccessible_before_unseal(self) -> None:
        self.start()
        with self.assertRaisesRegex(TrialLifecycleConflict, "prior unseal"):
            self.store.append(
                trial_id=self.registration.trial_id,
                event_type=TrialLifecycleEventType.HOLDOUT_LABELS_ACCESSED,
                occurred_at=self.registration.registered_at + timedelta(seconds=2),
                actor_id="evaluation-runner",
                reason="Attempt label access.",
                holdout_id=self.registration.holdout_id,
            )

    def test_holdout_access_is_audited_in_sequence(self) -> None:
        started = self.start()
        unsealed = self.unseal()
        accessed = self.store.append(
            trial_id=self.registration.trial_id,
            event_type=TrialLifecycleEventType.HOLDOUT_LABELS_ACCESSED,
            occurred_at=self.registration.registered_at + timedelta(seconds=3),
            actor_id="evaluation-runner",
            reason="Resolve the sealed holdout labels exactly once.",
            holdout_id=self.registration.holdout_id,
        )

        self.assertEqual(
            self.store.list_events(self.registration.trial_id),
            (started, unsealed, accessed),
        )
        self.assertEqual(accessed.previous_event_id, unsealed.event_id)

    def test_negative_completed_trial_remains_queryable(self) -> None:
        self.start()
        self.unseal()
        self.access_results()
        outcome = self.complete(passed=False)

        self.assertFalse(outcome.passed)
        self.assertEqual(self.store.outcomes(self.registration.trial_id), (outcome,))

    def test_failed_and_aborted_trials_remain_visible(self) -> None:
        results = []
        for index, event_type in enumerate(
            (
                TrialLifecycleEventType.TRIAL_FAILED,
                TrialLifecycleEventType.TRIAL_ABORTED,
            ),
            start=1,
        ):
            value = registration(
                strategy_family_id=f"independent-family-{index}",
                configuration_hash=digest("d" if index == 1 else "e"),
            )
            self.registry.register(value)
            self.start(value)
            result = self.store.append(
                trial_id=value.trial_id,
                event_type=event_type,
                occurred_at=value.registered_at + timedelta(seconds=2),
                actor_id="evaluation-runner",
                reason="Preserve the unsuccessful terminal state.",
            )
            results.append((value, result))

        for value, result in results:
            self.assertEqual(self.store.outcomes(value.trial_id), (result,))

    def test_completed_trial_cannot_receive_another_outcome(self) -> None:
        self.start()
        self.unseal()
        self.access_results()
        self.complete(passed=False)

        with self.assertRaisesRegex(TrialLifecycleConflict, "only invalidation"):
            self.store.append(
                trial_id=self.registration.trial_id,
                event_type=TrialLifecycleEventType.TRIAL_COMPLETED,
                occurred_at=self.registration.registered_at + timedelta(seconds=5),
                actor_id="evaluation-runner",
                reason="Attempt to rewrite the terminal outcome.",
                metrics=(
                    ("max_drawdown", Decimal("-0.01")),
                    ("net_cagr", Decimal("0.50")),
                    ("turnover", Decimal("1.00")),
                ),
                passed=True,
            )

    def test_completed_trial_can_be_append_only_invalidated(self) -> None:
        self.start()
        self.unseal()
        self.access_results()
        completed = self.complete(passed=False)
        invalidated = self.store.append(
            trial_id=self.registration.trial_id,
            event_type=TrialLifecycleEventType.TRIAL_INVALIDATED,
            occurred_at=self.registration.registered_at + timedelta(seconds=5),
            actor_id="research-owner",
            reason="A later audit proved the input lineage invalid.",
        )

        self.assertEqual(
            self.store.outcomes(self.registration.trial_id),
            (completed, invalidated),
        )

    def test_post_unseal_retuning_cannot_reuse_confirmatory_holdout(self) -> None:
        self.start()
        self.unseal()
        successor = registration(
            registered_at=self.registration.registered_at + timedelta(seconds=10),
            parent_trial_id=self.registration.trial_id,
            configuration_hash=digest("d"),
        )
        self.registry.register(successor)

        with self.assertRaisesRegex(TrialLifecycleConflict, "unsealed holdout"):
            self.start(successor)

    def test_post_unseal_successor_can_be_exploratory(self) -> None:
        self.start()
        self.unseal()
        successor = registration(
            registered_at=self.registration.registered_at + timedelta(seconds=10),
            stage=TrialStage.EXPLORATORY,
            parent_trial_id=self.registration.trial_id,
            configuration_hash=digest("d"),
            stressed_slippage_bps=None,
        )
        self.registry.register(successor)

        event = self.start(successor)
        self.assertIs(event.event_type, TrialLifecycleEventType.TRIAL_STARTED)

    def test_event_payload_tampering_is_detected(self) -> None:
        event = self.start()
        path = next((self.root / "events" / self.registration.trial_id).glob("*.json"))
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["event"]["reason"] = "A rewritten reason."
        path.write_text(json.dumps(payload), encoding="utf-8")

        with self.assertRaisesRegex(TrialLifecycleIntegrityError, "event ID"):
            self.store.list_events(self.registration.trial_id)
        self.assertEqual(event.sequence, 1)

    def test_confirmatory_completion_requires_audited_holdout_results(self) -> None:
        self.start()
        self.unseal()

        with self.assertRaisesRegex(TrialLifecycleConflict, "results access"):
            self.complete(passed=False)

    def test_deleted_middle_event_breaks_chain(self) -> None:
        self.start()
        self.unseal()
        self.store.append(
            trial_id=self.registration.trial_id,
            event_type=TrialLifecycleEventType.HOLDOUT_RESULTS_ACCESSED,
            occurred_at=self.registration.registered_at + timedelta(seconds=3),
            actor_id="evaluation-runner",
            reason="Read terminal holdout metrics.",
            holdout_id=self.registration.holdout_id,
        )
        paths = sorted((self.root / "events" / self.registration.trial_id).glob("*.json"))
        paths[1].unlink()

        with self.assertRaisesRegex(TrialLifecycleIntegrityError, "sequence"):
            self.store.list_events(self.registration.trial_id)

    def test_lifecycle_codec_round_trips_exactly(self) -> None:
        event = self.start()
        self.assertEqual(
            decode_trial_lifecycle_event(encode_trial_lifecycle_event(event)),
            event,
        )


if __name__ == "__main__":
    unittest.main()
