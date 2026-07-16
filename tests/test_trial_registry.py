from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from india_swing.evaluation import (
    LocalTrialRegistry,
    TrialNotRegistered,
    TrialRegistration,
    TrialRegistryConfig,
    TrialRegistrationError,
    TrialRegistrationIntegrityError,
    TrialStage,
    decode_trial_registration,
    encode_trial_registration,
)


UTC = timezone.utc


def digest(character: str) -> str:
    return character * 64


def registration(**overrides: object) -> TrialRegistration:
    values: dict[str, object] = {
        "registered_at": datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
        "stage": TrialStage.CONFIRMATORY,
        "hypothesis": "Cross-sectional momentum improves net return over equal weight.",
        "strategy_family_id": "nse-swing-momentum-v1",
        "parent_trial_id": None,
        "evaluation_start": date(2022, 1, 3),
        "evaluation_end": date(2025, 12, 31),
        "universe_snapshot_ids": (digest("1"), digest("2")),
        "data_snapshot_ids": (digest("3"), digest("4")),
        "split_plan_id": digest("5"),
        "label_horizon_sessions": 10,
        "benchmark_id": "equal-weight-eligible-universe-v1",
        "primary_metric": "net_cagr",
        "secondary_metrics": ("max_drawdown", "turnover"),
        "model_bundle_id": "deterministic-momentum-baseline-v1",
        "source_commit": "9ab4167",
        "dependency_hash": digest("6"),
        "configuration_hash": digest("7"),
        "exclusions_hash": digest("8"),
        "risk_policy_hash": digest("9"),
        "execution_policy_version": "next-session-pessimistic-v1",
        "execution_policy_hash": digest("a"),
        "cost_schedule_version": "india-equity-cost-placeholder-binding-v1",
        "cost_schedule_hash": digest("b"),
        "base_slippage_bps": Decimal("12"),
        "stressed_slippage_bps": Decimal("25"),
        "pass_thresholds": (
            ("max_drawdown", Decimal("-0.20")),
            ("net_cagr", Decimal("0.10")),
        ),
        "multiple_testing_policy": "holm-familywise-v1",
        "random_seed": 1729,
        "repetition_count": 1,
        "holdout_id": digest("c"),
        "holdout_sealed": True,
        "synthetic": False,
    }
    values.update(overrides)
    return TrialRegistration(**values)


class TrialRegistrationTests(unittest.TestCase):
    def test_registry_config_has_safe_default_and_explicit_override(self) -> None:
        self.assertEqual(
            TrialRegistryConfig.from_env({}).data_root,
            Path("var/trial_registry"),
        )
        self.assertEqual(
            TrialRegistryConfig.from_env(
                {"INDIA_SWING_TRIAL_REGISTRY_ROOT": "D:/sealed/trials"}
            ).data_root,
            Path("D:/sealed/trials"),
        )

    def test_trial_registration_contains_required_hashes_and_thresholds(self) -> None:
        value = registration()

        self.assertEqual(len(value.trial_id), 64)
        self.assertEqual(len(value.configuration_hash), 64)
        self.assertEqual(len(value.cost_schedule_hash), 64)
        self.assertIn(
            value.primary_metric,
            {name for name, _ in value.pass_thresholds},
        )
        self.assertGreater(value.base_slippage_bps, 0)

    def test_confirmatory_trial_declares_primary_metric_before_run(self) -> None:
        with self.assertRaisesRegex(TrialRegistrationError, "primary_metric"):
            registration(primary_metric="")

    def test_confirmatory_trial_requires_sealed_holdout(self) -> None:
        with self.assertRaisesRegex(TrialRegistrationError, "sealed holdout"):
            registration(holdout_id=None, holdout_sealed=False)

    def test_confirmatory_trial_requires_stressed_slippage_scenario(self) -> None:
        with self.assertRaisesRegex(TrialRegistrationError, "stressed slippage"):
            registration(stressed_slippage_bps=Decimal("12"))

    def test_reportable_trial_rejects_zero_slippage(self) -> None:
        with self.assertRaisesRegex(TrialRegistrationError, "zero base slippage"):
            registration(base_slippage_bps=Decimal("0"))

    def test_changed_configuration_requires_new_trial_id(self) -> None:
        original = registration()
        successor = registration(
            registered_at=original.registered_at + timedelta(seconds=1),
            hypothesis="A revised momentum definition improves net return.",
            parent_trial_id=original.trial_id,
            configuration_hash=digest("d"),
        )

        self.assertNotEqual(original.trial_id, successor.trial_id)
        self.assertEqual(successor.parent_trial_id, original.trial_id)
        self.assertEqual(successor.strategy_family_id, original.strategy_family_id)

    def test_nested_mutation_invalidates_registration(self) -> None:
        value = registration()
        object.__setattr__(value, "configuration_hash", digest("e"))

        with self.assertRaisesRegex(
            TrialRegistrationIntegrityError,
            "content identity",
        ):
            value.verify_content_identity()

    def test_registration_codec_round_trips_exactly(self) -> None:
        value = registration()
        self.assertEqual(decode_trial_registration(encode_trial_registration(value)), value)


class LocalTrialRegistryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "trials"
        self.store = LocalTrialRegistry(self.root)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_evaluation_cannot_start_without_registered_trial(self) -> None:
        with self.assertRaises(TrialNotRegistered):
            self.store.require_registered(digest("f"))

    def test_registration_is_create_once_and_idempotent(self) -> None:
        value = registration()
        first = self.store.register(value)
        original = (self.root / f"{value.trial_id}.json").read_bytes()
        second = self.store.register(value)

        self.assertEqual(first, second)
        self.assertEqual(
            (self.root / f"{value.trial_id}.json").read_bytes(),
            original,
        )

    def test_stored_registration_tampering_is_detected(self) -> None:
        value = registration()
        self.store.register(value)
        path = self.root / f"{value.trial_id}.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["registration"]["hypothesis"] = "A result-informed rewrite."
        path.write_text(json.dumps(payload), encoding="utf-8")

        with self.assertRaisesRegex(
            TrialRegistrationIntegrityError,
            "trial ID",
        ):
            self.store.get(value.trial_id)

    def test_registered_parent_and_successor_remain_queryable(self) -> None:
        parent = registration()
        successor = registration(
            registered_at=parent.registered_at + timedelta(seconds=1),
            parent_trial_id=parent.trial_id,
            configuration_hash=digest("d"),
        )
        self.store.register(parent)
        self.store.register(successor)

        self.assertEqual(self.store.get(parent.trial_id), parent)
        self.assertEqual(self.store.get(successor.trial_id), successor)
        self.assertEqual(len(tuple(self.root.glob("*.json"))), 2)
        self.assertEqual(
            self.store.registrations_for_family(parent.strategy_family_id),
            (parent, successor),
        )

    def test_changed_family_configuration_requires_registered_parent(self) -> None:
        parent = registration()
        unlinked = registration(
            registered_at=parent.registered_at + timedelta(seconds=1),
            configuration_hash=digest("d"),
        )
        self.store.register(parent)

        with self.assertRaisesRegex(TrialRegistrationError, "registered parent"):
            self.store.register(unlinked)


if __name__ == "__main__":
    unittest.main()
