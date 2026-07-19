from __future__ import annotations

import json
import io
import tempfile
import unittest
from contextlib import redirect_stdout
from dataclasses import replace
from pathlib import Path

from india_swing.demo import build_demo, main
from india_swing.domain.models import DecisionAction, RunStatus
from india_swing.shadow_alerts import (
    LocalShadowNotificationOutbox,
    ShadowAlertError,
    ShadowAlertKind,
    ShadowNotificationNotFound,
    ShadowNotificationStoreError,
    build_shadow_alert,
    render_shadow_alert,
)


def _buy_result():
    pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
    return pipeline.run(snapshot, instruments, portfolio, reference_context)


def _no_trade_result():
    pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
    locked = [replace(item, lower_circuit_locked=True) for item in instruments]
    return pipeline.run(snapshot, locked, portfolio, reference_context)


class ShadowAlertTests(unittest.TestCase):
    def test_builds_complete_non_executable_candidate(self) -> None:
        result = _buy_result()

        alert = build_shadow_alert(result)

        self.assertIs(alert.kind, ShadowAlertKind.CANDIDATE)
        self.assertEqual(alert.source_run_id, result.run_id)
        self.assertEqual(alert.source_pipeline_integrity_hash, result.integrity_hash)
        self.assertEqual(alert.decision, result.decision)
        self.assertFalse(alert.decision.execution_eligible)
        self.assertTrue(alert.evidence_ids)
        self.assertEqual(len(alert.alert_id), 64)
        alert.verify_integrity()

    def test_renderer_contains_complete_logic_and_authority_warning(self) -> None:
        alert = build_shadow_alert(_buy_result())

        message = render_shadow_alert(alert)

        self.assertTrue(
            message.startswith("RESEARCH-ONLY SHADOW ALERT — DO NOT EXECUTE\n")
        )
        for expected in (
            "Symbol: DEMO-SMALL",
            "Entry range:",
            "Stop:",
            "Target:",
            "Planned maximum loss:",
            "Net reward/risk:",
            "Expected R:",
            "Probability status:",
            "Why this candidate:",
            "Thesis:",
            "Bear case:",
            "Cancel if:",
            "Evidence:",
            "This artifact records a paper observation only.",
        ):
            self.assertIn(expected, message)

    def test_no_trade_result_creates_no_trade_shadow_notice(self) -> None:
        result = _no_trade_result()

        alert = build_shadow_alert(result)
        message = render_shadow_alert(alert)

        self.assertIs(result.decision.action, DecisionAction.NO_TRADE)
        self.assertIs(alert.kind, ShadowAlertKind.NO_TRADE)
        self.assertEqual(alert.evidence_ids, ())
        self.assertIn("No candidate passed every gate.", message)

    def test_failed_result_cannot_create_shadow_alert(self) -> None:
        result = _buy_result()
        object.__setattr__(result, "status", RunStatus.FAILED)

        with self.assertRaisesRegex(ShadowAlertError, "integrity"):
            build_shadow_alert(result)

    def test_mutated_nested_decision_cannot_create_shadow_alert(self) -> None:
        result = _buy_result()
        object.__setattr__(result.decision, "target", result.decision.stop)

        with self.assertRaisesRegex(ShadowAlertError, "integrity"):
            build_shadow_alert(result)

    def test_executable_decision_cannot_enter_shadow_outbox(self) -> None:
        result = _buy_result()
        object.__setattr__(result.decision, "execution_eligible", True)
        object.__setattr__(
            result.decision,
            "integrity_hash",
            result.decision._calculated_integrity_hash(),
        )
        object.__setattr__(result, "integrity_hash", result._calculated_integrity_hash())

        with self.assertRaisesRegex(ShadowAlertError, "executable"):
            build_shadow_alert(result)

    def test_selected_research_must_match_decision_thesis(self) -> None:
        result = _buy_result()
        selected = next(item for item in result.research if item.symbol == result.decision.symbol)
        changed = replace(selected, thesis="different public thesis")
        object.__setattr__(
            result,
            "research",
            tuple(changed if item is selected else item for item in result.research),
        )
        object.__setattr__(result, "integrity_hash", result._calculated_integrity_hash())

        with self.assertRaisesRegex(ShadowAlertError, "differs"):
            build_shadow_alert(result)

    def test_recomputed_id_cannot_hide_an_invalid_authority_mutation(self) -> None:
        alert = build_shadow_alert(_buy_result())
        object.__setattr__(alert, "mode", "EXECUTE")
        object.__setattr__(alert, "alert_id", alert._calculated_alert_id())

        with self.assertRaisesRegex(ShadowAlertError, "integrity"):
            alert.verify_integrity()


class ShadowNotificationOutboxTests(unittest.TestCase):
    def test_publish_read_and_idempotent_retry(self) -> None:
        alert = build_shadow_alert(_buy_result())
        with tempfile.TemporaryDirectory() as temp_dir:
            outbox = LocalShadowNotificationOutbox(Path(temp_dir))

            first = outbox.put(alert)
            original = outbox.path_for(alert.alert_id).read_bytes()
            second = outbox.put(alert)

            self.assertEqual(first, second)
            self.assertEqual(first.alert_id, alert.alert_id)
            self.assertEqual(first.source_decision_integrity_hash, alert.decision.integrity_hash)
            self.assertEqual(outbox.get(alert.alert_id), first)
            self.assertEqual(outbox.path_for(alert.alert_id).read_bytes(), original)

    def test_tampered_message_is_rejected(self) -> None:
        alert = build_shadow_alert(_buy_result())
        with tempfile.TemporaryDirectory() as temp_dir:
            outbox = LocalShadowNotificationOutbox(Path(temp_dir))
            outbox.put(alert)
            path = outbox.path_for(alert.alert_id)
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["notification"]["message"] = "BUY NOW"
            path.write_text(json.dumps(payload), encoding="utf-8")

            with self.assertRaisesRegex(ShadowNotificationStoreError, "invalid"):
                outbox.get(alert.alert_id)

    def test_duplicate_json_keys_are_rejected(self) -> None:
        alert = build_shadow_alert(_buy_result())
        with tempfile.TemporaryDirectory() as temp_dir:
            outbox = LocalShadowNotificationOutbox(Path(temp_dir))
            outbox.put(alert)
            path = outbox.path_for(alert.alert_id)
            original = path.read_text(encoding="utf-8")
            path.write_text(
                original.replace(
                    '"codec_schema_version":',
                    '"codec_schema_version":"shadow-notification-json/v1","codec_schema_version":',
                    1,
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ShadowNotificationStoreError, "duplicate"):
                outbox.get(alert.alert_id)

    def test_unknown_alert_and_traversal_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            outbox = LocalShadowNotificationOutbox(Path(temp_dir))
            with self.assertRaises(ShadowNotificationNotFound):
                outbox.get("0" * 64)
            with self.assertRaises(ShadowNotificationStoreError):
                outbox.get("../escape")

    def test_invalid_alert_creates_no_outbox_directory(self) -> None:
        alert = build_shadow_alert(_buy_result())
        object.__setattr__(alert.decision, "target", alert.decision.stop)
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "outbox"

            with self.assertRaisesRegex(ShadowNotificationStoreError, "invalid"):
                LocalShadowNotificationOutbox(root).put(alert)

            self.assertFalse(root.exists())

    def test_demo_can_optionally_publish_shadow_notification(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            with redirect_stdout(io.StringIO()):
                exit_code = main(
                    [
                        "--output-dir",
                        str(root / "audit"),
                        "--shadow-outbox-dir",
                        str(root / "shadow"),
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertEqual(len(tuple((root / "audit").glob("*.json"))), 1)
            notifications = tuple((root / "shadow" / "notifications").glob("*.json"))
            self.assertEqual(len(notifications), 1)
            payload = json.loads(notifications[0].read_text(encoding="utf-8"))
            self.assertEqual(payload["notification"]["mode"], "RESEARCH_ONLY")


if __name__ == "__main__":
    unittest.main()
