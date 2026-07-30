from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest import mock

from india_swing import promoted_graph_publisher_cli
from india_swing.promoted_graph_publisher import (
    PromotedGraphPromotionBinding,
    PromotedGraphPublicationSpec,
    PromotedGraphSessionBinding,
)
from tests.test_promoted_graph_publisher import _build_fixture_and_stores


def _run_cli(argv: list[str]) -> tuple[int, dict[str, object]]:
    stdout = io.StringIO()
    with contextlib.redirect_stdout(stdout):
        exit_code = promoted_graph_publisher_cli.main(argv)
    return exit_code, json.loads(stdout.getvalue())


def _run_cli_capture_both(argv: list[str]) -> tuple[int, str, str]:
    stdout = io.StringIO()
    stderr = io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        exit_code = promoted_graph_publisher_cli.main(argv)
    return exit_code, stdout.getvalue(), stderr.getvalue()


def _minimal_spec() -> PromotedGraphPublicationSpec:
    """Valid in-memory arguments for parser-boundary tests with no disk graph."""

    return PromotedGraphPublicationSpec(
        promotion_bindings=(
            PromotedGraphPromotionBinding("1" * 64, date(2026, 7, 15)),
        ),
        identity_evidence_artifact_ids=(),
        identity_review_bundle_ids=(),
        calendar_materialization_id="2" * 64,
        session_bindings=(
            PromotedGraphSessionBinding(date(2026, 7, 15), "3" * 64),
        ),
        corporate_action_snapshot_id="4" * 64,
        cutoff=datetime(2026, 7, 20, tzinfo=timezone.utc),
    )


class PromotedGraphPublisherCliTests(unittest.TestCase):
    def _argv(self, root: Path, spec, **overrides) -> list[str]:
        values = {
            "--reference-root": str(root / "unused-reference"),
            "--identity-evidence-root": str(root / "unused-identity-evidence"),
            "--calendar-root": str(root / "unused-calendar"),
            "--daily-reports-root": str(root / "unused-daily-reports"),
            "--historical-corpus-root": str(root / "unused-historical-corpus"),
            "--promoted-root": str(root / "unused-promoted"),
            "--publication-root": str(root / "unused-publication"),
            "--calendar-materialization-id": spec.calendar_materialization_id,
            "--corporate-action-snapshot-id": spec.corporate_action_snapshot_id,
            "--cutoff": spec.cutoff.isoformat(),
        }
        values.update(overrides)
        argv: list[str] = []
        for key, value in values.items():
            argv.extend([key, str(value)])
        for binding in spec.promotion_bindings:
            argv.extend(
                [
                    "--promotion-binding",
                    f"{binding.promotion_id}@{binding.expected_report_date.isoformat()}",
                ]
            )
        for binding in spec.session_bindings:
            argv.extend(
                [
                    "--session-binding",
                    f"{binding.market_session.isoformat()}@{binding.historical_corpus_id}",
                ]
            )
        for value in spec.identity_evidence_artifact_ids:
            argv.extend(["--identity-evidence-id", value])
        for value in spec.identity_review_bundle_ids:
            argv.extend(["--identity-review-id", value])
        return argv

    def test_success_path_returns_sanitized_audit_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stores, spec, _roots = _build_fixture_and_stores(root)
            argv = self._argv(root, spec)
            with mock.patch.object(
                promoted_graph_publisher_cli,
                "build_promoted_graph_stores",
                return_value=stores,
            ):
                exit_code, payload = _run_cli(argv)

        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["status"], "COMPLETE")
        self.assertTrue(payload["paper_only"])
        self.assertFalse(payload["execution_eligible"])
        expected_keys = {
            "status",
            "spec_id",
            "manifest_id",
            "promotion_bindings",
            "identity_evidence_artifact_ids",
            "identity_review_bundle_ids",
            "calendar_materialization_id",
            "session_bindings",
            "corporate_action_snapshot_id",
            "cutoff",
            "intake_id",
            "adjudication_id",
            "session_artifacts",
            "stable_history_panel_id",
            "adjustment_bridge_id",
            "effective_tick_panel_id",
            "adjustment_readiness",
            "adjustment_actionable",
            "effective_tick_readiness",
            "effective_tick_actionable",
            "paper_only",
            "execution_eligible",
        }
        self.assertEqual(set(payload), expected_keys)

    def test_repeated_invocation_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stores, spec, _roots = _build_fixture_and_stores(root)
            argv = self._argv(root, spec)
            with mock.patch.object(
                promoted_graph_publisher_cli,
                "build_promoted_graph_stores",
                return_value=stores,
            ):
                first_code, first_payload = _run_cli(argv)
                second_code, second_payload = _run_cli(argv)

        self.assertEqual(first_code, 0)
        self.assertEqual(second_code, 0)
        self.assertEqual(first_payload["manifest_id"], second_payload["manifest_id"])

    def test_ordinary_store_error_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stores, spec, _roots = _build_fixture_and_stores(root)
            argv = self._argv(
                root, spec, **{"--corporate-action-snapshot-id": "0" * 64}
            )
            with mock.patch.object(
                promoted_graph_publisher_cli,
                "build_promoted_graph_stores",
                return_value=stores,
            ):
                exit_code, payload = _run_cli(argv)

        self.assertEqual(exit_code, 2)
        self.assertEqual(set(payload), {"status", "error_type"})
        self.assertEqual(payload["status"], "FAILED")

    def test_missing_required_argument_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = _minimal_spec()
            argv = self._argv(root, spec)
            flag_index = argv.index("--calendar-materialization-id")
            del argv[flag_index : flag_index + 2]
            exit_code, stdout_text, stderr_text = _run_cli_capture_both(argv)

        self.assertEqual(exit_code, 2)
        self.assertEqual(stderr_text, "")
        payload = json.loads(stdout_text)
        self.assertEqual(set(payload), {"status", "error_type"})
        self.assertEqual(payload["status"], "FAILED")
        self.assertNotIn("calendar-materialization-id", stdout_text)
        self.assertNotIn("usage:", stdout_text.lower())

    def test_unknown_option_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = _minimal_spec()
            argv = self._argv(root, spec) + ["--not-a-real-option", "value"]
            exit_code, stdout_text, stderr_text = _run_cli_capture_both(argv)

        self.assertEqual(exit_code, 2)
        self.assertEqual(stderr_text, "")
        payload = json.loads(stdout_text)
        self.assertEqual(set(payload), {"status", "error_type"})
        self.assertEqual(payload["status"], "FAILED")
        self.assertNotIn("not-a-real-option", stdout_text)

    def test_malformed_promotion_binding_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = _minimal_spec()
            argv = self._argv(root, spec)
            index = argv.index("--promotion-binding")
            argv[index + 1] = "not-a-valid-binding"
            exit_code, stdout_text, stderr_text = _run_cli_capture_both(argv)

        self.assertEqual(exit_code, 2)
        self.assertEqual(stderr_text, "")
        payload = json.loads(stdout_text)
        self.assertEqual(set(payload), {"status", "error_type"})
        self.assertEqual(payload["status"], "FAILED")
        self.assertNotIn("not-a-valid-binding", stdout_text)

    def test_malformed_session_binding_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = _minimal_spec()
            argv = self._argv(root, spec)
            index = argv.index("--session-binding")
            argv[index + 1] = "also-not-valid"
            exit_code, stdout_text, stderr_text = _run_cli_capture_both(argv)

        self.assertEqual(exit_code, 2)
        self.assertEqual(stderr_text, "")
        payload = json.loads(stdout_text)
        self.assertEqual(set(payload), {"status", "error_type"})
        self.assertEqual(payload["status"], "FAILED")
        self.assertNotIn("also-not-valid", stdout_text)

    def test_no_network_broker_or_discovery_flags_exist(self) -> None:
        parser = promoted_graph_publisher_cli.build_parser()
        option_strings = {
            option
            for action in parser._actions
            for option in action.option_strings
        }
        banned = ("list", "latest", "broker", "telegram", "network")
        for option in option_strings:
            lowered = option.lower()
            self.assertFalse(
                any(bad in lowered for bad in banned),
                f"unexpected CLI flag {option!r}",
            )


if __name__ == "__main__":
    unittest.main()
