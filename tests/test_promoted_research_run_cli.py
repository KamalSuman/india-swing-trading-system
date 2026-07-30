from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from india_swing import promoted_research_run_cli
from india_swing.evaluation.promoted_intents import PromotedIntentPolicyConfig
from india_swing.features.promoted_cross_section import PromotedCrossSectionConfig
from india_swing.features.promoted_technical import PromotedTechnicalFeatureConfig
from tests.test_promoted_research_run import _GRAPH_MANIFEST, _base_request, _fresh_stores


def _run_cli(argv: list[str]) -> tuple[int, dict[str, object]]:
    stdout = io.StringIO()
    with contextlib.redirect_stdout(stdout):
        exit_code = promoted_research_run_cli.main(argv)
    return exit_code, json.loads(stdout.getvalue())


def _run_cli_capture_both(argv: list[str]) -> tuple[int, str, str]:
    stdout = io.StringIO()
    stderr = io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        exit_code = promoted_research_run_cli.main(argv)
    return exit_code, stdout.getvalue(), stderr.getvalue()


class PromotedResearchRunCliTests(unittest.TestCase):
    def _argv(self, root: Path, request, **overrides) -> list[str]:
        values = {
            "--reference-root": str(root / "unused-reference"),
            "--identity-evidence-root": str(root / "unused-identity-evidence"),
            "--calendar-root": str(root / "unused-calendar"),
            "--daily-reports-root": str(root / "unused-daily-reports"),
            "--historical-corpus-root": str(root / "unused-historical-corpus"),
            "--promoted-root": str(root / "unused-promoted"),
            "--graph-publication-root": str(root / "unused-graph-publication"),
            "--engine-run-root": str(root / "unused-engine-run"),
            "--research-run-root": str(root / "unused-research-run"),
            "--graph-manifest-id": request.graph_manifest_id,
            "--signal-session": request.signal_session.isoformat(),
            "--entry-session": request.entry_session.isoformat(),
            "--cutoff": request.cutoff.isoformat(),
            "--initial-capital": str(request.initial_capital),
        }
        values.update(overrides)
        argv: list[str] = []
        for key, value in values.items():
            argv.extend([key, str(value)])
        return argv

    def test_success_path_uses_default_configs_and_returns_sanitized_audit_json(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stores = _fresh_stores(root)
            request = _base_request()
            argv = self._argv(root, request)
            with mock.patch.object(
                promoted_research_run_cli,
                "build_promoted_research_stores",
                return_value=stores,
            ):
                exit_code, payload = _run_cli(argv)

        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["status"], "COMPLETE")
        self.assertTrue(payload["paper_only"])
        self.assertFalse(payload["notification_eligible"])
        self.assertFalse(payload["execution_eligible"])
        # The graph's real, permanently collection-only/non-actionable
        # readiness is reported exactly, never upgraded, and the CLI still
        # completes successfully -- a collection-only paper research pass
        # is not an exception.
        self.assertEqual(
            payload["adjustment_readiness"], _GRAPH_MANIFEST.adjustment_readiness.value
        )
        self.assertEqual(
            payload["adjustment_actionable"], _GRAPH_MANIFEST.adjustment_actionable
        )
        self.assertEqual(
            payload["effective_tick_readiness"],
            _GRAPH_MANIFEST.effective_tick_readiness.value,
        )
        self.assertEqual(
            payload["effective_tick_actionable"],
            _GRAPH_MANIFEST.effective_tick_actionable,
        )
        # The CLI never accepts config objects/flags: it always uses the
        # existing production default constructors, regardless of what
        # config this test's own `request` object happens to carry.
        self.assertEqual(
            payload["technical_config_id"], PromotedTechnicalFeatureConfig().config_id
        )
        self.assertEqual(
            payload["cross_section_config_id"],
            PromotedCrossSectionConfig().config_id,
        )
        self.assertEqual(
            payload["intent_config_id"], PromotedIntentPolicyConfig().config_id
        )
        expected_keys = {
            "status",
            "research_run_id",
            "research_request_id",
            "graph_manifest_id",
            "graph_spec_id",
            "adjustment_bridge_id",
            "effective_tick_panel_id",
            "expected_reference_promotion_ids",
            "expected_corporate_action_snapshot_id",
            "engine_request_id",
            "engine_run_id",
            "feature_input_panel_id",
            "technical_config_id",
            "technical_panel_id",
            "cross_section_config_id",
            "cross_section_panel_id",
            "intent_config_id",
            "research_intent_batch_id",
            "replay_run_id",
            "signal_session",
            "entry_session",
            "cutoff",
            "initial_capital",
            "candidate_count",
            "intent_count",
            "adjustment_readiness",
            "adjustment_actionable",
            "effective_tick_readiness",
            "effective_tick_actionable",
            "paper_only",
            "notification_eligible",
            "execution_eligible",
        }
        self.assertEqual(set(payload), expected_keys)

    def test_repeated_invocation_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stores = _fresh_stores(root)
            request = _base_request()
            argv = self._argv(root, request)
            with mock.patch.object(
                promoted_research_run_cli,
                "build_promoted_research_stores",
                return_value=stores,
            ):
                first_code, first_payload = _run_cli(argv)
                second_code, second_payload = _run_cli(argv)

        self.assertEqual(first_code, 0)
        self.assertEqual(second_code, 0)
        self.assertEqual(
            first_payload["research_run_id"], second_payload["research_run_id"]
        )

    def test_ordinary_store_error_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stores = _fresh_stores(root)
            request = _base_request()
            argv = self._argv(root, request, **{"--graph-manifest-id": "0" * 64})
            with mock.patch.object(
                promoted_research_run_cli,
                "build_promoted_research_stores",
                return_value=stores,
            ):
                exit_code, payload = _run_cli(argv)

        self.assertEqual(exit_code, 2)
        self.assertEqual(set(payload), {"status", "error_type"})
        self.assertEqual(payload["status"], "FAILED")

    def test_missing_required_argument_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stores = _fresh_stores(root)
            request = _base_request()
            argv = self._argv(root, request)
            flag_index = argv.index("--graph-manifest-id")
            del argv[flag_index : flag_index + 2]
            with mock.patch.object(
                promoted_research_run_cli,
                "build_promoted_research_stores",
                return_value=stores,
            ):
                exit_code, stdout_text, stderr_text = _run_cli_capture_both(argv)

        self.assertEqual(exit_code, 2)
        self.assertEqual(stderr_text, "")
        payload = json.loads(stdout_text)
        self.assertEqual(set(payload), {"status", "error_type"})
        self.assertEqual(payload["status"], "FAILED")
        self.assertNotIn("graph-manifest-id", stdout_text)
        self.assertNotIn("usage:", stdout_text.lower())

    def test_unknown_option_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stores = _fresh_stores(root)
            request = _base_request()
            argv = self._argv(root, request) + ["--not-a-real-option", "value"]
            with mock.patch.object(
                promoted_research_run_cli,
                "build_promoted_research_stores",
                return_value=stores,
            ):
                exit_code, stdout_text, stderr_text = _run_cli_capture_both(argv)

        self.assertEqual(exit_code, 2)
        self.assertEqual(stderr_text, "")
        payload = json.loads(stdout_text)
        self.assertEqual(set(payload), {"status", "error_type"})
        self.assertEqual(payload["status"], "FAILED")
        self.assertNotIn("not-a-real-option", stdout_text)

    def test_malformed_cutoff_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stores = _fresh_stores(root)
            request = _base_request()
            argv = self._argv(root, request, **{"--cutoff": "not-a-datetime"})
            with mock.patch.object(
                promoted_research_run_cli,
                "build_promoted_research_stores",
                return_value=stores,
            ):
                exit_code, stdout_text, stderr_text = _run_cli_capture_both(argv)

        self.assertEqual(exit_code, 2)
        self.assertEqual(stderr_text, "")
        payload = json.loads(stdout_text)
        self.assertEqual(set(payload), {"status", "error_type"})
        self.assertEqual(payload["status"], "FAILED")
        self.assertNotIn("not-a-datetime", stdout_text)

    def test_no_network_broker_or_discovery_flags_exist(self) -> None:
        parser = promoted_research_run_cli.build_parser()
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
