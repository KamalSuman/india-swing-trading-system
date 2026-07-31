from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from india_swing._exact_replay import ExactReplayScope
from india_swing import promoted_operational_preparation_cli
from india_swing.promoted_operational_preparation import (
    LocalPromotedOperationalPreparationStore,
)
from tests.test_promoted_operational_preparation import (
    _EMPTY_LINEAGE,
    _NONEMPTY_LINEAGE,
    _StubResolver,
)


class _FakeEngineStores:
    def __init__(self, engine_runs, research_intents) -> None:
        self.engine_runs = engine_runs
        self.research_intents = research_intents


class _FakeResearchStores:
    def __init__(self, research_runs, engine, replay_scope: ExactReplayScope) -> None:
        self.research_runs = research_runs
        self.engine = engine
        self._replay_scope = replay_scope


def _fake_stores(root: Path, lineage):
    research_run_manifest, engine_run_manifest, batch = lineage
    replay_scope = ExactReplayScope()
    research_stores = _FakeResearchStores(
        research_runs=_StubResolver(
            {research_run_manifest.research_run_id: research_run_manifest}
        ),
        engine=_FakeEngineStores(
            engine_runs=_StubResolver({engine_run_manifest.run_id: engine_run_manifest}),
            research_intents=_StubResolver({batch.batch_id: batch}),
        ),
        replay_scope=replay_scope,
    )
    preparations = LocalPromotedOperationalPreparationStore(
        root / "preparations",
        research_runs=research_stores.research_runs,
        engine_runs=research_stores.engine.engine_runs,
        research_intents=research_stores.engine.research_intents,
        replay_scope=replay_scope,
    )
    return research_run_manifest, research_stores, preparations


def _run_cli(argv: list[str]) -> tuple[int, dict[str, object]]:
    stdout = io.StringIO()
    with contextlib.redirect_stdout(stdout):
        exit_code = promoted_operational_preparation_cli.main(argv)
    return exit_code, json.loads(stdout.getvalue())


def _run_cli_capture_both(argv: list[str]) -> tuple[int, str, str]:
    stdout = io.StringIO()
    stderr = io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        exit_code = promoted_operational_preparation_cli.main(argv)
    return exit_code, stdout.getvalue(), stderr.getvalue()


class PromotedOperationalPreparationCliTests(unittest.TestCase):
    def _argv(self, root: Path, research_run_id: str, **overrides) -> list[str]:
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
            "--operational-preparation-root": str(root / "unused-operational-preparation"),
            "--research-run-id": research_run_id,
        }
        values.update(overrides)
        argv: list[str] = []
        for key, value in values.items():
            argv.extend([key, str(value)])
        return argv

    def test_success_is_sanitized_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            research_run_manifest, research_stores, preparations = _fake_stores(
                root, _NONEMPTY_LINEAGE
            )
            argv = self._argv(root, research_run_manifest.research_run_id)
            with mock.patch.object(
                promoted_operational_preparation_cli,
                "build_promoted_operational_preparation_store",
                return_value=(research_stores, preparations),
            ):
                first_code, first_payload = _run_cli(argv)
                second_code, second_payload = _run_cli(argv)

        self.assertEqual(first_code, 0)
        self.assertEqual(second_code, 0)
        self.assertEqual(first_payload["status"], "COMPLETE")
        self.assertEqual(
            first_payload["preparation_id"], second_payload["preparation_id"]
        )
        self.assertTrue(first_payload["paper_only"])
        self.assertFalse(first_payload["notification_eligible"])
        self.assertFalse(first_payload["execution_eligible"])
        self.assertEqual(first_payload["selected_count"], 2)
        self.assertEqual(first_payload["candidate_ids"], second_payload["candidate_ids"])
        self.assertEqual(
            first_payload["listing_keys"], ["NSE:RELIANCE", "NSE:TCS"]
        )
        expected_keys = {
            "status",
            "preparation_id",
            "research_run_id",
            "research_request_id",
            "graph_manifest_id",
            "graph_request_id",
            "engine_run_id",
            "engine_request_id",
            "research_intent_batch_id",
            "signal_session",
            "target_session",
            "cutoff",
            "candidate_ids",
            "research_intent_ids",
            "listing_keys",
            "selected_count",
            "blocked_count",
            "source_universe_complete",
            "readiness",
            "paper_only",
            "notification_eligible",
            "execution_eligible",
        }
        self.assertEqual(set(first_payload), expected_keys)

        with tempfile.TemporaryDirectory() as empty_tmp:
            empty_root = Path(empty_tmp)
            (
                empty_research_run_manifest,
                empty_research_stores,
                empty_preparations,
            ) = _fake_stores(empty_root, _EMPTY_LINEAGE)
            empty_argv = self._argv(
                empty_root, empty_research_run_manifest.research_run_id
            )
            with mock.patch.object(
                promoted_operational_preparation_cli,
                "build_promoted_operational_preparation_store",
                return_value=(empty_research_stores, empty_preparations),
            ):
                empty_exit_code, empty_payload = _run_cli(empty_argv)

        self.assertEqual(empty_exit_code, 0)
        self.assertEqual(empty_payload["status"], "COMPLETE")
        self.assertEqual(empty_payload["selected_count"], 0)
        self.assertEqual(empty_payload["candidate_ids"], [])

    def test_argument_and_runtime_failures_are_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            research_run_manifest, research_stores, preparations = _fake_stores(
                root, _NONEMPTY_LINEAGE
            )
            valid_argv = self._argv(root, research_run_manifest.research_run_id)

            with mock.patch.object(
                promoted_operational_preparation_cli,
                "build_promoted_operational_preparation_store",
                return_value=(research_stores, preparations),
            ):
                # Missing required argument.
                missing_argv = list(valid_argv)
                flag_index = missing_argv.index("--research-run-id")
                del missing_argv[flag_index : flag_index + 2]
                exit_code, stdout_text, stderr_text = _run_cli_capture_both(
                    missing_argv
                )
                self.assertEqual(exit_code, 2)
                self.assertEqual(stderr_text, "")
                payload = json.loads(stdout_text)
                self.assertEqual(set(payload), {"status", "error_type"})
                self.assertEqual(payload["status"], "FAILED")
                self.assertNotIn("research-run-id", stdout_text)
                self.assertNotIn("usage:", stdout_text.lower())

                # Unknown option.
                unknown_argv = list(valid_argv) + ["--not-a-real-option", "value"]
                exit_code, stdout_text, stderr_text = _run_cli_capture_both(
                    unknown_argv
                )
                self.assertEqual(exit_code, 2)
                self.assertEqual(stderr_text, "")
                payload = json.loads(stdout_text)
                self.assertEqual(set(payload), {"status", "error_type"})
                self.assertEqual(payload["status"], "FAILED")
                self.assertNotIn("not-a-real-option", stdout_text)

                # Malformed (non-hex-shaped) research-run-id: a real runtime
                # rejection, not an argparse failure.
                malformed_argv = self._argv(root, "not-a-valid-id")
                exit_code, payload = _run_cli(malformed_argv)
                self.assertEqual(exit_code, 2)
                self.assertEqual(set(payload), {"status", "error_type"})
                self.assertEqual(payload["status"], "FAILED")

                # Missing (well-shaped but unresolvable) research run.
                missing_run_argv = self._argv(root, "0" * 64)
                exit_code, payload = _run_cli(missing_run_argv)
                self.assertEqual(exit_code, 2)
                self.assertEqual(set(payload), {"status", "error_type"})
                self.assertEqual(payload["status"], "FAILED")


if __name__ == "__main__":
    unittest.main()
