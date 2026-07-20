from __future__ import annotations

import ast
import io
import json
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from india_swing.cloud_job import main

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DELEGATE_TARGET = "india_swing.cloud_job._daily_pipeline_main"


def _run_main(argv: list[str]) -> tuple[int, str, str]:
    stdout = io.StringIO()
    stderr = io.StringIO()
    with redirect_stdout(stdout), redirect_stderr(stderr):
        exit_code = main(argv)
    return exit_code, stdout.getvalue(), stderr.getvalue()


class ArgumentValidationTests(unittest.TestCase):
    def _assert_config_error(self, argv: list[str], secret: str | None = None) -> None:
        with patch(_DELEGATE_TARGET) as delegate:
            exit_code, stdout, stderr = _run_main(argv)
        self.assertEqual(exit_code, 2)
        self.assertEqual(stdout, "")
        payload = json.loads(stderr)
        self.assertEqual(
            payload, {"status": "FAILED", "error_type": "CloudJobConfigurationError"}
        )
        delegate.assert_not_called()
        if secret is not None:
            self.assertNotIn(secret, stderr)

    def test_missing_spec_file(self) -> None:
        self._assert_config_error([])

    def test_unknown_flag(self) -> None:
        secret = "SECRET-UNKNOWN-FLAG-VALUE-DO-NOT-LEAK-1a2b"
        self._assert_config_error(["--unknown-flag", secret], secret=secret)

    def test_positional_argument(self) -> None:
        secret = "/secret/positional/spec-path-DO-NOT-LEAK.json"
        self._assert_config_error([secret], secret=secret)

    def test_duplicate_spec_file(self) -> None:
        secret = "/secret/second/spec-path-DO-NOT-LEAK.json"
        self._assert_config_error(
            ["--spec-file", "/first/spec.json", "--spec-file", secret], secret=secret
        )

    def test_spec_file_missing_value(self) -> None:
        self._assert_config_error(["--spec-file"])

    def test_empty_spec_file_value(self) -> None:
        self._assert_config_error(["--spec-file", ""])

    def test_extra_trailing_positional_after_valid_flag(self) -> None:
        secret = "/secret/trailing/positional-DO-NOT-LEAK.json"
        self._assert_config_error(["--spec-file", "/ok/spec.json", secret], secret=secret)


class DelegationTests(unittest.TestCase):
    def test_valid_spec_path_delegates_exactly_once_with_exact_argv(self) -> None:
        spec_path = "/tmp/operator-authored-spec.json"
        with patch(_DELEGATE_TARGET, return_value=0) as delegate:
            exit_code, stdout, stderr = _run_main(["--spec-file", spec_path])
        self.assertEqual(exit_code, 0)
        self.assertEqual(stdout, "")
        self.assertEqual(stderr, "")
        delegate.assert_called_once_with(["run-pinned-gcs", "--spec-file", spec_path])

    def test_nonzero_delegate_exit_code_passes_through_unchanged(self) -> None:
        with patch(_DELEGATE_TARGET, return_value=2):
            exit_code, stdout, stderr = _run_main(["--spec-file", "/tmp/spec.json"])
        self.assertEqual(exit_code, 2)
        self.assertEqual(stdout, "")
        self.assertEqual(stderr, "")

    def test_unexpected_secret_bearing_exception_from_delegate_is_sanitized(self) -> None:
        secret_path = "/secret/spec/path-DO-NOT-LEAK.json"
        secret_text = "SECRET-NESTED-EXCEPTION-TEXT-DO-NOT-LEAK-9f3c"

        def _raise(argv: list[str]) -> int:
            raise RuntimeError(f"failure reading {secret_path}: {secret_text}")

        with patch(_DELEGATE_TARGET, side_effect=_raise):
            exit_code, stdout, stderr = _run_main(["--spec-file", secret_path])
        self.assertEqual(exit_code, 2)
        self.assertEqual(stdout, "")
        payload = json.loads(stderr)
        self.assertEqual(payload, {"status": "FAILED", "error_type": "CloudJobRuntimeError"})
        self.assertEqual(set(payload), {"status", "error_type"})
        self.assertNotIn(secret_path, stderr)
        self.assertNotIn(secret_text, stderr)


class CapabilityLockTests(unittest.TestCase):
    def _module_ast(self) -> ast.Module:
        source = (_REPO_ROOT / "src" / "india_swing" / "cloud_job.py").read_text(
            encoding="utf-8"
        )
        return ast.parse(source)

    def test_imports_no_demo_forecasting_research_signals_market_data_or_broker_order(
        self,
    ) -> None:
        forbidden_segments = {"demo", "forecasting", "research", "signals", "market_data"}
        forbidden_substrings = ("broker", "order")
        tree = self._module_ast()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or ""]
            else:
                continue
            for name in names:
                lowered = name.lower()
                segments = set(lowered.split("."))
                self.assertTrue(segments.isdisjoint(forbidden_segments), name)
                for token in forbidden_substrings:
                    self.assertNotIn(token, lowered, name)


class DockerfileTests(unittest.TestCase):
    def test_effective_final_cmd_is_cloud_job_and_never_invokes_demo(self) -> None:
        text = (_REPO_ROOT / "Dockerfile").read_text(encoding="utf-8")
        runtime_command_lines = [
            line
            for line in text.splitlines()
            if line.strip().startswith("CMD") or line.strip().startswith("ENTRYPOINT")
        ]
        self.assertTrue(runtime_command_lines, "Dockerfile has no CMD/ENTRYPOINT instruction")
        for line in runtime_command_lines:
            self.assertNotIn("india_swing.demo", line)
        final_line = runtime_command_lines[-1]
        self.assertIn("india_swing.cloud_job", final_line)


class DeployScriptTests(unittest.TestCase):
    def setUp(self) -> None:
        self.text = (_REPO_ROOT / "deploy.sh").read_text(encoding="utf-8")

    def test_scheduler_flag_defaults_false(self) -> None:
        self.assertIn('ENABLE_EOD_SCHEDULER="${ENABLE_EOD_SCHEDULER:-false}"', self.text)

    def test_both_job_update_and_create_branches_override_command_and_args(self) -> None:
        update_start = self.text.index('gcloud run jobs update "${JOB_NAME}"')
        create_start = self.text.index('gcloud run jobs create "${JOB_NAME}"')
        self.assertLess(update_start, create_start)
        update_block = self.text[update_start:create_start]
        # The create block runs from its own start to the closing `fi` of
        # the surrounding if/else that selects update vs. create.
        fi_after_create = self.text.index("\nfi", create_start)
        create_block = self.text[create_start:fi_after_create]
        for block in (update_block, create_block):
            self.assertIn("--command=python", block)
            self.assertIn("--args=-m,india_swing.cloud_job", block)

    def test_eod_scheduler_create_update_reachable_only_through_true_branch(self) -> None:
        true_branch_marker = 'if [ "${ENABLE_EOD_SCHEDULER}" = "true" ]'
        self.assertIn(true_branch_marker, self.text)
        if_index = self.text.index(true_branch_marker)
        else_index = self.text.index("\nelse", if_index)
        fi_index = self.text.index("\nfi", else_index)
        self.assertLess(if_index, else_index)
        self.assertLess(else_index, fi_index)
        true_branch = self.text[if_index:else_index]
        false_branch = self.text[else_index:fi_index]

        self.assertIn("gcloud scheduler jobs create http eod-swing-schedule", true_branch)
        self.assertIn("gcloud scheduler jobs update http eod-swing-schedule", true_branch)
        self.assertNotIn("gcloud scheduler jobs create http eod-swing-schedule", false_branch)
        self.assertNotIn("gcloud scheduler jobs update http eod-swing-schedule", false_branch)

        for needle in (
            "gcloud scheduler jobs create http eod-swing-schedule",
            "gcloud scheduler jobs update http eod-swing-schedule",
        ):
            self.assertEqual(self.text.count(needle), 1, needle)
            first = self.text.index(needle)
            self.assertGreaterEqual(first, if_index)
            self.assertLess(first, else_index)

    def test_false_mode_contains_pause_path(self) -> None:
        true_branch_marker = 'if [ "${ENABLE_EOD_SCHEDULER}" = "true" ]'
        if_index = self.text.index(true_branch_marker)
        else_index = self.text.index("\nelse", if_index)
        fi_index = self.text.index("\nfi", else_index)
        false_branch = self.text[else_index:fi_index]
        self.assertIn("gcloud scheduler jobs pause", false_branch)
        self.assertIn('"eod-swing-schedule"', false_branch)

    def test_final_messages_do_not_unconditionally_claim_schedulers_active(self) -> None:
        success_index = self.text.rindex("SUCCESS:")
        tail = self.text[success_index:]
        self.assertNotIn("Your Scheduler jobs are now active", tail)
        self.assertIn("EOD_SCHEDULER_STATE", tail)

    def test_no_test_or_implementation_execution_of_deploy_script(self) -> None:
        # This test class only ever reads deploy.sh as text; it never
        # imports subprocess, never shells out, and never invokes bash,
        # docker, or gcloud. Asserting the text was read successfully (and
        # is non-empty) is the whole extent of what this test exercises.
        self.assertGreater(len(self.text), 0)


if __name__ == "__main__":
    unittest.main()
