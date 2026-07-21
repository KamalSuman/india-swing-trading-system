from __future__ import annotations

import ast
import io
import json
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

import india_swing.state_restore_job as job_module


_MODULE_PATH = Path(job_module.__file__)
_REPO_ROOT = _MODULE_PATH.parents[2]


def _run(argv) -> tuple[int, str, str]:
    stdout = io.StringIO()
    stderr = io.StringIO()
    with redirect_stdout(stdout), redirect_stderr(stderr):
        result = job_module.main(argv)
    return result, stdout.getvalue(), stderr.getvalue()


class StateRestoreJobTests(unittest.TestCase):
    def test_delegates_exact_argument_once_and_forwards_exit_code(self) -> None:
        spec_path = "/var/run/india-swing-control/restore-spec.json"
        for delegated_code in (0, 2, 17):
            with self.subTest(delegated_code=delegated_code):
                with patch.object(
                    job_module,
                    "_daily_pipeline_main",
                    return_value=delegated_code,
                ) as delegate:
                    result, stdout, stderr = _run(["--spec-file", spec_path])
                self.assertEqual(result, delegated_code)
                self.assertEqual(stdout, "")
                self.assertEqual(stderr, "")
                delegate.assert_called_once_with(
                    ["restore-pinned-state", "--spec-file", spec_path]
                )

    def test_invalid_arguments_fail_closed_without_delegation(self) -> None:
        cases = (
            [],
            ["--unknown", "value"],
            ["--spec-file"],
            ["--spec-file", ""],
            ["--spec-file", "bad\x00path"],
            ["--spec-file=value"],
            ["positional"],
            ["--spec-file", "a", "--spec-file", "b"],
            ["--spec-file", 123],
        )
        for argv in cases:
            with self.subTest(argv=argv):
                with patch.object(job_module, "_daily_pipeline_main") as delegate:
                    result, stdout, stderr = _run(argv)
                self.assertEqual(result, 2)
                self.assertEqual(stdout, "")
                self.assertEqual(
                    json.loads(stderr),
                    {
                        "status": "FAILED",
                        "error_type": "StateRestoreJobConfigurationError",
                    },
                )
                delegate.assert_not_called()

    def test_unexpected_delegate_exception_is_sanitized(self) -> None:
        secret = "secret delegate failure"
        with patch.object(
            job_module,
            "_daily_pipeline_main",
            side_effect=RuntimeError(secret),
        ):
            result, stdout, stderr = _run(["--spec-file", "/control/spec.json"])
        self.assertEqual(result, 2)
        self.assertEqual(stdout, "")
        self.assertEqual(
            json.loads(stderr),
            {"status": "FAILED", "error_type": "StateRestoreJobRuntimeError"},
        )
        self.assertNotIn(secret, stderr)

    def test_base_exception_from_delegate_propagates(self) -> None:
        with patch.object(
            job_module,
            "_daily_pipeline_main",
            side_effect=KeyboardInterrupt,
        ):
            with self.assertRaises(KeyboardInterrupt):
                _run(["--spec-file", "/control/spec.json"])

    def test_none_argv_reads_sys_argv_once(self) -> None:
        with (
            patch.object(
                sys,
                "argv",
                ["state-restore-job", "--spec-file", "/control/spec.json"],
            ),
            patch.object(job_module, "_daily_pipeline_main", return_value=0) as delegate,
        ):
            result, stdout, stderr = _run(None)
        self.assertEqual((result, stdout, stderr), (0, "", ""))
        delegate.assert_called_once_with(
            ["restore-pinned-state", "--spec-file", "/control/spec.json"]
        )

    def test_console_script_is_registered(self) -> None:
        pyproject = (_REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
        self.assertIn(
            'india-swing-state-restore = "india_swing.state_restore_job:main"',
            pyproject,
        )


class CapabilityLockTests(unittest.TestCase):
    _EXACT_ALLOWED_IMPORTS = frozenset(
        {
            (0, "__future__", "annotations", None),
            (0, "json", None, None),
            (0, "sys", None, None),
            (0, "typing", "Sequence", None),
            (0, "india_swing.daily_pipeline.cli", "main", "_daily_pipeline_main"),
        }
    )

    def _module_ast(self) -> ast.Module:
        return ast.parse(_MODULE_PATH.read_text(encoding="utf-8"))

    def test_imports_match_exact_wrapper_allowlist(self) -> None:
        actual: set[tuple[int, str, str | None, str | None]] = set()
        for node in ast.walk(self._module_ast()):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    actual.add((0, alias.name, None, alias.asname))
            elif isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    actual.add((node.level or 0, node.module or "", alias.name, alias.asname))
        self.assertEqual(actual, self._EXACT_ALLOWED_IMPORTS)

    def test_wrapper_has_no_file_gcs_environment_broker_or_mutation_capability(self) -> None:
        forbidden = (
            "open",
            "path",
            "storage",
            "google",
            "client",
            "environ",
            "getenv",
            "broker",
            "order",
            "notification",
            "subprocess",
            "mkdir",
            "write",
            "rename",
            "replace",
            "unlink",
        )
        offenders: list[str] = []
        for node in ast.walk(self._module_ast()):
            if isinstance(node, ast.Name):
                candidate = node.id.lower()
            elif isinstance(node, ast.Attribute):
                candidate = node.attr.lower()
            else:
                continue
            if any(token in candidate for token in forbidden):
                offenders.append(candidate)
        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
