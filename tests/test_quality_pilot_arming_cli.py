from __future__ import annotations

import ast
import inspect
import json
import os
import tempfile
import unittest
from pathlib import Path

import india_swing.quality_pilot_arming_cli as cli_module
from india_swing.quality_pilot.arming import encode_quality_pilot_arming_manifest, encode_quality_pilot_runbook_draft
from india_swing.quality_pilot.invocation_control_plane import decode_quality_pilot_invocation_runbook
from tests.test_quality_pilot_arming import _draft, _manifest, _runbook


class CompileRunbookTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.draft_path = os.path.join(self.tmpdir, "draft.json")
        self.output_path = os.path.join(self.tmpdir, "runbook.json")
        draft = _draft()
        with open(self.draft_path, "wb") as handle:
            handle.write(encode_quality_pilot_runbook_draft(draft))

    def test_compile_runbook_creates_a_new_file(self) -> None:
        rc = cli_module.main(["compile-runbook", "--draft-file", self.draft_path, "--output-file", self.output_path])
        self.assertEqual(rc, 0)
        self.assertTrue(os.path.exists(self.output_path))
        with open(self.output_path, "rb") as handle:
            runbook = decode_quality_pilot_invocation_runbook(handle.read())
        self.assertEqual(len(runbook.windows), 80)

    def test_compile_runbook_never_overwrites_an_existing_output(self) -> None:
        rc1 = cli_module.main(["compile-runbook", "--draft-file", self.draft_path, "--output-file", self.output_path])
        self.assertEqual(rc1, 0)
        with open(self.output_path, "rb") as handle:
            original_bytes = handle.read()
        rc2 = cli_module.main(["compile-runbook", "--draft-file", self.draft_path, "--output-file", self.output_path])
        self.assertEqual(rc2, 2)
        with open(self.output_path, "rb") as handle:
            self.assertEqual(handle.read(), original_bytes)

    def test_compile_runbook_rejects_relative_paths(self) -> None:
        rc = cli_module.main(["compile-runbook", "--draft-file", "draft.json", "--output-file", self.output_path])
        self.assertEqual(rc, 2)

    def test_compile_runbook_rejects_traversal_paths(self) -> None:
        traversal = os.path.join(self.tmpdir, "..", "escape.json")
        rc = cli_module.main(["compile-runbook", "--draft-file", self.draft_path, "--output-file", traversal])
        self.assertEqual(rc, 2)

    def test_compile_runbook_rejects_missing_option(self) -> None:
        rc = cli_module.main(["compile-runbook", "--draft-file", self.draft_path])
        self.assertEqual(rc, 2)

    def test_compile_runbook_rejects_duplicate_option(self) -> None:
        rc = cli_module.main(
            [
                "compile-runbook", "--draft-file", self.draft_path, "--draft-file", self.draft_path,
                "--output-file", self.output_path,
            ]
        )
        self.assertEqual(rc, 2)

    def test_compile_runbook_rejects_malformed_draft(self) -> None:
        bad_draft_path = os.path.join(self.tmpdir, "bad-draft.json")
        with open(bad_draft_path, "wb") as handle:
            handle.write(b"not json\n")
        rc = cli_module.main(["compile-runbook", "--draft-file", bad_draft_path, "--output-file", self.output_path])
        self.assertEqual(rc, 2)
        self.assertFalse(os.path.exists(self.output_path))

    def test_compile_runbook_emits_sanitized_stdout_envelope(self) -> None:
        import contextlib
        import io

        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            rc = cli_module.main(["compile-runbook", "--draft-file", self.draft_path, "--output-file", self.output_path])
        self.assertEqual(rc, 0)
        envelope = json.loads(buffer.getvalue())
        self.assertEqual(envelope["status"], "QUALITY_PILOT_RUNBOOK_COMPILED")
        self.assertTrue(envelope["quality_only"])
        self.assertNotIn("draft_file", envelope)
        self.assertNotIn(self.tmpdir, json.dumps(envelope))


class InspectPlanTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.runbook, encoded = _runbook()
        self.runbook_path = os.path.join(self.tmpdir, "runbook.json")
        with open(self.runbook_path, "wb") as handle:
            handle.write(encoded)
        self.manifest = _manifest(self.runbook)
        self.manifest_path = os.path.join(self.tmpdir, "manifest.json")
        with open(self.manifest_path, "wb") as handle:
            handle.write(encode_quality_pilot_arming_manifest(self.manifest))

    def test_inspect_plan_emits_compact_sanitized_envelope(self) -> None:
        import contextlib
        import io

        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            rc = cli_module.main(
                ["inspect-plan", "--runbook-file", self.runbook_path, "--manifest-file", self.manifest_path]
            )
        self.assertEqual(rc, 0)
        envelope = json.loads(buffer.getvalue())
        self.assertEqual(envelope["status"], "QUALITY_PILOT_PLAN_INSPECTED")
        self.assertEqual(envelope["manifest_id"], self.manifest.manifest_id)
        self.assertEqual(len(envelope["scheduler_job_names"]), 4)
        self.assertFalse(envelope["armed"])
        self.assertTrue(envelope["quality_only"])
        rendered = json.dumps(envelope)
        for secret_fragment in ("kite-api-key", "kite-access-token"):
            self.assertNotIn(secret_fragment, rendered)

    def test_inspect_plan_rejects_manifest_disagreeing_with_runbook(self) -> None:
        other_runbook, other_encoded = _runbook(bucket="a-different-quality-pilot-bucket")
        other_runbook_path = os.path.join(self.tmpdir, "other-runbook.json")
        with open(other_runbook_path, "wb") as handle:
            handle.write(other_encoded)
        rc = cli_module.main(
            ["inspect-plan", "--runbook-file", other_runbook_path, "--manifest-file", self.manifest_path]
        )
        self.assertEqual(rc, 2)

    def test_inspect_plan_rejects_missing_files(self) -> None:
        missing_path = os.path.join(self.tmpdir, "does-not-exist.json")
        rc = cli_module.main(["inspect-plan", "--runbook-file", missing_path, "--manifest-file", self.manifest_path])
        self.assertEqual(rc, 2)

    def test_inspect_plan_rejects_unknown_command(self) -> None:
        rc = cli_module.main(["not-a-command"])
        self.assertEqual(rc, 2)

    def test_no_command_fails_closed(self) -> None:
        rc = cli_module.main([])
        self.assertEqual(rc, 2)


class RegressionAndCapabilityTests(unittest.TestCase):
    def test_module_never_accesses_credentials_gcp_kite_or_network(self) -> None:
        source = inspect.getsource(cli_module)
        tree = ast.parse(source)
        forbidden_modules = {
            "socket", "subprocess", "requests", "urllib", "httpx", "google", "kiteconnect", "time",
        }
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0])
        self.assertEqual(imported & forbidden_modules, set())
        lowered = source.lower()
        for token in ("kitecredentials", "storage.client(", "webbrowser.", "gcloud", "subprocess."):
            self.assertNotIn(token, lowered, msg=token)

    def test_module_uses_exclusive_create_for_output_files(self) -> None:
        source = inspect.getsource(cli_module)
        self.assertIn("O_EXCL", source)
        self.assertIn("O_CREAT", source)


if __name__ == "__main__":
    unittest.main()
