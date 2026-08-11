from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

from india_swing.forward_paper.hydrated_cloud_control import (
    decode_forward_paper_hydrated_cloud_launch,
)
from india_swing.forward_paper.hydrated_launch_cli import main
from india_swing.promoted_operational_hydrated_cloud_control import (
    encode_promoted_operational_hydrated_cloud_launch,
)

from tests.test_forward_paper_hydrated_cloud_control import _launch


class ForwardPaperHydratedLaunchCLITests(unittest.TestCase):
    def _argv(self, root: Path) -> tuple[list[str], Path]:
        launch = _launch()
        promoted_file = root / "promoted-launch.json"
        promoted_file.write_bytes(
            encode_promoted_operational_hydrated_cloud_launch(
                launch.promoted_input_launch
            )
        )
        output_file = root / "forward-launch.json"
        request = launch.dataset_request
        return [
            "--promoted-launch-file", str(promoted_file),
            "--output-launch-file", str(output_file),
            "--dataset-bucket", request.bucket,
            "--dataset-id", request.dataset_id,
            "--dataset-generation", str(request.generation),
            "--dataset-sha256", request.expected_sha256,
            "--decision-cutoff", launch.decision_cutoff.isoformat(),
            "--expected-market-sessions", ";".join(
                item.isoformat() for item in launch.expected_market_sessions
            ),
            "--corporate-action-snapshot-id", launch.corporate_action_snapshot_id,
            "--tick-panel-id", launch.tick_panel_id,
            "--output-bucket", launch.output_bucket,
        ], output_file

    def test_writes_one_exact_launch_and_reports_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            argv, output = self._argv(Path(directory).resolve())
            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                code = main(argv)
            self.assertEqual(code, 0)
            self.assertEqual(stderr.getvalue(), "")
            restored = decode_forward_paper_hydrated_cloud_launch(output.read_bytes())
            result = json.loads(stdout.getvalue())
            self.assertEqual(result["launch_id"], restored.launch_id)
            self.assertTrue(result["collection_only"])

    def test_existing_output_is_never_overwritten(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            argv, output = self._argv(Path(directory).resolve())
            output.write_bytes(b"keep-me")
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                code = main(argv)
            self.assertEqual(code, 2)
            self.assertEqual(output.read_bytes(), b"keep-me")

    def test_missing_inputs_fail_with_sanitized_json(self) -> None:
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            code = main([])
        self.assertEqual(code, 2)
        self.assertEqual(
            json.loads(stderr.getvalue()),
            {"error_type": "ForwardPaperHydratedLaunchCLIError", "status": "FAILED"},
        )


if __name__ == "__main__":
    unittest.main()
