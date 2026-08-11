from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import timedelta, timezone, datetime
from pathlib import Path

from india_swing.evaluation.nse_archive_research_dataset_gcs import (
    PinnedNseArchiveResearchDatasetRequest,
)
from india_swing.forward_paper.hydrated_cloud_control import (
    ForwardPaperHydratedCloudLaunch,
    encode_forward_paper_hydrated_cloud_launch,
)
from india_swing.forward_paper.hydrated_cloud_job import main

from tests.test_promoted_operational_hydrated_cloud_job import _Fixture


class FakeInnerJob:
    def __init__(self, signal_session: str, *, exit_code: int = 0) -> None:
        self.signal_session = signal_session
        self.exit_code = exit_code
        self.calls: list[tuple[list[str], dict[str, object]]] = []

    def __call__(self, argv: list[str], **kwargs: object) -> int:
        self.calls.append((list(argv), dict(kwargs)))
        if self.exit_code != 0:
            print('{"status":"FAILED"}', file=__import__("sys").stderr)
            return self.exit_code
        graph_id = "3" * 64
        manifest_id = "4" * 64
        value = {
            "blocked_feature_count": 2,
            "collection_only": True,
            "computed_feature_count": 50,
            "execution_eligible": False,
            "graph_id": graph_id,
            "manifest_generation": 91,
            "manifest_object_name": (
                "research/forward-paper-operational/v1/"
                f"{self.signal_session}/{graph_id}/{manifest_id}.json"
            ),
            "manifest_sha256": "5" * 64,
            "notification_eligible": False,
            "paper_trade_eligible": False,
            "receipt_id": "6" * 64,
            "request_id": "7" * 64,
            "signal_session": self.signal_session,
            "status": "FORWARD_PAPER_OPERATIONAL_GRAPH_PUBLISHED",
        }
        print(json.dumps(value, separators=(",", ":"), sort_keys=True))
        return 0


def _forward_launch(fixture: _Fixture) -> ForwardPaperHydratedCloudLaunch:
    promoted = fixture.launch()
    signal = promoted.target_session
    sessions = tuple(signal - timedelta(days=value) for value in range(59, -1, -1))
    return ForwardPaperHydratedCloudLaunch(
        promoted_input_launch=promoted,
        dataset_request=PinnedNseArchiveResearchDatasetRequest(
            bucket="india-swing-data",
            dataset_id="8" * 64,
            generation=1786469190290325,
            expected_sha256="9" * 64,
        ),
        decision_cutoff=datetime.combine(
            signal, datetime.min.time(), tzinfo=timezone.utc
        ),
        expected_market_sessions=sessions,
        corporate_action_snapshot_id="a" * 64,
        tick_panel_id="b" * 64,
        output_bucket=promoted.state_bucket,
    )


class ForwardPaperHydratedCloudJobTests(unittest.TestCase):
    def test_hydrates_promoted_inputs_and_invokes_inner_once(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            fixture = _Fixture(root / "source")
            launch = _forward_launch(fixture)
            launch_file = root / "forward-launch.json"
            launch_file.write_bytes(encode_forward_paper_hydrated_cloud_launch(launch))
            runtime = root / "runtime"
            runtime.mkdir()
            market = root / "mounted-market-data"
            market.mkdir()
            inner = FakeInnerJob(launch.signal_session.isoformat())
            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                code = main(
                    ["--launch-file", str(launch_file), "--market-data-root", str(market)],
                    runtime_parent=runtime,
                    gcs_client_factory=lambda: fixture.client,
                    inner_job_main=inner,
                )
            self.assertEqual(code, 0)
            self.assertEqual(stderr.getvalue(), "")
            self.assertEqual(len(inner.calls), 1)
            result = json.loads(stdout.getvalue())
            self.assertEqual(result["status"], "FORWARD_PAPER_HYDRATED_CLOUD_JOB_COMPLETE")
            self.assertEqual(result["launch_id"], launch.launch_id)
            self.assertEqual(
                result["input_snapshot_id"],
                launch.promoted_input_launch.input_restore.expected_snapshot_id,
            )
            argv, kwargs = inner.calls[0]
            self.assertIn(str(market), argv)
            self.assertEqual(argv[argv.index("--dataset-generation") + 1], str(launch.dataset_request.generation))
            self.assertIs(kwargs["reader_factory"]()._client, fixture.client)
            self.assertIs(kwargs["writer_factory"]()._client, fixture.client)
            self.assertTrue((runtime / "promoted_root").is_dir())
            self.assertFalse((runtime / "state").exists())

    def test_inner_failure_is_not_forwarded(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            fixture = _Fixture(root / "source")
            launch = _forward_launch(fixture)
            launch_file = root / "forward-launch.json"
            launch_file.write_bytes(encode_forward_paper_hydrated_cloud_launch(launch))
            runtime = root / "runtime"
            runtime.mkdir()
            market = root / "market"
            market.mkdir()
            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                code = main(
                    ["--launch-file", str(launch_file), "--market-data-root", str(market)],
                    runtime_parent=runtime,
                    gcs_client_factory=lambda: fixture.client,
                    inner_job_main=FakeInnerJob(
                        launch.signal_session.isoformat(), exit_code=2
                    ),
                )
            self.assertEqual(code, 2)
            self.assertEqual(stdout.getvalue(), "")
            self.assertEqual(
                json.loads(stderr.getvalue()),
                {"error_type": "ForwardPaperHydratedCloudJobError", "status": "FAILED"},
            )


if __name__ == "__main__":
    unittest.main()
