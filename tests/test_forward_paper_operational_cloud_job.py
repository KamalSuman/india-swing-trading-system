from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace

from india_swing.forward_paper.operational_cloud_job import (
    ForwardPaperOperationalCloudRuntime,
    _default_runtime_factory,
    main,
)
from india_swing.forward_paper.signal_tick import ExactForwardPaperTickPanelResolver
from india_swing.evaluation.nse_archive_research_identity_checkpoint import (
    NseArchiveResearchIdentityCheckpoint,
)
from india_swing.forward_paper.operational_job import (
    NseArchiveForwardPaperHistoryBuilder,
)

from tests.test_forward_paper_operational import _operational_artifacts
from tests.test_forward_paper_operational_gcs import ExactResolver, FakeWriter
from tests.test_forward_paper_operational_job import DatasetResolver
from tests.test_nse_archive_research_dataset import _baseline_dataset


class ForwardPaperOperationalCloudJobTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temporary = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temporary.name)
        cls.raw, cls.actions, cls.ticks, _ = _operational_artifacts(cls.root)
        cls.dataset = _baseline_dataset()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temporary.cleanup()

    def _argv(self) -> list[str]:
        paths = []
        for name in (
            "market-data-root",
            "reference-root",
            "identity-evidence-root",
            "calendar-root",
            "daily-reports-root",
            "historical-corpus-root",
            "promoted-root",
            "engine-run-root",
        ):
            paths.extend((f"--{name}", str(self.root / name)))
        spec = self.raw.spec
        return paths + [
            "--dataset-id",
            spec.dataset_id,
            "--dataset-bucket",
            "india-swing-research-data",
            "--dataset-generation",
            "1786469190290325",
            "--dataset-sha256",
            "a" * 64,
            "--signal-session",
            spec.signal_session.isoformat(),
            "--decision-cutoff",
            spec.decision_cutoff.isoformat(),
            "--expected-market-sessions",
            ";".join(value.isoformat() for value in spec.expected_market_sessions),
            "--corporate-action-snapshot-id",
            self.actions.snapshot_id,
            "--tick-panel-id",
            self.ticks.panel_id,
            "--bucket",
            "india-swing-test-state",
        ]

    def _runtime(self) -> ForwardPaperOperationalCloudRuntime:
        return ForwardPaperOperationalCloudRuntime(
            history_builder=NseArchiveForwardPaperHistoryBuilder(
                datasets=DatasetResolver(self.dataset),
                reader=object(),
            ),
            corporate_actions=ExactResolver(self.actions),
            tick_panels=ExactResolver(self.ticks),
        )

    def test_main_runs_exact_job_and_emits_one_canonical_envelope(self) -> None:
        writer = FakeWriter()
        stdout = io.StringIO()
        stderr = io.StringIO()
        with patch(
            "india_swing.forward_paper.operational_job."
            "iter_nse_archive_research_price_stream_sessions_from",
            return_value=iter(self.raw.sessions),
        ), redirect_stdout(stdout), redirect_stderr(stderr):
            code = main(
                self._argv(),
                runtime_factory=lambda _arguments: self._runtime(),
                writer_factory=lambda: writer,
            )
        self.assertEqual(code, 0)
        self.assertEqual(stderr.getvalue(), "")
        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        payload = json.loads(lines[0])
        self.assertEqual(payload["status"], "FORWARD_PAPER_OPERATIONAL_GRAPH_PUBLISHED")
        self.assertTrue(payload["collection_only"])
        self.assertFalse(payload["paper_trade_eligible"])
        self.assertFalse(payload["notification_eligible"])
        self.assertFalse(payload["execution_eligible"])
        self.assertEqual(payload["signal_session"], self.raw.spec.signal_session.isoformat())
        self.assertEqual(len(writer.calls), 1)

    def test_invalid_or_incomplete_arguments_fail_with_sanitized_json(self) -> None:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            code = main([])
        self.assertEqual(code, 2)
        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(
            json.loads(stderr.getvalue()),
            {
                "error_type": "ForwardPaperOperationalCloudJobError",
                "status": "FAILED",
            },
        )

    def test_relative_hydration_root_is_rejected_before_runtime_factory(self) -> None:
        argv = self._argv()
        argv[1] = "relative-root"
        calls = []
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            code = main(argv, runtime_factory=lambda value: calls.append(value))
        self.assertEqual(code, 2)
        self.assertEqual(calls, [])

    def test_default_runtime_uses_signal_first_exact_tick_resolver(self) -> None:
        arguments = type("Arguments", (), {})()
        values = self._argv()
        parser_values = {}
        for index in range(0, len(values), 2):
            parser_values[values[index][2:].replace("-", "_")] = values[index + 1]
        for name, value in parser_values.items():
            if name.endswith("_root"):
                value = Path(value)
            elif name == "dataset_generation":
                value = int(value)
            setattr(arguments, name, value)
        legacy = object()
        with patch(
            "india_swing.forward_paper.operational_cloud_job."
            "read_pinned_nse_archive_research_dataset",
            return_value=self.dataset,
        ), patch(
            "india_swing.forward_paper.operational_cloud_job."
            "build_promoted_engine_stores",
            return_value=SimpleNamespace(effective_session_ticks=legacy),
        ):
            runtime = _default_runtime_factory(arguments, dataset_reader=object())
        self.assertIsInstance(runtime.tick_panels, ExactForwardPaperTickPanelResolver)
        self.assertIs(runtime.tick_panels.promoted_ticks, legacy)
        self.assertEqual(
            runtime.tick_panels.signal_ticks.root,
            arguments.promoted_root / "forward-paper-signal-ticks",
        )

    def test_partial_checkpoint_pin_is_rejected_without_a_gcs_read(self) -> None:
        argv = self._argv() + ["--identity-checkpoint-id", "b" * 64]
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            code = main(argv, reader_factory=lambda: object())
        self.assertEqual(code, 2)
        self.assertEqual(stdout.getvalue(), "")
        lines = stderr.getvalue().splitlines()
        self.assertEqual(
            json.loads(lines[-1]),
            {
                "error_type": "ForwardPaperOperationalCloudJobError",
                "status": "FAILED",
            },
        )

    def test_default_runtime_loads_one_complete_exact_checkpoint_pin(self) -> None:
        arguments = type("Arguments", (), {})()
        values = self._argv()
        parser_values = {}
        for index in range(0, len(values), 2):
            parser_values[values[index][2:].replace("-", "_")] = values[index + 1]
        for name, value in parser_values.items():
            if name.endswith("_root"):
                value = Path(value)
            elif name == "dataset_generation":
                value = int(value)
            setattr(arguments, name, value)
        arguments.identity_checkpoint_id = "b" * 64
        arguments.identity_checkpoint_bucket = "india-swing-research-data"
        arguments.identity_checkpoint_generation = 19
        arguments.identity_checkpoint_sha256 = "c" * 64
        checkpoint = object.__new__(NseArchiveResearchIdentityCheckpoint)
        with patch(
            "india_swing.forward_paper.operational_cloud_job."
            "read_pinned_nse_archive_research_dataset",
            return_value=self.dataset,
        ), patch(
            "india_swing.forward_paper.operational_cloud_job."
            "read_pinned_nse_archive_research_identity_checkpoint",
            return_value=checkpoint,
        ) as read_checkpoint, patch(
            "india_swing.forward_paper.operational_cloud_job."
            "verify_nse_archive_research_identity_checkpoint_for_dataset",
        ) as verify_checkpoint, patch(
            "india_swing.forward_paper.operational_cloud_job."
            "build_promoted_engine_stores",
            return_value=SimpleNamespace(effective_session_ticks=object()),
        ):
            runtime = _default_runtime_factory(arguments, dataset_reader=object())
        self.assertIs(runtime.history_builder.identity_checkpoint, checkpoint)
        self.assertEqual(read_checkpoint.call_count, 1)
        verify_checkpoint.assert_called_once_with(checkpoint, self.dataset)


if __name__ == "__main__":
    unittest.main()
