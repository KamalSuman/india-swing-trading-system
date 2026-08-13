from __future__ import annotations

import io
import inspect
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from india_swing.forward_paper.operational import (
    assemble_forward_paper_operational_research_graph,
)
from india_swing.forward_paper.operational_cloud_job import (
    ForwardPaperOperationalCloudRuntime,
)
from india_swing.forward_paper.operational_gcs import (
    publish_forward_paper_operational_graph,
)
from india_swing.forward_paper.operational_job import (
    NseArchiveForwardPaperHistoryBuilder,
)
from india_swing.forward_paper.research_cloud_job import (
    FORWARD_PAPER_RESEARCH_DESIGN,
    ForwardPaperResearchCloudRuntime,
    forward_paper_research_design_configs,
    main,
)
from india_swing.forward_paper import research_cloud_job as cloud_module

from tests.test_forward_paper_operational import _operational_artifacts
from tests.test_forward_paper_operational_gcs import (
    ExactResolver,
    FakeReader,
    FakeWriter,
)
from tests.test_forward_paper_operational_job import DatasetResolver
from tests.test_nse_archive_research_dataset import _baseline_dataset


class ForwardPaperResearchCloudJobTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temporary = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temporary.name)
        cls.raw, cls.actions, cls.ticks, _ = _operational_artifacts(cls.root)
        cls.graph = assemble_forward_paper_operational_research_graph(
            source_window=cls.raw,
            corporate_actions=cls.actions,
            tick_panel=cls.ticks,
        )
        cls.dataset = _baseline_dataset()
        cls.bucket = "india-swing-test-state"

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temporary.cleanup()

    def _source(self, writer: FakeWriter):
        return publish_forward_paper_operational_graph(
            graph=self.graph,
            bucket=self.bucket,
            writer=writer,
        )

    def _argv(self, source) -> list[str]:
        values: list[str] = []
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
            values.extend((f"--{name}", str(self.root / name)))
        baseline, challenger = forward_paper_research_design_configs(
            FORWARD_PAPER_RESEARCH_DESIGN
        )
        return values + [
            "--dataset-id",
            self.raw.spec.dataset_id,
            "--dataset-bucket",
            "india-swing-research-data",
            "--dataset-generation",
            "1786469190290325",
            "--dataset-sha256",
            "a" * 64,
            "--source-bucket",
            self.bucket,
            "--source-graph-id",
            self.graph.graph_id,
            "--source-manifest-object-name",
            source.manifest_object.object_name,
            "--source-manifest-generation",
            str(source.manifest_object.generation),
            "--source-manifest-sha256",
            source.manifest_object.sha256,
            "--output-bucket",
            self.bucket,
            "--research-design",
            FORWARD_PAPER_RESEARCH_DESIGN,
            "--baseline-config-id",
            baseline.config_id,
            "--challenger-config-id",
            challenger.config_id,
            "--comparison-top-tiers",
            "10",
        ]

    def _runtime(self) -> ForwardPaperResearchCloudRuntime:
        return ForwardPaperResearchCloudRuntime(
            operational=ForwardPaperOperationalCloudRuntime(
                history_builder=NseArchiveForwardPaperHistoryBuilder(
                    datasets=DatasetResolver(self.dataset),
                    reader=object(),
                ),
                corporate_actions=ExactResolver(self.actions),
                tick_panels=ExactResolver(self.ticks),
            )
        )

    def test_main_emits_one_canonical_non_authoritative_receipt(self) -> None:
        writer = FakeWriter()
        source = self._source(writer)
        stdout = io.StringIO()
        stderr = io.StringIO()
        with patch(
            "india_swing.forward_paper.operational_job."
            "iter_nse_archive_research_price_stream_sessions_from",
            return_value=iter(self.raw.sessions),
        ), redirect_stdout(stdout), redirect_stderr(stderr):
            code = main(
                self._argv(source),
                runtime_factory=lambda _arguments: self._runtime(),
                reader_factory=lambda: FakeReader(writer),
                writer_factory=lambda: writer,
            )
        self.assertEqual(code, 0)
        self.assertEqual(stderr.getvalue(), "")
        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        payload = json.loads(lines[0])
        self.assertEqual(
            json.dumps(payload, separators=(",", ":"), sort_keys=True), lines[0]
        )
        self.assertEqual(payload["status"], "FORWARD_PAPER_RESEARCH_RUN_PUBLISHED")
        self.assertEqual(payload["source_graph_id"], self.graph.graph_id)
        self.assertTrue(payload["collection_only"])
        for key in (
            "promotion_eligible",
            "paper_trade_eligible",
            "notification_eligible",
            "execution_eligible",
        ):
            self.assertFalse(payload[key])
        self.assertEqual(len(writer.calls), 2)

    def test_config_identity_mismatch_stops_before_any_factory(self) -> None:
        writer = FakeWriter()
        source = self._source(writer)
        argv = self._argv(source)
        argv[argv.index("--challenger-config-id") + 1] = "0" * 64
        calls: list[str] = []
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            code = main(
                argv,
                runtime_factory=lambda _arguments: calls.append("runtime"),
                reader_factory=lambda: calls.append("reader"),
                writer_factory=lambda: calls.append("writer"),
            )
        self.assertEqual(code, 2)
        self.assertEqual(calls, [])
        self.assertEqual(
            json.loads(stderr.getvalue()),
            {
                "error_type": "ForwardPaperResearchCloudJobError",
                "status": "FAILED",
            },
        )

    def test_invalid_arguments_fail_with_sanitized_json(self) -> None:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            code = main([])
        self.assertEqual(code, 2)
        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(
            json.loads(stderr.getvalue()),
            {
                "error_type": "ForwardPaperResearchCloudJobError",
                "status": "FAILED",
            },
        )

    def test_relative_root_is_rejected_before_factories(self) -> None:
        writer = FakeWriter()
        source = self._source(writer)
        argv = self._argv(source)
        argv[1] = "relative-root"
        calls: list[str] = []
        with redirect_stderr(io.StringIO()):
            code = main(
                argv,
                runtime_factory=lambda _arguments: calls.append("runtime"),
                reader_factory=lambda: calls.append("reader"),
                writer_factory=lambda: calls.append("writer"),
            )
        self.assertEqual(code, 2)
        self.assertEqual(calls, [])

    def test_design_changes_only_the_declared_high_volatility_threshold(self) -> None:
        baseline, challenger = forward_paper_research_design_configs(
            FORWARD_PAPER_RESEARCH_DESIGN
        )
        left = baseline._identity()
        right = challenger._identity()
        self.assertEqual(
            {key for key in left if left[key] != right[key]},
            {"high_volatility_threshold"},
        )
        self.assertEqual(right["high_volatility_threshold"], challenger.high_volatility_threshold)

    def test_cloud_entry_has_no_broker_notification_or_latest_selection(self) -> None:
        source = inspect.getsource(cloud_module).lower()
        for token in (
            "list_blobs",
            "place_order",
            "kiteconnect",
            "telegram",
            "send_alert",
        ):
            self.assertNotIn(token, source)


if __name__ == "__main__":
    unittest.main()
