from __future__ import annotations

import inspect
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from india_swing.forward_paper import operational_job as job_module
from india_swing.forward_paper.operational_job import (
    ForwardPaperOperationalJobError,
    ForwardPaperOperationalJobRequest,
    NseArchiveForwardPaperHistoryBuilder,
    run_forward_paper_operational_job,
)

from tests.test_forward_paper_operational import _operational_artifacts
from tests.test_forward_paper_operational_gcs import ExactResolver, FakeWriter
from tests.test_nse_archive_research_dataset import _baseline_dataset


class DatasetResolver:
    def __init__(self, dataset) -> None:
        self.dataset = dataset
        self.calls: list[str] = []

    def get(self, dataset_id: str):
        self.calls.append(dataset_id)
        return self.dataset


class ForwardPaperOperationalJobTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temporary = tempfile.TemporaryDirectory()
        cls.raw, cls.actions, cls.ticks, _ = _operational_artifacts(
            Path(cls.temporary.name)
        )
        cls.dataset = _baseline_dataset()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temporary.cleanup()

    def _request(self) -> ForwardPaperOperationalJobRequest:
        spec = self.raw.spec
        return ForwardPaperOperationalJobRequest(
            dataset_id=spec.dataset_id,
            signal_session=spec.signal_session,
            decision_cutoff=spec.decision_cutoff,
            expected_market_sessions=spec.expected_market_sessions,
            corporate_action_snapshot_id=self.actions.snapshot_id,
            tick_panel_id=self.ticks.panel_id,
            bucket="india-swing-test-state",
        )

    def _builder(self):
        datasets = DatasetResolver(self.dataset)
        return NseArchiveForwardPaperHistoryBuilder(
            datasets=datasets,
            reader=object(),
        ), datasets

    def test_builder_reconstructs_exact_window_from_dataset_stream(self) -> None:
        builder, datasets = self._builder()
        with patch.object(
            job_module,
            "iter_nse_archive_research_price_stream_sessions_from",
            return_value=iter(self.raw.sessions),
        ) as stream:
            rebuilt = builder.build(self.raw.spec)
        self.assertEqual(rebuilt.window_id, self.raw.window_id)
        self.assertEqual(datasets.calls, [self.dataset.dataset_id])
        stream.assert_called_once_with(
            self.dataset,
            builder.reader,
            start_session=self.raw.spec.expected_market_sessions[0],
        )

    def test_job_rebuilds_assembles_and_seals_one_manifest(self) -> None:
        builder, _ = self._builder()
        writer = FakeWriter()
        request = self._request()
        with patch.object(
            job_module,
            "iter_nse_archive_research_price_stream_sessions_from",
            return_value=iter(self.raw.sessions),
        ):
            receipt = run_forward_paper_operational_job(
                request=request,
                history_builder=builder,
                corporate_actions=ExactResolver(self.actions),
                tick_panels=ExactResolver(self.ticks),
                writer=writer,
            )
        self.assertEqual(receipt.request.request_id, request.request_id)
        self.assertEqual(receipt.graph.source_window.window_id, self.raw.window_id)
        self.assertEqual(receipt.publication.manifest.graph_id, receipt.graph.graph_id)
        self.assertEqual(len(writer.calls), 1)
        self.assertTrue(receipt.collection_only)
        self.assertFalse(receipt.paper_trade_eligible)
        self.assertFalse(receipt.notification_eligible)
        self.assertFalse(receipt.execution_eligible)
        receipt.verify_content_identity()

    def test_job_reports_ordered_sanitized_stage_events(self) -> None:
        builder, _ = self._builder()
        events = []
        with patch.object(
            job_module,
            "iter_nse_archive_research_price_stream_sessions_from",
            return_value=iter(self.raw.sessions),
        ):
            run_forward_paper_operational_job(
                request=self._request(),
                history_builder=builder,
                corporate_actions=ExactResolver(self.actions),
                tick_panels=ExactResolver(self.ticks),
                writer=FakeWriter(),
                stage_observer=lambda stage, status, details: events.append(
                    (stage, status, dict(details))
                ),
            )

        self.assertEqual(
            [(stage, status) for stage, status, _details in events],
            [
                ("history_reconstruction", "started"),
                ("history_reconstruction", "completed"),
                ("evidence_resolution", "started"),
                ("evidence_resolution", "completed"),
                ("graph_assembly", "started"),
                ("adjustment_derivation", "started"),
                ("adjustment_derivation", "completed"),
                ("feature_input_derivation", "started"),
                ("feature_input_derivation", "completed"),
                ("technical_feature_derivation", "started"),
                ("technical_feature_derivation", "completed"),
                ("graph_assembly", "completed"),
                ("publication", "started"),
                ("publication", "completed"),
            ],
        )
        for _stage, _status, details in events:
            self.assertTrue(all(type(value) is int and value >= 0 for value in details.values()))

    def test_wrong_exact_artifact_fails_before_publication(self) -> None:
        builder, _ = self._builder()
        writer = FakeWriter()
        with patch.object(
            job_module,
            "iter_nse_archive_research_price_stream_sessions_from",
            return_value=iter(self.raw.sessions),
        ):
            with self.assertRaises(ForwardPaperOperationalJobError):
                run_forward_paper_operational_job(
                    request=self._request(),
                    history_builder=builder,
                    corporate_actions=ExactResolver(object()),
                    tick_panels=ExactResolver(self.ticks),
                    writer=writer,
                )
        self.assertEqual(writer.calls, [])

    def test_request_identity_binds_all_exact_inputs(self) -> None:
        first = self._request()
        second = self._request()
        self.assertEqual(first.request_id, second.request_id)
        self.assertEqual(first.history_spec.spec_id, self.raw.spec.spec_id)
        first.verify_content_identity()

    def test_module_has_no_clock_environment_listing_broker_or_notification_capability(self) -> None:
        source = inspect.getsource(job_module).lower()
        for token in (
            "datetime.now(",
            "os.environ",
            "list_blobs",
            "latest",
            "place_order",
            "telegram",
            "send_alert",
            "kronos",
        ):
            self.assertNotIn(token, source)


if __name__ == "__main__":
    unittest.main()
