from __future__ import annotations

import inspect
import tempfile
import unittest
from dataclasses import fields
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from india_swing.daily_pipeline.state_publication import PublishedStateObject
from india_swing.features.promoted_cross_section import PromotedCrossSectionConfig
from india_swing.forward_paper import research_gcs as gcs_module
from india_swing.forward_paper import research_job as job_module
from india_swing.forward_paper.operational import (
    ForwardPaperOperationalResearchGraph,
    assemble_forward_paper_operational_research_graph,
)
from india_swing.forward_paper.operational_gcs import (
    publish_forward_paper_operational_graph,
)
from india_swing.forward_paper.research_gcs import (
    FORWARD_PAPER_RESEARCH_MANIFEST_MAXIMUM_BYTES,
    ForwardPaperOperationalManifestPin,
    ForwardPaperResearchManifestError,
    ForwardPaperResearchRunManifest,
    decode_forward_paper_research_manifest,
    encode_forward_paper_research_manifest,
    forward_paper_research_manifest_object_name,
    restore_forward_paper_research_run,
)
from india_swing.forward_paper.research_job import (
    ForwardPaperResearchJobError,
    ForwardPaperResearchJobRequest,
    run_forward_paper_research_job,
)

from tests.test_forward_paper_operational import _operational_artifacts
from tests.test_forward_paper_operational_gcs import (
    ExactResolver,
    FakeReader,
    FakeWriter,
)


class ForwardPaperResearchGCSTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temporary = tempfile.TemporaryDirectory()
        cls.raw, cls.actions, cls.ticks, _ = _operational_artifacts(
            Path(cls.temporary.name)
        )
        cls.graph = assemble_forward_paper_operational_research_graph(
            source_window=cls.raw,
            corporate_actions=cls.actions,
            tick_panel=cls.ticks,
        )
        cls.bucket = "india-swing-test-state"
        cls.baseline = PromotedCrossSectionConfig(minimum_computed_instruments=1)
        cls.challenger = PromotedCrossSectionConfig(
            minimum_computed_instruments=1,
            high_volatility_threshold=Decimal("0.40"),
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temporary.cleanup()

    def _request(self, writer: FakeWriter):
        source = publish_forward_paper_operational_graph(
            graph=self.graph,
            bucket=self.bucket,
            writer=writer,
        )
        pin = ForwardPaperOperationalManifestPin(
            bucket=self.bucket,
            expected_graph_id=self.graph.graph_id,
            object_name=source.manifest_object.object_name,
            generation=source.manifest_object.generation,
            sha256=source.manifest_object.sha256,
        )
        return ForwardPaperResearchJobRequest(
            source_pin=pin,
            baseline_config=self.baseline,
            challenger_config=self.challenger,
            comparison_top_tiers=10,
            output_bucket=self.bucket,
        )

    def _run(self):
        writer = FakeWriter()
        request = self._request(writer)
        stages: list[tuple[str, str, dict[str, int]]] = []
        receipt = run_forward_paper_research_job(
            request=request,
            reader=FakeReader(writer),
            history_windows=ExactResolver(self.raw),
            corporate_actions=ExactResolver(self.actions),
            tick_panels=ExactResolver(self.ticks),
            writer=writer,
            stage_observer=lambda stage, status, details: stages.append(
                (stage, status, dict(details))
            ),
        )
        return writer, request, receipt, stages

    def test_job_restores_exact_graph_runs_both_arms_and_publishes_manifest(self) -> None:
        writer, request, receipt, stages = self._run()
        self.assertEqual(receipt.run.source_graph.graph_id, self.graph.graph_id)
        self.assertEqual(receipt.publication.manifest.source_pin, request.source_pin)
        self.assertEqual(receipt.publication.manifest.run_id, receipt.run.run_id)
        self.assertEqual(len(writer.calls), 2)
        call = writer.calls[-1]
        self.assertEqual(call["content_type"], "application/json")
        self.assertEqual(
            call["maximum_bytes"], FORWARD_PAPER_RESEARCH_MANIFEST_MAXIMUM_BYTES
        )
        self.assertEqual(
            call["object_name"],
            forward_paper_research_manifest_object_name(receipt.publication.manifest),
        )
        self.assertEqual(
            decode_forward_paper_research_manifest(call["content_bytes"]),
            receipt.publication.manifest,
        )
        self.assertEqual(
            [(stage, status) for stage, status, _ in stages],
            [
                ("operational_graph_restore", "started"),
                ("adjustment_derivation", "started"),
                ("adjustment_derivation", "completed"),
                ("feature_input_derivation", "started"),
                ("feature_input_derivation", "completed"),
                ("technical_feature_derivation", "started"),
                ("technical_feature_derivation", "completed"),
                ("operational_graph_restore", "completed"),
                ("baseline_challenger", "started"),
                ("baseline_challenger", "completed"),
                ("research_publication", "started"),
                ("research_publication", "completed"),
            ],
        )
        receipt.verify_content_identity()

    def test_job_does_not_repeat_recursive_graph_verification(self) -> None:
        writer = FakeWriter()
        request = self._request(writer)
        with patch.object(
            ForwardPaperOperationalResearchGraph,
            "verify_content_identity",
            side_effect=AssertionError("recursive verification must not repeat"),
        ):
            receipt = run_forward_paper_research_job(
                request=request,
                reader=FakeReader(writer),
                history_windows=ExactResolver(self.raw),
                corporate_actions=ExactResolver(self.actions),
                tick_panels=ExactResolver(self.ticks),
                writer=writer,
            )
        self.assertEqual(receipt.run.source_graph.graph_id, self.graph.graph_id)

    def test_research_restore_reads_exact_research_and_source_generations(self) -> None:
        writer, _, receipt, _ = self._run()
        reader = FakeReader(writer)
        restored = restore_forward_paper_research_run(
            expected_run_id=receipt.run.run_id,
            bucket=self.bucket,
            manifest_object_name=receipt.publication.manifest_object.object_name,
            manifest_generation=receipt.publication.manifest_object.generation,
            manifest_sha256=receipt.publication.manifest_object.sha256,
            baseline_config=self.baseline,
            challenger_config=self.challenger,
            reader=reader,
            history_windows=ExactResolver(self.raw),
            corporate_actions=ExactResolver(self.actions),
            tick_panels=ExactResolver(self.ticks),
        )
        self.assertEqual(restored.run_id, receipt.run.run_id)
        self.assertEqual(
            [call["generation"] for call in reader.calls],
            [
                receipt.publication.manifest_object.generation,
                receipt.publication.manifest.source_pin.generation,
            ],
        )

    def test_wrong_config_or_source_pin_fails_closed(self) -> None:
        writer, request, receipt, _ = self._run()
        with self.assertRaises(ForwardPaperResearchManifestError):
            restore_forward_paper_research_run(
                expected_run_id=receipt.run.run_id,
                bucket=self.bucket,
                manifest_object_name=receipt.publication.manifest_object.object_name,
                manifest_generation=receipt.publication.manifest_object.generation,
                manifest_sha256=receipt.publication.manifest_object.sha256,
                baseline_config=PromotedCrossSectionConfig(
                    minimum_computed_instruments=2
                ),
                challenger_config=self.challenger,
                reader=FakeReader(writer),
                history_windows=ExactResolver(self.raw),
                corporate_actions=ExactResolver(self.actions),
                tick_panels=ExactResolver(self.ticks),
            )
        original = request.source_pin.sha256
        object.__setattr__(request.source_pin, "sha256", "0" * 64)
        try:
            with self.assertRaises(ForwardPaperResearchJobError):
                request.verify_content_identity()
        finally:
            object.__setattr__(request.source_pin, "sha256", original)

    def test_canonical_manifest_with_wrong_signal_session_fails_recompute(self) -> None:
        writer, _, receipt, _ = self._run()
        original = receipt.publication.manifest
        values = {
            item.name: getattr(original, item.name)
            for item in fields(original)
            if item.name != "manifest_id"
        }
        values["signal_session"] = original.signal_session + timedelta(days=1)
        wrong = ForwardPaperResearchRunManifest(**values)
        payload = encode_forward_paper_research_manifest(wrong)
        published = writer.create_or_verify(
            bucket=self.bucket,
            object_name=forward_paper_research_manifest_object_name(wrong),
            content_bytes=payload,
            content_type="application/json",
            maximum_bytes=FORWARD_PAPER_RESEARCH_MANIFEST_MAXIMUM_BYTES,
        )
        with self.assertRaises(ForwardPaperResearchManifestError):
            restore_forward_paper_research_run(
                expected_run_id=receipt.run.run_id,
                bucket=self.bucket,
                manifest_object_name=published.object_name,
                manifest_generation=published.generation,
                manifest_sha256=published.sha256,
                baseline_config=self.baseline,
                challenger_config=self.challenger,
                reader=FakeReader(writer),
                history_windows=ExactResolver(self.raw),
                corporate_actions=ExactResolver(self.actions),
                tick_panels=ExactResolver(self.ticks),
            )

    def test_manifest_codec_rejects_duplicate_and_noncanonical_payloads(self) -> None:
        _, _, receipt, _ = self._run()
        payload = encode_forward_paper_research_manifest(receipt.publication.manifest)
        duplicate = payload.replace(
            b'{"baseline_arm_id":',
            b'{"run_id":"' + receipt.run.run_id.encode("ascii") + b'","baseline_arm_id":',
            1,
        )
        with self.assertRaises(ForwardPaperResearchManifestError):
            decode_forward_paper_research_manifest(duplicate)
        with self.assertRaises(ForwardPaperResearchManifestError):
            decode_forward_paper_research_manifest(payload.rstrip(b"\n"))

    def test_malicious_writer_result_is_rejected(self) -> None:
        writer = FakeWriter()
        request = self._request(writer)
        writer.override = PublishedStateObject(
            object_name="research/forward-paper-baseline-challenger/v1/wrong.json",
            generation=2,
            byte_count=1,
            sha256="0" * 64,
        )
        with self.assertRaises(ForwardPaperResearchJobError):
            run_forward_paper_research_job(
                request=request,
                reader=FakeReader(writer),
                history_windows=ExactResolver(self.raw),
                corporate_actions=ExactResolver(self.actions),
                tick_panels=ExactResolver(self.ticks),
                writer=writer,
            )

    def test_receipt_has_no_promotion_notification_or_execution_authority(self) -> None:
        _, _, receipt, _ = self._run()
        self.assertTrue(receipt.collection_only)
        for name in (
            "promotion_eligible",
            "paper_trade_eligible",
            "notification_eligible",
            "execution_eligible",
        ):
            self.assertFalse(getattr(receipt, name))

    def test_modules_have_no_listing_latest_clock_broker_or_notification_capability(self) -> None:
        source = (inspect.getsource(gcs_module) + inspect.getsource(job_module)).lower()
        for token in (
            "list_blobs",
            "datetime.now(",
            "os.environ",
            "place_order",
            "telegram",
            "send_alert",
            "kronos",
        ):
            self.assertNotIn(token, source)


if __name__ == "__main__":
    unittest.main()
