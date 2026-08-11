from __future__ import annotations

import hashlib
import inspect
import tempfile
import unittest
from pathlib import Path

from india_swing.daily_pipeline.acquisition import GCSObjectPayload
from india_swing.daily_pipeline.state_publication import PublishedStateObject
from india_swing.forward_paper import operational_gcs as gcs_module
from india_swing.forward_paper.operational import (
    assemble_forward_paper_operational_research_graph,
)
from india_swing.forward_paper.operational_gcs import (
    FORWARD_PAPER_OPERATIONAL_MANIFEST_MAXIMUM_BYTES,
    ForwardPaperOperationalManifestError,
    decode_forward_paper_operational_manifest,
    encode_forward_paper_operational_manifest,
    forward_paper_operational_manifest_object_name,
    publish_forward_paper_operational_graph,
    restore_forward_paper_operational_graph,
)

from tests.test_forward_paper_operational import (
    _direct_tick_panel,
    _operational_artifacts,
)


class FakeWriter:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.store: dict[tuple[str, str], tuple[bytes, int]] = {}
        self.override: object = None

    def create_or_verify(
        self, *, bucket, object_name, content_bytes, content_type, maximum_bytes
    ):
        self.calls.append(
            {
                "bucket": bucket,
                "object_name": object_name,
                "content_bytes": content_bytes,
                "content_type": content_type,
                "maximum_bytes": maximum_bytes,
            }
        )
        if self.override is not None:
            return self.override
        key = (bucket, object_name)
        if key in self.store:
            existing, generation = self.store[key]
            if existing != content_bytes:
                raise ValueError
        else:
            generation = len(self.store) + 1
            self.store[key] = (content_bytes, generation)
        return PublishedStateObject(
            object_name=object_name,
            generation=generation,
            byte_count=len(content_bytes),
            sha256=hashlib.sha256(content_bytes).hexdigest(),
        )


class FakeReader:
    def __init__(self, writer: FakeWriter) -> None:
        self.writer = writer
        self.calls: list[dict[str, object]] = []

    def read_generation(
        self, *, bucket, object_name, generation, maximum_bytes
    ) -> GCSObjectPayload:
        self.calls.append(
            {
                "bucket": bucket,
                "object_name": object_name,
                "generation": generation,
                "maximum_bytes": maximum_bytes,
            }
        )
        content, stored_generation = self.writer.store[(bucket, object_name)]
        if stored_generation != generation:
            raise ValueError
        return GCSObjectPayload(content_bytes=content, generation=stored_generation)


class ExactResolver:
    def __init__(self, artifact) -> None:
        self.artifact = artifact
        self.calls: list[str] = []

    def get(self, artifact_id: str):
        self.calls.append(artifact_id)
        return self.artifact

    def build(self, spec):
        self.calls.append(spec.spec_id)
        return self.artifact


class ForwardPaperOperationalGCSTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temporary = tempfile.TemporaryDirectory()
        raw, actions, ticks, first = _operational_artifacts(Path(cls.temporary.name))
        cls.raw = raw
        cls.actions = actions
        cls.ticks = ticks
        cls.graph = assemble_forward_paper_operational_research_graph(
            source_window=raw,
            corporate_actions=actions,
            tick_panel=ticks,
        )
        cls.direct_ticks = _direct_tick_panel(raw, ticks, first)
        cls.direct_graph = assemble_forward_paper_operational_research_graph(
            source_window=raw,
            corporate_actions=actions,
            tick_panel=cls.direct_ticks,
        )
        cls.bucket = "india-swing-test-state"

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temporary.cleanup()

    def _publication(self):
        writer = FakeWriter()
        completed = publish_forward_paper_operational_graph(
            graph=self.graph,
            bucket=self.bucket,
            writer=writer,
        )
        return writer, completed

    def test_publish_writes_one_manifest_only_and_verifies_writer_result(self) -> None:
        writer, completed = self._publication()
        self.assertEqual(len(writer.calls), 1)
        call = writer.calls[0]
        self.assertEqual(call["bucket"], self.bucket)
        self.assertEqual(call["content_type"], "application/json")
        self.assertEqual(
            call["maximum_bytes"], FORWARD_PAPER_OPERATIONAL_MANIFEST_MAXIMUM_BYTES
        )
        self.assertEqual(
            call["object_name"],
            forward_paper_operational_manifest_object_name(completed.manifest),
        )
        decoded = decode_forward_paper_operational_manifest(call["content_bytes"])
        self.assertEqual(decoded, completed.manifest)

    def test_restore_reads_exact_generation_and_recomputes_graph(self) -> None:
        writer, completed = self._publication()
        reader = FakeReader(writer)
        history = ExactResolver(self.raw)
        actions = ExactResolver(self.actions)
        ticks = ExactResolver(self.ticks)
        restored = restore_forward_paper_operational_graph(
            expected_graph_id=self.graph.graph_id,
            bucket=self.bucket,
            manifest_object_name=completed.manifest_object.object_name,
            manifest_generation=completed.manifest_object.generation,
            manifest_sha256=completed.manifest_object.sha256,
            reader=reader,
            history_windows=history,
            corporate_actions=actions,
            tick_panels=ticks,
        )
        self.assertEqual(restored.graph_id, self.graph.graph_id)
        self.assertEqual(len(reader.calls), 1)
        self.assertEqual(
            reader.calls[0]["generation"], completed.manifest_object.generation
        )
        self.assertEqual(history.calls, [self.raw.spec.spec_id])
        self.assertEqual(actions.calls, [self.actions.snapshot_id])
        self.assertEqual(ticks.calls, [self.ticks.panel_id])

    def test_direct_signal_tick_panel_survives_publish_and_exact_restore(self) -> None:
        writer = FakeWriter()
        completed = publish_forward_paper_operational_graph(
            graph=self.direct_graph,
            bucket=self.bucket,
            writer=writer,
        )
        restored = restore_forward_paper_operational_graph(
            expected_graph_id=self.direct_graph.graph_id,
            bucket=self.bucket,
            manifest_object_name=completed.manifest_object.object_name,
            manifest_generation=completed.manifest_object.generation,
            manifest_sha256=completed.manifest_object.sha256,
            reader=FakeReader(writer),
            history_windows=ExactResolver(self.raw),
            corporate_actions=ExactResolver(self.actions),
            tick_panels=ExactResolver(self.direct_ticks),
        )
        self.assertEqual(restored.graph_id, self.direct_graph.graph_id)
        self.assertEqual(restored.tick_panel.panel_id, self.direct_ticks.panel_id)

    def test_wrong_pin_and_wrong_exact_resolver_fail_closed(self) -> None:
        writer, completed = self._publication()
        kwargs = dict(
            expected_graph_id=self.graph.graph_id,
            bucket=self.bucket,
            manifest_object_name=completed.manifest_object.object_name,
            manifest_generation=completed.manifest_object.generation,
            manifest_sha256=completed.manifest_object.sha256,
            reader=FakeReader(writer),
            history_windows=ExactResolver(self.raw),
            corporate_actions=ExactResolver(self.actions),
            tick_panels=ExactResolver(self.ticks),
        )
        with self.assertRaises(ForwardPaperOperationalManifestError):
            restore_forward_paper_operational_graph(
                **{**kwargs, "manifest_sha256": "0" * 64}
            )
        with self.assertRaises(ForwardPaperOperationalManifestError):
            restore_forward_paper_operational_graph(
                **{**kwargs, "history_windows": ExactResolver(object())}
            )

    def test_malicious_writer_result_is_rejected(self) -> None:
        writer = FakeWriter()
        writer.override = PublishedStateObject(
            object_name="research/forward-paper-operational/v1/wrong.json",
            generation=1,
            byte_count=1,
            sha256="0" * 64,
        )
        with self.assertRaises(ForwardPaperOperationalManifestError):
            publish_forward_paper_operational_graph(
                graph=self.graph,
                bucket=self.bucket,
                writer=writer,
            )
        self.assertEqual(len(writer.calls), 1)

    def test_manifest_codec_rejects_duplicate_keys_and_noncanonical_bytes(self) -> None:
        _, completed = self._publication()
        payload = encode_forward_paper_operational_manifest(completed.manifest)
        duplicate = payload.replace(
            b'{"adjusted_window_id":',
            b'{"graph_id":"' + self.graph.graph_id.encode("ascii") + b'","adjusted_window_id":',
            1,
        )
        with self.assertRaises(ForwardPaperOperationalManifestError):
            decode_forward_paper_operational_manifest(duplicate)
        with self.assertRaises(ForwardPaperOperationalManifestError):
            decode_forward_paper_operational_manifest(payload.rstrip(b"\n"))

    def test_module_has_no_listing_latest_clock_broker_or_notification_capability(self) -> None:
        source = inspect.getsource(gcs_module).lower()
        for token in (
            "list_blobs",
            "latest",
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
