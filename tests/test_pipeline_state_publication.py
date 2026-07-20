from __future__ import annotations

import ast
import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from india_swing.daily_pipeline import state_publication
from india_swing.daily_pipeline.models import DailyPipelineRun
from india_swing.daily_pipeline.state_inventory import (
    ROOT_NAMES,
    PipelineStateInventory,
    PipelineStateRoots,
    build_pipeline_state_inventory,
)
from india_swing.daily_pipeline.state_publication import (
    CompletedPipelineStatePublication,
    GoogleCloudStorageStateObjectWriter,
    PipelineStatePublicationManifest,
    PublishedStateObject,
    StatePublicationError,
    encode_pipeline_state_publication_manifest,
    parse_pipeline_state_publication_manifest,
    publish_pipeline_state,
)

from tests.test_promotion import daily_run as _promotion_daily_run

_REPO_ROOT = Path(__file__).resolve().parent.parent
_MODULE_PATH = _REPO_ROOT / "src" / "india_swing" / "daily_pipeline" / "state_publication.py"


def _make_roots(base: Path) -> PipelineStateRoots:
    kwargs = {}
    for name in ROOT_NAMES:
        root_path = base / name
        root_path.mkdir(parents=True, exist_ok=True)
        kwargs[name] = root_path
    return PipelineStateRoots(**kwargs)


def _write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


class _FakePreconditionFailed(Exception):
    """Stand-in for google.api_core.exceptions.PreconditionFailed."""


class _FakeGCSStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.generations: dict[str, int] = {}
        self._next_generation = 1000

    def create(self, object_name: str, content_bytes: bytes) -> int:
        if object_name in self.objects:
            raise LookupError("already exists")
        generation = self._next_generation
        self._next_generation += 1
        self.objects[object_name] = content_bytes
        self.generations[object_name] = generation
        return generation


class _FakeBlob:
    def __init__(
        self,
        store: _FakeGCSStore,
        object_name: str,
        requested_generation: object,
        *,
        precondition_cls: type[Exception],
    ) -> None:
        self._store = store
        self.name = object_name
        self.requested_generation = requested_generation
        self.generation = requested_generation
        self._precondition_cls = precondition_cls
        self.upload_calls: list[dict[str, object]] = []
        self.reload_calls: list[dict[str, object]] = []
        self.download_calls: list[dict[str, object]] = []

    def upload_from_string(
        self, content_bytes: bytes, *, content_type, if_generation_match, checksum, retry
    ) -> None:
        self.upload_calls.append(
            {
                "content_bytes": content_bytes,
                "content_type": content_type,
                "if_generation_match": if_generation_match,
                "checksum": checksum,
                "retry": retry,
            }
        )
        try:
            generation = self._store.create(self.name, content_bytes)
        except LookupError:
            raise self._precondition_cls("object already exists") from None
        self.generation = generation

    def reload(self, *, retry) -> None:
        self.reload_calls.append({"retry": retry})
        self.generation = self._store.generations.get(self.name)

    def download_as_bytes(self, *, end, raw_download, if_generation_match, retry) -> bytes:
        self.download_calls.append(
            {
                "end": end,
                "raw_download": raw_download,
                "if_generation_match": if_generation_match,
                "retry": retry,
            }
        )
        stored = self._store.objects.get(self.name)
        stored_generation = self._store.generations.get(self.name)
        if stored is None or stored_generation != if_generation_match:
            raise LookupError("generation mismatch")
        if end is None:
            return stored
        return stored[: end + 1]


class _FakeBucket:
    def __init__(self, store: _FakeGCSStore, name: str, *, precondition_cls: type[Exception]) -> None:
        self._store = store
        self.name = name
        self._precondition_cls = precondition_cls
        self.blob_calls: list[tuple[str, object]] = []
        self.blobs: list[_FakeBlob] = []

    def blob(self, object_name: str, generation: object = None) -> _FakeBlob:
        self.blob_calls.append((object_name, generation))
        blob = _FakeBlob(self._store, object_name, generation, precondition_cls=self._precondition_cls)
        self.blobs.append(blob)
        return blob


class _FakeStorageClient:
    """Stand-in for google.cloud.storage.Client. Has no listing method."""

    def __init__(self, *, precondition_cls: type[Exception] = _FakePreconditionFailed) -> None:
        self.store = _FakeGCSStore()
        self._precondition_cls = precondition_cls
        self.bucket_calls: list[str] = []
        self.buckets: list[_FakeBucket] = []

    def bucket(self, name: str) -> _FakeBucket:
        self.bucket_calls.append(name)
        bucket = _FakeBucket(self.store, name, precondition_cls=self._precondition_cls)
        self.buckets.append(bucket)
        return bucket


class RecordingWriter:
    """Fake StateObjectWriter. Never contacts GCP; records every call in order."""

    def __init__(self, *, fail_at: int | None = None, malicious_at: int | None = None) -> None:
        self.calls: list[dict[str, object]] = []
        self._fail_at = fail_at
        self._malicious_at = malicious_at
        self._next_generation = 1

    def create_or_verify(
        self,
        *,
        bucket: str,
        object_name: str,
        content_bytes: bytes,
        content_type: str,
        maximum_bytes: int,
    ) -> PublishedStateObject:
        index = len(self.calls)
        self.calls.append(
            {
                "bucket": bucket,
                "object_name": object_name,
                "content_bytes": content_bytes,
                "content_type": content_type,
                "maximum_bytes": maximum_bytes,
            }
        )
        if self._fail_at is not None and index == self._fail_at:
            raise RuntimeError("SECRET-WRITER-FAILURE-DO-NOT-LEAK-4c1a")
        if self._malicious_at is not None and index == self._malicious_at:
            return PublishedStateObject(
                object_name="state/v1/blobs/malicious/attacker-chosen-name",
                generation=1,
                byte_count=999999,
                sha256="f" * 64,
            )
        generation = self._next_generation
        self._next_generation += 1
        return PublishedStateObject(
            object_name=object_name,
            generation=generation,
            byte_count=len(content_bytes),
            sha256=hashlib.sha256(content_bytes).hexdigest(),
        )


class _PublicationTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)
        self.roots = _make_roots(self.base)
        self.run = _promotion_daily_run()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _build_inventory(self) -> PipelineStateInventory:
        _write(self.roots.calendar_data / "a.json", b"alpha-content")
        _write(self.roots.calendar_data / "b.json", b"alpha-content")
        _write(self.roots.daily_reports / "c.json", b"gamma-content-longer")
        return build_pipeline_state_inventory(self.run, self.roots)

    def _valid_manifest(self, **overrides: object) -> PipelineStatePublicationManifest:
        inventory_object = PublishedStateObject(
            object_name="state/v1/inventories/2026-01-01/" + "a" * 64 + "/" + "b" * 64 + ".json",
            generation=100,
            byte_count=10,
            sha256="c" * 64,
        )
        blob = PublishedStateObject(
            object_name="state/v1/blobs/dd/" + "d" * 64,
            generation=101,
            byte_count=20,
            sha256="d" * 64,
        )
        kwargs: dict[str, object] = dict(
            schema_version=1,
            bucket="test-bucket",
            run_id="a" * 64,
            previous_run_id=None,
            market_session=self.run.market_session.__class__.fromisoformat("2026-01-01"),
            cutoff=self.run.cutoff,
            inventory_id="b" * 64,
            inventory_object=inventory_object,
            blob_objects=(blob,),
        )
        kwargs.update(overrides)
        return PipelineStatePublicationManifest(**kwargs)


class ManifestCodecTests(_PublicationTestCase):
    def test_round_trip_encode_parse(self) -> None:
        manifest = self._valid_manifest()
        encoded = encode_pipeline_state_publication_manifest(manifest)
        parsed = parse_pipeline_state_publication_manifest(encoded)
        self.assertEqual(parsed.publication_id, manifest.publication_id)
        self.assertEqual(encode_pipeline_state_publication_manifest(parsed), encoded)

    def test_object_names_derived_from_ids(self) -> None:
        manifest = self._valid_manifest()
        self.assertIn(manifest.run_id, manifest.inventory_object.object_name)
        self.assertIn(manifest.inventory_id, manifest.inventory_object.object_name)
        self.assertIn(manifest.blob_objects[0].sha256, manifest.blob_objects[0].object_name)

    def test_unknown_top_level_key_rejected(self) -> None:
        encoded = encode_pipeline_state_publication_manifest(self._valid_manifest())
        tampered = encoded[:-2] + b',"extra_unknown_key":1}\n'
        with self.assertRaises(StatePublicationError):
            parse_pipeline_state_publication_manifest(tampered)

    def test_missing_top_level_key_rejected(self) -> None:
        encoded = encode_pipeline_state_publication_manifest(self._valid_manifest())
        tampered = encoded.replace(b'"bucket":', b'"renamed_bucket":')
        with self.assertRaises(StatePublicationError):
            parse_pipeline_state_publication_manifest(tampered)

    def test_duplicate_json_key_rejected(self) -> None:
        manifest = self._valid_manifest()
        payload = (
            b'{"blob_objects":[],"blob_objects":[],"bucket":"' + manifest.bucket.encode() + b'",'
            b'"cutoff":"' + manifest.cutoff.isoformat().encode() + b'",'
            b'"inventory_id":"' + manifest.inventory_id.encode() + b'",'
            b'"inventory_object":{"byte_count":1,"generation":1,"object_name":"x","sha256":"'
            + (b"a" * 64) + b'"},'
            b'"market_session":"' + manifest.market_session.isoformat().encode() + b'",'
            b'"previous_run_id":null,"publication_id":"' + (b"a" * 64) + b'",'
            b'"run_id":"' + manifest.run_id.encode() + b'","schema_version":1}\n'
        )
        with self.assertRaises(StatePublicationError):
            parse_pipeline_state_publication_manifest(payload)

    def test_float_rejected(self) -> None:
        encoded = encode_pipeline_state_publication_manifest(self._valid_manifest())
        tampered = encoded.replace(b'"schema_version":1', b'"schema_version":1.0')
        with self.assertRaises(StatePublicationError):
            parse_pipeline_state_publication_manifest(tampered)

    def test_nan_constant_rejected(self) -> None:
        encoded = encode_pipeline_state_publication_manifest(self._valid_manifest())
        tampered = encoded.replace(b'"schema_version":1', b'"schema_version":NaN')
        with self.assertRaises(StatePublicationError):
            parse_pipeline_state_publication_manifest(tampered)

    def test_bool_as_int_schema_version_rejected(self) -> None:
        encoded = encode_pipeline_state_publication_manifest(self._valid_manifest())
        tampered = encoded.replace(b'"schema_version":1', b'"schema_version":true')
        with self.assertRaises(StatePublicationError):
            parse_pipeline_state_publication_manifest(tampered)

    def test_malformed_utf8_rejected(self) -> None:
        with self.assertRaises(StatePublicationError):
            parse_pipeline_state_publication_manifest(b"\xff\xfe\x00\x01")

    def test_malformed_json_rejected(self) -> None:
        with self.assertRaises(StatePublicationError):
            parse_pipeline_state_publication_manifest(b"{not valid json")

    def test_noncanonical_whitespace_rejected(self) -> None:
        encoded = encode_pipeline_state_publication_manifest(self._valid_manifest())
        tampered = encoded.replace(b'"schema_version":1', b'"schema_version": 1')
        with self.assertRaises(StatePublicationError):
            parse_pipeline_state_publication_manifest(tampered)

    def test_noncanonical_key_order_rejected(self) -> None:
        manifest = self._valid_manifest()
        payload = (
            b'{"schema_version":1,"bucket":"' + manifest.bucket.encode() + b'",'
            b'"run_id":"' + manifest.run_id.encode() + b'","previous_run_id":null,'
            b'"market_session":"' + manifest.market_session.isoformat().encode() + b'",'
            b'"cutoff":"' + manifest.cutoff.isoformat().encode() + b'",'
            b'"inventory_id":"' + manifest.inventory_id.encode() + b'",'
            b'"inventory_object":{"byte_count":10,"generation":100,"object_name":"'
            + manifest.inventory_object.object_name.encode() + b'","sha256":"'
            + manifest.inventory_object.sha256.encode() + b'"},'
            b'"blob_objects":[{"byte_count":20,"generation":101,"object_name":"'
            + manifest.blob_objects[0].object_name.encode() + b'","sha256":"'
            + manifest.blob_objects[0].sha256.encode() + b'"}],'
            b'"publication_id":"' + manifest.publication_id.encode() + b'"}\n'
        )
        with self.assertRaises(StatePublicationError):
            parse_pipeline_state_publication_manifest(payload)

    def test_noncanonical_timestamp_rejected(self) -> None:
        manifest = self._valid_manifest()
        encoded = encode_pipeline_state_publication_manifest(manifest)
        cutoff_field = b'"cutoff":"' + manifest.cutoff.isoformat().encode("utf-8") + b'"'
        self.assertIn(cutoff_field, encoded)
        z_form = cutoff_field.replace(b"+00:00", b"Z")
        tampered = encoded.replace(cutoff_field, z_form)
        with self.assertRaises(StatePublicationError):
            parse_pipeline_state_publication_manifest(tampered)

    def test_missing_final_newline_rejected(self) -> None:
        encoded = encode_pipeline_state_publication_manifest(self._valid_manifest())
        with self.assertRaises(StatePublicationError):
            parse_pipeline_state_publication_manifest(encoded[:-1])

    def test_extra_trailing_newline_rejected(self) -> None:
        encoded = encode_pipeline_state_publication_manifest(self._valid_manifest())
        with self.assertRaises(StatePublicationError):
            parse_pipeline_state_publication_manifest(encoded + b"\n")

    def test_publication_id_mismatch_rejected(self) -> None:
        manifest = self._valid_manifest()
        encoded = encode_pipeline_state_publication_manifest(manifest)
        tampered_id = "f" if manifest.publication_id[0] != "f" else "e"
        tampered_id += manifest.publication_id[1:]
        tampered = encoded.replace(
            f'"publication_id":"{manifest.publication_id}"'.encode(),
            f'"publication_id":"{tampered_id}"'.encode(),
        )
        self.assertNotEqual(tampered, encoded)
        with self.assertRaises(StatePublicationError):
            parse_pipeline_state_publication_manifest(tampered)

    def test_payload_not_bytes_rejected(self) -> None:
        with self.assertRaises(StatePublicationError):
            parse_pipeline_state_publication_manifest("not-bytes")  # type: ignore[arg-type]

    def test_empty_payload_rejected(self) -> None:
        with self.assertRaises(StatePublicationError):
            parse_pipeline_state_publication_manifest(b"")

    def test_oversized_payload_rejected(self) -> None:
        with patch.object(state_publication, "MAXIMUM_PUBLICATION_MANIFEST_BYTES", 4):
            with self.assertRaises(StatePublicationError):
                parse_pipeline_state_publication_manifest(b"12345")


class ManifestDataclassStrictnessTests(_PublicationTestCase):
    def test_schema_version_wrong_rejected(self) -> None:
        with self.assertRaises(StatePublicationError):
            self._valid_manifest(schema_version=2)

    def test_schema_version_bool_rejected(self) -> None:
        with self.assertRaises(StatePublicationError):
            self._valid_manifest(schema_version=True)

    def test_bucket_invalid_rejected(self) -> None:
        with self.assertRaises(StatePublicationError):
            self._valid_manifest(bucket="Invalid_Bucket")

    def test_bucket_ip_shaped_rejected(self) -> None:
        with self.assertRaises(StatePublicationError):
            self._valid_manifest(bucket="192.168.1.100")

    def test_run_id_invalid_rejected(self) -> None:
        with self.assertRaises(StatePublicationError):
            self._valid_manifest(run_id="not-a-hash")

    def test_previous_run_id_invalid_rejected(self) -> None:
        with self.assertRaises(StatePublicationError):
            self._valid_manifest(previous_run_id="not-a-hash")

    def test_inventory_id_invalid_rejected(self) -> None:
        with self.assertRaises(StatePublicationError):
            self._valid_manifest(inventory_id="A" * 64)

    def test_inventory_object_name_mismatch_rejected(self) -> None:
        bad_inventory_object = PublishedStateObject(
            object_name="state/v1/inventories/wrong/path.json",
            generation=1,
            byte_count=1,
            sha256="a" * 64,
        )
        with self.assertRaises(StatePublicationError):
            self._valid_manifest(inventory_object=bad_inventory_object)

    def test_inventory_object_subclass_rejected(self) -> None:
        class _ShapedPublishedObject(PublishedStateObject):
            pass

        shaped = _ShapedPublishedObject(
            object_name="state/v1/inventories/2026-01-01/" + "a" * 64 + "/" + "b" * 64 + ".json",
            generation=1,
            byte_count=1,
            sha256="a" * 64,
        )
        with self.assertRaises(StatePublicationError):
            self._valid_manifest(inventory_object=shaped)

    def test_blob_objects_not_tuple_rejected(self) -> None:
        blob = PublishedStateObject(
            object_name="state/v1/blobs/dd/" + "d" * 64, generation=1, byte_count=1, sha256="d" * 64
        )
        with self.assertRaises(StatePublicationError):
            self._valid_manifest(blob_objects=[blob])

    def test_blob_objects_wrong_order_rejected(self) -> None:
        first = PublishedStateObject(
            object_name="state/v1/blobs/ff/" + "f" * 64, generation=1, byte_count=1, sha256="f" * 64
        )
        second = PublishedStateObject(
            object_name="state/v1/blobs/aa/" + "a" * 64, generation=2, byte_count=1, sha256="a" * 64
        )
        with self.assertRaises(StatePublicationError):
            self._valid_manifest(blob_objects=(first, second))

    def test_blob_objects_duplicate_hash_rejected(self) -> None:
        blob = PublishedStateObject(
            object_name="state/v1/blobs/dd/" + "d" * 64, generation=1, byte_count=1, sha256="d" * 64
        )
        with self.assertRaises(StatePublicationError):
            self._valid_manifest(blob_objects=(blob, blob))

    def test_blob_object_name_mismatch_rejected(self) -> None:
        bad_blob = PublishedStateObject(
            object_name="state/v1/blobs/wrong/path", generation=1, byte_count=1, sha256="d" * 64
        )
        with self.assertRaises(StatePublicationError):
            self._valid_manifest(blob_objects=(bad_blob,))

    def test_post_construction_bucket_mutation_detected(self) -> None:
        manifest = self._valid_manifest()
        object.__setattr__(manifest, "bucket", "a-different-bucket")
        with self.assertRaises(StatePublicationError):
            manifest.verify_content_identity()

    def test_post_construction_blob_objects_mutation_detected(self) -> None:
        manifest = self._valid_manifest()
        tampered_blob = PublishedStateObject(
            object_name="state/v1/blobs/ee/" + "e" * 64, generation=1, byte_count=1, sha256="e" * 64
        )
        object.__setattr__(manifest, "blob_objects", (tampered_blob,))
        with self.assertRaises(StatePublicationError):
            manifest.verify_content_identity()

    def test_published_state_object_generation_bool_rejected(self) -> None:
        with self.assertRaises(StatePublicationError):
            PublishedStateObject(object_name="x", generation=True, byte_count=1, sha256="a" * 64)

    def test_published_state_object_hash_uppercase_rejected(self) -> None:
        with self.assertRaises(StatePublicationError):
            PublishedStateObject(object_name="x", generation=1, byte_count=1, sha256="A" * 64)

    def test_completed_publication_rejects_manifest_type(self) -> None:
        publication_object = PublishedStateObject(
            object_name="state/v1/publications/x", generation=1, byte_count=1, sha256="a" * 64
        )
        with self.assertRaises(StatePublicationError):
            CompletedPipelineStatePublication(
                manifest="not-a-manifest",  # type: ignore[arg-type]
                publication_object=publication_object,
            )

    def test_completed_publication_rejects_publication_object_name_mismatch(self) -> None:
        manifest = self._valid_manifest()
        wrong_publication_object = PublishedStateObject(
            object_name="state/v1/publications/wrong/path.json",
            generation=1,
            byte_count=1,
            sha256="a" * 64,
        )
        with self.assertRaises(StatePublicationError):
            CompletedPipelineStatePublication(
                manifest=manifest, publication_object=wrong_publication_object
            )

    def test_completed_publication_rejects_byte_count_mismatch(self) -> None:
        manifest = self._valid_manifest()
        expected_bytes = encode_pipeline_state_publication_manifest(manifest)
        correct_name = state_publication._publication_object_name(
            manifest.market_session, manifest.run_id, manifest.publication_id
        )
        wrong_publication_object = PublishedStateObject(
            object_name=correct_name,
            generation=1,
            byte_count=len(expected_bytes) + 1,
            sha256=hashlib.sha256(expected_bytes).hexdigest(),
        )
        with self.assertRaises(StatePublicationError):
            CompletedPipelineStatePublication(
                manifest=manifest, publication_object=wrong_publication_object
            )


class GoogleCloudStorageWriterCreateTests(unittest.TestCase):
    def test_sdk_client_initialization_failure_is_sanitized(self) -> None:
        class _BrokenStorage:
            @staticmethod
            def Client():
                raise RuntimeError("SECRET-CLIENT-INIT-DO-NOT-LEAK")

        with patch.object(state_publication, "storage", _BrokenStorage()):
            try:
                GoogleCloudStorageStateObjectWriter()
                self.fail("expected StatePublicationError")
            except StatePublicationError as exc:
                self.assertNotIn("SECRET-CLIENT-INIT-DO-NOT-LEAK", str(exc))
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)

    def test_bucket_handle_failure_is_sanitized(self) -> None:
        class _BrokenClient:
            def bucket(self, name):
                raise RuntimeError("SECRET-BUCKET-HANDLE-DO-NOT-LEAK")

        writer = GoogleCloudStorageStateObjectWriter(client=_BrokenClient())
        try:
            writer.create_or_verify(
                bucket="my-bucket",
                object_name="state/v1/blobs/aa/" + "a" * 64,
                content_bytes=b"content",
                content_type="application/octet-stream",
                maximum_bytes=1024,
            )
            self.fail("expected StatePublicationError")
        except StatePublicationError as exc:
            self.assertNotIn("SECRET-BUCKET-HANDLE-DO-NOT-LEAK", str(exc))
            self.assertIsNone(exc.__cause__)
            self.assertIsNone(exc.__context__)

    def setUp(self) -> None:
        self.client = _FakeStorageClient()
        self.patcher = patch.object(
            state_publication, "PreconditionFailed", _FakePreconditionFailed
        )
        self.patcher.start()
        self.addCleanup(self.patcher.stop)
        self.writer = GoogleCloudStorageStateObjectWriter(client=self.client)

    def test_exact_upload_from_string_arguments(self) -> None:
        content = b"file-content"
        result = self.writer.create_or_verify(
            bucket="my-bucket",
            object_name="state/v1/blobs/aa/" + "a" * 64,
            content_bytes=content,
            content_type="application/octet-stream",
            maximum_bytes=1024,
        )
        blob = self.client.buckets[0].blob_calls
        self.assertEqual(blob, [("state/v1/blobs/aa/" + "a" * 64, None)])
        upload_call = self.client.store  # sanity: object stored
        self.assertIn("state/v1/blobs/aa/" + "a" * 64, upload_call.objects)
        self.assertEqual(result.object_name, "state/v1/blobs/aa/" + "a" * 64)
        self.assertEqual(result.byte_count, len(content))
        self.assertEqual(result.sha256, hashlib.sha256(content).hexdigest())
        self.assertGreater(result.generation, 0)

    def test_upload_from_string_uses_exact_keyword_arguments(self) -> None:
        content = b"file-content"
        object_name = "state/v1/blobs/aa/" + "a" * 64
        self.writer.create_or_verify(
            bucket="my-bucket",
            object_name=object_name,
            content_bytes=content,
            content_type="application/json",
            maximum_bytes=1024,
        )
        recorded_blob = None
        for bucket in self.client.buckets:
            for call_object_name, _ in bucket.blob_calls:
                if call_object_name == object_name:
                    recorded_blob = bucket
        self.assertIsNotNone(recorded_blob)

    def test_missing_generation_after_create_rejected(self) -> None:
        class _NoGenerationBlob:
            def __init__(self) -> None:
                self.generation = None

            def upload_from_string(self, *args, **kwargs):
                return None

        class _NoGenerationBucket:
            def blob(self, object_name, generation=None):
                return _NoGenerationBlob()

        class _NoGenerationClient:
            def bucket(self, name):
                return _NoGenerationBucket()

        writer = GoogleCloudStorageStateObjectWriter(client=_NoGenerationClient())
        with self.assertRaises(StatePublicationError):
            writer.create_or_verify(
                bucket="my-bucket",
                object_name="state/v1/blobs/aa/" + "a" * 64,
                content_bytes=b"x",
                content_type="application/octet-stream",
                maximum_bytes=1024,
            )

    def test_bool_generation_after_create_rejected(self) -> None:
        class _BoolGenerationBlob:
            def __init__(self) -> None:
                self.generation = None

            def upload_from_string(self, *args, **kwargs):
                self.generation = True

        class _BoolGenerationBucket:
            def blob(self, object_name, generation=None):
                return _BoolGenerationBlob()

        class _BoolGenerationClient:
            def bucket(self, name):
                return _BoolGenerationBucket()

        writer = GoogleCloudStorageStateObjectWriter(client=_BoolGenerationClient())
        with self.assertRaises(StatePublicationError):
            writer.create_or_verify(
                bucket="my-bucket",
                object_name="state/v1/blobs/aa/" + "a" * 64,
                content_bytes=b"x",
                content_type="application/octet-stream",
                maximum_bytes=1024,
            )

    def test_string_generation_after_create_rejected(self) -> None:
        class _StringGenerationBlob:
            def __init__(self) -> None:
                self.generation = None

            def upload_from_string(self, *args, **kwargs):
                self.generation = "123"

        class _StringGenerationBucket:
            def blob(self, object_name, generation=None):
                return _StringGenerationBlob()

        class _StringGenerationClient:
            def bucket(self, name):
                return _StringGenerationBucket()

        writer = GoogleCloudStorageStateObjectWriter(client=_StringGenerationClient())
        with self.assertRaises(StatePublicationError):
            writer.create_or_verify(
                bucket="my-bucket",
                object_name="state/v1/blobs/aa/" + "a" * 64,
                content_bytes=b"x",
                content_type="application/octet-stream",
                maximum_bytes=1024,
            )

    def test_zero_generation_after_create_rejected(self) -> None:
        class _ZeroGenerationBlob:
            def __init__(self) -> None:
                self.generation = None

            def upload_from_string(self, *args, **kwargs):
                self.generation = 0

        class _ZeroGenerationBucket:
            def blob(self, object_name, generation=None):
                return _ZeroGenerationBlob()

        class _ZeroGenerationClient:
            def bucket(self, name):
                return _ZeroGenerationBucket()

        writer = GoogleCloudStorageStateObjectWriter(client=_ZeroGenerationClient())
        with self.assertRaises(StatePublicationError):
            writer.create_or_verify(
                bucket="my-bucket",
                object_name="state/v1/blobs/aa/" + "a" * 64,
                content_bytes=b"x",
                content_type="application/octet-stream",
                maximum_bytes=1024,
            )

    def test_too_large_generation_after_create_rejected(self) -> None:
        class _HugeGenerationBlob:
            def __init__(self) -> None:
                self.generation = None

            def upload_from_string(self, *args, **kwargs):
                self.generation = 2**63

        class _HugeGenerationBucket:
            def blob(self, object_name, generation=None):
                return _HugeGenerationBlob()

        class _HugeGenerationClient:
            def bucket(self, name):
                return _HugeGenerationBucket()

        writer = GoogleCloudStorageStateObjectWriter(client=_HugeGenerationClient())
        with self.assertRaises(StatePublicationError):
            writer.create_or_verify(
                bucket="my-bucket",
                object_name="state/v1/blobs/aa/" + "a" * 64,
                content_bytes=b"x",
                content_type="application/octet-stream",
                maximum_bytes=1024,
            )

    def test_non_precondition_sdk_error_sanitized_without_read_fallback(self) -> None:
        class _RaisingBlob:
            def upload_from_string(self, *args, **kwargs):
                raise RuntimeError("SECRET-SDK-FAILURE-DO-NOT-LEAK-9c2f")

            def reload(self, *, retry):
                raise AssertionError("reload must not be called on a non-conflict SDK error")

            def download_as_bytes(self, **kwargs):
                raise AssertionError("download must not be called on a non-conflict SDK error")

        class _RaisingBucket:
            def blob(self, object_name, generation=None):
                return _RaisingBlob()

        class _RaisingClient:
            def bucket(self, name):
                return _RaisingBucket()

        writer = GoogleCloudStorageStateObjectWriter(client=_RaisingClient())
        try:
            writer.create_or_verify(
                bucket="my-bucket",
                object_name="state/v1/blobs/aa/" + "a" * 64,
                content_bytes=b"x",
                content_type="application/octet-stream",
                maximum_bytes=1024,
            )
            self.fail("expected StatePublicationError")
        except StatePublicationError as exc:
            self.assertNotIn("SECRET-SDK-FAILURE-DO-NOT-LEAK-9c2f", str(exc))
            self.assertIsNone(exc.__cause__)
            self.assertIsNone(exc.__context__)


class GoogleCloudStorageWriterConflictTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = _FakeStorageClient()
        self.patcher = patch.object(
            state_publication, "PreconditionFailed", _FakePreconditionFailed
        )
        self.patcher.start()
        self.addCleanup(self.patcher.stop)
        self.writer = GoogleCloudStorageStateObjectWriter(client=self.client)
        self.object_name = "state/v1/blobs/aa/" + "a" * 64
        self.content = b"already-published-content"
        # Pre-populate the store as if a prior attempt already created it.
        self.client.store.create(self.object_name, self.content)

    def test_conflict_verification_success_uses_exact_args_and_no_overwrite(self) -> None:
        result = self.writer.create_or_verify(
            bucket="my-bucket",
            object_name=self.object_name,
            content_bytes=self.content,
            content_type="application/octet-stream",
            maximum_bytes=1024,
        )
        self.assertEqual(result.byte_count, len(self.content))
        self.assertEqual(result.sha256, hashlib.sha256(self.content).hexdigest())

        # client.bucket(...) is called once for the failed create attempt
        # and once more for the generation-pinned read; each call returns
        # its own bucket handle (matching real GCS SDK semantics), so
        # blob() calls are aggregated across every bucket handle produced.
        self.assertEqual(self.client.bucket_calls, ["my-bucket", "my-bucket"])
        all_blob_calls = [call for bucket in self.client.buckets for call in bucket.blob_calls]
        self.assertEqual(len(all_blob_calls), 2)  # create attempt + pinned read
        first_blob_name, first_generation_arg = all_blob_calls[0]
        self.assertEqual(first_blob_name, self.object_name)
        self.assertIsNone(first_generation_arg)
        second_blob_name, second_generation_arg = all_blob_calls[1]
        self.assertEqual(second_blob_name, self.object_name)
        self.assertEqual(second_generation_arg, self.client.store.generations[self.object_name])

        # Exactly one upload_from_string call total (the failed create):
        # never a second upload/overwrite attempt.
        all_blobs = [blob for bucket in self.client.buckets for blob in bucket.blobs]
        upload_call_count = sum(len(blob.upload_calls) for blob in all_blobs)
        self.assertEqual(upload_call_count, 1)

    def test_conflict_reload_and_download_exact_arguments(self) -> None:
        self.writer.create_or_verify(
            bucket="my-bucket",
            object_name=self.object_name,
            content_bytes=self.content,
            content_type="application/octet-stream",
            maximum_bytes=4096,
        )
        all_blobs = [blob for bucket in self.client.buckets for blob in bucket.blobs]
        create_attempt_blob = all_blobs[0]
        pinned_read_blob = all_blobs[1]

        self.assertEqual(create_attempt_blob.reload_calls, [{"retry": None}])
        self.assertEqual(pinned_read_blob.reload_calls, [])
        self.assertEqual(
            pinned_read_blob.download_calls,
            [
                {
                    "end": 4096,
                    "raw_download": True,
                    "if_generation_match": self.client.store.generations[self.object_name],
                    "retry": None,
                }
            ],
        )

    def test_missing_generation_from_reload_rejected(self) -> None:
        class _ReloadNoGenerationBlob:
            def __init__(self) -> None:
                self.generation = None

            def upload_from_string(self, *args, **kwargs):
                raise _FakePreconditionFailed("conflict")

            def reload(self, *, retry):
                self.generation = None

        class _Bucket:
            def blob(self, object_name, generation=None):
                return _ReloadNoGenerationBlob()

        class _Client:
            def bucket(self, name):
                return _Bucket()

        writer = GoogleCloudStorageStateObjectWriter(client=_Client())
        with patch.object(state_publication, "PreconditionFailed", _FakePreconditionFailed):
            with self.assertRaises(StatePublicationError):
                writer.create_or_verify(
                    bucket="my-bucket",
                    object_name=self.object_name,
                    content_bytes=self.content,
                    content_type="application/octet-stream",
                    maximum_bytes=1024,
                )

    def test_bool_generation_from_reload_rejected(self) -> None:
        class _ReloadBoolGenerationBlob:
            def __init__(self) -> None:
                self.generation = None

            def upload_from_string(self, *args, **kwargs):
                raise _FakePreconditionFailed("conflict")

            def reload(self, *, retry):
                self.generation = True

        class _Bucket:
            def blob(self, object_name, generation=None):
                return _ReloadBoolGenerationBlob()

        class _Client:
            def bucket(self, name):
                return _Bucket()

        writer = GoogleCloudStorageStateObjectWriter(client=_Client())
        with patch.object(state_publication, "PreconditionFailed", _FakePreconditionFailed):
            with self.assertRaises(StatePublicationError):
                writer.create_or_verify(
                    bucket="my-bucket",
                    object_name=self.object_name,
                    content_bytes=self.content,
                    content_type="application/octet-stream",
                    maximum_bytes=1024,
                )

    def test_mismatched_generation_on_pinned_blob_rejected(self) -> None:
        content = self.content

        class _CreateBlob:
            generation = None

            def upload_from_string(self, *args, **kwargs):
                raise _FakePreconditionFailed("conflict")

            def reload(self, *, retry):
                self.generation = 555

        class _PinnedBlob:
            generation = 556

            def download_as_bytes(self, **kwargs):
                return content

        class _Bucket:
            def blob(self, object_name, generation=None):
                if generation is None:
                    return _CreateBlob()
                return _PinnedBlob()

        class _Client:
            def bucket(self, name):
                return _Bucket()

        writer = GoogleCloudStorageStateObjectWriter(client=_Client())
        with patch.object(state_publication, "PreconditionFailed", _FakePreconditionFailed):
            with self.assertRaises(StatePublicationError):
                writer.create_or_verify(
                    bucket="my-bucket",
                    object_name=self.object_name,
                    content_bytes=content,
                    content_type="application/octet-stream",
                    maximum_bytes=1024,
                )

    def test_tampered_downloaded_bytes_rejected(self) -> None:
        wrong_content = b"tampered-bytes-do-not-match"
        store = _FakeGCSStore()
        store.create(self.object_name, wrong_content)

        class _MismatchBlob:
            def __init__(self, generation: int | None) -> None:
                self.generation = generation

            def upload_from_string(self, *args, **kwargs):
                raise _FakePreconditionFailed("conflict")

            def reload(self, *, retry):
                self.generation = store.generations[self.object_name if hasattr(self, "object_name") else None]

        class _Bucket:
            def __init__(self) -> None:
                self._generation = store.generations[
                    next(iter(store.generations))
                ]

            def blob(self, object_name, generation=None):
                if generation is None:
                    blob = _MismatchBlob(None)
                    blob.reload = lambda *, retry: setattr(blob, "generation", self._generation)
                    return blob
                pinned = _FakeBlob(store, object_name, generation, precondition_cls=_FakePreconditionFailed)
                return pinned

        class _Client:
            def __init__(self) -> None:
                self.bucket_instance = _Bucket()

            def bucket(self, name):
                return self.bucket_instance

        writer = GoogleCloudStorageStateObjectWriter(client=_Client())
        with patch.object(state_publication, "PreconditionFailed", _FakePreconditionFailed):
            with self.assertRaises(StatePublicationError):
                writer.create_or_verify(
                    bucket="my-bucket",
                    object_name=self.object_name,
                    content_bytes=self.content,
                    content_type="application/octet-stream",
                    maximum_bytes=1024,
                )

    def test_wrong_return_type_from_download_rejected(self) -> None:
        class _WrongTypeBlob:
            def __init__(self, generation: int | None) -> None:
                self.generation = generation

            def upload_from_string(self, *args, **kwargs):
                raise _FakePreconditionFailed("conflict")

            def reload(self, *, retry):
                self.generation = 555

            def download_as_bytes(self, **kwargs):
                return "not-bytes"

        class _Bucket:
            def blob(self, object_name, generation=None):
                return _WrongTypeBlob(generation)

        class _Client:
            def bucket(self, name):
                return _Bucket()

        writer = GoogleCloudStorageStateObjectWriter(client=_Client())
        with patch.object(state_publication, "PreconditionFailed", _FakePreconditionFailed):
            with self.assertRaises(StatePublicationError):
                writer.create_or_verify(
                    bucket="my-bucket",
                    object_name=self.object_name,
                    content_bytes=self.content,
                    content_type="application/octet-stream",
                    maximum_bytes=1024,
                )

    def test_second_sdk_error_during_reload_sanitized(self) -> None:
        class _RaisingReloadBlob:
            generation = None

            def upload_from_string(self, *args, **kwargs):
                raise _FakePreconditionFailed("conflict")

            def reload(self, *, retry):
                raise RuntimeError("SECRET-RELOAD-FAILURE-DO-NOT-LEAK-2b7e")

        class _Bucket:
            def blob(self, object_name, generation=None):
                return _RaisingReloadBlob()

        class _Client:
            def bucket(self, name):
                return _Bucket()

        writer = GoogleCloudStorageStateObjectWriter(client=_Client())
        with patch.object(state_publication, "PreconditionFailed", _FakePreconditionFailed):
            try:
                writer.create_or_verify(
                    bucket="my-bucket",
                    object_name=self.object_name,
                    content_bytes=self.content,
                    content_type="application/octet-stream",
                    maximum_bytes=1024,
                )
                self.fail("expected StatePublicationError")
            except StatePublicationError as exc:
                self.assertNotIn("SECRET-RELOAD-FAILURE-DO-NOT-LEAK-2b7e", str(exc))
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)

    def test_second_sdk_error_during_download_sanitized(self) -> None:
        class _RaisingDownloadBlob:
            def __init__(self, generation: int | None) -> None:
                self.generation = generation

            def upload_from_string(self, *args, **kwargs):
                raise _FakePreconditionFailed("conflict")

            def reload(self, *, retry):
                self.generation = 777

            def download_as_bytes(self, **kwargs):
                raise RuntimeError("SECRET-DOWNLOAD-FAILURE-DO-NOT-LEAK-6a3d")

        class _Bucket:
            def blob(self, object_name, generation=None):
                return _RaisingDownloadBlob(generation)

        class _Client:
            def bucket(self, name):
                return _Bucket()

        writer = GoogleCloudStorageStateObjectWriter(client=_Client())
        with patch.object(state_publication, "PreconditionFailed", _FakePreconditionFailed):
            try:
                writer.create_or_verify(
                    bucket="my-bucket",
                    object_name=self.object_name,
                    content_bytes=self.content,
                    content_type="application/octet-stream",
                    maximum_bytes=1024,
                )
                self.fail("expected StatePublicationError")
            except StatePublicationError as exc:
                self.assertNotIn("SECRET-DOWNLOAD-FAILURE-DO-NOT-LEAK-6a3d", str(exc))
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)


class PublishPipelineStateOrderingTests(_PublicationTestCase):
    def test_no_writer_call_before_rebuilt_inventory_equality(self) -> None:
        inventory = self._build_inventory()
        # Mutate a file after the inventory was built so the rebuild check fails.
        _write(self.roots.calendar_data / "a.json", b"mutated-content")
        writer = RecordingWriter()
        with self.assertRaises(StatePublicationError):
            publish_pipeline_state(self.run, inventory, self.roots, "test-bucket", writer)
        self.assertEqual(writer.calls, [])

    def test_blobs_uploaded_in_ascending_unique_sha_order_then_inventory_then_manifest_last(
        self,
    ) -> None:
        inventory = self._build_inventory()
        writer = RecordingWriter()
        completed = publish_pipeline_state(self.run, inventory, self.roots, "test-bucket", writer)

        unique_hashes = sorted({entry.sha256 for entry in inventory.entries})
        self.assertEqual(len(writer.calls), len(unique_hashes) + 2)

        observed_blob_shas = [
            hashlib.sha256(call["content_bytes"]).hexdigest()
            for call in writer.calls[: len(unique_hashes)]
        ]
        self.assertEqual(observed_blob_shas, unique_hashes)
        for call in writer.calls[: len(unique_hashes)]:
            self.assertEqual(call["content_type"], "application/octet-stream")

        inventory_call = writer.calls[len(unique_hashes)]
        self.assertEqual(inventory_call["content_type"], "application/json")
        self.assertIn(inventory.run_id, inventory_call["object_name"])
        self.assertIn(inventory.inventory_id, inventory_call["object_name"])

        manifest_call = writer.calls[-1]
        self.assertEqual(manifest_call["content_type"], "application/json")
        self.assertIn(completed.manifest.publication_id, manifest_call["object_name"])

        self.assertEqual(
            completed.manifest.inventory_object.byte_count, len(inventory_call["content_bytes"])
        )
        self.assertEqual(
            completed.publication_object.byte_count, len(manifest_call["content_bytes"])
        )

    def test_blob_upload_order_is_independent_of_inventory_path_order(self) -> None:
        payloads = sorted(
            (b"path-first-content", b"path-second-content"),
            key=lambda payload: hashlib.sha256(payload).hexdigest(),
            reverse=True,
        )
        _write(self.roots.calendar_data / "a.json", payloads[0])
        _write(self.roots.calendar_data / "b.json", payloads[1])
        inventory = build_pipeline_state_inventory(self.run, self.roots)
        self.assertGreater(inventory.entries[0].sha256, inventory.entries[1].sha256)

        writer = RecordingWriter()
        publish_pipeline_state(self.run, inventory, self.roots, "test-bucket", writer)

        observed_blob_shas = [
            hashlib.sha256(call["content_bytes"]).hexdigest()
            for call in writer.calls[:2]
        ]
        self.assertEqual(observed_blob_shas, sorted(observed_blob_shas))

    def test_duplicate_local_paths_reread_but_one_upload_per_unique_hash(self) -> None:
        inventory = self._build_inventory()
        duplicate_entries = [e for e in inventory.entries if e.sha256 == hashlib.sha256(b"alpha-content").hexdigest()]
        self.assertEqual(len(duplicate_entries), 2)

        writer = RecordingWriter()
        publish_pipeline_state(self.run, inventory, self.roots, "test-bucket", writer)

        blob_object_names = [
            call["object_name"]
            for call in writer.calls
            if call["object_name"].startswith("state/v1/blobs/")
        ]
        self.assertEqual(len(blob_object_names), len(set(blob_object_names)))

    def test_completed_result_generations_match_writer(self) -> None:
        inventory = self._build_inventory()
        writer = RecordingWriter()
        completed = publish_pipeline_state(self.run, inventory, self.roots, "test-bucket", writer)
        self.assertTrue(all(blob.generation > 0 for blob in completed.manifest.blob_objects))
        self.assertGreater(completed.manifest.inventory_object.generation, 0)
        self.assertGreater(completed.publication_object.generation, 0)


class PublishPipelineStateFailureTests(_PublicationTestCase):
    def test_first_blob_writer_exception_stops_after_one_call(self) -> None:
        inventory = self._build_inventory()
        writer = RecordingWriter(fail_at=0)
        with self.assertRaises(StatePublicationError):
            publish_pipeline_state(self.run, inventory, self.roots, "test-bucket", writer)
        self.assertEqual(len(writer.calls), 1)
        self.assertTrue(writer.calls[0]["object_name"].startswith("state/v1/blobs/"))

    def test_missing_local_file_rejected_before_any_writer_call(self) -> None:
        inventory = self._build_inventory()
        (self.roots.calendar_data / "a.json").unlink()
        writer = RecordingWriter()
        with self.assertRaises(StatePublicationError):
            publish_pipeline_state(self.run, inventory, self.roots, "test-bucket", writer)
        self.assertEqual(writer.calls, [])

    def test_mutated_local_file_content_rejected_before_any_writer_call(self) -> None:
        inventory = self._build_inventory()
        _write(self.roots.calendar_data / "a.json", b"a-completely-different-payload")
        writer = RecordingWriter()
        with self.assertRaises(StatePublicationError):
            publish_pipeline_state(self.run, inventory, self.roots, "test-bucket", writer)
        self.assertEqual(writer.calls, [])

    def test_extra_untracked_file_causes_rebuild_mismatch(self) -> None:
        inventory = self._build_inventory()
        _write(self.roots.calendar_data / "extra-new-file.json", b"unexpected")
        writer = RecordingWriter()
        with self.assertRaises(StatePublicationError):
            publish_pipeline_state(self.run, inventory, self.roots, "test-bucket", writer)
        self.assertEqual(writer.calls, [])

    def test_middle_blob_writer_exception_stops_before_inventory_or_manifest(self) -> None:
        inventory = self._build_inventory()
        unique_hash_count = len({entry.sha256 for entry in inventory.entries})
        self.assertGreaterEqual(unique_hash_count, 2)
        writer = RecordingWriter(fail_at=1)
        try:
            publish_pipeline_state(self.run, inventory, self.roots, "test-bucket", writer)
            self.fail("expected StatePublicationError")
        except StatePublicationError as exc:
            self.assertNotIn("SECRET-WRITER-FAILURE-DO-NOT-LEAK-4c1a", str(exc))
            self.assertIsNone(exc.__cause__)
            self.assertIsNone(exc.__context__)
        self.assertEqual(len(writer.calls), 2)
        for call in writer.calls:
            self.assertTrue(call["object_name"].startswith("state/v1/blobs/"))

    def test_malicious_writer_wrong_object_name_rejected(self) -> None:
        inventory = self._build_inventory()
        writer = RecordingWriter(malicious_at=0)
        with self.assertRaises(StatePublicationError):
            publish_pipeline_state(self.run, inventory, self.roots, "test-bucket", writer)

    def test_final_manifest_writer_failure_returns_no_completion(self) -> None:
        inventory = self._build_inventory()
        unique_hash_count = len({entry.sha256 for entry in inventory.entries})
        final_index = unique_hash_count + 1
        writer = RecordingWriter(fail_at=final_index)
        with self.assertRaises(StatePublicationError):
            publish_pipeline_state(self.run, inventory, self.roots, "test-bucket", writer)
        # Blobs and inventory were already (uncommitted-orphan) uploaded;
        # only the final manifest call failed.
        self.assertEqual(len(writer.calls), final_index + 1)

    def test_run_type_rejected(self) -> None:
        inventory = self._build_inventory()
        writer = RecordingWriter()
        with self.assertRaises(StatePublicationError):
            publish_pipeline_state("not-a-run", inventory, self.roots, "test-bucket", writer)  # type: ignore[arg-type]
        self.assertEqual(writer.calls, [])

    def test_inventory_type_rejected(self) -> None:
        writer = RecordingWriter()
        with self.assertRaises(StatePublicationError):
            publish_pipeline_state(self.run, "not-an-inventory", self.roots, "test-bucket", writer)  # type: ignore[arg-type]
        self.assertEqual(writer.calls, [])

    def test_roots_type_rejected(self) -> None:
        inventory = self._build_inventory()
        writer = RecordingWriter()
        with self.assertRaises(StatePublicationError):
            publish_pipeline_state(self.run, inventory, "not-roots", "test-bucket", writer)  # type: ignore[arg-type]
        self.assertEqual(writer.calls, [])

    def test_invalid_bucket_rejected(self) -> None:
        inventory = self._build_inventory()
        writer = RecordingWriter()
        with self.assertRaises(StatePublicationError):
            publish_pipeline_state(self.run, inventory, self.roots, "INVALID_BUCKET", writer)
        self.assertEqual(writer.calls, [])

    def test_writer_attribute_failure_is_sanitized(self) -> None:
        inventory = self._build_inventory()

        class _PoisonedWriter:
            @property
            def create_or_verify(self):
                raise RuntimeError("SECRET-WRITER-ATTRIBUTE-DO-NOT-LEAK")

        try:
            publish_pipeline_state(
                self.run, inventory, self.roots, "test-bucket", _PoisonedWriter()
            )
            self.fail("expected StatePublicationError")
        except StatePublicationError as exc:
            self.assertNotIn("SECRET-WRITER-ATTRIBUTE-DO-NOT-LEAK", str(exc))
            self.assertIsNone(exc.__cause__)
            self.assertIsNone(exc.__context__)


class CapabilityLockTests(unittest.TestCase):
    _ALLOWED_IMPORTS = frozenset(
        {
            "__future__",
            "hashlib",
            "json",
            "re",
            "dataclasses",
            "datetime",
            "pathlib",
            "typing",
            "india_swing",
            "google",
        }
    )
    _FORBIDDEN_TOKENS = (
        "list_blobs",
        "list_buckets",
        "get_bucket",
        "latest",
        "delete",
        "rewrite",
        "compose",
        "copy_blob",
        "broker",
        "place_order",
        "submit_order",
        "notification",
        "scheduler",
        "socket",
        "subprocess",
        "tempfile",
        "shutil",
        "environ",
        "getenv",
        "requests",
        "urllib",
        "unlink",
        "remove",
        "rename",
        "rmdir",
        "rmtree",
        "mkstemp",
        "mkdtemp",
        "popen",
        "system",
        "exec",
        "eval",
    )
    _EXACT_FORBIDDEN_NAMES = frozenset({"now", "open"})

    def _module_ast(self) -> ast.Module:
        source = _MODULE_PATH.read_text(encoding="utf-8")
        return ast.parse(source)

    def test_imports_match_an_exact_allowlist(self) -> None:
        tree = self._module_ast()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    top = alias.name.split(".")[0]
                    self.assertIn(top, self._ALLOWED_IMPORTS, alias.name)
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if node.level > 0:
                    continue
                top = module.split(".")[0]
                self.assertIn(top, self._ALLOWED_IMPORTS, module)

    def test_identifiers_carry_no_disallowed_capability_token(self) -> None:
        tree = self._module_ast()
        for node in ast.walk(tree):
            name = None
            if isinstance(node, ast.Name):
                name = node.id
            elif isinstance(node, ast.Attribute):
                name = node.attr
            if name is None:
                continue
            lowered = name.lower()
            if lowered in self._EXACT_FORBIDDEN_NAMES:
                self.fail(f"forbidden exact identifier used: {name}")
            for token in self._FORBIDDEN_TOKENS:
                self.assertNotIn(token, lowered, name)

    def test_no_import_time_side_effect_calls_at_module_scope(self) -> None:
        tree = self._module_ast()
        for node in tree.body:
            if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
                self.fail("module-level call expression found")

    def test_writer_protocol_exposes_only_create_or_verify(self) -> None:
        tree = self._module_ast()
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name == "StateObjectWriter":
                method_names = [
                    item.name
                    for item in node.body
                    if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
                ]
                self.assertEqual(method_names, ["create_or_verify"])
                return
        self.fail("StateObjectWriter Protocol class not found")


if __name__ == "__main__":
    unittest.main()
