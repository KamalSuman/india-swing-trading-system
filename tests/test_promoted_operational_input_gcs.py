from __future__ import annotations

import ast
import hashlib
import inspect
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import india_swing.promoted_operational_input_gcs as gcs_module
from india_swing.daily_pipeline.acquisition import GCSObjectPayload
from india_swing.daily_pipeline.state_publication import PublishedStateObject
from india_swing.promoted_operational_assembly import encode_promoted_operational_assembly_spec
from india_swing.promoted_operational_cloud_control import PromotedOperationalCloudRunControl
from india_swing.promoted_operational_input_gcs import (
    AcquiredPromotedOperationalInputSnapshot,
    CompletedPromotedOperationalInputPublication,
    PromotedOperationalInputGCSError,
    PromotedOperationalInputRestoreRequest,
    acquire_promoted_operational_input_snapshot,
    hydrate_promoted_operational_input_snapshot,
    publish_promoted_operational_input_snapshot,
)
from india_swing.promoted_operational_input_snapshot import ROOT_INPUT_NAMES

from tests import test_promoted_operational_job as _job_tests


def _roots(base: Path) -> dict[str, Path]:
    return {name: base / name.replace("_", "-") for name in ROOT_INPUT_NAMES}


class FakeWriter:
    def __init__(self) -> None:
        self.store: dict[tuple[str, str], tuple[bytes, int]] = {}
        self.calls: list[str] = []
        self.raise_error: Exception | None = None
        self.malicious_result: object = None

    def create_or_verify(self, *, bucket, object_name, content_bytes, content_type, maximum_bytes):
        self.calls.append(object_name)
        if self.raise_error is not None:
            raise self.raise_error
        if self.malicious_result is not None:
            return self.malicious_result
        key = (bucket, object_name)
        if key in self.store:
            existing_bytes, generation = self.store[key]
            if existing_bytes != content_bytes:
                raise ValueError("writer content mismatch at existing path")
            return PublishedStateObject(
                object_name=object_name, generation=generation,
                byte_count=len(existing_bytes), sha256=hashlib.sha256(existing_bytes).hexdigest(),
            )
        generation = len(self.store) + 1
        self.store[key] = (content_bytes, generation)
        return PublishedStateObject(
            object_name=object_name, generation=generation,
            byte_count=len(content_bytes), sha256=hashlib.sha256(content_bytes).hexdigest(),
        )


class FakeReader:
    def __init__(self, writer: FakeWriter) -> None:
        self.writer = writer
        self.calls: list[dict[str, object]] = []
        self.raise_error: Exception | None = None
        self.override: dict[str, object] | None = None

    def read_generation(self, *, bucket, object_name, generation, maximum_bytes):
        self.calls.append(
            dict(bucket=bucket, object_name=object_name, generation=generation, maximum_bytes=maximum_bytes)
        )
        if self.raise_error is not None:
            raise self.raise_error
        if self.override is not None and self.override.get("object_name") == object_name:
            return self.override["payload"]
        content_bytes, stored_generation = self.writer.store[(bucket, object_name)]
        if stored_generation != generation:
            raise ValueError("simulated generation mismatch")
        return GCSObjectPayload(content_bytes=content_bytes, generation=generation)


class _Fixture:
    def __init__(self, tmp_path: Path, *, open_positions: int = 0) -> None:
        self.tmp_path = tmp_path
        tmp_path.mkdir(parents=True, exist_ok=True)
        _preparation, _run_spec, _portfolio, _artifact, self.spec = _job_tests._fixture(
            open_positions=open_positions
        )
        self.assembly_spec_file = tmp_path / "assembly.json"
        self.assembly_spec_file.write_bytes(encode_promoted_operational_assembly_spec(self.spec))
        self.state_root = tmp_path / "state"
        self.roots = _roots(tmp_path / "src")
        for path in self.roots.values():
            path.mkdir(parents=True, exist_ok=True)
        self.writer = FakeWriter()

    def control(self, **overrides) -> PromotedOperationalCloudRunControl:
        kwargs: dict[str, object] = dict(
            expected_assembly_spec_id=self.spec.assembly_spec_id,
            expected_operational_run_spec_id="b" * 64,
            target_session=self.spec.target_session,
            state_bucket=self.spec.binding_bucket,
            assembly_spec_file=self.assembly_spec_file,
            state_root=self.state_root,
            **self.roots,
        )
        kwargs.update(overrides)
        return PromotedOperationalCloudRunControl(**kwargs)

    def destination_control(self, prefix: str = "dst") -> PromotedOperationalCloudRunControl:
        # All twelve destinations must share one existing common parent
        # directory (the new revision-2 hydration requirement); state_root
        # deliberately lives OUTSIDE that common parent since it must never
        # participate in staging/common-parent identity.
        common = self.tmp_path / f"{prefix}-common"
        common.mkdir(parents=True, exist_ok=True)
        dest_roots = {name: common / name.replace("_", "-") for name in ROOT_INPUT_NAMES}
        return PromotedOperationalCloudRunControl(
            expected_assembly_spec_id=self.spec.assembly_spec_id,
            expected_operational_run_spec_id="b" * 64,
            target_session=self.spec.target_session,
            state_bucket=self.spec.binding_bucket,
            assembly_spec_file=common / "assembly.json",
            state_root=self.tmp_path / f"{prefix}-state",
            **dest_roots,
        )

    def publish(self, **kwargs) -> CompletedPromotedOperationalInputPublication:
        return publish_promoted_operational_input_snapshot(
            control=self.control(), bucket=self.spec.binding_bucket, writer=self.writer, **kwargs
        )

    def restore_request(self, publication: CompletedPromotedOperationalInputPublication) -> PromotedOperationalInputRestoreRequest:
        manifest_object = publication.manifest_object
        return PromotedOperationalInputRestoreRequest(
            bucket=self.spec.binding_bucket,
            manifest_object_name=manifest_object.object_name,
            generation=manifest_object.generation,
            expected_sha256=manifest_object.sha256,
            expected_snapshot_id=publication.manifest.snapshot_id,
            expected_assembly_spec_id=self.spec.assembly_spec_id,
            target_session=self.spec.target_session,
        )


class PublicationTests(unittest.TestCase):
    def test_writes_deduplicated_blobs_then_manifest_last(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "a.txt").write_bytes(b"same")
            (fx.roots["promoted_root"] / "b.txt").write_bytes(b"same")
            (fx.roots["engine_run_root"] / "c.txt").write_bytes(b"different")
            publication = fx.publish()
            # assembly file + 2 unique blobs (a/b share content) + manifest = 4 writes
            self.assertEqual(len(fx.writer.calls), 4)
            self.assertIn("/manifests/", fx.writer.calls[-1])
            for call in fx.writer.calls[:-1]:
                self.assertIn("/blobs/", call)

    def test_deterministic_object_names(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            publication = fx.publish()
            expected_prefix = (
                f"promoted-operational-input/v1/{fx.spec.target_session.isoformat()}/"
                f"{fx.spec.assembly_spec_id}/manifests/"
            )
            self.assertTrue(publication.manifest_object.object_name.startswith(expected_prefix))
            for blob in publication.manifest.blob_objects:
                self.assertTrue(
                    blob.object_name.startswith(
                        f"promoted-operational-input/v1/{fx.spec.target_session.isoformat()}/"
                        f"{fx.spec.assembly_spec_id}/blobs/"
                    )
                )

    def test_publish_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "a.txt").write_bytes(b"x")
            publication1 = fx.publish()
            publication2 = fx.publish()
            self.assertEqual(publication1.manifest.snapshot_id, publication2.manifest.snapshot_id)

    def test_rejects_malicious_published_state_object_return(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            fx.writer.malicious_result = "not-a-published-state-object"
            with self.assertRaises(PromotedOperationalInputGCSError):
                fx.publish()

    def test_writer_failure_at_first_blob_stops_before_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            fx.writer.raise_error = RuntimeError("boom")
            with self.assertRaises(PromotedOperationalInputGCSError):
                fx.publish()
            self.assertEqual(len(fx.writer.calls), 1)
            self.assertFalse(any("/manifests/" in call for call in fx.writer.calls))

    def test_writer_failure_at_manifest_stops_after_all_blobs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "a.txt").write_bytes(b"x")
            original_create = fx.writer.create_or_verify

            def _fail_on_manifest(*, bucket, object_name, content_bytes, content_type, maximum_bytes):
                if "/manifests/" in object_name:
                    fx.writer.calls.append(object_name)
                    raise RuntimeError("boom")
                return original_create(
                    bucket=bucket, object_name=object_name, content_bytes=content_bytes,
                    content_type=content_type, maximum_bytes=maximum_bytes,
                )

            fx.writer.create_or_verify = _fail_on_manifest
            with self.assertRaises(PromotedOperationalInputGCSError):
                fx.publish()
            self.assertEqual(len(fx.writer.calls), 3)  # assembly-spec blob + one file blob + manifest attempt

    def test_wrong_object_name_or_hash_from_writer_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())

            def _wrong_name(*, bucket, object_name, content_bytes, content_type, maximum_bytes):
                return PublishedStateObject(
                    object_name="promoted-operational-input/v1/wrong.json",
                    generation=1, byte_count=len(content_bytes),
                    sha256=hashlib.sha256(content_bytes).hexdigest(),
                )

            fx.writer.create_or_verify = _wrong_name
            with self.assertRaises(PromotedOperationalInputGCSError):
                fx.publish()


class RestoreTests(unittest.TestCase):
    def test_restore_reaches_manifest_then_every_unique_blob_with_exact_generation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "a.txt").write_bytes(b"x")
            publication = fx.publish()
            reader = FakeReader(fx.writer)
            request = fx.restore_request(publication)
            acquired = acquire_promoted_operational_input_snapshot(request=request, reader=reader)
            self.assertEqual(reader.calls[0]["object_name"], request.manifest_object_name)
            for call in reader.calls:
                self.assertGreater(call["generation"], 0)
                self.assertGreater(call["maximum_bytes"], 0)
            self.assertEqual(len(acquired.blobs), len(publication.manifest.blob_objects))

    def test_externally_pinned_manifest_hash_is_authoritative(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            publication = fx.publish()
            reader = FakeReader(fx.writer)
            request = PromotedOperationalInputRestoreRequest(
                bucket=fx.spec.binding_bucket, manifest_object_name=publication.manifest_object.object_name,
                generation=publication.manifest_object.generation, expected_sha256="9" * 64,
                expected_snapshot_id=publication.manifest.snapshot_id,
                expected_assembly_spec_id=fx.spec.assembly_spec_id, target_session=fx.spec.target_session,
            )
            with self.assertRaises(PromotedOperationalInputGCSError):
                acquire_promoted_operational_input_snapshot(request=request, reader=reader)

    def test_wrong_manifest_generation_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            publication = fx.publish()
            reader = FakeReader(fx.writer)
            request = PromotedOperationalInputRestoreRequest(
                bucket=fx.spec.binding_bucket, manifest_object_name=publication.manifest_object.object_name,
                generation=publication.manifest_object.generation + 1,
                expected_sha256=publication.manifest_object.sha256,
                expected_snapshot_id=publication.manifest.snapshot_id,
                expected_assembly_spec_id=fx.spec.assembly_spec_id, target_session=fx.spec.target_session,
            )
            with self.assertRaises(PromotedOperationalInputGCSError):
                acquire_promoted_operational_input_snapshot(request=request, reader=reader)

    def test_wrong_expected_snapshot_id_is_rejected_at_request_construction(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            publication = fx.publish()
            with self.assertRaises(PromotedOperationalInputGCSError):
                PromotedOperationalInputRestoreRequest(
                    bucket=fx.spec.binding_bucket, manifest_object_name=publication.manifest_object.object_name,
                    generation=publication.manifest_object.generation,
                    expected_sha256=publication.manifest_object.sha256, expected_snapshot_id="9" * 64,
                    expected_assembly_spec_id=fx.spec.assembly_spec_id, target_session=fx.spec.target_session,
                )

    def test_truncated_manifest_object_name_is_rejected(self) -> None:
        with self.assertRaises(PromotedOperationalInputGCSError):
            PromotedOperationalInputRestoreRequest(
                bucket="a-bucket", manifest_object_name="promoted-operational-input/v1/not-a-real-path.json",
                generation=1, expected_sha256="a" * 64, expected_snapshot_id="b" * 64,
                expected_assembly_spec_id="c" * 64,
                target_session=__import__("datetime").date(2026, 1, 1),
            )

    def test_wrong_artifact_generation_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "a.txt").write_bytes(b"x")
            publication = fx.publish()
            reader = FakeReader(fx.writer)
            first_blob = publication.manifest.blob_objects[0]
            reader.override = {
                "object_name": first_blob.object_name,
                "payload": GCSObjectPayload(
                    content_bytes=fx.writer.store[(fx.spec.binding_bucket, first_blob.object_name)][0],
                    generation=999,
                ),
            }
            request = fx.restore_request(publication)
            with self.assertRaises(PromotedOperationalInputGCSError):
                acquire_promoted_operational_input_snapshot(request=request, reader=reader)

    def test_tampered_blob_content_fails_hash_check(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "a.txt").write_bytes(b"x")
            publication = fx.publish()
            reader = FakeReader(fx.writer)
            first_blob = publication.manifest.blob_objects[0]
            key = (fx.spec.binding_bucket, first_blob.object_name)
            tampered_bytes = fx.writer.store[key][0] + b"tampered"
            reader.override = {
                "object_name": first_blob.object_name,
                "payload": GCSObjectPayload(content_bytes=tampered_bytes, generation=fx.writer.store[key][1]),
            }
            request = fx.restore_request(publication)
            with self.assertRaises(PromotedOperationalInputGCSError):
                acquire_promoted_operational_input_snapshot(request=request, reader=reader)

    def test_reader_failure_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            publication = fx.publish()
            reader = FakeReader(fx.writer)
            reader.raise_error = RuntimeError("boom")
            request = fx.restore_request(publication)
            with self.assertRaises(PromotedOperationalInputGCSError):
                acquire_promoted_operational_input_snapshot(request=request, reader=reader)

    def test_cross_linked_foreign_bundle_is_rejected(self) -> None:
        # Use a genuinely different assembly (open_positions=1 changes the
        # content-addressed assembly_spec_id) so fixture B's manifest is
        # not merely a byte-identical duplicate of fixture A's.
        with tempfile.TemporaryDirectory() as outer:
            base = Path(outer).resolve()
            fx_a = _Fixture(base / "a")
            fx_b = _Fixture(base / "b", open_positions=1)
            self.assertNotEqual(fx_a.spec.assembly_spec_id, fx_b.spec.assembly_spec_id)
            publication_a = fx_a.publish()
            publication_b = fx_b.publish()
            reader_a = FakeReader(fx_a.writer)
            for key, value in fx_b.writer.store.items():
                reader_a.writer.store.setdefault(key, value)
            with self.assertRaises(PromotedOperationalInputGCSError):
                PromotedOperationalInputRestoreRequest(
                    bucket=fx_a.spec.binding_bucket,
                    manifest_object_name=publication_b.manifest_object.object_name,
                    generation=publication_b.manifest_object.generation,
                    expected_sha256=publication_b.manifest_object.sha256,
                    expected_snapshot_id=publication_b.manifest.snapshot_id,
                    expected_assembly_spec_id=fx_a.spec.assembly_spec_id,
                    target_session=fx_a.spec.target_session,
                )


class HydrationTests(unittest.TestCase):
    def test_full_round_trip_hydrates_exact_destinations(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "sub").mkdir(parents=True, exist_ok=True)
            (fx.roots["reference_root"] / "sub" / "a.txt").write_bytes(b"hello")
            (fx.roots["promoted_root"] / "b.json").write_bytes(b'{"x":1}')
            publication = fx.publish()
            reader = FakeReader(fx.writer)
            request = fx.restore_request(publication)
            acquired = acquire_promoted_operational_input_snapshot(request=request, reader=reader)

            dest_control = fx.destination_control()
            restore = hydrate_promoted_operational_input_snapshot(control=dest_control, acquired=acquired)
            self.assertEqual(restore.manifest.snapshot_id, publication.manifest.snapshot_id)
            self.assertEqual(
                (dest_control.reference_root / "sub" / "a.txt").read_bytes(), b"hello"
            )
            self.assertEqual((dest_control.promoted_root / "b.json").read_bytes(), b'{"x":1}')
            self.assertEqual(
                dest_control.assembly_spec_file.read_bytes(), fx.assembly_spec_file.read_bytes()
            )
            self.assertFalse(dest_control.state_root.exists())

    def test_empty_roots_are_created_and_remain_empty(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            publication = fx.publish()
            reader = FakeReader(fx.writer)
            request = fx.restore_request(publication)
            acquired = acquire_promoted_operational_input_snapshot(request=request, reader=reader)
            dest_control = fx.destination_control()
            hydrate_promoted_operational_input_snapshot(control=dest_control, acquired=acquired)
            self.assertTrue(dest_control.calendar_root.is_dir())
            self.assertEqual(list(dest_control.calendar_root.iterdir()), [])

    def test_state_root_is_never_created_or_touched(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "a.txt").write_bytes(b"x")
            publication = fx.publish()
            reader = FakeReader(fx.writer)
            request = fx.restore_request(publication)
            acquired = acquire_promoted_operational_input_snapshot(request=request, reader=reader)
            dest_control = fx.destination_control()
            hydrate_promoted_operational_input_snapshot(control=dest_control, acquired=acquired)
            self.assertFalse(dest_control.state_root.exists())

    def test_preexisting_assembly_spec_file_fails_before_any_write(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            publication = fx.publish()
            reader = FakeReader(fx.writer)
            request = fx.restore_request(publication)
            acquired = acquire_promoted_operational_input_snapshot(request=request, reader=reader)
            dest_control = fx.destination_control()
            dest_control.assembly_spec_file.write_bytes(b"already here")
            with self.assertRaises(PromotedOperationalInputGCSError):
                hydrate_promoted_operational_input_snapshot(control=dest_control, acquired=acquired)
            for root_name in ROOT_INPUT_NAMES:
                self.assertFalse(getattr(dest_control, root_name).exists())

    def test_nonempty_root_destination_fails_before_any_write(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "a.txt").write_bytes(b"x")
            publication = fx.publish()
            reader = FakeReader(fx.writer)
            request = fx.restore_request(publication)
            acquired = acquire_promoted_operational_input_snapshot(request=request, reader=reader)
            dest_control = fx.destination_control()
            dest_control.promoted_root.mkdir(parents=True, exist_ok=True)
            (dest_control.promoted_root / "existing.txt").write_bytes(b"already here")
            with self.assertRaises(PromotedOperationalInputGCSError):
                hydrate_promoted_operational_input_snapshot(control=dest_control, acquired=acquired)
            self.assertFalse(dest_control.assembly_spec_file.exists())

    def test_forced_write_failure_returns_only_static_error(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "a.txt").write_bytes(b"x")
            publication = fx.publish()
            reader = FakeReader(fx.writer)
            request = fx.restore_request(publication)
            acquired = acquire_promoted_operational_input_snapshot(request=request, reader=reader)
            dest_control = fx.destination_control()

            with mock.patch.object(gcs_module, "_write_verified_file", side_effect=OSError("disk full")):
                with self.assertRaises(PromotedOperationalInputGCSError):
                    hydrate_promoted_operational_input_snapshot(control=dest_control, acquired=acquired)


class Revision2HydrationSafetyTests(unittest.TestCase):
    def _acquired(self, fx: _Fixture):
        (fx.roots["reference_root"] / "a.txt").write_bytes(b"x")
        publication = fx.publish()
        reader = FakeReader(fx.writer)
        request = fx.restore_request(publication)
        return acquire_promoted_operational_input_snapshot(request=request, reader=reader)

    def test_preexisting_empty_root_directory_now_fails_unlike_revision_1(self) -> None:
        # Revision 1 allowed an existing EMPTY root directory; revision 2
        # requires every destination to be exactly absent.
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            acquired = self._acquired(fx)
            dest_control = fx.destination_control()
            dest_control.calendar_root.mkdir(parents=True, exist_ok=True)
            with self.assertRaises(PromotedOperationalInputGCSError):
                hydrate_promoted_operational_input_snapshot(control=dest_control, acquired=acquired)
            self.assertFalse(dest_control.assembly_spec_file.exists())

    def test_destinations_without_a_common_parent_fail_before_staging(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            acquired = self._acquired(fx)
            dest_control = fx.destination_control()
            # Move one root to a sibling directory outside the common parent.
            foreign_root = fx.tmp_path / "foreign" / "promoted-root"
            object.__setattr__(dest_control, "promoted_root", foreign_root)
            with mock.patch.object(gcs_module, "tempfile") as tempfile_mock:
                with self.assertRaises(PromotedOperationalInputGCSError):
                    hydrate_promoted_operational_input_snapshot(control=dest_control, acquired=acquired)
                tempfile_mock.mkdtemp.assert_not_called()

    def test_no_existing_common_parent_fails_before_staging(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            acquired = self._acquired(fx)
            dest_control = fx.destination_control()
            # The common parent directory itself was never created.
            import shutil

            shutil.rmtree(dest_control.assembly_spec_file.parent)
            with self.assertRaises(PromotedOperationalInputGCSError):
                hydrate_promoted_operational_input_snapshot(control=dest_control, acquired=acquired)

    def test_stage_verification_failure_writes_nothing_to_real_destinations(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            acquired = self._acquired(fx)
            dest_control = fx.destination_control()

            def _raise(_inventory):
                raise RuntimeError("boom")

            with mock.patch.object(
                gcs_module, "encode_promoted_operational_input_inventory", side_effect=_raise
            ):
                with self.assertRaises(Exception):
                    hydrate_promoted_operational_input_snapshot(control=dest_control, acquired=acquired)
            self.assertFalse(dest_control.assembly_spec_file.exists())
            for root_name in ROOT_INPUT_NAMES:
                self.assertFalse(getattr(dest_control, root_name).exists())

    def test_forced_rename_failure_yields_no_completed_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            acquired = self._acquired(fx)
            dest_control = fx.destination_control()

            original_rename = gcs_module.os.rename
            calls = {"count": 0}

            def _fail_second_rename(source, destination):
                calls["count"] += 1
                if calls["count"] == 2:
                    raise OSError("simulated rename failure")
                return original_rename(source, destination)

            with mock.patch.object(gcs_module.os, "rename", side_effect=_fail_second_rename):
                with self.assertRaises(PromotedOperationalInputGCSError):
                    hydrate_promoted_operational_input_snapshot(control=dest_control, acquired=acquired)
            self.assertGreaterEqual(calls["count"], 2)
            # No repair/cleanup is attempted: the first rename's destination
            # is left in place (not deleted) even though the overall
            # operation reports no completed result.
            self.assertTrue(dest_control.assembly_spec_file.exists())

    def test_successful_hydration_reaches_exactly_twelve_renames(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            acquired = self._acquired(fx)
            dest_control = fx.destination_control()

            original_rename = gcs_module.os.rename
            calls: list[tuple[object, object]] = []

            def _tracked_rename(source, destination):
                calls.append((source, destination))
                return original_rename(source, destination)

            with mock.patch.object(gcs_module.os, "rename", side_effect=_tracked_rename):
                hydrate_promoted_operational_input_snapshot(control=dest_control, acquired=acquired)
            self.assertEqual(len(calls), 12)


class Revision2PublicationAuthorityTests(unittest.TestCase):
    def test_malformed_foreign_assembly_causes_zero_writer_calls(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            fx.assembly_spec_file.write_bytes(b"not a valid assembly spec")
            with self.assertRaises(PromotedOperationalInputGCSError):
                fx.publish()
            self.assertEqual(fx.writer.calls, [])

    def test_assembly_mutation_between_inventory_and_capability_causes_zero_writer_calls(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            original_build = gcs_module.build_promoted_operational_input_inventory

            def _mutate_then_build(control):
                result = original_build(control)
                fx.assembly_spec_file.write_bytes(b"mutated after inventory build")
                return result

            with mock.patch.object(
                gcs_module, "build_promoted_operational_input_inventory", side_effect=_mutate_then_build
            ):
                with self.assertRaises(PromotedOperationalInputGCSError):
                    fx.publish()
            self.assertEqual(fx.writer.calls, [])

    def test_writer_time_control_mutation_suppresses_the_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "a.txt").write_bytes(b"x")
            control = fx.control()

            original_create = fx.writer.create_or_verify

            def _mutate_then_create(*, bucket, object_name, content_bytes, content_type, maximum_bytes):
                result = original_create(
                    bucket=bucket, object_name=object_name, content_bytes=content_bytes,
                    content_type=content_type, maximum_bytes=maximum_bytes,
                )
                object.__setattr__(control, "expected_assembly_spec_id", "9" * 64)
                return result

            fx.writer.create_or_verify = _mutate_then_create
            with self.assertRaises(PromotedOperationalInputGCSError):
                publish_promoted_operational_input_snapshot(
                    control=control, bucket=fx.spec.binding_bucket, writer=fx.writer,
                )
            self.assertFalse(any("/manifests/" in call for call in fx.writer.calls))

    def test_writer_time_path_mutation_cannot_redirect_blob_upload(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "a.txt").write_bytes(b"x")
            control = fx.control()

            decoy_root = fx.tmp_path / "decoy"
            decoy_root.mkdir(parents=True, exist_ok=True)
            (decoy_root / "a.txt").write_bytes(b"DECOY-CONTENT")

            first_call = {"done": False}
            original_create = fx.writer.create_or_verify

            def _mutate_then_create(*, bucket, object_name, content_bytes, content_type, maximum_bytes):
                if not first_call["done"]:
                    first_call["done"] = True
                    object.__setattr__(control, "reference_root", decoy_root)
                return original_create(
                    bucket=bucket, object_name=object_name, content_bytes=content_bytes,
                    content_type=content_type, maximum_bytes=maximum_bytes,
                )

            fx.writer.create_or_verify = _mutate_then_create
            # The redirect attempt is separately caught by the
            # end-of-publication rebaseline check (suppressing the
            # manifest) -- but critically, the DECOY content must never
            # have been uploaded in the first place, since every
            # entry-to-path mapping was precomputed before the writer was
            # ever exposed.
            with self.assertRaises(PromotedOperationalInputGCSError):
                publish_promoted_operational_input_snapshot(
                    control=control, bucket=fx.spec.binding_bucket, writer=fx.writer,
                )
            for (bucket_name, object_name), (content_bytes, _generation) in fx.writer.store.items():
                self.assertNotEqual(content_bytes, b"DECOY-CONTENT")

    def test_retained_manifest_is_a_freshly_reconstructed_instance_not_the_caller_alias(self) -> None:
        # Every aggregate that retains a manifest must store a freshly
        # reconstructed exact instance -- never the caller-owned alias
        # itself, which a later object.__setattr__ on that alias could
        # otherwise still corrupt after this boundary already trusted it.
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "a.txt").write_bytes(b"x")
            publication = fx.publish()
            original_manifest = publication.manifest

            # Reconstructing CompletedPromotedOperationalInputPublication
            # from an already-trusted manifest still yields a fresh,
            # independently-verified instance rather than the alias.
            recheck = CompletedPromotedOperationalInputPublication(
                manifest=original_manifest, manifest_object=publication.manifest_object,
            )
            self.assertIsNot(recheck.manifest, original_manifest)

            reader = FakeReader(fx.writer)
            request = fx.restore_request(publication)
            acquired = acquire_promoted_operational_input_snapshot(request=request, reader=reader)
            self.assertIsNot(acquired.manifest, original_manifest)

            dest_control = fx.destination_control()
            restore = hydrate_promoted_operational_input_snapshot(control=dest_control, acquired=acquired)
            self.assertIsNot(restore.manifest, original_manifest)
            self.assertIsNot(restore.manifest, acquired.manifest)


class Revision3StagingIdentityTests(unittest.TestCase):
    def _acquired(self, fx: _Fixture):
        (fx.roots["reference_root"] / "a.txt").write_bytes(b"x")
        publication = fx.publish()
        reader = FakeReader(fx.writer)
        request = fx.restore_request(publication)
        return acquire_promoted_operational_input_snapshot(request=request, reader=reader)

    def test_parent_identity_replacement_before_mkdtemp_prevents_any_staging(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            acquired = self._acquired(fx)
            dest_control = fx.destination_control()

            original_check = gcs_module._verify_common_parent_identity
            calls = {"count": 0}

            def _fail_first(parent, expected_identity):
                calls["count"] += 1
                if calls["count"] == 1:
                    raise PromotedOperationalInputGCSError("simulated parent replacement before mkdtemp")
                return original_check(parent, expected_identity)

            with mock.patch.object(gcs_module, "_verify_common_parent_identity", side_effect=_fail_first):
                with mock.patch.object(gcs_module.tempfile, "mkdtemp") as mkdtemp_mock:
                    with self.assertRaises(PromotedOperationalInputGCSError):
                        hydrate_promoted_operational_input_snapshot(control=dest_control, acquired=acquired)
                    mkdtemp_mock.assert_not_called()
            self.assertFalse(dest_control.assembly_spec_file.exists())

    def test_parent_identity_replacement_immediately_after_mkdtemp_prevents_staged_writes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            acquired = self._acquired(fx)
            dest_control = fx.destination_control()

            original_check = gcs_module._verify_common_parent_identity
            calls = {"count": 0}

            def _fail_second(parent, expected_identity):
                calls["count"] += 1
                if calls["count"] == 2:
                    raise PromotedOperationalInputGCSError("simulated parent replacement after mkdtemp")
                return original_check(parent, expected_identity)

            with mock.patch.object(gcs_module, "_verify_common_parent_identity", side_effect=_fail_second):
                with mock.patch.object(gcs_module, "_write_verified_file") as write_mock:
                    with self.assertRaises(PromotedOperationalInputGCSError):
                        hydrate_promoted_operational_input_snapshot(control=dest_control, acquired=acquired)
                    write_mock.assert_not_called()
            self.assertEqual(calls["count"], 2)
            self.assertFalse(dest_control.assembly_spec_file.exists())

    def test_staging_identity_replacement_before_staged_tree_creation_prevents_writes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            acquired = self._acquired(fx)
            dest_control = fx.destination_control()

            original_check = gcs_module._verify_staging_identity
            calls = {"count": 0}

            def _fail_first(staging, expected_identity):
                calls["count"] += 1
                if calls["count"] == 1:
                    raise PromotedOperationalInputGCSError("simulated staging replacement before writes")
                return original_check(staging, expected_identity)

            with mock.patch.object(gcs_module, "_verify_staging_identity", side_effect=_fail_first):
                with mock.patch.object(gcs_module, "_write_verified_file") as write_mock:
                    with self.assertRaises(PromotedOperationalInputGCSError):
                        hydrate_promoted_operational_input_snapshot(control=dest_control, acquired=acquired)
                    write_mock.assert_not_called()
            self.assertEqual(calls["count"], 1)

    def test_staging_identity_replacement_after_staged_tree_creation_prevents_verification(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            acquired = self._acquired(fx)
            dest_control = fx.destination_control()

            original_check = gcs_module._verify_staging_identity
            calls = {"count": 0}

            def _fail_second(staging, expected_identity):
                calls["count"] += 1
                if calls["count"] == 2:
                    raise PromotedOperationalInputGCSError("simulated staging replacement after tree creation")
                return original_check(staging, expected_identity)

            with mock.patch.object(gcs_module, "_verify_staging_identity", side_effect=_fail_second):
                with mock.patch.object(
                    gcs_module, "build_promoted_operational_input_inventory"
                ) as build_mock:
                    with self.assertRaises(PromotedOperationalInputGCSError):
                        hydrate_promoted_operational_input_snapshot(control=dest_control, acquired=acquired)
                    build_mock.assert_not_called()
            self.assertEqual(calls["count"], 2)

    def test_staging_identity_replacement_before_first_rename_prevents_any_rename(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            acquired = self._acquired(fx)
            dest_control = fx.destination_control()

            original_check = gcs_module._verify_staging_identity
            calls = {"count": 0}

            def _fail_third(staging, expected_identity):
                calls["count"] += 1
                if calls["count"] == 3:
                    raise PromotedOperationalInputGCSError("simulated staging replacement before rename")
                return original_check(staging, expected_identity)

            with mock.patch.object(gcs_module, "_verify_staging_identity", side_effect=_fail_third):
                with mock.patch.object(gcs_module.os, "rename") as rename_mock:
                    with self.assertRaises(PromotedOperationalInputGCSError):
                        hydrate_promoted_operational_input_snapshot(control=dest_control, acquired=acquired)
                    rename_mock.assert_not_called()
            self.assertEqual(calls["count"], 3)

    def test_mocked_short_write_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            acquired = self._acquired(fx)
            dest_control = fx.destination_control()

            original_fdopen = gcs_module.os.fdopen

            class _ShortWriteHandle:
                def __init__(self, real_handle):
                    self._real = real_handle

                def __enter__(self):
                    return self

                def __exit__(self, *exc_info):
                    return self._real.__exit__(*exc_info)

                def write(self, data):
                    written = self._real.write(data)
                    return max(written - 1, 0)

                def flush(self):
                    self._real.flush()

                def fileno(self):
                    return self._real.fileno()

            def _patched_fdopen(fd, mode="r", *args, **kwargs):
                return _ShortWriteHandle(original_fdopen(fd, mode, *args, **kwargs))

            with mock.patch.object(gcs_module.os, "fdopen", side_effect=_patched_fdopen):
                with self.assertRaises(PromotedOperationalInputGCSError):
                    hydrate_promoted_operational_input_snapshot(control=dest_control, acquired=acquired)
            self.assertFalse(dest_control.assembly_spec_file.exists())

    def test_mocked_hard_link_count_replacement_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            acquired = self._acquired(fx)
            dest_control = fx.destination_control()

            original_fstat = gcs_module.os.fstat
            calls = {"count": 0}

            class _FakeStat:
                def __init__(self, real: object) -> None:
                    self.st_mode = real.st_mode
                    self.st_dev = real.st_dev
                    self.st_ino = real.st_ino
                    self.st_size = real.st_size
                    self.st_nlink = 2
                    self.st_file_attributes = getattr(real, "st_file_attributes", 0)

            def _tampered_fstat(fd):
                real = original_fstat(fd)
                calls["count"] += 1
                if calls["count"] == 1:
                    return _FakeStat(real)
                return real

            with mock.patch.object(gcs_module.os, "fstat", side_effect=_tampered_fstat):
                with self.assertRaises(PromotedOperationalInputGCSError):
                    hydrate_promoted_operational_input_snapshot(control=dest_control, acquired=acquired)
            self.assertFalse(dest_control.assembly_spec_file.exists())


class AdversarialMutationTests(unittest.TestCase):
    def test_mutated_manifest_object_post_construction_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            publication = fx.publish()
            object.__setattr__(publication.manifest_object, "byte_count", publication.manifest_object.byte_count + 1)
            with self.assertRaises(PromotedOperationalInputGCSError):
                CompletedPromotedOperationalInputPublication(
                    manifest=publication.manifest, manifest_object=publication.manifest_object,
                )

    def test_mutated_acquired_snapshot_blob_content_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "a.txt").write_bytes(b"x")
            publication = fx.publish()
            reader = FakeReader(fx.writer)
            request = fx.restore_request(publication)
            acquired = acquire_promoted_operational_input_snapshot(request=request, reader=reader)
            tampered_blob = acquired.blobs[0]
            object.__setattr__(tampered_blob, "content_bytes", tampered_blob.content_bytes + b"x")
            with self.assertRaises(PromotedOperationalInputGCSError):
                AcquiredPromotedOperationalInputSnapshot(
                    request=acquired.request, manifest=acquired.manifest, blobs=acquired.blobs,
                )

    def test_mutated_control_before_hydration_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            (fx.roots["reference_root"] / "a.txt").write_bytes(b"x")
            publication = fx.publish()
            reader = FakeReader(fx.writer)
            request = fx.restore_request(publication)
            acquired = acquire_promoted_operational_input_snapshot(request=request, reader=reader)
            dest_control = fx.destination_control()
            object.__setattr__(dest_control, "expected_assembly_spec_id", "9" * 64)
            with self.assertRaises(PromotedOperationalInputGCSError):
                hydrate_promoted_operational_input_snapshot(control=dest_control, acquired=acquired)


class SanitizationTests(unittest.TestCase):
    SECRET_MARKER = "SUPER-SECRET-INPUT-GCS-MARKER-8k3f"

    def _assert_sanitized(self, exc: Exception) -> None:
        self.assertIsInstance(exc, PromotedOperationalInputGCSError)
        message = str(exc)
        self.assertNotIn(self.SECRET_MARKER, message)

    def test_writer_exception_containing_secret_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            fx.writer.raise_error = RuntimeError(f"leaked token={self.SECRET_MARKER}")
            try:
                fx.publish()
                self.fail("expected PromotedOperationalInputGCSError")
            except PromotedOperationalInputGCSError as exc:
                self._assert_sanitized(exc)

    def test_reader_exception_containing_secret_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            publication = fx.publish()
            reader = FakeReader(fx.writer)
            reader.raise_error = RuntimeError(f"leaked token={self.SECRET_MARKER}")
            request = fx.restore_request(publication)
            try:
                acquire_promoted_operational_input_snapshot(request=request, reader=reader)
                self.fail("expected PromotedOperationalInputGCSError")
            except PromotedOperationalInputGCSError as exc:
                self._assert_sanitized(exc)


class RegressionAndCapabilityTests(unittest.TestCase):
    def test_module_has_no_forbidden_capability(self) -> None:
        source = inspect.getsource(gcs_module)
        tree = ast.parse(source)
        # tempfile is legitimately required here (unlike the pure snapshot
        # module) to create the one private randomized staging directory
        # the revision-2 hydration design requires -- it is never used for
        # ambient/system temp-directory discovery, only tempfile.mkdtemp
        # under an already-verified common parent.
        forbidden_modules = {
            "socket", "subprocess", "requests", "urllib", "httpx", "google",
            "kiteconnect", "time", "threading", "asyncio", "glob",
        }
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0])
        self.assertEqual(imported & forbidden_modules, set())
        lowered = source.lower()
        for token in (
            "os.environ", "getenv(", "datetime.now(", "sleep(", "list_blobs(",
            "list_objects(", "storage.client(", "place_order(", "modify_order(",
            "cancel_order(", "import subprocess", "subprocess.",
        ):
            self.assertNotIn(token, lowered, msg=token)

        # The module docstring legitimately explains, in prose, what this
        # boundary deliberately does NOT do (Telegram, deployment) -- a
        # plain substring scan over that prose would false-positive.
        # Instead assert the forbidden symbols are never actually defined
        # or imported.
        for forbidden_symbol in (
            "TelegramBotConfig", "TelegramDeliveryRequest", "deliver_telegram_notification",
            "UrllibTelegramHTTPTransport", "LocalTelegramDeliveryReceiptStore", "KiteLoginCredentials",
        ):
            self.assertFalse(hasattr(gcs_module, forbidden_symbol))

    def test_module_does_not_import_a_concrete_gcs_sdk_client(self) -> None:
        source = inspect.getsource(gcs_module)
        self.assertNotIn("from google.cloud", source)
        self.assertNotIn("import google.cloud", source)

    def test_module_never_dereferences_state_root_for_filesystem_capability(self) -> None:
        # control.state_root's VALUE is legitimately passed through
        # opaquely into PromotedOperationalCloudRunControl's own
        # constructor (which requires it as a field) when reconstructing
        # a detached/staging control -- it is never the target of any
        # filesystem-capable call. Proven at runtime, more meaningfully,
        # by HydrationTests.test_state_root_is_never_created_or_touched.
        source = inspect.getsource(gcs_module)
        forbidden_patterns = (
            "lstat(control.state_root", "lstat(baseline_control.state_root",
            "scandir(control.state_root", "scandir(baseline_control.state_root",
            "rename(control.state_root", "rename(baseline_control.state_root",
            "mkdir(control.state_root", "mkdir(baseline_control.state_root",
            "_write_verified_file(control.state_root", "_write_verified_file(baseline_control.state_root",
            "read_stable_regular_file(control.state_root", "read_stable_regular_file(baseline_control.state_root",
        )
        for pattern in forbidden_patterns:
            self.assertNotIn(pattern, source, msg=pattern)


if __name__ == "__main__":
    unittest.main()
