from __future__ import annotations

import hashlib
import inspect
import unittest
from dataclasses import replace as _dc_replace
from datetime import timedelta

from india_swing.daily_pipeline.state_publication import PublishedStateObject
from india_swing.promoted_operational_persistence import (
    build_promoted_operational_advisory,
    build_promoted_operational_terminal_record,
    promoted_paper_registration_from_result,
)
from india_swing.promoted_terminal_binding import (
    MAXIMUM_TERMINAL_BINDING_BYTES,
    PromotedOperationalTerminalBindingRecord,
    build_promoted_operational_terminal_binding_record,
    encode_promoted_operational_terminal_binding_record,
    promoted_operational_terminal_binding_object_name,
)
from india_swing.promoted_terminal_binding_control_plane import (
    GoogleCloudStorageTerminalBindingReader,
    LoadedPromotedOperationalTerminalBinding,
    ObservedTerminalBindingObject,
    PromotedTerminalBindingControlPlaneError,
    load_optional_trusted_promoted_operational_terminal_binding,
    load_trusted_promoted_operational_terminal_binding,
    seal_promoted_operational_terminal_binding,
)

from tests import test_promoted_operational_persistence as _persistence_tests


def _flip_hex(value: str) -> str:
    replacement = "1" if value[0] == "0" else "0"
    return replacement + value[1:]


def _fixture():
    result = _persistence_tests._complete_paper_buy_result()
    advisory = build_promoted_operational_advisory(result)
    registration = promoted_paper_registration_from_result(result, advisory)
    terminal = build_promoted_operational_terminal_record(result, advisory, registration)
    return terminal, result.spec


class _FakeWriter:
    """Fake StateObjectWriter with real create-once semantics: an object
    already stored with different bytes fails closed; identical bytes are
    idempotent. Never contacts GCP."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.generations: dict[str, int] = {}
        self.calls: list[dict[str, object]] = []
        self._next_generation = 1

    def create_or_verify(
        self, *, bucket, object_name, content_bytes, content_type, maximum_bytes
    ) -> PublishedStateObject:
        self.calls.append(
            dict(
                bucket=bucket,
                object_name=object_name,
                content_bytes=content_bytes,
                content_type=content_type,
                maximum_bytes=maximum_bytes,
            )
        )
        existing = self.objects.get(object_name)
        if existing is not None:
            if existing != content_bytes:
                raise RuntimeError("SECRET-WRITER-CONFLICT-DO-NOT-LEAK-3f9a")
            return PublishedStateObject(
                object_name=object_name,
                generation=self.generations[object_name],
                byte_count=len(existing),
                sha256=hashlib.sha256(existing).hexdigest(),
            )
        generation = self._next_generation
        self._next_generation += 1
        self.objects[object_name] = content_bytes
        self.generations[object_name] = generation
        return PublishedStateObject(
            object_name=object_name,
            generation=generation,
            byte_count=len(content_bytes),
            sha256=hashlib.sha256(content_bytes).hexdigest(),
        )


class _MaliciousWriter:
    """Fake StateObjectWriter that returns a self-consistent but wrong
    PublishedStateObject, to prove the caller independently re-verifies."""

    def create_or_verify(self, **_kwargs) -> PublishedStateObject:
        return PublishedStateObject(
            object_name="attacker-chosen-name.json",
            generation=1,
            byte_count=999999,
            sha256="f" * 64,
        )


class _FakeGCSStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.generations: dict[str, int] = {}


class _FakeBlob:
    def __init__(self, store: _FakeGCSStore, object_name: str, generation) -> None:
        self._store = store
        self.name = object_name
        self.generation = generation
        self.reload_calls: list[dict[str, object]] = []
        self.download_calls: list[dict[str, object]] = []

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
    def __init__(self, store: _FakeGCSStore) -> None:
        self._store = store
        self.blob_calls: list[tuple[str, object]] = []
        self.blobs: list[_FakeBlob] = []

    def blob(self, object_name: str, generation=None) -> _FakeBlob:
        self.blob_calls.append((object_name, generation))
        blob = _FakeBlob(self._store, object_name, generation)
        self.blobs.append(blob)
        return blob


class _FakeClient:
    """Stand-in for google.cloud.storage.Client. Has no listing method."""

    def __init__(self) -> None:
        self.store = _FakeGCSStore()
        self.bucket_calls: list[str] = []
        self.buckets: list[_FakeBucket] = []

    def bucket(self, name: str) -> _FakeBucket:
        self.bucket_calls.append(name)
        bucket = _FakeBucket(self.store)
        self.buckets.append(bucket)
        return bucket

    def seed(self, object_name: str, content_bytes: bytes, *, generation: int) -> None:
        self.store.objects[object_name] = content_bytes
        self.store.generations[object_name] = generation


class PromotedTerminalBindingControlPlaneTests(unittest.TestCase):
    def test_seal_writes_exactly_one_create_once_object_and_verifies_the_returned_published_object(
        self,
    ) -> None:
        terminal, spec = _fixture()
        writer = _FakeWriter()
        sealed = seal_promoted_operational_terminal_binding(
            terminal=terminal, spec=spec, bucket="test-bucket", writer=writer
        )
        self.assertEqual(len(writer.calls), 1)
        call = writer.calls[0]
        expected_name = promoted_operational_terminal_binding_object_name(spec)
        self.assertEqual(call["object_name"], expected_name)
        self.assertEqual(call["content_type"], "application/json")
        self.assertEqual(call["maximum_bytes"], MAXIMUM_TERMINAL_BINDING_BYTES)
        expected_payload = encode_promoted_operational_terminal_binding_record(
            build_promoted_operational_terminal_binding_record(terminal, spec)
        )
        self.assertEqual(call["content_bytes"], expected_payload)
        self.assertEqual(sealed.published.object_name, expected_name)
        self.assertEqual(sealed.published.byte_count, len(expected_payload))

        with self.assertRaises(PromotedTerminalBindingControlPlaneError):
            seal_promoted_operational_terminal_binding(
                terminal=terminal, spec=spec, bucket="test-bucket", writer=_MaliciousWriter()
            )

    def test_identical_reseal_is_idempotent_and_conflicting_expected_terminal_id_fails_closed_without_overwrite(
        self,
    ) -> None:
        terminal, spec = _fixture()
        writer = _FakeWriter()
        first = seal_promoted_operational_terminal_binding(
            terminal=terminal, spec=spec, bucket="test-bucket", writer=writer
        )
        second = seal_promoted_operational_terminal_binding(
            terminal=terminal, spec=spec, bucket="test-bucket", writer=writer
        )
        self.assertEqual(first.record.binding_id, second.record.binding_id)
        self.assertEqual(first.published, second.published)
        object_name = promoted_operational_terminal_binding_object_name(spec)
        self.assertEqual(len(writer.objects), 1)

        # A different terminal for the SAME spec_id (same object name)
        # produces a different expected_terminal_id/binding record --
        # a genuine conflict at the create-once object name.
        conflicting_terminal = _dc_replace(
            terminal, evaluated_at=terminal.evaluated_at - timedelta(seconds=1)
        )
        self.assertNotEqual(conflicting_terminal.terminal_id, terminal.terminal_id)
        self.assertEqual(conflicting_terminal.spec_id, terminal.spec_id)
        with self.assertRaises(PromotedTerminalBindingControlPlaneError):
            seal_promoted_operational_terminal_binding(
                terminal=conflicting_terminal, spec=spec, bucket="test-bucket", writer=writer
            )
        # No overwrite, no delete: the originally sealed bytes are unchanged.
        self.assertEqual(len(writer.objects), 1)
        self.assertEqual(
            writer.objects[object_name],
            encode_promoted_operational_terminal_binding_record(first.record),
        )

    def test_load_reads_current_generation_pins_it_and_verifies_raw_download_bytes(
        self,
    ) -> None:
        terminal, spec = _fixture()
        record = build_promoted_operational_terminal_binding_record(terminal, spec)
        payload = encode_promoted_operational_terminal_binding_record(record)
        object_name = promoted_operational_terminal_binding_object_name(spec)

        client = _FakeClient()
        client.seed(object_name, payload, generation=42)
        reader = GoogleCloudStorageTerminalBindingReader(client)

        loaded = load_trusted_promoted_operational_terminal_binding(
            spec=spec, bucket="test-bucket", reader=reader
        )
        self.assertIs(type(loaded), LoadedPromotedOperationalTerminalBinding)
        self.assertEqual(loaded.record.binding_id, record.binding_id)
        self.assertEqual(loaded.generation, 42)
        self.assertEqual(loaded.byte_count, len(payload))
        self.assertEqual(loaded.sha256, hashlib.sha256(payload).hexdigest())
        self.assertEqual(loaded.binding.spec_id, spec.spec_id)
        self.assertEqual(loaded.binding.expected_terminal_id, terminal.terminal_id)

        all_blobs = [blob for bucket in client.buckets for blob in bucket.blobs]
        self.assertEqual(len(all_blobs), 2)
        observe_blob, pinned_blob = all_blobs
        self.assertEqual(len(observe_blob.reload_calls), 1)
        self.assertEqual(observe_blob.download_calls, [])
        self.assertEqual(pinned_blob.reload_calls, [])
        self.assertEqual(len(pinned_blob.download_calls), 1)
        download_call = pinned_blob.download_calls[0]
        self.assertEqual(download_call["end"], MAXIMUM_TERMINAL_BINDING_BYTES)
        self.assertTrue(download_call["raw_download"])
        self.assertEqual(download_call["if_generation_match"], 42)

    def test_load_rejects_generation_change_between_observation_and_pinned_download(
        self,
    ) -> None:
        terminal, spec = _fixture()
        record = build_promoted_operational_terminal_binding_record(terminal, spec)
        payload = encode_promoted_operational_terminal_binding_record(record)

        class _ObserveBlob:
            generation = 42

            def reload(self, *, retry) -> None:
                pass

        class _PinnedBlobWithChangedGeneration:
            def __init__(self, requested_generation: int) -> None:
                self.generation = requested_generation + 1

            def download_as_bytes(self, **_kwargs) -> bytes:
                return payload

        class _Bucket:
            def blob(self, name, generation=None):
                if generation is None:
                    return _ObserveBlob()
                return _PinnedBlobWithChangedGeneration(generation)

        class _Client:
            def bucket(self, name):
                return _Bucket()

        reader = GoogleCloudStorageTerminalBindingReader(_Client())
        with self.assertRaises(PromotedTerminalBindingControlPlaneError):
            load_trusted_promoted_operational_terminal_binding(
                spec=spec, bucket="test-bucket", reader=reader
            )

    def test_load_rejects_missing_object_oversized_payload_short_read_and_client_errors_with_sanitized_errors(
        self,
    ) -> None:
        terminal, spec = _fixture()
        object_name = promoted_operational_terminal_binding_object_name(spec)

        with self.subTest(case="missing_object"):
            client = _FakeClient()
            reader = GoogleCloudStorageTerminalBindingReader(client)
            try:
                load_trusted_promoted_operational_terminal_binding(
                    spec=spec, bucket="test-bucket", reader=reader
                )
                self.fail("expected PromotedTerminalBindingControlPlaneError")
            except PromotedTerminalBindingControlPlaneError as exc:
                self.assertNotIn("test-bucket", str(exc))
                self.assertNotIn(object_name, str(exc))
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)

        with self.subTest(case="oversized_payload"):
            huge_payload = b"x" * (MAXIMUM_TERMINAL_BINDING_BYTES + 1)
            client = _FakeClient()
            client.seed(object_name, huge_payload, generation=1)
            reader = GoogleCloudStorageTerminalBindingReader(client)
            with self.assertRaises(PromotedTerminalBindingControlPlaneError):
                load_trusted_promoted_operational_terminal_binding(
                    spec=spec, bucket="test-bucket", reader=reader
                )

        with self.subTest(case="short_read"):

            class _ShortReadBlob:
                generation = 5

                def reload(self, *, retry) -> None:
                    pass

                def download_as_bytes(self, **_kwargs) -> bytes:
                    return b""

            class _Bucket:
                def blob(self, name, generation=None):
                    return _ShortReadBlob()

            class _Client:
                def bucket(self, name):
                    return _Bucket()

            reader = GoogleCloudStorageTerminalBindingReader(_Client())
            with self.assertRaises(PromotedTerminalBindingControlPlaneError):
                load_trusted_promoted_operational_terminal_binding(
                    spec=spec, bucket="test-bucket", reader=reader
                )

        with self.subTest(case="client_error_sanitized"):

            class _RaisingBlob:
                def reload(self, *, retry) -> None:
                    raise RuntimeError("SECRET-CLIENT-FAILURE-DO-NOT-LEAK-7f2a")

            class _Bucket:
                def blob(self, name, generation=None):
                    return _RaisingBlob()

            class _Client:
                def bucket(self, name):
                    return _Bucket()

            reader = GoogleCloudStorageTerminalBindingReader(_Client())
            try:
                load_trusted_promoted_operational_terminal_binding(
                    spec=spec, bucket="test-bucket", reader=reader
                )
                self.fail("expected PromotedTerminalBindingControlPlaneError")
            except PromotedTerminalBindingControlPlaneError as exc:
                self.assertNotIn("SECRET-CLIENT-FAILURE-DO-NOT-LEAK-7f2a", str(exc))
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)

    def test_load_rejects_binding_whose_spec_session_preparation_or_identity_does_not_match_the_live_spec(
        self,
    ) -> None:
        terminal, spec = _fixture()
        record = build_promoted_operational_terminal_binding_record(terminal, spec)
        payload = encode_promoted_operational_terminal_binding_record(record)
        object_name = promoted_operational_terminal_binding_object_name(spec)

        with self.subTest(case="foreign_spec_sealed_binding"):
            foreign_result = _persistence_tests._complete_no_trade_result()
            foreign_advisory = build_promoted_operational_advisory(foreign_result)
            foreign_terminal = build_promoted_operational_terminal_record(
                foreign_result, foreign_advisory, None
            )
            foreign_spec = foreign_result.spec
            self.assertNotEqual(foreign_spec.spec_id, spec.spec_id)
            foreign_record = build_promoted_operational_terminal_binding_record(
                foreign_terminal, foreign_spec
            )
            foreign_payload = encode_promoted_operational_terminal_binding_record(foreign_record)
            client = _FakeClient()
            client.seed(object_name, foreign_payload, generation=1)
            reader = GoogleCloudStorageTerminalBindingReader(client)
            with self.assertRaises(PromotedTerminalBindingControlPlaneError):
                load_trusted_promoted_operational_terminal_binding(
                    spec=spec, bucket="test-bucket", reader=reader
                )

        with self.subTest(case="wrong_target_session"):
            forged = PromotedOperationalTerminalBindingRecord(
                spec_id=record.spec_id,
                target_session=record.target_session + timedelta(days=1),
                preparation_id=record.preparation_id,
                expected_terminal_id=record.expected_terminal_id,
                terminal_completed_at=record.terminal_completed_at,
            )
            forged_payload = encode_promoted_operational_terminal_binding_record(forged)
            client = _FakeClient()
            client.seed(object_name, forged_payload, generation=1)
            reader = GoogleCloudStorageTerminalBindingReader(client)
            with self.assertRaises(PromotedTerminalBindingControlPlaneError):
                load_trusted_promoted_operational_terminal_binding(
                    spec=spec, bucket="test-bucket", reader=reader
                )

        with self.subTest(case="wrong_preparation_id"):
            forged = PromotedOperationalTerminalBindingRecord(
                spec_id=record.spec_id,
                target_session=record.target_session,
                preparation_id=_flip_hex(record.preparation_id),
                expected_terminal_id=record.expected_terminal_id,
                terminal_completed_at=record.terminal_completed_at,
            )
            forged_payload = encode_promoted_operational_terminal_binding_record(forged)
            client = _FakeClient()
            client.seed(object_name, forged_payload, generation=1)
            reader = GoogleCloudStorageTerminalBindingReader(client)
            with self.assertRaises(PromotedTerminalBindingControlPlaneError):
                load_trusted_promoted_operational_terminal_binding(
                    spec=spec, bucket="test-bucket", reader=reader
                )

        with self.subTest(case="binding_id_does_not_recompute"):
            tampered_id = "0" * 64 if record.binding_id[0] != "0" else "1" * 64
            tampered_payload = payload.replace(
                ('"binding_id":"' + record.binding_id + '"').encode(),
                ('"binding_id":"' + tampered_id + '"').encode(),
                1,
            )
            client = _FakeClient()
            client.seed(object_name, tampered_payload, generation=1)
            reader = GoogleCloudStorageTerminalBindingReader(client)
            with self.assertRaises(PromotedTerminalBindingControlPlaneError):
                load_trusted_promoted_operational_terminal_binding(
                    spec=spec, bucket="test-bucket", reader=reader
                )

    def test_read_current_optional_returns_none_only_for_proven_absence_and_raises_for_every_other_failure(
        self,
    ) -> None:
        terminal, spec = _fixture()
        object_name = promoted_operational_terminal_binding_object_name(spec)

        class _FakeOptionalReader:
            def __init__(self, *, observed=None, raise_exc=None) -> None:
                self._observed = observed
                self._raise_exc = raise_exc

            def read_current(self, **_kwargs):
                raise AssertionError(
                    "read_current must not be called by the optional load path"
                )

            def read_current_optional(self, *, bucket, object_name, maximum_bytes):
                if self._raise_exc is not None:
                    raise self._raise_exc
                return self._observed

        with self.subTest(case="proven_absence"):
            reader = _FakeOptionalReader(observed=None)
            result = load_optional_trusted_promoted_operational_terminal_binding(
                spec=spec, bucket="test-bucket", reader=reader
            )
            self.assertIsNone(result)

        with self.subTest(case="corrupt_bytes"):
            corrupt_bytes = b"not-json-at-all"
            corrupt_observed = ObservedTerminalBindingObject(
                object_name=object_name,
                generation=1,
                byte_count=len(corrupt_bytes),
                sha256=hashlib.sha256(corrupt_bytes).hexdigest(),
                content_bytes=corrupt_bytes,
            )
            reader = _FakeOptionalReader(observed=corrupt_observed)
            try:
                load_optional_trusted_promoted_operational_terminal_binding(
                    spec=spec, bucket="test-bucket", reader=reader
                )
                self.fail("expected PromotedTerminalBindingControlPlaneError")
            except PromotedTerminalBindingControlPlaneError as exc:
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)

        with self.subTest(case="oversized_payload"):
            reader = _FakeOptionalReader(
                raise_exc=RuntimeError("SECRET-OVERSIZED-DO-NOT-LEAK-1a2b")
            )
            try:
                load_optional_trusted_promoted_operational_terminal_binding(
                    spec=spec, bucket="test-bucket", reader=reader
                )
                self.fail("expected PromotedTerminalBindingControlPlaneError")
            except PromotedTerminalBindingControlPlaneError as exc:
                self.assertNotIn("SECRET-OVERSIZED-DO-NOT-LEAK-1a2b", str(exc))
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)

        with self.subTest(case="mid_read_generation_change"):
            reader = _FakeOptionalReader(
                raise_exc=RuntimeError("SECRET-GENERATION-CHANGE-DO-NOT-LEAK-3c4d")
            )
            try:
                load_optional_trusted_promoted_operational_terminal_binding(
                    spec=spec, bucket="test-bucket", reader=reader
                )
                self.fail("expected PromotedTerminalBindingControlPlaneError")
            except PromotedTerminalBindingControlPlaneError as exc:
                self.assertNotIn("SECRET-GENERATION-CHANGE-DO-NOT-LEAK-3c4d", str(exc))
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)

        with self.subTest(case="foreign_spec_binding"):
            foreign_result = _persistence_tests._complete_no_trade_result()
            foreign_advisory = build_promoted_operational_advisory(foreign_result)
            foreign_terminal = build_promoted_operational_terminal_record(
                foreign_result, foreign_advisory, None
            )
            foreign_spec = foreign_result.spec
            self.assertNotEqual(foreign_spec.spec_id, spec.spec_id)
            foreign_record = build_promoted_operational_terminal_binding_record(
                foreign_terminal, foreign_spec
            )
            foreign_payload = encode_promoted_operational_terminal_binding_record(foreign_record)
            foreign_observed = ObservedTerminalBindingObject(
                object_name=object_name,
                generation=1,
                byte_count=len(foreign_payload),
                sha256=hashlib.sha256(foreign_payload).hexdigest(),
                content_bytes=foreign_payload,
            )
            reader = _FakeOptionalReader(observed=foreign_observed)
            try:
                load_optional_trusted_promoted_operational_terminal_binding(
                    spec=spec, bucket="test-bucket", reader=reader
                )
                self.fail("expected PromotedTerminalBindingControlPlaneError")
            except PromotedTerminalBindingControlPlaneError as exc:
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)

        with self.subTest(case="arbitrary_client_error"):
            reader = _FakeOptionalReader(raise_exc=RuntimeError("SECRET-CLIENT-DO-NOT-LEAK-5e6f"))
            try:
                load_optional_trusted_promoted_operational_terminal_binding(
                    spec=spec, bucket="test-bucket", reader=reader
                )
                self.fail("expected PromotedTerminalBindingControlPlaneError")
            except PromotedTerminalBindingControlPlaneError as exc:
                self.assertNotIn("SECRET-CLIENT-DO-NOT-LEAK-5e6f", str(exc))
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)

    def test_adapter_never_infers_absence_when_the_sdk_not_found_type_is_unavailable(
        self,
    ) -> None:
        import india_swing.promoted_terminal_binding_control_plane as control_plane_module

        self.assertIsNone(control_plane_module.NotFound)

        terminal, spec = _fixture()
        object_name = promoted_operational_terminal_binding_object_name(spec)

        with self.subTest(case="missing_object_via_reload_lookuperror"):

            class _MissingBlob:
                generation = None

                def reload(self, *, retry) -> None:
                    raise LookupError("not found")

            class _Bucket:
                def blob(self, name, generation=None):
                    return _MissingBlob()

            class _Client:
                def bucket(self, name):
                    return _Bucket()

            reader = GoogleCloudStorageTerminalBindingReader(_Client())
            with self.assertRaises(PromotedTerminalBindingControlPlaneError):
                reader.read_current_optional(
                    bucket="test-bucket",
                    object_name=object_name,
                    maximum_bytes=MAXIMUM_TERMINAL_BINDING_BYTES,
                )

        with self.subTest(case="empty_payload_not_absence"):

            class _EmptyBlob:
                generation = 5

                def reload(self, *, retry) -> None:
                    pass

                def download_as_bytes(self, **_kwargs) -> bytes:
                    return b""

            class _Bucket:
                def blob(self, name, generation=None):
                    return _EmptyBlob()

            class _Client:
                def bucket(self, name):
                    return _Bucket()

            reader = GoogleCloudStorageTerminalBindingReader(_Client())
            with self.assertRaises(PromotedTerminalBindingControlPlaneError):
                reader.read_current_optional(
                    bucket="test-bucket",
                    object_name=object_name,
                    maximum_bytes=MAXIMUM_TERMINAL_BINDING_BYTES,
                )

        with self.subTest(case="falsy_generation_not_absence"):

            class _FalsyGenerationBlob:
                generation = 0

                def reload(self, *, retry) -> None:
                    pass

            class _Bucket:
                def blob(self, name, generation=None):
                    return _FalsyGenerationBlob()

            class _Client:
                def bucket(self, name):
                    return _Bucket()

            reader = GoogleCloudStorageTerminalBindingReader(_Client())
            with self.assertRaises(PromotedTerminalBindingControlPlaneError):
                reader.read_current_optional(
                    bucket="test-bucket",
                    object_name=object_name,
                    maximum_bytes=MAXIMUM_TERMINAL_BINDING_BYTES,
                )

        with self.subTest(case="attribute_error_not_absence"):

            class _NoGenerationAttrBlob:
                def reload(self, *, retry) -> None:
                    pass

            class _Bucket:
                def blob(self, name, generation=None):
                    return _NoGenerationAttrBlob()

            class _Client:
                def bucket(self, name):
                    return _Bucket()

            reader = GoogleCloudStorageTerminalBindingReader(_Client())
            with self.assertRaises(PromotedTerminalBindingControlPlaneError):
                reader.read_current_optional(
                    bucket="test-bucket",
                    object_name=object_name,
                    maximum_bytes=MAXIMUM_TERMINAL_BINDING_BYTES,
                )

        with self.subTest(case="key_error_not_absence"):

            class _KeyErrorBlob:
                generation = 5

                def reload(self, *, retry) -> None:
                    raise KeyError("missing")

            class _Bucket:
                def blob(self, name, generation=None):
                    return _KeyErrorBlob()

            class _Client:
                def bucket(self, name):
                    return _Bucket()

            reader = GoogleCloudStorageTerminalBindingReader(_Client())
            with self.assertRaises(PromotedTerminalBindingControlPlaneError):
                reader.read_current_optional(
                    bucket="test-bucket",
                    object_name=object_name,
                    maximum_bytes=MAXIMUM_TERMINAL_BINDING_BYTES,
                )

        with self.subTest(case="lookup_error_from_download_not_absence"):

            class _DownloadLookupErrorBlob:
                generation = 5

                def reload(self, *, retry) -> None:
                    pass

                def download_as_bytes(self, **_kwargs) -> bytes:
                    raise LookupError("generation mismatch")

            class _Bucket:
                def blob(self, name, generation=None):
                    return _DownloadLookupErrorBlob()

            class _Client:
                def bucket(self, name):
                    return _Bucket()

            reader = GoogleCloudStorageTerminalBindingReader(_Client())
            with self.assertRaises(PromotedTerminalBindingControlPlaneError):
                reader.read_current_optional(
                    bucket="test-bucket",
                    object_name=object_name,
                    maximum_bytes=MAXIMUM_TERMINAL_BINDING_BYTES,
                )

    def test_bucket_preflight_asks_one_bucket_level_question_and_never_touches_an_object(
        self,
    ) -> None:
        class _FakeBucketHandle:
            def __init__(self, name) -> None:
                self.name = name

        class _RecordingClient:
            def __init__(self) -> None:
                self.get_bucket_calls: list[str] = []

            def get_bucket(self, name):
                self.get_bucket_calls.append(name)
                return _FakeBucketHandle(name)

            def bucket(self, name):
                raise AssertionError(
                    "bucket() must not be called by verify_bucket_reachable"
                )

        client = _RecordingClient()
        reader = GoogleCloudStorageTerminalBindingReader(client)
        result = reader.verify_bucket_reachable(bucket="test-bucket")
        self.assertIsNone(result)
        self.assertEqual(client.get_bucket_calls, ["test-bucket"])

    def test_bucket_preflight_fails_closed_for_a_missing_bucket_a_name_mismatch_and_any_client_error(
        self,
    ) -> None:
        with self.subTest(case="raising_get_bucket"):

            class _RaisingClient:
                def get_bucket(self, name):
                    raise RuntimeError("SECRET-BUCKET-MISSING-DO-NOT-LEAK-2f6a")

            reader = GoogleCloudStorageTerminalBindingReader(_RaisingClient())
            try:
                reader.verify_bucket_reachable(bucket="test-bucket")
                self.fail("expected PromotedTerminalBindingControlPlaneError")
            except PromotedTerminalBindingControlPlaneError as exc:
                self.assertNotIn("SECRET-BUCKET-MISSING-DO-NOT-LEAK-2f6a", str(exc))
                self.assertNotIn("test-bucket", str(exc))
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)

        with self.subTest(case="name_mismatch"):

            class _MismatchHandle:
                name = "a-different-bucket"

            class _MismatchClient:
                def get_bucket(self, name):
                    return _MismatchHandle()

            reader = GoogleCloudStorageTerminalBindingReader(_MismatchClient())
            with self.assertRaises(PromotedTerminalBindingControlPlaneError):
                reader.verify_bucket_reachable(bucket="test-bucket")

        with self.subTest(case="missing_name_attribute"):

            class _NoNameHandle:
                pass

            class _NoNameClient:
                def get_bucket(self, name):
                    return _NoNameHandle()

            reader = GoogleCloudStorageTerminalBindingReader(_NoNameClient())
            with self.assertRaises(PromotedTerminalBindingControlPlaneError):
                reader.verify_bucket_reachable(bucket="test-bucket")

        with self.subTest(case="non_string_name"):

            class _NonStringNameHandle:
                name = 12345

            class _NonStringNameClient:
                def get_bucket(self, name):
                    return _NonStringNameHandle()

            reader = GoogleCloudStorageTerminalBindingReader(_NonStringNameClient())
            with self.assertRaises(PromotedTerminalBindingControlPlaneError):
                reader.verify_bucket_reachable(bucket="test-bucket")

        with self.subTest(case="arbitrary_client_error"):

            class _ArbitraryErrorClient:
                def get_bucket(self, name):
                    raise ValueError("SECRET-ARBITRARY-DO-NOT-LEAK-9c1d")

            reader = GoogleCloudStorageTerminalBindingReader(_ArbitraryErrorClient())
            try:
                reader.verify_bucket_reachable(bucket="test-bucket")
                self.fail("expected PromotedTerminalBindingControlPlaneError")
            except PromotedTerminalBindingControlPlaneError as exc:
                self.assertNotIn("SECRET-ARBITRARY-DO-NOT-LEAK-9c1d", str(exc))
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)

    def test_control_plane_never_reads_the_local_terminal_store_and_never_derives_the_binding_from_it(
        self,
    ) -> None:
        import india_swing.promoted_terminal_binding as binding_module
        import india_swing.promoted_terminal_binding_control_plane as control_plane_module

        forbidden_names = (
            "LocalPromotedOperationalTerminalStore",
            "LocalPromotedOperationalAdvisoryOutbox",
            "LocalPaperTradeLedger",
        )
        forbidden_calls = ("open(", "pathlib", "Path(")
        for module in (binding_module, control_plane_module):
            source = inspect.getsource(module)
            for forbidden in forbidden_names:
                self.assertNotIn(forbidden, source)
            for forbidden in forbidden_calls:
                self.assertNotIn(forbidden, source)
            self.assertFalse(hasattr(module, "LocalPromotedOperationalTerminalStore"))
            self.assertFalse(hasattr(module, "LocalPromotedOperationalAdvisoryOutbox"))
            self.assertFalse(hasattr(module, "LocalPaperTradeLedger"))

    def test_public_contract_has_no_environment_credential_network_telegram_broker_or_current_time_capability(
        self,
    ) -> None:
        import india_swing.promoted_terminal_binding as binding_module
        import india_swing.promoted_terminal_binding_control_plane as control_plane_module

        for module in (binding_module, control_plane_module):
            source = inspect.getsource(module)
            for forbidden in (
                "import google",
                "google.cloud.storage.Client",
                "import requests",
                "import urllib",
                "import http",
                "import socket",
                "telegram",
                "kiteconnect",
                "import subprocess",
                "os.environ",
                "os.getenv",
                "datetime.now(",
                "datetime.utcnow(",
                ".utcnow(",
                ".today(",
                "time.time(",
                "import random",
            ):
                self.assertNotIn(forbidden, source)

        with self.assertRaises(PromotedTerminalBindingControlPlaneError):
            GoogleCloudStorageTerminalBindingReader(None)
        signature = inspect.signature(GoogleCloudStorageTerminalBindingReader.__init__)
        self.assertEqual(list(signature.parameters), ["self", "client"])
        self.assertEqual(signature.parameters["client"].default, inspect.Parameter.empty)


if __name__ == "__main__":
    unittest.main()
