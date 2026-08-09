from __future__ import annotations

import ast
import inspect
import unittest
from hashlib import sha256

from india_swing.daily_pipeline.acquisition import GCSObjectPayload
from india_swing.daily_pipeline.state_publication import PublishedStateObject
from india_swing.quality_pilot import canonical_response as canonical_response_module
from india_swing.quality_pilot import observation_store as observation_store_module
from india_swing.quality_pilot.canonical_response import (
    EndpointFamily,
    MAXIMUM_ENCODED_BYTES,
    PILOT_PROTOCOL_SHA256,
    QualityPilotObservation,
    ScheduledWindowKind,
    encode_observation,
)
from india_swing.quality_pilot.observation_store import (
    QUALITY_OBSERVATION_CONTENT_TYPE,
    QUALITY_OBSERVATION_STORE_POLICY_VERSION,
    LoadedQualityPilotObservation,
    PinnedQualityPilotObservationRequest,
    PublishedQualityPilotObservation,
    QualityPilotObservationStoreError,
    canonical_observation_object_name,
    publish_quality_pilot_observation,
    read_pinned_quality_pilot_observation,
)

from tests.test_quality_pilot_canonical_response import (
    PILOT_RUN_ID,
    SESSION,
    _catalog_observation,
    _catalog_window,
    _instrument,
    _ohlcv_observation,
    _ohlcv_window,
    _quote_observation,
    _quote_window,
)


BUCKET = "test-quality-pilot-bucket"


def _fake_sha256(label: str) -> str:
    return sha256(label.encode("utf-8")).hexdigest()


class FakeStateObjectWriter:
    """Fake StateObjectWriter. Never contacts GCP; idempotent create-or-verify."""

    def __init__(self) -> None:
        self.store: dict[tuple[str, str], tuple[bytes, int]] = {}
        self.calls: list[dict[str, object]] = []
        self._next_generation = 1
        self.raise_error: Exception | None = None
        self.malicious_result: object = None

    def create_or_verify(
        self,
        *,
        bucket: str,
        object_name: str,
        content_bytes: bytes,
        content_type: str,
        maximum_bytes: int,
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
        if self.raise_error is not None:
            raise self.raise_error
        if self.malicious_result is not None:
            return self.malicious_result
        key = (bucket, object_name)
        if key in self.store:
            stored_bytes, generation = self.store[key]
            return PublishedStateObject(
                object_name=object_name,
                generation=generation,
                byte_count=len(stored_bytes),
                sha256=sha256(stored_bytes).hexdigest(),
            )
        generation = self._next_generation
        self._next_generation += 1
        self.store[key] = (content_bytes, generation)
        return PublishedStateObject(
            object_name=object_name,
            generation=generation,
            byte_count=len(content_bytes),
            sha256=sha256(content_bytes).hexdigest(),
        )


class FakeGCSObjectReader:
    """Fake GCSObjectReader. Never contacts GCP; records every call made."""

    def __init__(self, *, generation: int, content_bytes: bytes) -> None:
        self.generation = generation
        self.content_bytes = content_bytes
        self.calls: list[dict[str, object]] = []
        self.raise_error: Exception | None = None
        self.malicious_result: object = None

    def read_generation(
        self, *, bucket: str, object_name: str, generation: int, maximum_bytes: int
    ) -> GCSObjectPayload:
        self.calls.append(
            dict(
                bucket=bucket, object_name=object_name, generation=generation, maximum_bytes=maximum_bytes
            )
        )
        if self.raise_error is not None:
            raise self.raise_error
        if self.malicious_result is not None:
            return self.malicious_result
        return GCSObjectPayload(content_bytes=self.content_bytes, generation=self.generation)


def _publish(observation: QualityPilotObservation, *, bucket: str = BUCKET, writer=None):
    writer = writer or FakeStateObjectWriter()
    return publish_quality_pilot_observation(observation, bucket, writer), writer


def _pinned_request_for(
    observation: QualityPilotObservation, published: PublishedQualityPilotObservation, **overrides
) -> PinnedQualityPilotObservationRequest:
    kwargs = dict(
        bucket=published.bucket,
        object_name=published.object_name,
        generation=published.generation,
        expected_encoded_sha256=published.encoded_sha256,
        expected_observation_id=observation.observation_id,
        pilot_run_id=observation.window.pilot_run_id,
        market_session=observation.window.market_session,
        window_kind=observation.window.window_kind,
        endpoint_family=observation.window.endpoint_family,
        chunk_index=observation.request.chunk_index,
        chunk_count=observation.request.chunk_count,
    )
    kwargs.update(overrides)
    return PinnedQualityPilotObservationRequest(**kwargs)


class QualityPilotPublishHappyPathTests(unittest.TestCase):
    def test_publishes_catalog_deterministically_to_exact_path(self) -> None:
        observation = _catalog_observation()
        published, writer = _publish(observation)
        self.assertEqual(len(writer.calls), 1)
        call = writer.calls[0]
        self.assertEqual(call["bucket"], BUCKET)
        self.assertEqual(call["content_type"], QUALITY_OBSERVATION_CONTENT_TYPE)
        self.assertEqual(call["maximum_bytes"], MAXIMUM_ENCODED_BYTES)
        expected_bytes = encode_observation(observation)
        self.assertEqual(call["content_bytes"], expected_bytes)
        expected_path = canonical_observation_object_name(
            observation.window.pilot_run_id,
            observation.window.market_session,
            observation.window.window_kind,
            observation.request.chunk_index,
            observation.request.chunk_count,
            observation.observation_id,
        )
        self.assertEqual(published.object_name, expected_path)
        self.assertEqual(published.encoded_byte_count, len(expected_bytes))
        self.assertEqual(published.encoded_sha256, sha256(expected_bytes).hexdigest())
        self.assertEqual(published.generation, 1)
        self.assertEqual(published.storage_policy_version, QUALITY_OBSERVATION_STORE_POLICY_VERSION)
        self.assertEqual(published.protocol_sha256, PILOT_PROTOCOL_SHA256)

    def test_publishes_quote_deterministically(self) -> None:
        observation = _quote_observation()
        published, writer = _publish(observation)
        self.assertEqual(len(writer.calls), 1)
        self.assertEqual(published.endpoint_family, EndpointFamily.FULL_QUOTE)

    def test_publishes_ohlcv_deterministically(self) -> None:
        observation = _ohlcv_observation()
        published, writer = _publish(observation)
        self.assertEqual(len(writer.calls), 1)
        self.assertEqual(published.endpoint_family, EndpointFamily.DAILY_OHLCV)

    def test_publishing_same_observation_twice_is_idempotent(self) -> None:
        observation = _catalog_observation()
        writer = FakeStateObjectWriter()
        first, _ = _publish(observation, writer=writer)
        second, _ = _publish(observation, writer=writer)
        self.assertEqual(len(writer.calls), 2)
        self.assertEqual(first.object_name, second.object_name)
        self.assertEqual(first.generation, second.generation)
        self.assertEqual(writer.calls[0]["content_bytes"], writer.calls[1]["content_bytes"])

    def test_publishing_a_correction_creates_a_distinct_path_and_preserves_the_original(
        self,
    ) -> None:
        window = _catalog_window()
        original = _catalog_observation(window=window)
        writer = FakeStateObjectWriter()
        published_original, _ = _publish(original, writer=writer)
        correction = QualityPilotObservation(
            window=original.window,
            request=original.request,
            payload=original.payload,
            corrects_observation_id=original.observation_id,
        )
        published_correction, _ = _publish(correction, writer=writer)
        self.assertNotEqual(published_correction.object_name, published_original.object_name)
        original_key = (BUCKET, published_original.object_name)
        stored_bytes, stored_generation = writer.store[original_key]
        self.assertEqual(stored_generation, published_original.generation)
        self.assertEqual(stored_bytes, encode_observation(original))


class QualityPilotPublishRejectionTests(unittest.TestCase):
    def test_rejects_malformed_bucket(self) -> None:
        observation = _catalog_observation()
        writer = FakeStateObjectWriter()
        with self.assertRaises(QualityPilotObservationStoreError):
            publish_quality_pilot_observation(observation, "AB", writer)
        self.assertEqual(writer.calls, [])

    def test_rejects_ip_shaped_bucket(self) -> None:
        observation = _catalog_observation()
        writer = FakeStateObjectWriter()
        with self.assertRaises(QualityPilotObservationStoreError):
            publish_quality_pilot_observation(observation, "192.168.1.1", writer)
        self.assertEqual(writer.calls, [])

    def test_rejects_non_observation_type(self) -> None:
        writer = FakeStateObjectWriter()
        with self.assertRaises(QualityPilotObservationStoreError):
            publish_quality_pilot_observation(object(), BUCKET, writer)
        self.assertEqual(writer.calls, [])

    def test_rejects_malicious_writer_object_name(self) -> None:
        observation = _catalog_observation()
        writer = FakeStateObjectWriter()
        writer.malicious_result = PublishedStateObject(
            object_name="quality-pilot/v1/forged.json",
            generation=1,
            byte_count=100,
            sha256=_fake_sha256("forged"),
        )
        with self.assertRaises(QualityPilotObservationStoreError):
            publish_quality_pilot_observation(observation, BUCKET, writer)

    def test_rejects_malicious_writer_byte_count(self) -> None:
        observation = _catalog_observation()
        encoded = encode_observation(observation)
        expected_path = canonical_observation_object_name(
            observation.window.pilot_run_id,
            observation.window.market_session,
            observation.window.window_kind,
            observation.request.chunk_index,
            observation.request.chunk_count,
            observation.observation_id,
        )
        writer = FakeStateObjectWriter()
        writer.malicious_result = PublishedStateObject(
            object_name=expected_path,
            generation=1,
            byte_count=len(encoded) + 1,
            sha256=sha256(encoded).hexdigest(),
        )
        with self.assertRaises(QualityPilotObservationStoreError):
            publish_quality_pilot_observation(observation, BUCKET, writer)

    def test_rejects_malicious_writer_sha256(self) -> None:
        observation = _catalog_observation()
        encoded = encode_observation(observation)
        expected_path = canonical_observation_object_name(
            observation.window.pilot_run_id,
            observation.window.market_session,
            observation.window.window_kind,
            observation.request.chunk_index,
            observation.request.chunk_count,
            observation.observation_id,
        )
        writer = FakeStateObjectWriter()
        writer.malicious_result = PublishedStateObject(
            object_name=expected_path, generation=1, byte_count=len(encoded), sha256="0" * 64
        )
        with self.assertRaises(QualityPilotObservationStoreError):
            publish_quality_pilot_observation(observation, BUCKET, writer)

    def test_rejects_foreign_writer_result_type(self) -> None:
        observation = _catalog_observation()
        writer = FakeStateObjectWriter()
        writer.malicious_result = object()
        with self.assertRaises(QualityPilotObservationStoreError):
            publish_quality_pilot_observation(observation, BUCKET, writer)

    def test_rejects_subclassed_writer_result_type(self) -> None:
        class _Subclass(PublishedStateObject):
            pass

        observation = _catalog_observation()
        encoded = encode_observation(observation)
        expected_path = canonical_observation_object_name(
            observation.window.pilot_run_id,
            observation.window.market_session,
            observation.window.window_kind,
            observation.request.chunk_index,
            observation.request.chunk_count,
            observation.observation_id,
        )
        writer = FakeStateObjectWriter()
        writer.malicious_result = _Subclass(
            object_name=expected_path,
            generation=1,
            byte_count=len(encoded),
            sha256=sha256(encoded).hexdigest(),
        )
        with self.assertRaises(QualityPilotObservationStoreError):
            publish_quality_pilot_observation(observation, BUCKET, writer)

    def test_writer_exception_with_planted_secret_is_sanitized(self) -> None:
        secret = "SECRET-PLANTED-VALUE-MUST-NOT-LEAK/var/data/topsecret.json"
        observation = _catalog_observation()
        writer = FakeStateObjectWriter()
        writer.raise_error = RuntimeError(secret)
        with self.assertRaises(QualityPilotObservationStoreError) as context:
            publish_quality_pilot_observation(observation, BUCKET, writer)
        exc = context.exception
        self.assertNotIn(secret, str(exc))
        self.assertNotIn(secret, repr(exc))
        self.assertIsNone(exc.__cause__)
        self.assertIsNone(exc.__context__)
        self.assertNotIn(BUCKET, str(exc))

    def test_writer_never_called_after_prepublication_verification_failure(self) -> None:
        writer = FakeStateObjectWriter()
        with self.assertRaises(QualityPilotObservationStoreError):
            publish_quality_pilot_observation(object(), BUCKET, writer)
        self.assertEqual(writer.calls, [])
        with self.assertRaises(QualityPilotObservationStoreError):
            publish_quality_pilot_observation(_catalog_observation(), "AB", writer)
        self.assertEqual(writer.calls, [])

    def test_writer_called_at_most_once_on_success(self) -> None:
        writer = FakeStateObjectWriter()
        publish_quality_pilot_observation(_catalog_observation(), BUCKET, writer)
        self.assertEqual(len(writer.calls), 1)


class QualityPilotReadHappyPathTests(unittest.TestCase):
    def test_reads_pinned_catalog_observation(self) -> None:
        observation = _catalog_observation()
        published, _writer = _publish(observation)
        encoded = encode_observation(observation)
        reader = FakeGCSObjectReader(generation=published.generation, content_bytes=encoded)
        request = _pinned_request_for(observation, published)
        loaded = read_pinned_quality_pilot_observation(request, reader)
        self.assertEqual(len(reader.calls), 1)
        call = reader.calls[0]
        self.assertEqual(call["bucket"], published.bucket)
        self.assertEqual(call["object_name"], published.object_name)
        self.assertEqual(call["generation"], published.generation)
        self.assertEqual(call["maximum_bytes"], MAXIMUM_ENCODED_BYTES)
        self.assertEqual(loaded.observation.observation_id, observation.observation_id)
        self.assertEqual(loaded.request, request)

    def test_reads_pinned_quote_and_ohlcv_observations(self) -> None:
        for build in (_quote_observation, _ohlcv_observation):
            with self.subTest(build=build):
                observation = build()
                published, _writer = _publish(observation)
                encoded = encode_observation(observation)
                reader = FakeGCSObjectReader(generation=published.generation, content_bytes=encoded)
                request = _pinned_request_for(observation, published)
                loaded = read_pinned_quality_pilot_observation(request, reader)
                self.assertEqual(loaded.observation.observation_id, observation.observation_id)


class QualityPilotReadRejectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.observation = _catalog_observation()
        self.published, _writer = _publish(self.observation)
        self.encoded = encode_observation(self.observation)

    def _reader(self, **overrides) -> FakeGCSObjectReader:
        kwargs = dict(generation=self.published.generation, content_bytes=self.encoded)
        kwargs.update(overrides)
        return FakeGCSObjectReader(**kwargs)

    def _request(self, **overrides) -> PinnedQualityPilotObservationRequest:
        return _pinned_request_for(self.observation, self.published, **overrides)

    def test_rejects_empty_content(self) -> None:
        reader = self._reader(content_bytes=b"")
        with self.assertRaises(QualityPilotObservationStoreError):
            read_pinned_quality_pilot_observation(self._request(), reader)

    def test_rejects_oversized_content(self) -> None:
        reader = self._reader(content_bytes=b"0" * (MAXIMUM_ENCODED_BYTES + 1))
        with self.assertRaises(QualityPilotObservationStoreError):
            read_pinned_quality_pilot_observation(self._request(), reader)

    def test_rejects_non_bytes_content(self) -> None:
        reader = self._reader(content_bytes="not-bytes")  # type: ignore[arg-type]
        with self.assertRaises(QualityPilotObservationStoreError):
            read_pinned_quality_pilot_observation(self._request(), reader)

    def test_rejects_wrong_generation_from_reader(self) -> None:
        reader = self._reader(generation=self.published.generation + 1)
        with self.assertRaises(QualityPilotObservationStoreError):
            read_pinned_quality_pilot_observation(self._request(), reader)

    def test_rejects_bool_generation_from_reader(self) -> None:
        reader = self._reader()
        reader.malicious_result = GCSObjectPayload(content_bytes=self.encoded, generation=True)
        with self.assertRaises(QualityPilotObservationStoreError):
            read_pinned_quality_pilot_observation(self._request(), reader)

    def test_rejects_float_generation_equal_to_pinned_int(self) -> None:
        # Python numeric equality makes 1.0 == 1 -- a bare `!=` comparison
        # would silently accept a float generation. The pinned generation
        # here is genuinely 1 (this is the first-ever publish in this test),
        # so a float 1.0 payload generation must still be rejected on type
        # alone, never reaching the numeric comparison.
        self.assertEqual(self.published.generation, 1)
        reader = self._reader()
        reader.malicious_result = GCSObjectPayload(content_bytes=self.encoded, generation=1.0)
        with self.assertRaises(QualityPilotObservationStoreError):
            read_pinned_quality_pilot_observation(self._request(), reader)

    def test_rejects_hostile_comparator_generation_without_executing_it(self) -> None:
        secret = "SECRET-GENERATION-COMPARATOR-MUST-NOT-LEAK"

        class _HostileGeneration:
            def __eq__(self, other):
                raise RuntimeError(secret)

            def __ne__(self, other):
                raise RuntimeError(secret)

            def __hash__(self):
                return 0

        reader = self._reader()
        reader.malicious_result = GCSObjectPayload(
            content_bytes=self.encoded, generation=_HostileGeneration()
        )
        with self.assertRaises(QualityPilotObservationStoreError) as context:
            read_pinned_quality_pilot_observation(self._request(), reader)
        exc = context.exception
        self.assertNotIn(secret, str(exc))
        self.assertNotIn(secret, repr(exc))
        self.assertIsNone(exc.__cause__)
        self.assertIsNone(exc.__context__)

    def test_rejects_wrong_content_hash(self) -> None:
        tampered = self.encoded.replace(b"INFY", b"TCSX")
        reader = self._reader(content_bytes=tampered) if len(tampered) == len(self.encoded) else self._reader(
            content_bytes=self.encoded + b" "
        )
        with self.assertRaises(QualityPilotObservationStoreError):
            read_pinned_quality_pilot_observation(self._request(), reader)

    def test_rejects_malformed_noncanonical_content(self) -> None:
        garbage = b'{"not": "canonical"}'
        request = _pinned_request_for(
            self.observation, self.published, expected_encoded_sha256=sha256(garbage).hexdigest()
        )
        reader = self._reader(content_bytes=garbage)
        with self.assertRaises(QualityPilotObservationStoreError):
            read_pinned_quality_pilot_observation(request, reader)

    def test_rejects_forged_observation_id_route(self) -> None:
        # _catalog_observation() with no overrides is fully deterministic --
        # a genuinely different fixture is needed to get a different
        # observation_id, otherwise this would trivially match by accident.
        other_observation = _catalog_observation(
            instruments=(_instrument(token=202, symbol="ZZZZ"),)
        )
        self.assertNotEqual(other_observation.observation_id, self.observation.observation_id)
        with self.assertRaises(QualityPilotObservationStoreError):
            self._request(expected_observation_id=other_observation.observation_id)

    def test_rejects_pilot_run_id_mismatch(self) -> None:
        with self.assertRaises(QualityPilotObservationStoreError):
            self._request(pilot_run_id=_fake_sha256("different-run"))

    def test_rejects_market_session_mismatch(self) -> None:
        from datetime import date

        with self.assertRaises(QualityPilotObservationStoreError):
            self._request(market_session=date(2020, 1, 1))

    def test_rejects_window_kind_mismatch(self) -> None:
        with self.assertRaises(QualityPilotObservationStoreError):
            self._request(window_kind=ScheduledWindowKind.QUOTE_0920)

    def test_rejects_endpoint_family_mismatch(self) -> None:
        with self.assertRaises(QualityPilotObservationStoreError):
            self._request(endpoint_family=EndpointFamily.FULL_QUOTE)

    def test_rejects_chunk_index_mismatch(self) -> None:
        with self.assertRaises(QualityPilotObservationStoreError):
            self._request(chunk_index=2, chunk_count=2)

    def test_rejects_chunk_count_mismatch(self) -> None:
        with self.assertRaises(QualityPilotObservationStoreError):
            self._request(chunk_count=2)

    def test_reader_exception_with_planted_secret_is_sanitized(self) -> None:
        secret = "SECRET-PLANTED-VALUE-MUST-NOT-LEAK/var/data/topsecret.json"
        reader = self._reader()
        reader.raise_error = RuntimeError(secret)
        with self.assertRaises(QualityPilotObservationStoreError) as context:
            read_pinned_quality_pilot_observation(self._request(), reader)
        exc = context.exception
        self.assertNotIn(secret, str(exc))
        self.assertIsNone(exc.__cause__)
        self.assertIsNone(exc.__context__)

    def test_replay_failure_on_tampered_but_hash_matching_content_is_sanitized(self) -> None:
        # A content mutation that happens to preserve the sha256 is
        # astronomically unlikely to occur naturally; instead prove replay
        # failures are sanitized via a byte string whose hash we compute
        # fresh so decode fails deep inside canonical_response, not at the
        # sha256 gate this module itself checks first.
        garbage = b"{}"
        request = _pinned_request_for(
            self.observation, self.published, expected_encoded_sha256=sha256(garbage).hexdigest()
        )
        reader = self._reader(content_bytes=garbage)
        with self.assertRaises(QualityPilotObservationStoreError) as context:
            read_pinned_quality_pilot_observation(request, reader)
        exc = context.exception
        self.assertIsNone(exc.__cause__)
        self.assertIsNone(exc.__context__)


class QualityPilotPinnedRequestConstructionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.observation = _catalog_observation()
        self.published, _writer = _publish(self.observation)

    def test_rejects_malformed_bucket(self) -> None:
        with self.assertRaises(QualityPilotObservationStoreError):
            _pinned_request_for(self.observation, self.published, bucket="AB")

    def test_rejects_traversal_object_name(self) -> None:
        with self.assertRaises(QualityPilotObservationStoreError):
            _pinned_request_for(
                self.observation, self.published, object_name="../../etc/passwd"
            )

    def test_rejects_caller_forged_object_name(self) -> None:
        with self.assertRaises(QualityPilotObservationStoreError):
            _pinned_request_for(
                self.observation, self.published, object_name="quality-pilot/v1/forged.json"
            )

    def test_rejects_bool_generation(self) -> None:
        with self.assertRaises(QualityPilotObservationStoreError):
            _pinned_request_for(self.observation, self.published, generation=True)

    def test_rejects_zero_generation(self) -> None:
        with self.assertRaises(QualityPilotObservationStoreError):
            _pinned_request_for(self.observation, self.published, generation=0)

    def test_rejects_negative_generation(self) -> None:
        with self.assertRaises(QualityPilotObservationStoreError):
            _pinned_request_for(self.observation, self.published, generation=-1)

    def test_rejects_uppercase_sha256(self) -> None:
        with self.assertRaises(QualityPilotObservationStoreError):
            _pinned_request_for(
                self.observation,
                self.published,
                expected_encoded_sha256=self.published.encoded_sha256.upper(),
            )

    def test_rejects_uppercase_observation_id(self) -> None:
        with self.assertRaises(QualityPilotObservationStoreError):
            _pinned_request_for(
                self.observation,
                self.published,
                expected_observation_id=self.observation.observation_id.upper(),
            )

    def test_rejects_invalid_chunk_route(self) -> None:
        with self.assertRaises(QualityPilotObservationStoreError):
            _pinned_request_for(self.observation, self.published, chunk_index=2, chunk_count=1)

    def test_never_accepts_absent_generation(self) -> None:
        with self.assertRaises(TypeError):
            PinnedQualityPilotObservationRequest(
                bucket=BUCKET,
                object_name=self.published.object_name,
                expected_encoded_sha256=self.published.encoded_sha256,
                expected_observation_id=self.observation.observation_id,
                pilot_run_id=self.observation.window.pilot_run_id,
                market_session=self.observation.window.market_session,
                window_kind=self.observation.window.window_kind,
                endpoint_family=self.observation.window.endpoint_family,
                chunk_index=1,
                chunk_count=1,
            )  # type: ignore[call-arg]


class QualityPilotPublishedObservationConstructionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.observation = _catalog_observation()
        self.published, _writer = _publish(self.observation)

    def _kwargs(self, **overrides) -> dict:
        kwargs = dict(
            storage_policy_version=self.published.storage_policy_version,
            protocol_sha256=self.published.protocol_sha256,
            observation_id=self.published.observation_id,
            pilot_run_id=self.published.pilot_run_id,
            market_session=self.published.market_session,
            window_kind=self.published.window_kind,
            endpoint_family=self.published.endpoint_family,
            chunk_index=self.published.chunk_index,
            chunk_count=self.published.chunk_count,
            bucket=self.published.bucket,
            object_name=self.published.object_name,
            generation=self.published.generation,
            encoded_byte_count=self.published.encoded_byte_count,
            encoded_sha256=self.published.encoded_sha256,
        )
        kwargs.update(overrides)
        return kwargs

    def test_rejects_wrong_storage_policy_version(self) -> None:
        with self.assertRaises(QualityPilotObservationStoreError):
            PublishedQualityPilotObservation(**self._kwargs(storage_policy_version="wrong/v1"))

    def test_rejects_wrong_protocol_hash(self) -> None:
        with self.assertRaises(QualityPilotObservationStoreError):
            PublishedQualityPilotObservation(**self._kwargs(protocol_sha256="0" * 64))

    def test_rejects_object_name_route_mismatch(self) -> None:
        with self.assertRaises(QualityPilotObservationStoreError):
            PublishedQualityPilotObservation(**self._kwargs(chunk_count=2))

    def test_altered_posture_field_rejected(self) -> None:
        published = self.published
        with self.assertRaises(AttributeError):
            object.__setattr__(published, "capital_eligible", True)


class QualityPilotPostureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.observation = _catalog_observation()

    def _assert_fixed_posture(self, obj) -> None:
        self.assertTrue(obj.quality_only)
        for name in (
            "counts_toward_o0",
            "counts_toward_clean_accumulation",
            "research_partition_eligible",
            "training_eligible",
            "feature_eligible",
            "label_eligible",
            "signal_eligible",
            "paper_trade_eligible",
            "notification_eligible",
            "execution_eligible",
            "capital_eligible",
        ):
            self.assertFalse(getattr(obj, name), msg=name)

    def test_published_observation_posture(self) -> None:
        published, _writer = _publish(self.observation)
        self._assert_fixed_posture(published)

    def test_pinned_request_posture(self) -> None:
        published, _writer = _publish(self.observation)
        request = _pinned_request_for(self.observation, published)
        self._assert_fixed_posture(request)

    def test_loaded_observation_posture(self) -> None:
        published, _writer = _publish(self.observation)
        encoded = encode_observation(self.observation)
        reader = FakeGCSObjectReader(generation=published.generation, content_bytes=encoded)
        request = _pinned_request_for(self.observation, published)
        loaded = read_pinned_quality_pilot_observation(request, reader)
        self._assert_fixed_posture(loaded)


class QualityPilotStructuralTests(unittest.TestCase):
    def setUp(self) -> None:
        self.source = inspect.getsource(observation_store_module)

    def test_no_filesystem_network_environment_or_clock_access(self) -> None:
        forbidden = (
            "open(",
            "Path(",
            "os.environ",
            "os.getenv",
            "socket.",
            "requests.",
            "urllib.",
            "httpx.",
            "datetime.now(",
            "datetime.utcnow(",
            "time.time(",
            "time.sleep(",
            "subprocess.",
            "os.system(",
        )
        for token in forbidden:
            self.assertNotIn(token, self.source, msg=f"forbidden token found: {token}")

    def test_no_gcp_sdk_broker_or_discovery_capability(self) -> None:
        forbidden = (
            "storage.client(",
            "google.cloud",
            "kiteconnect",
            "broker.",
            "place_order(",
            "execute_order(",
            "compute_feature(",
            "calculate_return(",
            "generate_signal(",
            "scheduler.",
            "crontab",
            ".list_blobs(",
            ".list(",
            "latest_at_or_before",
            "find_by_selection_key",
            ".delete(",
            ".overwrite(",
            ".update(",
            ".replace(",
        )
        lowered = self.source.lower()
        for token in forbidden:
            self.assertNotIn(token.lower(), lowered, msg=f"forbidden token found: {token}")

    def test_ast_scan_finds_no_import_of_forbidden_modules(self) -> None:
        tree = ast.parse(self.source)
        forbidden_modules = {
            "os",
            "sys",
            "socket",
            "subprocess",
            "requests",
            "urllib",
            "httpx",
            "shutil",
            "pathlib",
            "sqlite3",
            "pickle",
            "shelve",
        }
        imported_modules: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imported_modules.add(alias.name.split(".")[0])
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported_modules.add(node.module.split(".")[0])
        self.assertEqual(imported_modules & forbidden_modules, set())

    def test_writer_and_reader_protocols_have_only_the_two_named_methods(self) -> None:
        # The module only ever calls writer.create_or_verify and
        # reader.read_generation -- no list/latest/delete/update capability.
        self.assertIn("create_or_verify(", self.source)
        self.assertIn("read_generation(", self.source)
        for forbidden in (".list_blobs(", ".delete_blob(", "find_latest(", "select_latest("):
            self.assertNotIn(forbidden, self.source)


class QualityPilotRegressionTests(unittest.TestCase):
    def test_storage_policy_version_and_content_type_are_pinned(self) -> None:
        self.assertEqual(QUALITY_OBSERVATION_STORE_POLICY_VERSION, "quality_observation_store_v1")
        self.assertEqual(QUALITY_OBSERVATION_CONTENT_TYPE, "application/json")

    def test_shared_maximum_encoded_bytes_ceiling_is_not_duplicated_or_widened(self) -> None:
        self.assertEqual(
            observation_store_module.MAXIMUM_ENCODED_BYTES, 32 * 1024 * 1024
        )

    def test_chunk_route_ceiling_is_the_accepted_exported_canonical_response_ceiling(
        self,
    ) -> None:
        self.assertIs(
            observation_store_module.MAXIMUM_CHUNK_COUNT,
            canonical_response_module.MAXIMUM_CHUNK_COUNT,
        )
        observation = _catalog_observation()
        published, _writer = _publish(observation)
        with self.assertRaises(QualityPilotObservationStoreError):
            _pinned_request_for(
                observation,
                published,
                chunk_index=1,
                chunk_count=observation_store_module.MAXIMUM_CHUNK_COUNT + 1,
            )

    def test_store_module_defines_no_duplicate_private_chunk_ceiling(self) -> None:
        source = inspect.getsource(observation_store_module)
        self.assertNotIn("_MAXIMUM_CHUNK_COUNT", source)

    def test_protocol_hash_is_pinned(self) -> None:
        self.assertEqual(
            PILOT_PROTOCOL_SHA256,
            "b29e34fdc3f62134034fbf032cf67d39ce06acd5082f9d1de680fcac2260f8e5",
        )

    def test_canonical_path_grammar_is_pinned(self) -> None:
        observation = _catalog_observation()
        path = canonical_observation_object_name(
            observation.window.pilot_run_id,
            observation.window.market_session,
            observation.window.window_kind,
            observation.request.chunk_index,
            observation.request.chunk_count,
            observation.observation_id,
        )
        expected = (
            f"quality-pilot/v1/{observation.window.pilot_run_id}/"
            f"{observation.window.market_session.isoformat()}/"
            f"{observation.window.window_kind.value}/1-of-1/"
            f"{observation.observation_id}.json"
        )
        self.assertEqual(path, expected)


if __name__ == "__main__":
    unittest.main()
