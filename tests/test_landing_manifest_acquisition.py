from __future__ import annotations

import ast
import hashlib
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

from india_swing.daily_pipeline.acquisition import (
    AcquisitionError,
    GCSObjectPayload,
    LandingManifestObjectRequest,
)
from india_swing.daily_pipeline.landing_manifest import (
    MAXIMUM_LANDING_MANIFEST_BYTES,
    LandingManifestVerifier,
    TrustedLandingManifestBinding,
    VerifiedLandingManifest,
)
from india_swing.daily_pipeline.landing_manifest_acquisition import (
    AcquiredLandingManifest,
    LandingManifestAcquisitionError,
    acquire_verified_landing_manifest,
)

from tests.test_landing_manifest import (
    _BUCKET,
    _CUTOFF,
    _NOT_BEFORE,
    _TARGET_SESSION,
    _binding_for,
    _encode,
    _valid_manifest_dict,
)


def _manifest_object_name(target_session: date = _TARGET_SESSION) -> str:
    return f"landing/{target_session.isoformat()}/landing-manifest.json"


def _valid_manifest_bytes(*, target_session: date = _TARGET_SESSION) -> bytes:
    return _encode(_valid_manifest_dict(target_session=target_session))


def _valid_manifest_bytes_padded_to_limit() -> bytes:
    base = _valid_manifest_bytes()
    pad_length = MAXIMUM_LANDING_MANIFEST_BYTES - len(base)
    assert pad_length >= 0
    return base + b" " * pad_length


def _valid_request(
    *,
    target_session: date = _TARGET_SESSION,
    bucket: str = _BUCKET,
    generation: int = 777,
) -> LandingManifestObjectRequest:
    return LandingManifestObjectRequest(
        bucket=bucket,
        object_name=_manifest_object_name(target_session),
        generation=generation,
        target_session=target_session,
    )


class FakeGCSObjectReader:
    """Fake GCSObjectReader. Never contacts GCP; records every call made."""

    def __init__(
        self, *, response: object = None, raises: BaseException | None = None
    ) -> None:
        self._response = response
        self._raises = raises
        self.calls: list[dict[str, object]] = []

    def read_generation(
        self, *, bucket: str, object_name: str, generation: int, maximum_bytes: int
    ) -> GCSObjectPayload:
        self.calls.append(
            {
                "bucket": bucket,
                "object_name": object_name,
                "generation": generation,
                "maximum_bytes": maximum_bytes,
            }
        )
        if self._raises is not None:
            raise self._raises
        return self._response


def _reader_for(manifest_bytes: bytes, *, generation: int = 777) -> FakeGCSObjectReader:
    return FakeGCSObjectReader(
        response=GCSObjectPayload(content_bytes=manifest_bytes, generation=generation)
    )


class LandingManifestObjectRequestTests(unittest.TestCase):
    def test_accepts_canonical_request(self) -> None:
        request = _valid_request()
        self.assertEqual(request.object_name, _manifest_object_name())
        self.assertEqual(request.generation, 777)

    def test_rejects_wrong_session_in_object_path(self) -> None:
        with self.assertRaises(AcquisitionError):
            LandingManifestObjectRequest(
                bucket=_BUCKET,
                object_name="landing/2026-07-17/landing-manifest.json",
                generation=777,
                target_session=_TARGET_SESSION,
            )

    def test_rejects_path_traversal_object_name(self) -> None:
        with self.assertRaises(AcquisitionError):
            LandingManifestObjectRequest(
                bucket=_BUCKET,
                object_name="landing/../secrets/landing-manifest.json",
                generation=777,
                target_session=_TARGET_SESSION,
            )

    def test_rejects_backslash_object_name(self) -> None:
        with self.assertRaises(AcquisitionError):
            LandingManifestObjectRequest(
                bucket=_BUCKET,
                object_name="landing\\2026-07-16\\landing-manifest.json",
                generation=777,
                target_session=_TARGET_SESSION,
            )

    def test_rejects_absolute_object_name(self) -> None:
        with self.assertRaises(AcquisitionError):
            LandingManifestObjectRequest(
                bucket=_BUCKET,
                object_name="/landing/2026-07-16/landing-manifest.json",
                generation=777,
                target_session=_TARGET_SESSION,
            )

    def test_rejects_browser_renamed_manifest(self) -> None:
        with self.assertRaises(AcquisitionError):
            LandingManifestObjectRequest(
                bucket=_BUCKET,
                object_name="landing/2026-07-16/landing-manifest (1).json",
                generation=777,
                target_session=_TARGET_SESSION,
            )

    def test_rejects_unicode_confusable_object_name(self) -> None:
        # U+0455 CYRILLIC SMALL LETTER DZE visually resembles "s"; the
        # exact-equality check must reject any lookalike, not just enforce
        # a general shape.
        confusable = "landing/2026-07-16/landing-manifeѕt.json"
        with self.assertRaises(AcquisitionError):
            LandingManifestObjectRequest(
                bucket=_BUCKET,
                object_name=confusable,
                generation=777,
                target_session=_TARGET_SESSION,
            )

    def test_rejects_bool_generation(self) -> None:
        with self.assertRaises(AcquisitionError):
            LandingManifestObjectRequest(
                bucket=_BUCKET,
                object_name=_manifest_object_name(),
                generation=True,
                target_session=_TARGET_SESSION,
            )

    def test_rejects_zero_generation(self) -> None:
        with self.assertRaises(AcquisitionError):
            LandingManifestObjectRequest(
                bucket=_BUCKET,
                object_name=_manifest_object_name(),
                generation=0,
                target_session=_TARGET_SESSION,
            )

    def test_rejects_negative_generation(self) -> None:
        with self.assertRaises(AcquisitionError):
            LandingManifestObjectRequest(
                bucket=_BUCKET,
                object_name=_manifest_object_name(),
                generation=-1,
                target_session=_TARGET_SESSION,
            )

    def test_rejects_generation_above_int64_max(self) -> None:
        with self.assertRaises(AcquisitionError):
            LandingManifestObjectRequest(
                bucket=_BUCKET,
                object_name=_manifest_object_name(),
                generation=9223372036854775808,
                target_session=_TARGET_SESSION,
            )

    def test_rejects_invalid_bucket(self) -> None:
        with self.assertRaises(AcquisitionError):
            LandingManifestObjectRequest(
                bucket="Bad_Bucket!",
                object_name=_manifest_object_name(),
                generation=777,
                target_session=_TARGET_SESSION,
            )


class AcquireVerifiedLandingManifestAcceptanceTests(unittest.TestCase):
    def test_valid_acquisition_matches_direct_verifier_result(self) -> None:
        manifest_bytes = _valid_manifest_bytes()
        binding = _binding_for(manifest_bytes)
        request = _valid_request(generation=777)
        reader = _reader_for(manifest_bytes, generation=777)

        result = acquire_verified_landing_manifest(request, binding, reader)
        direct = LandingManifestVerifier().verify(manifest_bytes, binding)

        self.assertIsInstance(result, AcquiredLandingManifest)
        self.assertEqual(result.request, request)
        self.assertEqual(result.manifest, direct)
        self.assertEqual(result.request.bucket, request.bucket)
        self.assertEqual(result.request.object_name, request.object_name)
        self.assertEqual(result.request.generation, 777)
        self.assertEqual(result.request.target_session, _TARGET_SESSION)
        self.assertEqual(result.manifest.manifest_sha256, direct.manifest_sha256)
        self.assertEqual(result.manifest.manifest_bytes, manifest_bytes)

    def test_exact_reader_call_mapping_and_single_call(self) -> None:
        manifest_bytes = _valid_manifest_bytes()
        binding = _binding_for(manifest_bytes)
        request = _valid_request(generation=777)
        reader = _reader_for(manifest_bytes, generation=777)

        acquire_verified_landing_manifest(request, binding, reader)

        self.assertEqual(
            reader.calls,
            [
                {
                    "bucket": request.bucket,
                    "object_name": request.object_name,
                    "generation": 777,
                    "maximum_bytes": MAXIMUM_LANDING_MANIFEST_BYTES,
                }
            ],
        )

    def test_content_exactly_at_the_shared_limit_succeeds(self) -> None:
        manifest_bytes = _valid_manifest_bytes_padded_to_limit()
        self.assertEqual(len(manifest_bytes), MAXIMUM_LANDING_MANIFEST_BYTES)
        binding = _binding_for(manifest_bytes)
        request = _valid_request(generation=777)
        reader = _reader_for(manifest_bytes, generation=777)

        result = acquire_verified_landing_manifest(request, binding, reader)

        self.assertIsInstance(result, AcquiredLandingManifest)
        self.assertIsInstance(result.manifest, VerifiedLandingManifest)


class RequestBindingMismatchTests(unittest.TestCase):
    def test_bucket_mismatch_fails_before_any_read(self) -> None:
        binding = _binding_for(_valid_manifest_bytes(), bucket=_BUCKET)
        request = _valid_request(bucket="another-syntactically-valid-bucket")
        reader = FakeGCSObjectReader()

        with self.assertRaises(LandingManifestAcquisitionError):
            acquire_verified_landing_manifest(request, binding, reader)

        self.assertEqual(reader.calls, [])

    def test_target_session_mismatch_fails_before_any_read(self) -> None:
        binding = _binding_for(_valid_manifest_bytes(), target_session=_TARGET_SESSION)
        mismatched_session = date(2026, 7, 17)
        request = LandingManifestObjectRequest(
            bucket=_BUCKET,
            object_name=_manifest_object_name(mismatched_session),
            generation=777,
            target_session=mismatched_session,
        )
        reader = FakeGCSObjectReader()

        with self.assertRaises(LandingManifestAcquisitionError):
            acquire_verified_landing_manifest(request, binding, reader)

        self.assertEqual(reader.calls, [])

    def test_wrong_request_type_fails_before_any_read(self) -> None:
        binding = _binding_for(_valid_manifest_bytes())
        reader = FakeGCSObjectReader()

        with self.assertRaises(LandingManifestAcquisitionError):
            acquire_verified_landing_manifest("not-a-request", binding, reader)

        self.assertEqual(reader.calls, [])

    def test_wrong_binding_type_fails_before_any_read(self) -> None:
        request = _valid_request()
        reader = FakeGCSObjectReader()

        with self.assertRaises(LandingManifestAcquisitionError):
            acquire_verified_landing_manifest(request, "not-a-binding", reader)

        self.assertEqual(reader.calls, [])


class MutatedRequestBindingDefenseTests(unittest.TestCase):
    def test_mutated_request_bucket_fails_before_any_read(self) -> None:
        binding = _binding_for(_valid_manifest_bytes())
        request = _valid_request()
        object.__setattr__(request, "bucket", "another-syntactically-valid-bucket")
        reader = FakeGCSObjectReader()

        with self.assertRaises(LandingManifestAcquisitionError):
            acquire_verified_landing_manifest(request, binding, reader)

        self.assertEqual(reader.calls, [])

    def test_mutated_request_generation_to_invalid_value_fails_before_any_read(self) -> None:
        binding = _binding_for(_valid_manifest_bytes())
        request = _valid_request()
        object.__setattr__(request, "generation", -1)
        reader = FakeGCSObjectReader()

        with self.assertRaises(LandingManifestAcquisitionError):
            acquire_verified_landing_manifest(request, binding, reader)

        self.assertEqual(reader.calls, [])

    def test_mutated_binding_expected_hash_fails_before_any_read(self) -> None:
        binding = _binding_for(_valid_manifest_bytes())
        object.__setattr__(binding, "expected_manifest_sha256", "not-a-hash")
        request = _valid_request()
        reader = FakeGCSObjectReader()

        with self.assertRaises(LandingManifestAcquisitionError):
            acquire_verified_landing_manifest(request, binding, reader)

        self.assertEqual(reader.calls, [])

    def test_mutated_binding_target_session_fails_before_any_read(self) -> None:
        binding = _binding_for(_valid_manifest_bytes())
        object.__setattr__(binding, "target_session", date(2026, 7, 17))
        request = _valid_request()
        reader = FakeGCSObjectReader()

        with self.assertRaises(LandingManifestAcquisitionError):
            acquire_verified_landing_manifest(request, binding, reader)

        self.assertEqual(reader.calls, [])


class ReaderFailureTests(unittest.TestCase):
    def test_ordinary_reader_exception_is_sanitized(self) -> None:
        binding = _binding_for(_valid_manifest_bytes())
        request = _valid_request()
        secret = "secret-download-path-do-not-leak-6a2d"
        reader = FakeGCSObjectReader(raises=ValueError(secret))

        with self.assertRaises(LandingManifestAcquisitionError) as ctx:
            acquire_verified_landing_manifest(request, binding, reader)

        self.assertNotIn(secret, str(ctx.exception))
        self.assertEqual(len(reader.calls), 1)

    def test_base_exception_from_reader_propagates_unchanged(self) -> None:
        binding = _binding_for(_valid_manifest_bytes())
        request = _valid_request()
        sentinel = KeyboardInterrupt()
        reader = FakeGCSObjectReader(raises=sentinel)

        with self.assertRaises(KeyboardInterrupt) as ctx:
            acquire_verified_landing_manifest(request, binding, reader)

        self.assertIs(ctx.exception, sentinel)


class PayloadValidationTests(unittest.TestCase):
    def test_wrong_payload_type_fails(self) -> None:
        binding = _binding_for(_valid_manifest_bytes())
        request = _valid_request()
        reader = FakeGCSObjectReader(response="not-a-payload")

        with self.assertRaises(LandingManifestAcquisitionError):
            acquire_verified_landing_manifest(request, binding, reader)

    def test_missing_observed_generation_fails(self) -> None:
        manifest_bytes = _valid_manifest_bytes()
        binding = _binding_for(manifest_bytes)
        request = _valid_request()
        reader = FakeGCSObjectReader(
            response=GCSObjectPayload(content_bytes=manifest_bytes, generation=None)
        )

        with self.assertRaises(LandingManifestAcquisitionError):
            acquire_verified_landing_manifest(request, binding, reader)

    def test_bool_observed_generation_fails(self) -> None:
        manifest_bytes = _valid_manifest_bytes()
        binding = _binding_for(manifest_bytes)
        request = _valid_request(generation=1)
        reader = FakeGCSObjectReader(
            response=GCSObjectPayload(content_bytes=manifest_bytes, generation=True)
        )

        with self.assertRaises(LandingManifestAcquisitionError):
            acquire_verified_landing_manifest(request, binding, reader)

    def test_string_observed_generation_fails(self) -> None:
        manifest_bytes = _valid_manifest_bytes()
        binding = _binding_for(manifest_bytes)
        request = _valid_request()
        reader = FakeGCSObjectReader(
            response=GCSObjectPayload(content_bytes=manifest_bytes, generation="777")
        )

        with self.assertRaises(LandingManifestAcquisitionError):
            acquire_verified_landing_manifest(request, binding, reader)

    def test_non_positive_observed_generation_fails(self) -> None:
        manifest_bytes = _valid_manifest_bytes()
        binding = _binding_for(manifest_bytes)
        request = _valid_request()
        reader = FakeGCSObjectReader(
            response=GCSObjectPayload(content_bytes=manifest_bytes, generation=0)
        )

        with self.assertRaises(LandingManifestAcquisitionError):
            acquire_verified_landing_manifest(request, binding, reader)

    def test_over_limit_observed_generation_fails(self) -> None:
        manifest_bytes = _valid_manifest_bytes()
        binding = _binding_for(manifest_bytes)
        request = _valid_request(generation=777)
        reader = FakeGCSObjectReader(
            response=GCSObjectPayload(content_bytes=manifest_bytes, generation=2**100)
        )

        with self.assertRaises(LandingManifestAcquisitionError):
            acquire_verified_landing_manifest(request, binding, reader)

    def test_mismatched_observed_generation_fails(self) -> None:
        manifest_bytes = _valid_manifest_bytes()
        binding = _binding_for(manifest_bytes)
        request = _valid_request(generation=777)
        reader = FakeGCSObjectReader(
            response=GCSObjectPayload(content_bytes=manifest_bytes, generation=999)
        )

        with self.assertRaises(LandingManifestAcquisitionError):
            acquire_verified_landing_manifest(request, binding, reader)

    def test_non_bytes_content_fails(self) -> None:
        binding = _binding_for(_valid_manifest_bytes())
        request = _valid_request()
        reader = FakeGCSObjectReader(
            response=GCSObjectPayload(content_bytes="not-bytes", generation=request.generation)
        )

        with self.assertRaises(LandingManifestAcquisitionError):
            acquire_verified_landing_manifest(request, binding, reader)

    def test_empty_content_fails(self) -> None:
        binding = _binding_for(_valid_manifest_bytes())
        request = _valid_request()
        reader = FakeGCSObjectReader(
            response=GCSObjectPayload(content_bytes=b"", generation=request.generation)
        )

        with self.assertRaises(LandingManifestAcquisitionError):
            acquire_verified_landing_manifest(request, binding, reader)

    def test_content_one_byte_over_the_shared_limit_fails(self) -> None:
        oversized = b"a" * (MAXIMUM_LANDING_MANIFEST_BYTES + 1)
        binding = TrustedLandingManifestBinding(
            expected_manifest_sha256=hashlib.sha256(oversized).hexdigest(),
            allowed_bucket=_BUCKET,
            target_session=_TARGET_SESSION,
            not_before=_NOT_BEFORE,
            cutoff=_CUTOFF,
        )
        request = _valid_request()
        reader = FakeGCSObjectReader(
            response=GCSObjectPayload(content_bytes=oversized, generation=request.generation)
        )

        with self.assertRaises(LandingManifestAcquisitionError):
            acquire_verified_landing_manifest(request, binding, reader)

    def test_hash_mismatch_fails(self) -> None:
        manifest_bytes = _valid_manifest_bytes()
        other_bytes = manifest_bytes + b" "
        binding = _binding_for(other_bytes)
        request = _valid_request()
        reader = _reader_for(manifest_bytes, generation=request.generation)

        with self.assertRaises(LandingManifestAcquisitionError):
            acquire_verified_landing_manifest(request, binding, reader)


class VerifierStageFailureTests(unittest.TestCase):
    def test_invalid_utf8_after_hash_match_fails_at_verification_stage(self) -> None:
        bad_bytes = b"\xff\xfe\x00\x01not-valid-utf8"
        binding = _binding_for(bad_bytes)
        request = _valid_request()
        reader = _reader_for(bad_bytes, generation=request.generation)

        with self.assertRaises(LandingManifestAcquisitionError):
            acquire_verified_landing_manifest(request, binding, reader)

    def test_malformed_json_after_hash_match_fails_at_verification_stage(self) -> None:
        bad_bytes = b'{"schema_version": 1, "objects": ['
        binding = _binding_for(bad_bytes)
        request = _valid_request()
        reader = _reader_for(bad_bytes, generation=request.generation)

        with self.assertRaises(LandingManifestAcquisitionError):
            acquire_verified_landing_manifest(request, binding, reader)

    def test_duplicate_json_key_after_hash_match_fails_at_verification_stage(self) -> None:
        text = (
            '{"schema_version":1,"schema_version":1,"knowledge_time":"2026-07-16T13:30:00Z",'
            '"target_session":"2026-07-16","objects":[]}'
        )
        bad_bytes = text.encode("utf-8")
        binding = _binding_for(bad_bytes)
        request = _valid_request()
        reader = _reader_for(bad_bytes, generation=request.generation)

        with self.assertRaises(LandingManifestAcquisitionError):
            acquire_verified_landing_manifest(request, binding, reader)

    def test_future_knowledge_time_after_hash_match_fails_at_verification_stage(self) -> None:
        manifest_bytes = _encode(_valid_manifest_dict(knowledge_time="2026-07-16T23:59:59Z"))
        binding = _binding_for(
            manifest_bytes, cutoff=datetime(2026, 7, 16, 10, 0, 0, tzinfo=timezone.utc)
        )
        request = _valid_request()
        reader = _reader_for(manifest_bytes, generation=request.generation)

        with self.assertRaises(LandingManifestAcquisitionError):
            acquire_verified_landing_manifest(request, binding, reader)


class AcquiredLandingManifestDirectConstructionTests(unittest.TestCase):
    def _valid_pair(self) -> tuple[LandingManifestObjectRequest, VerifiedLandingManifest]:
        manifest_bytes = _valid_manifest_bytes()
        binding = _binding_for(manifest_bytes)
        manifest = LandingManifestVerifier().verify(manifest_bytes, binding)
        request = _valid_request(generation=777)
        return request, manifest

    def test_valid_direct_construction_succeeds(self) -> None:
        request, manifest = self._valid_pair()

        wrapper = AcquiredLandingManifest(request=request, manifest=manifest)

        self.assertIs(wrapper.request, request)
        self.assertIs(wrapper.manifest, manifest)

    def test_wrong_request_type_fails(self) -> None:
        _, manifest = self._valid_pair()
        with self.assertRaises(LandingManifestAcquisitionError):
            AcquiredLandingManifest(request="not-a-request", manifest=manifest)

    def test_wrong_manifest_type_fails(self) -> None:
        request, _ = self._valid_pair()
        with self.assertRaises(LandingManifestAcquisitionError):
            AcquiredLandingManifest(request=request, manifest="not-a-manifest")

    def test_bucket_mismatch_between_request_and_manifest_binding_fails(self) -> None:
        _, manifest = self._valid_pair()
        mismatched_request = LandingManifestObjectRequest(
            bucket="another-syntactically-valid-bucket",
            object_name=_manifest_object_name(),
            generation=777,
            target_session=_TARGET_SESSION,
        )

        with self.assertRaises(LandingManifestAcquisitionError):
            AcquiredLandingManifest(request=mismatched_request, manifest=manifest)

    def test_target_session_mismatch_between_request_and_manifest_fails(self) -> None:
        _, manifest = self._valid_pair()
        mismatched_session = date(2026, 7, 17)
        mismatched_request = LandingManifestObjectRequest(
            bucket=_BUCKET,
            object_name=_manifest_object_name(mismatched_session),
            generation=777,
            target_session=mismatched_session,
        )

        with self.assertRaises(LandingManifestAcquisitionError):
            AcquiredLandingManifest(request=mismatched_request, manifest=manifest)

    def test_mutated_nested_manifest_field_fails_closed(self) -> None:
        request, manifest = self._valid_pair()
        object.__setattr__(manifest, "knowledge_time", "tampered-not-a-datetime")

        with self.assertRaises(LandingManifestAcquisitionError):
            AcquiredLandingManifest(request=request, manifest=manifest)

    def test_message_is_static_and_never_leaks_content(self) -> None:
        _, manifest = self._valid_pair()
        secret = "secret-bucket-do-not-leak-4d8e"
        mismatched_request = LandingManifestObjectRequest(
            bucket=secret,
            object_name=_manifest_object_name(),
            generation=777,
            target_session=_TARGET_SESSION,
        )

        with self.assertRaises(LandingManifestAcquisitionError) as ctx:
            AcquiredLandingManifest(request=mismatched_request, manifest=manifest)

        self.assertNotIn(secret, str(ctx.exception))


class _MutatingReader:
    """Fake GCSObjectReader that, on read_generation, runs a caller-supplied
    mutation against objects the test still holds a reference to (the
    original request/binding), then returns a caller-supplied payload.
    Records every call made so a test can prove the reader itself received
    only pre-mutation values.
    """

    def __init__(self, *, mutate, response: object) -> None:
        self._mutate = mutate
        self._response = response
        self.calls: list[dict[str, object]] = []

    def read_generation(
        self, *, bucket: str, object_name: str, generation: int, maximum_bytes: int
    ) -> object:
        self.calls.append(
            {
                "bucket": bucket,
                "object_name": object_name,
                "generation": generation,
                "maximum_bytes": maximum_bytes,
            }
        )
        self._mutate()
        return self._response


class InCallMutationAttackTests(unittest.TestCase):
    def test_reader_mutating_original_binding_hash_cannot_make_tampered_payload_acceptable(
        self,
    ) -> None:
        manifest_bytes = _valid_manifest_bytes()
        binding = _binding_for(manifest_bytes)
        request = _valid_request(generation=777)
        original_expected_hash = binding.expected_manifest_sha256

        tampered_bytes = manifest_bytes + b"tampered-extra-bytes-not-in-trusted-manifest"
        tampered_hash = hashlib.sha256(tampered_bytes).hexdigest()

        def mutate() -> None:
            # Attempt to retroactively make the tampered payload's hash the
            # "trusted" one by mutating the ORIGINAL binding object the
            # caller still holds a reference to.
            object.__setattr__(binding, "expected_manifest_sha256", tampered_hash)

        reader = _MutatingReader(
            mutate=mutate,
            response=GCSObjectPayload(content_bytes=tampered_bytes, generation=777),
        )

        with self.assertRaises(LandingManifestAcquisitionError):
            acquire_verified_landing_manifest(request, binding, reader)

        # The reader still received the pre-mutation call arguments.
        self.assertEqual(len(reader.calls), 1)
        self.assertEqual(reader.calls[0]["bucket"], request.bucket)
        self.assertEqual(reader.calls[0]["object_name"], request.object_name)
        self.assertEqual(reader.calls[0]["generation"], request.generation)
        # The original binding object was indeed mutated by the attacker...
        self.assertEqual(binding.expected_manifest_sha256, tampered_hash)
        self.assertNotEqual(original_expected_hash, tampered_hash)

    def test_reader_mutating_original_request_bucket_does_not_affect_returned_lineage(
        self,
    ) -> None:
        manifest_bytes = _valid_manifest_bytes()
        binding = _binding_for(manifest_bytes)
        request = _valid_request(generation=777)
        original_bucket = request.bucket

        def mutate() -> None:
            object.__setattr__(request, "bucket", "another-syntactically-valid-bucket")

        reader = _MutatingReader(
            mutate=mutate,
            response=GCSObjectPayload(content_bytes=manifest_bytes, generation=777),
        )

        result = acquire_verified_landing_manifest(request, binding, reader)

        # The call the reader actually received used the pre-mutation bucket.
        self.assertEqual(reader.calls[0]["bucket"], original_bucket)
        # The original request object was indeed mutated by the attacker...
        self.assertEqual(request.bucket, "another-syntactically-valid-bucket")
        # ...but the returned wrapper's provenance is bound to the
        # pre-mutation snapshot, not the now-tampered original.
        self.assertIsInstance(result, AcquiredLandingManifest)
        self.assertEqual(result.request.bucket, original_bucket)

    def test_reader_mutating_original_binding_target_session_does_not_change_verification(
        self,
    ) -> None:
        manifest_bytes = _valid_manifest_bytes()
        binding = _binding_for(manifest_bytes)
        request = _valid_request(generation=777)
        original_target_session = binding.target_session

        def mutate() -> None:
            object.__setattr__(binding, "target_session", date(2099, 1, 1))

        reader = _MutatingReader(
            mutate=mutate,
            response=GCSObjectPayload(content_bytes=manifest_bytes, generation=777),
        )

        result = acquire_verified_landing_manifest(request, binding, reader)

        self.assertEqual(result.manifest.target_session, original_target_session)
        self.assertEqual(binding.target_session, date(2099, 1, 1))


_EXACT_ALLOWED_ACQUISITION_IMPORTS = frozenset((
    # (level, module, imported name, asname); level 0 covers both plain
    # `import x` (module=alias.name, imported name=None) and absolute
    # `from x import y`. Level > 0 covers `from .x import y` within the
    # daily_pipeline package. This is a closed set: any import in
    # landing_manifest_acquisition.py not exactly in this set, and any
    # entry here missing from that file, fails the equality assertion below.
    (0, "__future__", "annotations", None),
    (0, "hashlib", None, None),
    (0, "dataclasses", "dataclass", None),
    (1, "acquisition", "GCSObjectPayload", None),
    (1, "acquisition", "GCSObjectReader", None),
    (1, "acquisition", "LandingManifestObjectRequest", None),
    (1, "landing_manifest", "MAXIMUM_LANDING_MANIFEST_BYTES", None),
    (1, "landing_manifest", "LandingManifestVerifier", None),
    (1, "landing_manifest", "TrustedLandingManifestBinding", None),
    (1, "landing_manifest", "VerifiedLandingManifest", None),
))

_EXACT_ALLOWED_ACQUISITION_CALL_TARGETS = frozenset((
    # The production module's entire callable surface: the dataclass()
    # decorator factory; the type()/len()/ValueError() building blocks used
    # for independent inline validation; reconstructing the request/binding
    # types (including AcquiredLandingManifest.__post_init__'s own defensive
    # reconstruction/re-verification); invoking the injected reader;
    # hashing/hex-encoding the downloaded bytes; raising the module's own
    # error type; and constructing/calling the manifest verifier. Any other
    # call name -- a GCS/storage client, requests/urllib, os/subprocess, a
    # broker/order/notification helper, a strategy/model/LLM call, a
    # listing/"latest" helper, or a retry/fallback wrapper -- fails this
    # test.
    "dataclass",
    "type",
    "len",
    "ValueError",
    "AcquiredLandingManifest",
    "LandingManifestObjectRequest",
    "TrustedLandingManifestBinding",
    "LandingManifestAcquisitionError",
    "read_generation",
    "sha256",
    "hexdigest",
    "LandingManifestVerifier",
    "verify",
))

_FORBIDDEN_ACQUISITION_NAME_TOKENS = (
    # "gcs" is deliberately excluded: GCSObjectReader/GCSObjectPayload are
    # the exact, already-permitted injected protocol/dataclass this module
    # composes against, and a bare substring match would flag their own
    # legitimate type names as if they were client-construction capability.
    "google",
    "storage",
    "blob",
    "bucketlist",
    "client",
    "requests",
    "urllib",
    "http",
    "socket",
    "subprocess",
    "broker",
    "retry",
    "fallback",
    "latest",
    "listdir",
    "scandir",
    "walk",
    "environ",
    "getenv",
    "now",
    "utcnow",
    "today",
    "notify",
    "notification",
    "order",
    "strategy",
    "model",
    "llm",
    "write",
    "upload",
    "delete",
    "list",
)


class LandingManifestAcquisitionCapabilityTests(unittest.TestCase):
    """Proves landing_manifest_acquisition.py introduces no storage-client
    construction, network API, listing/latest selection, retry/fallback,
    second-source substitution, environment/current-clock, filesystem
    discovery, subprocess, notification, broker/order, strategy/model/LLM,
    scheduler, deployment, or GCS write capability. Imports and the
    callable surface are both locked to exact closed sets.
    """

    def _module_ast(self) -> ast.Module:
        source = (
            Path(__file__).resolve().parent.parent
            / "src"
            / "india_swing"
            / "daily_pipeline"
            / "landing_manifest_acquisition.py"
        ).read_text(encoding="utf-8")
        return ast.parse(source)

    def test_imports_match_an_exact_allowlist(self) -> None:
        tree = self._module_ast()
        actual: set[tuple[int, str, str | None, str | None]] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    actual.add((0, alias.name, None, alias.asname))
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                level = node.level or 0
                for alias in node.names:
                    actual.add((level, module, alias.name, alias.asname))
        self.assertEqual(actual, _EXACT_ALLOWED_ACQUISITION_IMPORTS)

    def test_callable_surface_is_locked_to_an_exact_allowlist(self) -> None:
        tree = self._module_ast()
        offenders = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if isinstance(func, ast.Name):
                target = func.id
            elif isinstance(func, ast.Attribute):
                target = func.attr
            else:
                offenders.append(ast.dump(func))
                continue
            if target not in _EXACT_ALLOWED_ACQUISITION_CALL_TARGETS:
                offenders.append(target)
        self.assertEqual(offenders, [])

    def test_identifiers_carry_no_disallowed_capability_token(self) -> None:
        tree = self._module_ast()
        offenders = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Name):
                candidate = node.id.lower()
            elif isinstance(node, ast.Attribute):
                candidate = node.attr.lower()
            else:
                continue
            for token in _FORBIDDEN_ACQUISITION_NAME_TOKENS:
                if token in candidate:
                    offenders.append(candidate)
        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
