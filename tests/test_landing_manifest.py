from __future__ import annotations

import dataclasses
import hashlib
import json
import unittest
from datetime import date, datetime, timedelta, timezone

from india_swing.daily_pipeline.acquisition import AcquisitionFileType, LandingObjectRequest
from india_swing.daily_pipeline.landing_manifest import (
    LandingManifestError,
    LandingManifestVerifier,
    TrustedLandingManifestBinding,
    VerifiedLandingManifest,
)

_TARGET_SESSION = date(2026, 7, 16)
_BUCKET = "trusted-bucket"
_KNOWLEDGE_TIME = "2026-07-16T13:30:00Z"
_NOT_BEFORE = datetime(2026, 7, 16, 0, 0, 0, tzinfo=timezone.utc)
_CUTOFF = datetime(2026, 7, 16, 23, 59, 59, tzinfo=timezone.utc)
_SM_SHA256 = hashlib.sha256(b"security-master-content").hexdigest()
_DB_SHA256 = hashlib.sha256(b"daily-bundle-content").hexdigest()


def _sm_object_name(target_session: date = _TARGET_SESSION) -> str:
    return f"landing/{target_session.isoformat()}/NSE_CM_security_{target_session.strftime('%d%m%Y')}.csv.gz"


def _db_object_name(target_session: date = _TARGET_SESSION) -> str:
    return f"landing/{target_session.isoformat()}/Reports-Daily-Multiple.zip"


def _valid_objects(
    *,
    target_session: date = _TARGET_SESSION,
    bucket: str = _BUCKET,
    sm_generation: object = 123,
    sm_sha256: str = _SM_SHA256,
    sm_object_name: str | None = None,
    db_generation: object = 456,
    db_sha256: str = _DB_SHA256,
    db_object_name: str | None = None,
) -> list[dict[str, object]]:
    return [
        {
            "file_type": "SECURITY_MASTER",
            "bucket": bucket,
            "object_name": sm_object_name if sm_object_name is not None else _sm_object_name(target_session),
            "generation": sm_generation,
            "sha256": sm_sha256,
        },
        {
            "file_type": "DAILY_BUNDLE",
            "bucket": bucket,
            "object_name": db_object_name if db_object_name is not None else _db_object_name(target_session),
            "generation": db_generation,
            "sha256": db_sha256,
        },
    ]


def _valid_manifest_dict(
    *,
    target_session: date = _TARGET_SESSION,
    knowledge_time: str = _KNOWLEDGE_TIME,
    objects: list[dict[str, object]] | None = None,
    schema_version: object = 1,
) -> dict[str, object]:
    return {
        "schema_version": schema_version,
        "knowledge_time": knowledge_time,
        "target_session": target_session.isoformat(),
        "objects": objects if objects is not None else _valid_objects(target_session=target_session),
    }


def _encode(manifest: dict[str, object]) -> bytes:
    return json.dumps(manifest, separators=(",", ":")).encode("utf-8")


def _binding_for(
    manifest_bytes: bytes,
    *,
    bucket: str = _BUCKET,
    target_session: date = _TARGET_SESSION,
    not_before: datetime = _NOT_BEFORE,
    cutoff: datetime = _CUTOFF,
) -> TrustedLandingManifestBinding:
    return TrustedLandingManifestBinding(
        expected_manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        allowed_bucket=bucket,
        target_session=target_session,
        not_before=not_before,
        cutoff=cutoff,
    )


def _valid_pair() -> tuple[bytes, TrustedLandingManifestBinding]:
    manifest_bytes = _encode(_valid_manifest_dict())
    return manifest_bytes, _binding_for(manifest_bytes)


def _handcrafted_valid_text(
    *,
    target_session: date = _TARGET_SESSION,
    bucket: str = _BUCKET,
    knowledge_time: str = _KNOWLEDGE_TIME,
) -> str:
    return (
        "{"
        '"schema_version":1,'
        f'"knowledge_time":"{knowledge_time}",'
        f'"target_session":"{target_session.isoformat()}",'
        '"objects":['
        f'{{"file_type":"SECURITY_MASTER","bucket":"{bucket}",'
        f'"object_name":"{_sm_object_name(target_session)}","generation":123,"sha256":"{_SM_SHA256}"}},'
        f'{{"file_type":"DAILY_BUNDLE","bucket":"{bucket}",'
        f'"object_name":"{_db_object_name(target_session)}","generation":456,"sha256":"{_DB_SHA256}"}}'
        "]}"
    )


class TrustedLandingManifestBindingTests(unittest.TestCase):
    def test_accepts_valid_binding(self) -> None:
        binding = TrustedLandingManifestBinding(
            expected_manifest_sha256="0" * 64,
            allowed_bucket=_BUCKET,
            target_session=_TARGET_SESSION,
            not_before=_NOT_BEFORE,
            cutoff=_CUTOFF,
        )
        self.assertEqual(binding.allowed_bucket, _BUCKET)

    def test_rejects_malformed_expected_hash(self) -> None:
        for bad in ("NOT-A-HASH", "0" * 63, "0" * 65, ("A" * 64)):
            with self.assertRaises(LandingManifestError):
                TrustedLandingManifestBinding(
                    expected_manifest_sha256=bad,
                    allowed_bucket=_BUCKET,
                    target_session=_TARGET_SESSION,
                    not_before=_NOT_BEFORE,
                    cutoff=_CUTOFF,
                )

    def test_rejects_invalid_bucket(self) -> None:
        with self.assertRaises(LandingManifestError):
            TrustedLandingManifestBinding(
                expected_manifest_sha256="0" * 64,
                allowed_bucket="Bad_Bucket!",
                target_session=_TARGET_SESSION,
                not_before=_NOT_BEFORE,
                cutoff=_CUTOFF,
            )

    def test_rejects_non_date_target_session(self) -> None:
        with self.assertRaises(LandingManifestError):
            TrustedLandingManifestBinding(
                expected_manifest_sha256="0" * 64,
                allowed_bucket=_BUCKET,
                target_session="2026-07-16",
                not_before=_NOT_BEFORE,
                cutoff=_CUTOFF,
            )

    def test_rejects_naive_cutoff(self) -> None:
        with self.assertRaises(LandingManifestError):
            TrustedLandingManifestBinding(
                expected_manifest_sha256="0" * 64,
                allowed_bucket=_BUCKET,
                target_session=_TARGET_SESSION,
                not_before=_NOT_BEFORE,
                cutoff=datetime(2026, 7, 16, 23, 59, 59),
            )

    def test_rejects_naive_not_before(self) -> None:
        with self.assertRaises(LandingManifestError):
            TrustedLandingManifestBinding(
                expected_manifest_sha256="0" * 64,
                allowed_bucket=_BUCKET,
                target_session=_TARGET_SESSION,
                not_before=datetime(2026, 7, 16, 0, 0, 0),
                cutoff=_CUTOFF,
            )

    def test_rejects_not_before_after_cutoff(self) -> None:
        with self.assertRaises(LandingManifestError):
            TrustedLandingManifestBinding(
                expected_manifest_sha256="0" * 64,
                allowed_bucket=_BUCKET,
                target_session=_TARGET_SESSION,
                not_before=datetime(2027, 1, 1, tzinfo=timezone.utc),
                cutoff=_CUTOFF,
            )

    def test_accepts_not_before_equal_to_cutoff(self) -> None:
        binding = TrustedLandingManifestBinding(
            expected_manifest_sha256="0" * 64,
            allowed_bucket=_BUCKET,
            target_session=_TARGET_SESSION,
            not_before=_CUTOFF,
            cutoff=_CUTOFF,
        )
        self.assertEqual(binding.not_before, _CUTOFF)


class LandingManifestVerifierAcceptanceTests(unittest.TestCase):
    def test_accepts_valid_manifest_and_returns_complete_lineage(self) -> None:
        manifest_bytes, binding = _valid_pair()
        verifier = LandingManifestVerifier()

        result = verifier.verify(manifest_bytes, binding)

        self.assertIsInstance(result, VerifiedLandingManifest)
        self.assertEqual(result.schema_version, 1)
        self.assertEqual(result.manifest_sha256, binding.expected_manifest_sha256)
        self.assertEqual(result.manifest_bytes, manifest_bytes)
        self.assertEqual(result.target_session, _TARGET_SESSION)
        self.assertEqual(
            result.knowledge_time, datetime(2026, 7, 16, 13, 30, 0, tzinfo=timezone.utc)
        )
        self.assertIsInstance(result.security_master, LandingObjectRequest)
        self.assertIsInstance(result.daily_bundle, LandingObjectRequest)
        self.assertEqual(result.security_master.file_type, AcquisitionFileType.SECURITY_MASTER)
        self.assertEqual(result.daily_bundle.file_type, AcquisitionFileType.DAILY_BUNDLE)
        self.assertEqual(result.security_master.generation, 123)
        self.assertEqual(result.daily_bundle.generation, 456)
        self.assertEqual(result.security_master.object_name, _sm_object_name())
        self.assertEqual(result.daily_bundle.object_name, _db_object_name())
        self.assertIs(result.binding, binding)

    def test_reversed_object_order_produces_same_named_result(self) -> None:
        forward = _valid_objects()
        reversed_objects = list(reversed(forward))
        manifest_bytes = _encode(_valid_manifest_dict(objects=reversed_objects))
        binding = _binding_for(manifest_bytes)

        result = LandingManifestVerifier().verify(manifest_bytes, binding)

        self.assertEqual(result.security_master.file_type, AcquisitionFileType.SECURITY_MASTER)
        self.assertEqual(result.daily_bundle.file_type, AcquisitionFileType.DAILY_BUNDLE)
        self.assertEqual(result.security_master.generation, 123)
        self.assertEqual(result.daily_bundle.generation, 456)

    def test_returned_dataclasses_are_frozen_and_bytes_are_exact(self) -> None:
        manifest_bytes, binding = _valid_pair()
        result = LandingManifestVerifier().verify(manifest_bytes, binding)

        with self.assertRaises(dataclasses.FrozenInstanceError):
            result.schema_version = 2  # type: ignore[misc]
        with self.assertRaises(dataclasses.FrozenInstanceError):
            binding.allowed_bucket = "other"  # type: ignore[misc]

        self.assertIs(type(result.manifest_bytes), bytes)
        self.assertEqual(result.manifest_bytes, manifest_bytes)


class LandingManifestVerifierHashBoundaryTests(unittest.TestCase):
    def test_one_byte_tampering_fails_the_external_hash(self) -> None:
        manifest_bytes, binding = _valid_pair()
        tampered = manifest_bytes[:-1] + bytes([manifest_bytes[-1] ^ 0x01])

        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(tampered, binding)

    def test_recomputing_a_self_declared_hash_cannot_bypass_the_external_binding(self) -> None:
        # An attacker changes the generation (and could recompute any value
        # INSIDE the manifest, including a self-declared manifest id/hash if
        # one existed) but the binding's expected hash is pinned to the
        # ORIGINAL trusted bytes. The tampered bytes' own sha256 will not
        # match, regardless of what the attacker recomputes internally.
        original_bytes, binding = _valid_pair()
        tampered_objects = _valid_objects(sm_generation=999)
        tampered_bytes = _encode(_valid_manifest_dict(objects=tampered_objects))
        self.assertNotEqual(original_bytes, tampered_bytes)

        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(tampered_bytes, binding)

    def test_internal_manifest_id_or_self_declared_hash_field_is_never_consulted(self) -> None:
        # Even if the manifest JSON smuggled in extra fields claiming to be
        # its own hash, the schema's exact-key-set check rejects the extra
        # field before any such value could be consulted.
        manifest = _valid_manifest_dict()
        manifest["manifest_id"] = hashlib.sha256(b"anything").hexdigest()
        manifest_bytes = _encode(manifest)
        binding = _binding_for(manifest_bytes)

        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(manifest_bytes, binding)


class LandingManifestVerifierBucketTests(unittest.TestCase):
    def test_syntactically_valid_non_allowed_bucket_fails(self) -> None:
        objects = _valid_objects(bucket="another-syntactically-valid-bucket")
        manifest_bytes = _encode(_valid_manifest_dict(objects=objects))
        binding = _binding_for(manifest_bytes, bucket=_BUCKET)

        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(manifest_bytes, binding)


class LandingManifestVerifierTimeTests(unittest.TestCase):
    def test_knowledge_time_after_cutoff_fails(self) -> None:
        cutoff = datetime(2026, 7, 16, 10, 0, 0, tzinfo=timezone.utc)
        manifest_bytes = _encode(_valid_manifest_dict(knowledge_time="2026-07-16T13:30:00Z"))
        binding = _binding_for(manifest_bytes, cutoff=cutoff)

        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(manifest_bytes, binding)

    def test_knowledge_time_equal_to_cutoff_succeeds(self) -> None:
        cutoff = datetime(2026, 7, 16, 13, 30, 0, tzinfo=timezone.utc)
        manifest_bytes = _encode(_valid_manifest_dict(knowledge_time="2026-07-16T13:30:00Z"))
        binding = _binding_for(manifest_bytes, cutoff=cutoff)

        result = LandingManifestVerifier().verify(manifest_bytes, binding)

        self.assertEqual(result.knowledge_time, cutoff)

    def test_knowledge_time_equal_to_not_before_succeeds(self) -> None:
        not_before = datetime(2026, 7, 16, 13, 30, 0, tzinfo=timezone.utc)
        manifest_bytes = _encode(_valid_manifest_dict(knowledge_time="2026-07-16T13:30:00Z"))
        binding = _binding_for(manifest_bytes, not_before=not_before, cutoff=_CUTOFF)

        result = LandingManifestVerifier().verify(manifest_bytes, binding)

        self.assertEqual(result.knowledge_time, not_before)

    def test_knowledge_time_before_not_before_fails(self) -> None:
        not_before = datetime(2026, 7, 16, 14, 0, 0, tzinfo=timezone.utc)
        manifest_bytes = _encode(_valid_manifest_dict(knowledge_time="2026-07-16T13:30:00Z"))
        binding = _binding_for(manifest_bytes, not_before=not_before, cutoff=_CUTOFF)

        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(manifest_bytes, binding)

    def test_naive_knowledge_time_fails(self) -> None:
        manifest_bytes = _encode(_valid_manifest_dict(knowledge_time="2026-07-16T13:30:00"))
        binding = _binding_for(manifest_bytes)

        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(manifest_bytes, binding)

    def test_non_utc_knowledge_time_fails(self) -> None:
        manifest_bytes = _encode(_valid_manifest_dict(knowledge_time="2026-07-16T19:00:00+05:30"))
        binding = _binding_for(manifest_bytes)

        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(manifest_bytes, binding)


class LandingManifestVerifierSessionAndObjectTests(unittest.TestCase):
    def test_target_session_mismatch_fails(self) -> None:
        manifest_bytes, _ = _valid_pair()
        binding = _binding_for(manifest_bytes, target_session=date(2026, 7, 17))

        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(manifest_bytes, binding)

    def test_wrong_date_in_object_path_fails(self) -> None:
        objects = _valid_objects(sm_object_name="landing/2026-07-16/NSE_CM_security_17072026.csv.gz")
        manifest_bytes = _encode(_valid_manifest_dict(objects=objects))
        binding = _binding_for(manifest_bytes)

        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(manifest_bytes, binding)

    def test_wrong_session_prefix_in_object_path_fails(self) -> None:
        objects = _valid_objects(sm_object_name="landing/2026-07-17/NSE_CM_security_16072026.csv.gz")
        manifest_bytes = _encode(_valid_manifest_dict(objects=objects))
        binding = _binding_for(manifest_bytes)

        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(manifest_bytes, binding)

    def test_path_traversal_in_object_name_fails(self) -> None:
        objects = _valid_objects(sm_object_name="landing/../secrets/NSE_CM_security_16072026.csv.gz")
        manifest_bytes = _encode(_valid_manifest_dict(objects=objects))
        binding = _binding_for(manifest_bytes)

        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(manifest_bytes, binding)

    def test_renamed_daily_bundle_object_fails(self) -> None:
        objects = _valid_objects(db_object_name="landing/2026-07-16/Reports-Daily-Multiple-Renamed.zip")
        manifest_bytes = _encode(_valid_manifest_dict(objects=objects))
        binding = _binding_for(manifest_bytes)

        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(manifest_bytes, binding)

    def test_uppercase_sha256_fails(self) -> None:
        objects = _valid_objects(sm_sha256=_SM_SHA256.upper())
        manifest_bytes = _encode(_valid_manifest_dict(objects=objects))
        binding = _binding_for(manifest_bytes)

        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(manifest_bytes, binding)

    def test_malformed_sha256_fails(self) -> None:
        objects = _valid_objects(sm_sha256="not-a-hash")
        manifest_bytes = _encode(_valid_manifest_dict(objects=objects))
        binding = _binding_for(manifest_bytes)

        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(manifest_bytes, binding)

    def test_missing_file_type_fails(self) -> None:
        objects = [_valid_objects()[0]]  # only SECURITY_MASTER, length 1
        manifest_bytes = _encode(_valid_manifest_dict(objects=objects))
        binding = _binding_for(manifest_bytes)

        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(manifest_bytes, binding)

    def test_duplicate_file_type_fails(self) -> None:
        sm = _valid_objects()[0]
        objects = [sm, dict(sm)]  # two SECURITY_MASTER entries, no DAILY_BUNDLE
        manifest_bytes = _encode(_valid_manifest_dict(objects=objects))
        binding = _binding_for(manifest_bytes)

        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(manifest_bytes, binding)

    def test_bool_generation_fails(self) -> None:
        objects = _valid_objects(sm_generation=True)
        manifest_bytes = _encode(_valid_manifest_dict(objects=objects))
        binding = _binding_for(manifest_bytes)

        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(manifest_bytes, binding)

    def test_zero_generation_fails(self) -> None:
        objects = _valid_objects(sm_generation=0)
        manifest_bytes = _encode(_valid_manifest_dict(objects=objects))
        binding = _binding_for(manifest_bytes)

        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(manifest_bytes, binding)

    def test_negative_generation_fails(self) -> None:
        objects = _valid_objects(sm_generation=-5)
        manifest_bytes = _encode(_valid_manifest_dict(objects=objects))
        binding = _binding_for(manifest_bytes)

        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(manifest_bytes, binding)

    def test_string_generation_fails(self) -> None:
        objects = _valid_objects(sm_generation="123")
        manifest_bytes = _encode(_valid_manifest_dict(objects=objects))
        binding = _binding_for(manifest_bytes)

        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(manifest_bytes, binding)

    def test_float_generation_fails(self) -> None:
        objects = _valid_objects(sm_generation=123.5)
        manifest_bytes = _encode(_valid_manifest_dict(objects=objects))
        binding = _binding_for(manifest_bytes)

        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(manifest_bytes, binding)


class LandingManifestVerifierSchemaTests(unittest.TestCase):
    def test_missing_top_level_key_fails(self) -> None:
        manifest = _valid_manifest_dict()
        del manifest["knowledge_time"]
        manifest_bytes = _encode(manifest)
        binding = _binding_for(manifest_bytes)

        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(manifest_bytes, binding)

    def test_extra_top_level_key_fails(self) -> None:
        manifest = _valid_manifest_dict()
        manifest["unexpected"] = "value"
        manifest_bytes = _encode(manifest)
        binding = _binding_for(manifest_bytes)

        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(manifest_bytes, binding)

    def test_missing_object_key_fails(self) -> None:
        objects = _valid_objects()
        del objects[0]["generation"]
        manifest_bytes = _encode(_valid_manifest_dict(objects=objects))
        binding = _binding_for(manifest_bytes)

        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(manifest_bytes, binding)

    def test_extra_object_key_fails(self) -> None:
        objects = _valid_objects()
        objects[0]["extra"] = "value"
        manifest_bytes = _encode(_valid_manifest_dict(objects=objects))
        binding = _binding_for(manifest_bytes)

        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(manifest_bytes, binding)

    def test_duplicate_top_level_json_key_fails(self) -> None:
        text = _handcrafted_valid_text()
        tampered = text.replace('"schema_version":1,', '"schema_version":1,"schema_version":1,', 1)
        manifest_bytes = tampered.encode("utf-8")
        binding = _binding_for(manifest_bytes)

        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(manifest_bytes, binding)

    def test_duplicate_nested_object_json_key_fails(self) -> None:
        text = _handcrafted_valid_text()
        needle = f'"bucket":"{_BUCKET}","object_name"'
        replacement = f'"bucket":"{_BUCKET}","bucket":"{_BUCKET}","object_name"'
        tampered = text.replace(needle, replacement, 1)
        self.assertNotEqual(text, tampered)
        manifest_bytes = tampered.encode("utf-8")
        binding = _binding_for(manifest_bytes)

        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(manifest_bytes, binding)

    def test_schema_version_must_be_exact_integer_one(self) -> None:
        for bad_version in (2, 0, "1", 1.0, True):
            manifest = _valid_manifest_dict(schema_version=bad_version)
            manifest_bytes = _encode(manifest)
            binding = _binding_for(manifest_bytes)
            with self.assertRaises(LandingManifestError):
                LandingManifestVerifier().verify(manifest_bytes, binding)

    def test_empty_bytes_fail(self) -> None:
        _, binding = _valid_pair()
        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(b"", binding)

    def test_non_bytes_input_fails(self) -> None:
        _, binding = _valid_pair()
        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify("not-bytes", binding)  # type: ignore[arg-type]
        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(bytearray(b"{}"), binding)  # type: ignore[arg-type]

    def test_oversized_manifest_fails(self) -> None:
        _, binding = _valid_pair()
        oversized = b"a" * (64 * 1024 + 1)
        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(oversized, binding)

    def test_invalid_utf8_fails(self) -> None:
        bad_bytes = b"\xff\xfe\x00\x01not-valid-utf8"
        binding = _binding_for(bad_bytes)
        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(bad_bytes, binding)

    def test_malformed_json_fails(self) -> None:
        bad_bytes = b'{"schema_version": 1, "objects": ['
        binding = _binding_for(bad_bytes)
        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(bad_bytes, binding)

    def test_nan_and_infinity_are_rejected(self) -> None:
        for token in ("NaN", "Infinity", "-Infinity"):
            bad_bytes = f'{{"schema_version": 1, "extra": {token}}}'.encode("utf-8")
            binding = _binding_for(bad_bytes)
            with self.assertRaises(LandingManifestError):
                LandingManifestVerifier().verify(bad_bytes, binding)

    def test_invalid_binding_type_fails(self) -> None:
        manifest_bytes, _ = _valid_pair()
        with self.assertRaises(LandingManifestError):
            LandingManifestVerifier().verify(manifest_bytes, "not-a-binding")  # type: ignore[arg-type]


class LandingManifestVerifierSanitizationTests(unittest.TestCase):
    def test_injected_secret_in_bucket_never_appears_in_error(self) -> None:
        secret = "SECRET-TOKEN-do-not-leak-9f3a"
        objects = _valid_objects(bucket=secret)
        manifest_bytes = _encode(_valid_manifest_dict(objects=objects))
        binding = _binding_for(manifest_bytes, bucket=_BUCKET)

        with self.assertRaises(LandingManifestError) as ctx:
            LandingManifestVerifier().verify(manifest_bytes, binding)
        self.assertNotIn(secret, str(ctx.exception))

    def test_injected_secret_in_object_name_never_appears_in_error(self) -> None:
        secret = "SECRET-PATH-do-not-leak-71cd"
        objects = _valid_objects(sm_object_name=f"landing/{secret}/traversal")
        manifest_bytes = _encode(_valid_manifest_dict(objects=objects))
        binding = _binding_for(manifest_bytes)

        with self.assertRaises(LandingManifestError) as ctx:
            LandingManifestVerifier().verify(manifest_bytes, binding)
        self.assertNotIn(secret, str(ctx.exception))

    def test_injected_secret_in_malformed_json_never_appears_in_error(self) -> None:
        secret = "SECRET-JSON-do-not-leak-44bb"
        bad_bytes = f'{{"schema_version": 1, "leak": "{secret}"'.encode("utf-8")
        binding = _binding_for(bad_bytes)

        with self.assertRaises(LandingManifestError) as ctx:
            LandingManifestVerifier().verify(bad_bytes, binding)
        self.assertNotIn(secret, str(ctx.exception))

    def test_hash_mismatch_error_never_contains_either_hash(self) -> None:
        manifest_bytes, binding = _valid_pair()
        tampered = manifest_bytes + b"x"

        with self.assertRaises(LandingManifestError) as ctx:
            LandingManifestVerifier().verify(tampered, binding)
        self.assertNotIn(binding.expected_manifest_sha256, str(ctx.exception))
        self.assertNotIn(hashlib.sha256(tampered).hexdigest(), str(ctx.exception))

    def test_huge_integer_token_raises_sanitized_error_not_raw_value_error(self) -> None:
        huge_digits = "9" * 5000
        bad_bytes = f'{{"schema_version": 1, "extra": {huge_digits}}}'.encode("utf-8")
        binding = _binding_for(bad_bytes)

        with self.assertRaises(LandingManifestError) as ctx:
            LandingManifestVerifier().verify(bad_bytes, binding)
        message = str(ctx.exception)
        self.assertNotIn(huge_digits, message)
        self.assertNotIn("5000", message)
        self.assertNotIn("int_max_str_digits", message)
        self.assertNotIn("ValueError", message)


class VerifiedLandingManifestDefensiveConstructionTests(unittest.TestCase):
    def test_direct_construction_without_binding_is_not_possible(self) -> None:
        manifest_bytes, binding = _valid_pair()
        result = LandingManifestVerifier().verify(manifest_bytes, binding)

        with self.assertRaises(TypeError):
            VerifiedLandingManifest(
                schema_version=result.schema_version,
                manifest_sha256=result.manifest_sha256,
                manifest_bytes=result.manifest_bytes,
                knowledge_time=result.knowledge_time,
                target_session=result.target_session,
                security_master=result.security_master,
                daily_bundle=result.daily_bundle,
            )  # type: ignore[call-arg]

    def test_direct_construction_with_hash_mismatched_binding_fails(self) -> None:
        manifest_bytes, binding = _valid_pair()
        result = LandingManifestVerifier().verify(manifest_bytes, binding)
        other_bytes = _encode(_valid_manifest_dict(objects=_valid_objects(sm_generation=999)))
        other_binding = _binding_for(other_bytes)

        with self.assertRaises(LandingManifestError):
            VerifiedLandingManifest(
                schema_version=result.schema_version,
                manifest_sha256=result.manifest_sha256,
                manifest_bytes=result.manifest_bytes,
                knowledge_time=result.knowledge_time,
                target_session=result.target_session,
                security_master=result.security_master,
                daily_bundle=result.daily_bundle,
                binding=other_binding,
            )

    def test_direct_construction_with_session_mismatched_binding_fails(self) -> None:
        manifest_bytes, binding = _valid_pair()
        result = LandingManifestVerifier().verify(manifest_bytes, binding)
        mismatched_binding = _binding_for(manifest_bytes, target_session=date(2026, 7, 17))

        with self.assertRaises(LandingManifestError):
            VerifiedLandingManifest(
                schema_version=result.schema_version,
                manifest_sha256=result.manifest_sha256,
                manifest_bytes=result.manifest_bytes,
                knowledge_time=result.knowledge_time,
                target_session=result.target_session,
                security_master=result.security_master,
                daily_bundle=result.daily_bundle,
                binding=mismatched_binding,
            )

    def test_direct_construction_with_bucket_mismatched_binding_fails(self) -> None:
        manifest_bytes, binding = _valid_pair()
        result = LandingManifestVerifier().verify(manifest_bytes, binding)
        mismatched_binding = _binding_for(manifest_bytes, bucket="another-syntactically-valid-bucket")

        with self.assertRaises(LandingManifestError):
            VerifiedLandingManifest(
                schema_version=result.schema_version,
                manifest_sha256=result.manifest_sha256,
                manifest_bytes=result.manifest_bytes,
                knowledge_time=result.knowledge_time,
                target_session=result.target_session,
                security_master=result.security_master,
                daily_bundle=result.daily_bundle,
                binding=mismatched_binding,
            )

    def test_direct_construction_with_knowledge_time_outside_binding_bounds_fails(self) -> None:
        manifest_bytes, binding = _valid_pair()
        result = LandingManifestVerifier().verify(manifest_bytes, binding)
        narrow_binding = _binding_for(
            manifest_bytes,
            not_before=datetime(2026, 7, 16, 20, 0, 0, tzinfo=timezone.utc),
            cutoff=_CUTOFF,
        )

        with self.assertRaises(LandingManifestError):
            VerifiedLandingManifest(
                schema_version=result.schema_version,
                manifest_sha256=result.manifest_sha256,
                manifest_bytes=result.manifest_bytes,
                knowledge_time=result.knowledge_time,  # 13:30, before narrow_binding.not_before=20:00
                target_session=result.target_session,
                security_master=result.security_master,
                daily_bundle=result.daily_bundle,
                binding=narrow_binding,
            )


class LandingManifestVerifierCapabilityTests(unittest.TestCase):
    def test_no_listing_or_latest_shaped_capability_exists(self) -> None:
        for candidate in (LandingManifestVerifier, TrustedLandingManifestBinding, VerifiedLandingManifest):
            members = [name for name in dir(candidate) if not name.startswith("__")]
            self.assertFalse(
                any("list" in name.lower() or "latest" in name.lower() for name in members),
                f"{candidate!r} unexpectedly exposes a listing/latest-shaped member",
            )


if __name__ == "__main__":
    unittest.main()
