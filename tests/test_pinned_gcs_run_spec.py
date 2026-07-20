from __future__ import annotations

import ast
import json
import unittest
from datetime import date, datetime, timedelta, timezone, tzinfo
from pathlib import Path

from india_swing.daily_pipeline.acquisition import LandingManifestObjectRequest
from india_swing.daily_pipeline.landing_manifest import TrustedLandingManifestBinding
from india_swing.daily_pipeline.pinned_gcs_run_spec import (
    MAXIMUM_PINNED_GCS_RUN_SPEC_BYTES,
    PINNED_GCS_RUN_SPEC_SCHEMA_VERSION,
    PinnedGCSRunSpec,
    PinnedGCSRunSpecError,
    parse_pinned_gcs_run_spec,
)

_BUCKET = "trusted-gcs-run-spec-bucket"
_SESSION = date(2026, 7, 20)
_SHA256_HEX = "a" * 64
_CALENDAR_ID = "b" * 64
_PREVIOUS_RUN_ID = "c" * 64
_NOT_BEFORE = "2026-07-20T00:00:00Z"
_BINDING_CUTOFF = "2026-07-20T14:00:00Z"
_RUN_CUTOFF = "2026-07-20T15:00:00Z"


def _manifest_object_name(session: date = _SESSION) -> str:
    return f"landing/{session.isoformat()}/landing-manifest.json"


def _session_value(value: object) -> object:
    return value.isoformat() if isinstance(value, date) else value


def _valid_spec_dict(
    *,
    schema_version: object = PINNED_GCS_RUN_SPEC_SCHEMA_VERSION,
    bucket: object = _BUCKET,
    allowed_bucket: object = _BUCKET,
    manifest_session: object = _SESSION,
    binding_session: object = _SESSION,
    run_session: object = _SESSION,
    object_name: object = None,
    generation: object = 777,
    expected_manifest_sha256: object = _SHA256_HEX,
    not_before: object = _NOT_BEFORE,
    binding_cutoff: object = _BINDING_CUTOFF,
    run_cutoff: object = _RUN_CUTOFF,
    calendar_materialization_id: object = _CALENDAR_ID,
    previous_run_id: object = None,
) -> dict[str, object]:
    resolved_object_name = (
        object_name
        if object_name is not None
        else _manifest_object_name(manifest_session if isinstance(manifest_session, date) else _SESSION)
    )
    return {
        "schema_version": schema_version,
        "manifest_request": {
            "bucket": bucket,
            "object_name": resolved_object_name,
            "generation": generation,
            "target_session": _session_value(manifest_session),
        },
        "trusted_binding": {
            "expected_manifest_sha256": expected_manifest_sha256,
            "allowed_bucket": allowed_bucket,
            "target_session": _session_value(binding_session),
            "not_before": not_before,
            "cutoff": binding_cutoff,
        },
        "run": {
            "market_session": _session_value(run_session),
            "cutoff": run_cutoff,
            "calendar_materialization_id": calendar_materialization_id,
            "previous_run_id": previous_run_id,
        },
    }


def _encode(spec: dict[str, object]) -> bytes:
    return json.dumps(spec, separators=(",", ":")).encode("utf-8")


def _valid_spec_text(**overrides: object) -> str:
    return json.dumps(_valid_spec_dict(**overrides), separators=(",", ":"))


def _valid_spec_bytes(**overrides: object) -> bytes:
    return _encode(_valid_spec_dict(**overrides))


def _valid_request(*, generation: int = 777, session: date = _SESSION) -> LandingManifestObjectRequest:
    return LandingManifestObjectRequest(
        bucket=_BUCKET,
        object_name=_manifest_object_name(session),
        generation=generation,
        target_session=session,
    )


def _valid_binding(*, session: date = _SESSION) -> TrustedLandingManifestBinding:
    return TrustedLandingManifestBinding(
        expected_manifest_sha256=_SHA256_HEX,
        allowed_bucket=_BUCKET,
        target_session=session,
        not_before=datetime(2026, 7, 20, 0, 0, 0, tzinfo=timezone.utc),
        cutoff=datetime(2026, 7, 20, 14, 0, 0, tzinfo=timezone.utc),
    )


def _valid_spec_kwargs(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = dict(
        schema_version=PINNED_GCS_RUN_SPEC_SCHEMA_VERSION,
        manifest_request=_valid_request(),
        trusted_binding=_valid_binding(),
        market_session=_SESSION,
        cutoff=datetime(2026, 7, 20, 15, 0, 0, tzinfo=timezone.utc),
        calendar_materialization_id=_CALENDAR_ID,
        previous_run_id=None,
    )
    base.update(overrides)
    return base


class _StrSubclass(str):
    pass


class _RaisingTzInfo(tzinfo):
    def __init__(self, secret: str) -> None:
        self._secret = secret

    def utcoffset(self, dt: object) -> None:
        raise RuntimeError(self._secret)

    def tzname(self, dt: object) -> None:
        raise RuntimeError(self._secret)

    def dst(self, dt: object) -> None:
        raise RuntimeError(self._secret)


class ParseAcceptanceTests(unittest.TestCase):
    def test_parses_exact_values(self) -> None:
        result = parse_pinned_gcs_run_spec(_valid_spec_bytes())

        self.assertIsInstance(result, PinnedGCSRunSpec)
        self.assertEqual(result.schema_version, PINNED_GCS_RUN_SPEC_SCHEMA_VERSION)
        self.assertIsInstance(result.manifest_request, LandingManifestObjectRequest)
        self.assertEqual(result.manifest_request.bucket, _BUCKET)
        self.assertEqual(result.manifest_request.object_name, _manifest_object_name())
        self.assertEqual(result.manifest_request.generation, 777)
        self.assertEqual(result.manifest_request.target_session, _SESSION)
        self.assertIsInstance(result.trusted_binding, TrustedLandingManifestBinding)
        self.assertEqual(result.trusted_binding.expected_manifest_sha256, _SHA256_HEX)
        self.assertEqual(result.trusted_binding.allowed_bucket, _BUCKET)
        self.assertEqual(result.trusted_binding.target_session, _SESSION)
        self.assertEqual(result.market_session, _SESSION)
        self.assertEqual(result.calendar_materialization_id, _CALENDAR_ID)
        self.assertIsNone(result.previous_run_id)

    def test_z_suffix_datetimes_normalize_to_utc(self) -> None:
        result = parse_pinned_gcs_run_spec(_valid_spec_bytes())

        self.assertEqual(result.trusted_binding.not_before, datetime(2026, 7, 20, 0, 0, 0, tzinfo=timezone.utc))
        self.assertEqual(result.trusted_binding.cutoff, datetime(2026, 7, 20, 14, 0, 0, tzinfo=timezone.utc))
        self.assertEqual(result.cutoff, datetime(2026, 7, 20, 15, 0, 0, tzinfo=timezone.utc))
        self.assertEqual(result.trusted_binding.not_before.tzinfo, timezone.utc)
        self.assertEqual(result.trusted_binding.cutoff.tzinfo, timezone.utc)
        self.assertEqual(result.cutoff.tzinfo, timezone.utc)

    def test_non_utc_offset_datetimes_normalize_to_utc(self) -> None:
        result = parse_pinned_gcs_run_spec(
            _valid_spec_bytes(
                not_before="2026-07-20T05:30:00+05:30",
                binding_cutoff="2026-07-20T19:30:00+05:30",
                run_cutoff="2026-07-20T20:30:00+05:30",
            )
        )

        self.assertEqual(result.trusted_binding.not_before, datetime(2026, 7, 20, 0, 0, 0, tzinfo=timezone.utc))
        self.assertEqual(result.trusted_binding.cutoff, datetime(2026, 7, 20, 14, 0, 0, tzinfo=timezone.utc))
        self.assertEqual(result.cutoff, datetime(2026, 7, 20, 15, 0, 0, tzinfo=timezone.utc))
        self.assertEqual(result.cutoff.tzinfo, timezone.utc)

    def test_fractional_seconds_are_accepted(self) -> None:
        result = parse_pinned_gcs_run_spec(_valid_spec_bytes(not_before="2026-07-20T00:00:00.123456Z"))
        self.assertEqual(
            result.trusted_binding.not_before,
            datetime(2026, 7, 20, 0, 0, 0, 123456, tzinfo=timezone.utc),
        )

    def test_two_parses_of_equal_input_produce_distinct_nested_object_identities(self) -> None:
        spec_bytes = _valid_spec_bytes()

        result_a = parse_pinned_gcs_run_spec(spec_bytes)
        result_b = parse_pinned_gcs_run_spec(spec_bytes)

        self.assertEqual(result_a, result_b)
        self.assertIsNot(result_a.manifest_request, result_b.manifest_request)
        self.assertIsNot(result_a.trusted_binding, result_b.trusted_binding)

    def test_explicit_null_previous_run_id_is_accepted(self) -> None:
        result = parse_pinned_gcs_run_spec(_valid_spec_bytes(previous_run_id=None))
        self.assertIsNone(result.previous_run_id)

    def test_valid_previous_run_id_is_retained_exactly(self) -> None:
        result = parse_pinned_gcs_run_spec(_valid_spec_bytes(previous_run_id=_PREVIOUS_RUN_ID))
        self.assertEqual(result.previous_run_id, _PREVIOUS_RUN_ID)

    def test_object_key_ordering_is_irrelevant(self) -> None:
        spec = _valid_spec_dict()
        reordered = {
            "run": spec["run"],
            "trusted_binding": spec["trusted_binding"],
            "manifest_request": {
                "target_session": spec["manifest_request"]["target_session"],
                "generation": spec["manifest_request"]["generation"],
                "object_name": spec["manifest_request"]["object_name"],
                "bucket": spec["manifest_request"]["bucket"],
            },
            "schema_version": spec["schema_version"],
        }

        result = parse_pinned_gcs_run_spec(_encode(reordered))

        self.assertEqual(result, parse_pinned_gcs_run_spec(_valid_spec_bytes()))


class ParseShapeTests(unittest.TestCase):
    def test_rejects_non_bytes_input(self) -> None:
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec("not-bytes")  # type: ignore[arg-type]

    def test_rejects_empty_bytes(self) -> None:
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(b"")

    def test_accepts_content_exactly_at_the_byte_limit(self) -> None:
        base = _valid_spec_bytes()
        pad_length = MAXIMUM_PINNED_GCS_RUN_SPEC_BYTES - len(base)
        self.assertGreaterEqual(pad_length, 0)
        padded = base + b" " * pad_length
        self.assertEqual(len(padded), MAXIMUM_PINNED_GCS_RUN_SPEC_BYTES)

        result = parse_pinned_gcs_run_spec(padded)

        self.assertIsInstance(result, PinnedGCSRunSpec)

    def test_rejects_content_one_byte_over_the_limit(self) -> None:
        oversized = b" " * (MAXIMUM_PINNED_GCS_RUN_SPEC_BYTES + 1)
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(oversized)

    def test_rejects_invalid_utf8(self) -> None:
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(b"\xff\xfe\x00\x01not-valid-utf8")

    def test_rejects_malformed_json(self) -> None:
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(b'{"schema_version": 1, "manifest_request": [')

    def test_rejects_non_object_top_level(self) -> None:
        for bad in (b"[]", b'"a string"', b"123", b"true", b"null"):
            with self.assertRaises(PinnedGCSRunSpecError):
                parse_pinned_gcs_run_spec(bad)

    def test_rejects_missing_top_level_key(self) -> None:
        spec = _valid_spec_dict()
        del spec["run"]
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(_encode(spec))

    def test_rejects_extra_top_level_key(self) -> None:
        spec = _valid_spec_dict()
        spec["unexpected"] = "value"
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(_encode(spec))

    def test_rejects_missing_manifest_request_key(self) -> None:
        spec = _valid_spec_dict()
        del spec["manifest_request"]["generation"]
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(_encode(spec))

    def test_rejects_extra_manifest_request_key(self) -> None:
        spec = _valid_spec_dict()
        spec["manifest_request"]["extra"] = "x"
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(_encode(spec))

    def test_rejects_missing_trusted_binding_key(self) -> None:
        spec = _valid_spec_dict()
        del spec["trusted_binding"]["not_before"]
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(_encode(spec))

    def test_rejects_extra_trusted_binding_key(self) -> None:
        spec = _valid_spec_dict()
        spec["trusted_binding"]["extra"] = "x"
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(_encode(spec))

    def test_rejects_missing_run_key(self) -> None:
        spec = _valid_spec_dict()
        del spec["run"]["previous_run_id"]
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(_encode(spec))

    def test_rejects_extra_run_key(self) -> None:
        spec = _valid_spec_dict()
        spec["run"]["extra"] = "x"
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(_encode(spec))

    def test_rejects_duplicate_top_level_key(self) -> None:
        text = _valid_spec_text()
        tampered = text.replace('"schema_version":1,', '"schema_version":1,"schema_version":1,', 1)
        self.assertNotEqual(text, tampered)
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(tampered.encode("utf-8"))

    def test_rejects_duplicate_manifest_request_key(self) -> None:
        text = _valid_spec_text()
        needle = f'"bucket":"{_BUCKET}"'
        tampered = text.replace(needle, needle + "," + needle, 1)
        self.assertNotEqual(text, tampered)
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(tampered.encode("utf-8"))

    def test_rejects_duplicate_trusted_binding_key(self) -> None:
        text = _valid_spec_text()
        needle = f'"expected_manifest_sha256":"{_SHA256_HEX}"'
        tampered = text.replace(needle, needle + "," + needle, 1)
        self.assertNotEqual(text, tampered)
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(tampered.encode("utf-8"))

    def test_rejects_duplicate_run_key(self) -> None:
        text = _valid_spec_text()
        needle = f'"calendar_materialization_id":"{_CALENDAR_ID}"'
        tampered = text.replace(needle, needle + "," + needle, 1)
        self.assertNotEqual(text, tampered)
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(tampered.encode("utf-8"))

    def test_rejects_bool_schema_version(self) -> None:
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(_encode(_valid_spec_dict(schema_version=True)))

    def test_rejects_wrong_schema_version_value(self) -> None:
        for bad in (0, 2, "1"):
            with self.assertRaises(PinnedGCSRunSpecError):
                parse_pinned_gcs_run_spec(_encode(_valid_spec_dict(schema_version=bad)))

    def test_rejects_float_schema_version(self) -> None:
        text = '{"schema_version":1.0,"manifest_request":{},"trusted_binding":{},"run":{}}'
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(text.encode("utf-8"))

    def test_rejects_floats_nan_infinity(self) -> None:
        for token in ("1.5", "NaN", "Infinity", "-Infinity"):
            text = f'{{"schema_version": 1, "extra": {token}}}'
            with self.assertRaises(PinnedGCSRunSpecError):
                parse_pinned_gcs_run_spec(text.encode("utf-8"))

    def test_rejects_overlong_integer_token(self) -> None:
        huge_digits = "9" * 30
        text = f'{{"schema_version": 1, "extra": {huge_digits}}}'
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(text.encode("utf-8"))


class RequestBindingRejectionTests(unittest.TestCase):
    def test_rejects_wrong_type_manifest_request(self) -> None:
        spec = _valid_spec_dict()
        spec["manifest_request"] = "not-an-object"
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(_encode(spec))

    def test_rejects_wrong_type_trusted_binding(self) -> None:
        spec = _valid_spec_dict()
        spec["trusted_binding"] = []
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(_encode(spec))

    def test_rejects_bucket_mismatch(self) -> None:
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(
                _valid_spec_bytes(bucket=_BUCKET, allowed_bucket="another-syntactically-valid-bucket")
            )

    def test_rejects_invalid_bucket(self) -> None:
        for bad_bucket in ("Bad_Bucket!", "a"):
            with self.assertRaises(PinnedGCSRunSpecError):
                parse_pinned_gcs_run_spec(
                    _valid_spec_bytes(bucket=bad_bucket, allowed_bucket=bad_bucket)
                )

    def test_rejects_canonical_path_mismatch(self) -> None:
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(
                _valid_spec_bytes(object_name=f"landing/{_SESSION.isoformat()}/wrong-name.json")
            )

    def test_rejects_wrong_session_path(self) -> None:
        other_session = date(2026, 7, 21)
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(
                _valid_spec_bytes(object_name=_manifest_object_name(other_session))
            )

    def test_rejects_path_traversal(self) -> None:
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(
                _valid_spec_bytes(object_name="landing/../secrets/landing-manifest.json")
            )

    def test_rejects_bool_generation(self) -> None:
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(_valid_spec_bytes(generation=True))

    def test_rejects_zero_generation(self) -> None:
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(_valid_spec_bytes(generation=0))

    def test_rejects_negative_generation(self) -> None:
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(_valid_spec_bytes(generation=-5))

    def test_rejects_string_generation(self) -> None:
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(_valid_spec_bytes(generation="777"))

    def test_rejects_float_generation(self) -> None:
        text = _valid_spec_text().replace('"generation":777', '"generation":777.0', 1)
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(text.encode("utf-8"))

    def test_rejects_generation_signed_int64_overflow(self) -> None:
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(_valid_spec_bytes(generation=9223372036854775808))

    def test_accepts_generation_at_int64_max(self) -> None:
        result = parse_pinned_gcs_run_spec(_valid_spec_bytes(generation=9223372036854775807))
        self.assertEqual(result.manifest_request.generation, 9223372036854775807)

    def test_rejects_malformed_hash(self) -> None:
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(_valid_spec_bytes(expected_manifest_sha256="not-a-hash"))

    def test_rejects_uppercase_hash(self) -> None:
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(_valid_spec_bytes(expected_manifest_sha256=_SHA256_HEX.upper()))

    def test_rejects_manifest_and_binding_session_mismatch(self) -> None:
        other = date(2026, 7, 21)
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(
                _valid_spec_bytes(manifest_session=other, object_name=_manifest_object_name(other))
            )

    def test_rejects_manifest_and_run_session_mismatch(self) -> None:
        other = date(2026, 7, 21)
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(_valid_spec_bytes(run_session=other))

    def test_rejects_binding_and_run_session_mismatch(self) -> None:
        other = date(2026, 7, 21)
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(_valid_spec_bytes(binding_session=other))

    def test_rejects_malformed_not_before(self) -> None:
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(_valid_spec_bytes(not_before="not-a-datetime"))

    def test_rejects_noncanonical_not_before_missing_seconds(self) -> None:
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(_valid_spec_bytes(not_before="2026-07-20T00:00Z"))

    def test_rejects_naive_not_before(self) -> None:
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(_valid_spec_bytes(not_before="2026-07-20T00:00:00"))

    def test_rejects_malformed_run_cutoff(self) -> None:
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(_valid_spec_bytes(run_cutoff="not-a-datetime"))

    def test_rejects_naive_run_cutoff(self) -> None:
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(_valid_spec_bytes(run_cutoff="2026-07-20T15:00:00"))

    def test_rejects_not_before_after_binding_cutoff(self) -> None:
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(_valid_spec_bytes(not_before="2026-07-20T23:59:59Z"))

    def test_rejects_binding_cutoff_after_run_cutoff(self) -> None:
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(_valid_spec_bytes(binding_cutoff="2026-07-20T23:59:59Z"))

    def test_accepts_binding_cutoff_equal_to_run_cutoff(self) -> None:
        result = parse_pinned_gcs_run_spec(_valid_spec_bytes(binding_cutoff=_RUN_CUTOFF))
        self.assertEqual(result.trusted_binding.cutoff, result.cutoff)


class RunFieldRejectionTests(unittest.TestCase):
    def test_rejects_malformed_calendar_id(self) -> None:
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(_valid_spec_bytes(calendar_materialization_id="not-a-hash"))

    def test_rejects_uppercase_calendar_id(self) -> None:
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(
                _valid_spec_bytes(calendar_materialization_id=_CALENDAR_ID.upper())
            )

    def test_rejects_missing_previous_run_id_key(self) -> None:
        spec = _valid_spec_dict()
        del spec["run"]["previous_run_id"]
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(_encode(spec))

    def test_rejects_wrong_type_previous_run_id(self) -> None:
        for bad in (123, True):
            with self.assertRaises(PinnedGCSRunSpecError):
                parse_pinned_gcs_run_spec(_valid_spec_bytes(previous_run_id=bad))

    def test_rejects_malformed_previous_run_id(self) -> None:
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(_valid_spec_bytes(previous_run_id="not-a-hash"))

    def test_rejects_uppercase_previous_run_id(self) -> None:
        with self.assertRaises(PinnedGCSRunSpecError):
            parse_pinned_gcs_run_spec(_valid_spec_bytes(previous_run_id=_PREVIOUS_RUN_ID.upper()))

    def test_null_previous_run_id_is_accepted_and_not_defaulted_or_discovered(self) -> None:
        result = parse_pinned_gcs_run_spec(_valid_spec_bytes(previous_run_id=None))
        self.assertIsNone(result.previous_run_id)


class PinnedGCSRunSpecDirectConstructionTests(unittest.TestCase):
    def test_valid_construction_succeeds(self) -> None:
        spec = PinnedGCSRunSpec(**_valid_spec_kwargs())
        self.assertEqual(spec.market_session, _SESSION)

    def test_rejects_wrong_type_manifest_request(self) -> None:
        with self.assertRaises(PinnedGCSRunSpecError):
            PinnedGCSRunSpec(**_valid_spec_kwargs(manifest_request="not-a-request"))

    def test_rejects_wrong_type_trusted_binding(self) -> None:
        with self.assertRaises(PinnedGCSRunSpecError):
            PinnedGCSRunSpec(**_valid_spec_kwargs(trusted_binding="not-a-binding"))

    def test_rejects_shaped_request_proxy_with_poisoned_equality(self) -> None:
        real = _valid_request()

        class _ShapedRequestProxy:
            def __init__(self) -> None:
                self.bucket = real.bucket
                self.object_name = real.object_name
                self.generation = real.generation
                self.target_session = real.target_session

            def __eq__(self, other: object) -> bool:
                return True

            def __hash__(self) -> int:
                return 0

        with self.assertRaises(PinnedGCSRunSpecError):
            PinnedGCSRunSpec(**_valid_spec_kwargs(manifest_request=_ShapedRequestProxy()))

    def test_rejects_shaped_binding_proxy_with_poisoned_equality(self) -> None:
        real = _valid_binding()

        class _ShapedBindingProxy:
            def __init__(self) -> None:
                self.expected_manifest_sha256 = real.expected_manifest_sha256
                self.allowed_bucket = real.allowed_bucket
                self.target_session = real.target_session
                self.not_before = real.not_before
                self.cutoff = real.cutoff

            def __eq__(self, other: object) -> bool:
                return True

            def __hash__(self) -> int:
                return 0

        with self.assertRaises(PinnedGCSRunSpecError):
            PinnedGCSRunSpec(**_valid_spec_kwargs(trusted_binding=_ShapedBindingProxy()))

    def test_rejects_request_subclass(self) -> None:
        class _RequestSubclass(LandingManifestObjectRequest):
            pass

        subclass_instance = _RequestSubclass(
            bucket=_BUCKET,
            object_name=_manifest_object_name(),
            generation=777,
            target_session=_SESSION,
        )
        with self.assertRaises(PinnedGCSRunSpecError):
            PinnedGCSRunSpec(**_valid_spec_kwargs(manifest_request=subclass_instance))

    def test_rejects_binding_subclass(self) -> None:
        class _BindingSubclass(TrustedLandingManifestBinding):
            pass

        subclass_instance = _BindingSubclass(
            expected_manifest_sha256=_SHA256_HEX,
            allowed_bucket=_BUCKET,
            target_session=_SESSION,
            not_before=datetime(2026, 7, 20, 0, 0, 0, tzinfo=timezone.utc),
            cutoff=datetime(2026, 7, 20, 14, 0, 0, tzinfo=timezone.utc),
        )
        with self.assertRaises(PinnedGCSRunSpecError):
            PinnedGCSRunSpec(**_valid_spec_kwargs(trusted_binding=subclass_instance))

    def test_rejects_post_construction_mutated_request_field(self) -> None:
        request = _valid_request()
        object.__setattr__(request, "bucket", "another-syntactically-valid-bucket")
        with self.assertRaises(PinnedGCSRunSpecError):
            PinnedGCSRunSpec(**_valid_spec_kwargs(manifest_request=request))

    def test_rejects_post_construction_mutated_binding_field(self) -> None:
        binding = _valid_binding()
        object.__setattr__(binding, "allowed_bucket", "another-syntactically-valid-bucket")
        with self.assertRaises(PinnedGCSRunSpecError):
            PinnedGCSRunSpec(**_valid_spec_kwargs(trusted_binding=binding))

    def test_defensive_snapshot_identity(self) -> None:
        request = _valid_request()
        binding = _valid_binding()

        spec = PinnedGCSRunSpec(
            **_valid_spec_kwargs(manifest_request=request, trusted_binding=binding)
        )

        self.assertIsNot(spec.manifest_request, request)
        self.assertIsNot(spec.trusted_binding, binding)
        self.assertEqual(spec.manifest_request, request)
        self.assertEqual(spec.trusted_binding, binding)

        object.__setattr__(request, "bucket", "another-syntactically-valid-bucket")
        object.__setattr__(binding, "allowed_bucket", "another-syntactically-valid-bucket")
        self.assertEqual(spec.manifest_request.bucket, _BUCKET)
        self.assertEqual(spec.trusted_binding.allowed_bucket, _BUCKET)

    def test_direct_construction_failure_is_static_and_sanitized(self) -> None:
        with self.assertRaises(PinnedGCSRunSpecError) as ctx:
            PinnedGCSRunSpec(**_valid_spec_kwargs(schema_version=2))
        self.assertEqual(
            str(ctx.exception), "pinned gcs run spec schema version is unsupported"
        )

    def test_non_utc_binding_datetimes_normalize_on_direct_construction(self) -> None:
        ist = timezone(timedelta(hours=5, minutes=30))
        binding = TrustedLandingManifestBinding(
            expected_manifest_sha256=_SHA256_HEX,
            allowed_bucket=_BUCKET,
            target_session=_SESSION,
            not_before=datetime(2026, 7, 20, 5, 30, 0, tzinfo=ist),
            cutoff=datetime(2026, 7, 20, 19, 30, 0, tzinfo=ist),
        )

        spec = PinnedGCSRunSpec(**_valid_spec_kwargs(trusted_binding=binding))

        self.assertEqual(spec.trusted_binding.not_before, datetime(2026, 7, 20, 0, 0, 0, tzinfo=timezone.utc))
        self.assertEqual(spec.trusted_binding.cutoff, datetime(2026, 7, 20, 14, 0, 0, tzinfo=timezone.utc))

    def test_rejects_str_subclass_calendar_materialization_id(self) -> None:
        with self.assertRaises(PinnedGCSRunSpecError):
            PinnedGCSRunSpec(
                **_valid_spec_kwargs(calendar_materialization_id=_StrSubclass(_CALENDAR_ID))
            )

    def test_rejects_str_subclass_previous_run_id(self) -> None:
        with self.assertRaises(PinnedGCSRunSpecError):
            PinnedGCSRunSpec(
                **_valid_spec_kwargs(previous_run_id=_StrSubclass(_PREVIOUS_RUN_ID))
            )

    def test_rejects_str_subclass_request_bucket(self) -> None:
        request = _valid_request()
        object.__setattr__(request, "bucket", _StrSubclass(_BUCKET))
        with self.assertRaises(PinnedGCSRunSpecError):
            PinnedGCSRunSpec(**_valid_spec_kwargs(manifest_request=request))

    def test_rejects_str_subclass_request_object_name(self) -> None:
        request = _valid_request()
        object.__setattr__(request, "object_name", _StrSubclass(_manifest_object_name()))
        with self.assertRaises(PinnedGCSRunSpecError):
            PinnedGCSRunSpec(**_valid_spec_kwargs(manifest_request=request))

    def test_rejects_str_subclass_binding_expected_manifest_sha256(self) -> None:
        binding = _valid_binding()
        object.__setattr__(binding, "expected_manifest_sha256", _StrSubclass(_SHA256_HEX))
        with self.assertRaises(PinnedGCSRunSpecError):
            PinnedGCSRunSpec(**_valid_spec_kwargs(trusted_binding=binding))

    def test_rejects_str_subclass_binding_allowed_bucket(self) -> None:
        binding = _valid_binding()
        object.__setattr__(binding, "allowed_bucket", _StrSubclass(_BUCKET))
        with self.assertRaises(PinnedGCSRunSpecError):
            PinnedGCSRunSpec(**_valid_spec_kwargs(trusted_binding=binding))

    def test_raising_tzinfo_on_binding_not_before_never_leaks_secret(self) -> None:
        secret = "SECRET-NOT-BEFORE-TZINFO-RUNTIME-ERROR-DO-NOT-LEAK-8e2c"
        binding = _valid_binding()
        object.__setattr__(
            binding,
            "not_before",
            datetime(2026, 7, 20, 0, 0, 0, tzinfo=_RaisingTzInfo(secret)),
        )
        with self.assertRaises(PinnedGCSRunSpecError) as ctx:
            PinnedGCSRunSpec(**_valid_spec_kwargs(trusted_binding=binding))
        self.assertNotIn(secret, str(ctx.exception))
        self.assertNotIn("RuntimeError", str(ctx.exception))

    def test_raising_tzinfo_on_binding_cutoff_never_leaks_secret(self) -> None:
        secret = "SECRET-BINDING-CUTOFF-TZINFO-RUNTIME-ERROR-DO-NOT-LEAK-5a1d"
        binding = _valid_binding()
        object.__setattr__(
            binding,
            "cutoff",
            datetime(2026, 7, 20, 14, 0, 0, tzinfo=_RaisingTzInfo(secret)),
        )
        with self.assertRaises(PinnedGCSRunSpecError) as ctx:
            PinnedGCSRunSpec(**_valid_spec_kwargs(trusted_binding=binding))
        self.assertNotIn(secret, str(ctx.exception))
        self.assertNotIn("RuntimeError", str(ctx.exception))

    def test_raising_tzinfo_on_run_cutoff_never_leaks_secret(self) -> None:
        secret = "SECRET-RUN-CUTOFF-TZINFO-RUNTIME-ERROR-DO-NOT-LEAK-3f9d"
        malicious_cutoff = datetime(2026, 7, 20, 15, 0, 0, tzinfo=_RaisingTzInfo(secret))
        with self.assertRaises(PinnedGCSRunSpecError) as ctx:
            PinnedGCSRunSpec(**_valid_spec_kwargs(cutoff=malicious_cutoff))
        self.assertNotIn(secret, str(ctx.exception))
        self.assertNotIn("RuntimeError", str(ctx.exception))


class SanitizationTests(unittest.TestCase):
    def test_secret_bucket_never_leaks(self) -> None:
        secret = "secret-bucket-do-not-leak-9f2a"
        with self.assertRaises(PinnedGCSRunSpecError) as ctx:
            parse_pinned_gcs_run_spec(_valid_spec_bytes(bucket=secret, allowed_bucket=_BUCKET))
        self.assertNotIn(secret, str(ctx.exception))

    def test_secret_path_never_leaks(self) -> None:
        secret = "SECRET-PATH-DO-NOT-LEAK-3d4e"
        with self.assertRaises(PinnedGCSRunSpecError) as ctx:
            parse_pinned_gcs_run_spec(
                _valid_spec_bytes(object_name=f"landing/{secret}/traversal")
            )
        self.assertNotIn(secret, str(ctx.exception))

    def test_secret_hash_never_leaks(self) -> None:
        secret = "SECRET-HASH-VALUE-DO-NOT-LEAK-7c1b"
        with self.assertRaises(PinnedGCSRunSpecError) as ctx:
            parse_pinned_gcs_run_spec(_valid_spec_bytes(expected_manifest_sha256=secret))
        self.assertNotIn(secret, str(ctx.exception))

    def test_secret_date_never_leaks(self) -> None:
        secret = "SECRET-DATE-DO-NOT-LEAK"
        with self.assertRaises(PinnedGCSRunSpecError) as ctx:
            parse_pinned_gcs_run_spec(_valid_spec_bytes(manifest_session=secret))
        self.assertNotIn(secret, str(ctx.exception))

    def test_secret_id_never_leaks(self) -> None:
        secret = "SECRET-CALENDAR-ID-DO-NOT-LEAK-2b9f"
        with self.assertRaises(PinnedGCSRunSpecError) as ctx:
            parse_pinned_gcs_run_spec(_valid_spec_bytes(calendar_materialization_id=secret))
        self.assertNotIn(secret, str(ctx.exception))

    def test_nested_exception_like_secret_never_leaks(self) -> None:
        secret = "SECRET-NESTED-EXCEPTION-TEXT-DO-NOT-LEAK-4f8a"
        text = f'{{"schema_version": 1, "leak": "{secret}"'
        with self.assertRaises(PinnedGCSRunSpecError) as ctx:
            parse_pinned_gcs_run_spec(text.encode("utf-8"))
        self.assertNotIn(secret, str(ctx.exception))


_EXACT_ALLOWED_SPEC_IMPORTS = frozenset((
    # (level, module, imported name, asname). Closed set: any import in
    # pinned_gcs_run_spec.py not exactly in this set, and any entry here
    # missing from that file, fails the equality assertion below.
    (0, "__future__", "annotations", None),
    (0, "json", None, None),
    (0, "re", None, None),
    (0, "dataclasses", "dataclass", None),
    (0, "datetime", "date", None),
    (0, "datetime", "datetime", None),
    (0, "datetime", "timezone", None),
    (1, "acquisition", "AcquisitionError", None),
    (1, "acquisition", "LandingManifestObjectRequest", None),
    (1, "landing_manifest", "LandingManifestError", None),
    (1, "landing_manifest", "TrustedLandingManifestBinding", None),
))

_EXACT_ALLOWED_SPEC_CALL_TARGETS = frozenset((
    # The production module's entire callable surface: building the
    # module-level frozenset/regex constants; raising its own error type
    # (plus a plain ValueError used purely as an internal, immediately
    # caught-and-sanitized signal inside one try block); the
    # type()/len()/int()/set() building blocks used for strict inline
    # exact-type validation; str.fullmatch/endswith/decode and
    # date/datetime.fromisoformat/astimezone for canonical parsing;
    # json.loads for strict decoding; object.__setattr__ for the
    # post-validation UTC-normalization and snapshot-replacement writes;
    # constructing/reconstructing the two existing canonical value types;
    # and the module's own two private parsing helpers. Any other call
    # name -- a GCS/storage client, os/pathlib, requests/urllib,
    # subprocess, a broker/order/notification helper, a strategy/model/LLM
    # call, a listing/"latest" helper, or a retry/fallback wrapper --
    # fails this test.
    "dataclass",
    "frozenset",
    "compile",
    "PinnedGCSRunSpecError",
    "ValueError",
    "len",
    "int",
    "fullmatch",
    "fromisoformat",
    "endswith",
    "astimezone",
    "utcoffset",
    "type",
    "LandingManifestObjectRequest",
    "TrustedLandingManifestBinding",
    "__setattr__",
    "decode",
    "loads",
    "set",
    "_parse_canonical_date",
    "_parse_rfc3339_utc",
    "PinnedGCSRunSpec",
))

_FORBIDDEN_SPEC_NAME_TOKENS = (
    # "llm" is deliberately excluded: it is an unavoidable substring of the
    # standard library's own re.Pattern.fullmatch (fu-LLM-atch), which this
    # module legitimately calls many times; excluding it here does not
    # weaken anything, since the exact import allowlist above already
    # proves no strategy/model/LLM-shaped dependency can be imported at
    # all.
    "path",
    "open",
    "filesystem",
    "environ",
    "getenv",
    "now",
    "utcnow",
    "today",
    "google",
    "storage",
    "client",
    "list",
    "latest",
    "retry",
    "fallback",
    "subprocess",
    "notify",
    "notification",
    "broker",
    "order",
    "strategy",
    "model",
    "scheduler",
    "deploy",
)


class PinnedGCSRunSpecCapabilityTests(unittest.TestCase):
    """Proves pinned_gcs_run_spec.py introduces no filesystem, environment,
    current-clock, GCS/storage/client, listing/latest selection,
    retry/fallback, subprocess, notification, broker/order,
    strategy/model/LLM, scheduler, or deployment capability. Imports and
    the callable surface are both locked to exact closed sets.
    """

    def _module_ast(self) -> ast.Module:
        source = (
            Path(__file__).resolve().parent.parent
            / "src"
            / "india_swing"
            / "daily_pipeline"
            / "pinned_gcs_run_spec.py"
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
        self.assertEqual(actual, _EXACT_ALLOWED_SPEC_IMPORTS)

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
            if target not in _EXACT_ALLOWED_SPEC_CALL_TARGETS:
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
            for token in _FORBIDDEN_SPEC_NAME_TOKENS:
                if token in candidate:
                    offenders.append(candidate)
        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
