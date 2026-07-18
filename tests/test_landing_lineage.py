from __future__ import annotations

import dataclasses
import hashlib
import json
import unittest
from datetime import date, datetime, timedelta, timezone

from india_swing.daily_pipeline.acquisition import AcquiredFile, AcquisitionFileType, LandingObjectRequest
from india_swing.daily_pipeline.landing_inputs import VerifiedLandingInputs
from india_swing.daily_pipeline.landing_lineage import (
    LANDING_INPUT_LINEAGE_SCHEMA_VERSION,
    LandingInputLineage,
    LandingLineageError,
    LandingObjectLineage,
    build_landing_input_lineage,
)
from india_swing.daily_pipeline.landing_manifest import (
    LandingManifestVerifier,
    TrustedLandingManifestBinding,
    VerifiedLandingManifest,
)

_TARGET_SESSION = date(2026, 7, 16)
_BUCKET = "trusted-bucket"
_NOT_BEFORE = datetime(2026, 7, 16, 0, 0, 0, tzinfo=timezone.utc)
_CUTOFF = datetime(2026, 7, 16, 23, 59, 59, tzinfo=timezone.utc)
_KNOWLEDGE_TIME = "2026-07-16T13:30:00Z"
_SM_SHA256 = hashlib.sha256(b"security-master-content").hexdigest()
_DB_SHA256 = hashlib.sha256(b"daily-bundle-content").hexdigest()


def _sm_object_name(target_session: date = _TARGET_SESSION) -> str:
    return f"landing/{target_session.isoformat()}/NSE_CM_security_{target_session.strftime('%d%m%Y')}.csv.gz"


def _db_object_name(target_session: date = _TARGET_SESSION) -> str:
    return f"landing/{target_session.isoformat()}/Reports-Daily-Multiple.zip"


def _manifest_dict(
    *, target_session: date = _TARGET_SESSION, knowledge_time: str = _KNOWLEDGE_TIME, bucket: str = _BUCKET
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "knowledge_time": knowledge_time,
        "target_session": target_session.isoformat(),
        "objects": [
            {
                "file_type": "SECURITY_MASTER",
                "bucket": bucket,
                "object_name": _sm_object_name(target_session),
                "generation": 123,
                "sha256": _SM_SHA256,
            },
            {
                "file_type": "DAILY_BUNDLE",
                "bucket": bucket,
                "object_name": _db_object_name(target_session),
                "generation": 456,
                "sha256": _DB_SHA256,
            },
        ],
    }


def _verified_manifest(
    *,
    target_session: date = _TARGET_SESSION,
    knowledge_time: str = _KNOWLEDGE_TIME,
    not_before: datetime = _NOT_BEFORE,
    cutoff: datetime = _CUTOFF,
) -> VerifiedLandingManifest:
    manifest_bytes = json.dumps(
        _manifest_dict(target_session=target_session, knowledge_time=knowledge_time),
        separators=(",", ":"),
    ).encode("utf-8")
    binding = TrustedLandingManifestBinding(
        expected_manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        allowed_bucket=_BUCKET,
        target_session=target_session,
        not_before=not_before,
        cutoff=cutoff,
    )
    return LandingManifestVerifier().verify(manifest_bytes, binding)


def _acquired_for(request: LandingObjectRequest, *, content_bytes: bytes) -> AcquiredFile:
    return AcquiredFile(
        bucket=request.bucket,
        object_name=request.object_name,
        generation=request.generation,
        target_session=request.target_session,
        file_type=request.file_type,
        content_bytes=content_bytes,
        sha256_hash=hashlib.sha256(content_bytes).hexdigest(),
    )


def _valid_inputs(
    *, manifest: VerifiedLandingManifest | None = None, run_cutoff: datetime = _CUTOFF
) -> VerifiedLandingInputs:
    manifest = manifest if manifest is not None else _verified_manifest()
    sm_acquired = _acquired_for(manifest.security_master, content_bytes=b"security-master-content")
    db_acquired = _acquired_for(manifest.daily_bundle, content_bytes=b"daily-bundle-content")
    return VerifiedLandingInputs(
        manifest=manifest,
        market_session=_TARGET_SESSION,
        run_cutoff=run_cutoff,
        security_master=sm_acquired,
        daily_bundle=db_acquired,
    )


def _valid_object_lineage(file_type: AcquisitionFileType = AcquisitionFileType.SECURITY_MASTER) -> LandingObjectLineage:
    if file_type is AcquisitionFileType.SECURITY_MASTER:
        return LandingObjectLineage(
            file_type=AcquisitionFileType.SECURITY_MASTER,
            bucket=_BUCKET,
            object_name=_sm_object_name(),
            generation=123,
            target_session=_TARGET_SESSION,
            sha256_hash=_SM_SHA256,
        )
    return LandingObjectLineage(
        file_type=AcquisitionFileType.DAILY_BUNDLE,
        bucket=_BUCKET,
        object_name=_db_object_name(),
        generation=456,
        target_session=_TARGET_SESSION,
        sha256_hash=_DB_SHA256,
    )


class LandingLineageAcceptanceTests(unittest.TestCase):
    def test_valid_inputs_project_every_field_and_produce_deterministic_lineage_id(self) -> None:
        inputs = _valid_inputs()

        lineage = build_landing_input_lineage(inputs)

        self.assertIsInstance(lineage, LandingInputLineage)
        self.assertEqual(lineage.schema_version, LANDING_INPUT_LINEAGE_SCHEMA_VERSION)
        self.assertEqual(lineage.manifest_sha256, inputs.manifest.manifest_sha256)
        self.assertEqual(lineage.manifest_knowledge_time, inputs.manifest.knowledge_time)
        self.assertEqual(lineage.binding_not_before, inputs.manifest.binding.not_before)
        self.assertEqual(lineage.binding_cutoff, inputs.manifest.binding.cutoff)
        self.assertEqual(lineage.target_session, _TARGET_SESSION)
        self.assertEqual(lineage.security_master.file_type, AcquisitionFileType.SECURITY_MASTER)
        self.assertEqual(lineage.security_master.bucket, inputs.security_master.bucket)
        self.assertEqual(lineage.security_master.object_name, inputs.security_master.object_name)
        self.assertEqual(lineage.security_master.generation, inputs.security_master.generation)
        self.assertEqual(lineage.security_master.sha256_hash, inputs.security_master.sha256_hash)
        self.assertEqual(lineage.daily_bundle.file_type, AcquisitionFileType.DAILY_BUNDLE)
        self.assertEqual(lineage.daily_bundle.bucket, inputs.daily_bundle.bucket)
        self.assertRegex(lineage.lineage_id, r"\A[0-9a-f]{64}\Z")

        again = build_landing_input_lineage(inputs)
        self.assertEqual(lineage.lineage_id, again.lineage_id)

    def test_equivalent_timezone_representations_produce_same_lineage_id(self) -> None:
        cutoff_utc = datetime(2026, 7, 16, 18, 0, 0, tzinfo=timezone.utc)
        cutoff_ist = cutoff_utc.astimezone(timezone(timedelta(hours=5, minutes=30)))
        self.assertNotEqual(cutoff_utc.tzinfo, cutoff_ist.tzinfo)
        self.assertEqual(cutoff_utc, cutoff_ist)

        manifest = _verified_manifest(cutoff=cutoff_utc)
        inputs_utc = _valid_inputs(manifest=manifest, run_cutoff=cutoff_utc)
        inputs_ist = _valid_inputs(manifest=manifest, run_cutoff=cutoff_ist)

        lineage_utc = build_landing_input_lineage(inputs_utc)
        lineage_ist = build_landing_input_lineage(inputs_ist)

        self.assertEqual(lineage_utc.lineage_id, lineage_ist.lineage_id)
        self.assertEqual(lineage_utc.binding_cutoff, lineage_ist.binding_cutoff)
        self.assertEqual(lineage_utc.binding_cutoff.tzinfo, timezone.utc)

    def test_no_raw_bytes_or_reader_capability_retained(self) -> None:
        inputs = _valid_inputs()
        lineage = build_landing_input_lineage(inputs)

        lineage_field_names = {f.name for f in dataclasses.fields(lineage)}
        object_field_names = {f.name for f in dataclasses.fields(lineage.security_master)}
        self.assertNotIn("manifest_bytes", lineage_field_names)
        self.assertNotIn("content_bytes", object_field_names)
        self.assertNotIn("reader", lineage_field_names)
        for name in lineage_field_names:
            self.assertNotIsInstance(getattr(lineage, name), bytes)
        for name in object_field_names:
            self.assertNotIsInstance(getattr(lineage.security_master, name), bytes)


class LandingObjectLineageDirectConstructionTests(unittest.TestCase):
    def test_valid_construction_succeeds(self) -> None:
        lineage = _valid_object_lineage()
        self.assertEqual(lineage.generation, 123)

    def test_rejects_bool_generation(self) -> None:
        with self.assertRaises(LandingLineageError):
            LandingObjectLineage(
                file_type=AcquisitionFileType.SECURITY_MASTER,
                bucket=_BUCKET,
                object_name=_sm_object_name(),
                generation=True,
                target_session=_TARGET_SESSION,
                sha256_hash=_SM_SHA256,
            )

    def test_rejects_nonpositive_generation(self) -> None:
        for bad_generation in (0, -1):
            with self.assertRaises(LandingLineageError):
                LandingObjectLineage(
                    file_type=AcquisitionFileType.SECURITY_MASTER,
                    bucket=_BUCKET,
                    object_name=_sm_object_name(),
                    generation=bad_generation,
                    target_session=_TARGET_SESSION,
                    sha256_hash=_SM_SHA256,
                )

    def test_rejects_generation_above_int64_max(self) -> None:
        with self.assertRaises(LandingLineageError):
            LandingObjectLineage(
                file_type=AcquisitionFileType.SECURITY_MASTER,
                bucket=_BUCKET,
                object_name=_sm_object_name(),
                generation=9223372036854775808,
                target_session=_TARGET_SESSION,
                sha256_hash=_SM_SHA256,
            )

    def test_accepts_generation_at_int64_max(self) -> None:
        lineage = LandingObjectLineage(
            file_type=AcquisitionFileType.SECURITY_MASTER,
            bucket=_BUCKET,
            object_name=_sm_object_name(),
            generation=9223372036854775807,
            target_session=_TARGET_SESSION,
            sha256_hash=_SM_SHA256,
        )
        self.assertEqual(lineage.generation, 9223372036854775807)

    def test_rejects_noncanonical_object_path(self) -> None:
        for bad_object_name in (
            "landing/2026-07-16/NSE_CM_security_17072026.csv.gz",  # wrong date
            "landing/2026-07-17/NSE_CM_security_16072026.csv.gz",  # wrong session prefix
            "landing/../secrets/NSE_CM_security_16072026.csv.gz",  # traversal
        ):
            with self.assertRaises(LandingLineageError):
                LandingObjectLineage(
                    file_type=AcquisitionFileType.SECURITY_MASTER,
                    bucket=_BUCKET,
                    object_name=bad_object_name,
                    generation=123,
                    target_session=_TARGET_SESSION,
                    sha256_hash=_SM_SHA256,
                )

    def test_rejects_renamed_daily_bundle_path(self) -> None:
        with self.assertRaises(LandingLineageError):
            LandingObjectLineage(
                file_type=AcquisitionFileType.DAILY_BUNDLE,
                bucket=_BUCKET,
                object_name="landing/2026-07-16/Reports-Daily-Multiple-Renamed.zip",
                generation=456,
                target_session=_TARGET_SESSION,
                sha256_hash=_DB_SHA256,
            )

    def test_rejects_invalid_file_type(self) -> None:
        with self.assertRaises(LandingLineageError):
            LandingObjectLineage(
                file_type="SECURITY_MASTER",  # type: ignore[arg-type]
                bucket=_BUCKET,
                object_name=_sm_object_name(),
                generation=123,
                target_session=_TARGET_SESSION,
                sha256_hash=_SM_SHA256,
            )

    def test_rejects_malformed_sha256(self) -> None:
        for bad_sha in ("not-a-hash", _SM_SHA256.upper(), "0" * 63):
            with self.assertRaises(LandingLineageError):
                LandingObjectLineage(
                    file_type=AcquisitionFileType.SECURITY_MASTER,
                    bucket=_BUCKET,
                    object_name=_sm_object_name(),
                    generation=123,
                    target_session=_TARGET_SESSION,
                    sha256_hash=bad_sha,
                )

    def test_rejects_invalid_bucket(self) -> None:
        with self.assertRaises(LandingLineageError):
            LandingObjectLineage(
                file_type=AcquisitionFileType.SECURITY_MASTER,
                bucket="Bad_Bucket!",
                object_name=_sm_object_name(),
                generation=123,
                target_session=_TARGET_SESSION,
                sha256_hash=_SM_SHA256,
            )

    def test_injected_secret_path_never_appears_in_error(self) -> None:
        secret = "SECRET-PATH-DO-NOT-LEAK-3d4e"
        with self.assertRaises(LandingLineageError) as ctx:
            LandingObjectLineage(
                file_type=AcquisitionFileType.SECURITY_MASTER,
                bucket=_BUCKET,
                object_name=f"landing/{secret}/traversal",
                generation=123,
                target_session=_TARGET_SESSION,
                sha256_hash=_SM_SHA256,
            )
        self.assertNotIn(secret, str(ctx.exception))


class LandingInputLineageDirectConstructionTests(unittest.TestCase):
    def _kwargs(self, **overrides: object) -> dict[str, object]:
        base = dict(
            schema_version=LANDING_INPUT_LINEAGE_SCHEMA_VERSION,
            manifest_sha256=hashlib.sha256(b"manifest").hexdigest(),
            manifest_knowledge_time=datetime(2026, 7, 16, 13, 30, 0, tzinfo=timezone.utc),
            binding_not_before=_NOT_BEFORE,
            binding_cutoff=_CUTOFF,
            target_session=_TARGET_SESSION,
            security_master=_valid_object_lineage(AcquisitionFileType.SECURITY_MASTER),
            daily_bundle=_valid_object_lineage(AcquisitionFileType.DAILY_BUNDLE),
        )
        base.update(overrides)
        return base

    def test_valid_construction_succeeds(self) -> None:
        lineage = LandingInputLineage(**self._kwargs())
        self.assertRegex(lineage.lineage_id, r"\A[0-9a-f]{64}\Z")

    def test_rejects_wrong_schema_version(self) -> None:
        with self.assertRaises(LandingLineageError):
            LandingInputLineage(**self._kwargs(schema_version="nse-cm-landing-input-lineage/v2"))

    def test_rejects_malformed_manifest_hash(self) -> None:
        with self.assertRaises(LandingLineageError):
            LandingInputLineage(**self._kwargs(manifest_sha256="not-a-hash"))

    def test_rejects_naive_times(self) -> None:
        naive = datetime(2026, 7, 16, 13, 30, 0)
        for field_name in ("manifest_knowledge_time", "binding_not_before", "binding_cutoff"):
            with self.assertRaises(LandingLineageError):
                LandingInputLineage(**self._kwargs(**{field_name: naive}))

    def test_rejects_invalid_time_ordering(self) -> None:
        with self.assertRaises(LandingLineageError):
            LandingInputLineage(
                **self._kwargs(
                    manifest_knowledge_time=datetime(2026, 7, 15, 0, 0, 0, tzinfo=timezone.utc)
                )
            )  # before binding_not_before
        with self.assertRaises(LandingLineageError):
            LandingInputLineage(
                **self._kwargs(
                    manifest_knowledge_time=datetime(2026, 7, 17, 0, 0, 0, tzinfo=timezone.utc)
                )
            )  # after binding_cutoff

    def test_rejects_wrong_nested_type(self) -> None:
        with self.assertRaises(LandingLineageError):
            LandingInputLineage(**self._kwargs(security_master="not-a-lineage-object"))
        with self.assertRaises(LandingLineageError):
            LandingInputLineage(**self._kwargs(daily_bundle="not-a-lineage-object"))

    def test_rejects_swapped_file_roles(self) -> None:
        with self.assertRaises(LandingLineageError):
            LandingInputLineage(
                **self._kwargs(
                    security_master=_valid_object_lineage(AcquisitionFileType.DAILY_BUNDLE),
                )
            )
        with self.assertRaises(LandingLineageError):
            LandingInputLineage(
                **self._kwargs(
                    daily_bundle=_valid_object_lineage(AcquisitionFileType.SECURITY_MASTER),
                )
            )

    def test_rejects_mismatched_target_session(self) -> None:
        with self.assertRaises(LandingLineageError):
            LandingInputLineage(**self._kwargs(target_session=date(2026, 7, 17)))

    def test_rejects_non_date_target_session(self) -> None:
        with self.assertRaises(LandingLineageError):
            LandingInputLineage(**self._kwargs(target_session="2026-07-16"))


class LandingLineageBuilderTests(unittest.TestCase):
    def test_rejects_non_exact_inputs_type(self) -> None:
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage("not-inputs")  # type: ignore[arg-type]

    def test_rejects_corrupted_manifest_hash(self) -> None:
        inputs = _valid_inputs()
        object.__setattr__(inputs.manifest, "manifest_sha256", "0" * 64)
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_corrupted_binding_relationship(self) -> None:
        inputs = _valid_inputs()
        object.__setattr__(inputs.manifest, "target_session", date(2026, 7, 17))
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_corrupted_market_session(self) -> None:
        inputs = _valid_inputs()
        object.__setattr__(inputs, "market_session", date(2026, 7, 17))
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_corrupted_acquired_identity_field(self) -> None:
        inputs = _valid_inputs()
        object.__setattr__(inputs.security_master, "bucket", "another-syntactically-valid-bucket")
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_non_bytes_acquired_content(self) -> None:
        inputs = _valid_inputs()
        object.__setattr__(inputs.security_master, "content_bytes", "not-bytes")
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_empty_acquired_content(self) -> None:
        inputs = _valid_inputs()
        object.__setattr__(inputs.security_master, "content_bytes", b"")
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_tampered_content_with_stale_hash(self) -> None:
        inputs = _valid_inputs()
        object.__setattr__(inputs.security_master, "content_bytes", b"tampered-content")
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_self_consistent_but_wrong_hash(self) -> None:
        inputs = _valid_inputs()
        new_content = b"attacker-controlled-but-internally-self-consistent-content"
        object.__setattr__(inputs.security_master, "content_bytes", new_content)
        object.__setattr__(inputs.security_master, "sha256_hash", hashlib.sha256(new_content).hexdigest())
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_valid_inputs_still_build_after_read_only_inspection(self) -> None:
        inputs = _valid_inputs()
        lineage = build_landing_input_lineage(inputs)
        self.assertIsInstance(lineage, LandingInputLineage)


class LandingLineageSanitizationTests(unittest.TestCase):
    def test_injected_secret_bucket_never_appears_in_builder_error(self) -> None:
        inputs = _valid_inputs()
        secret = "secret-bucket-do-not-leak-77aa"
        object.__setattr__(inputs.security_master, "bucket", secret)
        with self.assertRaises(LandingLineageError) as ctx:
            build_landing_input_lineage(inputs)
        self.assertNotIn(secret, str(ctx.exception))

    def test_injected_secret_content_never_appears_in_builder_error(self) -> None:
        inputs = _valid_inputs()
        secret_content = b"SECRET-CONTENT-DO-NOT-LEAK-6f2c"
        object.__setattr__(inputs.security_master, "content_bytes", secret_content)
        with self.assertRaises(LandingLineageError) as ctx:
            build_landing_input_lineage(inputs)
        self.assertNotIn(secret_content.decode(), str(ctx.exception))

    def test_injected_secret_hash_never_appears_in_builder_error(self) -> None:
        inputs = _valid_inputs()
        fake_hash = hashlib.sha256(b"attacker-content").hexdigest()
        object.__setattr__(inputs.security_master, "sha256_hash", fake_hash)
        with self.assertRaises(LandingLineageError) as ctx:
            build_landing_input_lineage(inputs)
        self.assertNotIn(fake_hash, str(ctx.exception))


class LandingLineageBuilderHardeningTests(unittest.TestCase):
    """Regression coverage for revision-8 review findings: object.__setattr__
    corruption must fail closed with a static LandingLineageError before any
    unsafe hashing, attribute access, or comparison -- never a raw TypeError
    or AttributeError -- and binding.allowed_bucket must be authoritative
    for both object requests.
    """

    def test_rejects_manifest_bytes_mutated_to_string(self) -> None:
        # Codex reproduced a raw TypeError here before this hardening.
        inputs = _valid_inputs()
        object.__setattr__(inputs.manifest, "manifest_bytes", "not-bytes")
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_manifest_bytes_mutated_to_non_bytes_non_string(self) -> None:
        inputs = _valid_inputs()
        object.__setattr__(inputs.manifest, "manifest_bytes", None)
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_manifest_bytes_mutated_to_empty(self) -> None:
        inputs = _valid_inputs()
        object.__setattr__(inputs.manifest, "manifest_bytes", b"")
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_binding_replaced_by_plain_object(self) -> None:
        # Codex reproduced a raw AttributeError here before this hardening.
        inputs = _valid_inputs()
        object.__setattr__(inputs.manifest, "binding", object())
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_malformed_binding_expected_hash(self) -> None:
        inputs = _valid_inputs()
        object.__setattr__(inputs.manifest.binding, "expected_manifest_sha256", "not-a-hash")
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_malformed_manifest_sha256(self) -> None:
        inputs = _valid_inputs()
        object.__setattr__(inputs.manifest, "manifest_sha256", "not-a-hash")
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_malformed_binding_allowed_bucket(self) -> None:
        inputs = _valid_inputs()
        object.__setattr__(inputs.manifest.binding, "allowed_bucket", "Bad_Bucket!")
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_valid_but_request_mismatched_allowed_bucket(self) -> None:
        # binding.allowed_bucket is syntactically valid but no longer equals
        # either object request's bucket -- must still be caught.
        inputs = _valid_inputs()
        object.__setattr__(
            inputs.manifest.binding, "allowed_bucket", "another-syntactically-valid-bucket"
        )
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_mutated_schema_version(self) -> None:
        inputs = _valid_inputs()
        object.__setattr__(inputs.manifest, "schema_version", 2)
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_bool_schema_version(self) -> None:
        inputs = _valid_inputs()
        object.__setattr__(inputs.manifest, "schema_version", True)
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_non_date_manifest_target_session(self) -> None:
        inputs = _valid_inputs()
        object.__setattr__(inputs.manifest, "target_session", "2026-07-16")
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_non_date_binding_target_session(self) -> None:
        inputs = _valid_inputs()
        object.__setattr__(inputs.manifest.binding, "target_session", "2026-07-16")
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_non_date_market_session(self) -> None:
        inputs = _valid_inputs()
        object.__setattr__(inputs, "market_session", "2026-07-16")
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_non_utc_manifest_knowledge_time(self) -> None:
        inputs = _valid_inputs()
        non_utc = datetime(2026, 7, 16, 19, 0, 0, tzinfo=timezone(timedelta(hours=5, minutes=30)))
        object.__setattr__(inputs.manifest, "knowledge_time", non_utc)
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_naive_manifest_knowledge_time(self) -> None:
        inputs = _valid_inputs()
        object.__setattr__(inputs.manifest, "knowledge_time", datetime(2026, 7, 16, 13, 30, 0))
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_naive_binding_not_before(self) -> None:
        inputs = _valid_inputs()
        object.__setattr__(inputs.manifest.binding, "not_before", datetime(2026, 7, 16, 0, 0, 0))
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_naive_binding_cutoff(self) -> None:
        inputs = _valid_inputs()
        object.__setattr__(inputs.manifest.binding, "cutoff", datetime(2026, 7, 16, 23, 59, 59))
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_malformed_binding_not_before_type(self) -> None:
        inputs = _valid_inputs()
        object.__setattr__(inputs.manifest.binding, "not_before", "2026-07-16T00:00:00Z")
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_naive_run_cutoff(self) -> None:
        inputs = _valid_inputs()
        object.__setattr__(inputs, "run_cutoff", datetime(2026, 7, 16, 23, 59, 59))
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_non_datetime_run_cutoff(self) -> None:
        inputs = _valid_inputs()
        object.__setattr__(inputs, "run_cutoff", "2026-07-16T23:59:59Z")
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_run_cutoff_before_binding_cutoff(self) -> None:
        inputs = _valid_inputs()
        object.__setattr__(
            inputs, "run_cutoff", datetime(2026, 7, 16, 12, 0, 0, tzinfo=timezone.utc)
        )
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_wrong_type_security_master_request(self) -> None:
        inputs = _valid_inputs()
        object.__setattr__(inputs.manifest, "security_master", "not-a-request")
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_wrong_type_daily_bundle_request(self) -> None:
        inputs = _valid_inputs()
        object.__setattr__(inputs.manifest, "daily_bundle", "not-a-request")
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_swapped_request_roles(self) -> None:
        inputs = _valid_inputs()
        original_daily_bundle = inputs.manifest.daily_bundle
        object.__setattr__(inputs.manifest, "security_master", original_daily_bundle)
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_request_session_mismatch(self) -> None:
        inputs = _valid_inputs()
        other_session = date(2026, 7, 17)
        other_request = LandingObjectRequest(
            bucket=_BUCKET,
            object_name=_sm_object_name(other_session),
            generation=123,
            expected_sha256=_SM_SHA256,
            target_session=other_session,
            file_type=AcquisitionFileType.SECURITY_MASTER,
        )
        object.__setattr__(inputs.manifest, "security_master", other_request)
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_request_bucket_not_matching_binding_allowed_bucket(self) -> None:
        inputs = _valid_inputs()
        other_request = LandingObjectRequest(
            bucket="another-syntactically-valid-bucket",
            object_name=_sm_object_name(),
            generation=123,
            expected_sha256=_SM_SHA256,
            target_session=_TARGET_SESSION,
            file_type=AcquisitionFileType.SECURITY_MASTER,
        )
        object.__setattr__(inputs.manifest, "security_master", other_request)
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_valid_inputs_still_build_after_hardening(self) -> None:
        inputs = _valid_inputs()
        lineage = build_landing_input_lineage(inputs)
        self.assertIsInstance(lineage, LandingInputLineage)


class LandingLineageHardeningSanitizationTests(unittest.TestCase):
    def test_injected_secret_in_binding_allowed_bucket_never_appears_in_error(self) -> None:
        inputs = _valid_inputs()
        secret = "secret-allowed-bucket-do-not-leak-51ff"
        object.__setattr__(inputs.manifest.binding, "allowed_bucket", secret)
        with self.assertRaises(LandingLineageError) as ctx:
            build_landing_input_lineage(inputs)
        self.assertNotIn(secret, str(ctx.exception))

    def test_injected_secret_in_manifest_sha256_never_appears_in_error(self) -> None:
        inputs = _valid_inputs()
        secret = "SECRET-MANIFEST-HASH-DO-NOT-LEAK-22bb"
        object.__setattr__(inputs.manifest, "manifest_sha256", secret)
        with self.assertRaises(LandingLineageError) as ctx:
            build_landing_input_lineage(inputs)
        self.assertNotIn(secret, str(ctx.exception))

    def test_injected_secret_in_binding_expected_hash_never_appears_in_error(self) -> None:
        inputs = _valid_inputs()
        secret = "SECRET-BINDING-HASH-DO-NOT-LEAK-93dd"
        object.__setattr__(inputs.manifest.binding, "expected_manifest_sha256", secret)
        with self.assertRaises(LandingLineageError) as ctx:
            build_landing_input_lineage(inputs)
        self.assertNotIn(secret, str(ctx.exception))


class LandingLineageManifestReverificationTests(unittest.TestCase):
    """Regression coverage for the revision-9 review finding: a mutable
    structured request field (on inputs.manifest) and its matching acquired
    metadata could be changed together, consistently, while manifest_bytes
    and its externally bound SHA-256 still encoded the original value. The
    builder must reparse manifest_bytes against binding as the sole
    object-request authority, so any such coordinated mutation is rejected
    regardless of how internally self-consistent it looks.
    """

    def test_rejects_consistently_mutated_generation_bypass(self) -> None:
        # Codex mutated inputs.manifest.security_master.generation and
        # inputs.security_master.generation together to the same value and
        # the pre-revision-9 builder accepted it.
        inputs = _valid_inputs()
        other_generation = 789
        object.__setattr__(inputs.manifest.security_master, "generation", other_generation)
        object.__setattr__(inputs.security_master, "generation", other_generation)
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_consistently_mutated_daily_bundle_generation_bypass(self) -> None:
        # Daily-bundle counterpart proving the fix is not security-master-specific.
        inputs = _valid_inputs()
        other_generation = 999
        object.__setattr__(inputs.manifest.daily_bundle, "generation", other_generation)
        object.__setattr__(inputs.daily_bundle, "generation", other_generation)
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_consistently_mutated_expected_hash_bypass(self) -> None:
        inputs = _valid_inputs()
        alternative_content = b"attacker-controlled-but-self-consistent-alternative-content"
        alternative_hash = hashlib.sha256(alternative_content).hexdigest()
        object.__setattr__(inputs.manifest.security_master, "expected_sha256", alternative_hash)
        object.__setattr__(inputs.security_master, "content_bytes", alternative_content)
        object.__setattr__(inputs.security_master, "sha256_hash", alternative_hash)
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_rejects_consistently_mutated_bucket_bypass(self) -> None:
        inputs = _valid_inputs()
        other_bucket = "another-syntactically-valid-bucket"
        object.__setattr__(inputs.manifest.binding, "allowed_bucket", other_bucket)
        object.__setattr__(inputs.manifest.security_master, "bucket", other_bucket)
        object.__setattr__(inputs.manifest.daily_bundle, "bucket", other_bucket)
        object.__setattr__(inputs.security_master, "bucket", other_bucket)
        object.__setattr__(inputs.daily_bundle, "bucket", other_bucket)
        with self.assertRaises(LandingLineageError):
            build_landing_input_lineage(inputs)

    def test_valid_inputs_still_build_after_reverification_hardening(self) -> None:
        inputs = _valid_inputs()
        lineage = build_landing_input_lineage(inputs)
        self.assertIsInstance(lineage, LandingInputLineage)


class LandingLineageManifestReverificationSanitizationTests(unittest.TestCase):
    def test_injected_generation_bypass_secret_never_appears_in_error(self) -> None:
        inputs = _valid_inputs()
        secret_generation = 918273645
        object.__setattr__(inputs.manifest.security_master, "generation", secret_generation)
        object.__setattr__(inputs.security_master, "generation", secret_generation)
        with self.assertRaises(LandingLineageError) as ctx:
            build_landing_input_lineage(inputs)
        self.assertNotIn(str(secret_generation), str(ctx.exception))

    def test_injected_hash_and_bytes_bypass_secret_never_appears_in_error(self) -> None:
        inputs = _valid_inputs()
        alternative_content = b"SECRET-ALTERNATIVE-CONTENT-DO-NOT-LEAK-4f1e"
        alternative_hash = hashlib.sha256(alternative_content).hexdigest()
        object.__setattr__(inputs.manifest.security_master, "expected_sha256", alternative_hash)
        object.__setattr__(inputs.security_master, "content_bytes", alternative_content)
        object.__setattr__(inputs.security_master, "sha256_hash", alternative_hash)
        with self.assertRaises(LandingLineageError) as ctx:
            build_landing_input_lineage(inputs)
        message = str(ctx.exception)
        self.assertNotIn(alternative_hash, message)
        self.assertNotIn(alternative_content.decode(), message)

    def test_injected_bucket_bypass_secret_never_appears_in_error(self) -> None:
        inputs = _valid_inputs()
        secret_bucket = "secret-coordinated-bucket-do-not-leak-a9c2"
        object.__setattr__(inputs.manifest.binding, "allowed_bucket", secret_bucket)
        object.__setattr__(inputs.manifest.security_master, "bucket", secret_bucket)
        object.__setattr__(inputs.manifest.daily_bundle, "bucket", secret_bucket)
        object.__setattr__(inputs.security_master, "bucket", secret_bucket)
        object.__setattr__(inputs.daily_bundle, "bucket", secret_bucket)
        with self.assertRaises(LandingLineageError) as ctx:
            build_landing_input_lineage(inputs)
        self.assertNotIn(secret_bucket, str(ctx.exception))


class LandingLineageCapabilityTests(unittest.TestCase):
    def test_no_listing_or_latest_shaped_capability_exists(self) -> None:
        for candidate in (LandingObjectLineage, LandingInputLineage):
            members = [name for name in dir(candidate) if not name.startswith("__")]
            self.assertFalse(
                any("list" in name.lower() or "latest" in name.lower() for name in members),
                f"{candidate!r} unexpectedly exposes a listing/latest-shaped member",
            )


if __name__ == "__main__":
    unittest.main()
