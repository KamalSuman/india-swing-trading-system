from __future__ import annotations

import dataclasses
import hashlib
import json
import unittest
from datetime import date, datetime, timezone

from india_swing.daily_pipeline.acquisition import (
    AcquiredFile,
    AcquisitionFileType,
    LandingManifestObjectRequest,
    LandingObjectRequest,
)
from india_swing.daily_pipeline.landing_inputs import (
    LandingInputError,
    LandingObjectReader,
    VerifiedLandingInputs,
    acquire_verified_landing_inputs,
)
from india_swing.daily_pipeline.landing_manifest import (
    LandingManifestVerifier,
    TrustedLandingManifestBinding,
    VerifiedLandingManifest,
)
from india_swing.daily_pipeline.landing_manifest_acquisition import AcquiredLandingManifest

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
    *,
    target_session: date = _TARGET_SESSION,
    knowledge_time: str = _KNOWLEDGE_TIME,
    bucket: str = _BUCKET,
    sm_sha256: str = _SM_SHA256,
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
                "sha256": sm_sha256,
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
    sm_sha256: str = _SM_SHA256,
) -> VerifiedLandingManifest:
    manifest_bytes = json.dumps(
        _manifest_dict(
            target_session=target_session, knowledge_time=knowledge_time, sm_sha256=sm_sha256
        ),
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


class FakeLandingObjectReader:
    """Fake LandingObjectReader. Never lists/latest; records every call made."""

    def __init__(
        self,
        *,
        responses: dict[AcquisitionFileType, object] | None = None,
        raises: dict[AcquisitionFileType, BaseException] | None = None,
    ) -> None:
        self._responses = responses or {}
        self._raises = raises or {}
        self.calls: list[LandingObjectRequest] = []

    def read(self, request: LandingObjectRequest) -> AcquiredFile:
        self.calls.append(request)
        if request.file_type in self._raises:
            raise self._raises[request.file_type]
        return self._responses[request.file_type]


def _valid_manifest_and_reader() -> tuple[
    VerifiedLandingManifest, FakeLandingObjectReader, AcquiredFile, AcquiredFile
]:
    manifest = _verified_manifest()
    sm_acquired = _acquired_for(manifest.security_master, content_bytes=b"security-master-content")
    db_acquired = _acquired_for(manifest.daily_bundle, content_bytes=b"daily-bundle-content")
    reader = FakeLandingObjectReader(
        responses={
            AcquisitionFileType.SECURITY_MASTER: sm_acquired,
            AcquisitionFileType.DAILY_BUNDLE: db_acquired,
        }
    )
    return manifest, reader, sm_acquired, db_acquired


def _manifest_source_object_name(target_session: date = _TARGET_SESSION) -> str:
    return f"landing/{target_session.isoformat()}/landing-manifest.json"


def _acquired_landing_manifest(
    manifest: VerifiedLandingManifest, *, generation: int = 999
) -> AcquiredLandingManifest:
    request = LandingManifestObjectRequest(
        bucket=manifest.binding.allowed_bucket,
        object_name=_manifest_source_object_name(manifest.target_session),
        generation=generation,
        target_session=manifest.target_session,
    )
    return AcquiredLandingManifest(request=request, manifest=manifest)


class _ShapedBindingImpostor:
    """Carries the same five attribute names as TrustedLandingManifestBinding
    with genuinely valid-looking values, but is not that type and its
    __eq__ always returns True -- simulating an equality-poisoned impostor
    that could fool a defense relying on value comparison instead of an
    exact type check.
    """

    def __init__(
        self, *, expected_manifest_sha256, allowed_bucket, target_session, not_before, cutoff
    ) -> None:
        self.expected_manifest_sha256 = expected_manifest_sha256
        self.allowed_bucket = allowed_bucket
        self.target_session = target_session
        self.not_before = not_before
        self.cutoff = cutoff

    def __eq__(self, other: object) -> bool:
        return True

    def __hash__(self) -> int:
        return 0


def _shaped_binding_impostor_for(manifest: VerifiedLandingManifest) -> _ShapedBindingImpostor:
    real_binding = manifest.binding
    return _ShapedBindingImpostor(
        expected_manifest_sha256=real_binding.expected_manifest_sha256,
        allowed_bucket=real_binding.allowed_bucket,
        target_session=real_binding.target_session,
        not_before=real_binding.not_before,
        cutoff=real_binding.cutoff,
    )


class LandingInputsAcceptanceTests(unittest.TestCase):
    def test_valid_inputs_retain_exact_objects_and_make_two_ordered_reads(self) -> None:
        manifest, reader, sm_acquired, db_acquired = _valid_manifest_and_reader()

        result = acquire_verified_landing_inputs(
            manifest=manifest, market_session=_TARGET_SESSION, run_cutoff=_CUTOFF, reader=reader
        )

        self.assertIsInstance(result, VerifiedLandingInputs)
        self.assertIs(result.manifest, manifest)
        self.assertEqual(result.market_session, _TARGET_SESSION)
        self.assertEqual(result.run_cutoff, _CUTOFF)
        self.assertIs(result.security_master, sm_acquired)
        self.assertIs(result.daily_bundle, db_acquired)
        self.assertEqual(len(reader.calls), 2)
        self.assertIs(reader.calls[0], manifest.security_master)
        self.assertIs(reader.calls[1], manifest.daily_bundle)
        self.assertEqual(reader.calls[0].file_type, AcquisitionFileType.SECURITY_MASTER)
        self.assertEqual(reader.calls[1].file_type, AcquisitionFileType.DAILY_BUNDLE)


class LandingInputsTemporalTests(unittest.TestCase):
    def test_wrong_market_session_fails_before_any_reader_call(self) -> None:
        manifest, reader, _, _ = _valid_manifest_and_reader()
        with self.assertRaises(LandingInputError):
            acquire_verified_landing_inputs(
                manifest=manifest, market_session=date(2026, 7, 17), run_cutoff=_CUTOFF, reader=reader
            )
        self.assertEqual(reader.calls, [])

    def test_naive_run_cutoff_fails_before_any_reader_call(self) -> None:
        manifest, reader, _, _ = _valid_manifest_and_reader()
        with self.assertRaises(LandingInputError):
            acquire_verified_landing_inputs(
                manifest=manifest,
                market_session=_TARGET_SESSION,
                run_cutoff=datetime(2026, 7, 16, 23, 59, 59),
                reader=reader,
            )
        self.assertEqual(reader.calls, [])

    def test_run_cutoff_before_manifest_temporal_window_fails_before_any_reader_call(self) -> None:
        # For any valid VerifiedLandingManifest, knowledge_time <= binding.cutoff
        # always holds (enforced when the manifest itself was verified), so a
        # run_cutoff before knowledge_time is necessarily also before
        # binding.cutoff. This asserts the composite fail-closed rejection of
        # a run_cutoff before the manifest's whole temporal window, not that
        # the two internal comparisons can be uniquely isolated from here.
        manifest, reader, _, _ = _valid_manifest_and_reader()
        early_cutoff = datetime(2026, 7, 16, 10, 0, 0, tzinfo=timezone.utc)  # before knowledge_time and binding.cutoff
        with self.assertRaises(LandingInputError):
            acquire_verified_landing_inputs(
                manifest=manifest, market_session=_TARGET_SESSION, run_cutoff=early_cutoff, reader=reader
            )
        self.assertEqual(reader.calls, [])

    def test_binding_cutoff_after_run_cutoff_fails_before_any_reader_call(self) -> None:
        manifest, reader, _, _ = _valid_manifest_and_reader()
        # 15:00 is after knowledge_time (13:30, so that check passes) but before
        # the manifest's binding.cutoff (23:59:59), isolating this specific check.
        mid_cutoff = datetime(2026, 7, 16, 15, 0, 0, tzinfo=timezone.utc)
        with self.assertRaises(LandingInputError):
            acquire_verified_landing_inputs(
                manifest=manifest, market_session=_TARGET_SESSION, run_cutoff=mid_cutoff, reader=reader
            )
        self.assertEqual(reader.calls, [])

    def test_run_cutoff_equal_to_knowledge_time_and_binding_cutoff_succeeds(self) -> None:
        manifest = _verified_manifest(cutoff=datetime(2026, 7, 16, 13, 30, 0, tzinfo=timezone.utc))
        sm_acquired = _acquired_for(manifest.security_master, content_bytes=b"security-master-content")
        db_acquired = _acquired_for(manifest.daily_bundle, content_bytes=b"daily-bundle-content")
        reader = FakeLandingObjectReader(
            responses={
                AcquisitionFileType.SECURITY_MASTER: sm_acquired,
                AcquisitionFileType.DAILY_BUNDLE: db_acquired,
            }
        )
        run_cutoff = datetime(2026, 7, 16, 13, 30, 0, tzinfo=timezone.utc)

        result = acquire_verified_landing_inputs(
            manifest=manifest, market_session=_TARGET_SESSION, run_cutoff=run_cutoff, reader=reader
        )

        self.assertEqual(result.run_cutoff, run_cutoff)


class LandingInputsAcquiredMismatchTests(unittest.TestCase):
    def test_wrong_returned_type_fails(self) -> None:
        manifest, _, _, db_acquired = _valid_manifest_and_reader()
        reader = FakeLandingObjectReader(
            responses={
                AcquisitionFileType.SECURITY_MASTER: "not-an-acquired-file",
                AcquisitionFileType.DAILY_BUNDLE: db_acquired,
            }
        )
        with self.assertRaises(LandingInputError):
            acquire_verified_landing_inputs(
                manifest=manifest, market_session=_TARGET_SESSION, run_cutoff=_CUTOFF, reader=reader
            )
        self.assertEqual(len(reader.calls), 1)

    def test_mismatched_bucket_fails(self) -> None:
        manifest, _, sm_acquired, db_acquired = _valid_manifest_and_reader()
        tampered = dataclasses.replace(sm_acquired, bucket="another-syntactically-valid-bucket")
        reader = FakeLandingObjectReader(
            responses={
                AcquisitionFileType.SECURITY_MASTER: tampered,
                AcquisitionFileType.DAILY_BUNDLE: db_acquired,
            }
        )
        with self.assertRaises(LandingInputError):
            acquire_verified_landing_inputs(
                manifest=manifest, market_session=_TARGET_SESSION, run_cutoff=_CUTOFF, reader=reader
            )

    def test_mismatched_object_name_fails(self) -> None:
        manifest, _, sm_acquired, db_acquired = _valid_manifest_and_reader()
        tampered = dataclasses.replace(sm_acquired, object_name="landing/2026-07-16/wrong-name.csv.gz")
        reader = FakeLandingObjectReader(
            responses={
                AcquisitionFileType.SECURITY_MASTER: tampered,
                AcquisitionFileType.DAILY_BUNDLE: db_acquired,
            }
        )
        with self.assertRaises(LandingInputError):
            acquire_verified_landing_inputs(
                manifest=manifest, market_session=_TARGET_SESSION, run_cutoff=_CUTOFF, reader=reader
            )

    def test_mismatched_generation_fails(self) -> None:
        manifest, _, sm_acquired, db_acquired = _valid_manifest_and_reader()
        tampered = dataclasses.replace(sm_acquired, generation=999)
        reader = FakeLandingObjectReader(
            responses={
                AcquisitionFileType.SECURITY_MASTER: tampered,
                AcquisitionFileType.DAILY_BUNDLE: db_acquired,
            }
        )
        with self.assertRaises(LandingInputError):
            acquire_verified_landing_inputs(
                manifest=manifest, market_session=_TARGET_SESSION, run_cutoff=_CUTOFF, reader=reader
            )

    def test_mismatched_target_session_fails(self) -> None:
        manifest, _, sm_acquired, db_acquired = _valid_manifest_and_reader()
        tampered = dataclasses.replace(sm_acquired, target_session=date(2026, 7, 17))
        reader = FakeLandingObjectReader(
            responses={
                AcquisitionFileType.SECURITY_MASTER: tampered,
                AcquisitionFileType.DAILY_BUNDLE: db_acquired,
            }
        )
        with self.assertRaises(LandingInputError):
            acquire_verified_landing_inputs(
                manifest=manifest, market_session=_TARGET_SESSION, run_cutoff=_CUTOFF, reader=reader
            )

    def test_mismatched_file_type_fails(self) -> None:
        manifest, _, sm_acquired, db_acquired = _valid_manifest_and_reader()
        tampered = dataclasses.replace(sm_acquired, file_type=AcquisitionFileType.DAILY_BUNDLE)
        reader = FakeLandingObjectReader(
            responses={
                AcquisitionFileType.SECURITY_MASTER: tampered,
                AcquisitionFileType.DAILY_BUNDLE: db_acquired,
            }
        )
        with self.assertRaises(LandingInputError):
            acquire_verified_landing_inputs(
                manifest=manifest, market_session=_TARGET_SESSION, run_cutoff=_CUTOFF, reader=reader
            )


class LandingInputsContentIntegrityTests(unittest.TestCase):
    def test_tampered_content_with_stale_hash_fails(self) -> None:
        manifest, _, sm_acquired, db_acquired = _valid_manifest_and_reader()
        tampered = dataclasses.replace(sm_acquired, content_bytes=b"tampered-content-not-matching-hash")
        reader = FakeLandingObjectReader(
            responses={
                AcquisitionFileType.SECURITY_MASTER: tampered,
                AcquisitionFileType.DAILY_BUNDLE: db_acquired,
            }
        )
        with self.assertRaises(LandingInputError):
            acquire_verified_landing_inputs(
                manifest=manifest, market_session=_TARGET_SESSION, run_cutoff=_CUTOFF, reader=reader
            )

    def test_non_bytes_content_fails(self) -> None:
        manifest, _, sm_acquired, db_acquired = _valid_manifest_and_reader()
        tampered = dataclasses.replace(sm_acquired, content_bytes="not-bytes")
        reader = FakeLandingObjectReader(
            responses={
                AcquisitionFileType.SECURITY_MASTER: tampered,
                AcquisitionFileType.DAILY_BUNDLE: db_acquired,
            }
        )
        with self.assertRaises(LandingInputError):
            acquire_verified_landing_inputs(
                manifest=manifest, market_session=_TARGET_SESSION, run_cutoff=_CUTOFF, reader=reader
            )

    def test_empty_content_fails(self) -> None:
        # content_bytes, the acquired sha256_hash, and the manifest's own
        # expected_sha256 all agree on sha256(b""), so every match/hash
        # check passes; only the explicit non-empty guard can reject this.
        empty_sha256 = hashlib.sha256(b"").hexdigest()
        manifest = _verified_manifest(sm_sha256=empty_sha256)
        empty_acquired = _acquired_for(manifest.security_master, content_bytes=b"")
        db_acquired = _acquired_for(manifest.daily_bundle, content_bytes=b"daily-bundle-content")
        reader = FakeLandingObjectReader(
            responses={
                AcquisitionFileType.SECURITY_MASTER: empty_acquired,
                AcquisitionFileType.DAILY_BUNDLE: db_acquired,
            }
        )
        with self.assertRaises(LandingInputError):
            acquire_verified_landing_inputs(
                manifest=manifest, market_session=_TARGET_SESSION, run_cutoff=_CUTOFF, reader=reader
            )
        self.assertEqual(len(reader.calls), 1)

    def test_self_consistent_but_wrong_hash_fails(self) -> None:
        # The acquired object's own content and sha256_hash agree with each
        # other (a self-consistent forgery) but do not match what the
        # manifest actually requested. This must still fail.
        manifest, _, sm_acquired, db_acquired = _valid_manifest_and_reader()
        new_content = b"attacker-controlled-but-internally-self-consistent-content"
        tampered = dataclasses.replace(
            sm_acquired,
            content_bytes=new_content,
            sha256_hash=hashlib.sha256(new_content).hexdigest(),
        )
        reader = FakeLandingObjectReader(
            responses={
                AcquisitionFileType.SECURITY_MASTER: tampered,
                AcquisitionFileType.DAILY_BUNDLE: db_acquired,
            }
        )
        with self.assertRaises(LandingInputError):
            acquire_verified_landing_inputs(
                manifest=manifest, market_session=_TARGET_SESSION, run_cutoff=_CUTOFF, reader=reader
            )


class LandingInputsReaderFailureTests(unittest.TestCase):
    def test_first_read_failure_propagates_without_attempting_second_read(self) -> None:
        manifest, _, _, db_acquired = _valid_manifest_and_reader()
        boom = ValueError("boom")
        reader = FakeLandingObjectReader(
            raises={AcquisitionFileType.SECURITY_MASTER: boom},
            responses={AcquisitionFileType.DAILY_BUNDLE: db_acquired},
        )
        with self.assertRaises(ValueError) as ctx:
            acquire_verified_landing_inputs(
                manifest=manifest, market_session=_TARGET_SESSION, run_cutoff=_CUTOFF, reader=reader
            )
        self.assertIs(ctx.exception, boom)
        self.assertEqual(len(reader.calls), 1)

    def test_second_read_failure_propagates_after_exactly_two_attempts_no_retry(self) -> None:
        manifest, _, sm_acquired, _ = _valid_manifest_and_reader()
        boom = ValueError("boom")
        reader = FakeLandingObjectReader(
            responses={AcquisitionFileType.SECURITY_MASTER: sm_acquired},
            raises={AcquisitionFileType.DAILY_BUNDLE: boom},
        )
        with self.assertRaises(ValueError) as ctx:
            acquire_verified_landing_inputs(
                manifest=manifest, market_session=_TARGET_SESSION, run_cutoff=_CUTOFF, reader=reader
            )
        self.assertIs(ctx.exception, boom)
        self.assertEqual(len(reader.calls), 2)


class VerifiedLandingInputsDirectConstructionTests(unittest.TestCase):
    def test_valid_direct_construction_succeeds(self) -> None:
        manifest, _, sm_acquired, db_acquired = _valid_manifest_and_reader()
        result = VerifiedLandingInputs(
            manifest=manifest,
            market_session=_TARGET_SESSION,
            run_cutoff=_CUTOFF,
            security_master=sm_acquired,
            daily_bundle=db_acquired,
        )
        self.assertIs(result.manifest, manifest)

    def test_direct_construction_with_wrong_market_session_fails(self) -> None:
        manifest, _, sm_acquired, db_acquired = _valid_manifest_and_reader()
        with self.assertRaises(LandingInputError):
            VerifiedLandingInputs(
                manifest=manifest,
                market_session=date(2026, 7, 17),
                run_cutoff=_CUTOFF,
                security_master=sm_acquired,
                daily_bundle=db_acquired,
            )

    def test_direct_construction_with_naive_cutoff_fails(self) -> None:
        manifest, _, sm_acquired, db_acquired = _valid_manifest_and_reader()
        with self.assertRaises(LandingInputError):
            VerifiedLandingInputs(
                manifest=manifest,
                market_session=_TARGET_SESSION,
                run_cutoff=datetime(2026, 7, 16, 23, 59, 59),
                security_master=sm_acquired,
                daily_bundle=db_acquired,
            )

    def test_direct_construction_with_run_cutoff_before_manifest_temporal_window_fails(self) -> None:
        # See test_run_cutoff_before_manifest_temporal_window_fails_before_any_reader_call:
        # knowledge_time <= binding.cutoff always holds for a valid manifest,
        # so this is a composite rejection of the whole temporal window, not
        # proof that either internal comparison is uniquely isolated here.
        manifest, _, sm_acquired, db_acquired = _valid_manifest_and_reader()
        with self.assertRaises(LandingInputError):
            VerifiedLandingInputs(
                manifest=manifest,
                market_session=_TARGET_SESSION,
                run_cutoff=datetime(2026, 7, 16, 10, 0, 0, tzinfo=timezone.utc),
                security_master=sm_acquired,
                daily_bundle=db_acquired,
            )

    def test_direct_construction_with_mismatched_acquired_object_fails(self) -> None:
        manifest, _, sm_acquired, db_acquired = _valid_manifest_and_reader()
        tampered = dataclasses.replace(sm_acquired, generation=999)
        with self.assertRaises(LandingInputError):
            VerifiedLandingInputs(
                manifest=manifest,
                market_session=_TARGET_SESSION,
                run_cutoff=_CUTOFF,
                security_master=tampered,
                daily_bundle=db_acquired,
            )


class ManifestAcquisitionAcceptanceTests(unittest.TestCase):
    def test_manifest_acquisition_reaches_verified_inputs_as_a_defensive_snapshot(self) -> None:
        manifest, reader, sm_acquired, db_acquired = _valid_manifest_and_reader()
        original_acquisition = _acquired_landing_manifest(manifest, generation=999)

        result = acquire_verified_landing_inputs(
            manifest=manifest,
            market_session=_TARGET_SESSION,
            run_cutoff=_CUTOFF,
            reader=reader,
            manifest_acquisition=original_acquisition,
        )

        self.assertIsNotNone(result.manifest_acquisition)
        self.assertEqual(result.manifest_acquisition.request.bucket, _BUCKET)
        self.assertEqual(result.manifest_acquisition.request.generation, 999)
        self.assertEqual(result.manifest_acquisition.request.target_session, _TARGET_SESSION)
        self.assertEqual(result.manifest_acquisition.manifest, manifest)
        # A fresh, defensively-reconstructed snapshot, not the caller's
        # original object identity.
        self.assertIsNot(result.manifest_acquisition, original_acquisition)
        self.assertIsNot(result.manifest_acquisition.request, original_acquisition.request)

    def test_omitted_manifest_acquisition_remains_none(self) -> None:
        manifest, reader, _, _ = _valid_manifest_and_reader()

        result = acquire_verified_landing_inputs(
            manifest=manifest, market_session=_TARGET_SESSION, run_cutoff=_CUTOFF, reader=reader
        )

        self.assertIsNone(result.manifest_acquisition)


class ManifestAcquisitionRejectionTests(unittest.TestCase):
    def test_mismatched_acquisition_manifest_fails_before_any_reader_call(self) -> None:
        manifest, reader, _, _ = _valid_manifest_and_reader()
        other_manifest = _verified_manifest(knowledge_time="2026-07-16T14:00:00Z")
        acquisition = _acquired_landing_manifest(other_manifest)

        with self.assertRaises(LandingInputError):
            acquire_verified_landing_inputs(
                manifest=manifest,
                market_session=_TARGET_SESSION,
                run_cutoff=_CUTOFF,
                reader=reader,
                manifest_acquisition=acquisition,
            )

        self.assertEqual(reader.calls, [])

    def test_wrong_type_acquisition_fails_before_any_reader_call(self) -> None:
        manifest, reader, _, _ = _valid_manifest_and_reader()

        with self.assertRaises(LandingInputError):
            acquire_verified_landing_inputs(
                manifest=manifest,
                market_session=_TARGET_SESSION,
                run_cutoff=_CUTOFF,
                reader=reader,
                manifest_acquisition="not-an-acquisition",  # type: ignore[arg-type]
            )

        self.assertEqual(reader.calls, [])

    def test_acquisition_bucket_not_matching_manifest_binding_fails_before_any_reader_call(
        self,
    ) -> None:
        manifest, reader, _, _ = _valid_manifest_and_reader()
        acquisition = _acquired_landing_manifest(manifest)
        object.__setattr__(acquisition.request, "bucket", "another-syntactically-valid-bucket")

        with self.assertRaises(LandingInputError):
            acquire_verified_landing_inputs(
                manifest=manifest,
                market_session=_TARGET_SESSION,
                run_cutoff=_CUTOFF,
                reader=reader,
                manifest_acquisition=acquisition,
            )

        self.assertEqual(reader.calls, [])

    def test_mutated_acquisition_request_generation_fails_before_any_reader_call(self) -> None:
        manifest, reader, _, _ = _valid_manifest_and_reader()
        acquisition = _acquired_landing_manifest(manifest)
        object.__setattr__(acquisition.request, "generation", True)

        with self.assertRaises(LandingInputError):
            acquire_verified_landing_inputs(
                manifest=manifest,
                market_session=_TARGET_SESSION,
                run_cutoff=_CUTOFF,
                reader=reader,
                manifest_acquisition=acquisition,
            )

        self.assertEqual(reader.calls, [])

    def test_mutated_acquisition_manifest_field_fails_before_any_reader_call(self) -> None:
        manifest, reader, _, _ = _valid_manifest_and_reader()
        # A distinct-but-equal manifest object: mutating the acquisition's
        # own copy must not also corrupt the `manifest` argument used
        # directly by _verify_temporal_bounds, which would isolate the
        # wrong check.
        acquisition = _acquired_landing_manifest(_verified_manifest())
        object.__setattr__(acquisition.manifest, "knowledge_time", "not-a-datetime")

        with self.assertRaises(LandingInputError):
            acquire_verified_landing_inputs(
                manifest=manifest,
                market_session=_TARGET_SESSION,
                run_cutoff=_CUTOFF,
                reader=reader,
                manifest_acquisition=acquisition,
            )

        self.assertEqual(reader.calls, [])

    def test_mutated_acquisition_binding_not_before_never_leaks_raw_exception(self) -> None:
        manifest, reader, _, _ = _valid_manifest_and_reader()
        acquisition = _acquired_landing_manifest(_verified_manifest())
        secret = "SECRET-MUTATED-TIME-DO-NOT-LEAK"
        object.__setattr__(acquisition.manifest.binding, "not_before", secret)

        with self.assertRaises(LandingInputError) as ctx:
            acquire_verified_landing_inputs(
                manifest=manifest,
                market_session=_TARGET_SESSION,
                run_cutoff=_CUTOFF,
                reader=reader,
                manifest_acquisition=acquisition,
            )

        self.assertNotIn(secret, str(ctx.exception))
        self.assertEqual(reader.calls, [])

    def test_mutated_acquisition_binding_cutoff_wrong_type_never_leaks_raw_exception(self) -> None:
        manifest, reader, _, _ = _valid_manifest_and_reader()
        acquisition = _acquired_landing_manifest(_verified_manifest())
        object.__setattr__(acquisition.manifest.binding, "cutoff", 12345)

        with self.assertRaises(LandingInputError):
            acquire_verified_landing_inputs(
                manifest=manifest,
                market_session=_TARGET_SESSION,
                run_cutoff=_CUTOFF,
                reader=reader,
                manifest_acquisition=acquisition,
            )

        self.assertEqual(reader.calls, [])

    def test_acquisition_binding_replaced_by_plain_object_never_leaks_raw_exception(self) -> None:
        manifest, reader, _, _ = _valid_manifest_and_reader()
        acquisition = _acquired_landing_manifest(_verified_manifest())
        object.__setattr__(acquisition.manifest, "binding", object())

        with self.assertRaises(LandingInputError):
            acquire_verified_landing_inputs(
                manifest=manifest,
                market_session=_TARGET_SESSION,
                run_cutoff=_CUTOFF,
                reader=reader,
                manifest_acquisition=acquisition,
            )

        self.assertEqual(reader.calls, [])

    def test_shaped_binding_impostor_with_poisoned_equality_is_rejected_before_any_read(
        self,
    ) -> None:
        # Every field is genuinely valid and __eq__ always returns True, so
        # a defense that reconstructs-then-compares-by-value (rather than
        # requiring the exact type first) would be fooled into treating
        # this impostor as a real TrustedLandingManifestBinding.
        manifest, reader, _, _ = _valid_manifest_and_reader()
        acquisition = _acquired_landing_manifest(_verified_manifest())
        impostor = _shaped_binding_impostor_for(acquisition.manifest)
        object.__setattr__(acquisition.manifest, "binding", impostor)

        with self.assertRaises(LandingInputError) as ctx:
            acquire_verified_landing_inputs(
                manifest=manifest,
                market_session=_TARGET_SESSION,
                run_cutoff=_CUTOFF,
                reader=reader,
                manifest_acquisition=acquisition,
            )

        self.assertEqual(str(ctx.exception), "landing input manifest acquisition is invalid")
        self.assertEqual(reader.calls, [])


class VerifiedLandingInputsManifestAcquisitionDirectConstructionTests(unittest.TestCase):
    def test_valid_construction_with_manifest_acquisition_succeeds(self) -> None:
        manifest, _, sm_acquired, db_acquired = _valid_manifest_and_reader()
        acquisition = _acquired_landing_manifest(manifest)

        result = VerifiedLandingInputs(
            manifest=manifest,
            market_session=_TARGET_SESSION,
            run_cutoff=_CUTOFF,
            security_master=sm_acquired,
            daily_bundle=db_acquired,
            manifest_acquisition=acquisition,
        )

        # __post_init__ replaces manifest_acquisition with a defensive
        # snapshot, so this is value-equal but not the caller's own object.
        self.assertEqual(result.manifest_acquisition, acquisition)
        self.assertIsNot(result.manifest_acquisition, acquisition)

    def test_direct_construction_retains_a_defensively_distinct_snapshot(self) -> None:
        manifest, _, sm_acquired, db_acquired = _valid_manifest_and_reader()
        acquisition = _acquired_landing_manifest(manifest)

        result = VerifiedLandingInputs(
            manifest=manifest,
            market_session=_TARGET_SESSION,
            run_cutoff=_CUTOFF,
            security_master=sm_acquired,
            daily_bundle=db_acquired,
            manifest_acquisition=acquisition,
        )

        self.assertIsNot(result.manifest_acquisition, acquisition)
        self.assertIsNot(result.manifest_acquisition.request, acquisition.request)
        self.assertIsNot(result.manifest_acquisition.manifest, acquisition.manifest)
        self.assertIsNot(
            result.manifest_acquisition.manifest.binding, acquisition.manifest.binding
        )
        self.assertEqual(result.manifest_acquisition.request, acquisition.request)
        self.assertEqual(result.manifest_acquisition.manifest, acquisition.manifest)
        self.assertEqual(
            result.manifest_acquisition.manifest.binding, acquisition.manifest.binding
        )

        # Mutating the caller's original objects after construction must
        # not be able to reach back into the stored snapshot.
        object.__setattr__(acquisition.request, "bucket", "another-syntactically-valid-bucket")
        object.__setattr__(acquisition.manifest.binding, "allowed_bucket", "another-syntactically-valid-bucket")
        self.assertEqual(result.manifest_acquisition.request.bucket, _BUCKET)
        self.assertEqual(result.manifest_acquisition.manifest.binding.allowed_bucket, _BUCKET)

    def test_construction_without_manifest_acquisition_still_defaults_to_none(self) -> None:
        manifest, _, sm_acquired, db_acquired = _valid_manifest_and_reader()

        result = VerifiedLandingInputs(
            manifest=manifest,
            market_session=_TARGET_SESSION,
            run_cutoff=_CUTOFF,
            security_master=sm_acquired,
            daily_bundle=db_acquired,
        )

        self.assertIsNone(result.manifest_acquisition)

    def test_construction_with_mismatched_acquisition_manifest_fails(self) -> None:
        manifest, _, sm_acquired, db_acquired = _valid_manifest_and_reader()
        other_manifest = _verified_manifest(knowledge_time="2026-07-16T14:00:00Z")
        acquisition = _acquired_landing_manifest(other_manifest)

        with self.assertRaises(LandingInputError):
            VerifiedLandingInputs(
                manifest=manifest,
                market_session=_TARGET_SESSION,
                run_cutoff=_CUTOFF,
                security_master=sm_acquired,
                daily_bundle=db_acquired,
                manifest_acquisition=acquisition,
            )

    def test_construction_with_wrong_type_acquisition_fails(self) -> None:
        manifest, _, sm_acquired, db_acquired = _valid_manifest_and_reader()

        with self.assertRaises(LandingInputError):
            VerifiedLandingInputs(
                manifest=manifest,
                market_session=_TARGET_SESSION,
                run_cutoff=_CUTOFF,
                security_master=sm_acquired,
                daily_bundle=db_acquired,
                manifest_acquisition="not-an-acquisition",  # type: ignore[arg-type]
            )


class LandingInputsCapabilityTests(unittest.TestCase):
    def test_no_listing_or_latest_shaped_capability_exists(self) -> None:
        for candidate in (LandingObjectReader, VerifiedLandingInputs):
            members = [name for name in dir(candidate) if not name.startswith("__")]
            self.assertFalse(
                any("list" in name.lower() or "latest" in name.lower() for name in members),
                f"{candidate!r} unexpectedly exposes a listing/latest-shaped member",
            )


class LandingInputsSanitizationTests(unittest.TestCase):
    def test_injected_secret_path_never_appears_in_error(self) -> None:
        manifest, _, sm_acquired, db_acquired = _valid_manifest_and_reader()
        secret = "SECRET-PATH-DO-NOT-LEAK-8a1f"
        tampered = dataclasses.replace(sm_acquired, object_name=f"landing/{secret}/tampered")
        reader = FakeLandingObjectReader(
            responses={
                AcquisitionFileType.SECURITY_MASTER: tampered,
                AcquisitionFileType.DAILY_BUNDLE: db_acquired,
            }
        )
        with self.assertRaises(LandingInputError) as ctx:
            acquire_verified_landing_inputs(
                manifest=manifest, market_session=_TARGET_SESSION, run_cutoff=_CUTOFF, reader=reader
            )
        self.assertNotIn(secret, str(ctx.exception))

    def test_injected_secret_bucket_never_appears_in_error(self) -> None:
        manifest, _, sm_acquired, db_acquired = _valid_manifest_and_reader()
        secret = "secret-bucket-do-not-leak-2c9d"
        tampered = dataclasses.replace(sm_acquired, bucket=secret)
        reader = FakeLandingObjectReader(
            responses={
                AcquisitionFileType.SECURITY_MASTER: tampered,
                AcquisitionFileType.DAILY_BUNDLE: db_acquired,
            }
        )
        with self.assertRaises(LandingInputError) as ctx:
            acquire_verified_landing_inputs(
                manifest=manifest, market_session=_TARGET_SESSION, run_cutoff=_CUTOFF, reader=reader
            )
        self.assertNotIn(secret, str(ctx.exception))

    def test_injected_secret_content_never_appears_in_error(self) -> None:
        manifest, _, sm_acquired, db_acquired = _valid_manifest_and_reader()
        secret_content = b"SECRET-CONTENT-DO-NOT-LEAK-9f7a"
        tampered = dataclasses.replace(sm_acquired, content_bytes=secret_content)
        reader = FakeLandingObjectReader(
            responses={
                AcquisitionFileType.SECURITY_MASTER: tampered,
                AcquisitionFileType.DAILY_BUNDLE: db_acquired,
            }
        )
        with self.assertRaises(LandingInputError) as ctx:
            acquire_verified_landing_inputs(
                manifest=manifest, market_session=_TARGET_SESSION, run_cutoff=_CUTOFF, reader=reader
            )
        self.assertNotIn(secret_content.decode(), str(ctx.exception))

    def test_injected_secret_hash_never_appears_in_error(self) -> None:
        manifest, _, sm_acquired, db_acquired = _valid_manifest_and_reader()
        fake_hash = hashlib.sha256(b"attacker-content").hexdigest()
        tampered = dataclasses.replace(sm_acquired, sha256_hash=fake_hash)
        reader = FakeLandingObjectReader(
            responses={
                AcquisitionFileType.SECURITY_MASTER: tampered,
                AcquisitionFileType.DAILY_BUNDLE: db_acquired,
            }
        )
        with self.assertRaises(LandingInputError) as ctx:
            acquire_verified_landing_inputs(
                manifest=manifest, market_session=_TARGET_SESSION, run_cutoff=_CUTOFF, reader=reader
            )
        message = str(ctx.exception)
        self.assertNotIn(fake_hash, message)
        self.assertNotIn(sm_acquired.sha256_hash, message)


if __name__ == "__main__":
    unittest.main()
