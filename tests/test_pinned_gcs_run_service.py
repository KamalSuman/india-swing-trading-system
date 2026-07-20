from __future__ import annotations

import ast
import hashlib
import json
import tempfile
import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone, tzinfo
from pathlib import Path
from unittest.mock import patch

from india_swing.calendar_data.artifact_store import LocalCalendarSourceArtifactStore
from india_swing.calendar_data.materialization import (
    CollectionCalendarMaterialization,
    materialize_collection_calendar,
)
from india_swing.calendar_data.materialization_store import (
    CALENDAR_MATERIALIZATION_STORE_CODEC_VERSION,
    CALENDAR_MATERIALIZATION_STORE_DATASET,
    CALENDAR_MATERIALIZATION_STORE_SCHEMA_VERSION,
    MATERIALIZATION_FILENAME,
    CalendarMaterializationStoreManifest,
    StoredCalendarMaterialization,
)
from india_swing.calendar_data.models import CALENDAR_DECLARATION_SCHEMA_VERSION
from india_swing.daily_pipeline.acquisition import GCSObjectPayload, LandingManifestObjectRequest
from india_swing.daily_pipeline.calendar_materialization_acquisition import (
    AcquiredCalendarMaterialization,
    CalendarMaterializationAcquisitionError,
    CalendarMaterializationObjectRequest,
    acquire_calendar_materialization,
)
from india_swing.calendar_data.materialization_codec import (
    MAXIMUM_CALENDAR_MATERIALIZATION_BYTES,
    encode_calendar_materialization,
)
from india_swing.daily_pipeline.landing_manifest import TrustedLandingManifestBinding
from india_swing.daily_pipeline.pinned_gcs_run_service import (
    PinnedGCSRunServiceError,
    run_daily_pipeline_from_pinned_gcs_run_spec,
)
from india_swing.daily_pipeline.pinned_gcs_run_spec import (
    PINNED_GCS_RUN_SPEC_SCHEMA_VERSION,
    PINNED_GCS_RUN_SPEC_SCHEMA_VERSION_WITH_CALENDAR,
    PinnedGCSRunSpec,
)
from india_swing.reference.models import ReferenceReadiness


UTC = timezone.utc
_SESSION = date(2026, 7, 20)  # Monday
_OTHER_SESSION = date(2026, 7, 21)  # Tuesday
_SOURCE_VALIDATED = datetime(2026, 7, 15, 13, 0, 0, tzinfo=UTC)
_CALENDAR_CUTOFF = datetime(2026, 7, 20, 12, 0, 0, tzinfo=UTC)
_BUCKET = "trusted-run-service-bucket"
_SHA256_HEX = "a" * 64
_PREVIOUS_RUN_ID = "c" * 64
_NOT_BEFORE = datetime(2026, 7, 20, 0, 0, 0, tzinfo=UTC)
_BINDING_CUTOFF = datetime(2026, 7, 20, 14, 0, 0, tzinfo=UTC)
_RUN_CUTOFF = datetime(2026, 7, 20, 15, 0, 0, tzinfo=UTC)
_ENCODED_PLACEHOLDER = b"pinned-gcs-run-service-test-materialization-bytes"
_CALENDAR_ACQUISITION_BUCKET = "trusted-calendar-materialization-bucket"
_CALENDAR_ACQUISITION_GENERATION = 999
_ERR_SPEC = "pinned gcs run service spec is invalid"
_ERR_CALENDAR_ACQUISITION = "pinned gcs run service calendar acquisition failed"
_ERR_CALENDAR = "pinned gcs run service calendar materialization is invalid"
_ERR_EXECUTION = "pinned gcs run service execution failed"

_SERVICE_TARGET = "india_swing.daily_pipeline.pinned_gcs_run_service.run_daily_pipeline_from_pinned_gcs_manifest"
_ACQUIRE_TARGET = (
    "india_swing.daily_pipeline.pinned_gcs_run_service.acquire_calendar_materialization"
)


def _manifest_object_name(session: date = _SESSION) -> str:
    return f"landing/{session.isoformat()}/landing-manifest.json"


def _valid_request(session: date = _SESSION) -> LandingManifestObjectRequest:
    return LandingManifestObjectRequest(
        bucket=_BUCKET,
        object_name=_manifest_object_name(session),
        generation=777,
        target_session=session,
    )


def _valid_binding(session: date = _SESSION) -> TrustedLandingManifestBinding:
    return TrustedLandingManifestBinding(
        expected_manifest_sha256=_SHA256_HEX,
        allowed_bucket=_BUCKET,
        target_session=session,
        not_before=_NOT_BEFORE,
        cutoff=_BINDING_CUTOFF,
    )


def _valid_spec_kwargs(*, calendar_materialization_id: str, **overrides: object) -> dict[str, object]:
    session = overrides.pop("_session", _SESSION)
    base: dict[str, object] = dict(
        schema_version=PINNED_GCS_RUN_SPEC_SCHEMA_VERSION,
        manifest_request=_valid_request(session),
        trusted_binding=_valid_binding(session),
        market_session=session,
        cutoff=_RUN_CUTOFF,
        calendar_materialization_id=calendar_materialization_id,
        previous_run_id=None,
    )
    base.update(overrides)
    return base


def _valid_spec_kwargs_v2(
    *, calendar_request: CalendarMaterializationObjectRequest, **overrides: object
) -> dict[str, object]:
    session = overrides.pop("_session", _SESSION)
    base: dict[str, object] = dict(
        schema_version=PINNED_GCS_RUN_SPEC_SCHEMA_VERSION_WITH_CALENDAR,
        manifest_request=_valid_request(session),
        trusted_binding=_valid_binding(session),
        market_session=session,
        cutoff=_RUN_CUTOFF,
        calendar_materialization_id=calendar_request.materialization_id,
        previous_run_id=None,
        calendar_request=calendar_request,
    )
    base.update(overrides)
    return base


def _calendar_object_name(materialization_id: str) -> str:
    return f"calendar-materializations/{materialization_id}/materialization.json"


class FakeGCSObjectReader:
    """Fake GCSObjectReader. Never contacts GCP; records every call made."""

    def __init__(self, *, generation: object, content_bytes: object) -> None:
        self.generation = generation
        self.content_bytes = content_bytes
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
        return GCSObjectPayload(content_bytes=self.content_bytes, generation=self.generation)


def _import_base_source(calendar_root: Path, inputs_root: Path, *, document_id: str = "CMTR-BASE-2026"):
    source_name = f"{document_id}.pdf"
    declaration_name = f"{document_id}.events.json"
    source_bytes = f"%PDF-1.7\n{document_id}\n%%EOF\n".encode("ascii")
    inputs_root.mkdir(parents=True, exist_ok=True)
    source_path = inputs_root / source_name
    declaration_path = inputs_root / declaration_name
    source_path.write_bytes(source_bytes)
    declaration_path.write_text(
        json.dumps(
            {
                "schema_version": CALENDAR_DECLARATION_SCHEMA_VERSION,
                "exchange": "NSE",
                "segment": "CM",
                "claimed_authority": "NSE",
                "claimed_document_id": document_id,
                "claimed_issue_date": "2026-01-01",
                "claimed_source_url": f"https://example.invalid/{source_name}",
                "source_filename": source_name,
                "source_media_type": "application/pdf",
                "source_byte_count": len(source_bytes),
                "source_sha256": hashlib.sha256(source_bytes).hexdigest(),
                "events": [
                    {
                        "event_type": "BASE_WEEKLY_SCHEDULE",
                        "effective_from": "2026-01-01",
                        "effective_to_exclusive": "2027-01-01",
                        "weekdays": ["MON", "TUE", "WED", "THU", "FRI"],
                        "windows": [
                            {
                                "phase": "LIVE_CONTINUOUS",
                                "opens": "09:15:00",
                                "closes": "15:30:00",
                            }
                        ],
                        "supersedes_event_ids": [],
                        "source_locator": {
                            "page": 1,
                            "section": "CM schedule",
                            "record": "regular",
                        },
                        "reason": "Regular capital-market schedule",
                    }
                ],
            },
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    times = iter((_SOURCE_VALIDATED - timedelta(seconds=1), _SOURCE_VALIDATED))
    return LocalCalendarSourceArtifactStore(
        calendar_root,
        clock=lambda: next(times),
    ).import_source(source_path, declaration_path)


def _build_materialization(
    root: Path,
    *,
    session: date = _SESSION,
    cutoff: datetime = _CALENDAR_CUTOFF,
    document_id: str = "CMTR-BASE-2026",
) -> CollectionCalendarMaterialization:
    source = _import_base_source(root / "calendar", root / "inputs" / document_id, document_id=document_id)
    return materialize_collection_calendar(
        sources=(source,),
        coverage_start=session,
        coverage_end=session,
        cutoff=cutoff,
        observed_date_artifacts=(),
    )


def _build_v2_fixture(
    root: Path,
    *,
    session: date = _SESSION,
    cutoff: datetime = _CALENDAR_CUTOFF,
    document_id: str = "CMTR-V2-BASE",
    generation: int = _CALENDAR_ACQUISITION_GENERATION,
    bucket: str = _CALENDAR_ACQUISITION_BUCKET,
) -> tuple[
    CollectionCalendarMaterialization,
    bytes,
    CalendarMaterializationObjectRequest,
    FakeGCSObjectReader,
]:
    """Real, canonically-encoded materialization plus a matching
    generation-pinned calendar_request and an injected fake reader ready
    to serve it -- reused across every schema-v2 test in this file."""

    materialization = _build_materialization(
        root, session=session, cutoff=cutoff, document_id=document_id
    )
    payload = encode_calendar_materialization(materialization)
    calendar_request = CalendarMaterializationObjectRequest(
        bucket=bucket,
        object_name=_calendar_object_name(materialization.materialization_id),
        generation=generation,
        expected_sha256=hashlib.sha256(payload).hexdigest(),
        materialization_id=materialization.materialization_id,
    )
    fake_reader = FakeGCSObjectReader(generation=generation, content_bytes=payload)
    return materialization, payload, calendar_request, fake_reader


def _store_manifest_for(
    materialization: CollectionCalendarMaterialization, encoded: bytes
) -> CalendarMaterializationStoreManifest:
    provisional = CalendarMaterializationStoreManifest(
        schema_version=CALENDAR_MATERIALIZATION_STORE_SCHEMA_VERSION,
        manifest_id="0" * 64,
        artifact_id=materialization.materialization_id,
        dataset=CALENDAR_MATERIALIZATION_STORE_DATASET,
        exchange="NSE",
        segment="CM",
        cutoff=materialization.cutoff,
        coverage_start=materialization.coverage_start,
        coverage_end=materialization.coverage_end,
        readiness=ReferenceReadiness.COLLECTION_ONLY,
        actionable=False,
        materialization_schema_version=materialization.schema_version,
        materialization_policy_version=materialization.policy_version,
        materialization_codec_version=CALENDAR_MATERIALIZATION_STORE_CODEC_VERSION,
        materialization_filename=MATERIALIZATION_FILENAME,
        materialization_byte_count=len(encoded),
        materialization_sha256=hashlib.sha256(encoded).hexdigest(),
        calendar_snapshot_id=materialization.calendar_snapshot.snapshot_id,
        calendar_snapshot_version=materialization.calendar_snapshot.version,
        source_manifests=materialization.source_manifests,
        observed_evidence_bindings=materialization.observed_evidence_bindings,
        source_count=len(materialization.source_manifests),
        day_count=len(materialization.day_resolutions),
        session_count=sum(day.is_session for day in materialization.calendar_snapshot.days),
        observed_evidence_count=len(materialization.observed_evidence_bindings),
        observed_date_count=sum(
            len(value.observed_dates) for value in materialization.observed_evidence_bindings
        ),
    )
    return replace(provisional, manifest_id=provisional._calculated_manifest_id())


def _stored_materialization(
    root: Path,
    *,
    session: date = _SESSION,
    cutoff: datetime = _CALENDAR_CUTOFF,
    document_id: str = "CMTR-BASE-2026",
) -> StoredCalendarMaterialization:
    materialization = _build_materialization(root, session=session, cutoff=cutoff, document_id=document_id)
    manifest = _store_manifest_for(materialization, _ENCODED_PLACEHOLDER)
    return StoredCalendarMaterialization(
        path=root / "fixture-materialization",
        manifest=manifest,
        materialization=materialization,
        encoded_bytes=_ENCODED_PLACEHOLDER,
    )


class _RaisingTzInfo(tzinfo):
    def __init__(self, secret: str) -> None:
        self._secret = secret

    def utcoffset(self, dt: object) -> None:
        raise RuntimeError(self._secret)

    def tzname(self, dt: object) -> None:
        raise RuntimeError(self._secret)

    def dst(self, dt: object) -> None:
        raise RuntimeError(self._secret)


class _MarkerBaseException(BaseException):
    pass


class _JobSpy:
    def __init__(self, *, result: object = None, raises: BaseException | None = None) -> None:
        self.calls: list[dict[str, object]] = []
        self._result = result
        self._raises = raises

    def __call__(self, **kwargs: object) -> object:
        self.calls.append(kwargs)
        if self._raises is not None:
            raise self._raises
        return self._result


class PinnedGCSRunServiceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.reader = object()
        self.reference_store = object()
        self.daily_store = object()
        self.historical_store = object()
        self.identity_store = object()
        self.adjudication_store = object()
        self.run_store = object()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _dependencies(self) -> dict[str, object]:
        return dict(
            reader=self.reader,
            reference_store=self.reference_store,
            daily_store=self.daily_store,
            historical_store=self.historical_store,
            identity_store=self.identity_store,
            adjudication_store=self.adjudication_store,
            run_store=self.run_store,
        )

    def _stored(self, **kwargs: object) -> StoredCalendarMaterialization:
        return _stored_materialization(self.root, **kwargs)

    def _spec(self, stored: StoredCalendarMaterialization, **overrides: object) -> PinnedGCSRunSpec:
        return PinnedGCSRunSpec(
            **_valid_spec_kwargs(
                calendar_materialization_id=stored.manifest.artifact_id, **overrides
            )
        )


class ServiceAcceptanceTests(PinnedGCSRunServiceTestCase):
    def test_success_calls_delegated_job_exactly_once_with_exact_arguments(self) -> None:
        stored = self._stored()
        spec = self._spec(stored)
        sentinel = object()
        spy = _JobSpy(result=sentinel)

        with patch(_SERVICE_TARGET, spy):
            result = run_daily_pipeline_from_pinned_gcs_run_spec(
                spec, stored, **self._dependencies()
            )

        self.assertIs(result, sentinel)
        self.assertEqual(len(spy.calls), 1)
        call = spy.calls[0]
        self.assertEqual(call["manifest_request"], spec.manifest_request)
        self.assertIsNot(call["manifest_request"], spec.manifest_request)
        self.assertEqual(call["binding"], spec.trusted_binding)
        self.assertIsNot(call["binding"], spec.trusted_binding)
        self.assertIs(call["reader"], self.reader)
        self.assertEqual(call["market_session"], _SESSION)
        self.assertEqual(call["cutoff"], _RUN_CUTOFF)
        self.assertEqual(call["calendar_materialization_id"], stored.manifest.artifact_id)
        self.assertIs(call["calendar"], stored.materialization.calendar_snapshot)
        self.assertIsNone(call["previous_run_id"])
        self.assertIs(call["reference_store"], self.reference_store)
        self.assertIs(call["daily_store"], self.daily_store)
        self.assertIs(call["historical_store"], self.historical_store)
        self.assertIs(call["identity_store"], self.identity_store)
        self.assertIs(call["adjudication_store"], self.adjudication_store)
        self.assertIs(call["run_store"], self.run_store)
        self.assertEqual(len(call), 14)

    def test_success_with_non_null_previous_run_id(self) -> None:
        stored = self._stored()
        spec = self._spec(stored, previous_run_id=_PREVIOUS_RUN_ID)
        spy = _JobSpy(result=object())

        with patch(_SERVICE_TARGET, spy):
            run_daily_pipeline_from_pinned_gcs_run_spec(spec, stored, **self._dependencies())

        self.assertEqual(spy.calls[0]["previous_run_id"], _PREVIOUS_RUN_ID)

    def test_schema_v1_never_calls_reader_read_generation(self) -> None:
        stored = self._stored()
        spec = self._spec(stored)

        class _ReaderThatMustNotBeCalled:
            def read_generation(self, **_kwargs: object) -> None:
                raise AssertionError("schema v1 must never call read_generation")

        spy = _JobSpy(result=object())
        dependencies = {**self._dependencies(), "reader": _ReaderThatMustNotBeCalled()}

        with patch(_SERVICE_TARGET, spy):
            run_daily_pipeline_from_pinned_gcs_run_spec(spec, stored, **dependencies)

        self.assertEqual(len(spy.calls), 1)

    def test_schema_v2_success_acquires_calendar_then_delegates_exactly_once(self) -> None:
        materialization, _payload, calendar_request, fake_reader = _build_v2_fixture(self.root)
        spec = PinnedGCSRunSpec(**_valid_spec_kwargs_v2(calendar_request=calendar_request))
        sentinel = object()
        spy = _JobSpy(result=sentinel)
        dependencies = {**self._dependencies(), "reader": fake_reader}

        with patch(_SERVICE_TARGET, spy):
            result = run_daily_pipeline_from_pinned_gcs_run_spec(spec, None, **dependencies)

        self.assertIs(result, sentinel)
        self.assertEqual(
            fake_reader.calls,
            [
                {
                    "bucket": calendar_request.bucket,
                    "object_name": calendar_request.object_name,
                    "generation": calendar_request.generation,
                    "maximum_bytes": MAXIMUM_CALENDAR_MATERIALIZATION_BYTES,
                }
            ],
        )
        self.assertEqual(len(spy.calls), 1)
        call = spy.calls[0]
        self.assertEqual(call["manifest_request"], spec.manifest_request)
        self.assertIsNot(call["manifest_request"], spec.manifest_request)
        self.assertEqual(call["binding"], spec.trusted_binding)
        self.assertIsNot(call["binding"], spec.trusted_binding)
        self.assertIs(call["reader"], fake_reader)
        self.assertEqual(call["market_session"], _SESSION)
        self.assertEqual(call["cutoff"], _RUN_CUTOFF)
        self.assertEqual(call["calendar_materialization_id"], calendar_request.materialization_id)
        self.assertEqual(call["calendar"], materialization.calendar_snapshot)
        self.assertIsNone(call["previous_run_id"])
        self.assertIs(call["reference_store"], self.reference_store)
        self.assertIs(call["daily_store"], self.daily_store)
        self.assertIs(call["historical_store"], self.historical_store)
        self.assertIs(call["identity_store"], self.identity_store)
        self.assertIs(call["adjudication_store"], self.adjudication_store)
        self.assertIs(call["run_store"], self.run_store)
        self.assertEqual(len(call), 14)


class ServiceSpecPreflightTests(PinnedGCSRunServiceTestCase):
    def test_wrong_spec_type_is_rejected_before_job_call(self) -> None:
        stored = self._stored()
        spy = _JobSpy(result=object())

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError):
                run_daily_pipeline_from_pinned_gcs_run_spec(
                    "not-a-spec", stored, **self._dependencies()
                )

        self.assertEqual(spy.calls, [])

    def test_spec_subclass_is_rejected_before_job_call(self) -> None:
        stored = self._stored()

        class _SpecSubclass(PinnedGCSRunSpec):
            pass

        subclass_instance = _SpecSubclass(
            **_valid_spec_kwargs(calendar_materialization_id=stored.manifest.artifact_id)
        )
        spy = _JobSpy(result=object())

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError):
                run_daily_pipeline_from_pinned_gcs_run_spec(
                    subclass_instance, stored, **self._dependencies()
                )

        self.assertEqual(spy.calls, [])

    def test_shaped_spec_proxy_with_poisoned_equality_is_rejected(self) -> None:
        stored = self._stored()
        real = self._spec(stored)

        class _ShapedSpecProxy:
            def __init__(self) -> None:
                for name in (
                    "schema_version",
                    "manifest_request",
                    "trusted_binding",
                    "market_session",
                    "cutoff",
                    "calendar_materialization_id",
                    "previous_run_id",
                ):
                    setattr(self, name, getattr(real, name))

            def __eq__(self, other: object) -> bool:
                return True

            def __hash__(self) -> int:
                return 0

        spy = _JobSpy(result=object())

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError):
                run_daily_pipeline_from_pinned_gcs_run_spec(
                    _ShapedSpecProxy(), stored, **self._dependencies()
                )

        self.assertEqual(spy.calls, [])

    def test_post_construction_mutated_nested_request_field_is_rejected(self) -> None:
        stored = self._stored()
        spec = self._spec(stored)
        object.__setattr__(spec.manifest_request, "bucket", "another-syntactically-valid-bucket")
        spy = _JobSpy(result=object())

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError):
                run_daily_pipeline_from_pinned_gcs_run_spec(spec, stored, **self._dependencies())

        self.assertEqual(spy.calls, [])

    def test_post_construction_mutated_calendar_id_mismatch_is_rejected(self) -> None:
        stored = self._stored()
        spec = self._spec(stored)
        object.__setattr__(spec, "calendar_materialization_id", "f" * 64)
        spy = _JobSpy(result=object())

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError):
                run_daily_pipeline_from_pinned_gcs_run_spec(spec, stored, **self._dependencies())

        self.assertEqual(spy.calls, [])

    def test_secret_bearing_raising_tzinfo_on_spec_cutoff_never_leaks(self) -> None:
        stored = self._stored()
        secret = "SECRET-SERVICE-SPEC-CUTOFF-TZINFO-DO-NOT-LEAK-6b3e"
        spec = self._spec(stored)
        object.__setattr__(
            spec, "cutoff", datetime(2026, 7, 20, 15, 0, 0, tzinfo=_RaisingTzInfo(secret))
        )
        spy = _JobSpy(result=object())

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError) as ctx:
                run_daily_pipeline_from_pinned_gcs_run_spec(spec, stored, **self._dependencies())

        self.assertNotIn(secret, str(ctx.exception))
        self.assertNotIn("RuntimeError", str(ctx.exception))
        self.assertEqual(spy.calls, [])

    def test_v1_spec_with_calendar_request_is_rejected(self) -> None:
        stored = self._stored()
        spec = self._spec(stored)
        _materialization, _payload, calendar_request, fake_reader = _build_v2_fixture(self.root)
        object.__setattr__(spec, "calendar_request", calendar_request)
        spy = _JobSpy(result=object())
        dependencies = {**self._dependencies(), "reader": fake_reader}

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError):
                run_daily_pipeline_from_pinned_gcs_run_spec(spec, stored, **dependencies)

        self.assertEqual(spy.calls, [])
        self.assertEqual(fake_reader.calls, [])

    def test_v2_spec_with_local_calendar_is_rejected(self) -> None:
        stored = self._stored()
        _materialization, _payload, calendar_request, fake_reader = _build_v2_fixture(self.root)
        spec = PinnedGCSRunSpec(**_valid_spec_kwargs_v2(calendar_request=calendar_request))
        spy = _JobSpy(result=object())
        dependencies = {**self._dependencies(), "reader": fake_reader}

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError):
                run_daily_pipeline_from_pinned_gcs_run_spec(spec, stored, **dependencies)

        self.assertEqual(spy.calls, [])
        self.assertEqual(fake_reader.calls, [])

    def test_mutated_unsupported_schema_version_is_rejected(self) -> None:
        stored = self._stored()
        spec = self._spec(stored)
        object.__setattr__(spec, "schema_version", 3)
        spy = _JobSpy(result=object())

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError):
                run_daily_pipeline_from_pinned_gcs_run_spec(spec, stored, **self._dependencies())

        self.assertEqual(spy.calls, [])

    def test_v2_calendar_request_subclass_is_rejected_before_any_read(self) -> None:
        # PinnedGCSRunSpec's own __post_init__ already rejects a subclass
        # at construction time, so a valid spec is built first and the
        # subclass substituted afterward via object.__setattr__ -- this
        # specifically exercises this service's own fresh_spec
        # reconstruction (which re-runs PinnedGCSRunSpec's validation on
        # the post-construction-substituted value) rather than the
        # original construction call.
        _materialization, _payload, calendar_request, fake_reader = _build_v2_fixture(self.root)
        spec = PinnedGCSRunSpec(**_valid_spec_kwargs_v2(calendar_request=calendar_request))

        class _RequestSubclass(CalendarMaterializationObjectRequest):
            pass

        subclass_instance = _RequestSubclass(
            bucket=calendar_request.bucket,
            object_name=calendar_request.object_name,
            generation=calendar_request.generation,
            expected_sha256=calendar_request.expected_sha256,
            materialization_id=calendar_request.materialization_id,
        )
        object.__setattr__(spec, "calendar_request", subclass_instance)
        spy = _JobSpy(result=object())
        dependencies = {**self._dependencies(), "reader": fake_reader}

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError):
                run_daily_pipeline_from_pinned_gcs_run_spec(spec, None, **dependencies)

        self.assertEqual(spy.calls, [])
        self.assertEqual(fake_reader.calls, [])

    def test_v2_post_construction_mutated_calendar_request_is_rejected_before_any_read(
        self,
    ) -> None:
        _materialization, _payload, calendar_request, fake_reader = _build_v2_fixture(self.root)
        spec = PinnedGCSRunSpec(**_valid_spec_kwargs_v2(calendar_request=calendar_request))
        # Mutating materialization_id (not generation/bucket) makes the
        # object_name/materialization_id pairing inconsistent, so
        # PinnedGCSRunSpec's own defensive reconstruction of calendar_request
        # -- triggered by this service's own fresh_spec reconstruction --
        # fails before any read is attempted.
        object.__setattr__(spec.calendar_request, "materialization_id", "f" * 64)
        spy = _JobSpy(result=object())
        dependencies = {**self._dependencies(), "reader": fake_reader}

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError):
                run_daily_pipeline_from_pinned_gcs_run_spec(spec, None, **dependencies)

        self.assertEqual(spy.calls, [])
        self.assertEqual(fake_reader.calls, [])


class ServiceCalendarPreflightTests(PinnedGCSRunServiceTestCase):
    def test_wrong_calendar_type_is_rejected_before_job_call(self) -> None:
        stored = self._stored()
        spec = self._spec(stored)
        spy = _JobSpy(result=object())

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError):
                run_daily_pipeline_from_pinned_gcs_run_spec(
                    spec, "not-a-materialization", **self._dependencies()
                )

        self.assertEqual(spy.calls, [])

    def test_calendar_subclass_is_rejected_before_job_call(self) -> None:
        stored = self._stored()
        spec = self._spec(stored)

        class _StoredSubclass(StoredCalendarMaterialization):
            pass

        subclass_instance = _StoredSubclass(
            path=stored.path,
            manifest=stored.manifest,
            materialization=stored.materialization,
            encoded_bytes=stored.encoded_bytes,
        )
        spy = _JobSpy(result=object())

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError):
                run_daily_pipeline_from_pinned_gcs_run_spec(
                    spec, subclass_instance, **self._dependencies()
                )

        self.assertEqual(spy.calls, [])

    def test_shaped_calendar_proxy_with_poisoned_equality_is_rejected(self) -> None:
        stored = self._stored()
        spec = self._spec(stored)

        class _ShapedStoredProxy:
            def __init__(self) -> None:
                self.path = stored.path
                self.manifest = stored.manifest
                self.materialization = stored.materialization
                self.encoded_bytes = stored.encoded_bytes

            def __eq__(self, other: object) -> bool:
                return True

            def __hash__(self) -> int:
                return 0

        spy = _JobSpy(result=object())

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError):
                run_daily_pipeline_from_pinned_gcs_run_spec(
                    spec, _ShapedStoredProxy(), **self._dependencies()
                )

        self.assertEqual(spy.calls, [])

    def test_tampered_manifest_identity_is_rejected(self) -> None:
        stored = self._stored()
        spec = self._spec(stored)
        object.__setattr__(stored.manifest, "artifact_id", "f" * 64)
        spy = _JobSpy(result=object())

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError):
                run_daily_pipeline_from_pinned_gcs_run_spec(spec, stored, **self._dependencies())

        self.assertEqual(spy.calls, [])

    def test_tampered_materialization_identity_is_rejected(self) -> None:
        stored = self._stored()
        spec = self._spec(stored)
        object.__setattr__(stored.materialization, "coverage_end", _OTHER_SESSION)
        spy = _JobSpy(result=object())

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError):
                run_daily_pipeline_from_pinned_gcs_run_spec(spec, stored, **self._dependencies())

        self.assertEqual(spy.calls, [])

    def test_tampered_calendar_snapshot_identity_is_rejected(self) -> None:
        stored = self._stored()
        spec = self._spec(stored)
        object.__setattr__(stored.materialization.calendar_snapshot, "exchange", "NSE ")
        spy = _JobSpy(result=object())

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError):
                run_daily_pipeline_from_pinned_gcs_run_spec(spec, stored, **self._dependencies())

        self.assertEqual(spy.calls, [])

    def test_wrong_artifact_and_materialization_id_binding_is_rejected(self) -> None:
        stored_a = self._stored(session=_SESSION, document_id="CMTR-BASE-A")
        stored_b = self._stored(session=_OTHER_SESSION, document_id="CMTR-BASE-B")
        spec = self._spec(stored_a)
        # spec is bound to stored_a's calendar_materialization_id, but the
        # caller supplies the unrelated stored_b wrapper.
        spy = _JobSpy(result=object())

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError):
                run_daily_pipeline_from_pinned_gcs_run_spec(
                    spec, stored_b, **self._dependencies()
                )

        self.assertEqual(spy.calls, [])

    def test_wrong_calendar_snapshot_id_binding_is_rejected_despite_self_consistent_manifest(
        self,
    ) -> None:
        stored = self._stored()
        spec = self._spec(stored)
        tampered_manifest = replace(stored.manifest, calendar_snapshot_id="f" * 64)
        tampered_manifest = replace(
            tampered_manifest, manifest_id=tampered_manifest._calculated_manifest_id()
        )
        tampered_stored = StoredCalendarMaterialization(
            path=stored.path,
            manifest=tampered_manifest,
            materialization=stored.materialization,
            encoded_bytes=stored.encoded_bytes,
        )
        spy = _JobSpy(result=object())

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError):
                run_daily_pipeline_from_pinned_gcs_run_spec(
                    spec, tampered_stored, **self._dependencies()
                )

        self.assertEqual(spy.calls, [])

    def test_manifest_cutoff_disagreement_with_materialization_is_rejected(self) -> None:
        stored = self._stored()
        spec = self._spec(stored)
        other_cutoff = _CALENDAR_CUTOFF + timedelta(hours=1)
        tampered_manifest = replace(stored.manifest, cutoff=other_cutoff)
        tampered_manifest = replace(
            tampered_manifest, manifest_id=tampered_manifest._calculated_manifest_id()
        )
        tampered_stored = StoredCalendarMaterialization(
            path=stored.path,
            manifest=tampered_manifest,
            materialization=stored.materialization,
            encoded_bytes=stored.encoded_bytes,
        )
        spy = _JobSpy(result=object())

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError):
                run_daily_pipeline_from_pinned_gcs_run_spec(
                    spec, tampered_stored, **self._dependencies()
                )

        self.assertEqual(spy.calls, [])

    def test_calendar_session_absence_is_rejected(self) -> None:
        weekend = date(2026, 7, 18)  # Saturday: covered but not a session
        stored = self._stored(session=weekend)
        spec = self._spec(stored, _session=weekend)
        spy = _JobSpy(result=object())

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError):
                run_daily_pipeline_from_pinned_gcs_run_spec(spec, stored, **self._dependencies())

        self.assertEqual(spy.calls, [])

    def test_future_calendar_cutoff_is_rejected(self) -> None:
        stored = self._stored(cutoff=_RUN_CUTOFF + timedelta(hours=1))
        spec = self._spec(stored)
        spy = _JobSpy(result=object())

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError):
                run_daily_pipeline_from_pinned_gcs_run_spec(spec, stored, **self._dependencies())

        self.assertEqual(spy.calls, [])

    def test_calendar_cutoff_equal_to_spec_cutoff_is_accepted(self) -> None:
        stored = self._stored(cutoff=_RUN_CUTOFF)
        spec = self._spec(stored)
        spy = _JobSpy(result=object())

        with patch(_SERVICE_TARGET, spy):
            run_daily_pipeline_from_pinned_gcs_run_spec(spec, stored, **self._dependencies())

        self.assertEqual(len(spy.calls), 1)


class ServiceDelegationFailureTests(PinnedGCSRunServiceTestCase):
    def test_ordinary_secret_bearing_job_exception_is_collapsed(self) -> None:
        stored = self._stored()
        spec = self._spec(stored)
        secret = "SECRET-DELEGATED-JOB-FAILURE-DO-NOT-LEAK-2e9c"
        spy = _JobSpy(raises=ValueError(secret))

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError) as ctx:
                run_daily_pipeline_from_pinned_gcs_run_spec(spec, stored, **self._dependencies())

        message = str(ctx.exception)
        self.assertEqual(message, _ERR_EXECUTION)
        self.assertNotIn(secret, message)
        self.assertIsNone(ctx.exception.__cause__)
        self.assertIsNone(ctx.exception.__context__)

    def test_injected_same_class_service_error_from_job_is_not_bare_reraised(self) -> None:
        stored = self._stored()
        spec = self._spec(stored)
        secret = "SECRET-INJECTED-JOB-SERVICE-ERROR-DO-NOT-LEAK-1f9a"
        injected = PinnedGCSRunServiceError(secret)
        spy = _JobSpy(raises=injected)

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError) as ctx:
                run_daily_pipeline_from_pinned_gcs_run_spec(spec, stored, **self._dependencies())

        self.assertIsNot(ctx.exception, injected)
        self.assertEqual(str(ctx.exception), _ERR_EXECUTION)
        self.assertNotIn(secret, str(ctx.exception))
        self.assertIsNone(ctx.exception.__cause__)
        self.assertIsNone(ctx.exception.__context__)

    def test_base_exception_from_job_is_not_intercepted(self) -> None:
        stored = self._stored()
        spec = self._spec(stored)
        spy = _JobSpy(raises=_MarkerBaseException("marker"))

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(_MarkerBaseException):
                run_daily_pipeline_from_pinned_gcs_run_spec(spec, stored, **self._dependencies())


class ServiceV2CalendarAcquisitionRejectionTests(PinnedGCSRunServiceTestCase):
    def test_rejects_wrong_payload_type_from_reader(self) -> None:
        _materialization, _payload, calendar_request, _fake_reader = _build_v2_fixture(self.root)
        spec = PinnedGCSRunSpec(**_valid_spec_kwargs_v2(calendar_request=calendar_request))

        class _BadPayloadReader:
            def read_generation(self, **_kwargs: object) -> str:
                return "not-a-payload"

        spy = _JobSpy(result=object())
        dependencies = {**self._dependencies(), "reader": _BadPayloadReader()}

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError) as ctx:
                run_daily_pipeline_from_pinned_gcs_run_spec(spec, None, **dependencies)

        self.assertEqual(str(ctx.exception), _ERR_CALENDAR_ACQUISITION)
        self.assertEqual(spy.calls, [])

    def test_rejects_bool_generation_from_reader(self) -> None:
        _materialization, payload, calendar_request, _fake_reader = _build_v2_fixture(
            self.root, generation=1
        )
        spec = PinnedGCSRunSpec(**_valid_spec_kwargs_v2(calendar_request=calendar_request))
        bad_reader = FakeGCSObjectReader(generation=True, content_bytes=payload)
        spy = _JobSpy(result=object())
        dependencies = {**self._dependencies(), "reader": bad_reader}

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError) as ctx:
                run_daily_pipeline_from_pinned_gcs_run_spec(spec, None, **dependencies)

        self.assertEqual(str(ctx.exception), _ERR_CALENDAR_ACQUISITION)
        self.assertEqual(spy.calls, [])

    def test_rejects_non_bytes_content_from_reader(self) -> None:
        _materialization, _payload, calendar_request, fake_reader = _build_v2_fixture(self.root)
        fake_reader.content_bytes = "not-bytes"
        spec = PinnedGCSRunSpec(**_valid_spec_kwargs_v2(calendar_request=calendar_request))
        spy = _JobSpy(result=object())
        dependencies = {**self._dependencies(), "reader": fake_reader}

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError) as ctx:
                run_daily_pipeline_from_pinned_gcs_run_spec(spec, None, **dependencies)

        self.assertEqual(str(ctx.exception), _ERR_CALENDAR_ACQUISITION)
        self.assertEqual(spy.calls, [])

    def test_rejects_empty_content_from_reader(self) -> None:
        _materialization, _payload, calendar_request, fake_reader = _build_v2_fixture(self.root)
        fake_reader.content_bytes = b""
        spec = PinnedGCSRunSpec(**_valid_spec_kwargs_v2(calendar_request=calendar_request))
        spy = _JobSpy(result=object())
        dependencies = {**self._dependencies(), "reader": fake_reader}

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError) as ctx:
                run_daily_pipeline_from_pinned_gcs_run_spec(spec, None, **dependencies)

        self.assertEqual(str(ctx.exception), _ERR_CALENDAR_ACQUISITION)
        self.assertEqual(spy.calls, [])

    def test_rejects_over_limit_content_via_patched_ceiling(self) -> None:
        _materialization, payload, calendar_request, fake_reader = _build_v2_fixture(self.root)
        spec = PinnedGCSRunSpec(**_valid_spec_kwargs_v2(calendar_request=calendar_request))
        spy = _JobSpy(result=object())
        dependencies = {**self._dependencies(), "reader": fake_reader}

        with patch(
            "india_swing.daily_pipeline.calendar_materialization_acquisition."
            "MAXIMUM_CALENDAR_MATERIALIZATION_BYTES",
            len(payload) - 1,
        ):
            with patch(_SERVICE_TARGET, spy):
                with self.assertRaises(PinnedGCSRunServiceError) as ctx:
                    run_daily_pipeline_from_pinned_gcs_run_spec(spec, None, **dependencies)

        self.assertEqual(str(ctx.exception), _ERR_CALENDAR_ACQUISITION)
        self.assertEqual(spy.calls, [])

    def test_rejects_stored_byte_hash_mismatch_from_reader(self) -> None:
        _materialization, payload, calendar_request, fake_reader = _build_v2_fixture(self.root)
        fake_reader.content_bytes = payload + b"tampered"
        spec = PinnedGCSRunSpec(**_valid_spec_kwargs_v2(calendar_request=calendar_request))
        spy = _JobSpy(result=object())
        dependencies = {**self._dependencies(), "reader": fake_reader}

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError) as ctx:
                run_daily_pipeline_from_pinned_gcs_run_spec(spec, None, **dependencies)

        self.assertEqual(str(ctx.exception), _ERR_CALENDAR_ACQUISITION)
        self.assertEqual(spy.calls, [])

    def test_rejects_malformed_noncanonical_bytes_from_reader(self) -> None:
        materialization, _payload, _calendar_request, _fake_reader = _build_v2_fixture(self.root)
        malformed = b"not-canonical-json-bytes"
        calendar_request = CalendarMaterializationObjectRequest(
            bucket=_CALENDAR_ACQUISITION_BUCKET,
            object_name=_calendar_object_name(materialization.materialization_id),
            generation=_CALENDAR_ACQUISITION_GENERATION,
            expected_sha256=hashlib.sha256(malformed).hexdigest(),
            materialization_id=materialization.materialization_id,
        )
        fake_reader = FakeGCSObjectReader(
            generation=_CALENDAR_ACQUISITION_GENERATION, content_bytes=malformed
        )
        spec = PinnedGCSRunSpec(**_valid_spec_kwargs_v2(calendar_request=calendar_request))
        spy = _JobSpy(result=object())
        dependencies = {**self._dependencies(), "reader": fake_reader}

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError) as ctx:
                run_daily_pipeline_from_pinned_gcs_run_spec(spec, None, **dependencies)

        self.assertEqual(str(ctx.exception), _ERR_CALENDAR_ACQUISITION)
        self.assertEqual(spy.calls, [])

    def test_rejects_wrong_type_acquisition_result(self) -> None:
        _materialization, _payload, calendar_request, fake_reader = _build_v2_fixture(self.root)
        spec = PinnedGCSRunSpec(**_valid_spec_kwargs_v2(calendar_request=calendar_request))
        spy = _JobSpy(result=object())
        dependencies = {**self._dependencies(), "reader": fake_reader}

        with patch(_ACQUIRE_TARGET, return_value="not-an-acquired-value"):
            with patch(_SERVICE_TARGET, spy):
                with self.assertRaises(PinnedGCSRunServiceError) as ctx:
                    run_daily_pipeline_from_pinned_gcs_run_spec(spec, None, **dependencies)

        self.assertEqual(str(ctx.exception), _ERR_CALENDAR)
        self.assertEqual(spy.calls, [])

    def test_rejects_acquisition_result_subclass(self) -> None:
        _materialization, _payload, calendar_request, fake_reader = _build_v2_fixture(self.root)
        spec = PinnedGCSRunSpec(**_valid_spec_kwargs_v2(calendar_request=calendar_request))
        real_acquired = acquire_calendar_materialization(calendar_request, reader=fake_reader)
        fake_reader.calls.clear()

        class _AcquiredSubclass(AcquiredCalendarMaterialization):
            pass

        subclass_instance = _AcquiredSubclass(
            request=real_acquired.request,
            observed_generation=real_acquired.observed_generation,
            observed_sha256=real_acquired.observed_sha256,
            materialization=real_acquired.materialization,
        )
        spy = _JobSpy(result=object())
        dependencies = {**self._dependencies(), "reader": fake_reader}

        with patch(_ACQUIRE_TARGET, return_value=subclass_instance):
            with patch(_SERVICE_TARGET, spy):
                with self.assertRaises(PinnedGCSRunServiceError) as ctx:
                    run_daily_pipeline_from_pinned_gcs_run_spec(spec, None, **dependencies)

        self.assertEqual(str(ctx.exception), _ERR_CALENDAR)
        self.assertEqual(spy.calls, [])

    def test_rejects_post_construction_mutated_acquisition_result(self) -> None:
        _materialization, _payload, calendar_request, fake_reader = _build_v2_fixture(self.root)
        spec = PinnedGCSRunSpec(**_valid_spec_kwargs_v2(calendar_request=calendar_request))
        real_acquired = acquire_calendar_materialization(calendar_request, reader=fake_reader)
        fake_reader.calls.clear()
        object.__setattr__(
            real_acquired, "observed_generation", real_acquired.observed_generation + 1
        )
        spy = _JobSpy(result=object())
        dependencies = {**self._dependencies(), "reader": fake_reader}

        with patch(_ACQUIRE_TARGET, return_value=real_acquired):
            with patch(_SERVICE_TARGET, spy):
                with self.assertRaises(PinnedGCSRunServiceError) as ctx:
                    run_daily_pipeline_from_pinned_gcs_run_spec(spec, None, **dependencies)

        self.assertEqual(str(ctx.exception), _ERR_CALENDAR)
        self.assertEqual(spy.calls, [])

    def test_rejects_acquisition_result_for_a_different_materialization(self) -> None:
        _materialization, _payload, calendar_request, fake_reader = _build_v2_fixture(self.root)
        spec = PinnedGCSRunSpec(**_valid_spec_kwargs_v2(calendar_request=calendar_request))
        (
            _other_materialization,
            _other_payload,
            other_request,
            other_reader,
        ) = _build_v2_fixture(self.root, document_id="CMTR-V2-OTHER")
        wrong_acquired = acquire_calendar_materialization(other_request, reader=other_reader)
        spy = _JobSpy(result=object())
        dependencies = {**self._dependencies(), "reader": fake_reader}

        with patch(_ACQUIRE_TARGET, return_value=wrong_acquired):
            with patch(_SERVICE_TARGET, spy):
                with self.assertRaises(PinnedGCSRunServiceError) as ctx:
                    run_daily_pipeline_from_pinned_gcs_run_spec(spec, None, **dependencies)

        self.assertEqual(str(ctx.exception), _ERR_CALENDAR)
        self.assertEqual(spy.calls, [])

    def test_missing_market_session_is_rejected(self) -> None:
        weekend = date(2026, 7, 18)  # Saturday: covered but not a session
        _materialization, _payload, calendar_request, fake_reader = _build_v2_fixture(
            self.root, session=weekend
        )
        spec = PinnedGCSRunSpec(
            **_valid_spec_kwargs_v2(calendar_request=calendar_request, _session=weekend)
        )
        spy = _JobSpy(result=object())
        dependencies = {**self._dependencies(), "reader": fake_reader}

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError) as ctx:
                run_daily_pipeline_from_pinned_gcs_run_spec(spec, None, **dependencies)

        self.assertEqual(str(ctx.exception), _ERR_CALENDAR)
        self.assertNotIn(weekend.isoformat(), str(ctx.exception))
        self.assertIsNone(ctx.exception.__cause__)
        self.assertIsNone(ctx.exception.__context__)
        self.assertEqual(spy.calls, [])

    def test_future_calendar_cutoff_is_rejected(self) -> None:
        _materialization, _payload, calendar_request, fake_reader = _build_v2_fixture(
            self.root, cutoff=_RUN_CUTOFF + timedelta(hours=1)
        )
        spec = PinnedGCSRunSpec(**_valid_spec_kwargs_v2(calendar_request=calendar_request))
        spy = _JobSpy(result=object())
        dependencies = {**self._dependencies(), "reader": fake_reader}

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError) as ctx:
                run_daily_pipeline_from_pinned_gcs_run_spec(spec, None, **dependencies)

        self.assertEqual(str(ctx.exception), _ERR_CALENDAR)
        self.assertEqual(spy.calls, [])

    def test_calendar_cutoff_equal_to_spec_cutoff_is_accepted(self) -> None:
        _materialization, _payload, calendar_request, fake_reader = _build_v2_fixture(
            self.root, cutoff=_RUN_CUTOFF
        )
        spec = PinnedGCSRunSpec(**_valid_spec_kwargs_v2(calendar_request=calendar_request))
        spy = _JobSpy(result=object())
        dependencies = {**self._dependencies(), "reader": fake_reader}

        with patch(_SERVICE_TARGET, spy):
            run_daily_pipeline_from_pinned_gcs_run_spec(spec, None, **dependencies)

        self.assertEqual(len(spy.calls), 1)

    def test_calendar_acquisition_failure_never_invokes_job_or_second_read(self) -> None:
        _materialization, _payload, calendar_request, fake_reader = _build_v2_fixture(self.root)
        fake_reader.content_bytes = b""
        spec = PinnedGCSRunSpec(**_valid_spec_kwargs_v2(calendar_request=calendar_request))
        spy = _JobSpy(result=object())
        dependencies = {**self._dependencies(), "reader": fake_reader}

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError):
                run_daily_pipeline_from_pinned_gcs_run_spec(spec, None, **dependencies)

        self.assertEqual(len(fake_reader.calls), 1)
        self.assertEqual(spy.calls, [])

    def test_base_exception_from_calendar_acquisition_is_not_intercepted(self) -> None:
        _materialization, _payload, calendar_request, _fake_reader = _build_v2_fixture(self.root)
        spec = PinnedGCSRunSpec(**_valid_spec_kwargs_v2(calendar_request=calendar_request))

        class _RaisingReader:
            def read_generation(self, **_kwargs: object) -> None:
                raise _MarkerBaseException("marker")

        spy = _JobSpy(result=object())
        dependencies = {**self._dependencies(), "reader": _RaisingReader()}

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(_MarkerBaseException):
                run_daily_pipeline_from_pinned_gcs_run_spec(spec, None, **dependencies)

        self.assertEqual(spy.calls, [])


class ServiceV2SanitizationTests(PinnedGCSRunServiceTestCase):
    def test_secret_bearing_reader_exception_is_sanitized(self) -> None:
        _materialization, _payload, calendar_request, _fake_reader = _build_v2_fixture(self.root)
        spec = PinnedGCSRunSpec(**_valid_spec_kwargs_v2(calendar_request=calendar_request))
        secret = "SECRET-SERVICE-READER-FAILURE-DO-NOT-LEAK-4d1a"

        class _DistinctiveReaderFailure(RuntimeError):
            pass

        class _RaisingReader:
            def read_generation(self, **_kwargs: object) -> None:
                raise _DistinctiveReaderFailure(secret)

        spy = _JobSpy(result=object())
        dependencies = {**self._dependencies(), "reader": _RaisingReader()}

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError) as ctx:
                run_daily_pipeline_from_pinned_gcs_run_spec(spec, None, **dependencies)

        self.assertEqual(str(ctx.exception), _ERR_CALENDAR_ACQUISITION)
        self.assertNotIn(secret, str(ctx.exception))
        self.assertNotIn(secret, repr(ctx.exception))
        self.assertNotIn("_DistinctiveReaderFailure", str(ctx.exception))
        self.assertIsNone(ctx.exception.__cause__)
        self.assertIsNone(ctx.exception.__context__)
        self.assertEqual(spy.calls, [])

    def test_injected_same_class_service_error_from_reader_is_not_bare_reraised(self) -> None:
        _materialization, _payload, calendar_request, _fake_reader = _build_v2_fixture(self.root)
        spec = PinnedGCSRunSpec(**_valid_spec_kwargs_v2(calendar_request=calendar_request))
        secret = "SECRET-INJECTED-SERVICE-ERROR-DO-NOT-LEAK-8f2b"
        injected = PinnedGCSRunServiceError(secret)

        class _RaisingReader:
            def read_generation(self, **_kwargs: object) -> None:
                raise injected

        spy = _JobSpy(result=object())
        dependencies = {**self._dependencies(), "reader": _RaisingReader()}

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError) as ctx:
                run_daily_pipeline_from_pinned_gcs_run_spec(spec, None, **dependencies)

        self.assertIsNot(ctx.exception, injected)
        self.assertEqual(str(ctx.exception), _ERR_CALENDAR_ACQUISITION)
        self.assertNotIn(secret, str(ctx.exception))
        self.assertIsNone(ctx.exception.__cause__)
        self.assertIsNone(ctx.exception.__context__)
        self.assertEqual(spy.calls, [])

    def test_injected_same_class_acquisition_error_from_reader_is_not_bare_reraised(self) -> None:
        _materialization, _payload, calendar_request, _fake_reader = _build_v2_fixture(self.root)
        spec = PinnedGCSRunSpec(**_valid_spec_kwargs_v2(calendar_request=calendar_request))
        secret = "SECRET-INJECTED-ACQUISITION-ERROR-DO-NOT-LEAK-3c7d"
        injected = CalendarMaterializationAcquisitionError(secret)

        class _RaisingReader:
            def read_generation(self, **_kwargs: object) -> None:
                raise injected

        spy = _JobSpy(result=object())
        dependencies = {**self._dependencies(), "reader": _RaisingReader()}

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError) as ctx:
                run_daily_pipeline_from_pinned_gcs_run_spec(spec, None, **dependencies)

        self.assertIsNot(ctx.exception, injected)
        self.assertEqual(str(ctx.exception), _ERR_CALENDAR_ACQUISITION)
        self.assertNotIn(secret, str(ctx.exception))
        self.assertIsNone(ctx.exception.__cause__)
        self.assertIsNone(ctx.exception.__context__)
        self.assertEqual(spy.calls, [])

    def test_secret_bearing_acquisition_result_validation_failure_is_sanitized(self) -> None:
        _materialization, _payload, calendar_request, fake_reader = _build_v2_fixture(self.root)
        spec = PinnedGCSRunSpec(**_valid_spec_kwargs_v2(calendar_request=calendar_request))
        real_acquired = acquire_calendar_materialization(calendar_request, reader=fake_reader)
        fake_reader.calls.clear()
        distinctive_bucket = "distinctive-mutated-bucket-do-not-leak-7e2a"
        object.__setattr__(real_acquired.request, "bucket", distinctive_bucket)

        spy = _JobSpy(result=object())
        dependencies = {**self._dependencies(), "reader": fake_reader}

        with patch(_ACQUIRE_TARGET, return_value=real_acquired):
            with patch(_SERVICE_TARGET, spy):
                with self.assertRaises(PinnedGCSRunServiceError) as ctx:
                    run_daily_pipeline_from_pinned_gcs_run_spec(spec, None, **dependencies)

        self.assertEqual(str(ctx.exception), _ERR_CALENDAR)
        self.assertNotIn(distinctive_bucket, str(ctx.exception))
        self.assertIsNone(ctx.exception.__cause__)
        self.assertIsNone(ctx.exception.__context__)
        self.assertEqual(spy.calls, [])

    def test_content_identity_failure_after_acquisition_is_sanitized(self) -> None:
        materialization, _payload, calendar_request, fake_reader = _build_v2_fixture(self.root)
        spec = PinnedGCSRunSpec(**_valid_spec_kwargs_v2(calendar_request=calendar_request))
        real_acquired = acquire_calendar_materialization(calendar_request, reader=fake_reader)
        fake_reader.calls.clear()
        object.__setattr__(real_acquired.materialization, "coverage_end", _OTHER_SESSION)

        spy = _JobSpy(result=object())
        dependencies = {**self._dependencies(), "reader": fake_reader}

        with patch(_ACQUIRE_TARGET, return_value=real_acquired):
            with patch(_SERVICE_TARGET, spy):
                with self.assertRaises(PinnedGCSRunServiceError) as ctx:
                    run_daily_pipeline_from_pinned_gcs_run_spec(spec, None, **dependencies)

        self.assertEqual(str(ctx.exception), _ERR_CALENDAR)
        self.assertNotIn(materialization.materialization_id, str(ctx.exception))
        self.assertIsNone(ctx.exception.__cause__)
        self.assertIsNone(ctx.exception.__context__)
        self.assertEqual(spy.calls, [])

    def test_secret_bearing_session_cutoff_failure_is_sanitized(self) -> None:
        weekend = date(2026, 7, 18)
        _materialization, _payload, calendar_request, fake_reader = _build_v2_fixture(
            self.root, session=weekend
        )
        spec = PinnedGCSRunSpec(
            **_valid_spec_kwargs_v2(calendar_request=calendar_request, _session=weekend)
        )
        spy = _JobSpy(result=object())
        dependencies = {**self._dependencies(), "reader": fake_reader}

        with patch(_SERVICE_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError) as ctx:
                run_daily_pipeline_from_pinned_gcs_run_spec(spec, None, **dependencies)

        self.assertEqual(str(ctx.exception), _ERR_CALENDAR)
        self.assertNotIn(weekend.isoformat(), str(ctx.exception))
        self.assertIsNone(ctx.exception.__cause__)
        self.assertIsNone(ctx.exception.__context__)
        self.assertEqual(spy.calls, [])


_EXACT_ALLOWED_SERVICE_IMPORTS = frozenset((
    # (level, module, imported name, asname). Closed set: any import in
    # pinned_gcs_run_service.py not exactly in this set, and any entry here
    # missing from that file, fails the equality assertion below.
    (0, "__future__", "annotations", None),
    (0, "india_swing.calendar_data.materialization", "CollectionCalendarMaterialization", None),
    (0, "india_swing.calendar_data.materialization_store", "CalendarMaterializationStoreManifest", None),
    (0, "india_swing.calendar_data.materialization_store", "StoredCalendarMaterialization", None),
    (0, "india_swing.daily_reports.artifact_store", "LocalDailyBundleArtifactStore", None),
    (0, "india_swing.historical_prices.artifact_store", "LocalHistoricalPriceArtifactStore", None),
    (0, "india_swing.identity_registry.adjudication_store", "LocalIdentityAdjudicationQueueStore", None),
    (0, "india_swing.identity_registry.artifact_store", "LocalIdentityRegistryStore", None),
    (0, "india_swing.reference.calendar", "CalendarSnapshot", None),
    (0, "india_swing.reference_data.artifact_store", "LocalReferenceArtifactStore", None),
    (1, "acquisition", "GCSObjectReader", None),
    (1, "calendar_materialization_acquisition", "AcquiredCalendarMaterialization", None),
    (1, "calendar_materialization_acquisition", "CalendarMaterializationObjectRequest", None),
    (1, "calendar_materialization_acquisition", "acquire_calendar_materialization", None),
    (1, "gcs_landing_job", "run_daily_pipeline_from_pinned_gcs_manifest", None),
    (1, "models", "DailyPipelineRun", None),
    (1, "pinned_gcs_run_spec", "PINNED_GCS_RUN_SPEC_SCHEMA_VERSION", None),
    (1, "pinned_gcs_run_spec", "PINNED_GCS_RUN_SPEC_SCHEMA_VERSION_WITH_CALENDAR", None),
    (1, "pinned_gcs_run_spec", "PinnedGCSRunSpec", None),
    (1, "store", "LocalDailyPipelineRunStore", None),
))

_EXACT_ALLOWED_SERVICE_CALL_TARGETS = frozenset((
    # The production module's entire callable surface: raising its own
    # error type, exact-type checks, reconstructing PinnedGCSRunSpec,
    # calling the three existing verify_content_identity() methods and
    # CalendarSnapshot.require_session, and invoking the one delegated job
    # function. Any other call name -- a GCS/storage client, os/pathlib,
    # requests/urllib, subprocess, a broker/order/notification helper, a
    # strategy/model/LLM call, a listing/"latest" helper, a retry/fallback
    # wrapper, or a CLI/scheduler/deployment hook -- fails this test.
    "PinnedGCSRunServiceError",
    "type",
    "PinnedGCSRunSpec",
    "verify_content_identity",
    "require_session",
    "run_daily_pipeline_from_pinned_gcs_manifest",
    "AcquiredCalendarMaterialization",
    "acquire_calendar_materialization",
))

_FORBIDDEN_SERVICE_NAME_TOKENS = (
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
    "llm",
    "scheduler",
    "deploy",
    "cli",
)


class PinnedGCSRunServiceCapabilityTests(unittest.TestCase):
    """Proves pinned_gcs_run_service.py introduces no filesystem,
    environment, current-clock, GCS/storage/client, listing/latest
    selection, retry/fallback, subprocess, notification, broker/order,
    strategy/model/LLM, scheduler, CLI, or deployment capability. Imports
    and the callable surface are both locked to exact closed sets.
    """

    def _module_ast(self) -> ast.Module:
        source = (
            Path(__file__).resolve().parent.parent
            / "src"
            / "india_swing"
            / "daily_pipeline"
            / "pinned_gcs_run_service.py"
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
        self.assertEqual(actual, _EXACT_ALLOWED_SERVICE_IMPORTS)

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
            if target not in _EXACT_ALLOWED_SERVICE_CALL_TARGETS:
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
            for token in _FORBIDDEN_SERVICE_NAME_TOKENS:
                if token in candidate:
                    offenders.append(candidate)
        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
