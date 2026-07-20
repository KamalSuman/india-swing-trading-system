from __future__ import annotations

import ast
import hashlib
import json
import tempfile
import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from india_swing.calendar_data.artifact_store import LocalCalendarSourceArtifactStore
from india_swing.calendar_data.materialization import (
    CollectionCalendarMaterialization,
    materialize_collection_calendar,
)
from india_swing.calendar_data.materialization_codec import encode_calendar_materialization
from india_swing.calendar_data.materialization_store import (
    CALENDAR_MATERIALIZATION_STORE_DATASET,
    LocalCalendarMaterializationStore,
    StoredCalendarMaterialization,
)
from india_swing.calendar_data.models import CALENDAR_DECLARATION_SCHEMA_VERSION
from india_swing.daily_pipeline.acquisition import GCSObjectPayload
from india_swing.daily_pipeline.calendar_materialization_acquisition import (
    CalendarMaterializationObjectRequest,
)
from india_swing.daily_pipeline.pinned_gcs_run_file_boundary import (
    PinnedGCSRunFileBoundaryError,
    load_pinned_gcs_run_spec_file,
    run_daily_pipeline_from_pinned_gcs_run_spec_file,
)
from india_swing.daily_pipeline.pinned_gcs_run_service import PinnedGCSRunServiceError
from india_swing.daily_pipeline.pinned_gcs_run_spec import (
    MAXIMUM_PINNED_GCS_RUN_SPEC_BYTES,
    PINNED_GCS_RUN_SPEC_SCHEMA_VERSION,
    PINNED_GCS_RUN_SPEC_SCHEMA_VERSION_WITH_CALENDAR,
    PinnedGCSRunSpec,
)


UTC = timezone.utc
_SESSION = date(2026, 7, 20)  # Monday
_SOURCE_VALIDATED = datetime(2026, 7, 15, 13, 0, 0, tzinfo=UTC)
_CALENDAR_CUTOFF = datetime(2026, 7, 20, 12, 0, 0, tzinfo=UTC)
_BUCKET = "trusted-run-file-boundary-bucket"
_SHA256_HEX = "a" * 64
_PREVIOUS_RUN_ID = "c" * 64
_NOT_BEFORE = "2026-07-20T00:00:00Z"
_BINDING_CUTOFF = "2026-07-20T14:00:00Z"
_RUN_CUTOFF = "2026-07-20T15:00:00Z"
_CALENDAR_ACQUISITION_BUCKET = "trusted-calendar-materialization-bucket"
_CALENDAR_ACQUISITION_GENERATION = 999
_ERR_LOAD = "pinned gcs run spec file could not be loaded"
_ERR_SCHEMA = "pinned gcs run file boundary schema routing failed"
_ERR_CALENDAR = "pinned gcs run file boundary calendar acquisition failed"

_BOUNDARY_TARGET = (
    "india_swing.daily_pipeline.pinned_gcs_run_file_boundary."
    "run_daily_pipeline_from_pinned_gcs_run_spec"
)
_LOAD_TARGET = (
    "india_swing.daily_pipeline.pinned_gcs_run_file_boundary.load_pinned_gcs_run_spec_file"
)


def _manifest_object_name(session: date = _SESSION) -> str:
    return f"landing/{session.isoformat()}/landing-manifest.json"


def _valid_spec_dict(
    *,
    calendar_materialization_id: str,
    session: date = _SESSION,
    previous_run_id: object = None,
) -> dict[str, object]:
    return {
        "schema_version": PINNED_GCS_RUN_SPEC_SCHEMA_VERSION,
        "manifest_request": {
            "bucket": _BUCKET,
            "object_name": _manifest_object_name(session),
            "generation": 777,
            "target_session": session.isoformat(),
        },
        "trusted_binding": {
            "expected_manifest_sha256": _SHA256_HEX,
            "allowed_bucket": _BUCKET,
            "target_session": session.isoformat(),
            "not_before": _NOT_BEFORE,
            "cutoff": _BINDING_CUTOFF,
        },
        "run": {
            "market_session": session.isoformat(),
            "cutoff": _RUN_CUTOFF,
            "calendar_materialization_id": calendar_materialization_id,
            "previous_run_id": previous_run_id,
        },
    }


def _spec_bytes(**kwargs: object) -> bytes:
    return json.dumps(_valid_spec_dict(**kwargs), separators=(",", ":")).encode("utf-8")


def _calendar_object_name(materialization_id: str) -> str:
    return f"calendar-materializations/{materialization_id}/materialization.json"


def _calendar_request_dict(
    materialization: CollectionCalendarMaterialization, payload: bytes
) -> dict[str, object]:
    return {
        "bucket": _CALENDAR_ACQUISITION_BUCKET,
        "object_name": _calendar_object_name(materialization.materialization_id),
        "generation": _CALENDAR_ACQUISITION_GENERATION,
        "expected_sha256": hashlib.sha256(payload).hexdigest(),
        "materialization_id": materialization.materialization_id,
    }


def _valid_spec_dict_v2(
    *,
    calendar_request: dict[str, object],
    session: date = _SESSION,
    previous_run_id: object = None,
) -> dict[str, object]:
    return {
        "schema_version": PINNED_GCS_RUN_SPEC_SCHEMA_VERSION_WITH_CALENDAR,
        "manifest_request": {
            "bucket": _BUCKET,
            "object_name": _manifest_object_name(session),
            "generation": 777,
            "target_session": session.isoformat(),
        },
        "trusted_binding": {
            "expected_manifest_sha256": _SHA256_HEX,
            "allowed_bucket": _BUCKET,
            "target_session": session.isoformat(),
            "not_before": _NOT_BEFORE,
            "cutoff": _BINDING_CUTOFF,
        },
        "calendar_request": calendar_request,
        "run": {
            "market_session": session.isoformat(),
            "cutoff": _RUN_CUTOFF,
            "calendar_materialization_id": calendar_request["materialization_id"],
            "previous_run_id": previous_run_id,
        },
    }


def _spec_bytes_v2(**kwargs: object) -> bytes:
    return json.dumps(_valid_spec_dict_v2(**kwargs), separators=(",", ":")).encode("utf-8")


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


def _build_materialization(store_root: Path, inputs_root: Path) -> CollectionCalendarMaterialization:
    # LocalCalendarMaterializationStore._replay reconstructs
    # LocalCalendarSourceArtifactStore(self.root), so the calendar source
    # must be imported into the same root the materialization store uses,
    # not a separate directory.
    source = _import_base_source(store_root, inputs_root)
    return materialize_collection_calendar(
        sources=(source,),
        coverage_start=_SESSION,
        coverage_end=_SESSION,
        cutoff=_CALENDAR_CUTOFF,
        observed_date_artifacts=(),
    )


def _build_v2_fixture(
    root: Path, *, document_id: str = "CMTR-V2-BASE"
) -> tuple[CollectionCalendarMaterialization, bytes, dict[str, object], FakeGCSObjectReader]:
    """Real, canonically-encoded materialization plus a matching
    generation-pinned calendar_request dict and an injected fake reader
    ready to serve it. Schema v2 never uses a LocalCalendarMaterializationStore,
    so this builds the materialization directly rather than via
    _put_stored_materialization."""

    materialization = _build_materialization(root / document_id, root / "inputs" / document_id)
    payload = encode_calendar_materialization(materialization)
    calendar_request = _calendar_request_dict(materialization, payload)
    fake_reader = FakeGCSObjectReader(
        generation=_CALENDAR_ACQUISITION_GENERATION, content_bytes=payload
    )
    return materialization, payload, calendar_request, fake_reader


def _put_stored_materialization(root: Path) -> tuple[LocalCalendarMaterializationStore, StoredCalendarMaterialization]:
    store_root = root / "calendar-materializations"
    store = LocalCalendarMaterializationStore(
        root=store_root,
        daily_reports_root=root / "daily-reports",
    )
    materialization = _build_materialization(store_root, root / "inputs" / "CMTR-BASE-2026")
    stored = store.put(materialization)
    return store, stored


class _JobSpy:
    def __init__(self, *, result: object = None, raises: BaseException | None = None) -> None:
        self.calls: list[dict[str, object]] = []
        self._result = result
        self._raises = raises

    def __call__(self, *args: object, **kwargs: object) -> object:
        self.calls.append({"args": args, "kwargs": kwargs})
        if self._raises is not None:
            raise self._raises
        return self._result


class _MarkerBaseException(BaseException):
    pass


class PinnedGCSRunFileBoundaryTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.calendar_store, self.stored = _put_stored_materialization(self.root)
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
            calendar_store=self.calendar_store,
            reader=self.reader,
            reference_store=self.reference_store,
            daily_store=self.daily_store,
            historical_store=self.historical_store,
            identity_store=self.identity_store,
            adjudication_store=self.adjudication_store,
            run_store=self.run_store,
        )

    def _write_spec(self, name: str = "spec.json", **kwargs: object) -> Path:
        kwargs.setdefault("calendar_materialization_id", self.stored.manifest.artifact_id)
        path = self.root / name
        path.write_bytes(_spec_bytes(**kwargs))
        return path


class LoadAcceptanceTests(PinnedGCSRunFileBoundaryTestCase):
    def test_loads_valid_spec_file(self) -> None:
        path = self._write_spec()

        spec = load_pinned_gcs_run_spec_file(str(path))

        self.assertIsInstance(spec, PinnedGCSRunSpec)
        self.assertEqual(spec.calendar_materialization_id, self.stored.manifest.artifact_id)

    def test_accepts_content_exactly_at_the_byte_limit(self) -> None:
        base = _spec_bytes(calendar_materialization_id=self.stored.manifest.artifact_id)
        pad_length = MAXIMUM_PINNED_GCS_RUN_SPEC_BYTES - len(base)
        self.assertGreaterEqual(pad_length, 0)
        padded = base + b" " * pad_length
        self.assertEqual(len(padded), MAXIMUM_PINNED_GCS_RUN_SPEC_BYTES)
        path = self.root / "padded.json"
        path.write_bytes(padded)

        spec = load_pinned_gcs_run_spec_file(str(path))

        self.assertIsInstance(spec, PinnedGCSRunSpec)

    def test_loads_valid_schema_v2_spec_file_with_exact_calendar_request(self) -> None:
        materialization, _payload, calendar_request, _fake_reader = _build_v2_fixture(self.root)
        path = self.root / "spec-v2.json"
        path.write_bytes(_spec_bytes_v2(calendar_request=calendar_request))

        spec = load_pinned_gcs_run_spec_file(str(path))

        self.assertIsInstance(spec, PinnedGCSRunSpec)
        self.assertEqual(spec.schema_version, PINNED_GCS_RUN_SPEC_SCHEMA_VERSION_WITH_CALENDAR)
        self.assertIsInstance(spec.calendar_request, CalendarMaterializationObjectRequest)
        self.assertEqual(spec.calendar_request.materialization_id, materialization.materialization_id)
        self.assertEqual(spec.calendar_request.bucket, _CALENDAR_ACQUISITION_BUCKET)
        self.assertEqual(spec.calendar_materialization_id, materialization.materialization_id)


class LoadRejectionTests(PinnedGCSRunFileBoundaryTestCase):
    def test_rejects_non_str_path_object(self) -> None:
        path = self._write_spec()
        with self.assertRaises(PinnedGCSRunFileBoundaryError):
            load_pinned_gcs_run_spec_file(path)  # type: ignore[arg-type]

    def test_rejects_bytes_path(self) -> None:
        path = self._write_spec()
        with self.assertRaises(PinnedGCSRunFileBoundaryError):
            load_pinned_gcs_run_spec_file(str(path).encode("utf-8"))  # type: ignore[arg-type]

    def test_rejects_empty_string(self) -> None:
        with self.assertRaises(PinnedGCSRunFileBoundaryError):
            load_pinned_gcs_run_spec_file("")

    def test_rejects_nul_embedded_path(self) -> None:
        with self.assertRaises(PinnedGCSRunFileBoundaryError):
            load_pinned_gcs_run_spec_file("some\x00path.json")

    def test_rejects_missing_file(self) -> None:
        with self.assertRaises(PinnedGCSRunFileBoundaryError):
            load_pinned_gcs_run_spec_file(str(self.root / "does-not-exist.json"))

    def test_rejects_directory_path(self) -> None:
        directory = self.root / "a-directory"
        directory.mkdir()
        with self.assertRaises(PinnedGCSRunFileBoundaryError):
            load_pinned_gcs_run_spec_file(str(directory))

    def test_rejects_symlink_to_valid_spec_file(self) -> None:
        path = self._write_spec()
        link = self.root / "spec-link.json"
        try:
            link.symlink_to(path)
        except (OSError, NotImplementedError):
            self.skipTest("platform cannot create symlinks")
        with self.assertRaises(PinnedGCSRunFileBoundaryError):
            load_pinned_gcs_run_spec_file(str(link))

    def test_rejects_file_one_byte_over_the_limit(self) -> None:
        oversized = b" " * (MAXIMUM_PINNED_GCS_RUN_SPEC_BYTES + 1)
        path = self.root / "oversized.json"
        path.write_bytes(oversized)
        with self.assertRaises(PinnedGCSRunFileBoundaryError):
            load_pinned_gcs_run_spec_file(str(path))

    def test_rejects_larger_file(self) -> None:
        larger = b" " * (MAXIMUM_PINNED_GCS_RUN_SPEC_BYTES * 2)
        path = self.root / "larger.json"
        path.write_bytes(larger)
        with self.assertRaises(PinnedGCSRunFileBoundaryError):
            load_pinned_gcs_run_spec_file(str(path))

    def test_rejects_malformed_json(self) -> None:
        path = self.root / "malformed.json"
        path.write_bytes(b"{not-json")
        with self.assertRaises(PinnedGCSRunFileBoundaryError):
            load_pinned_gcs_run_spec_file(str(path))

    def test_rejects_structurally_valid_json_failing_spec_schema(self) -> None:
        path = self.root / "wrong-schema.json"
        path.write_bytes(json.dumps({"unexpected": "shape"}).encode("utf-8"))
        with self.assertRaises(PinnedGCSRunFileBoundaryError):
            load_pinned_gcs_run_spec_file(str(path))

    def test_secret_bearing_filesystem_failure_never_leaks(self) -> None:
        secret = "SECRET-FILE-BOUNDARY-OSERROR-DO-NOT-LEAK-91ad"

        class _DistinctiveFilesystemFailure(OSError):
            pass

        class _RaisingPath(type(Path())):
            def lstat(self, *args: object, **kwargs: object) -> object:
                raise _DistinctiveFilesystemFailure(secret)

        with self.assertRaises(PinnedGCSRunFileBoundaryError) as ctx:
            with patch(
                "india_swing.daily_pipeline.pinned_gcs_run_file_boundary.Path",
                _RaisingPath,
            ):
                load_pinned_gcs_run_spec_file(str(self.root / "irrelevant.json"))

        message = str(ctx.exception)
        self.assertEqual(message, _ERR_LOAD)
        self.assertNotIn(secret, message)
        self.assertNotIn("_DistinctiveFilesystemFailure", message)
        self.assertIsNone(ctx.exception.__cause__)
        self.assertIsNone(ctx.exception.__context__)

    def test_injected_same_class_error_from_filesystem_is_not_bare_reraised(self) -> None:
        secret = "SECRET-INJECTED-FILE-BOUNDARY-ERROR-DO-NOT-LEAK-6a2f"
        injected = PinnedGCSRunFileBoundaryError(secret)

        class _RaisingPath(type(Path())):
            def lstat(self, *args: object, **kwargs: object) -> object:
                raise injected

        with self.assertRaises(PinnedGCSRunFileBoundaryError) as ctx:
            with patch(
                "india_swing.daily_pipeline.pinned_gcs_run_file_boundary.Path",
                _RaisingPath,
            ):
                load_pinned_gcs_run_spec_file(str(self.root / "irrelevant.json"))

        self.assertIsNot(ctx.exception, injected)
        self.assertEqual(str(ctx.exception), _ERR_LOAD)
        self.assertNotIn(secret, str(ctx.exception))
        self.assertIsNone(ctx.exception.__cause__)
        self.assertIsNone(ctx.exception.__context__)


class DelegationTests(PinnedGCSRunFileBoundaryTestCase):
    def test_success_calls_delegated_service_exactly_once_with_exact_arguments(self) -> None:
        path = self._write_spec()
        sentinel = object()
        spy = _JobSpy(result=sentinel)

        with patch.object(self.calendar_store, "get", return_value=self.stored):
            with patch(_BOUNDARY_TARGET, spy):
                result = run_daily_pipeline_from_pinned_gcs_run_spec_file(
                    str(path), **self._dependencies()
                )

        self.assertIs(result, sentinel)
        self.assertEqual(len(spy.calls), 1)
        call = spy.calls[0]
        args = call["args"]
        kwargs = call["kwargs"]
        self.assertEqual(len(args), 2)
        spec_arg, calendar_arg = args
        self.assertIsInstance(spec_arg, PinnedGCSRunSpec)
        self.assertEqual(spec_arg.calendar_materialization_id, self.stored.manifest.artifact_id)
        self.assertIsNone(spec_arg.previous_run_id)
        self.assertIs(calendar_arg, self.stored)
        self.assertEqual(len(kwargs), 7)
        self.assertIs(kwargs["reader"], self.reader)
        self.assertIs(kwargs["reference_store"], self.reference_store)
        self.assertIs(kwargs["daily_store"], self.daily_store)
        self.assertIs(kwargs["historical_store"], self.historical_store)
        self.assertIs(kwargs["identity_store"], self.identity_store)
        self.assertIs(kwargs["adjudication_store"], self.adjudication_store)
        self.assertIs(kwargs["run_store"], self.run_store)

    def test_success_with_non_null_previous_run_id(self) -> None:
        path = self._write_spec(name="spec-prev.json", previous_run_id=_PREVIOUS_RUN_ID)
        spy = _JobSpy(result=object())

        with patch.object(self.calendar_store, "get", return_value=self.stored):
            with patch(_BOUNDARY_TARGET, spy):
                run_daily_pipeline_from_pinned_gcs_run_spec_file(
                    str(path), **self._dependencies()
                )

        self.assertEqual(len(spy.calls), 1)
        spec_arg = spy.calls[0]["args"][0]
        self.assertEqual(spec_arg.previous_run_id, _PREVIOUS_RUN_ID)

    def test_calendar_store_get_is_called_exactly_once_with_exact_id(self) -> None:
        path = self._write_spec()
        spy = _JobSpy(result=object())
        original_get = self.calendar_store.get
        calls: list[str] = []

        def _recording_get(artifact_id: str) -> object:
            calls.append(artifact_id)
            return original_get(artifact_id)

        with patch.object(self.calendar_store, "get", side_effect=_recording_get):
            with patch(_BOUNDARY_TARGET, spy):
                run_daily_pipeline_from_pinned_gcs_run_spec_file(
                    str(path), **self._dependencies()
                )

        self.assertEqual(calls, [self.stored.manifest.artifact_id])

    def test_wrong_calendar_store_type_is_rejected_before_get_call(self) -> None:
        path = self._write_spec()
        deps = self._dependencies()
        deps["calendar_store"] = object()
        spy = _JobSpy(result=object())

        with patch(_BOUNDARY_TARGET, spy):
            with self.assertRaises(PinnedGCSRunFileBoundaryError):
                run_daily_pipeline_from_pinned_gcs_run_spec_file(str(path), **deps)

        self.assertEqual(spy.calls, [])

    def test_calendar_store_subclass_is_rejected_before_get_call(self) -> None:
        class _StoreSubclass(LocalCalendarMaterializationStore):
            pass

        subclass_store = _StoreSubclass(
            root=self.root / "calendar-materializations",
            daily_reports_root=self.root / "daily-reports",
        )
        path = self._write_spec()
        deps = self._dependencies()
        deps["calendar_store"] = subclass_store
        spy = _JobSpy(result=object())

        with patch(_BOUNDARY_TARGET, spy):
            with self.assertRaises(PinnedGCSRunFileBoundaryError):
                run_daily_pipeline_from_pinned_gcs_run_spec_file(str(path), **deps)

        self.assertEqual(spy.calls, [])

    def test_unknown_calendar_id_against_real_empty_store_is_rejected(self) -> None:
        empty_store = LocalCalendarMaterializationStore(
            root=self.root / "empty-materializations",
            daily_reports_root=self.root / "empty-daily-reports",
        )
        path = self._write_spec()
        deps = self._dependencies()
        deps["calendar_store"] = empty_store
        spy = _JobSpy(result=object())

        with patch(_BOUNDARY_TARGET, spy):
            with self.assertRaises(PinnedGCSRunFileBoundaryError):
                run_daily_pipeline_from_pinned_gcs_run_spec_file(str(path), **deps)

        self.assertEqual(spy.calls, [])

    def test_secret_bearing_calendar_store_get_failure_never_leaks(self) -> None:
        secret = "SECRET-CALENDAR-STORE-GET-FAILURE-DO-NOT-LEAK-4c1f"
        path = self._write_spec()
        deps = self._dependencies()

        class _DistinctiveGetFailure(RuntimeError):
            pass

        with patch.object(self.calendar_store, "get", side_effect=_DistinctiveGetFailure(secret)):
            with self.assertRaises(PinnedGCSRunFileBoundaryError) as ctx:
                run_daily_pipeline_from_pinned_gcs_run_spec_file(str(path), **deps)

        message = str(ctx.exception)
        self.assertEqual(message, _ERR_CALENDAR)
        self.assertNotIn(secret, message)
        self.assertNotIn("_DistinctiveGetFailure", message)
        self.assertIsNone(ctx.exception.__cause__)
        self.assertIsNone(ctx.exception.__context__)

    def test_injected_same_class_error_from_calendar_store_get_is_not_bare_reraised(self) -> None:
        secret = "SECRET-INJECTED-CALENDAR-GET-ERROR-DO-NOT-LEAK-7d3e"
        injected = PinnedGCSRunFileBoundaryError(secret)
        path = self._write_spec()
        deps = self._dependencies()

        with patch.object(self.calendar_store, "get", side_effect=injected):
            with self.assertRaises(PinnedGCSRunFileBoundaryError) as ctx:
                run_daily_pipeline_from_pinned_gcs_run_spec_file(str(path), **deps)

        self.assertIsNot(ctx.exception, injected)
        self.assertEqual(str(ctx.exception), _ERR_CALENDAR)
        self.assertNotIn(secret, str(ctx.exception))
        self.assertIsNone(ctx.exception.__cause__)
        self.assertIsNone(ctx.exception.__context__)

    def test_v1_calendar_store_failure_calls_get_once_and_never_delegates(self) -> None:
        path = self._write_spec()
        spy = _JobSpy(result=object())
        calls: list[str] = []

        def _failing_get(artifact_id: str) -> object:
            calls.append(artifact_id)
            raise RuntimeError("boom")

        with patch.object(self.calendar_store, "get", side_effect=_failing_get):
            with patch(_BOUNDARY_TARGET, spy):
                with self.assertRaises(PinnedGCSRunFileBoundaryError):
                    run_daily_pipeline_from_pinned_gcs_run_spec_file(
                        str(path), **self._dependencies()
                    )

        self.assertEqual(len(calls), 1)
        self.assertEqual(spy.calls, [])

    def test_v1_with_none_calendar_store_is_rejected_before_get_call(self) -> None:
        path = self._write_spec()
        deps = self._dependencies()
        deps["calendar_store"] = None
        spy = _JobSpy(result=object())

        with patch(_BOUNDARY_TARGET, spy):
            with self.assertRaises(PinnedGCSRunFileBoundaryError):
                run_daily_pipeline_from_pinned_gcs_run_spec_file(str(path), **deps)

        self.assertEqual(spy.calls, [])

    def test_wrong_loaded_spec_type_is_rejected_before_get_or_service(self) -> None:
        path = self._write_spec()
        spy = _JobSpy(result=object())

        with patch(_LOAD_TARGET, return_value="not-a-spec"):
            with patch.object(self.calendar_store, "get") as get_spy:
                with patch(_BOUNDARY_TARGET, spy):
                    with self.assertRaises(PinnedGCSRunFileBoundaryError):
                        run_daily_pipeline_from_pinned_gcs_run_spec_file(
                            str(path), **self._dependencies()
                        )

        get_spy.assert_not_called()
        self.assertEqual(spy.calls, [])

    def test_loaded_spec_subclass_is_rejected_before_get_or_service(self) -> None:
        path = self._write_spec()
        real_spec = load_pinned_gcs_run_spec_file(str(path))

        class _SpecSubclass(PinnedGCSRunSpec):
            pass

        subclass_instance = _SpecSubclass(
            schema_version=real_spec.schema_version,
            manifest_request=real_spec.manifest_request,
            trusted_binding=real_spec.trusted_binding,
            market_session=real_spec.market_session,
            cutoff=real_spec.cutoff,
            calendar_materialization_id=real_spec.calendar_materialization_id,
            previous_run_id=real_spec.previous_run_id,
        )
        spy = _JobSpy(result=object())

        with patch(_LOAD_TARGET, return_value=subclass_instance):
            with patch.object(self.calendar_store, "get") as get_spy:
                with patch(_BOUNDARY_TARGET, spy):
                    with self.assertRaises(PinnedGCSRunFileBoundaryError):
                        run_daily_pipeline_from_pinned_gcs_run_spec_file(
                            str(path), **self._dependencies()
                        )

        get_spy.assert_not_called()
        self.assertEqual(spy.calls, [])

    def test_shaped_loaded_spec_proxy_is_rejected_before_get_or_service(self) -> None:
        path = self._write_spec()
        real_spec = load_pinned_gcs_run_spec_file(str(path))

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
                    "calendar_request",
                ):
                    setattr(self, name, getattr(real_spec, name))

            def __eq__(self, other: object) -> bool:
                return True

            def __hash__(self) -> int:
                return 0

        spy = _JobSpy(result=object())

        with patch(_LOAD_TARGET, return_value=_ShapedSpecProxy()):
            with patch.object(self.calendar_store, "get") as get_spy:
                with patch(_BOUNDARY_TARGET, spy):
                    with self.assertRaises(PinnedGCSRunFileBoundaryError):
                        run_daily_pipeline_from_pinned_gcs_run_spec_file(
                            str(path), **self._dependencies()
                        )

        get_spy.assert_not_called()
        self.assertEqual(spy.calls, [])

    def test_unsupported_schema_version_is_rejected_before_get_or_service(self) -> None:
        path = self._write_spec()
        real_spec = load_pinned_gcs_run_spec_file(str(path))
        object.__setattr__(real_spec, "schema_version", 3)
        spy = _JobSpy(result=object())

        with patch(_LOAD_TARGET, return_value=real_spec):
            with patch.object(self.calendar_store, "get") as get_spy:
                with patch(_BOUNDARY_TARGET, spy):
                    with self.assertRaises(PinnedGCSRunFileBoundaryError):
                        run_daily_pipeline_from_pinned_gcs_run_spec_file(
                            str(path), **self._dependencies()
                        )

        get_spy.assert_not_called()
        self.assertEqual(spy.calls, [])

    def _assert_mutation_rejected_before_get_or_service(
        self, mutate: object, path: object | None = None
    ) -> None:
        # Shared helper for every "mutate the loaded spec, then prove the
        # reconstruction step (not ordinary equality) rejects it before
        # any get/service call" regression below.
        if path is None:
            path = self._write_spec()
        real_spec = load_pinned_gcs_run_spec_file(str(path))
        mutate(real_spec)
        spy = _JobSpy(result=object())

        with patch(_LOAD_TARGET, return_value=real_spec):
            with patch.object(self.calendar_store, "get") as get_spy:
                with patch(_BOUNDARY_TARGET, spy):
                    with self.assertRaises(PinnedGCSRunFileBoundaryError):
                        run_daily_pipeline_from_pinned_gcs_run_spec_file(
                            str(path), **self._dependencies()
                        )

        get_spy.assert_not_called()
        self.assertEqual(spy.calls, [])

    def test_schema_version_mutated_to_bool_true_is_rejected(self) -> None:
        # Regression for Codex's exact repro: True == 1 == PINNED_GCS_RUN_
        # SPEC_SCHEMA_VERSION under ordinary equality, so without the
        # reconstruction fix this would have incorrectly routed as v1 and
        # reached calendar_store.get.
        self._assert_mutation_rejected_before_get_or_service(
            lambda spec: object.__setattr__(spec, "schema_version", True)
        )

    def test_schema_version_mutated_to_bool_false_is_rejected(self) -> None:
        self._assert_mutation_rejected_before_get_or_service(
            lambda spec: object.__setattr__(spec, "schema_version", False)
        )

    def test_schema_version_mutated_to_int_subclass_is_rejected(self) -> None:
        class _IntSubclass(int):
            pass

        self._assert_mutation_rejected_before_get_or_service(
            lambda spec: object.__setattr__(
                spec, "schema_version", _IntSubclass(PINNED_GCS_RUN_SPEC_SCHEMA_VERSION)
            )
        )

    def test_schema_version_mutated_to_equality_poisoned_object_is_rejected(self) -> None:
        class _PoisonedEquality:
            def __eq__(self, other: object) -> bool:
                return True

            def __hash__(self) -> int:
                return 0

        self._assert_mutation_rejected_before_get_or_service(
            lambda spec: object.__setattr__(spec, "schema_version", _PoisonedEquality())
        )

    def test_mutated_calendar_materialization_id_is_rejected_before_get(self) -> None:
        self._assert_mutation_rejected_before_get_or_service(
            lambda spec: object.__setattr__(spec, "calendar_materialization_id", "not-hex")
        )

    def test_mutated_manifest_request_field_is_rejected_before_get(self) -> None:
        self._assert_mutation_rejected_before_get_or_service(
            lambda spec: object.__setattr__(spec.manifest_request, "generation", -1)
        )

    def test_mutated_trusted_binding_field_is_rejected_before_get(self) -> None:
        self._assert_mutation_rejected_before_get_or_service(
            lambda spec: object.__setattr__(spec.trusted_binding, "allowed_bucket", "")
        )

    def test_mutated_run_cutoff_is_rejected_before_get(self) -> None:
        self._assert_mutation_rejected_before_get_or_service(
            lambda spec: object.__setattr__(spec, "cutoff", datetime(2020, 1, 1))
        )

    def test_mutated_market_session_is_rejected_before_get(self) -> None:
        self._assert_mutation_rejected_before_get_or_service(
            lambda spec: object.__setattr__(spec, "market_session", "not-a-date")
        )

    def test_mutated_previous_run_id_is_rejected_before_get(self) -> None:
        self._assert_mutation_rejected_before_get_or_service(
            lambda spec: object.__setattr__(spec, "previous_run_id", "not-hex")
        )

    def test_mutated_v2_calendar_request_is_rejected_before_service(self) -> None:
        _materialization, _payload, calendar_request, fake_reader = _build_v2_fixture(self.root)
        path = self.root / "spec-v2-mutated-request.json"
        path.write_bytes(_spec_bytes_v2(calendar_request=calendar_request))
        real_spec = load_pinned_gcs_run_spec_file(str(path))
        object.__setattr__(real_spec.calendar_request, "materialization_id", "f" * 64)
        spy = _JobSpy(result=object())
        deps = self._dependencies()
        deps["calendar_store"] = None
        deps["reader"] = fake_reader

        with patch(_LOAD_TARGET, return_value=real_spec):
            with patch(_BOUNDARY_TARGET, spy):
                with self.assertRaises(PinnedGCSRunFileBoundaryError):
                    run_daily_pipeline_from_pinned_gcs_run_spec_file(str(path), **deps)

        self.assertEqual(spy.calls, [])
        self.assertEqual(fake_reader.calls, [])

    def test_snapshot_isolation_from_mutated_loaded_spec(self) -> None:
        # A hostile/buggy calendar_store.get() that mutates the ORIGINAL
        # loaded spec object as a side effect must not affect the spec the
        # delegated service receives -- it is built from an independently
        # reconstructed pre-mutation snapshot, not the caller's object.
        path = self._write_spec()
        loaded_spec = load_pinned_gcs_run_spec_file(str(path))
        original_id = loaded_spec.calendar_materialization_id
        original_bucket = loaded_spec.manifest_request.bucket
        spy = _JobSpy(result=object())

        def _mutating_get(artifact_id: str) -> StoredCalendarMaterialization:
            object.__setattr__(loaded_spec, "calendar_materialization_id", "f" * 64)
            object.__setattr__(loaded_spec.manifest_request, "bucket", "mutated-bucket")
            return self.stored

        with patch(_LOAD_TARGET, return_value=loaded_spec):
            with patch.object(self.calendar_store, "get", side_effect=_mutating_get) as get_spy:
                with patch(_BOUNDARY_TARGET, spy):
                    run_daily_pipeline_from_pinned_gcs_run_spec_file(
                        str(path), **self._dependencies()
                    )

        get_spy.assert_called_once_with(original_id)
        self.assertEqual(len(spy.calls), 1)
        spec_arg = spy.calls[0]["args"][0]
        self.assertIsNot(spec_arg, loaded_spec)
        self.assertEqual(spec_arg.calendar_materialization_id, original_id)
        self.assertEqual(spec_arg.manifest_request.bucket, original_bucket)

    def test_v2_with_none_calendar_store_delegates_with_none_and_no_get_call(self) -> None:
        materialization, _payload, calendar_request, fake_reader = _build_v2_fixture(self.root)
        path = self.root / "spec-v2-none-store.json"
        path.write_bytes(_spec_bytes_v2(calendar_request=calendar_request))
        sentinel = object()
        spy = _JobSpy(result=sentinel)
        deps = self._dependencies()
        deps["calendar_store"] = None
        deps["reader"] = fake_reader

        with patch(_BOUNDARY_TARGET, spy):
            result = run_daily_pipeline_from_pinned_gcs_run_spec_file(str(path), **deps)

        self.assertIs(result, sentinel)
        self.assertEqual(len(spy.calls), 1)
        call = spy.calls[0]
        args = call["args"]
        self.assertEqual(len(args), 2)
        spec_arg, calendar_arg = args
        self.assertIsNone(calendar_arg)
        self.assertEqual(spec_arg.calendar_materialization_id, materialization.materialization_id)
        self.assertIs(call["kwargs"]["reader"], fake_reader)

    def test_v2_with_exact_local_store_never_calls_get_and_delegates_with_none(self) -> None:
        materialization, _payload, calendar_request, fake_reader = _build_v2_fixture(self.root)
        path = self.root / "spec-v2-unused-store.json"
        path.write_bytes(_spec_bytes_v2(calendar_request=calendar_request))
        sentinel = object()
        spy = _JobSpy(result=sentinel)
        deps = self._dependencies()
        deps["reader"] = fake_reader
        distinctive_error = AssertionError("v2 must never call calendar_store.get")

        with patch.object(self.calendar_store, "get", side_effect=distinctive_error):
            with patch(_BOUNDARY_TARGET, spy):
                result = run_daily_pipeline_from_pinned_gcs_run_spec_file(str(path), **deps)

        self.assertIs(result, sentinel)
        self.assertEqual(len(spy.calls), 1)
        calendar_arg = spy.calls[0]["args"][1]
        self.assertIsNone(calendar_arg)
        self.assertEqual(materialization.materialization_id, calendar_request["materialization_id"])

    def test_v2_with_wrong_type_calendar_store_is_rejected_before_service(self) -> None:
        _materialization, _payload, calendar_request, fake_reader = _build_v2_fixture(self.root)
        path = self.root / "spec-v2-wrong-store.json"
        path.write_bytes(_spec_bytes_v2(calendar_request=calendar_request))
        spy = _JobSpy(result=object())
        deps = self._dependencies()
        deps["calendar_store"] = object()
        deps["reader"] = fake_reader

        with patch(_BOUNDARY_TARGET, spy):
            with self.assertRaises(PinnedGCSRunFileBoundaryError):
                run_daily_pipeline_from_pinned_gcs_run_spec_file(str(path), **deps)

        self.assertEqual(spy.calls, [])

    def test_v2_with_calendar_store_subclass_is_rejected_before_service(self) -> None:
        _materialization, _payload, calendar_request, fake_reader = _build_v2_fixture(self.root)
        path = self.root / "spec-v2-subclass-store.json"
        path.write_bytes(_spec_bytes_v2(calendar_request=calendar_request))

        class _StoreSubclass(LocalCalendarMaterializationStore):
            pass

        subclass_store = _StoreSubclass(
            root=self.root / "calendar-materializations",
            daily_reports_root=self.root / "daily-reports",
        )
        spy = _JobSpy(result=object())
        deps = self._dependencies()
        deps["calendar_store"] = subclass_store
        deps["reader"] = fake_reader

        with patch(_BOUNDARY_TARGET, spy):
            with self.assertRaises(PinnedGCSRunFileBoundaryError):
                run_daily_pipeline_from_pinned_gcs_run_spec_file(str(path), **deps)

        self.assertEqual(spy.calls, [])

    def test_v2_with_shaped_calendar_store_proxy_is_rejected_before_service(self) -> None:
        _materialization, _payload, calendar_request, fake_reader = _build_v2_fixture(self.root)
        path = self.root / "spec-v2-shaped-store.json"
        path.write_bytes(_spec_bytes_v2(calendar_request=calendar_request))

        class _ShapedStoreProxy:
            def __eq__(self, other: object) -> bool:
                return True

            def __hash__(self) -> int:
                return 0

        spy = _JobSpy(result=object())
        deps = self._dependencies()
        deps["calendar_store"] = _ShapedStoreProxy()
        deps["reader"] = fake_reader

        with patch(_BOUNDARY_TARGET, spy):
            with self.assertRaises(PinnedGCSRunFileBoundaryError):
                run_daily_pipeline_from_pinned_gcs_run_spec_file(str(path), **deps)

        self.assertEqual(spy.calls, [])

    def test_service_error_propagates_unchanged(self) -> None:
        path = self._write_spec()
        service_error = PinnedGCSRunServiceError("pinned gcs run service execution failed")
        spy = _JobSpy(raises=service_error)

        with patch(_BOUNDARY_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError) as ctx:
                run_daily_pipeline_from_pinned_gcs_run_spec_file(
                    str(path), **self._dependencies()
                )

        self.assertIs(ctx.exception, service_error)
        self.assertEqual(str(ctx.exception), "pinned gcs run service execution failed")

    def test_base_exception_from_service_is_not_intercepted(self) -> None:
        path = self._write_spec()
        spy = _JobSpy(raises=_MarkerBaseException("marker"))

        with patch(_BOUNDARY_TARGET, spy):
            with self.assertRaises(_MarkerBaseException):
                run_daily_pipeline_from_pinned_gcs_run_spec_file(
                    str(path), **self._dependencies()
                )

    def test_v2_service_error_propagates_unchanged(self) -> None:
        _materialization, _payload, calendar_request, fake_reader = _build_v2_fixture(self.root)
        path = self.root / "spec-v2-service-error.json"
        path.write_bytes(_spec_bytes_v2(calendar_request=calendar_request))
        service_error = PinnedGCSRunServiceError("pinned gcs run service execution failed")
        spy = _JobSpy(raises=service_error)
        deps = self._dependencies()
        deps["calendar_store"] = None
        deps["reader"] = fake_reader

        with patch(_BOUNDARY_TARGET, spy):
            with self.assertRaises(PinnedGCSRunServiceError) as ctx:
                run_daily_pipeline_from_pinned_gcs_run_spec_file(str(path), **deps)

        self.assertIs(ctx.exception, service_error)

    def test_v2_base_exception_from_service_is_not_intercepted(self) -> None:
        _materialization, _payload, calendar_request, fake_reader = _build_v2_fixture(self.root)
        path = self.root / "spec-v2-base-exception.json"
        path.write_bytes(_spec_bytes_v2(calendar_request=calendar_request))
        spy = _JobSpy(raises=_MarkerBaseException("marker"))
        deps = self._dependencies()
        deps["calendar_store"] = None
        deps["reader"] = fake_reader

        with patch(_BOUNDARY_TARGET, spy):
            with self.assertRaises(_MarkerBaseException):
                run_daily_pipeline_from_pinned_gcs_run_spec_file(str(path), **deps)


_EXACT_ALLOWED_BOUNDARY_IMPORTS = frozenset((
    # (level, module, imported name, asname). Closed set: any import in
    # pinned_gcs_run_file_boundary.py not exactly in this set, and any
    # entry here missing from that file, fails the equality assertion below.
    (0, "__future__", "annotations", None),
    (0, "stat", None, None),
    (0, "pathlib", "Path", None),
    (0, "india_swing.calendar_data.materialization_store", "LocalCalendarMaterializationStore", None),
    (0, "india_swing.daily_reports.artifact_store", "LocalDailyBundleArtifactStore", None),
    (0, "india_swing.historical_prices.artifact_store", "LocalHistoricalPriceArtifactStore", None),
    (0, "india_swing.identity_registry.adjudication_store", "LocalIdentityAdjudicationQueueStore", None),
    (0, "india_swing.identity_registry.artifact_store", "LocalIdentityRegistryStore", None),
    (0, "india_swing.reference_data.artifact_store", "LocalReferenceArtifactStore", None),
    (1, "acquisition", "GCSObjectReader", None),
    (1, "models", "DailyPipelineRun", None),
    (1, "pinned_gcs_run_service", "run_daily_pipeline_from_pinned_gcs_run_spec", None),
    (1, "pinned_gcs_run_spec", "MAXIMUM_PINNED_GCS_RUN_SPEC_BYTES", None),
    (1, "pinned_gcs_run_spec", "PINNED_GCS_RUN_SPEC_SCHEMA_VERSION", None),
    (1, "pinned_gcs_run_spec", "PINNED_GCS_RUN_SPEC_SCHEMA_VERSION_WITH_CALENDAR", None),
    (1, "pinned_gcs_run_spec", "PinnedGCSRunSpec", None),
    (1, "pinned_gcs_run_spec", "parse_pinned_gcs_run_spec", None),
    (1, "store", "LocalDailyPipelineRunStore", None),
))

_EXACT_ALLOWED_BOUNDARY_CALL_TARGETS = frozenset((
    # The production module's entire callable surface: raising its own
    # error type, exact-type checks, constructing a Path, stat inspection,
    # opening/reading the one caller-named file, parsing the spec bytes,
    # calling calendar_store.get, and invoking the one delegated service
    # function. Any other call name -- a GCS/storage client, os/environment
    # access, requests/urllib, subprocess, a broker/order/notification
    # helper, a strategy/model/LLM call, a listing/"latest" helper, a
    # retry/fallback wrapper, or a CLI/scheduler/deployment hook -- fails
    # this test.
    "PinnedGCSRunFileBoundaryError",
    "type",
    "Path",
    "lstat",
    "S_ISREG",
    "open",
    "read",
    "parse_pinned_gcs_run_spec",
    "load_pinned_gcs_run_spec_file",
    "PinnedGCSRunSpec",
    "get",
    "run_daily_pipeline_from_pinned_gcs_run_spec",
))

_FORBIDDEN_BOUNDARY_NAME_TOKENS = (
    "environ",
    "getenv",
    "now",
    "utcnow",
    "today",
    "google",
    "storage",
    "client",
    "glob",
    "iterdir",
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


class PinnedGCSRunFileBoundaryCapabilityTests(unittest.TestCase):
    """Proves pinned_gcs_run_file_boundary.py introduces exactly one new
    capability -- bounded binary reading of one caller-named regular file
    -- and no environment, clock, GCS/storage/client, listing/latest
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
            / "pinned_gcs_run_file_boundary.py"
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
        self.assertEqual(actual, _EXACT_ALLOWED_BOUNDARY_IMPORTS)

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
            if target not in _EXACT_ALLOWED_BOUNDARY_CALL_TARGETS:
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
            for token in _FORBIDDEN_BOUNDARY_NAME_TOKENS:
                if token in candidate:
                    offenders.append(candidate)
        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
