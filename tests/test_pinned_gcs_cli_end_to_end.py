from __future__ import annotations

import contextlib
import hashlib
import io
import json
import os
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from india_swing.calendar_data.artifact_store import LocalCalendarSourceArtifactStore
from india_swing.calendar_data.materialization import materialize_collection_calendar
from india_swing.calendar_data.materialization_codec import (
    MAXIMUM_CALENDAR_MATERIALIZATION_BYTES,
    encode_calendar_materialization,
)
from india_swing.calendar_data.materialization_store import LocalCalendarMaterializationStore
from india_swing.calendar_data.models import CALENDAR_DECLARATION_SCHEMA_VERSION
from india_swing.daily_pipeline.cli import main as cli_main
from india_swing.daily_pipeline.landing_manifest import MAXIMUM_LANDING_MANIFEST_BYTES
from india_swing.daily_pipeline.pinned_gcs_run_spec import (
    PINNED_GCS_RUN_SPEC_SCHEMA_VERSION,
    PINNED_GCS_RUN_SPEC_SCHEMA_VERSION_WITH_CALENDAR,
)
from india_swing.daily_pipeline.store import LocalDailyPipelineRunStore
from india_swing.reference.models import ReferenceReadiness
from india_swing.daily_pipeline.state_publication import PublishedStateObject

from tests.test_reconciliation import SESSION, _bundle_bytes, _master_bytes

_SECURITY_MASTER_MAXIMUM_BYTES = 32 * 1024 * 1024
_DAILY_BUNDLE_MAXIMUM_BYTES = 128 * 1024 * 1024

_BUCKET = "trusted-e2e-landing-bucket"
_STATE_BUCKET = "trusted-e2e-state-bucket"
_MANIFEST_GENERATION = 777
_SM_GENERATION = 111
_DB_GENERATION = 222
_CALENDAR_GENERATION = 555
_CALENDAR_CUTOFF = datetime(2026, 7, 15, 9, 0, 0, tzinfo=timezone.utc)
_NOT_BEFORE = "2026-07-15T00:00:00Z"
_BINDING_CUTOFF = "2026-07-15T14:00:00Z"
_KNOWLEDGE_TIME = "2026-07-15T13:00:00Z"

_REPO_VAR_DEFAULTS = {
    "INDIA_SWING_DAILY_PIPELINE_ROOT": "var/daily_pipeline",
    "INDIA_SWING_REFERENCE_DATA_ROOT": "var/reference_data",
    "INDIA_SWING_DAILY_REPORTS_ROOT": "var/daily_reports",
    "INDIA_SWING_HISTORICAL_PRICES_ROOT": "var/historical_prices",
    "INDIA_SWING_IDENTITY_REGISTRY_ROOT": "var/identity_registry",
    "INDIA_SWING_CALENDAR_DATA_ROOT": "var/calendar_data",
}


def _canon(path: object) -> str:
    """Pure lexical canonicalization (no filesystem I/O): matches the
    _canon idiom already used elsewhere in this codebase for path
    comparison without following symlinks or touching the target."""

    return os.path.normcase(os.path.abspath(str(path)))


# --------------------------------------------------------------------------
# Fake outer GCS SDK: patches only india_swing.daily_pipeline.acquisition.storage
# (this venv has no google-cloud-storage installed, so that name is already
# None; a stand-in with a .Client attribute is the closest available
# "storage.Client" patch target and GoogleCloudStorageObjectReader itself is
# never touched). Never lists, never selects "latest"; every object is
# resolved only by its exact (object_name, generation) key.
# --------------------------------------------------------------------------


class _FakeBlob:
    def __init__(self, client: "_FakeStorageClient", object_name: str, requested_generation: object, *, observed_generation: object, content_bytes: bytes) -> None:
        self._client = client
        self.name = object_name
        self.requested_generation = requested_generation
        self.generation = None
        self._observed_generation = observed_generation
        self._content_bytes = content_bytes

    def download_as_bytes(self, *, end=None, raw_download: bool = False, if_generation_match=None, retry=None) -> bytes:
        self._client.download_log.append(
            {
                "object_name": self.name,
                "requested_generation": self.requested_generation,
                "end": end,
                "raw_download": raw_download,
                "if_generation_match": if_generation_match,
                "retry": retry,
            }
        )
        self.generation = self._observed_generation
        if end is None:
            return self._content_bytes
        return self._content_bytes[: end + 1]


class _FakeBucket:
    def __init__(self, client: "_FakeStorageClient", name: str) -> None:
        self._client = client
        self.name = name
        self.blob_calls: list[tuple[str, object]] = []

    def blob(self, object_name: str, generation: object = None) -> _FakeBlob:
        self.blob_calls.append((object_name, generation))
        entry = self._client.objects.get((object_name, generation))
        if entry is None:
            raise LookupError("unregistered fake GCS object")
        observed_generation, content_bytes = entry
        return _FakeBlob(
            self._client,
            object_name,
            generation,
            observed_generation=observed_generation,
            content_bytes=content_bytes,
        )


class _FakeStorageClient:
    """Deterministic per-object fake keyed by exact (object_name, generation).

    objects: dict[(object_name, requested_generation)] -> (observed_generation, content_bytes).
    download_log records every download_as_bytes call, in order, across every
    object -- the single source of truth for asserting exact acquisition order.
    """

    def __init__(self, *, objects: dict[tuple[str, object], tuple[object, bytes]]) -> None:
        self.objects = objects
        self.bucket_calls: list[str] = []
        self.download_log: list[dict[str, object]] = []

    def bucket(self, bucket_name: str) -> _FakeBucket:
        self.bucket_calls.append(bucket_name)
        return _FakeBucket(self, bucket_name)


class _FakeStorageModule:
    """Stand-in for the `storage` name acquisition.py imports from
    google.cloud; its .Client attribute is the exact outer SDK constructor
    architecture_contract permits patching."""

    def __init__(self, client: _FakeStorageClient) -> None:
        self._client = client
        self.client_construction_count = 0

    def Client(self) -> _FakeStorageClient:
        self.client_construction_count += 1
        return self._client


class _RecordingStateWriter:
    """In-memory immutable writer for the CLI's durable publication stage."""

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def create_or_verify(self, **values: object) -> PublishedStateObject:
        self.calls.append(values)
        payload = values["content_bytes"]
        return PublishedStateObject(
            object_name=values["object_name"],
            generation=len(self.calls),
            byte_count=len(payload),
            sha256=hashlib.sha256(payload).hexdigest(),
        )


# --------------------------------------------------------------------------
# Calendar materialization fixture: real LocalCalendarSourceArtifactStore ->
# materialize_collection_calendar -> LocalCalendarMaterializationStore.put.
# The CLI's own LocalCalendarMaterializationStore.get(...) path is what
# retrieves it; this module never mocks the calendar store.
# --------------------------------------------------------------------------


def _import_base_calendar_source(store_root: Path, inputs_root: Path, *, document_id: str):
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
    times = iter(
        (
            datetime(2026, 7, 1, 12, 59, 59, tzinfo=timezone.utc),
            datetime(2026, 7, 1, 13, 0, 0, tzinfo=timezone.utc),
        )
    )
    return LocalCalendarSourceArtifactStore(
        store_root,
        clock=lambda: next(times),
    ).import_source(source_path, declaration_path)


def _build_stored_calendar(
    *,
    calendar_data_root: Path,
    daily_reports_root: Path,
    inputs_root: Path,
    coverage_start: date,
    coverage_end: date,
    cutoff: datetime,
    document_id: str,
):
    source = _import_base_calendar_source(calendar_data_root, inputs_root, document_id=document_id)
    materialization = materialize_collection_calendar(
        sources=(source,),
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        cutoff=cutoff,
        observed_date_artifacts=(),
    )
    store = LocalCalendarMaterializationStore(
        root=calendar_data_root,
        daily_reports_root=daily_reports_root,
    )
    return store, store.put(materialization)


def _calendar_object_name(materialization_id: str) -> str:
    return f"calendar-materializations/{materialization_id}/materialization.json"


def _build_v2_calendar_bytes(
    *,
    root: Path,
    coverage_start: date,
    coverage_end: date,
    cutoff: datetime,
    document_id: str,
) -> tuple[bytes, str]:
    """Real, canonically-encoded CollectionCalendarMaterialization bytes
    built entirely OUTSIDE the CLI-configured calendar_data_root (a
    separate root under the same TemporaryDirectory), via the real
    committed materializer and encode_calendar_materialization -- never
    LocalCalendarMaterializationStore.put, so the CLI's own configured
    local calendar store never contains this materialization."""

    source = _import_base_calendar_source(
        root / "v2-calendar-source",
        root / "v2-calendar-inputs" / document_id,
        document_id=document_id,
    )
    materialization = materialize_collection_calendar(
        sources=(source,),
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        cutoff=cutoff,
        observed_date_artifacts=(),
    )
    payload = encode_calendar_materialization(materialization)
    return payload, materialization.materialization_id


def _calendar_request_dict(
    materialization_id: str,
    payload: bytes,
    *,
    generation: int = _CALENDAR_GENERATION,
    bucket: str = _BUCKET,
) -> dict:
    return {
        "bucket": bucket,
        "object_name": _calendar_object_name(materialization_id),
        "generation": generation,
        "expected_sha256": hashlib.sha256(payload).hexdigest(),
        "materialization_id": materialization_id,
    }


# --------------------------------------------------------------------------
# Landing-manifest / run-spec fixture builders. Fixture hashes are always
# computed inside the test from the exact bytes the fake SDK will serve;
# expected_manifest_sha256 in the *spec* is a separate, independently
# supplied value (matching production's operator-governed contract).
# --------------------------------------------------------------------------


def _sm_object_name(session: date) -> str:
    return f"landing/{session.isoformat()}/NSE_CM_security_{session.strftime('%d%m%Y')}.csv.gz"


def _db_object_name(session: date) -> str:
    return f"landing/{session.isoformat()}/Reports-Daily-Multiple.zip"


def _manifest_object_name(session: date) -> str:
    return f"landing/{session.isoformat()}/landing-manifest.json"


def _manifest_bytes(
    session: date,
    *,
    sm_bytes: bytes,
    db_bytes: bytes,
    knowledge_time: str = _KNOWLEDGE_TIME,
    sm_generation: int = _SM_GENERATION,
    db_generation: int = _DB_GENERATION,
) -> bytes:
    manifest_dict = {
        "schema_version": 1,
        "knowledge_time": knowledge_time,
        "target_session": session.isoformat(),
        "objects": [
            {
                "file_type": "SECURITY_MASTER",
                "bucket": _BUCKET,
                "object_name": _sm_object_name(session),
                "generation": sm_generation,
                "sha256": hashlib.sha256(sm_bytes).hexdigest(),
            },
            {
                "file_type": "DAILY_BUNDLE",
                "bucket": _BUCKET,
                "object_name": _db_object_name(session),
                "generation": db_generation,
                "sha256": hashlib.sha256(db_bytes).hexdigest(),
            },
        ],
    }
    return json.dumps(manifest_dict, separators=(",", ":")).encode("utf-8")


def _run_spec_dict(
    *,
    session: date,
    manifest_generation: int,
    expected_manifest_sha256: str,
    calendar_materialization_id: str,
    run_cutoff: str,
    not_before: str = _NOT_BEFORE,
    binding_cutoff: str = _BINDING_CUTOFF,
    previous_run_id: str | None = None,
) -> dict:
    return {
        "schema_version": PINNED_GCS_RUN_SPEC_SCHEMA_VERSION,
        "manifest_request": {
            "bucket": _BUCKET,
            "object_name": _manifest_object_name(session),
            "generation": manifest_generation,
            "target_session": session.isoformat(),
        },
        "trusted_binding": {
            "expected_manifest_sha256": expected_manifest_sha256,
            "allowed_bucket": _BUCKET,
            "target_session": session.isoformat(),
            "not_before": not_before,
            "cutoff": binding_cutoff,
        },
        "run": {
            "market_session": session.isoformat(),
            "cutoff": run_cutoff,
            "calendar_materialization_id": calendar_materialization_id,
            "previous_run_id": previous_run_id,
        },
    }


def _run_spec_dict_v2(
    *,
    session: date,
    manifest_generation: int,
    expected_manifest_sha256: str,
    calendar_request: dict,
    run_cutoff: str,
    not_before: str = _NOT_BEFORE,
    binding_cutoff: str = _BINDING_CUTOFF,
    previous_run_id: str | None = None,
) -> dict:
    return {
        "schema_version": PINNED_GCS_RUN_SPEC_SCHEMA_VERSION_WITH_CALENDAR,
        "manifest_request": {
            "bucket": _BUCKET,
            "object_name": _manifest_object_name(session),
            "generation": manifest_generation,
            "target_session": session.isoformat(),
        },
        "trusted_binding": {
            "expected_manifest_sha256": expected_manifest_sha256,
            "allowed_bucket": _BUCKET,
            "target_session": session.isoformat(),
            "not_before": not_before,
            "cutoff": binding_cutoff,
        },
        "calendar_request": calendar_request,
        "run": {
            "market_session": session.isoformat(),
            "cutoff": run_cutoff,
            "calendar_materialization_id": calendar_request["materialization_id"],
            "previous_run_id": previous_run_id,
        },
    }


def _env_for(root: Path) -> dict[str, str]:
    return {
        "INDIA_SWING_DAILY_PIPELINE_ROOT": str(root / "daily_pipeline"),
        "INDIA_SWING_REFERENCE_DATA_ROOT": str(root / "reference_data"),
        "INDIA_SWING_DAILY_REPORTS_ROOT": str(root / "daily_reports"),
        "INDIA_SWING_HISTORICAL_PRICES_ROOT": str(root / "historical_prices"),
        "INDIA_SWING_IDENTITY_REGISTRY_ROOT": str(root / "identity_registry"),
        "INDIA_SWING_CALENDAR_DATA_ROOT": str(root / "calendar_data"),
    }


class _EndToEndFixtureTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.env = _env_for(self.root)
        self.calendar_data_root = self.root / "calendar_data"
        self.daily_reports_root = self.root / "daily_reports"
        # One aware-UTC anchor captured once per test. The run cutoff is
        # derived from it with a small bounded margin so the CLI's own
        # real-clock store timestamps (first_seen_at/validated_at have no
        # injectable clock and are genuine wall-clock time whenever this
        # suite runs) are always comfortably before it. No outcome assertion
        # anywhere in this suite depends on a calendar date, execution year,
        # or a cutoff that eventually expires -- the fixture's own session/
        # manifest/binding/calendar dates stay fixed at 2026-07-15, which
        # only ever needs to be *before* this anchor, never equal to "today".
        self.anchor = datetime.now(timezone.utc).replace(microsecond=0)
        self.run_cutoff_dt = self.anchor + timedelta(hours=1)
        self.run_cutoff = self.run_cutoff_dt.isoformat().replace("+00:00", "Z")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _assert_stderr_omits(self, stderr_text: str, *sensitive_values: object) -> None:
        for value in sensitive_values:
            if value:
                self.assertNotIn(str(value), stderr_text)

    def _build_calendar(
        self,
        *,
        coverage_start: date,
        coverage_end: date,
        cutoff: datetime,
        document_id: str = "CMTR-E2E-BASE",
    ):
        return _build_stored_calendar(
            calendar_data_root=self.calendar_data_root,
            daily_reports_root=self.daily_reports_root,
            inputs_root=self.root / "calendar-inputs" / document_id,
            coverage_start=coverage_start,
            coverage_end=coverage_end,
            cutoff=cutoff,
            document_id=document_id,
        )

    def _write_spec(self, name: str, spec_dict: dict) -> Path:
        path = self.root / name
        path.write_text(json.dumps(spec_dict, separators=(",", ":")), encoding="utf-8")
        return path

    def _run_cli(self, spec_path: Path, fake_client: _FakeStorageClient):
        stdout = io.StringIO()
        stderr = io.StringIO()
        fake_storage = _FakeStorageModule(fake_client)
        self.publication_writer = _RecordingStateWriter()
        runtime_env = {
            **self.env,
            "INDIA_SWING_STATE_PUBLICATION_BUCKET": _STATE_BUCKET,
        }
        with patch.dict(os.environ, runtime_env, clear=False):
            with (
                patch("india_swing.daily_pipeline.acquisition.storage", fake_storage),
                patch(
                    "india_swing.daily_pipeline.cli.GoogleCloudStorageStateObjectWriter",
                    return_value=self.publication_writer,
                ),
            ):
                with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                    exit_code = cli_main(["run-pinned-gcs", "--spec-file", str(spec_path)])
        return exit_code, stdout.getvalue(), stderr.getvalue(), fake_storage

    def _run_store(self) -> LocalDailyPipelineRunStore:
        return LocalDailyPipelineRunStore(self.root / "daily_pipeline")

    def _assert_no_run_published(self) -> None:
        self.assertEqual(self._run_store().list_runs(), ())

    def _happy_calendar_and_session(self):
        calendar_store, stored_calendar = self._build_calendar(
            coverage_start=date(2026, 7, 14),
            coverage_end=date(2026, 7, 15),
            cutoff=_CALENDAR_CUTOFF,
        )
        return stored_calendar.manifest.artifact_id

    def _assert_calendar_id_absent_from_local_store(self, calendar_materialization_id: str) -> None:
        store = LocalCalendarMaterializationStore(
            root=self.calendar_data_root, daily_reports_root=self.daily_reports_root,
        )
        with self.assertRaises(Exception):
            store.get(calendar_materialization_id)

    @staticmethod
    def _get_must_not_be_called(self_: object, artifact_id: str) -> None:
        raise AssertionError(
            "schema v2 must never call LocalCalendarMaterializationStore.get"
        )

    def _run_cli_v2(self, spec_path: Path, fake_client: _FakeStorageClient):
        # Pipeline state inventory requires every configured state root to
        # exist.  Schema v2 intentionally acquires its calendar from GCS and
        # never writes it into LocalCalendarMaterializationStore, so provision
        # the otherwise-empty configured root just as deployment does.
        self.calendar_data_root.mkdir(parents=True, exist_ok=True)
        with patch.object(
            LocalCalendarMaterializationStore, "get", self._get_must_not_be_called
        ):
            return self._run_cli(spec_path, fake_client)


class HappyPathTests(_EndToEndFixtureTestCase):
    def test_full_cli_to_persisted_run_chain(self) -> None:
        session = SESSION
        sm_bytes = _master_bytes()
        db_bytes = _bundle_bytes()
        calendar_id = self._happy_calendar_and_session()

        manifest_bytes = _manifest_bytes(session, sm_bytes=sm_bytes, db_bytes=db_bytes)
        expected_manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()

        spec_dict = _run_spec_dict(
            session=session,
            manifest_generation=_MANIFEST_GENERATION,
            expected_manifest_sha256=expected_manifest_sha256,
            calendar_materialization_id=calendar_id,
            run_cutoff=self.run_cutoff,
            previous_run_id=None,
        )
        spec_path = self._write_spec("spec.json", spec_dict)

        fake_client = _FakeStorageClient(
            objects={
                (_manifest_object_name(session), _MANIFEST_GENERATION): (
                    _MANIFEST_GENERATION,
                    manifest_bytes,
                ),
                (_sm_object_name(session), _SM_GENERATION): (_SM_GENERATION, sm_bytes),
                (_db_object_name(session), _DB_GENERATION): (_DB_GENERATION, db_bytes),
            }
        )

        exit_code, stdout, stderr, fake_storage = self._run_cli(spec_path, fake_client)

        self.assertEqual(exit_code, 0, stderr)
        self.assertEqual(stderr, "")
        payload = json.loads(stdout)
        self.assertEqual(payload["status"], "COMPLETE")
        self.assertEqual(payload["kind"], "PINNED_GCS_STATE_PUBLICATION")
        self.assertEqual(payload["state_bucket"], _STATE_BUCKET)
        self.assertEqual(
            payload["publication_object_generation"],
            len(self.publication_writer.calls),
        )

        reloaded = self._run_store().get(payload["run_id"])
        self.assertEqual(reloaded.market_session, session)
        self.assertEqual(reloaded.cutoff, self.run_cutoff_dt)
        self.assertIsNone(reloaded.previous_run_id)
        self.assertIs(reloaded.readiness, ReferenceReadiness.COLLECTION_ONLY)
        self.assertFalse(reloaded.actionable)
        self.assertFalse(reloaded.stable_identity_assigned)

        # Persisted calendar_materialization_id matches the spec's declared
        # ID exactly, and reloading that exact materialization through the
        # real store (never a mock, never latest-selection) confirms its
        # target-session coverage and that its cutoff does not follow the
        # exact derived run cutoff.
        self.assertEqual(reloaded.calendar_materialization_id, calendar_id)
        reloaded_calendar = LocalCalendarMaterializationStore(
            root=self.calendar_data_root,
            daily_reports_root=self.daily_reports_root,
        ).get(calendar_id)
        self.assertEqual(reloaded_calendar.manifest.artifact_id, calendar_id)
        self.assertLessEqual(reloaded_calendar.materialization.coverage_start, session)
        self.assertLessEqual(session, reloaded_calendar.materialization.coverage_end)
        self.assertLessEqual(reloaded_calendar.materialization.cutoff, self.run_cutoff_dt)

        self.assertEqual(fake_storage.client_construction_count, 1)
        self.assertEqual(
            [entry["object_name"] for entry in fake_client.download_log],
            [
                _manifest_object_name(session),
                _sm_object_name(session),
                _db_object_name(session),
            ],
        )
        for entry in fake_client.download_log:
            self.assertTrue(entry["raw_download"])
            self.assertEqual(
                entry["if_generation_match"], entry["requested_generation"]
            )
        self.assertEqual(fake_client.download_log[0]["end"], MAXIMUM_LANDING_MANIFEST_BYTES)
        self.assertEqual(
            fake_client.download_log[0]["if_generation_match"], _MANIFEST_GENERATION
        )
        self.assertEqual(fake_client.download_log[1]["end"], _SECURITY_MASTER_MAXIMUM_BYTES)
        self.assertEqual(fake_client.download_log[1]["if_generation_match"], _SM_GENERATION)
        self.assertEqual(fake_client.download_log[2]["end"], _DAILY_BUNDLE_MAXIMUM_BYTES)
        self.assertEqual(fake_client.download_log[2]["if_generation_match"], _DB_GENERATION)
        for bucket_call in fake_client.bucket_calls:
            self.assertEqual(bucket_call, _BUCKET)
        self.assertFalse(hasattr(fake_client, "list_blobs"))

        lineage = reloaded.landing_input_lineage
        self.assertIsNotNone(lineage)
        self.assertEqual(lineage.manifest_source.bucket, _BUCKET)
        self.assertEqual(lineage.manifest_source.object_name, _manifest_object_name(session))
        self.assertEqual(lineage.manifest_source.generation, _MANIFEST_GENERATION)
        self.assertEqual(lineage.manifest_source.target_session, session)
        self.assertEqual(lineage.security_master.bucket, _BUCKET)
        self.assertEqual(lineage.security_master.object_name, _sm_object_name(session))
        self.assertEqual(lineage.security_master.generation, _SM_GENERATION)
        self.assertEqual(lineage.security_master.sha256_hash, hashlib.sha256(sm_bytes).hexdigest())
        self.assertEqual(lineage.daily_bundle.bucket, _BUCKET)
        self.assertEqual(lineage.daily_bundle.object_name, _db_object_name(session))
        self.assertEqual(lineage.daily_bundle.generation, _DB_GENERATION)
        self.assertEqual(lineage.daily_bundle.sha256_hash, hashlib.sha256(db_bytes).hexdigest())

    def test_full_cli_to_persisted_run_chain_schema_v2(self) -> None:
        session = SESSION
        sm_bytes = _master_bytes()
        db_bytes = _bundle_bytes()

        calendar_payload, calendar_materialization_id = _build_v2_calendar_bytes(
            root=self.root,
            coverage_start=date(2026, 7, 14),
            coverage_end=date(2026, 7, 15),
            cutoff=_CALENDAR_CUTOFF,
            document_id="CMTR-E2E-V2-BASE",
        )
        # The CLI's own configured local calendar store must contain no
        # materialization for this ID, both before and after the run.
        self._assert_calendar_id_absent_from_local_store(calendar_materialization_id)
        calendar_request = _calendar_request_dict(calendar_materialization_id, calendar_payload)

        manifest_bytes = _manifest_bytes(session, sm_bytes=sm_bytes, db_bytes=db_bytes)
        expected_manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()

        spec_dict = _run_spec_dict_v2(
            session=session,
            manifest_generation=_MANIFEST_GENERATION,
            expected_manifest_sha256=expected_manifest_sha256,
            calendar_request=calendar_request,
            run_cutoff=self.run_cutoff,
            previous_run_id=None,
        )
        spec_path = self._write_spec("spec-v2.json", spec_dict)

        fake_client = _FakeStorageClient(
            objects={
                (_calendar_object_name(calendar_materialization_id), _CALENDAR_GENERATION): (
                    _CALENDAR_GENERATION,
                    calendar_payload,
                ),
                (_manifest_object_name(session), _MANIFEST_GENERATION): (
                    _MANIFEST_GENERATION,
                    manifest_bytes,
                ),
                (_sm_object_name(session), _SM_GENERATION): (_SM_GENERATION, sm_bytes),
                (_db_object_name(session), _DB_GENERATION): (_DB_GENERATION, db_bytes),
            }
        )

        exit_code, stdout, stderr, fake_storage = self._run_cli_v2(spec_path, fake_client)

        self.assertEqual(exit_code, 0, stderr)
        self.assertEqual(stderr, "")
        payload = json.loads(stdout)
        self.assertEqual(payload["status"], "COMPLETE")
        self.assertEqual(payload["kind"], "PINNED_GCS_STATE_PUBLICATION")
        self.assertEqual(payload["state_bucket"], _STATE_BUCKET)
        self.assertEqual(
            payload["publication_object_generation"],
            len(self.publication_writer.calls),
        )

        reloaded = self._run_store().get(payload["run_id"])
        self.assertEqual(reloaded.market_session, session)
        self.assertEqual(reloaded.cutoff, self.run_cutoff_dt)
        self.assertIsNone(reloaded.previous_run_id)
        self.assertIs(reloaded.readiness, ReferenceReadiness.COLLECTION_ONLY)
        self.assertFalse(reloaded.actionable)
        self.assertEqual(reloaded.calendar_materialization_id, calendar_materialization_id)

        self.assertEqual(fake_storage.client_construction_count, 1)
        self.assertEqual(
            [entry["object_name"] for entry in fake_client.download_log],
            [
                _calendar_object_name(calendar_materialization_id),
                _manifest_object_name(session),
                _sm_object_name(session),
                _db_object_name(session),
            ],
        )
        for entry in fake_client.download_log:
            self.assertTrue(entry["raw_download"])
            self.assertEqual(entry["if_generation_match"], entry["requested_generation"])
            self.assertIsNone(entry["retry"])
        self.assertEqual(
            fake_client.download_log[0]["end"], MAXIMUM_CALENDAR_MATERIALIZATION_BYTES
        )
        self.assertEqual(fake_client.download_log[0]["if_generation_match"], _CALENDAR_GENERATION)
        self.assertEqual(fake_client.download_log[1]["end"], MAXIMUM_LANDING_MANIFEST_BYTES)
        self.assertEqual(fake_client.download_log[1]["if_generation_match"], _MANIFEST_GENERATION)
        self.assertEqual(fake_client.download_log[2]["end"], _SECURITY_MASTER_MAXIMUM_BYTES)
        self.assertEqual(fake_client.download_log[2]["if_generation_match"], _SM_GENERATION)
        self.assertEqual(fake_client.download_log[3]["end"], _DAILY_BUNDLE_MAXIMUM_BYTES)
        self.assertEqual(fake_client.download_log[3]["if_generation_match"], _DB_GENERATION)
        for bucket_call in fake_client.bucket_calls:
            self.assertEqual(bucket_call, _BUCKET)

        lineage = reloaded.landing_input_lineage
        self.assertIsNotNone(lineage)
        self.assertEqual(lineage.manifest_source.bucket, _BUCKET)
        self.assertEqual(lineage.manifest_source.object_name, _manifest_object_name(session))
        self.assertEqual(lineage.manifest_source.generation, _MANIFEST_GENERATION)
        self.assertEqual(lineage.manifest_source.target_session, session)
        self.assertEqual(lineage.security_master.generation, _SM_GENERATION)
        self.assertEqual(lineage.daily_bundle.generation, _DB_GENERATION)

        # Still absent from the CLI's local calendar store after the run --
        # v2 never wrote to it either.
        self._assert_calendar_id_absent_from_local_store(calendar_materialization_id)


class FailurePathTests(_EndToEndFixtureTestCase):
    def _assert_no_pipeline_artifacts_created(self) -> None:
        for name in ("reference_data", "daily_reports", "historical_prices", "identity_registry"):
            self.assertFalse((self.root / name).exists(), name)
        self.assertEqual(self._run_store().list_runs(), ())

    def _assert_sanitized_failure_envelope(self, stdout: str, stderr: str) -> dict:
        self.assertEqual(stdout, "")
        payload = json.loads(stderr)
        self.assertEqual(set(payload), {"status", "error_type"})
        self.assertEqual(payload["status"], "FAILED")
        return payload

    def test_malformed_run_spec_yields_zero_downloads_and_zero_publication(self) -> None:
        malformed_text = "{not-valid-json"
        spec_path = self.root / "spec.json"
        spec_path.write_text(malformed_text, encoding="utf-8")
        fake_client = _FakeStorageClient(objects={})

        exit_code, stdout, stderr, fake_storage = self._run_cli(spec_path, fake_client)

        self.assertEqual(exit_code, 2)
        self._assert_sanitized_failure_envelope(stdout, stderr)
        self._assert_stderr_omits(stderr, malformed_text, str(spec_path))
        self.assertEqual(fake_client.download_log, [])
        self.assertEqual(fake_client.bucket_calls, [])
        self._assert_no_pipeline_artifacts_created()

    def test_manifest_hash_mismatch_downloads_only_manifest_and_publishes_nothing(self) -> None:
        session = SESSION
        sm_bytes = _master_bytes()
        db_bytes = _bundle_bytes()
        calendar_id = self._happy_calendar_and_session()

        manifest_bytes = _manifest_bytes(session, sm_bytes=sm_bytes, db_bytes=db_bytes)
        real_manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
        wrong_expected_sha256 = hashlib.sha256(b"not-the-real-manifest").hexdigest()

        spec_dict = _run_spec_dict(
            session=session,
            manifest_generation=_MANIFEST_GENERATION,
            expected_manifest_sha256=wrong_expected_sha256,
            calendar_materialization_id=calendar_id,
            run_cutoff=self.run_cutoff,
        )
        spec_path = self._write_spec("spec.json", spec_dict)
        fake_client = _FakeStorageClient(
            objects={
                (_manifest_object_name(session), _MANIFEST_GENERATION): (
                    _MANIFEST_GENERATION,
                    manifest_bytes,
                ),
                (_sm_object_name(session), _SM_GENERATION): (_SM_GENERATION, sm_bytes),
                (_db_object_name(session), _DB_GENERATION): (_DB_GENERATION, db_bytes),
            }
        )

        exit_code, stdout, stderr, fake_storage = self._run_cli(spec_path, fake_client)

        self.assertEqual(exit_code, 2)
        payload = self._assert_sanitized_failure_envelope(stdout, stderr)
        self.assertEqual(
            payload["error_type"], "PinnedGCSStatePublicationServiceError"
        )
        self.assertEqual(
            [entry["object_name"] for entry in fake_client.download_log],
            [_manifest_object_name(session)],
        )
        self._assert_stderr_omits(
            stderr,
            _BUCKET,
            _manifest_object_name(session),
            wrong_expected_sha256,
            real_manifest_sha256,
        )
        self._assert_no_pipeline_artifacts_created()

    def test_wrong_observed_manifest_generation_fails_before_any_data_object_read(self) -> None:
        session = SESSION
        sm_bytes = _master_bytes()
        db_bytes = _bundle_bytes()
        calendar_id = self._happy_calendar_and_session()

        manifest_bytes = _manifest_bytes(session, sm_bytes=sm_bytes, db_bytes=db_bytes)
        expected_manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
        wrong_observed_generation = 999999

        spec_dict = _run_spec_dict(
            session=session,
            manifest_generation=_MANIFEST_GENERATION,
            expected_manifest_sha256=expected_manifest_sha256,
            calendar_materialization_id=calendar_id,
            run_cutoff=self.run_cutoff,
        )
        spec_path = self._write_spec("spec.json", spec_dict)

        fake_client = _FakeStorageClient(
            objects={
                # Registered at the requested generation, but the simulated
                # SDK reports a mismatched post-download blob.generation --
                # exactly the defensive check
                # GoogleCloudStorageObjectReader.read_generation performs
                # independently of the caller-supplied generation.
                (_manifest_object_name(session), _MANIFEST_GENERATION): (
                    wrong_observed_generation,
                    manifest_bytes,
                ),
                (_sm_object_name(session), _SM_GENERATION): (_SM_GENERATION, sm_bytes),
                (_db_object_name(session), _DB_GENERATION): (_DB_GENERATION, db_bytes),
            }
        )

        exit_code, stdout, stderr, fake_storage = self._run_cli(spec_path, fake_client)

        self.assertEqual(exit_code, 2)
        self._assert_sanitized_failure_envelope(stdout, stderr)
        self.assertEqual(
            [entry["object_name"] for entry in fake_client.download_log],
            [_manifest_object_name(session)],
        )
        self._assert_stderr_omits(
            stderr,
            _BUCKET,
            _manifest_object_name(session),
            expected_manifest_sha256,
            wrong_observed_generation,
        )
        self._assert_no_pipeline_artifacts_created()

    def test_tampered_security_master_payload_permits_no_daily_bundle_read_or_publication(
        self,
    ) -> None:
        session = SESSION
        sm_bytes = _master_bytes()
        tampered_sm_bytes = sm_bytes + b"\x00tampered"
        db_bytes = _bundle_bytes()
        calendar_id = self._happy_calendar_and_session()

        # The manifest's own SECURITY_MASTER sha256 entry is computed from
        # the original sm_bytes; the SDK is made to actually serve different
        # (tampered) bytes at that exact pinned generation --
        # GCSLandingObjectReader's own post-download SHA-256 re-check must
        # catch this before the daily bundle is ever read.
        manifest_bytes = _manifest_bytes(session, sm_bytes=sm_bytes, db_bytes=db_bytes)
        expected_manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
        real_sm_sha256 = hashlib.sha256(sm_bytes).hexdigest()
        tampered_sm_sha256 = hashlib.sha256(tampered_sm_bytes).hexdigest()

        spec_dict = _run_spec_dict(
            session=session,
            manifest_generation=_MANIFEST_GENERATION,
            expected_manifest_sha256=expected_manifest_sha256,
            calendar_materialization_id=calendar_id,
            run_cutoff=self.run_cutoff,
        )
        spec_path = self._write_spec("spec.json", spec_dict)

        fake_client = _FakeStorageClient(
            objects={
                (_manifest_object_name(session), _MANIFEST_GENERATION): (
                    _MANIFEST_GENERATION,
                    manifest_bytes,
                ),
                (_sm_object_name(session), _SM_GENERATION): (_SM_GENERATION, tampered_sm_bytes),
                (_db_object_name(session), _DB_GENERATION): (_DB_GENERATION, db_bytes),
            }
        )

        exit_code, stdout, stderr, fake_storage = self._run_cli(spec_path, fake_client)

        self.assertEqual(exit_code, 2)
        payload = self._assert_sanitized_failure_envelope(stdout, stderr)
        self.assertEqual(
            payload["error_type"], "PinnedGCSStatePublicationServiceError"
        )
        self.assertEqual(
            [entry["object_name"] for entry in fake_client.download_log],
            [_manifest_object_name(session), _sm_object_name(session)],
        )
        self._assert_stderr_omits(
            stderr,
            _BUCKET,
            _sm_object_name(session),
            real_sm_sha256,
            tampered_sm_sha256,
        )
        self._assert_no_pipeline_artifacts_created()

    def test_future_calendar_cutoff_rejected_before_first_download(self) -> None:
        session = SESSION
        sm_bytes = _master_bytes()
        db_bytes = _bundle_bytes()
        # Calendar materialized with a cutoff strictly after the spec's run
        # cutoff -- rejected by the service's own preflight, before any GCS
        # read is ever attempted.
        future_cutoff = self.run_cutoff_dt + timedelta(days=1)
        _calendar_store, stored_calendar = self._build_calendar(
            coverage_start=date(2026, 7, 14),
            coverage_end=date(2026, 7, 15),
            cutoff=future_cutoff,
            document_id="CMTR-E2E-FUTURE-CUTOFF",
        )
        calendar_id = stored_calendar.manifest.artifact_id

        manifest_bytes = _manifest_bytes(session, sm_bytes=sm_bytes, db_bytes=db_bytes)
        expected_manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
        spec_dict = _run_spec_dict(
            session=session,
            manifest_generation=_MANIFEST_GENERATION,
            expected_manifest_sha256=expected_manifest_sha256,
            calendar_materialization_id=calendar_id,
            run_cutoff=self.run_cutoff,
        )
        spec_path = self._write_spec("spec.json", spec_dict)
        fake_client = _FakeStorageClient(
            objects={
                (_manifest_object_name(session), _MANIFEST_GENERATION): (
                    _MANIFEST_GENERATION,
                    manifest_bytes,
                ),
                (_sm_object_name(session), _SM_GENERATION): (_SM_GENERATION, sm_bytes),
                (_db_object_name(session), _DB_GENERATION): (_DB_GENERATION, db_bytes),
            }
        )

        exit_code, stdout, stderr, fake_storage = self._run_cli(spec_path, fake_client)

        self.assertEqual(exit_code, 2)
        payload = self._assert_sanitized_failure_envelope(stdout, stderr)
        self.assertEqual(
            payload["error_type"], "PinnedGCSStatePublicationServiceError"
        )
        self.assertEqual(fake_client.download_log, [])
        self.assertEqual(fake_client.bucket_calls, [])
        self._assert_stderr_omits(stderr, _BUCKET, calendar_id, expected_manifest_sha256)
        self._assert_no_pipeline_artifacts_created()

    def test_non_session_calendar_rejected_before_first_download(self) -> None:
        non_session_date = date(2026, 7, 18)  # Saturday: covered but not a session
        _calendar_store, stored_calendar = self._build_calendar(
            coverage_start=date(2026, 7, 17),
            coverage_end=date(2026, 7, 18),
            cutoff=datetime(2026, 7, 18, 9, 0, 0, tzinfo=timezone.utc),
            document_id="CMTR-E2E-NON-SESSION",
        )
        calendar_id = stored_calendar.manifest.artifact_id

        placeholder_hash = "a" * 64
        spec_dict = _run_spec_dict(
            session=non_session_date,
            manifest_generation=_MANIFEST_GENERATION,
            expected_manifest_sha256=placeholder_hash,
            calendar_materialization_id=calendar_id,
            run_cutoff=self.run_cutoff,
            not_before="2026-07-18T00:00:00Z",
            binding_cutoff="2026-07-18T14:00:00Z",
        )
        spec_path = self._write_spec("spec.json", spec_dict)
        fake_client = _FakeStorageClient(objects={})

        exit_code, stdout, stderr, fake_storage = self._run_cli(spec_path, fake_client)

        self.assertEqual(exit_code, 2)
        payload = self._assert_sanitized_failure_envelope(stdout, stderr)
        self.assertEqual(
            payload["error_type"], "PinnedGCSStatePublicationServiceError"
        )
        self.assertEqual(fake_client.download_log, [])
        self.assertEqual(fake_client.bucket_calls, [])
        self._assert_stderr_omits(
            stderr, _BUCKET, calendar_id, non_session_date.isoformat(), placeholder_hash
        )
        self._assert_no_pipeline_artifacts_created()

    def test_secret_bearing_bucket_and_reader_exception_never_leak(self) -> None:
        session = SESSION
        secret_bucket = "secret-e2e-bucket-do-not-leak-1a2b"
        secret_exception_text = "SECRET-E2E-READER-FAILURE-DO-NOT-LEAK-7c2d"
        calendar_id = self._happy_calendar_and_session()

        manifest_bytes = _manifest_bytes(
            session, sm_bytes=_master_bytes(), db_bytes=_bundle_bytes()
        )
        spec_dict = {
            "schema_version": PINNED_GCS_RUN_SPEC_SCHEMA_VERSION,
            "manifest_request": {
                "bucket": secret_bucket,
                "object_name": _manifest_object_name(session),
                "generation": _MANIFEST_GENERATION,
                "target_session": session.isoformat(),
            },
            "trusted_binding": {
                "expected_manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
                "allowed_bucket": secret_bucket,
                "target_session": session.isoformat(),
                "not_before": _NOT_BEFORE,
                "cutoff": _BINDING_CUTOFF,
            },
            "run": {
                "market_session": session.isoformat(),
                "cutoff": self.run_cutoff,
                "calendar_materialization_id": calendar_id,
                "previous_run_id": None,
            },
        }
        spec_path = self._write_spec("spec.json", spec_dict)

        class _RaisingBlob:
            def download_as_bytes(self, **kwargs: object) -> bytes:
                raise RuntimeError(secret_exception_text)

        class _RaisingBucket:
            def __init__(self, name: str) -> None:
                self.name = name

            def blob(self, object_name: str, generation: object = None) -> _RaisingBlob:
                return _RaisingBlob()

        class _RaisingClient:
            def __init__(self) -> None:
                self.bucket_calls: list[str] = []

            def bucket(self, bucket_name: str) -> _RaisingBucket:
                self.bucket_calls.append(bucket_name)
                return _RaisingBucket(bucket_name)

        raising_client = _RaisingClient()
        fake_storage = _FakeStorageModule(raising_client)
        stdout = io.StringIO()
        stderr = io.StringIO()
        runtime_env = {
            **self.env,
            "INDIA_SWING_STATE_PUBLICATION_BUCKET": _STATE_BUCKET,
        }
        with patch.dict(os.environ, runtime_env, clear=False):
            with (
                patch("india_swing.daily_pipeline.acquisition.storage", fake_storage),
                patch(
                    "india_swing.daily_pipeline.cli.GoogleCloudStorageStateObjectWriter",
                    return_value=_RecordingStateWriter(),
                ),
            ):
                with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                    exit_code = cli_main(["run-pinned-gcs", "--spec-file", str(spec_path)])

        self.assertEqual(exit_code, 2)
        stderr_text = stderr.getvalue()
        payload = self._assert_sanitized_failure_envelope(stdout.getvalue(), stderr_text)
        self.assertEqual(
            payload["error_type"], "PinnedGCSStatePublicationServiceError"
        )
        self._assert_stderr_omits(stderr_text, secret_bucket, secret_exception_text)
        self._assert_no_pipeline_artifacts_created()

    def _v2_calendar_stage_fixture(
        self, *, calendar_payload: bytes, calendar_materialization_id: str, calendar_generation: object
    ):
        session = SESSION
        sm_bytes = _master_bytes()
        db_bytes = _bundle_bytes()
        calendar_request = _calendar_request_dict(
            calendar_materialization_id, calendar_payload, generation=calendar_generation
        )
        manifest_bytes = _manifest_bytes(session, sm_bytes=sm_bytes, db_bytes=db_bytes)
        expected_manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
        spec_dict = _run_spec_dict_v2(
            session=session,
            manifest_generation=_MANIFEST_GENERATION,
            expected_manifest_sha256=expected_manifest_sha256,
            calendar_request=calendar_request,
            run_cutoff=self.run_cutoff,
        )
        spec_path = self._write_spec("spec-v2-calendar-failure.json", spec_dict)
        return session, sm_bytes, db_bytes, manifest_bytes, spec_path, calendar_request

    def test_v2_wrong_expected_calendar_sha256_downloads_only_calendar_and_publishes_nothing(
        self,
    ) -> None:
        calendar_payload, calendar_materialization_id = _build_v2_calendar_bytes(
            root=self.root,
            coverage_start=date(2026, 7, 14),
            coverage_end=date(2026, 7, 15),
            cutoff=_CALENDAR_CUTOFF,
            document_id="CMTR-E2E-V2-WRONG-HASH",
        )
        (
            session,
            sm_bytes,
            db_bytes,
            manifest_bytes,
            spec_path,
            calendar_request,
        ) = self._v2_calendar_stage_fixture(
            calendar_payload=calendar_payload,
            calendar_materialization_id=calendar_materialization_id,
            calendar_generation=_CALENDAR_GENERATION,
        )
        wrong_hash = hashlib.sha256(b"not-the-real-calendar-bytes").hexdigest()
        # Overwrite the spec's own declared expected_sha256 to a value that
        # disagrees with the bytes the fake SDK will actually serve.
        spec_dict = json.loads(spec_path.read_text(encoding="utf-8"))
        spec_dict["calendar_request"]["expected_sha256"] = wrong_hash
        spec_path.write_text(json.dumps(spec_dict, separators=(",", ":")), encoding="utf-8")

        fake_client = _FakeStorageClient(
            objects={
                (_calendar_object_name(calendar_materialization_id), _CALENDAR_GENERATION): (
                    _CALENDAR_GENERATION,
                    calendar_payload,
                ),
                (_manifest_object_name(session), _MANIFEST_GENERATION): (
                    _MANIFEST_GENERATION,
                    manifest_bytes,
                ),
                (_sm_object_name(session), _SM_GENERATION): (_SM_GENERATION, sm_bytes),
                (_db_object_name(session), _DB_GENERATION): (_DB_GENERATION, db_bytes),
            }
        )

        exit_code, stdout, stderr, fake_storage = self._run_cli_v2(spec_path, fake_client)

        self.assertEqual(exit_code, 2)
        payload = self._assert_sanitized_failure_envelope(stdout, stderr)
        self.assertEqual(
            payload["error_type"], "PinnedGCSStatePublicationServiceError"
        )
        self.assertEqual(
            [entry["object_name"] for entry in fake_client.download_log],
            [_calendar_object_name(calendar_materialization_id)],
        )
        self.assertEqual(fake_storage.client_construction_count, 1)
        self._assert_stderr_omits(
            stderr, _BUCKET, wrong_hash, calendar_materialization_id
        )
        self._assert_no_pipeline_artifacts_created()
        self._assert_calendar_id_absent_from_local_store(calendar_materialization_id)

    def test_v2_wrong_observed_calendar_generation_downloads_only_calendar_and_publishes_nothing(
        self,
    ) -> None:
        calendar_payload, calendar_materialization_id = _build_v2_calendar_bytes(
            root=self.root,
            coverage_start=date(2026, 7, 14),
            coverage_end=date(2026, 7, 15),
            cutoff=_CALENDAR_CUTOFF,
            document_id="CMTR-E2E-V2-WRONG-GENERATION",
        )
        (
            session,
            sm_bytes,
            db_bytes,
            manifest_bytes,
            spec_path,
            calendar_request,
        ) = self._v2_calendar_stage_fixture(
            calendar_payload=calendar_payload,
            calendar_materialization_id=calendar_materialization_id,
            calendar_generation=_CALENDAR_GENERATION,
        )
        wrong_observed_generation = 999999

        fake_client = _FakeStorageClient(
            objects={
                # Registered at the requested generation, but the simulated
                # SDK reports a mismatched post-download blob.generation.
                (_calendar_object_name(calendar_materialization_id), _CALENDAR_GENERATION): (
                    wrong_observed_generation,
                    calendar_payload,
                ),
                (_manifest_object_name(session), _MANIFEST_GENERATION): (
                    _MANIFEST_GENERATION,
                    manifest_bytes,
                ),
                (_sm_object_name(session), _SM_GENERATION): (_SM_GENERATION, sm_bytes),
                (_db_object_name(session), _DB_GENERATION): (_DB_GENERATION, db_bytes),
            }
        )

        exit_code, stdout, stderr, fake_storage = self._run_cli_v2(spec_path, fake_client)

        self.assertEqual(exit_code, 2)
        payload = self._assert_sanitized_failure_envelope(stdout, stderr)
        self.assertEqual(
            payload["error_type"], "PinnedGCSStatePublicationServiceError"
        )
        self.assertEqual(
            [entry["object_name"] for entry in fake_client.download_log],
            [_calendar_object_name(calendar_materialization_id)],
        )
        self.assertEqual(fake_storage.client_construction_count, 1)
        self._assert_stderr_omits(
            stderr, _BUCKET, calendar_materialization_id, str(wrong_observed_generation)
        )
        self._assert_no_pipeline_artifacts_created()
        self._assert_calendar_id_absent_from_local_store(calendar_materialization_id)

    def test_v2_malformed_calendar_bytes_downloads_only_calendar_and_publishes_nothing(
        self,
    ) -> None:
        session = SESSION
        sm_bytes = _master_bytes()
        db_bytes = _bundle_bytes()
        # Well-formed-hex materialization_id that never corresponds to a
        # real materialization; the served bytes are simply not canonical
        # calendar-materialization JSON at all, paired with THEIR OWN exact
        # hash, so the hash check itself passes and the strict canonical-
        # decode check is what fails.
        malformed_bytes = b"not-canonical-calendar-materialization-bytes"
        fake_materialization_id = "e" * 64
        calendar_request = {
            "bucket": _BUCKET,
            "object_name": _calendar_object_name(fake_materialization_id),
            "generation": _CALENDAR_GENERATION,
            "expected_sha256": hashlib.sha256(malformed_bytes).hexdigest(),
            "materialization_id": fake_materialization_id,
        }
        manifest_bytes = _manifest_bytes(session, sm_bytes=sm_bytes, db_bytes=db_bytes)
        expected_manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
        spec_dict = _run_spec_dict_v2(
            session=session,
            manifest_generation=_MANIFEST_GENERATION,
            expected_manifest_sha256=expected_manifest_sha256,
            calendar_request=calendar_request,
            run_cutoff=self.run_cutoff,
        )
        spec_path = self._write_spec("spec-v2-malformed-calendar.json", spec_dict)

        fake_client = _FakeStorageClient(
            objects={
                (_calendar_object_name(fake_materialization_id), _CALENDAR_GENERATION): (
                    _CALENDAR_GENERATION,
                    malformed_bytes,
                ),
                (_manifest_object_name(session), _MANIFEST_GENERATION): (
                    _MANIFEST_GENERATION,
                    manifest_bytes,
                ),
                (_sm_object_name(session), _SM_GENERATION): (_SM_GENERATION, sm_bytes),
                (_db_object_name(session), _DB_GENERATION): (_DB_GENERATION, db_bytes),
            }
        )

        exit_code, stdout, stderr, fake_storage = self._run_cli_v2(spec_path, fake_client)

        self.assertEqual(exit_code, 2)
        payload = self._assert_sanitized_failure_envelope(stdout, stderr)
        self.assertEqual(
            payload["error_type"], "PinnedGCSStatePublicationServiceError"
        )
        self.assertEqual(
            [entry["object_name"] for entry in fake_client.download_log],
            [_calendar_object_name(fake_materialization_id)],
        )
        self.assertEqual(fake_storage.client_construction_count, 1)
        self._assert_stderr_omits(stderr, _BUCKET, fake_materialization_id)
        self._assert_no_pipeline_artifacts_created()
        self._assert_calendar_id_absent_from_local_store(fake_materialization_id)


class EnvironmentIsolationTests(_EndToEndFixtureTestCase):
    def test_configured_roots_are_distinct_temp_descendants_never_the_repo_var_path(
        self,
    ) -> None:
        temp_root_canon = _canon(self.root)
        canon_by_env_name: dict[str, str] = {}
        for env_name, path_str in self.env.items():
            self.assertTrue(os.path.isabs(path_str), env_name)
            canon = _canon(path_str)
            self.assertTrue(
                canon == temp_root_canon or canon.startswith(temp_root_canon + os.sep),
                f"{env_name} does not resolve under the TemporaryDirectory root",
            )
            canon_by_env_name[env_name] = canon

        # Every configured root is a distinct path -- no two stores share a
        # directory.
        self.assertEqual(len(set(canon_by_env_name.values())), len(canon_by_env_name))

        # None of them can resolve to this repository's real var/ default,
        # proven by pure lexical comparison (_canon never touches the
        # filesystem) -- no repository var path is read or written here.
        for env_name, default_relative in _REPO_VAR_DEFAULTS.items():
            self.assertNotEqual(canon_by_env_name[env_name], _canon(default_relative))


class FakeStorageStackCapabilityTests(unittest.TestCase):
    """Structural proof that no member of the fake SDK stack -- the module
    stand-in, the client, the bucket, or the blob -- exposes a listing or
    latest-selection-shaped capability. Covers the types themselves, not
    only one client instance, so a future addition of such a member to any
    of the four classes fails this test regardless of which instance a test
    happens to construct.
    """

    def test_no_list_or_latest_capability_anywhere_in_the_fake_stack(self) -> None:
        for candidate in (_FakeStorageModule, _FakeStorageClient, _FakeBucket, _FakeBlob):
            members = [name for name in dir(candidate) if not name.startswith("__")]
            self.assertFalse(
                any("list" in name.lower() or "latest" in name.lower() for name in members),
                f"{candidate!r} unexpectedly exposes a listing/latest-shaped member",
            )


if __name__ == "__main__":
    unittest.main()
