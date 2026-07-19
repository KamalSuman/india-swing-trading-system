from __future__ import annotations

import ast
import dataclasses
import hashlib
import json
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from india_swing.daily_pipeline import (
    DailyLandingJobError,
    DailyPipelineIntegrityError,
    LocalDailyPipelineRunStore,
    run_daily_pipeline_from_landing_manifest,
)
from india_swing.daily_pipeline.acquisition import AcquiredFile, AcquisitionFileType
from india_swing.daily_pipeline.landing_manifest import (
    LandingManifestVerifier,
    TrustedLandingManifestBinding,
)
from india_swing.daily_pipeline.models import VERIFIED_LANDING_LINEAGE_UNAVAILABLE
from india_swing.daily_reports.artifact_store import LocalDailyBundleArtifactStore
from india_swing.historical_prices.artifact_store import LocalHistoricalPriceArtifactStore
from india_swing.identity_registry.adjudication_store import LocalIdentityAdjudicationQueueStore
from india_swing.identity_registry.artifact_store import LocalIdentityRegistryStore
from india_swing.reference_data.artifact_store import LocalReferenceArtifactStore

from tests.test_reconciliation import (
    BUNDLE_VALIDATED,
    CUTOFF,
    MASTER_VALIDATED,
    SESSION,
    _bundle_bytes,
    _calendar,
    _master_bytes,
)

_BUCKET = "trusted-landing-bucket"
_NOT_BEFORE = datetime(2026, 7, 15, 0, 0, 0, tzinfo=timezone.utc)
_KNOWLEDGE_TIME = "2026-07-15T13:00:00Z"
_BINDING_CUTOFF = datetime(2026, 7, 15, 14, 0, 0, tzinfo=timezone.utc)
_CALENDAR_MATERIALIZATION_ID = "8" * 64


def _clock_at(value: datetime):
    return lambda: value


def _sm_object_name(session: date = SESSION) -> str:
    return f"landing/{session.isoformat()}/NSE_CM_security_{session.strftime('%d%m%Y')}.csv.gz"


def _db_object_name(session: date = SESSION) -> str:
    return f"landing/{session.isoformat()}/Reports-Daily-Multiple.zip"


class FakeLandingObjectReader:
    """Fake LandingObjectReader. Never lists/latest; records every call made
    in order.
    """

    def __init__(
        self,
        *,
        responses: dict[AcquisitionFileType, object] | None = None,
        raises: dict[AcquisitionFileType, BaseException] | None = None,
    ) -> None:
        self._responses = responses or {}
        self._raises = raises or {}
        self.calls: list[object] = []

    def read(self, request: object) -> object:
        self.calls.append(request)
        if request.file_type in self._raises:
            raise self._raises[request.file_type]
        return self._responses[request.file_type]


def _build_manifest_bytes(
    *,
    target_session: date = SESSION,
    knowledge_time: str = _KNOWLEDGE_TIME,
    bucket: str = _BUCKET,
    sm_sha256: str | None = None,
    db_sha256: str | None = None,
) -> bytes:
    sm_sha256 = sm_sha256 or hashlib.sha256(_master_bytes()).hexdigest()
    db_sha256 = db_sha256 or hashlib.sha256(_bundle_bytes()).hexdigest()
    manifest_dict = {
        "schema_version": 1,
        "knowledge_time": knowledge_time,
        "target_session": target_session.isoformat(),
        "objects": [
            {
                "file_type": "SECURITY_MASTER",
                "bucket": bucket,
                "object_name": _sm_object_name(target_session),
                "generation": 111,
                "sha256": sm_sha256,
            },
            {
                "file_type": "DAILY_BUNDLE",
                "bucket": bucket,
                "object_name": _db_object_name(target_session),
                "generation": 222,
                "sha256": db_sha256,
            },
        ],
    }
    return json.dumps(manifest_dict, separators=(",", ":")).encode("utf-8")


def _binding_for(
    manifest_bytes: bytes,
    *,
    target_session: date = SESSION,
    bucket: str = _BUCKET,
    not_before: datetime = _NOT_BEFORE,
    cutoff: datetime = _BINDING_CUTOFF,
) -> TrustedLandingManifestBinding:
    return TrustedLandingManifestBinding(
        expected_manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        allowed_bucket=bucket,
        target_session=target_session,
        not_before=not_before,
        cutoff=cutoff,
    )


def _manifest_and_objects(
    *, target_session: date = SESSION, knowledge_time: str = _KNOWLEDGE_TIME
) -> tuple[bytes, TrustedLandingManifestBinding, AcquiredFile, AcquiredFile]:
    master_bytes = _master_bytes()
    bundle_bytes = _bundle_bytes()
    sm_sha256 = hashlib.sha256(master_bytes).hexdigest()
    db_sha256 = hashlib.sha256(bundle_bytes).hexdigest()
    manifest_bytes = _build_manifest_bytes(
        target_session=target_session,
        knowledge_time=knowledge_time,
        sm_sha256=sm_sha256,
        db_sha256=db_sha256,
    )
    binding = _binding_for(manifest_bytes, target_session=target_session)
    manifest = LandingManifestVerifier().verify(manifest_bytes, binding)
    sm_acquired = AcquiredFile(
        bucket=manifest.security_master.bucket,
        object_name=manifest.security_master.object_name,
        generation=manifest.security_master.generation,
        target_session=target_session,
        file_type=AcquisitionFileType.SECURITY_MASTER,
        content_bytes=master_bytes,
        sha256_hash=sm_sha256,
    )
    db_acquired = AcquiredFile(
        bucket=manifest.daily_bundle.bucket,
        object_name=manifest.daily_bundle.object_name,
        generation=manifest.daily_bundle.generation,
        target_session=target_session,
        file_type=AcquisitionFileType.DAILY_BUNDLE,
        content_bytes=bundle_bytes,
        sha256_hash=db_sha256,
    )
    return manifest_bytes, binding, sm_acquired, db_acquired


def _valid_reader(sm_acquired: AcquiredFile, db_acquired: AcquiredFile) -> FakeLandingObjectReader:
    return FakeLandingObjectReader(
        responses={
            AcquisitionFileType.SECURITY_MASTER: sm_acquired,
            AcquisitionFileType.DAILY_BUNDLE: db_acquired,
        }
    )


def _manifest_and_reader(
    *, target_session: date = SESSION, knowledge_time: str = _KNOWLEDGE_TIME
) -> tuple[bytes, TrustedLandingManifestBinding, FakeLandingObjectReader]:
    manifest_bytes, binding, sm_acquired, db_acquired = _manifest_and_objects(
        target_session=target_session, knowledge_time=knowledge_time
    )
    return manifest_bytes, binding, _valid_reader(sm_acquired, db_acquired)


class LandingJobTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.reference_store = LocalReferenceArtifactStore(
            self.root / "reference", clock=_clock_at(MASTER_VALIDATED)
        )
        self.daily_store = LocalDailyBundleArtifactStore(
            self.root / "daily", clock=_clock_at(BUNDLE_VALIDATED)
        )
        self.historical_store = LocalHistoricalPriceArtifactStore(
            self.root / "history", self.root / "daily"
        )
        self.identity_store = LocalIdentityRegistryStore(
            self.root / "identity", self.root / "reference"
        )
        self.adjudication_store = LocalIdentityAdjudicationQueueStore(
            self.root / "identity", self.identity_store
        )
        self.run_store = LocalDailyPipelineRunStore(self.root / "pipeline")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _run_job(self, manifest_bytes: object, binding: object, reader: object, **overrides: object):
        kwargs = dict(
            manifest_bytes=manifest_bytes,
            binding=binding,
            reader=reader,
            market_session=SESSION,
            cutoff=CUTOFF,
            calendar_materialization_id=_CALENDAR_MATERIALIZATION_ID,
            calendar=_calendar(),
            previous_run_id=None,
            reference_store=self.reference_store,
            daily_store=self.daily_store,
            historical_store=self.historical_store,
            identity_store=self.identity_store,
            adjudication_store=self.adjudication_store,
            run_store=self.run_store,
        )
        kwargs.update(overrides)
        return run_daily_pipeline_from_landing_manifest(**kwargs)

    def _assert_no_stores_created(self) -> None:
        self.assertFalse(self.reference_store.root.exists())
        self.assertFalse(self.daily_store.root.exists())
        self.assertFalse(self.run_store.runs_root.exists())


class LandingJobAcceptanceTests(LandingJobTestCase):
    def test_successful_composition_persists_lineage_with_pinned_generations_and_ordered_reads(
        self,
    ) -> None:
        manifest_bytes, binding, reader = _manifest_and_reader()
        expected_sm_sha256 = hashlib.sha256(_master_bytes()).hexdigest()
        expected_db_sha256 = hashlib.sha256(_bundle_bytes()).hexdigest()

        run = self._run_job(manifest_bytes, binding, reader)

        self.assertEqual(len(reader.calls), 2)
        self.assertEqual(reader.calls[0].file_type, AcquisitionFileType.SECURITY_MASTER)
        self.assertEqual(reader.calls[1].file_type, AcquisitionFileType.DAILY_BUNDLE)
        self.assertEqual(reader.calls[0].generation, 111)
        self.assertEqual(reader.calls[1].generation, 222)

        self.assertIsNotNone(run.landing_input_lineage)
        self.assertNotIn(VERIFIED_LANDING_LINEAGE_UNAVAILABLE, run.completeness_issues)
        self.assertEqual(run.landing_input_lineage.security_master.object_name, _sm_object_name())
        self.assertEqual(run.landing_input_lineage.daily_bundle.object_name, _db_object_name())
        self.assertEqual(run.landing_input_lineage.security_master.generation, 111)
        self.assertEqual(run.landing_input_lineage.daily_bundle.generation, 222)
        self.assertEqual(
            run.landing_input_lineage.security_master.sha256_hash, expected_sm_sha256
        )
        self.assertEqual(run.landing_input_lineage.daily_bundle.sha256_hash, expected_db_sha256)

        reloaded = self.run_store.get(run.run_id)
        self.assertEqual(reloaded, run)
        self.assertEqual(reloaded.landing_input_lineage, run.landing_input_lineage)
        self.assertEqual(
            reloaded.landing_input_lineage.security_master.sha256_hash, expected_sm_sha256
        )
        self.assertEqual(
            reloaded.landing_input_lineage.daily_bundle.sha256_hash, expected_db_sha256
        )


class LandingJobCreateOnceTests(LandingJobTestCase):
    def test_repeated_run_with_identical_manifest_is_idempotent(self) -> None:
        manifest_bytes, binding, reader = _manifest_and_reader()

        run_a = self._run_job(manifest_bytes, binding, reader)
        run_b = self._run_job(manifest_bytes, binding, reader)

        self.assertEqual(run_a.run_id, run_b.run_id)
        self.assertEqual(self.run_store.list_runs(), (run_a,))


class LandingJobPreviousRunTests(LandingJobTestCase):
    def test_previous_run_id_is_threaded_through_to_the_runner(self) -> None:
        manifest_bytes, binding, reader = _manifest_and_reader()
        first = self._run_job(manifest_bytes, binding, reader)

        second_manifest_bytes, second_binding, second_reader = _manifest_and_reader()
        with self.assertRaisesRegex(DailyPipelineIntegrityError, "preceding session"):
            self._run_job(
                second_manifest_bytes,
                second_binding,
                second_reader,
                previous_run_id=first.run_id,
            )


class LandingJobManifestRejectionTests(LandingJobTestCase):
    def test_tampered_manifest_bytes_fail_hash_check_before_any_read_or_store(self) -> None:
        manifest_bytes, binding, reader = _manifest_and_reader()
        tampered = manifest_bytes + b" "

        with self.assertRaises(DailyLandingJobError):
            self._run_job(tampered, binding, reader)

        self.assertEqual(reader.calls, [])
        self._assert_no_stores_created()

    def test_non_json_manifest_bytes_fail_before_any_read_or_store(self) -> None:
        manifest_bytes = b"not-json-at-all"
        binding = _binding_for(manifest_bytes, target_session=SESSION)
        reader = FakeLandingObjectReader()

        with self.assertRaises(DailyLandingJobError):
            self._run_job(manifest_bytes, binding, reader)

        self.assertEqual(reader.calls, [])
        self._assert_no_stores_created()

    def test_future_knowledge_time_fails_before_any_read_or_store(self) -> None:
        manifest_bytes = _build_manifest_bytes(knowledge_time="2026-07-15T15:00:00Z")
        binding = _binding_for(manifest_bytes, target_session=SESSION)
        reader = FakeLandingObjectReader()

        with self.assertRaises(DailyLandingJobError):
            self._run_job(manifest_bytes, binding, reader)

        self.assertEqual(reader.calls, [])
        self._assert_no_stores_created()

    def test_target_session_mismatch_fails_before_any_read_or_store(self) -> None:
        mismatched_session = SESSION + timedelta(days=1)
        manifest_bytes = _build_manifest_bytes(target_session=mismatched_session)
        binding = _binding_for(manifest_bytes, target_session=SESSION)
        reader = FakeLandingObjectReader()

        with self.assertRaises(DailyLandingJobError):
            self._run_job(manifest_bytes, binding, reader)

        self.assertEqual(reader.calls, [])
        self._assert_no_stores_created()

    def test_wrong_binding_type_fails_before_any_read_or_store(self) -> None:
        manifest_bytes, _, reader = _manifest_and_reader()

        with self.assertRaises(DailyLandingJobError):
            self._run_job(manifest_bytes, "not-a-binding", reader)

        self.assertEqual(reader.calls, [])
        self._assert_no_stores_created()


class LandingJobAcquisitionRejectionTests(LandingJobTestCase):
    def test_reader_failure_on_first_read_fails_before_any_store_write(self) -> None:
        manifest_bytes, binding, sm_acquired, db_acquired = _manifest_and_objects()
        reader = FakeLandingObjectReader(
            responses={AcquisitionFileType.DAILY_BUNDLE: db_acquired},
            raises={AcquisitionFileType.SECURITY_MASTER: ValueError("boom")},
        )

        with self.assertRaises(DailyLandingJobError):
            self._run_job(manifest_bytes, binding, reader)

        self.assertEqual(len(reader.calls), 1)
        self._assert_no_stores_created()

    def test_reader_failure_on_second_read_fails_before_any_store_write(self) -> None:
        manifest_bytes, binding, sm_acquired, db_acquired = _manifest_and_objects()
        reader = FakeLandingObjectReader(
            responses={AcquisitionFileType.SECURITY_MASTER: sm_acquired},
            raises={AcquisitionFileType.DAILY_BUNDLE: ValueError("boom")},
        )

        with self.assertRaises(DailyLandingJobError):
            self._run_job(manifest_bytes, binding, reader)

        self.assertEqual(len(reader.calls), 2)
        self._assert_no_stores_created()

    def test_reader_returns_wrong_type_fails_before_any_store_write(self) -> None:
        manifest_bytes, binding, _, db_acquired = _manifest_and_objects()
        reader = FakeLandingObjectReader(
            responses={
                AcquisitionFileType.SECURITY_MASTER: "not-an-acquired-file",
                AcquisitionFileType.DAILY_BUNDLE: db_acquired,
            }
        )

        with self.assertRaises(DailyLandingJobError):
            self._run_job(manifest_bytes, binding, reader)

        self._assert_no_stores_created()

    def test_market_session_mismatch_fails_before_any_read_or_store(self) -> None:
        manifest_bytes, binding, reader = _manifest_and_reader()
        mismatched_session = SESSION + timedelta(days=1)

        with self.assertRaises(DailyLandingJobError) as ctx:
            self._run_job(manifest_bytes, binding, reader, market_session=mismatched_session)

        self.assertEqual(str(ctx.exception), "daily landing job acquisition failed")
        self.assertEqual(reader.calls, [])
        self._assert_no_stores_created()

    def test_run_cutoff_before_trusted_binding_cutoff_fails_before_any_read_or_store(self) -> None:
        manifest_bytes, binding, reader = _manifest_and_reader()
        # Strictly between the manifest's knowledge_time (13:00) and the
        # trusted binding's own cutoff (14:00), isolating the specific
        # "run cutoff precedes binding cutoff" rejection from the also-valid
        # "run cutoff precedes knowledge_time" rejection.
        early_cutoff = datetime(2026, 7, 15, 13, 30, 0, tzinfo=timezone.utc)

        with self.assertRaises(DailyLandingJobError) as ctx:
            self._run_job(manifest_bytes, binding, reader, cutoff=early_cutoff)

        self.assertEqual(str(ctx.exception), "daily landing job acquisition failed")
        self.assertEqual(reader.calls, [])
        self._assert_no_stores_created()


class LandingJobSanitizationTests(LandingJobTestCase):
    def test_manifest_stage_failure_message_is_static_and_never_leaks_content(self) -> None:
        secret_bucket = "secret-manifest-bucket-do-not-leak-7c3d"
        manifest_bytes = _build_manifest_bytes(target_session=SESSION, bucket=secret_bucket)
        binding = _binding_for(manifest_bytes, target_session=SESSION, bucket=_BUCKET)
        reader = FakeLandingObjectReader()

        with self.assertRaises(DailyLandingJobError) as ctx:
            self._run_job(manifest_bytes, binding, reader)

        message = str(ctx.exception)
        self.assertEqual(message, "daily landing job manifest verification failed")
        self.assertNotIn(secret_bucket, message)
        self.assertEqual(reader.calls, [])

    def test_acquisition_stage_failure_message_is_static_and_never_leaks_content(self) -> None:
        manifest_bytes, binding, sm_acquired, db_acquired = _manifest_and_objects()
        secret = "SECRET-ACQUIRED-CONTENT-DO-NOT-LEAK-9f1a"
        tampered = dataclasses.replace(sm_acquired, content_bytes=secret.encode("utf-8"))
        reader = FakeLandingObjectReader(
            responses={
                AcquisitionFileType.SECURITY_MASTER: tampered,
                AcquisitionFileType.DAILY_BUNDLE: db_acquired,
            }
        )

        with self.assertRaises(DailyLandingJobError) as ctx:
            self._run_job(manifest_bytes, binding, reader)

        message = str(ctx.exception)
        self.assertEqual(message, "daily landing job acquisition failed")
        self.assertNotIn(secret, message)


class LandingJobNonExactShapeTests(LandingJobTestCase):
    def test_non_bytes_manifest_bytes_fails(self) -> None:
        _, binding, reader = _manifest_and_reader()

        with self.assertRaises(DailyLandingJobError):
            self._run_job("not-bytes", binding, reader)

        self.assertEqual(reader.calls, [])
        self._assert_no_stores_created()

    def test_reader_without_read_method_fails(self) -> None:
        manifest_bytes, binding, _ = _manifest_and_reader()

        with self.assertRaises(DailyLandingJobError):
            self._run_job(manifest_bytes, binding, object())

        self._assert_no_stores_created()


_EXACT_ALLOWED_JOB_IMPORTS = frozenset((
    # (level, module, imported name, asname); level 0 covers both plain
    # `import x` (module=alias.name, imported name=None) and absolute
    # `from x import y`. Level > 0 covers `from .x import y` within the
    # daily_pipeline package. This is a closed set: any import in
    # landing_job.py not exactly in this set, and any entry here missing
    # from landing_job.py, fails the equality assertion below.
    (0, "__future__", "annotations", None),
    (0, "datetime", "date", None),
    (0, "datetime", "datetime", None),
    (0, "india_swing.daily_reports.artifact_store", "LocalDailyBundleArtifactStore", None),
    (0, "india_swing.historical_prices.artifact_store", "LocalHistoricalPriceArtifactStore", None),
    (0, "india_swing.identity_registry.adjudication_store", "LocalIdentityAdjudicationQueueStore", None),
    (0, "india_swing.identity_registry.artifact_store", "LocalIdentityRegistryStore", None),
    (0, "india_swing.reference.calendar", "CalendarSnapshot", None),
    (0, "india_swing.reference_data.artifact_store", "LocalReferenceArtifactStore", None),
    (1, "landing_inputs", "LandingObjectReader", None),
    (1, "landing_inputs", "acquire_verified_landing_inputs", None),
    (1, "landing_manifest", "LandingManifestVerifier", None),
    (1, "landing_manifest", "TrustedLandingManifestBinding", None),
    (1, "models", "DailyPipelineRun", None),
    (1, "runner", "run_daily_pipeline_from_landing_inputs", None),
    (1, "store", "LocalDailyPipelineRunStore", None),
))

_EXACT_ALLOWED_JOB_CALL_TARGETS = frozenset((
    # The production module's entire callable surface: constructing the
    # manifest verifier and calling .verify() on it (stage 1), raising the
    # dedicated error type at either trust boundary, and invoking the two
    # remaining composition-stage functions (stages 2 and 3). Any other
    # call name -- a GCS/storage client, requests/urllib, os/subprocess,
    # a broker/order/notification helper, a strategy/model/LLM call, a
    # listing/"latest" helper, or a retry/fallback wrapper -- fails this
    # test.
    "LandingManifestVerifier",
    "verify",
    "DailyLandingJobError",
    "acquire_verified_landing_inputs",
    "run_daily_pipeline_from_landing_inputs",
))

_FORBIDDEN_JOB_NAME_TOKENS = (
    "gcs",
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
)


class LandingJobCapabilityTests(unittest.TestCase):
    """Proves landing_job.py introduces no GCS/client construction, network,
    filesystem discovery, environment, current-clock, listing/latest
    selection, retry/fallback, subprocess, notification, broker, order,
    strategy, model, or LLM capability. Imports are confined to the existing
    daily-pipeline composition dependencies and type-only standard-library
    needs.
    """

    def _job_ast(self) -> ast.Module:
        source = (
            Path(__file__).resolve().parent.parent
            / "src"
            / "india_swing"
            / "daily_pipeline"
            / "landing_job.py"
        ).read_text(encoding="utf-8")
        return ast.parse(source)

    def test_job_imports_match_an_exact_allowlist(self) -> None:
        tree = self._job_ast()
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
        self.assertEqual(actual, _EXACT_ALLOWED_JOB_IMPORTS)

    def test_job_callable_surface_is_locked_to_composition_calls(self) -> None:
        tree = self._job_ast()
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
            if target not in _EXACT_ALLOWED_JOB_CALL_TARGETS:
                offenders.append(target)
        self.assertEqual(offenders, [])

    def test_job_identifiers_carry_no_disallowed_capability_token(self) -> None:
        tree = self._job_ast()
        offenders = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Name):
                candidate = node.id.lower()
            elif isinstance(node, ast.Attribute):
                candidate = node.attr.lower()
            else:
                continue
            for token in _FORBIDDEN_JOB_NAME_TOKENS:
                if token in candidate:
                    offenders.append(candidate)
        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
