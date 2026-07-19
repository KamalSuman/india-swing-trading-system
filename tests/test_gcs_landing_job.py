from __future__ import annotations

import ast
import hashlib
import json
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from india_swing.daily_pipeline import DailyPipelineRun, LocalDailyPipelineRunStore
from india_swing.daily_pipeline.acquisition import GCSObjectPayload, LandingManifestObjectRequest
from india_swing.daily_pipeline.gcs_landing_job import (
    PinnedGCSLandingJobError,
    run_daily_pipeline_from_pinned_gcs_manifest,
)
from india_swing.daily_pipeline.landing_lineage import LANDING_INPUT_LINEAGE_SCHEMA_VERSION
from india_swing.daily_pipeline.landing_manifest import (
    MAXIMUM_LANDING_MANIFEST_BYTES,
    TrustedLandingManifestBinding,
)
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
_MANIFEST_GENERATION = 777
_SM_GENERATION = 111
_DB_GENERATION = 222
_SECURITY_MASTER_MAXIMUM_BYTES = 32 * 1024 * 1024
_DAILY_BUNDLE_MAXIMUM_BYTES = 128 * 1024 * 1024


def _clock_at(value: datetime):
    return lambda: value


def _sm_object_name(session: date = SESSION) -> str:
    return f"landing/{session.isoformat()}/NSE_CM_security_{session.strftime('%d%m%Y')}.csv.gz"


def _db_object_name(session: date = SESSION) -> str:
    return f"landing/{session.isoformat()}/Reports-Daily-Multiple.zip"


def _manifest_object_name(session: date = SESSION) -> str:
    return f"landing/{session.isoformat()}/landing-manifest.json"


def _manifest_bytes(
    *, sm_generation: int = _SM_GENERATION, db_generation: int = _DB_GENERATION
) -> bytes:
    manifest_dict = {
        "schema_version": 1,
        "knowledge_time": _KNOWLEDGE_TIME,
        "target_session": SESSION.isoformat(),
        "objects": [
            {
                "file_type": "SECURITY_MASTER",
                "bucket": _BUCKET,
                "object_name": _sm_object_name(),
                "generation": sm_generation,
                "sha256": hashlib.sha256(_master_bytes()).hexdigest(),
            },
            {
                "file_type": "DAILY_BUNDLE",
                "bucket": _BUCKET,
                "object_name": _db_object_name(),
                "generation": db_generation,
                "sha256": hashlib.sha256(_bundle_bytes()).hexdigest(),
            },
        ],
    }
    return json.dumps(manifest_dict, separators=(",", ":")).encode("utf-8")


def _binding_for(manifest_bytes: bytes) -> TrustedLandingManifestBinding:
    return TrustedLandingManifestBinding(
        expected_manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        allowed_bucket=_BUCKET,
        target_session=SESSION,
        not_before=_NOT_BEFORE,
        cutoff=_BINDING_CUTOFF,
    )


def _manifest_request(*, generation: int = _MANIFEST_GENERATION) -> LandingManifestObjectRequest:
    return LandingManifestObjectRequest(
        bucket=_BUCKET,
        object_name=_manifest_object_name(),
        generation=generation,
        target_session=SESSION,
    )


class FakeGCSObjectReader:
    """Fake GCSObjectReader. Never contacts GCP; records every call made in
    order; returns or raises a caller-configured value keyed by object_name.
    """

    def __init__(
        self,
        *,
        responses: dict[str, object] | None = None,
        raises: dict[str, BaseException] | None = None,
    ) -> None:
        self._responses = responses or {}
        self._raises = raises or {}
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
        if object_name in self._raises:
            raise self._raises[object_name]
        return self._responses[object_name]


def _valid_reader(
    *,
    manifest_bytes: bytes,
    manifest_generation: int = _MANIFEST_GENERATION,
    sm_generation: int = _SM_GENERATION,
    db_generation: int = _DB_GENERATION,
) -> FakeGCSObjectReader:
    return FakeGCSObjectReader(
        responses={
            _manifest_object_name(): GCSObjectPayload(
                content_bytes=manifest_bytes, generation=manifest_generation
            ),
            _sm_object_name(): GCSObjectPayload(
                content_bytes=_master_bytes(), generation=sm_generation
            ),
            _db_object_name(): GCSObjectPayload(
                content_bytes=_bundle_bytes(), generation=db_generation
            ),
        }
    )


class GCSLandingJobTestCase(unittest.TestCase):
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

    def _run_job(self, manifest_request: object, binding: object, reader: object, **overrides: object):
        kwargs = dict(
            manifest_request=manifest_request,
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
        return run_daily_pipeline_from_pinned_gcs_manifest(**kwargs)

    def _assert_no_stores_created(self) -> None:
        # self.root is a TemporaryDirectory dedicated solely to the six
        # injected stores (reference, daily, history, identity/adjudication,
        # pipeline); asserting it has no children at all proves none of
        # them -- not just reference_store/daily_store/run_store -- wrote
        # anything, without needing to know each store's private layout.
        self.assertEqual(list(self.root.iterdir()), [])


class GCSLandingJobAcceptanceTests(GCSLandingJobTestCase):
    def test_success_issues_exactly_three_pinned_reads_in_order_with_exact_arguments(self) -> None:
        manifest_bytes = _manifest_bytes()
        binding = _binding_for(manifest_bytes)
        reader = _valid_reader(manifest_bytes=manifest_bytes)

        run = self._run_job(_manifest_request(), binding, reader)

        self.assertIsInstance(run, DailyPipelineRun)
        self.assertEqual(
            reader.calls,
            [
                {
                    "bucket": _BUCKET,
                    "object_name": _manifest_object_name(),
                    "generation": _MANIFEST_GENERATION,
                    "maximum_bytes": MAXIMUM_LANDING_MANIFEST_BYTES,
                },
                {
                    "bucket": _BUCKET,
                    "object_name": _sm_object_name(),
                    "generation": _SM_GENERATION,
                    "maximum_bytes": _SECURITY_MASTER_MAXIMUM_BYTES,
                },
                {
                    "bucket": _BUCKET,
                    "object_name": _db_object_name(),
                    "generation": _DB_GENERATION,
                    "maximum_bytes": _DAILY_BUNDLE_MAXIMUM_BYTES,
                },
            ],
        )

    def test_success_produces_v2_lineage_with_exact_manifest_source_and_data_objects(self) -> None:
        manifest_bytes = _manifest_bytes()
        binding = _binding_for(manifest_bytes)
        reader = _valid_reader(manifest_bytes=manifest_bytes)

        run = self._run_job(_manifest_request(), binding, reader)

        lineage = run.landing_input_lineage
        self.assertIsNotNone(lineage)
        self.assertEqual(lineage.schema_version, LANDING_INPUT_LINEAGE_SCHEMA_VERSION)
        self.assertIsNotNone(lineage.manifest_source)
        self.assertEqual(lineage.manifest_source.bucket, _BUCKET)
        self.assertEqual(lineage.manifest_source.object_name, _manifest_object_name())
        self.assertEqual(lineage.manifest_source.generation, _MANIFEST_GENERATION)
        self.assertEqual(lineage.manifest_source.target_session, SESSION)
        self.assertEqual(lineage.security_master.object_name, _sm_object_name())
        self.assertEqual(lineage.security_master.generation, _SM_GENERATION)
        self.assertEqual(lineage.daily_bundle.object_name, _db_object_name())
        self.assertEqual(lineage.daily_bundle.generation, _DB_GENERATION)

        reloaded = self.run_store.get(run.run_id)
        self.assertEqual(reloaded, run)
        self.assertEqual(reloaded.landing_input_lineage.manifest_source, lineage.manifest_source)

    def test_changing_only_manifest_generation_changes_lineage_id_and_run_id(self) -> None:
        manifest_bytes = _manifest_bytes()
        binding = _binding_for(manifest_bytes)

        run_a = self._run_job(
            _manifest_request(generation=777),
            binding,
            _valid_reader(manifest_bytes=manifest_bytes, manifest_generation=777),
        )
        run_b = self._run_job(
            _manifest_request(generation=888),
            binding,
            _valid_reader(manifest_bytes=manifest_bytes, manifest_generation=888),
        )

        self.assertNotEqual(
            run_a.landing_input_lineage.lineage_id, run_b.landing_input_lineage.lineage_id
        )
        self.assertNotEqual(run_a.run_id, run_b.run_id)
        self.assertEqual(
            run_a.landing_input_lineage.manifest_sha256, run_b.landing_input_lineage.manifest_sha256
        )
        self.assertEqual(
            run_a.landing_input_lineage.security_master, run_b.landing_input_lineage.security_master
        )
        self.assertEqual(
            run_a.landing_input_lineage.daily_bundle, run_b.landing_input_lineage.daily_bundle
        )
        self.assertEqual(
            run_a.current_security_master_artifact_id, run_b.current_security_master_artifact_id
        )
        self.assertEqual(
            run_a.current_daily_bundle_artifact_id, run_b.current_daily_bundle_artifact_id
        )


class GCSLandingJobManifestStageRejectionTests(GCSLandingJobTestCase):
    def test_wrong_request_type_fails_before_any_read_or_store(self) -> None:
        manifest_bytes = _manifest_bytes()
        binding = _binding_for(manifest_bytes)
        reader = _valid_reader(manifest_bytes=manifest_bytes)

        with self.assertRaises(PinnedGCSLandingJobError):
            self._run_job("not-a-request", binding, reader)

        self.assertEqual(reader.calls, [])
        self._assert_no_stores_created()

    def test_wrong_binding_type_fails_before_any_read_or_store(self) -> None:
        reader = _valid_reader(manifest_bytes=_manifest_bytes())

        with self.assertRaises(PinnedGCSLandingJobError):
            self._run_job(_manifest_request(), "not-a-binding", reader)

        self.assertEqual(reader.calls, [])
        self._assert_no_stores_created()

    def test_bucket_binding_mismatch_fails_before_any_read_or_store(self) -> None:
        manifest_bytes = _manifest_bytes()
        binding = _binding_for(manifest_bytes)
        mismatched_request = LandingManifestObjectRequest(
            bucket="another-syntactically-valid-bucket",
            object_name=_manifest_object_name(),
            generation=_MANIFEST_GENERATION,
            target_session=SESSION,
        )
        reader = _valid_reader(manifest_bytes=manifest_bytes)

        with self.assertRaises(PinnedGCSLandingJobError):
            self._run_job(mismatched_request, binding, reader)

        self.assertEqual(reader.calls, [])
        self._assert_no_stores_created()

    def test_manifest_hash_mismatch_fails_before_any_data_object_read_or_store(self) -> None:
        manifest_bytes = _manifest_bytes()
        tampered_bytes = manifest_bytes + b" "
        binding = _binding_for(manifest_bytes)
        reader = FakeGCSObjectReader(
            responses={
                _manifest_object_name(): GCSObjectPayload(
                    content_bytes=tampered_bytes, generation=_MANIFEST_GENERATION
                )
            }
        )

        with self.assertRaises(PinnedGCSLandingJobError):
            self._run_job(_manifest_request(), binding, reader)

        self.assertEqual(len(reader.calls), 1)
        self._assert_no_stores_created()

    def test_manifest_reader_failure_fails_before_any_data_object_read_or_store(self) -> None:
        manifest_bytes = _manifest_bytes()
        binding = _binding_for(manifest_bytes)
        reader = FakeGCSObjectReader(raises={_manifest_object_name(): ValueError("boom")})

        with self.assertRaises(PinnedGCSLandingJobError):
            self._run_job(_manifest_request(), binding, reader)

        self.assertEqual(len(reader.calls), 1)
        self._assert_no_stores_created()

    def test_manifest_generation_mismatch_fails_before_any_data_object_read_or_store(self) -> None:
        manifest_bytes = _manifest_bytes()
        binding = _binding_for(manifest_bytes)
        reader = FakeGCSObjectReader(
            responses={
                _manifest_object_name(): GCSObjectPayload(
                    content_bytes=manifest_bytes, generation=999999
                )
            }
        )

        with self.assertRaises(PinnedGCSLandingJobError):
            self._run_job(_manifest_request(), binding, reader)

        self.assertEqual(len(reader.calls), 1)
        self._assert_no_stores_created()

    def test_manifest_verification_failure_after_valid_payload_fails_before_any_data_object_read_or_store(
        self,
    ) -> None:
        # The payload itself is exactly what acquisition's own checks
        # require: it matches the requested generation, and binding's
        # expected SHA-256 is computed from these exact bytes, so the
        # payload-type/generation/hash checks inside
        # acquire_verified_landing_manifest all pass. Only
        # LandingManifestVerifier.verify -- the fourth, distinct stage --
        # can reject it, since the bytes are not valid manifest JSON.
        malformed_manifest_bytes = b'{"schema_version": 1, "objects": ['
        binding = _binding_for(malformed_manifest_bytes)
        reader = FakeGCSObjectReader(
            responses={
                _manifest_object_name(): GCSObjectPayload(
                    content_bytes=malformed_manifest_bytes, generation=_MANIFEST_GENERATION
                )
            }
        )

        with self.assertRaises(PinnedGCSLandingJobError) as ctx:
            self._run_job(_manifest_request(), binding, reader)

        self.assertEqual(str(ctx.exception), "pinned gcs landing job manifest acquisition failed")
        self.assertEqual(len(reader.calls), 1)
        self._assert_no_stores_created()


class GCSLandingJobDataObjectStageRejectionTests(GCSLandingJobTestCase):
    def test_security_master_failure_permits_no_daily_bundle_read_or_store(self) -> None:
        manifest_bytes = _manifest_bytes()
        binding = _binding_for(manifest_bytes)
        reader = FakeGCSObjectReader(
            responses={
                _manifest_object_name(): GCSObjectPayload(
                    content_bytes=manifest_bytes, generation=_MANIFEST_GENERATION
                )
            },
            raises={_sm_object_name(): ValueError("boom")},
        )

        with self.assertRaises(PinnedGCSLandingJobError):
            self._run_job(_manifest_request(), binding, reader)

        self.assertEqual(len(reader.calls), 2)
        self._assert_no_stores_created()

    def test_daily_bundle_failure_permits_no_store(self) -> None:
        manifest_bytes = _manifest_bytes()
        binding = _binding_for(manifest_bytes)
        reader = FakeGCSObjectReader(
            responses={
                _manifest_object_name(): GCSObjectPayload(
                    content_bytes=manifest_bytes, generation=_MANIFEST_GENERATION
                ),
                _sm_object_name(): GCSObjectPayload(
                    content_bytes=_master_bytes(), generation=_SM_GENERATION
                ),
            },
            raises={_db_object_name(): ValueError("boom")},
        )

        with self.assertRaises(PinnedGCSLandingJobError):
            self._run_job(_manifest_request(), binding, reader)

        self.assertEqual(len(reader.calls), 3)
        self._assert_no_stores_created()

    def test_market_session_mismatch_after_valid_manifest_read_permits_no_data_object_read_or_store(
        self,
    ) -> None:
        manifest_bytes = _manifest_bytes()
        binding = _binding_for(manifest_bytes)
        reader = _valid_reader(manifest_bytes=manifest_bytes)

        with self.assertRaises(PinnedGCSLandingJobError):
            self._run_job(
                _manifest_request(), binding, reader, market_session=SESSION + timedelta(days=1)
            )

        self.assertEqual(len(reader.calls), 1)
        self._assert_no_stores_created()

    def test_cutoff_before_manifest_temporal_window_permits_no_data_object_read_or_store(
        self,
    ) -> None:
        manifest_bytes = _manifest_bytes()
        binding = _binding_for(manifest_bytes)
        reader = _valid_reader(manifest_bytes=manifest_bytes)
        early_cutoff = datetime(2026, 7, 15, 10, 0, 0, tzinfo=timezone.utc)

        with self.assertRaises(PinnedGCSLandingJobError):
            self._run_job(_manifest_request(), binding, reader, cutoff=early_cutoff)

        self.assertEqual(len(reader.calls), 1)
        self._assert_no_stores_created()


class GCSLandingJobSanitizationTests(GCSLandingJobTestCase):
    def test_manifest_stage_error_message_is_static_and_never_leaks_content(self) -> None:
        secret_bucket = "secret-manifest-bucket-do-not-leak-1a2b"
        manifest_bytes = _manifest_bytes()
        mismatched_request = LandingManifestObjectRequest(
            bucket=secret_bucket,
            object_name=_manifest_object_name(),
            generation=_MANIFEST_GENERATION,
            target_session=SESSION,
        )
        binding = _binding_for(manifest_bytes)
        reader = _valid_reader(manifest_bytes=manifest_bytes)

        with self.assertRaises(PinnedGCSLandingJobError) as ctx:
            self._run_job(mismatched_request, binding, reader)

        message = str(ctx.exception)
        self.assertEqual(message, "pinned gcs landing job manifest acquisition failed")
        self.assertNotIn(secret_bucket, message)
        self.assertEqual(reader.calls, [])

    def test_manifest_stage_reader_failure_sentinel_never_leaks(self) -> None:
        # Unlike the bucket/binding-mismatch case above (a pure local
        # validation failure with no nested exception at all), this proves
        # a nested reader-raised exception's own text is suppressed too --
        # the same guarantee the data-object-stage test already proves for
        # its own stage.
        manifest_bytes = _manifest_bytes()
        binding = _binding_for(manifest_bytes)
        secret = "SECRET-MANIFEST-READER-FAILURE-DO-NOT-LEAK-7f2e"
        reader = FakeGCSObjectReader(raises={_manifest_object_name(): ValueError(secret)})

        with self.assertRaises(PinnedGCSLandingJobError) as ctx:
            self._run_job(_manifest_request(), binding, reader)

        message = str(ctx.exception)
        self.assertEqual(message, "pinned gcs landing job manifest acquisition failed")
        self.assertNotIn(secret, message)
        self.assertEqual(len(reader.calls), 1)
        self._assert_no_stores_created()

    def test_manifest_stage_verifier_failure_sentinel_never_leaks(self) -> None:
        # Proves the fourth internal stage of acquire_verified_landing_manifest
        # (LandingManifestVerifier.verify itself) is also sanitized: a
        # secret embedded in an otherwise well-formed-but-invalid manifest
        # payload must not surface in the collapsed error.
        secret = "SECRET-MANIFEST-CONTENT-DO-NOT-LEAK-4d9a"
        malformed_manifest_bytes = f'{{"schema_version": 1, "leak": "{secret}"'.encode("utf-8")
        binding = _binding_for(malformed_manifest_bytes)
        reader = FakeGCSObjectReader(
            responses={
                _manifest_object_name(): GCSObjectPayload(
                    content_bytes=malformed_manifest_bytes, generation=_MANIFEST_GENERATION
                )
            }
        )

        with self.assertRaises(PinnedGCSLandingJobError) as ctx:
            self._run_job(_manifest_request(), binding, reader)

        message = str(ctx.exception)
        self.assertEqual(message, "pinned gcs landing job manifest acquisition failed")
        self.assertNotIn(secret, message)
        self.assertEqual(len(reader.calls), 1)
        self._assert_no_stores_created()

    def test_data_object_stage_error_message_is_static_and_never_leaks_content(self) -> None:
        manifest_bytes = _manifest_bytes()
        binding = _binding_for(manifest_bytes)
        secret = "SECRET-READER-FAILURE-DO-NOT-LEAK-9c3d"
        reader = FakeGCSObjectReader(
            responses={
                _manifest_object_name(): GCSObjectPayload(
                    content_bytes=manifest_bytes, generation=_MANIFEST_GENERATION
                )
            },
            raises={_sm_object_name(): ValueError(secret)},
        )

        with self.assertRaises(PinnedGCSLandingJobError) as ctx:
            self._run_job(_manifest_request(), binding, reader)

        message = str(ctx.exception)
        self.assertEqual(message, "pinned gcs landing job data object acquisition failed")
        self.assertNotIn(secret, message)


_EXACT_ALLOWED_GCS_JOB_IMPORTS = frozenset((
    # (level, module, imported name, asname); level 0 covers absolute
    # imports, level > 0 covers `from .x import y` within the
    # daily_pipeline package. Closed set: any import in gcs_landing_job.py
    # not exactly in this set, and any entry here missing from that file,
    # fails the equality assertion below.
    (0, "__future__", "annotations", None),
    (0, "datetime", "date", None),
    (0, "datetime", "datetime", None),
    (0, "india_swing.daily_reports.artifact_store", "LocalDailyBundleArtifactStore", None),
    (0, "india_swing.historical_prices.artifact_store", "LocalHistoricalPriceArtifactStore", None),
    (0, "india_swing.identity_registry.adjudication_store", "LocalIdentityAdjudicationQueueStore", None),
    (0, "india_swing.identity_registry.artifact_store", "LocalIdentityRegistryStore", None),
    (0, "india_swing.reference.calendar", "CalendarSnapshot", None),
    (0, "india_swing.reference_data.artifact_store", "LocalReferenceArtifactStore", None),
    (1, "acquisition", "GCSLandingObjectReader", None),
    (1, "acquisition", "GCSObjectReader", None),
    (1, "acquisition", "LandingManifestObjectRequest", None),
    (1, "landing_inputs", "acquire_verified_landing_inputs", None),
    (1, "landing_manifest", "TrustedLandingManifestBinding", None),
    (1, "landing_manifest_acquisition", "acquire_verified_landing_manifest", None),
    (1, "models", "DailyPipelineRun", None),
    (1, "runner", "run_daily_pipeline_from_landing_inputs", None),
    (1, "store", "LocalDailyPipelineRunStore", None),
))

_EXACT_ALLOWED_GCS_JOB_CALL_TARGETS = frozenset((
    # The production module's entire callable surface: raising its own
    # error type, constructing the injected-reader wrapper, and invoking
    # exactly the three existing composition-stage functions. Any other
    # call name -- a GCS/storage client, requests/urllib, os/subprocess, a
    # broker/order/notification helper, a strategy/model/LLM call, a
    # listing/"latest" helper, or a retry/fallback wrapper -- fails this
    # test.
    "PinnedGCSLandingJobError",
    "GCSLandingObjectReader",
    "acquire_verified_landing_manifest",
    "acquire_verified_landing_inputs",
    "run_daily_pipeline_from_landing_inputs",
))

_FORBIDDEN_GCS_JOB_NAME_TOKENS = (
    # "gcs" is deliberately excluded: GCSObjectReader/GCSLandingObjectReader
    # are the exact, already-permitted injected protocol/wrapper this
    # module composes against, and a bare substring match would flag their
    # own legitimate type names as if they were client-construction
    # capability.
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


class GCSLandingJobCapabilityTests(unittest.TestCase):
    """Proves gcs_landing_job.py introduces no storage-client construction,
    network API, listing/latest selection, retry/fallback, second-source
    substitution, environment/current-clock, filesystem discovery,
    subprocess, notification, broker/order, strategy/model/LLM, scheduler,
    or deployment capability. Imports and the callable surface are both
    locked to exact closed sets.
    """

    def _module_ast(self) -> ast.Module:
        source = (
            Path(__file__).resolve().parent.parent
            / "src"
            / "india_swing"
            / "daily_pipeline"
            / "gcs_landing_job.py"
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
        self.assertEqual(actual, _EXACT_ALLOWED_GCS_JOB_IMPORTS)

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
            if target not in _EXACT_ALLOWED_GCS_JOB_CALL_TARGETS:
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
            for token in _FORBIDDEN_GCS_JOB_NAME_TOKENS:
                if token in candidate:
                    offenders.append(candidate)
        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
