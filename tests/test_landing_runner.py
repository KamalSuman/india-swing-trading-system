from __future__ import annotations

import ast
import hashlib
import json
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from india_swing.daily_pipeline import (
    DailyPipelineIntegrityError,
    LocalDailyPipelineRunStore,
    run_daily_pipeline,
    run_daily_pipeline_from_landing_inputs,
)
from india_swing.daily_pipeline.acquisition import AcquiredFile, AcquisitionFileType
from india_swing.daily_pipeline.landing_inputs import VerifiedLandingInputs
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


def _valid_landing_inputs(
    *,
    market_session: date = SESSION,
    run_cutoff: datetime = CUTOFF,
) -> VerifiedLandingInputs:
    master_bytes = _master_bytes()
    bundle_bytes = _bundle_bytes()
    sm_sha256 = hashlib.sha256(master_bytes).hexdigest()
    db_sha256 = hashlib.sha256(bundle_bytes).hexdigest()

    manifest_dict = {
        "schema_version": 1,
        "knowledge_time": _KNOWLEDGE_TIME,
        "target_session": SESSION.isoformat(),
        "objects": [
            {
                "file_type": "SECURITY_MASTER",
                "bucket": _BUCKET,
                "object_name": _sm_object_name(),
                "generation": 111,
                "sha256": sm_sha256,
            },
            {
                "file_type": "DAILY_BUNDLE",
                "bucket": _BUCKET,
                "object_name": _db_object_name(),
                "generation": 222,
                "sha256": db_sha256,
            },
        ],
    }
    manifest_bytes = json.dumps(manifest_dict, separators=(",", ":")).encode("utf-8")
    binding = TrustedLandingManifestBinding(
        expected_manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        allowed_bucket=_BUCKET,
        target_session=SESSION,
        not_before=_NOT_BEFORE,
        cutoff=_BINDING_CUTOFF,
    )
    manifest = LandingManifestVerifier().verify(manifest_bytes, binding)

    security_master = AcquiredFile(
        bucket=manifest.security_master.bucket,
        object_name=manifest.security_master.object_name,
        generation=manifest.security_master.generation,
        target_session=SESSION,
        file_type=AcquisitionFileType.SECURITY_MASTER,
        content_bytes=master_bytes,
        sha256_hash=sm_sha256,
    )
    daily_bundle = AcquiredFile(
        bucket=manifest.daily_bundle.bucket,
        object_name=manifest.daily_bundle.object_name,
        generation=manifest.daily_bundle.generation,
        target_session=SESSION,
        file_type=AcquisitionFileType.DAILY_BUNDLE,
        content_bytes=bundle_bytes,
        sha256_hash=db_sha256,
    )
    return VerifiedLandingInputs(
        manifest=manifest,
        market_session=market_session,
        run_cutoff=run_cutoff,
        security_master=security_master,
        daily_bundle=daily_bundle,
    )


class LandingRunnerTestCase(unittest.TestCase):
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

    def _run_from_landing_inputs(self, landing_inputs: VerifiedLandingInputs, **overrides: object):
        kwargs = dict(
            landing_inputs=landing_inputs,
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
        return run_daily_pipeline_from_landing_inputs(**kwargs)


class LandingRunnerAcceptanceTests(LandingRunnerTestCase):
    def test_successful_run_persists_lineage_and_reopens(self) -> None:
        landing_inputs = _valid_landing_inputs()

        run = self._run_from_landing_inputs(landing_inputs)

        self.assertIsNotNone(run.landing_input_lineage)
        self.assertNotIn(VERIFIED_LANDING_LINEAGE_UNAVAILABLE, run.completeness_issues)
        self.assertEqual(run.landing_input_lineage.target_session, SESSION)
        self.assertEqual(
            run.landing_input_lineage.security_master.object_name, _sm_object_name()
        )
        self.assertEqual(run.landing_input_lineage.daily_bundle.object_name, _db_object_name())

        reloaded = self.run_store.get(run.run_id)
        self.assertEqual(reloaded, run)
        self.assertEqual(reloaded.landing_input_lineage, run.landing_input_lineage)
        self.assertNotIn(VERIFIED_LANDING_LINEAGE_UNAVAILABLE, reloaded.completeness_issues)


class LandingRunnerManualCompatibilityTests(LandingRunnerTestCase):
    def test_manual_run_still_omits_lineage(self) -> None:
        input_root = self.root / "manual-input"
        input_root.mkdir()
        master_file = input_root / "NSE_CM_security_15072026.csv.gz"
        bundle_file = input_root / "Reports-Daily-Multiple.zip"
        master_file.write_bytes(_master_bytes())
        bundle_file.write_bytes(_bundle_bytes())

        run = run_daily_pipeline(
            market_session=SESSION,
            cutoff=CUTOFF,
            calendar_materialization_id=_CALENDAR_MATERIALIZATION_ID,
            calendar=_calendar(),
            security_master_file=master_file,
            daily_bundle_file=bundle_file,
            previous_run_id=None,
            reference_store=self.reference_store,
            daily_store=self.daily_store,
            historical_store=self.historical_store,
            identity_store=self.identity_store,
            adjudication_store=self.adjudication_store,
            run_store=self.run_store,
        )

        self.assertIsNone(run.landing_input_lineage)
        self.assertIn(VERIFIED_LANDING_LINEAGE_UNAVAILABLE, run.completeness_issues)


class LandingRunnerValidationTests(LandingRunnerTestCase):
    def test_session_mismatch_fails_before_any_store_write(self) -> None:
        landing_inputs = _valid_landing_inputs(market_session=SESSION)

        with self.assertRaises(DailyPipelineIntegrityError):
            self._run_from_landing_inputs(
                landing_inputs, market_session=SESSION + timedelta(days=1)
            )

        self.assertFalse(self.reference_store.root.exists())
        self.assertFalse(self.daily_store.root.exists())
        self.assertFalse(self.run_store.runs_root.exists())

    def test_cutoff_mismatch_fails_before_any_store_write(self) -> None:
        landing_inputs = _valid_landing_inputs(run_cutoff=CUTOFF)

        with self.assertRaises(DailyPipelineIntegrityError):
            self._run_from_landing_inputs(landing_inputs, cutoff=CUTOFF + timedelta(seconds=1))

        self.assertFalse(self.reference_store.root.exists())
        self.assertFalse(self.daily_store.root.exists())
        self.assertFalse(self.run_store.runs_root.exists())

    def test_tampered_acquired_bytes_fail_before_any_store_write(self) -> None:
        landing_inputs = _valid_landing_inputs()
        object.__setattr__(
            landing_inputs.security_master,
            "content_bytes",
            b"tampered-content-not-matching-hash",
        )

        with self.assertRaises(DailyPipelineIntegrityError):
            self._run_from_landing_inputs(landing_inputs)

        self.assertFalse(self.reference_store.root.exists())
        self.assertFalse(self.daily_store.root.exists())
        self.assertFalse(self.run_store.runs_root.exists())

    def test_self_consistent_but_wrong_hash_fails_before_any_store_write(self) -> None:
        landing_inputs = _valid_landing_inputs()
        new_content = b"attacker-controlled-but-internally-self-consistent-content"
        object.__setattr__(landing_inputs.security_master, "content_bytes", new_content)
        object.__setattr__(
            landing_inputs.security_master,
            "sha256_hash",
            hashlib.sha256(new_content).hexdigest(),
        )

        with self.assertRaises(DailyPipelineIntegrityError):
            self._run_from_landing_inputs(landing_inputs)

        self.assertFalse(self.reference_store.root.exists())
        self.assertFalse(self.daily_store.root.exists())
        self.assertFalse(self.run_store.runs_root.exists())

    def test_wrong_type_fails_before_any_store_write(self) -> None:
        with self.assertRaises(DailyPipelineIntegrityError):
            self._run_from_landing_inputs("not-landing-inputs")  # type: ignore[arg-type]

        self.assertFalse(self.run_store.runs_root.exists())


class LandingRunnerCreateOnceTests(LandingRunnerTestCase):
    def test_repeated_run_with_identical_inputs_is_idempotent(self) -> None:
        landing_inputs = _valid_landing_inputs()

        run_a = self._run_from_landing_inputs(landing_inputs)
        run_b = self._run_from_landing_inputs(landing_inputs)

        self.assertEqual(run_a, run_b)
        self.assertEqual(run_a.run_id, run_b.run_id)
        self.assertEqual(self.run_store.list_runs(), (run_a,))

class LandingRunnerNestedMutationTests(LandingRunnerTestCase):
    _SENTINEL_PATH = "sentinel-mutated-object-path-should-never-leak"

    def test_mutated_manifest_knowledge_time_fails_closed_without_leaking(self) -> None:
        landing_inputs = _valid_landing_inputs()
        object.__setattr__(landing_inputs.manifest, "knowledge_time", self._SENTINEL_PATH)

        with self.assertRaises(DailyPipelineIntegrityError) as boom:
            self._run_from_landing_inputs(landing_inputs)

        message = str(boom.exception)
        self.assertNotIn(self._SENTINEL_PATH, message)
        self.assertNotIn(_BUCKET, message)
        self.assertNotIn(_sm_object_name(), message)
        self.assertNotIn(_db_object_name(), message)
        self.assertFalse(self.reference_store.root.exists())
        self.assertFalse(self.daily_store.root.exists())
        self.assertFalse(self.run_store.runs_root.exists())

    def test_mutated_manifest_binding_cutoff_fails_closed_without_leaking(self) -> None:
        landing_inputs = _valid_landing_inputs()
        object.__setattr__(landing_inputs.manifest.binding, "cutoff", self._SENTINEL_PATH)

        with self.assertRaises(DailyPipelineIntegrityError) as boom:
            self._run_from_landing_inputs(landing_inputs)

        message = str(boom.exception)
        self.assertNotIn(self._SENTINEL_PATH, message)
        self.assertNotIn(_BUCKET, message)
        self.assertNotIn(_sm_object_name(), message)
        self.assertNotIn(_db_object_name(), message)
        self.assertFalse(self.reference_store.root.exists())
        self.assertFalse(self.daily_store.root.exists())
        self.assertFalse(self.run_store.runs_root.exists())


_ALLOWED_RUNNER_IMPORT_ROOTS = frozenset((
    "__future__",
    "os",
    "re",
    "tempfile",
    "contextlib",
    "datetime",
    "pathlib",
    "typing",
    "india_swing",
))

_FORBIDDEN_RUNNER_NAME_TOKENS = (
    "gcs",
    "google",
    "storage",
    "blob",
    "bucketlist",
    "requests",
    "urllib",
    "http",
    "socket",
    "subprocess",
    "broker",
    "retry",
    "fallback",
    "latest",
)


class LandingRunnerCapabilityTests(unittest.TestCase):
    """Proves runner.py's landing-input integration adds no GCS, network,
    subprocess, broker, listing, latest-selection, retry, or fallback
    capability, permitting only the existing local tempfile/os
    file-materialization boundary.
    """

    def _runner_ast(self) -> ast.Module:
        source = (
            Path(__file__).resolve().parent.parent
            / "src"
            / "india_swing"
            / "daily_pipeline"
            / "runner.py"
        ).read_text(encoding="utf-8")
        return ast.parse(source)

    def test_runner_imports_are_confined_to_the_allowed_set(self) -> None:
        tree = self._runner_ast()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    root = alias.name.split(".")[0]
                    self.assertIn(root, _ALLOWED_RUNNER_IMPORT_ROOTS)
            elif isinstance(node, ast.ImportFrom):
                if node.level and node.level > 0:
                    continue
                module = node.module or ""
                root = module.split(".")[0]
                self.assertIn(root, _ALLOWED_RUNNER_IMPORT_ROOTS)

    def test_runner_identifiers_carry_no_disallowed_capability_token(self) -> None:
        tree = self._runner_ast()
        offenders = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Name):
                candidate = node.id.lower()
            elif isinstance(node, ast.Attribute):
                candidate = node.attr.lower()
            else:
                continue
            for token in _FORBIDDEN_RUNNER_NAME_TOKENS:
                if token in candidate:
                    offenders.append(candidate)
        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
