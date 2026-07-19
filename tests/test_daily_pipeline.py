from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from india_swing.daily_pipeline import (
    DailyPipelineIntegrityError,
    DailyPipelineRunConflict,
    LocalDailyPipelineRunStore,
    run_daily_pipeline,
)
from india_swing.daily_pipeline.landing_lineage import (
    LANDING_INPUT_LINEAGE_SCHEMA_VERSION,
    AcquisitionFileType,
    LandingInputLineage,
    LandingObjectLineage,
)
from india_swing.daily_pipeline.models import VERIFIED_LANDING_LINEAGE_UNAVAILABLE
from india_swing.daily_reports.artifact_store import LocalDailyBundleArtifactStore
from india_swing.historical_prices.artifact_store import LocalHistoricalPriceArtifactStore
from india_swing.identity_registry.adjudication_store import LocalIdentityAdjudicationQueueStore
from india_swing.identity_registry.artifact_store import LocalIdentityRegistryStore
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference_data.artifact_store import LocalReferenceArtifactStore

from tests.test_promotion import daily_run as _promotion_daily_run
from tests.test_reconciliation import (
    BUNDLE_FIRST_SEEN,
    BUNDLE_VALIDATED,
    CUTOFF,
    MASTER_FIRST_SEEN,
    MASTER_VALIDATED,
    SESSION,
    _bundle_bytes,
    _calendar,
    _clock,
    _master_bytes,
)


def _sm_object_name(session: date) -> str:
    return f"landing/{session.isoformat()}/NSE_CM_security_{session.strftime('%d%m%Y')}.csv.gz"


def _db_object_name(session: date) -> str:
    return f"landing/{session.isoformat()}/Reports-Daily-Multiple.zip"


def _landing_object_lineage(
    file_type: AcquisitionFileType, *, target_session: date, generation: int = 123
) -> LandingObjectLineage:
    object_name = (
        _sm_object_name(target_session)
        if file_type is AcquisitionFileType.SECURITY_MASTER
        else _db_object_name(target_session)
    )
    return LandingObjectLineage(
        file_type=file_type,
        bucket="trusted-landing-bucket",
        object_name=object_name,
        generation=generation,
        target_session=target_session,
        sha256_hash=hashlib.sha256(f"{file_type.value}-{generation}".encode()).hexdigest(),
    )


def _landing_input_lineage(
    *, target_session: date, binding_cutoff: datetime, sm_generation: int = 123
) -> LandingInputLineage:
    return LandingInputLineage(
        schema_version=LANDING_INPUT_LINEAGE_SCHEMA_VERSION,
        manifest_sha256=hashlib.sha256(b"trusted-manifest-bytes").hexdigest(),
        manifest_knowledge_time=binding_cutoff,
        binding_not_before=binding_cutoff - timedelta(hours=1),
        binding_cutoff=binding_cutoff,
        target_session=target_session,
        security_master=_landing_object_lineage(
            AcquisitionFileType.SECURITY_MASTER,
            target_session=target_session,
            generation=sm_generation,
        ),
        daily_bundle=_landing_object_lineage(
            AcquisitionFileType.DAILY_BUNDLE, target_session=target_session
        ),
    )


class DailyPipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.reference_root = self.root / "reference"
        self.daily_root = self.root / "daily"
        self.history_root = self.root / "history"
        self.identity_root = self.root / "identity"
        self.pipeline_root = self.root / "pipeline"
        self.input_root = self.root / "input"
        self.input_root.mkdir()
        self.master_file = self.input_root / "NSE_CM_security_15072026.csv.gz"
        self.bundle_file = self.input_root / "Reports-Daily-Multiple (1).zip"
        self.master_file.write_bytes(_master_bytes())
        self.bundle_file.write_bytes(_bundle_bytes())

        self.reference_store = LocalReferenceArtifactStore(
            self.reference_root,
            clock=_clock(MASTER_FIRST_SEEN, MASTER_VALIDATED),
        )
        self.daily_store = LocalDailyBundleArtifactStore(
            self.daily_root,
            clock=_clock(BUNDLE_FIRST_SEEN, BUNDLE_VALIDATED),
        )
        self.historical_store = LocalHistoricalPriceArtifactStore(
            self.history_root,
            self.daily_root,
        )
        self.identity_store = LocalIdentityRegistryStore(
            self.identity_root,
            self.reference_root,
        )
        self.adjudication_store = LocalIdentityAdjudicationQueueStore(
            self.identity_root,
            self.identity_store,
        )
        self.run_store = LocalDailyPipelineRunStore(self.pipeline_root)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _run(self, *, previous_run_id: str | None = None):
        return run_daily_pipeline(
            market_session=SESSION,
            cutoff=CUTOFF,
            calendar_materialization_id="8" * 64,
            calendar=_calendar(),
            security_master_file=self.master_file,
            daily_bundle_file=self.bundle_file,
            previous_run_id=previous_run_id,
            reference_store=self.reference_store,
            daily_store=self.daily_store,
            historical_store=self.historical_store,
            identity_store=self.identity_store,
            adjudication_store=self.adjudication_store,
            run_store=self.run_store,
        )

    def test_bootstrap_run_persists_complete_collection_only_lineage(self) -> None:
        run = self._run()

        self.assertEqual(run.market_session, SESSION)
        self.assertEqual(run.observed_dates, (SESSION,))
        self.assertEqual(len(run.security_master_artifact_ids), 1)
        self.assertEqual(len(run.daily_bundle_artifact_ids), 1)
        self.assertGreater(run.bar_count, 0)
        self.assertEqual(run.adjudication_case_count, run.identity_candidate_count)
        self.assertIn("NO_PREVIOUS_DAILY_RUN", run.completeness_issues)
        self.assertIn("IDENTITY_ADJUDICATION_REQUIRED", run.completeness_issues)
        self.assertIs(run.readiness, ReferenceReadiness.COLLECTION_ONLY)
        self.assertFalse(run.actionable)
        self.assertFalse(run.stable_identity_assigned)
        self.assertIsNone(run.landing_input_lineage)
        self.assertIn(VERIFIED_LANDING_LINEAGE_UNAVAILABLE, run.completeness_issues)
        reloaded = self.run_store.get(run.run_id)
        self.assertEqual(reloaded, run)
        self.assertIsNone(reloaded.landing_input_lineage)
        self.assertIn(VERIFIED_LANDING_LINEAGE_UNAVAILABLE, reloaded.completeness_issues)
        self.assertEqual(self.run_store.publish(run), run)
        self.assertEqual(self.run_store.list_runs(), (run,))

    def test_wrong_predecessor_and_tampered_run_fail_closed(self) -> None:
        run = self._run()
        with self.assertRaisesRegex(
            DailyPipelineIntegrityError,
            "preceding session",
        ):
            self._run(previous_run_id=run.run_id)

        path = self.run_store.path_for(run.run_id)
        value = json.loads(path.read_text(encoding="utf-8"))
        value["run"]["bar_count"] += 1
        path.write_text(json.dumps(value), encoding="utf-8")
        with self.assertRaises(DailyPipelineRunConflict):
            self.run_store.get(run.run_id)


class LandingInputLineagePersistenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.run_store = LocalDailyPipelineRunStore(Path(self.temporary.name) / "pipeline")
        self.base_run = _promotion_daily_run()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _with_lineage(self, lineage: object, **overrides: object):
        issues = set(self.base_run.completeness_issues) - {VERIFIED_LANDING_LINEAGE_UNAVAILABLE}
        return replace(
            self.base_run,
            landing_input_lineage=lineage,
            completeness_issues=tuple(sorted(issues)),
            **overrides,
        )

    def _valid_lineage(self, **overrides: object) -> LandingInputLineage:
        kwargs = dict(
            target_session=self.base_run.market_session,
            binding_cutoff=self.base_run.cutoff,
        )
        kwargs.update(overrides)
        return _landing_input_lineage(**kwargs)

    def test_legacy_run_persists_and_reloads_with_lineage_unavailable(self) -> None:
        run = self.base_run
        self.assertIsNone(run.landing_input_lineage)
        self.assertIn(VERIFIED_LANDING_LINEAGE_UNAVAILABLE, run.completeness_issues)
        self.run_store.publish(run)
        reloaded = self.run_store.get(run.run_id)
        self.assertEqual(reloaded, run)
        self.assertIsNone(reloaded.landing_input_lineage)
        self.assertIn(VERIFIED_LANDING_LINEAGE_UNAVAILABLE, reloaded.completeness_issues)

    def test_run_with_valid_lineage_round_trips_every_field(self) -> None:
        lineage = self._valid_lineage()
        run = self._with_lineage(lineage)
        self.run_store.publish(run)
        reloaded = self.run_store.get(run.run_id)
        self.assertEqual(reloaded, run)
        self.assertEqual(reloaded.landing_input_lineage, lineage)
        self.assertEqual(reloaded.landing_input_lineage.lineage_id, lineage.lineage_id)
        self.assertEqual(reloaded.landing_input_lineage.security_master, lineage.security_master)
        self.assertEqual(reloaded.landing_input_lineage.daily_bundle, lineage.daily_bundle)

    def test_none_without_unavailable_issue_fails(self) -> None:
        issues = tuple(
            sorted(set(self.base_run.completeness_issues) - {VERIFIED_LANDING_LINEAGE_UNAVAILABLE})
        )
        with self.assertRaises(DailyPipelineIntegrityError):
            replace(self.base_run, landing_input_lineage=None, completeness_issues=issues)

    def test_non_none_with_unavailable_issue_fails(self) -> None:
        with self.assertRaises(DailyPipelineIntegrityError):
            replace(self.base_run, landing_input_lineage=self._valid_lineage())

    def test_wrong_lineage_type_fails(self) -> None:
        with self.assertRaises(DailyPipelineIntegrityError):
            self._with_lineage("not-a-lineage")

    def test_wrong_target_session_fails(self) -> None:
        lineage = self._valid_lineage(
            target_session=self.base_run.market_session + timedelta(days=1)
        )
        with self.assertRaises(DailyPipelineIntegrityError):
            self._with_lineage(lineage)

    def test_binding_cutoff_after_run_cutoff_fails(self) -> None:
        lineage = self._valid_lineage(binding_cutoff=self.base_run.cutoff + timedelta(seconds=1))
        with self.assertRaises(DailyPipelineIntegrityError):
            self._with_lineage(lineage)

    def test_changing_lineage_changes_run_id(self) -> None:
        run_a = self._with_lineage(self._valid_lineage(sm_generation=123))
        run_b = self._with_lineage(self._valid_lineage(sm_generation=456))
        self.assertNotEqual(run_a.run_id, run_b.run_id)

    def test_equivalent_utc_normalized_lineage_is_deterministic(self) -> None:
        ist = timezone(timedelta(hours=5, minutes=30))
        binding_cutoff_ist = self.base_run.cutoff.astimezone(ist)
        run_utc = self._with_lineage(self._valid_lineage())
        run_ist = self._with_lineage(self._valid_lineage(binding_cutoff=binding_cutoff_ist))
        self.assertEqual(run_utc.run_id, run_ist.run_id)

    def test_v1_model_schema_version_is_rejected(self) -> None:
        with self.assertRaises(DailyPipelineIntegrityError):
            replace(self.base_run, schema_version="nse-cm-daily-pipeline-run/v1")

    def test_v1_store_envelope_is_rejected(self) -> None:
        run = self._with_lineage(self._valid_lineage())
        self.run_store.publish(run)
        path = self.run_store.path_for(run.run_id)
        value = json.loads(path.read_text(encoding="utf-8"))
        value["store_schema_version"] = "local-daily-pipeline-run/v1"
        path.write_text(json.dumps(value), encoding="utf-8")
        with self.assertRaises(DailyPipelineRunConflict):
            self.run_store.get(run.run_id)


class LandingInputLineageStoreTamperTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.run_store = LocalDailyPipelineRunStore(Path(self.temporary.name) / "pipeline")
        base_run = _promotion_daily_run()
        lineage = _landing_input_lineage(
            target_session=base_run.market_session, binding_cutoff=base_run.cutoff
        )
        issues = tuple(
            sorted(set(base_run.completeness_issues) - {VERIFIED_LANDING_LINEAGE_UNAVAILABLE})
        )
        self.run = replace(base_run, landing_input_lineage=lineage, completeness_issues=issues)
        self.run_store.publish(self.run)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _tamper(self, mutate) -> None:
        path = self.run_store.path_for(self.run.run_id)
        value = json.loads(path.read_text(encoding="utf-8"))
        mutate(value["run"]["landing_input_lineage"])
        path.write_text(json.dumps(value), encoding="utf-8")

    def test_missing_key_fails_closed(self) -> None:
        self._tamper(lambda lineage: lineage.pop("lineage_id"))
        with self.assertRaises(DailyPipelineRunConflict):
            self.run_store.get(self.run.run_id)

    def test_extra_key_fails_closed(self) -> None:
        self._tamper(lambda lineage: lineage.update({"extra_field": "x"}))
        with self.assertRaises(DailyPipelineRunConflict):
            self.run_store.get(self.run.run_id)

    def test_bool_generation_fails_closed(self) -> None:
        self._tamper(lambda lineage: lineage["security_master"].update({"generation": True}))
        with self.assertRaises(DailyPipelineRunConflict):
            self.run_store.get(self.run.run_id)

    def test_malformed_enum_fails_closed(self) -> None:
        self._tamper(lambda lineage: lineage["security_master"].update({"file_type": "NOT_A_TYPE"}))
        with self.assertRaises(DailyPipelineRunConflict):
            self.run_store.get(self.run.run_id)

    def test_swapped_role_fails_closed(self) -> None:
        self._tamper(lambda lineage: lineage["security_master"].update({"file_type": "DAILY_BUNDLE"}))
        with self.assertRaises(DailyPipelineRunConflict):
            self.run_store.get(self.run.run_id)

    def test_malformed_date_fails_closed(self) -> None:
        self._tamper(lambda lineage: lineage.update({"target_session": "not-a-date"}))
        with self.assertRaises(DailyPipelineRunConflict):
            self.run_store.get(self.run.run_id)

    def test_malformed_datetime_fails_closed(self) -> None:
        self._tamper(lambda lineage: lineage.update({"binding_cutoff": "not-a-datetime"}))
        with self.assertRaises(DailyPipelineRunConflict):
            self.run_store.get(self.run.run_id)

    def test_naive_datetime_fails_closed(self) -> None:
        self._tamper(lambda lineage: lineage.update({"binding_cutoff": "2026-07-16T11:30:00"}))
        with self.assertRaises(DailyPipelineRunConflict):
            self.run_store.get(self.run.run_id)

    def test_malformed_hash_fails_closed(self) -> None:
        self._tamper(lambda lineage: lineage.update({"manifest_sha256": "not-a-hash"}))
        with self.assertRaises(DailyPipelineRunConflict):
            self.run_store.get(self.run.run_id)

    def test_malformed_path_fails_closed(self) -> None:
        self._tamper(
            lambda lineage: lineage["security_master"].update({"object_name": "../traversal"})
        )
        with self.assertRaises(DailyPipelineRunConflict):
            self.run_store.get(self.run.run_id)

    def test_session_mismatch_fails_closed(self) -> None:
        self._tamper(
            lambda lineage: lineage["security_master"].update({"target_session": "2099-01-01"})
        )
        with self.assertRaises(DailyPipelineRunConflict):
            self.run_store.get(self.run.run_id)

    def test_altered_lineage_id_fails_closed(self) -> None:
        self._tamper(lambda lineage: lineage.update({"lineage_id": "0" * 64}))
        with self.assertRaises(DailyPipelineRunConflict):
            self.run_store.get(self.run.run_id)

    def test_secret_bearing_bucket_never_appears_in_error(self) -> None:
        secret = "secret-tampered-bucket-do-not-leak-4f2a"
        self._tamper(lambda lineage: lineage["security_master"].update({"bucket": secret}))
        with self.assertRaises(DailyPipelineRunConflict) as ctx:
            self.run_store.get(self.run.run_id)
        self.assertNotIn(secret, str(ctx.exception))

    def test_secret_bearing_hash_never_appears_in_error(self) -> None:
        secret = hashlib.sha256(b"SECRET-TAMPERED-CONTENT-DO-NOT-LEAK").hexdigest()
        self._tamper(lambda lineage: lineage.update({"manifest_sha256": secret}))
        with self.assertRaises(DailyPipelineRunConflict) as ctx:
            self.run_store.get(self.run.run_id)
        self.assertNotIn(secret, str(ctx.exception))


class LandingLineagePreConstructionMutationTests(unittest.TestCase):
    """Regression coverage for the revision-11 review finding: a
    LandingInputLineage mutated via object.__setattr__ before
    DailyPipelineRun construction was accepted and received a run_id.
    DailyPipelineRun.__post_init__ must now reject it with a sanitized
    DailyPipelineIntegrityError.
    """

    def setUp(self) -> None:
        self.base_run = _promotion_daily_run()

    def _valid_lineage(self) -> LandingInputLineage:
        return _landing_input_lineage(
            target_session=self.base_run.market_session,
            binding_cutoff=self.base_run.cutoff,
        )

    def _with_lineage(self, lineage: object):
        issues = tuple(
            sorted(set(self.base_run.completeness_issues) - {VERIFIED_LANDING_LINEAGE_UNAVAILABLE})
        )
        return replace(
            self.base_run, landing_input_lineage=lineage, completeness_issues=issues
        )

    def test_rejects_lineage_id_mutated_before_construction(self) -> None:
        lineage = self._valid_lineage()
        object.__setattr__(lineage, "lineage_id", "0" * 64)
        with self.assertRaises(DailyPipelineIntegrityError):
            self._with_lineage(lineage)

    def test_rejects_valid_primitive_generation_mutation(self) -> None:
        # Otherwise-valid primitive: still a positive int64 generation.
        lineage = self._valid_lineage()
        object.__setattr__(lineage.security_master, "generation", 999)
        with self.assertRaises(DailyPipelineIntegrityError):
            self._with_lineage(lineage)

    def test_rejects_mutated_nested_hash(self) -> None:
        lineage = self._valid_lineage()
        other_hash = hashlib.sha256(b"attacker-content").hexdigest()
        object.__setattr__(lineage.daily_bundle, "sha256_hash", other_hash)
        with self.assertRaises(DailyPipelineIntegrityError):
            self._with_lineage(lineage)

    def test_rejects_swapped_nested_role(self) -> None:
        lineage = self._valid_lineage()
        object.__setattr__(lineage.security_master, "file_type", AcquisitionFileType.DAILY_BUNDLE)
        with self.assertRaises(DailyPipelineIntegrityError):
            self._with_lineage(lineage)

    def test_rejects_mutated_nested_path(self) -> None:
        lineage = self._valid_lineage()
        object.__setattr__(lineage.security_master, "object_name", "../traversal")
        with self.assertRaises(DailyPipelineIntegrityError):
            self._with_lineage(lineage)

    def test_rejects_mutated_nested_session(self) -> None:
        lineage = self._valid_lineage()
        object.__setattr__(
            lineage.security_master,
            "target_session",
            self.base_run.market_session + timedelta(days=1),
        )
        with self.assertRaises(DailyPipelineIntegrityError):
            self._with_lineage(lineage)

    def test_rejects_naive_binding_not_before_mutation(self) -> None:
        lineage = self._valid_lineage()
        object.__setattr__(
            lineage, "binding_not_before", datetime(2026, 7, 15, 0, 0, 0)
        )
        with self.assertRaises(DailyPipelineIntegrityError):
            self._with_lineage(lineage)

    def test_rejects_wrong_type_nested_object_mutation(self) -> None:
        lineage = self._valid_lineage()
        object.__setattr__(lineage, "daily_bundle", "not-a-lineage-object")
        with self.assertRaises(DailyPipelineIntegrityError):
            self._with_lineage(lineage)

    def test_secret_bearing_mutation_never_appears_in_error(self) -> None:
        lineage = self._valid_lineage()
        secret = "secret-preconstruction-bucket-do-not-leak-9b4e"
        object.__setattr__(lineage.security_master, "bucket", secret)
        with self.assertRaises(DailyPipelineIntegrityError) as ctx:
            self._with_lineage(lineage)
        self.assertNotIn(secret, str(ctx.exception))

    def test_unmutated_lineage_still_constructs(self) -> None:
        run = self._with_lineage(self._valid_lineage())
        self.assertIsNotNone(run.landing_input_lineage)


class LandingLineagePostConstructionMutationTests(unittest.TestCase):
    """A valid run whose lineage is mutated after DailyPipelineRun
    construction must be rejected by run.verify_content_identity() and by
    store.publish() before any run directory or JSON target is created.
    """

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.run_store = LocalDailyPipelineRunStore(Path(self.temporary.name) / "pipeline")
        self.base_run = _promotion_daily_run()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _valid_run(self):
        lineage = _landing_input_lineage(
            target_session=self.base_run.market_session,
            binding_cutoff=self.base_run.cutoff,
        )
        issues = tuple(
            sorted(set(self.base_run.completeness_issues) - {VERIFIED_LANDING_LINEAGE_UNAVAILABLE})
        )
        return replace(
            self.base_run, landing_input_lineage=lineage, completeness_issues=issues
        )

    def _assert_rejected_and_unpublished(self, run) -> None:
        with self.assertRaises(DailyPipelineIntegrityError):
            run.verify_content_identity()
        with self.assertRaises(DailyPipelineIntegrityError):
            self.run_store.publish(run)
        self.assertFalse(self.run_store.path_for(run.run_id).exists())
        self.assertFalse(self.run_store.runs_root.exists())

    def test_rejects_lineage_id_mutated_after_construction(self) -> None:
        run = self._valid_run()
        object.__setattr__(run.landing_input_lineage, "lineage_id", "0" * 64)
        self._assert_rejected_and_unpublished(run)

    def test_rejects_nested_field_mutated_after_construction(self) -> None:
        run = self._valid_run()
        object.__setattr__(run.landing_input_lineage.security_master, "generation", 999)
        self._assert_rejected_and_unpublished(run)

    def test_rejects_lineage_replaced_after_construction(self) -> None:
        run = self._valid_run()
        replacement = _landing_input_lineage(
            target_session=self.base_run.market_session,
            binding_cutoff=self.base_run.cutoff,
            sm_generation=456,
        )
        object.__setattr__(run, "landing_input_lineage", replacement)
        self._assert_rejected_and_unpublished(run)

    def test_rejects_lineage_mutated_to_wrong_type_after_construction(self) -> None:
        run = self._valid_run()
        object.__setattr__(run, "landing_input_lineage", "not-a-lineage-object")
        self._assert_rejected_and_unpublished(run)

    def test_secret_bearing_mutation_never_appears_in_publish_error(self) -> None:
        run = self._valid_run()
        secret = hashlib.sha256(b"SECRET-POSTCONSTRUCTION-CONTENT-DO-NOT-LEAK").hexdigest()
        object.__setattr__(run.landing_input_lineage, "manifest_sha256", secret)
        with self.assertRaises(DailyPipelineIntegrityError) as verify_ctx:
            run.verify_content_identity()
        self.assertNotIn(secret, str(verify_ctx.exception))
        with self.assertRaises(DailyPipelineIntegrityError) as publish_ctx:
            self.run_store.publish(run)
        self.assertNotIn(secret, str(publish_ctx.exception))
        self.assertFalse(self.run_store.path_for(run.run_id).exists())

    def test_unmutated_run_still_verifies_and_publishes(self) -> None:
        run = self._valid_run()
        run.verify_content_identity()
        self.run_store.publish(run)
        self.assertEqual(self.run_store.get(run.run_id), run)


if __name__ == "__main__":
    unittest.main()
