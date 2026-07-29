from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

from india_swing.evaluation.engine import EvaluationDataReadiness
from india_swing.evaluation.promoted_experiment_assembly import (
    PROMOTED_EXPERIMENT_READINESS_POLICY_VERSION,
    PROMOTED_EXPERIMENT_READINESS_SCHEMA_VERSION,
    PromotedExperimentAssemblyError,
    PromotedExperimentReadinessConfig,
    PromotedExperimentReadinessIssueCode,
    PromotedExperimentReadinessReport,
    PromotedExperimentReadinessService,
    render_promoted_experiment_readiness,
)
from india_swing.evaluation.promoted_walk_forward import (
    PromotedFoldCrossSectionBinding,
)
from india_swing.features.historical_replay import (
    PromotedHistoricalReplayInput,
    PromotedHistoricalReplayService,
)
from india_swing.features.promoted_cross_section import (
    PromotedCrossSectionConfig,
)
from india_swing.features.store import (
    LocalPromotedCrossSectionStore,
    LocalPromotedTechnicalFeatureStore,
)
from tests.test_promoted_feature_persistence import (
    _Resolver as _SourceResolver,
    _artifacts,
)
from tests.test_promoted_technical_features import _small_config
from tests.test_promoted_walk_forward import (
    _Resolver,
    _dataset,
    _instrument,
    _plan,
    _universe_id,
)


_SHARED_EVIDENCE = None
_SHARED_TEMP = None


def _evidence(root: Path):
    global _SHARED_EVIDENCE, _SHARED_TEMP
    if _SHARED_EVIDENCE is not None:
        panel, plan, dataset, instruments, replay = _SHARED_EVIDENCE
        return (
            panel,
            plan,
            dataset,
            instruments,
            replay,
            _Resolver((panel,)),
        )
    _SHARED_TEMP = tempfile.TemporaryDirectory()
    root = Path(_SHARED_TEMP.name)
    source, _, _, _, _ = _artifacts(root / "evidence")
    replay_input = PromotedHistoricalReplayInput(
        market_session=source.adjustment_panel.signal_session,
        source_panel=source,
        technical_config=_small_config(),
        cross_section_config=PromotedCrossSectionConfig(
            minimum_computed_instruments=1
        ),
        cutoff=source.cutoff,
    )
    technical_store = LocalPromotedTechnicalFeatureStore(
        root / "store",
        _SourceResolver((source,)),
    )
    cross_store = LocalPromotedCrossSectionStore(
        root / "store",
        technical_store,
    )
    replay = PromotedHistoricalReplayService().run(
        inputs=(replay_input,),
        technical_store=technical_store,
        cross_section_store=cross_store,
    )
    result = replay.results[0]
    panel = cross_store.get(result.cross_section_panel_id)
    plan = _plan(result.market_session)
    universe_id = _universe_id(panel)
    dataset = _dataset(plan, universe_id)
    instruments = (_instrument(plan, universe_id),)
    _SHARED_EVIDENCE = (
        panel,
        plan,
        dataset,
        instruments,
        replay,
    )
    return panel, plan, dataset, instruments, replay, _Resolver((panel,))


def _relaxed_config() -> PromotedExperimentReadinessConfig:
    return PromotedExperimentReadinessConfig(
        minimum_total_sessions=50,
        minimum_fold_count=1,
        minimum_test_sessions_per_fold=5,
        minimum_instrument_count=1,
        minimum_session_eligible_instruments=1,
        required_dataset_readiness=EvaluationDataReadiness.SYNTHETIC,
    )


def _codes(report) -> set[PromotedExperimentReadinessIssueCode]:
    return {value.code for value in report.issues}


def tearDownModule() -> None:
    if _SHARED_TEMP is not None:
        _SHARED_TEMP.cleanup()


class PromotedExperimentReadinessConfigTests(unittest.TestCase):
    def test_defaults_require_three_year_broad_point_in_time_evidence(
        self,
    ) -> None:
        config = PromotedExperimentReadinessConfig()
        self.assertEqual(config.minimum_total_sessions, 756)
        self.assertEqual(config.minimum_fold_count, 6)
        self.assertEqual(config.minimum_instrument_count, 500)
        self.assertIs(
            config.required_dataset_readiness,
            EvaluationDataReadiness.POINT_IN_TIME_VERIFIED,
        )
        config.verify_content_identity()

    def test_synthetic_thresholds_are_explicitly_content_addressed(
        self,
    ) -> None:
        production = PromotedExperimentReadinessConfig()
        synthetic = _relaxed_config()
        self.assertNotEqual(production.config_id, synthetic.config_id)

    def test_rejects_bool_or_zero_threshold(self) -> None:
        with self.assertRaises(PromotedExperimentAssemblyError):
            PromotedExperimentReadinessConfig(
                minimum_fold_count=True
            )
        with self.assertRaises(PromotedExperimentAssemblyError):
            PromotedExperimentReadinessConfig(
                minimum_instrument_count=0
            )


class PromotedExperimentReadinessAuditTests(unittest.TestCase):
    def test_default_audit_reports_every_material_coverage_gap(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            (
                _,
                plan,
                dataset,
                instruments,
                replay,
                resolver,
            ) = _evidence(Path(tmp))
            report = PromotedExperimentReadinessService().audit(
                config=PromotedExperimentReadinessConfig(),
                split_plan=plan,
                dataset=dataset,
                instruments=instruments,
                replay_runs=(replay,),
                cross_section_resolver=resolver,
            )
        expected = {
            PromotedExperimentReadinessIssueCode
            .DATASET_READINESS_MISMATCH,
            PromotedExperimentReadinessIssueCode
            .INSUFFICIENT_TOTAL_SESSIONS,
            PromotedExperimentReadinessIssueCode
            .INSUFFICIENT_FOLD_COUNT,
            PromotedExperimentReadinessIssueCode.TEST_WINDOW_TOO_SHORT,
            PromotedExperimentReadinessIssueCode
            .INSUFFICIENT_INSTRUMENT_COUNT,
            PromotedExperimentReadinessIssueCode
            .SESSION_UNIVERSE_TOO_SMALL,
            PromotedExperimentReadinessIssueCode
            .INSTRUMENT_UNIVERSE_BINDING_INVALID,
            PromotedExperimentReadinessIssueCode
            .REPLAY_SOURCE_UNIVERSE_INCOMPLETE,
            PromotedExperimentReadinessIssueCode
            .CROSS_SECTION_SOURCE_UNIVERSE_INCOMPLETE,
        }
        self.assertEqual(_codes(report), expected)
        self.assertFalse(report.ready)
        self.assertFalse(report.evaluation_eligible)
        self.assertEqual(report.bindings, ())
        self.assertFalse(report.actionable)
        self.assertFalse(report.alert_eligible)
        self.assertFalse(report.execution_eligible)
        report.verify_content_identity()

    def test_relaxed_synthetic_audit_still_rejects_incomplete_universe(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            (
                _,
                plan,
                dataset,
                instruments,
                replay,
                resolver,
            ) = _evidence(Path(tmp))
            report = PromotedExperimentReadinessService().audit(
                config=_relaxed_config(),
                split_plan=plan,
                dataset=dataset,
                instruments=instruments,
                replay_runs=(replay,),
                cross_section_resolver=resolver,
            )
        self.assertEqual(
            _codes(report),
            {
                PromotedExperimentReadinessIssueCode
                .REPLAY_SOURCE_UNIVERSE_INCOMPLETE,
                PromotedExperimentReadinessIssueCode
                .CROSS_SECTION_SOURCE_UNIVERSE_INCOMPLETE,
            },
        )
        self.assertEqual(report.bindings, ())

    def test_unused_dataset_universe_snapshot_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            (
                _,
                plan,
                dataset,
                instruments,
                replay,
                resolver,
            ) = _evidence(Path(tmp))
            expanded_dataset = replace(
                dataset,
                universe_snapshot_ids=tuple(
                    sorted(
                        (
                            *dataset.universe_snapshot_ids,
                            "f" * 64,
                        )
                    )
                ),
            )
            report = PromotedExperimentReadinessService().audit(
                config=_relaxed_config(),
                split_plan=plan,
                dataset=expanded_dataset,
                instruments=instruments,
                replay_runs=(replay,),
                cross_section_resolver=resolver,
            )
        self.assertIn(
            PromotedExperimentReadinessIssueCode
            .INSTRUMENT_UNIVERSE_BINDING_INVALID,
            _codes(report),
        )

    def test_missing_exact_fold_replay_is_not_substituted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            (
                panel,
                original_plan,
                _,
                _,
                replay,
                resolver,
            ) = _evidence(Path(tmp))
            shifted_signal = (
                original_plan.folds[0].test_sessions[0]
                + timedelta(days=1)
            )
            plan = _plan(shifted_signal)
            universe_id = _universe_id(panel)
            report = PromotedExperimentReadinessService().audit(
                config=_relaxed_config(),
                split_plan=plan,
                dataset=_dataset(plan, universe_id),
                instruments=(_instrument(plan, universe_id),),
                replay_runs=(replay,),
                cross_section_resolver=resolver,
            )
        self.assertEqual(
            _codes(report),
            {
                PromotedExperimentReadinessIssueCode
                .MISSING_FOLD_REPLAY
            },
        )

    def test_duplicate_replay_session_is_ambiguous_even_when_identical(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            (
                _,
                plan,
                dataset,
                instruments,
                replay,
                resolver,
            ) = _evidence(Path(tmp))
            report = PromotedExperimentReadinessService().audit(
                config=_relaxed_config(),
                split_plan=plan,
                dataset=dataset,
                instruments=instruments,
                replay_runs=(replay, replay),
                cross_section_resolver=resolver,
            )
        self.assertEqual(
            _codes(report),
            {
                PromotedExperimentReadinessIssueCode
                .DUPLICATE_FOLD_REPLAY
            },
        )

    def test_unresolvable_cross_section_is_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            (
                _,
                plan,
                dataset,
                instruments,
                replay,
                _,
            ) = _evidence(Path(tmp))
            report = PromotedExperimentReadinessService().audit(
                config=_relaxed_config(),
                split_plan=plan,
                dataset=dataset,
                instruments=instruments,
                replay_runs=(replay,),
                cross_section_resolver=_Resolver(()),
            )
        self.assertEqual(
            _codes(report),
            {
                PromotedExperimentReadinessIssueCode
                .REPLAY_SOURCE_UNIVERSE_INCOMPLETE,
                PromotedExperimentReadinessIssueCode
                .CROSS_SECTION_UNAVAILABLE,
            },
        )

    def test_replay_summary_must_match_resolved_cross_section(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            (
                _,
                plan,
                dataset,
                instruments,
                replay,
                resolver,
            ) = _evidence(Path(tmp))
            result = replay.results[0]
            mismatched_result = replace(
                result,
                unassigned_entry_count=(
                    result.unassigned_entry_count + 1
                ),
            )
            mismatched_replay = replace(
                replay,
                results=(mismatched_result,),
            )
            report = PromotedExperimentReadinessService().audit(
                config=_relaxed_config(),
                split_plan=plan,
                dataset=dataset,
                instruments=instruments,
                replay_runs=(mismatched_replay,),
                cross_section_resolver=resolver,
            )
        self.assertEqual(
            _codes(report),
            {
                PromotedExperimentReadinessIssueCode
                .REPLAY_SOURCE_UNIVERSE_INCOMPLETE,
                PromotedExperimentReadinessIssueCode
                .CROSS_SECTION_BINDING_INVALID,
            },
        )

    def test_dataset_calendar_mismatch_is_reported_not_repaired(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            (
                panel,
                plan,
                dataset,
                instruments,
                replay,
                resolver,
            ) = _evidence(Path(tmp))
            shifted_plan = _plan(
                plan.folds[0].test_sessions[0]
                + timedelta(days=1)
            )
            report = PromotedExperimentReadinessService().audit(
                config=_relaxed_config(),
                split_plan=shifted_plan,
                dataset=dataset,
                instruments=instruments,
                replay_runs=(replay,),
                cross_section_resolver=resolver,
            )
        self.assertIn(
            PromotedExperimentReadinessIssueCode
            .DATASET_CALENDAR_MISMATCH,
            _codes(report),
        )
        self.assertEqual(_universe_id(panel), dataset.universe_snapshot_ids[0])


class PromotedExperimentReadinessReportTests(unittest.TestCase):
    def test_ready_report_can_only_authorize_offline_evaluation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            (
                panel,
                plan,
                dataset,
                instruments,
                replay,
                _,
            ) = _evidence(Path(tmp))
            binding = PromotedFoldCrossSectionBinding(
                fold_id=plan.folds[0].fold_id,
                signal_session=plan.folds[0].test_sessions[0],
                cross_section_panel_id=panel.panel_id,
            )
            report = PromotedExperimentReadinessReport(
                schema_version=(
                    PROMOTED_EXPERIMENT_READINESS_SCHEMA_VERSION
                ),
                policy_version=(
                    PROMOTED_EXPERIMENT_READINESS_POLICY_VERSION
                ),
                config_id=_relaxed_config().config_id,
                split_plan_id=plan.plan_id,
                dataset_id=dataset.dataset_id,
                replay_run_ids=(replay.run_id,),
                total_session_count=len(dataset.sessions),
                fold_count=1,
                instrument_count=len(instruments),
                minimum_observed_session_universe=1,
                issues=(),
                bindings=(binding,),
                ready=True,
                evaluation_eligible=True,
                actionable=False,
                alert_eligible=False,
                execution_eligible=False,
            )
        report.verify_content_identity()
        self.assertTrue(report.evaluation_eligible)
        self.assertFalse(report.actionable)
        self.assertFalse(report.alert_eligible)
        self.assertFalse(report.execution_eligible)

    def test_rendered_report_is_compact_and_actionable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            (
                _,
                plan,
                dataset,
                instruments,
                replay,
                resolver,
            ) = _evidence(Path(tmp))
            report = PromotedExperimentReadinessService().audit(
                config=PromotedExperimentReadinessConfig(),
                split_plan=plan,
                dataset=dataset,
                instruments=instruments,
                replay_runs=(replay,),
                cross_section_resolver=resolver,
            )
            markdown = render_promoted_experiment_readiness(report)
        self.assertIn("Offline evaluation ready: `NO`", markdown)
        self.assertIn("INSUFFICIENT_TOTAL_SESSIONS", markdown)
        self.assertIn("SESSION_UNIVERSE_TOO_SMALL", markdown)
        self.assertIn("never authorizes alerts", markdown)

    def test_service_exposes_only_audit(self) -> None:
        public = {
            value
            for value in dir(PromotedExperimentReadinessService)
            if not value.startswith("_")
        }
        self.assertEqual(public, {"audit"})


if __name__ == "__main__":
    unittest.main()
