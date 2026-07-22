from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

from india_swing.evaluation import (
    DailyExecutionPolicy,
    DeterministicComparisonRun,
    EqualWeightBenchmarkConfig,
    EquityPoint,
    EvaluationDataReadiness,
    EvaluationDataset,
    GeneratedIntentBatch,
    PurgedWalkForwardPlan,
    RegimeEnsembleEvaluationEngine,
    RegimeEnsembleEvaluationRun,
    RegimeEnsembleIntentConfig,
    RegimeEnsembleIntentGenerator,
    RegimeEnsembleIntentRun,
    RegimeEnsembleRunError,
    TrialEvaluationComparisonResult,
    TrialEvaluationResult,
    TrialRegistration,
    TrialStage,
    build_fold_comparison_summaries,
    regime_ensemble_trial_configuration_hash,
)
from india_swing.execution import SimulationBar, zerodha_nse_delivery_schedule_2026
from india_swing.identity import content_id

from tests.test_regime_ensemble_evaluation import (
    D,
    SESSIONS,
    STRONG,
    WEAK,
    _InstrumentProfile,
    _calendar,
    _fold_proposal_batch,
    _instruments,
    _intent_config,
    _split_plan,
)


def _execution_policy() -> DailyExecutionPolicy:
    return DailyExecutionPolicy(
        slippage_bps=D("10"), stressed_slippage_bps=D("25"), maximum_participation=D("0.0025")
    )


CALENDAR_SOURCE_ID = "e1" * 32


def _n_fold_plan(plan: PurgedWalkForwardPlan, folds: tuple) -> PurgedWalkForwardPlan:
    return PurgedWalkForwardPlan(
        calendar_version=plan.calendar_version,
        ordered_sessions=plan.ordered_sessions,
        label_horizon_sessions=plan.label_horizon_sessions,
        embargo_sessions=plan.embargo_sessions,
        folds=folds,
    )


def _dense_dataset(
    *, profiles: tuple[_InstrumentProfile, ...], universe_snapshot_ids: tuple[str, ...]
) -> EvaluationDataset:
    bars = []
    for profile in profiles:
        for index, session in enumerate(SESSIONS):
            close = profile.closes[index]
            bars.append(
                SimulationBar(
                    session=session,
                    symbol=profile.symbol,
                    open=close - Decimal("0.20"),
                    high=close + Decimal("1.00"),
                    low=close - Decimal("1.00"),
                    close=close,
                    volume=1000 + index * 10,
                )
            )
    return EvaluationDataset(
        sessions=SESSIONS,
        bars=tuple(sorted(bars, key=lambda value: (value.session, value.symbol))),
        source_snapshot_ids=(CALENDAR_SOURCE_ID,),
        universe_snapshot_ids=tuple(sorted(universe_snapshot_ids)),
        readiness=EvaluationDataReadiness.SYNTHETIC,
    )


def _build_intent_run(
    *,
    profiles: tuple[_InstrumentProfile, ...],
    intent_config: RegimeEnsembleIntentConfig | None = None,
    initial_capital: Decimal = D("100000"),
    num_folds: int = 1,
) -> tuple[RegimeEnsembleIntentRun, PurgedWalkForwardPlan]:
    calendar = _calendar()
    full_plan = _split_plan()
    folds = full_plan.folds[:num_folds]
    plan = full_plan if len(folds) == len(full_plan.folds) else _n_fold_plan(full_plan, folds)

    proposal_batches = tuple(
        _fold_proposal_batch(profiles=profiles, fold=fold, calendar=calendar) for fold in folds
    )
    universe_snapshot_ids = tuple(
        batch.universe_batch.universe_snapshot_id for batch in proposal_batches
    )
    signal_sessions = tuple(fold.test_sessions[0] for fold in folds)
    instruments = _instruments(
        profiles=profiles,
        signal_sessions=signal_sessions,
        universe_snapshot_ids=universe_snapshot_ids,
    )
    dataset = _dense_dataset(profiles=profiles, universe_snapshot_ids=universe_snapshot_ids)
    config = intent_config or _intent_config()
    execution_policy = _execution_policy()
    intent_run = RegimeEnsembleIntentGenerator().generate(
        config=config,
        split_plan=plan,
        dataset=dataset,
        instruments=instruments,
        proposal_batches=proposal_batches,
        execution_policy=execution_policy,
        initial_capital=initial_capital,
    )
    return intent_run, plan


def _benchmark_config() -> EqualWeightBenchmarkConfig:
    return EqualWeightBenchmarkConfig(
        maximum_constituents=2,
        gross_exposure_fraction=D("0.80"),
        stop_loss_fraction=D("0.50"),
        target_gain_fraction=D("0.50"),
        maximum_holding_sessions=3,
    )


def _cost_schedule():
    return zerodha_nse_delivery_schedule_2026()


def _registration(
    *,
    intent_run: RegimeEnsembleIntentRun,
    benchmark_config: EqualWeightBenchmarkConfig,
    cost_schedule=None,
    **overrides: object,
) -> TrialRegistration:
    cost_schedule = cost_schedule or _cost_schedule()
    values: dict[str, object] = dict(
        registered_at=datetime(2026, 7, 1, 12, tzinfo=UTC),
        stage=TrialStage.EXPLORATORY,
        hypothesis="Regime-aware ensemble scoring exceeds a liquid equal-weight basket.",
        strategy_family_id="regime-ensemble-orchestrator-v1",
        parent_trial_id=None,
        evaluation_start=intent_run.split_plan.ordered_sessions[0],
        evaluation_end=intent_run.split_plan.ordered_sessions[-1],
        universe_snapshot_ids=intent_run.dataset.universe_snapshot_ids,
        data_snapshot_ids=intent_run.dataset.source_snapshot_ids,
        split_plan_id=intent_run.split_plan.plan_id,
        label_horizon_sessions=intent_run.split_plan.label_horizon_sessions,
        benchmark_id=benchmark_config.benchmark_id,
        primary_metric="net_return",
        secondary_metrics=("max_drawdown", "net_profit", "trade_count", "turnover"),
        model_bundle_id=intent_run.config.config_id,
        source_commit="2937d01",
        dependency_hash="c" * 64,
        configuration_hash=regime_ensemble_trial_configuration_hash(
            intent_run.config, benchmark_config
        ),
        exclusions_hash="d" * 64,
        risk_policy_hash="e" * 64,
        execution_policy_version=intent_run.execution_policy.version,
        execution_policy_hash=intent_run.execution_policy.policy_id,
        cost_schedule_version=cost_schedule.policy_version,
        cost_schedule_hash=cost_schedule.schedule_id,
        base_slippage_bps=intent_run.execution_policy.slippage_bps,
        stressed_slippage_bps=intent_run.execution_policy.stressed_slippage_bps,
        pass_thresholds=(("net_return", D("-1")),),
        multiple_testing_policy="single-synthetic-baseline-v1",
        random_seed=1729,
        repetition_count=1,
        holdout_id=None,
        holdout_sealed=False,
        synthetic=True,
    )
    values.update(overrides)
    return TrialRegistration(**values)


def _evaluate(
    *,
    profiles: tuple[_InstrumentProfile, ...] = (STRONG, WEAK),
    num_folds: int = 1,
    registration_overrides: dict | None = None,
) -> tuple[RegimeEnsembleEvaluationRun, RegimeEnsembleIntentRun]:
    intent_run, _ = _build_intent_run(profiles=profiles, num_folds=num_folds)
    benchmark_config = _benchmark_config()
    cost_schedule = _cost_schedule()
    registration = _registration(
        intent_run=intent_run,
        benchmark_config=benchmark_config,
        cost_schedule=cost_schedule,
        **(registration_overrides or {}),
    )
    run = RegimeEnsembleEvaluationEngine().evaluate(
        registration=registration,
        intent_run=intent_run,
        benchmark_config=benchmark_config,
        cost_schedule=cost_schedule,
    )
    return run, intent_run


class RegimeEnsembleTrialConfigurationHashTests(unittest.TestCase):
    def test_hash_is_deterministic_and_binds_both_configs(self) -> None:
        intent_run, _ = _build_intent_run(profiles=(STRONG,))
        benchmark_config = _benchmark_config()
        first = regime_ensemble_trial_configuration_hash(intent_run.config, benchmark_config)
        second = regime_ensemble_trial_configuration_hash(intent_run.config, benchmark_config)
        self.assertEqual(first, second)
        self.assertEqual(
            first,
            content_id(
                {
                    "schema": "regime-ensemble-trial-configuration/v1",
                    "intent_config_id": intent_run.config.config_id,
                    "benchmark_id": benchmark_config.benchmark_id,
                },
                length=64,
            ),
        )

    def test_hash_rejects_wrong_types(self) -> None:
        intent_run, _ = _build_intent_run(profiles=(STRONG,))
        benchmark_config = _benchmark_config()
        with self.assertRaisesRegex(RegimeEnsembleRunError, "must be exact"):
            regime_ensemble_trial_configuration_hash("not-a-config", benchmark_config)
        with self.assertRaisesRegex(RegimeEnsembleRunError, "must be exact"):
            regime_ensemble_trial_configuration_hash(intent_run.config, "not-a-benchmark")


class RegimeEnsembleEvaluationEngineHappyPathTests(unittest.TestCase):
    def test_multi_fold_evaluation_returns_deterministic_base_and_stressed_results(
        self,
    ) -> None:
        run, intent_run = _evaluate(profiles=(STRONG, WEAK), num_folds=1)
        comparison = run.comparison_run.comparison

        self.assertIsNotNone(comparison.strategy_stressed)
        self.assertIsNotNone(comparison.benchmark_stressed)
        self.assertEqual(len(run.comparison_run.fold_summaries), len(intent_run.fold_results))

        other_run, _ = _evaluate(profiles=(STRONG, WEAK), num_folds=1)
        self.assertEqual(run.run_id, other_run.run_id)
        self.assertEqual(run.comparison_run.run_id, other_run.comparison_run.run_id)

    def test_evaluation_run_embeds_exact_nested_components_and_is_never_executable(
        self,
    ) -> None:
        run, intent_run = _evaluate(profiles=(STRONG, WEAK), num_folds=1)

        self.assertIs(run.intent_run, intent_run)
        self.assertIs(run.comparison_run.strategy_batch, intent_run.generated_batch)
        self.assertIs(run.comparison_run.benchmark_batch, run.benchmark_batch)
        self.assertFalse(run.execution_eligible)
        self.assertFalse(run.promotion_eligible)
        self.assertFalse(intent_run.execution_eligible)
        run.verify_content_identity()


class RegimeEnsembleEvaluationEngineRejectionTests(unittest.TestCase):
    def test_rejects_wrong_type_inputs(self) -> None:
        intent_run, _ = _build_intent_run(profiles=(STRONG,))
        benchmark_config = _benchmark_config()
        cost_schedule = _cost_schedule()
        registration = _registration(
            intent_run=intent_run, benchmark_config=benchmark_config, cost_schedule=cost_schedule
        )
        base_kwargs = dict(
            registration=registration,
            intent_run=intent_run,
            benchmark_config=benchmark_config,
            cost_schedule=cost_schedule,
        )
        engine = RegimeEnsembleEvaluationEngine()
        for field_name in ("registration", "intent_run", "benchmark_config", "cost_schedule"):
            kwargs = dict(base_kwargs)
            kwargs[field_name] = "not-exact"
            with self.assertRaisesRegex(RegimeEnsembleRunError, "must be exact"):
                engine.evaluate(**kwargs)

    def test_rejects_registration_mismatches(self) -> None:
        intent_run, _ = _build_intent_run(profiles=(STRONG,))
        benchmark_config = _benchmark_config()
        cost_schedule = _cost_schedule()
        valid_registration = _registration(
            intent_run=intent_run, benchmark_config=benchmark_config, cost_schedule=cost_schedule
        )
        engine = RegimeEnsembleEvaluationEngine()

        scenarios = {
            "model_bundle": {"model_bundle_id": "9" * 64},
            "benchmark": {"benchmark_id": "8" * 64},
            "configuration_hash": {"configuration_hash": "7" * 64},
            "split_plan": {"split_plan_id": "6" * 64},
            "data_snapshots": {"data_snapshot_ids": ("5" * 64,)},
            "universe_snapshots": {"universe_snapshot_ids": ("4" * 64,)},
            "label_horizon": {"label_horizon_sessions": 11},
            "execution_policy_version": {"execution_policy_version": "different-policy/v9"},
            "execution_policy_hash": {"execution_policy_hash": "3" * 64},
            "base_slippage": {"base_slippage_bps": D("999")},
            "cost_schedule_version": {"cost_schedule_version": "different-cost/v9"},
            "cost_schedule_hash": {"cost_schedule_hash": "2" * 64},
        }
        for name, override in scenarios.items():
            registration = replace(valid_registration, **override)
            with self.assertRaises(RegimeEnsembleRunError, msg=name):
                engine.evaluate(
                    registration=registration,
                    intent_run=intent_run,
                    benchmark_config=benchmark_config,
                    cost_schedule=cost_schedule,
                )

    def test_rejects_synthetic_readiness_mismatch(self) -> None:
        intent_run, _ = _build_intent_run(profiles=(STRONG,))
        benchmark_config = _benchmark_config()
        cost_schedule = _cost_schedule()
        registration = _registration(
            intent_run=intent_run,
            benchmark_config=benchmark_config,
            cost_schedule=cost_schedule,
            synthetic=False,
        )
        with self.assertRaises(RegimeEnsembleRunError):
            RegimeEnsembleEvaluationEngine().evaluate(
                registration=registration,
                intent_run=intent_run,
                benchmark_config=benchmark_config,
                cost_schedule=cost_schedule,
            )


class RegimeEnsembleEvaluationRunIdentityTests(unittest.TestCase):
    def test_verify_content_identity_detects_registration_mutation(self) -> None:
        run, _ = _evaluate(profiles=(STRONG,), num_folds=1)
        run.verify_content_identity()
        untouched_run_id = run.run_id
        object.__setattr__(run.registration, "hypothesis", "tampered hypothesis")

        with self.assertRaises(RegimeEnsembleRunError):
            run.verify_content_identity()
        self.assertEqual(run.run_id, untouched_run_id)

    def test_verify_content_identity_detects_benchmark_batch_mutation(self) -> None:
        run, _ = _evaluate(profiles=(STRONG,), num_folds=1)
        run.verify_content_identity()
        untouched_run_id = run.run_id
        object.__setattr__(run.benchmark_batch, "generator_id", "1" * 64)

        with self.assertRaises(RegimeEnsembleRunError):
            run.verify_content_identity()
        self.assertEqual(run.run_id, untouched_run_id)

    def test_verify_content_identity_detects_comparison_run_mutation(self) -> None:
        run, _ = _evaluate(profiles=(STRONG,), num_folds=1)
        run.verify_content_identity()
        untouched_run_id = run.run_id
        object.__setattr__(run.comparison_run, "run_id", "0" * 64)

        with self.assertRaises(RegimeEnsembleRunError):
            run.verify_content_identity()
        self.assertEqual(run.run_id, untouched_run_id)

    def test_direct_construction_rejects_mismatched_comparison_strategy_batch(self) -> None:
        run_a, intent_run_a = _evaluate(profiles=(STRONG,), num_folds=1)
        run_b, _ = _evaluate(profiles=(WEAK,), num_folds=1)

        with self.assertRaises(RegimeEnsembleRunError):
            RegimeEnsembleEvaluationRun(
                registration=run_a.registration,
                intent_run=intent_run_a,
                benchmark_config=run_a.benchmark_config,
                cost_schedule=run_a.cost_schedule,
                benchmark_batch=run_a.benchmark_batch,
                comparison_run=run_b.comparison_run,
            )


def _forge_comparison_run(
    run: RegimeEnsembleEvaluationRun,
    *,
    comparison_overrides: dict | None = None,
    result_overrides: dict | None = None,
) -> DeterministicComparisonRun:
    """Build a hash-consistent DeterministicComparisonRun with switched evidence.

    Every present TrialEvaluationResult receives the same result_overrides
    (so TrialEvaluationComparisonResult's own internal cross-check, which
    requires every nested result to share one split/dataset/policy/cost/
    capital binding, still passes), and fold summaries are rebuilt through
    the exact public build_fold_comparison_summaries against the forged
    comparison -- never hand-computed.
    """

    comparison = run.comparison_run.comparison
    result_overrides = result_overrides or {}

    def _forge_result(result):
        if result is None:
            return None
        if "initial_capital" in result_overrides:
            initial_capital = result_overrides["initial_capital"]
            zero_metrics = tuple(
                sorted(
                    {
                        "max_drawdown": D("0"),
                        "net_cagr": D("0"),
                        "net_profit": D("0"),
                        "net_return": D("0"),
                        "trade_count": D("0"),
                        "turnover": D("0"),
                    }.items()
                )
            )
            return TrialEvaluationResult(
                trial_id=result.trial_id,
                split_plan_id=result.split_plan_id,
                dataset_id=result.dataset_id,
                execution_policy_id=result.execution_policy_id,
                cost_schedule_id=result.cost_schedule_id,
                initial_capital=initial_capital,
                trades=(),
                charges=None,
                equity_curve=tuple(
                    EquityPoint(
                        session=point.session,
                        equity=initial_capital,
                        drawdown=D("0"),
                    )
                    for point in result.equity_curve
                ),
                metrics=zero_metrics,
                pass_thresholds=result.pass_thresholds,
                passed=True,
            )
        return replace(result, **result_overrides) if result_overrides else result

    comparison_kwargs = dict(
        trial_id=comparison.trial_id,
        strategy_id=comparison.strategy_id,
        benchmark_id=comparison.benchmark_id,
        primary_metric=comparison.primary_metric,
        base_slippage_bps=comparison.base_slippage_bps,
        stressed_slippage_bps=comparison.stressed_slippage_bps,
        strategy_base=_forge_result(comparison.strategy_base),
        benchmark_base=_forge_result(comparison.benchmark_base),
        strategy_stressed=_forge_result(comparison.strategy_stressed),
        benchmark_stressed=_forge_result(comparison.benchmark_stressed),
    )
    comparison_kwargs.update(comparison_overrides or {})
    forged_comparison = TrialEvaluationComparisonResult(**comparison_kwargs)

    fold_summaries = build_fold_comparison_summaries(
        split_plan=run.intent_run.split_plan,
        strategy_batch=run.comparison_run.strategy_batch,
        benchmark_batch=run.comparison_run.benchmark_batch,
        comparison=forged_comparison,
    )
    return DeterministicComparisonRun(
        strategy_batch=run.comparison_run.strategy_batch,
        benchmark_batch=run.comparison_run.benchmark_batch,
        comparison=forged_comparison,
        fold_summaries=fold_summaries,
    )


class RegimeEnsembleEvaluationRunDirectConstructionBindingTests(unittest.TestCase):
    """Regressions for the revision-10 correction: every binding TrialEvaluationEngine
    would enforce during a real evaluate() must also reject a directly reconstructed
    RegimeEnsembleEvaluationRun, without ever invoking evaluate() or the simulator
    again."""

    def test_rejects_synthetic_readiness_mismatch_without_evaluate(self) -> None:
        run, _ = _evaluate(profiles=(STRONG,), num_folds=1)
        mismatched = replace(run.registration, synthetic=False)

        with self.assertRaises(RegimeEnsembleRunError):
            RegimeEnsembleEvaluationRun(
                registration=mismatched,
                intent_run=run.intent_run,
                benchmark_config=run.benchmark_config,
                cost_schedule=run.cost_schedule,
                benchmark_batch=run.benchmark_batch,
                comparison_run=run.comparison_run,
            )

    def test_rejects_evaluation_date_containment_mismatch(self) -> None:
        run, intent_run = _evaluate(profiles=(STRONG,), num_folds=1)
        mismatched = replace(
            run.registration, evaluation_end=intent_run.split_plan.ordered_sessions[-2]
        )

        with self.assertRaises(RegimeEnsembleRunError):
            RegimeEnsembleEvaluationRun(
                registration=mismatched,
                intent_run=run.intent_run,
                benchmark_config=run.benchmark_config,
                cost_schedule=run.cost_schedule,
                benchmark_batch=run.benchmark_batch,
                comparison_run=run.comparison_run,
            )

    def test_rejects_base_and_stressed_slippage_mismatch(self) -> None:
        run, _ = _evaluate(profiles=(STRONG,), num_folds=1)
        for name, override in (
            ("base_slippage", {"base_slippage_bps": D("999")}),
            ("stressed_slippage", {"stressed_slippage_bps": D("999")}),
        ):
            mismatched = replace(run.registration, **override)
            with self.assertRaises(RegimeEnsembleRunError, msg=name):
                RegimeEnsembleEvaluationRun(
                    registration=mismatched,
                    intent_run=run.intent_run,
                    benchmark_config=run.benchmark_config,
                    cost_schedule=run.cost_schedule,
                    benchmark_batch=run.benchmark_batch,
                    comparison_run=run.comparison_run,
                )

    def test_rejects_forged_comparison_evidence_bindings(self) -> None:
        run, _ = _evaluate(profiles=(STRONG,), num_folds=1)
        scenarios = {
            "dataset_id": {"dataset_id": "f" * 64},
            "execution_policy_id": {"execution_policy_id": "e" * 64},
            "cost_schedule_id": {"cost_schedule_id": "d" * 64},
            "initial_capital": {"initial_capital": D("999999")},
            "pass_thresholds": {"pass_thresholds": (("net_return", D("-2")),)},
        }
        for name, override in scenarios.items():
            forged_run = _forge_comparison_run(run, result_overrides=override)
            with self.assertRaises(RegimeEnsembleRunError, msg=name):
                RegimeEnsembleEvaluationRun(
                    registration=run.registration,
                    intent_run=run.intent_run,
                    benchmark_config=run.benchmark_config,
                    cost_schedule=run.cost_schedule,
                    benchmark_batch=run.benchmark_batch,
                    comparison_run=forged_run,
                )

    def test_rejects_forged_comparison_primary_metric(self) -> None:
        run, _ = _evaluate(profiles=(STRONG,), num_folds=1)
        forged_run = _forge_comparison_run(
            run, comparison_overrides={"primary_metric": "max_drawdown"}
        )

        with self.assertRaises(RegimeEnsembleRunError):
            RegimeEnsembleEvaluationRun(
                registration=run.registration,
                intent_run=run.intent_run,
                benchmark_config=run.benchmark_config,
                cost_schedule=run.cost_schedule,
                benchmark_batch=run.benchmark_batch,
                comparison_run=forged_run,
            )

    def test_rejects_switched_fold_summary_not_replayed_by_build_fold_comparison_summaries(
        self,
    ) -> None:
        run, _ = _evaluate(profiles=(STRONG, WEAK), num_folds=1)
        comparison_run = run.comparison_run
        real_summary = comparison_run.fold_summaries[0]
        self.assertNotEqual(real_summary.first_session, real_summary.last_session)
        forged_summary = replace(
            real_summary,
            last_session=real_summary.first_session,
        )
        self.assertNotEqual(forged_summary.summary_id, real_summary.summary_id)
        forged_fold_summaries = (forged_summary,) + comparison_run.fold_summaries[1:]

        # Bypass DeterministicComparisonRun's own constructor-time replay check
        # (which would otherwise reject this) by mutating post-construction and
        # recomputing only run_id, so hash-only verification alone would pass.
        object.__setattr__(comparison_run, "fold_summaries", forged_fold_summaries)
        object.__setattr__(comparison_run, "run_id", comparison_run._calculated_id())
        comparison_run.verify_content_identity()

        with self.assertRaises(RegimeEnsembleRunError):
            RegimeEnsembleEvaluationRun(
                registration=run.registration,
                intent_run=run.intent_run,
                benchmark_config=run.benchmark_config,
                cost_schedule=run.cost_schedule,
                benchmark_batch=run.benchmark_batch,
                comparison_run=comparison_run,
            )


if __name__ == "__main__":
    unittest.main()
