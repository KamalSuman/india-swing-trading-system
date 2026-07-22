from __future__ import annotations

from dataclasses import dataclass, field

from india_swing.execution.costs import NseDeliveryCostSchedule
from india_swing.identity import content_id

from .baselines import (
    DeterministicComparisonRun,
    DeterministicEqualWeightBenchmarkGenerator,
    EqualWeightBenchmarkConfig,
    GeneratedIntentBatch,
    build_fold_comparison_summaries,
)
from .engine import TrialEvaluationComparisonEngine, TrialEvaluationEngine
from .regime_ensemble import RegimeEnsembleIntentConfig, RegimeEnsembleIntentRun
from .trials import TrialRegistration


class RegimeEnsembleRunError(ValueError):
    pass


def _verify_sanitized(verify: object, message: str) -> None:
    try:
        verify()
    except Exception:
        raise RegimeEnsembleRunError(message) from None


def _call_sanitized(fn: object, message: str, /, **kwargs: object) -> object:
    try:
        return fn(**kwargs)
    except Exception:
        raise RegimeEnsembleRunError(message) from None


def regime_ensemble_trial_configuration_hash(
    intent_config: RegimeEnsembleIntentConfig,
    benchmark_config: EqualWeightBenchmarkConfig,
) -> str:
    """The only accepted TrialRegistration.configuration_hash for this orchestrator."""

    if type(intent_config) is not RegimeEnsembleIntentConfig:
        raise RegimeEnsembleRunError("intent_config must be exact")
    _verify_sanitized(
        intent_config.verify_content_identity, "intent config content identity failed"
    )
    if type(benchmark_config) is not EqualWeightBenchmarkConfig:
        raise RegimeEnsembleRunError("benchmark_config must be exact")
    _verify_sanitized(
        benchmark_config.verify_content_identity, "benchmark config content identity failed"
    )
    return content_id(
        {
            "schema": "regime-ensemble-trial-configuration/v1",
            "intent_config_id": intent_config.config_id,
            "benchmark_id": benchmark_config.benchmark_id,
        },
        length=64,
    )


def _validate_registration_binding(
    *,
    registration: TrialRegistration,
    intent_run: RegimeEnsembleIntentRun,
    benchmark_config: EqualWeightBenchmarkConfig,
    cost_schedule: NseDeliveryCostSchedule,
) -> None:
    """Preregistration cross-checks shared by evaluate() and Run.verify_content_identity().

    Registration synthetic-versus-readiness semantics, requested metric/
    threshold-direction support, dataset calendar equality, and registered
    evaluation-date containment are enforced by reusing
    TrialEvaluationEngine's own existing binding-validation boundary
    (``_validate_bindings``) rather than duplicating those rules here -- this
    is the exact function TrialEvaluationEngine.evaluate() itself calls, so a
    directly reconstructed aggregate is now held to the identical standard
    without ever re-running the simulator.
    """

    if registration.model_bundle_id != intent_run.config.config_id:
        raise RegimeEnsembleRunError(
            "registration model bundle differs from the intent run config"
        )
    if registration.benchmark_id != benchmark_config.benchmark_id:
        raise RegimeEnsembleRunError(
            "registration benchmark differs from the benchmark config"
        )
    if registration.configuration_hash != regime_ensemble_trial_configuration_hash(
        intent_run.config, benchmark_config
    ):
        raise RegimeEnsembleRunError(
            "registration configuration hash differs from the bound configs"
        )
    _call_sanitized(
        TrialEvaluationEngine._validate_bindings,
        "registration binding differs from the intent run",
        registration=registration,
        split_plan=intent_run.split_plan,
        dataset=intent_run.dataset,
        execution_policy=intent_run.execution_policy,
        cost_schedule=cost_schedule,
    )


def _validate_comparison_binding(
    *,
    registration: TrialRegistration,
    intent_run: RegimeEnsembleIntentRun,
    cost_schedule: NseDeliveryCostSchedule,
    comparison_run: DeterministicComparisonRun,
) -> None:
    """Bind the comparison's own evidence back to the registration and intent run.

    DeterministicComparisonRun.verify_content_identity() only validates
    content hashes; it does not know about this orchestrator's registration,
    intent run, or cost schedule. This function requires every present
    base/stressed TrialEvaluationResult to bind the exact registered trial,
    split plan, dataset, execution policy, cost schedule, initial capital,
    and pass thresholds, and requires the comparison's own trial/strategy/
    benchmark/primary-metric/slippage fields to match the registration.
    """

    comparison = comparison_run.comparison
    if comparison.trial_id != registration.trial_id:
        raise RegimeEnsembleRunError("comparison trial differs from the registration")
    if comparison.strategy_id != registration.model_bundle_id:
        raise RegimeEnsembleRunError(
            "comparison strategy differs from the registration"
        )
    if comparison.benchmark_id != registration.benchmark_id:
        raise RegimeEnsembleRunError(
            "comparison benchmark differs from the registration"
        )
    if comparison.primary_metric != registration.primary_metric:
        raise RegimeEnsembleRunError(
            "comparison primary metric differs from the registration"
        )
    if (
        comparison.base_slippage_bps != registration.base_slippage_bps
        or comparison.stressed_slippage_bps != registration.stressed_slippage_bps
    ):
        raise RegimeEnsembleRunError(
            "comparison slippage differs from the registration"
        )

    stressed_present = (
        comparison.strategy_stressed is not None,
        comparison.benchmark_stressed is not None,
    )
    if stressed_present[0] != stressed_present[1]:
        raise RegimeEnsembleRunError(
            "comparison stressed results do not share an all-or-none shape"
        )
    if stressed_present[0] != (registration.stressed_slippage_bps is not None):
        raise RegimeEnsembleRunError(
            "comparison stressed shape differs from the registered stressed slippage"
        )

    for result in (
        comparison.strategy_base,
        comparison.benchmark_base,
        comparison.strategy_stressed,
        comparison.benchmark_stressed,
    ):
        if result is None:
            continue
        if (
            result.trial_id != registration.trial_id
            or result.split_plan_id != intent_run.split_plan.plan_id
            or result.dataset_id != intent_run.dataset.dataset_id
            or result.execution_policy_id != intent_run.execution_policy.policy_id
            or result.cost_schedule_id != cost_schedule.schedule_id
            or result.initial_capital != intent_run.initial_capital
            or result.pass_thresholds != registration.pass_thresholds
        ):
            raise RegimeEnsembleRunError(
                "comparison result binding differs from the registration or intent run"
            )


@dataclass(frozen=True, slots=True)
class RegimeEnsembleEvaluationRun:
    """One preregistration-bound evaluation of an already-approved intent run.

    This aggregate is research-only: ``execution_eligible`` and
    ``promotion_eligible`` are always false. It evaluates exactly one
    preregistered configuration; it cannot mutate parameters, aggregate a
    family, or authorize deployment or capital.
    """

    registration: TrialRegistration
    intent_run: RegimeEnsembleIntentRun
    benchmark_config: EqualWeightBenchmarkConfig
    cost_schedule: NseDeliveryCostSchedule
    benchmark_batch: GeneratedIntentBatch
    comparison_run: DeterministicComparisonRun
    run_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._verify()
        object.__setattr__(self, "run_id", self._calculated_id())

    def _verify(self) -> None:
        if type(self.registration) is not TrialRegistration:
            raise RegimeEnsembleRunError("registration must be exact")
        _verify_sanitized(
            self.registration.verify_content_identity, "registration content identity failed"
        )
        if type(self.intent_run) is not RegimeEnsembleIntentRun:
            raise RegimeEnsembleRunError("intent_run must be exact")
        _verify_sanitized(
            self.intent_run.verify_content_identity, "intent run content identity failed"
        )
        if type(self.benchmark_config) is not EqualWeightBenchmarkConfig:
            raise RegimeEnsembleRunError("benchmark_config must be exact")
        _verify_sanitized(
            self.benchmark_config.verify_content_identity,
            "benchmark config content identity failed",
        )
        if type(self.cost_schedule) is not NseDeliveryCostSchedule:
            raise RegimeEnsembleRunError("cost_schedule must be exact")
        _verify_sanitized(
            self.cost_schedule.verify_content_identity, "cost schedule content identity failed"
        )
        if type(self.benchmark_batch) is not GeneratedIntentBatch:
            raise RegimeEnsembleRunError("benchmark_batch must be exact")
        _verify_sanitized(
            self.benchmark_batch.verify_content_identity, "benchmark batch content identity failed"
        )
        if type(self.comparison_run) is not DeterministicComparisonRun:
            raise RegimeEnsembleRunError("comparison_run must be exact")
        _verify_sanitized(
            self.comparison_run.verify_content_identity, "comparison run content identity failed"
        )

        _validate_registration_binding(
            registration=self.registration,
            intent_run=self.intent_run,
            benchmark_config=self.benchmark_config,
            cost_schedule=self.cost_schedule,
        )

        if self.registration.trial_id != self.comparison_run.comparison.trial_id:
            raise RegimeEnsembleRunError(
                "registration trial differs from the comparison run"
            )
        if self.comparison_run.strategy_batch.batch_id != self.intent_run.generated_batch.batch_id:
            raise RegimeEnsembleRunError(
                "comparison strategy batch differs from the intent run's generated batch"
            )
        if self.comparison_run.benchmark_batch.batch_id != self.benchmark_batch.batch_id:
            raise RegimeEnsembleRunError(
                "comparison benchmark batch differs from the embedded benchmark batch"
            )

        _validate_comparison_binding(
            registration=self.registration,
            intent_run=self.intent_run,
            cost_schedule=self.cost_schedule,
            comparison_run=self.comparison_run,
        )

        expected_benchmark_batch = _call_sanitized(
            DeterministicEqualWeightBenchmarkGenerator().generate,
            "benchmark batch does not replay from the embedded intent run",
            config=self.benchmark_config,
            split_plan=self.intent_run.split_plan,
            dataset=self.intent_run.dataset,
            instruments=self.intent_run.instruments,
            execution_policy=self.intent_run.execution_policy,
            initial_capital=self.intent_run.initial_capital,
        )
        if expected_benchmark_batch.batch_id != self.benchmark_batch.batch_id:
            raise RegimeEnsembleRunError(
                "benchmark batch does not replay from the embedded intent run"
            )

        expected_fold_summaries = _call_sanitized(
            build_fold_comparison_summaries,
            "fold summaries do not replay from the embedded intent run",
            split_plan=self.intent_run.split_plan,
            strategy_batch=self.intent_run.generated_batch,
            benchmark_batch=expected_benchmark_batch,
            comparison=self.comparison_run.comparison,
        )
        if tuple(item.summary_id for item in expected_fold_summaries) != tuple(
            item.summary_id for item in self.comparison_run.fold_summaries
        ):
            raise RegimeEnsembleRunError(
                "fold summaries do not replay from the embedded intent run"
            )

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "regime-ensemble-evaluation-run/v1",
                "trial_id": self.registration.trial_id,
                "intent_run_id": self.intent_run.run_id,
                "benchmark_config_id": self.benchmark_config.benchmark_id,
                "cost_schedule_id": self.cost_schedule.schedule_id,
                "benchmark_batch_id": self.benchmark_batch.batch_id,
                "comparison_run_id": self.comparison_run.run_id,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._verify()
        if self.run_id != self._calculated_id():
            raise RegimeEnsembleRunError("evaluation run content identity failed")

    @property
    def execution_eligible(self) -> bool:
        return False

    @property
    def promotion_eligible(self) -> bool:
        return False


class RegimeEnsembleEvaluationEngine:
    """Bind one preregistered configuration to one deterministic comparison.

    This engine never copies or reimplements fill, cost, slippage, equity,
    metric, fold-summary, benchmark, or split formulas -- it calls the
    existing DeterministicEqualWeightBenchmarkGenerator and
    TrialEvaluationComparisonEngine exactly once each, and builds fold
    summaries only through build_fold_comparison_summaries.
    """

    def evaluate(
        self,
        *,
        registration: TrialRegistration,
        intent_run: RegimeEnsembleIntentRun,
        benchmark_config: EqualWeightBenchmarkConfig,
        cost_schedule: NseDeliveryCostSchedule,
    ) -> RegimeEnsembleEvaluationRun:
        if type(registration) is not TrialRegistration:
            raise RegimeEnsembleRunError("registration must be exact")
        _verify_sanitized(
            registration.verify_content_identity, "registration content identity failed"
        )
        if type(intent_run) is not RegimeEnsembleIntentRun:
            raise RegimeEnsembleRunError("intent_run must be exact")
        _verify_sanitized(
            intent_run.verify_content_identity, "intent run content identity failed"
        )
        if type(benchmark_config) is not EqualWeightBenchmarkConfig:
            raise RegimeEnsembleRunError("benchmark_config must be exact")
        _verify_sanitized(
            benchmark_config.verify_content_identity, "benchmark config content identity failed"
        )
        if type(cost_schedule) is not NseDeliveryCostSchedule:
            raise RegimeEnsembleRunError("cost_schedule must be exact")
        _verify_sanitized(
            cost_schedule.verify_content_identity, "cost schedule content identity failed"
        )

        _validate_registration_binding(
            registration=registration,
            intent_run=intent_run,
            benchmark_config=benchmark_config,
            cost_schedule=cost_schedule,
        )

        benchmark_batch = _call_sanitized(
            DeterministicEqualWeightBenchmarkGenerator().generate,
            "benchmark batch generation failed",
            config=benchmark_config,
            split_plan=intent_run.split_plan,
            dataset=intent_run.dataset,
            instruments=intent_run.instruments,
            execution_policy=intent_run.execution_policy,
            initial_capital=intent_run.initial_capital,
        )
        comparison = _call_sanitized(
            TrialEvaluationComparisonEngine().evaluate,
            "comparison evaluation failed",
            registration=registration,
            split_plan=intent_run.split_plan,
            dataset=intent_run.dataset,
            strategy_intents=intent_run.generated_batch.intents,
            benchmark_intents=benchmark_batch.intents,
            execution_policy=intent_run.execution_policy,
            cost_schedule=cost_schedule,
            initial_capital=intent_run.initial_capital,
        )
        fold_summaries = _call_sanitized(
            build_fold_comparison_summaries,
            "fold summary construction failed",
            split_plan=intent_run.split_plan,
            strategy_batch=intent_run.generated_batch,
            benchmark_batch=benchmark_batch,
            comparison=comparison,
        )
        comparison_run = _call_sanitized(
            DeterministicComparisonRun,
            "comparison run construction failed",
            strategy_batch=intent_run.generated_batch,
            benchmark_batch=benchmark_batch,
            comparison=comparison,
            fold_summaries=fold_summaries,
        )
        return RegimeEnsembleEvaluationRun(
            registration=registration,
            intent_run=intent_run,
            benchmark_config=benchmark_config,
            cost_schedule=cost_schedule,
            benchmark_batch=benchmark_batch,
            comparison_run=comparison_run,
        )
