from __future__ import annotations

import re
from dataclasses import dataclass, field, fields
from decimal import Decimal

from india_swing.identity import content_id

from .engine import SUPPORTED_METRICS, EvaluationDataReadiness
from .regime_ensemble_run import RegimeEnsembleEvaluationRun


REGIME_ENSEMBLE_EVALUATION_REPORT_SCHEMA_VERSION = "regime-ensemble-evaluation-report/v1"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")

REGIME_ENSEMBLE_REPORT_CAVEATS = (
    "Ensemble scores and score-implied returns are uncalibrated research values, not probabilities.",
    "This report makes no profitability claim; results are retrospective and cost-aware, not predictive.",
    "This report does not authorize promotion to confirmatory status or production deployment.",
    "This report carries no execution, broker, order, or real-capital authority.",
)


class RegimeEnsembleReportError(ValueError):
    pass


def _sha(value: str, name: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise RegimeEnsembleReportError(f"{name} must be a full lowercase SHA-256")


def _non_negative_int(value: int, name: str) -> None:
    if type(value) is not int or value < 0:
        raise RegimeEnsembleReportError(f"{name} must be a non-negative integer")


def _validate_metric_tuple(
    metrics: tuple[tuple[str, Decimal], ...], name: str
) -> None:
    if (
        type(metrics) is not tuple
        or tuple(item[0] for item in metrics) != tuple(sorted(SUPPORTED_METRICS))
    ):
        raise RegimeEnsembleReportError(f"{name} must contain the complete metric set")
    for _, value in metrics:
        if type(value) is not Decimal or not value.is_finite():
            raise RegimeEnsembleReportError(f"{name} values must be finite Decimals")


@dataclass(frozen=True, slots=True)
class RegimeEnsembleEvaluationReport:
    """A compact, derived research summary of one RegimeEnsembleEvaluationRun.

    This is descriptive evidence, not an authority object: it invents no
    confidence, probability, annual-return target, or narrative cause, and
    ``execution_eligible``/``promotion_eligible`` are always false.
    """

    trial_id: str
    evaluation_run_id: str
    intent_run_id: str
    deterministic_run_id: str
    comparison_id: str
    config_id: str
    benchmark_id: str
    split_plan_id: str
    dataset_id: str
    dataset_readiness: EvaluationDataReadiness
    fold_count: int
    proposal_count: int
    strategy_decision_count: int
    selected_intent_count: int
    strategy_trade_count: int
    benchmark_trade_count: int
    primary_metric: str
    strategy_base_metrics: tuple[tuple[str, Decimal], ...]
    benchmark_base_metrics: tuple[tuple[str, Decimal], ...]
    comparison_metrics: tuple[tuple[str, Decimal], ...]
    strategy_stressed_metrics: tuple[tuple[str, Decimal], ...] | None
    benchmark_stressed_metrics: tuple[tuple[str, Decimal], ...] | None
    fold_summary_ids: tuple[str, ...]
    passed: bool
    outperformed: bool
    caveats: tuple[str, ...] = field(default_factory=lambda: REGIME_ENSEMBLE_REPORT_CAVEATS)
    schema_version: str = REGIME_ENSEMBLE_EVALUATION_REPORT_SCHEMA_VERSION
    report_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.trial_id, "trial_id"),
            (self.evaluation_run_id, "evaluation_run_id"),
            (self.intent_run_id, "intent_run_id"),
            (self.deterministic_run_id, "deterministic_run_id"),
            (self.comparison_id, "comparison_id"),
            (self.config_id, "config_id"),
            (self.benchmark_id, "benchmark_id"),
            (self.split_plan_id, "split_plan_id"),
            (self.dataset_id, "dataset_id"),
        ):
            _sha(value, name)
        if type(self.dataset_readiness) is not EvaluationDataReadiness:
            raise RegimeEnsembleReportError("dataset_readiness must be exact")

        for value, name in (
            (self.fold_count, "fold_count"),
            (self.proposal_count, "proposal_count"),
            (self.strategy_decision_count, "strategy_decision_count"),
            (self.selected_intent_count, "selected_intent_count"),
            (self.strategy_trade_count, "strategy_trade_count"),
            (self.benchmark_trade_count, "benchmark_trade_count"),
        ):
            _non_negative_int(value, name)
        if self.fold_count < 1:
            raise RegimeEnsembleReportError("fold_count must be at least one")
        if self.proposal_count != self.strategy_decision_count:
            raise RegimeEnsembleReportError(
                "proposal_count must equal strategy_decision_count"
            )
        if self.selected_intent_count > self.strategy_decision_count:
            raise RegimeEnsembleReportError(
                "selected_intent_count cannot exceed strategy_decision_count"
            )
        if self.strategy_trade_count > self.selected_intent_count:
            raise RegimeEnsembleReportError(
                "strategy_trade_count cannot exceed selected_intent_count"
            )
        if self.proposal_count < self.fold_count:
            raise RegimeEnsembleReportError(
                "proposal_count cannot be smaller than fold_count"
            )

        if not isinstance(self.primary_metric, str) or self.primary_metric not in (
            SUPPORTED_METRICS - {"turnover"}
        ):
            raise RegimeEnsembleReportError("primary_metric is unsupported")

        _validate_metric_tuple(self.strategy_base_metrics, "strategy_base_metrics")
        _validate_metric_tuple(self.benchmark_base_metrics, "benchmark_base_metrics")

        if (
            type(self.comparison_metrics) is not tuple
            or not self.comparison_metrics
            or tuple(name for name, _ in self.comparison_metrics)
            != tuple(sorted({name for name, _ in self.comparison_metrics}))
        ):
            raise RegimeEnsembleReportError(
                "comparison_metrics must be a sorted unique non-empty tuple"
            )
        for name, value in self.comparison_metrics:
            if not isinstance(name, str) or not name:
                raise RegimeEnsembleReportError("comparison metric name is required")
            if type(value) is not Decimal or not value.is_finite():
                raise RegimeEnsembleReportError("comparison metric values must be finite Decimals")

        stressed = (self.strategy_stressed_metrics, self.benchmark_stressed_metrics)
        if (stressed[0] is None) != (stressed[1] is None):
            raise RegimeEnsembleReportError(
                "stressed metrics must both be present or absent"
            )
        for metrics in stressed:
            if metrics is not None:
                _validate_metric_tuple(metrics, "stressed_metrics")

        if (
            type(self.fold_summary_ids) is not tuple
            or len(self.fold_summary_ids) != self.fold_count
            or len(set(self.fold_summary_ids)) != len(self.fold_summary_ids)
        ):
            raise RegimeEnsembleReportError(
                "fold_summary_ids must be a unique tuple with one entry per fold"
            )
        for value in self.fold_summary_ids:
            _sha(value, "fold_summary_id")

        if type(self.passed) is not bool or type(self.outperformed) is not bool:
            raise RegimeEnsembleReportError("passed and outperformed must be bool")

        if self.caveats != REGIME_ENSEMBLE_REPORT_CAVEATS:
            raise RegimeEnsembleReportError("caveats must be the fixed research disclosure tuple")
        if self.schema_version != REGIME_ENSEMBLE_EVALUATION_REPORT_SCHEMA_VERSION:
            raise RegimeEnsembleReportError("unsupported evaluation report schema")

        object.__setattr__(self, "report_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "report_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.report_id != self._calculated_id():
            raise RegimeEnsembleReportError("report content identity failed")

    @property
    def execution_eligible(self) -> bool:
        return False

    @property
    def promotion_eligible(self) -> bool:
        return False


def build_regime_ensemble_evaluation_report(
    run: RegimeEnsembleEvaluationRun,
) -> RegimeEnsembleEvaluationReport:
    """Derive one compact report entirely from the exact verified evaluation run."""

    if type(run) is not RegimeEnsembleEvaluationRun:
        raise RegimeEnsembleReportError("run must be exact")
    run.verify_content_identity()

    comparison = run.comparison_run.comparison
    proposal_count = sum(
        len(result.proposal_batch.proposals) for result in run.intent_run.fold_results
    )
    return RegimeEnsembleEvaluationReport(
        trial_id=run.registration.trial_id,
        evaluation_run_id=run.run_id,
        intent_run_id=run.intent_run.run_id,
        deterministic_run_id=run.comparison_run.run_id,
        comparison_id=comparison.comparison_id,
        config_id=run.intent_run.config.config_id,
        benchmark_id=run.benchmark_config.benchmark_id,
        split_plan_id=run.intent_run.split_plan.plan_id,
        dataset_id=run.intent_run.dataset.dataset_id,
        dataset_readiness=run.intent_run.dataset.readiness,
        fold_count=len(run.intent_run.fold_results),
        proposal_count=proposal_count,
        strategy_decision_count=len(run.intent_run.generated_batch.decisions),
        selected_intent_count=len(run.intent_run.generated_batch.intents),
        strategy_trade_count=len(comparison.strategy_base.trades),
        benchmark_trade_count=len(comparison.benchmark_base.trades),
        primary_metric=comparison.primary_metric,
        strategy_base_metrics=comparison.strategy_base.metrics,
        benchmark_base_metrics=comparison.benchmark_base.metrics,
        comparison_metrics=comparison.comparison_metrics,
        strategy_stressed_metrics=(
            None
            if comparison.strategy_stressed is None
            else comparison.strategy_stressed.metrics
        ),
        benchmark_stressed_metrics=(
            None
            if comparison.benchmark_stressed is None
            else comparison.benchmark_stressed.metrics
        ),
        fold_summary_ids=tuple(item.summary_id for item in run.comparison_run.fold_summaries),
        passed=comparison.passed,
        outperformed=all(item.outperformed for item in run.comparison_run.fold_summaries),
    )
