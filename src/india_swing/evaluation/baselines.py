from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, localcontext
from enum import Enum

from india_swing.execution.costs import NseDeliveryCostSchedule
from india_swing.execution.simulator import LimitEntryOrder, SimulationBar
from india_swing.identity import content_id

from .engine import (
    DailyExecutionPolicy,
    EvaluationDataset,
    EvaluationTradeIntent,
    SUPPORTED_METRICS,
    TrialEvaluationComparisonEngine,
    TrialEvaluationComparisonResult,
    TrialEvaluationError,
)
from .models import PurgedWalkForwardPlan
from .trials import TrialRegistration


ZERO = Decimal("0")
ONE = Decimal("1")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class DeterministicBaselineError(TrialEvaluationError):
    pass


def _sha(value: str, name: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise DeterministicBaselineError(f"{name} must be a full lowercase SHA-256")


def _positive_decimal(value: Decimal, name: str) -> None:
    if type(value) is not Decimal or not value.is_finite() or value <= ZERO:
        raise DeterministicBaselineError(f"{name} must be a finite positive Decimal")


def _fraction(value: Decimal, name: str) -> None:
    _positive_decimal(value, name)
    if value >= ONE:
        raise DeterministicBaselineError(f"{name} must be below one")


def _positive_integer(value: int, name: str) -> None:
    if type(value) is not int or value <= 0:
        raise DeterministicBaselineError(f"{name} must be a positive integer")


def _normalized_text(value: str, name: str) -> None:
    if not isinstance(value, str) or not value or value != value.strip():
        raise DeterministicBaselineError(f"{name} must be normalized non-empty text")


class GeneratedIntentRole(str, Enum):
    STRATEGY = "STRATEGY"
    BENCHMARK = "BENCHMARK"


@dataclass(frozen=True, slots=True)
class PointInTimeInstrument:
    symbol: str
    isin: str
    universe_snapshot_id: str
    eligible_sessions: tuple[date, ...]
    tick_size: Decimal
    stable_instrument_id: str | None = None
    eligibility_bindings: tuple[tuple[date, str], ...] = ()
    instrument_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.symbol, str)
            or not self.symbol
            or self.symbol != self.symbol.strip().upper()
        ):
            raise DeterministicBaselineError("symbol must be normalized uppercase text")
        if (
            not isinstance(self.isin, str)
            or len(self.isin) != 12
            or self.isin != self.isin.strip().upper()
        ):
            raise DeterministicBaselineError("isin must be normalized 12-character text")
        _sha(self.universe_snapshot_id, "universe_snapshot_id")
        if (
            type(self.eligible_sessions) is not tuple
            or not self.eligible_sessions
            or any(type(value) is not date for value in self.eligible_sessions)
            or self.eligible_sessions != tuple(sorted(set(self.eligible_sessions)))
        ):
            raise DeterministicBaselineError(
                "eligible_sessions must be a sorted unique non-empty tuple"
            )
        _positive_decimal(self.tick_size, "tick_size")
        if self.stable_instrument_id is not None and (
            not isinstance(self.stable_instrument_id, str)
            or not self.stable_instrument_id.strip()
        ):
            raise DeterministicBaselineError(
                "stable_instrument_id must be non-empty text when supplied"
            )
        if type(self.eligibility_bindings) is not tuple:
            raise DeterministicBaselineError(
                "eligibility_bindings must be an immutable tuple"
            )
        if self.eligibility_bindings:
            if (
                tuple(session for session, _ in self.eligibility_bindings)
                != self.eligible_sessions
            ):
                raise DeterministicBaselineError(
                    "eligibility bindings must cover every eligible session exactly once"
                )
            for session, snapshot_id in self.eligibility_bindings:
                if type(session) is not date:
                    raise DeterministicBaselineError(
                        "eligibility binding sessions must be dates"
                    )
                _sha(snapshot_id, "eligibility binding universe_snapshot_id")
            if self.universe_snapshot_id != self.eligibility_bindings[0][1]:
                raise DeterministicBaselineError(
                    "primary universe snapshot must equal the first eligibility binding"
                )
        object.__setattr__(self, "instrument_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "point-in-time-evaluation-instrument/v1",
                "symbol": self.symbol,
                "isin": self.isin,
                "universe_snapshot_id": self.universe_snapshot_id,
                "eligible_sessions": self.eligible_sessions,
                "tick_size": self.tick_size,
                "stable_instrument_id": self.stable_instrument_id,
                "eligibility_bindings": self.eligibility_bindings,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.instrument_id != self._calculated_id():
            raise DeterministicBaselineError("instrument content identity failed")


@dataclass(frozen=True, slots=True)
class MomentumBaselineConfig:
    lookback_sessions: int
    maximum_positions: int
    gross_exposure_fraction: Decimal
    minimum_momentum: Decimal
    stop_loss_fraction: Decimal
    target_gain_fraction: Decimal
    maximum_holding_sessions: int
    version: str = "cross-sectional-close-momentum/v1"
    strategy_id: str = field(init=False)

    def __post_init__(self) -> None:
        _positive_integer(self.lookback_sessions, "lookback_sessions")
        _positive_integer(self.maximum_positions, "maximum_positions")
        _fraction(self.gross_exposure_fraction, "gross_exposure_fraction")
        if type(self.minimum_momentum) is not Decimal or not self.minimum_momentum.is_finite():
            raise DeterministicBaselineError("minimum_momentum must be a finite Decimal")
        _fraction(self.stop_loss_fraction, "stop_loss_fraction")
        _positive_decimal(self.target_gain_fraction, "target_gain_fraction")
        _positive_integer(self.maximum_holding_sessions, "maximum_holding_sessions")
        _normalized_text(self.version, "version")
        object.__setattr__(self, "strategy_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "deterministic-momentum-baseline-config/v1",
                "lookback_sessions": self.lookback_sessions,
                "maximum_positions": self.maximum_positions,
                "gross_exposure_fraction": self.gross_exposure_fraction,
                "minimum_momentum": self.minimum_momentum,
                "stop_loss_fraction": self.stop_loss_fraction,
                "target_gain_fraction": self.target_gain_fraction,
                "maximum_holding_sessions": self.maximum_holding_sessions,
                "version": self.version,
                "ranking": "MOMENTUM_DESC_THEN_SIGNAL_TURNOVER_DESC_THEN_SYMBOL",
                "signal_timing": "TEST_FOLD_FIRST_SESSION_CLOSE",
                "entry_timing": "NEXT_SESSION_LIMIT_AT_SIGNAL_CLOSE",
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.strategy_id != self._calculated_id():
            raise DeterministicBaselineError("strategy config content identity failed")


@dataclass(frozen=True, slots=True)
class EqualWeightBenchmarkConfig:
    maximum_constituents: int
    gross_exposure_fraction: Decimal
    stop_loss_fraction: Decimal
    target_gain_fraction: Decimal
    maximum_holding_sessions: int
    version: str = "point-in-time-liquid-equal-weight/v1"
    benchmark_id: str = field(init=False)

    def __post_init__(self) -> None:
        _positive_integer(self.maximum_constituents, "maximum_constituents")
        _fraction(self.gross_exposure_fraction, "gross_exposure_fraction")
        _fraction(self.stop_loss_fraction, "stop_loss_fraction")
        _positive_decimal(self.target_gain_fraction, "target_gain_fraction")
        _positive_integer(self.maximum_holding_sessions, "maximum_holding_sessions")
        _normalized_text(self.version, "version")
        object.__setattr__(self, "benchmark_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "deterministic-equal-weight-benchmark-config/v1",
                "maximum_constituents": self.maximum_constituents,
                "gross_exposure_fraction": self.gross_exposure_fraction,
                "stop_loss_fraction": self.stop_loss_fraction,
                "target_gain_fraction": self.target_gain_fraction,
                "maximum_holding_sessions": self.maximum_holding_sessions,
                "version": self.version,
                "selection": "SIGNAL_TURNOVER_DESC_THEN_SYMBOL",
                "weighting": "EQUAL_SLOT_NOTIONAL",
                "signal_timing": "TEST_FOLD_FIRST_SESSION_CLOSE",
                "entry_timing": "NEXT_SESSION_LIMIT_AT_SIGNAL_CLOSE",
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.benchmark_id != self._calculated_id():
            raise DeterministicBaselineError("benchmark config content identity failed")


@dataclass(frozen=True, slots=True)
class GeneratedSignalDecision:
    generator_id: str
    role: GeneratedIntentRole
    fold_id: str
    signal_session: date
    instrument_id: str
    symbol: str
    score_name: str
    score: Decimal | None
    selected: bool
    reason: str
    evidence_bar_ids: tuple[str, ...]
    decision_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.generator_id, "generator_id"),
            (self.fold_id, "fold_id"),
            (self.instrument_id, "instrument_id"),
        ):
            _sha(value, name)
        if type(self.role) is not GeneratedIntentRole:
            raise DeterministicBaselineError("role must be exact")
        if type(self.signal_session) is not date:
            raise DeterministicBaselineError("signal_session must be a date")
        if not isinstance(self.symbol, str) or self.symbol != self.symbol.strip().upper():
            raise DeterministicBaselineError("decision symbol must be normalized")
        _normalized_text(self.score_name, "score_name")
        if self.score is not None and (
            type(self.score) is not Decimal or not self.score.is_finite()
        ):
            raise DeterministicBaselineError("score must be a finite Decimal or None")
        if type(self.selected) is not bool:
            raise DeterministicBaselineError("selected must be bool")
        _normalized_text(self.reason, "reason")
        if (
            type(self.evidence_bar_ids) is not tuple
            or len(set(self.evidence_bar_ids)) != len(self.evidence_bar_ids)
        ):
            raise DeterministicBaselineError("evidence bar IDs must be an exact unique tuple")
        for value in self.evidence_bar_ids:
            _sha(value, "evidence_bar_id")
        if self.selected != (self.reason == "SELECTED"):
            raise DeterministicBaselineError("selected flag and reason disagree")
        object.__setattr__(self, "decision_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "generated-evaluation-signal-decision/v1",
                "generator_id": self.generator_id,
                "role": self.role,
                "fold_id": self.fold_id,
                "signal_session": self.signal_session,
                "instrument_id": self.instrument_id,
                "symbol": self.symbol,
                "score_name": self.score_name,
                "score": self.score,
                "selected": self.selected,
                "reason": self.reason,
                "evidence_bar_ids": self.evidence_bar_ids,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.decision_id != self._calculated_id():
            raise DeterministicBaselineError("signal decision content identity failed")


@dataclass(frozen=True, slots=True)
class GeneratedIntentBatch:
    generator_id: str
    role: GeneratedIntentRole
    split_plan_id: str
    source_snapshot_ids: tuple[str, ...]
    decisions: tuple[GeneratedSignalDecision, ...]
    intents: tuple[EvaluationTradeIntent, ...]
    batch_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha(self.generator_id, "generator_id")
        _sha(self.split_plan_id, "split_plan_id")
        if type(self.role) is not GeneratedIntentRole:
            raise DeterministicBaselineError("batch role must be exact")
        if (
            type(self.source_snapshot_ids) is not tuple
            or not self.source_snapshot_ids
            or self.source_snapshot_ids != tuple(sorted(set(self.source_snapshot_ids)))
        ):
            raise DeterministicBaselineError("source snapshots must be sorted and unique")
        for value in self.source_snapshot_ids:
            _sha(value, "source_snapshot_id")
        if (
            type(self.decisions) is not tuple
            or not self.decisions
            or self.decisions
            != tuple(sorted(self.decisions, key=lambda item: (item.signal_session, item.symbol)))
        ):
            raise DeterministicBaselineError("decisions must be an ordered exact tuple")
        decision_keys = tuple(
            (item.fold_id, item.signal_session, item.instrument_id)
            for item in self.decisions
        )
        if len(set(decision_keys)) != len(decision_keys):
            raise DeterministicBaselineError("decisions cannot contain duplicate candidates")
        for decision in self.decisions:
            if type(decision) is not GeneratedSignalDecision:
                raise DeterministicBaselineError("decisions must contain exact values")
            decision.verify_content_identity()
            if decision.generator_id != self.generator_id or decision.role is not self.role:
                raise DeterministicBaselineError("decision binding differs from batch")
        if (
            type(self.intents) is not tuple
            or self.intents
            != tuple(
                sorted(
                    self.intents,
                    key=lambda item: (item.entry_order.signal_session, item.intent_id),
                )
            )
        ):
            raise DeterministicBaselineError("intents must be an ordered exact tuple")
        for intent in self.intents:
            if type(intent) is not EvaluationTradeIntent:
                raise DeterministicBaselineError("intents must contain exact values")
            intent.verify_content_identity()
        selected_keys = {
            (item.signal_session, item.symbol, item.decision_id)
            for item in self.decisions
            if item.selected
        }
        intent_keys = {
            (item.entry_order.signal_session, item.entry_order.symbol, item.signal_id)
            for item in self.intents
        }
        if selected_keys != intent_keys:
            raise DeterministicBaselineError("selected decisions and intents disagree")
        object.__setattr__(self, "batch_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "generated-evaluation-intent-batch/v1",
                "generator_id": self.generator_id,
                "role": self.role,
                "split_plan_id": self.split_plan_id,
                "source_snapshot_ids": self.source_snapshot_ids,
                "decisions": self.decisions,
                "intents": self.intents,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.batch_id != self._calculated_id():
            raise DeterministicBaselineError("intent batch content identity failed")


@dataclass(frozen=True, slots=True)
class FoldComparisonSummary:
    fold_id: str
    first_session: date
    last_session: date
    primary_metric: str
    strategy_base_metrics: tuple[tuple[str, Decimal], ...]
    benchmark_base_metrics: tuple[tuple[str, Decimal], ...]
    strategy_stressed_metrics: tuple[tuple[str, Decimal], ...] | None
    benchmark_stressed_metrics: tuple[tuple[str, Decimal], ...] | None
    comparison_metrics: tuple[tuple[str, Decimal], ...] = field(init=False)
    outperformed: bool = field(init=False)
    summary_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha(self.fold_id, "fold_id")
        if (
            type(self.first_session) is not date
            or type(self.last_session) is not date
            or self.last_session < self.first_session
        ):
            raise DeterministicBaselineError("fold summary sessions are invalid")
        if self.primary_metric not in SUPPORTED_METRICS - {"turnover"}:
            raise DeterministicBaselineError("fold primary metric is unsupported")
        for metrics, name in (
            (self.strategy_base_metrics, "strategy_base_metrics"),
            (self.benchmark_base_metrics, "benchmark_base_metrics"),
        ):
            _validate_metric_tuple(metrics, name)
        stressed = (
            self.strategy_stressed_metrics,
            self.benchmark_stressed_metrics,
        )
        if (stressed[0] is None) != (stressed[1] is None):
            raise DeterministicBaselineError(
                "fold stress metrics must both be present or absent"
            )
        for metrics in stressed:
            if metrics is not None:
                _validate_metric_tuple(metrics, "stressed_metrics")
        base_excess = (
            dict(self.strategy_base_metrics)[self.primary_metric]
            - dict(self.benchmark_base_metrics)[self.primary_metric]
        )
        comparison_metrics = [("base_primary_excess", base_excess)]
        outperformed = base_excess >= ZERO
        if stressed[0] is not None and stressed[1] is not None:
            stressed_excess = (
                dict(stressed[0])[self.primary_metric]
                - dict(stressed[1])[self.primary_metric]
            )
            comparison_metrics.append(("stressed_primary_excess", stressed_excess))
            outperformed = outperformed and stressed_excess >= ZERO
        object.__setattr__(self, "comparison_metrics", tuple(comparison_metrics))
        object.__setattr__(self, "outperformed", outperformed)
        object.__setattr__(self, "summary_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "deterministic-fold-comparison-summary/v1",
                "fold_id": self.fold_id,
                "first_session": self.first_session,
                "last_session": self.last_session,
                "primary_metric": self.primary_metric,
                "strategy_base_metrics": self.strategy_base_metrics,
                "benchmark_base_metrics": self.benchmark_base_metrics,
                "strategy_stressed_metrics": self.strategy_stressed_metrics,
                "benchmark_stressed_metrics": self.benchmark_stressed_metrics,
                "comparison_metrics": self.comparison_metrics,
                "outperformed": self.outperformed,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.summary_id != self._calculated_id():
            raise DeterministicBaselineError("fold summary content identity failed")


def _validate_metric_tuple(
    metrics: tuple[tuple[str, Decimal], ...], name: str
) -> None:
    if (
        type(metrics) is not tuple
        or tuple(item[0] for item in metrics) != tuple(sorted(SUPPORTED_METRICS))
    ):
        raise DeterministicBaselineError(f"{name} must contain the complete metric set")
    for _, value in metrics:
        if type(value) is not Decimal or not value.is_finite():
            raise DeterministicBaselineError(f"{name} values must be finite Decimals")


@dataclass(frozen=True, slots=True)
class DeterministicComparisonRun:
    strategy_batch: GeneratedIntentBatch
    benchmark_batch: GeneratedIntentBatch
    comparison: TrialEvaluationComparisonResult
    fold_summaries: tuple[FoldComparisonSummary, ...]
    run_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.strategy_batch) is not GeneratedIntentBatch:
            raise DeterministicBaselineError("strategy_batch must be exact")
        if type(self.benchmark_batch) is not GeneratedIntentBatch:
            raise DeterministicBaselineError("benchmark_batch must be exact")
        if type(self.comparison) is not TrialEvaluationComparisonResult:
            raise DeterministicBaselineError("comparison must be exact")
        if (
            type(self.fold_summaries) is not tuple
            or not self.fold_summaries
            or self.fold_summaries
            != tuple(sorted(self.fold_summaries, key=lambda item: item.first_session))
        ):
            raise DeterministicBaselineError("fold summaries must be an ordered exact tuple")
        if len({item.fold_id for item in self.fold_summaries}) != len(self.fold_summaries):
            raise DeterministicBaselineError("fold summaries cannot repeat a fold")
        for summary in self.fold_summaries:
            if type(summary) is not FoldComparisonSummary:
                raise DeterministicBaselineError("fold summaries must contain exact values")
            summary.verify_content_identity()
            if summary.primary_metric != self.comparison.primary_metric:
                raise DeterministicBaselineError("fold summary primary metric differs")
            strategy_decisions = tuple(
                item for item in self.strategy_batch.decisions if item.fold_id == summary.fold_id
            )
            benchmark_decisions = tuple(
                item for item in self.benchmark_batch.decisions if item.fold_id == summary.fold_id
            )
            if not strategy_decisions or not benchmark_decisions:
                raise DeterministicBaselineError("fold summary has no generated decisions")
            if any(
                item.signal_session != summary.first_session
                for item in strategy_decisions + benchmark_decisions
            ):
                raise DeterministicBaselineError("fold summary signal boundary differs")
            fold_sessions = tuple(
                point.session
                for point in self.comparison.strategy_base.equity_curve
                if summary.first_session <= point.session <= summary.last_session
            )
            if not fold_sessions or (
                fold_sessions[0] != summary.first_session
                or fold_sessions[-1] != summary.last_session
            ):
                raise DeterministicBaselineError("fold summary curve boundary differs")
            expected = (
                _fold_metrics(
                    result=self.comparison.strategy_base,
                    batch=self.strategy_batch,
                    fold_sessions=fold_sessions,
                ),
                _fold_metrics(
                    result=self.comparison.benchmark_base,
                    batch=self.benchmark_batch,
                    fold_sessions=fold_sessions,
                ),
                None
                if self.comparison.strategy_stressed is None
                else _fold_metrics(
                    result=self.comparison.strategy_stressed,
                    batch=self.strategy_batch,
                    fold_sessions=fold_sessions,
                ),
                None
                if self.comparison.benchmark_stressed is None
                else _fold_metrics(
                    result=self.comparison.benchmark_stressed,
                    batch=self.benchmark_batch,
                    fold_sessions=fold_sessions,
                ),
            )
            actual = (
                summary.strategy_base_metrics,
                summary.benchmark_base_metrics,
                summary.strategy_stressed_metrics,
                summary.benchmark_stressed_metrics,
            )
            if actual != expected:
                raise DeterministicBaselineError(
                    "fold summary metrics differ from evaluation evidence"
                )
        self.strategy_batch.verify_content_identity()
        self.benchmark_batch.verify_content_identity()
        self.comparison.verify_content_identity()
        if self.strategy_batch.role is not GeneratedIntentRole.STRATEGY:
            raise DeterministicBaselineError("strategy batch has the wrong role")
        if self.benchmark_batch.role is not GeneratedIntentRole.BENCHMARK:
            raise DeterministicBaselineError("benchmark batch has the wrong role")
        if self.comparison.strategy_id != self.strategy_batch.generator_id:
            raise DeterministicBaselineError("comparison strategy differs from its batch")
        if self.comparison.benchmark_id != self.benchmark_batch.generator_id:
            raise DeterministicBaselineError("comparison benchmark differs from its batch")
        if (
            self.strategy_batch.split_plan_id != self.comparison.strategy_base.split_plan_id
            or self.benchmark_batch.split_plan_id
            != self.comparison.benchmark_base.split_plan_id
        ):
            raise DeterministicBaselineError("comparison split differs from generated batches")
        object.__setattr__(self, "run_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "deterministic-comparison-run/v1",
                "strategy_batch_id": self.strategy_batch.batch_id,
                "benchmark_batch_id": self.benchmark_batch.batch_id,
                "comparison_id": self.comparison.comparison_id,
                "fold_summary_ids": tuple(item.summary_id for item in self.fold_summaries),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self.strategy_batch.verify_content_identity()
        self.benchmark_batch.verify_content_identity()
        self.comparison.verify_content_identity()
        for summary in self.fold_summaries:
            summary.verify_content_identity()
        if self.run_id != self._calculated_id():
            raise DeterministicBaselineError("deterministic run content identity failed")


def _validate_inputs(
    *,
    split_plan: PurgedWalkForwardPlan,
    dataset: EvaluationDataset,
    instruments: tuple[PointInTimeInstrument, ...],
    initial_capital: Decimal,
    maximum_holding_sessions: int,
) -> dict[tuple[date, str], SimulationBar]:
    if type(split_plan) is not PurgedWalkForwardPlan:
        raise TypeError("split_plan must be exact")
    split_plan.verify_content_identity()
    if type(dataset) is not EvaluationDataset:
        raise TypeError("dataset must be exact")
    dataset.verify_content_identity()
    _positive_decimal(initial_capital, "initial_capital")
    if dataset.sessions != split_plan.ordered_sessions:
        raise DeterministicBaselineError("dataset calendar differs from split plan")
    if (
        type(instruments) is not tuple
        or not instruments
        or instruments != tuple(sorted(instruments, key=lambda item: item.symbol))
    ):
        raise DeterministicBaselineError("instruments must be a non-empty symbol-ordered tuple")
    symbols: set[str] = set()
    isins: set[str] = set()
    for instrument in instruments:
        if type(instrument) is not PointInTimeInstrument:
            raise DeterministicBaselineError("instruments must contain exact values")
        instrument.verify_content_identity()
        if instrument.symbol in symbols or instrument.isin in isins:
            raise DeterministicBaselineError("instrument symbols and ISINs must be unique")
        symbols.add(instrument.symbol)
        isins.add(instrument.isin)
        if instrument.universe_snapshot_id not in dataset.universe_snapshot_ids:
            raise DeterministicBaselineError("instrument universe is absent from dataset")
        if any(
            snapshot_id not in dataset.universe_snapshot_ids
            for _, snapshot_id in instrument.eligibility_bindings
        ):
            raise DeterministicBaselineError(
                "instrument eligibility binding is absent from dataset"
            )
        if any(session not in dataset.sessions for session in instrument.eligible_sessions):
            raise DeterministicBaselineError("instrument eligibility exceeds dataset calendar")
    for fold in split_plan.folds:
        if len(fold.test_sessions) < maximum_holding_sessions + 1:
            raise DeterministicBaselineError(
                "test fold cannot contain the registered next-session holding horizon"
            )
    return {(bar.session, bar.symbol): bar for bar in dataset.bars}


def _on_tick_floor(value: Decimal, tick_size: Decimal) -> Decimal:
    return (value / tick_size).to_integral_value(rounding=ROUND_FLOOR) * tick_size


def _on_tick_ceiling(value: Decimal, tick_size: Decimal) -> Decimal:
    return (value / tick_size).to_integral_value(rounding=ROUND_CEILING) * tick_size


def _fold_metrics(
    *,
    result: object,
    batch: GeneratedIntentBatch,
    fold_sessions: tuple[date, ...],
) -> tuple[tuple[str, Decimal], ...]:
    from .engine import TrialEvaluationResult

    if type(result) is not TrialEvaluationResult:
        raise DeterministicBaselineError("fold metrics require an exact evaluation result")
    points = tuple(point for point in result.equity_curve if point.session in fold_sessions)
    if tuple(point.session for point in points) != fold_sessions:
        raise DeterministicBaselineError("evaluation curve does not cover every fold session")
    intent_ids = {
        intent.intent_id
        for intent in batch.intents
        if intent.entry_order.signal_session in fold_sessions
    }
    trades = tuple(trade for trade in result.trades if trade.intent_id in intent_ids)
    start_equity = points[0].equity
    if start_equity <= ZERO:
        raise DeterministicBaselineError("fold start equity must be positive")
    final_equity = points[-1].equity
    net_profit = final_equity - start_equity
    net_return = net_profit / start_equity
    elapsed_days = Decimal(max((points[-1].session - points[0].session).days, 1))
    if final_equity <= ZERO:
        net_cagr = Decimal("-1")
    else:
        with localcontext() as context:
            context.prec = 28
            net_cagr = (
                ((final_equity / start_equity).ln() * Decimal(365) / elapsed_days).exp()
                - ONE
            )
    peak = start_equity
    max_drawdown = ZERO
    for point in points:
        peak = max(peak, point.equity)
        drawdown = point.equity / peak - ONE
        max_drawdown = min(max_drawdown, drawdown)
    turnover_value = sum(
        (
            trade.entry_fill.fill_price * trade.entry_fill.quantity
            + trade.exit_fill.fill_price * trade.exit_fill.quantity
            for trade in trades
        ),
        ZERO,
    )
    values = {
        "max_drawdown": max_drawdown,
        "net_cagr": net_cagr,
        "net_profit": net_profit,
        "net_return": net_return,
        "trade_count": Decimal(len(trades)),
        "turnover": turnover_value / start_equity,
    }
    return tuple(sorted(values.items()))


def _fold_summaries(
    *,
    split_plan: PurgedWalkForwardPlan,
    strategy_batch: GeneratedIntentBatch,
    benchmark_batch: GeneratedIntentBatch,
    comparison: TrialEvaluationComparisonResult,
) -> tuple[FoldComparisonSummary, ...]:
    summaries = []
    for fold in split_plan.folds:
        summaries.append(
            FoldComparisonSummary(
                fold_id=fold.fold_id,
                first_session=fold.test_sessions[0],
                last_session=fold.test_sessions[-1],
                primary_metric=comparison.primary_metric,
                strategy_base_metrics=_fold_metrics(
                    result=comparison.strategy_base,
                    batch=strategy_batch,
                    fold_sessions=fold.test_sessions,
                ),
                benchmark_base_metrics=_fold_metrics(
                    result=comparison.benchmark_base,
                    batch=benchmark_batch,
                    fold_sessions=fold.test_sessions,
                ),
                strategy_stressed_metrics=(
                    None
                    if comparison.strategy_stressed is None
                    else _fold_metrics(
                        result=comparison.strategy_stressed,
                        batch=strategy_batch,
                        fold_sessions=fold.test_sessions,
                    )
                ),
                benchmark_stressed_metrics=(
                    None
                    if comparison.benchmark_stressed is None
                    else _fold_metrics(
                        result=comparison.benchmark_stressed,
                        batch=benchmark_batch,
                        fold_sessions=fold.test_sessions,
                    )
                ),
            )
        )
    return tuple(summaries)


def _intent(
    *,
    decision: GeneratedSignalDecision,
    instrument: PointInTimeInstrument,
    signal_bar: SimulationBar,
    entry_session: date,
    slot_notional: Decimal,
    stop_loss_fraction: Decimal,
    target_gain_fraction: Decimal,
    maximum_holding_sessions: int,
    maximum_participation: Decimal,
) -> EvaluationTradeIntent:
    limit_price = _on_tick_floor(signal_bar.close, instrument.tick_size)
    if limit_price <= ZERO:
        raise DeterministicBaselineError("signal close cannot produce a positive limit")
    quantity = int(slot_notional / limit_price)
    if quantity <= 0:
        raise DeterministicBaselineError("selected instrument is unaffordable")
    stop_price = _on_tick_floor(
        limit_price * (ONE - stop_loss_fraction), instrument.tick_size
    )
    target_price = _on_tick_ceiling(
        limit_price * (ONE + target_gain_fraction), instrument.tick_size
    )
    if stop_price <= ZERO or not stop_price < limit_price < target_price:
        raise DeterministicBaselineError("configured exits are invalid at the signal price")
    return EvaluationTradeIntent(
        signal_id=decision.decision_id,
        universe_snapshot_id=instrument.universe_snapshot_id,
        isin=instrument.isin,
        entry_order=LimitEntryOrder(
            symbol=instrument.symbol,
            signal_session=decision.signal_session,
            first_eligible_session=entry_session,
            expiry_session=entry_session,
            quantity=quantity,
            limit_price=limit_price,
            tick_size=instrument.tick_size,
            maximum_participation=maximum_participation,
        ),
        stop_price=stop_price,
        target_price=target_price,
        max_holding_sessions=maximum_holding_sessions,
    )


class DeterministicMomentumIntentGenerator:
    def generate(
        self,
        *,
        config: MomentumBaselineConfig,
        split_plan: PurgedWalkForwardPlan,
        dataset: EvaluationDataset,
        instruments: tuple[PointInTimeInstrument, ...],
        execution_policy: DailyExecutionPolicy,
        initial_capital: Decimal,
    ) -> GeneratedIntentBatch:
        if type(config) is not MomentumBaselineConfig:
            raise TypeError("config must be exact")
        config.verify_content_identity()
        if type(execution_policy) is not DailyExecutionPolicy:
            raise TypeError("execution_policy must be exact")
        execution_policy.verify_content_identity()
        if config.maximum_holding_sessions > split_plan.label_horizon_sessions:
            raise DeterministicBaselineError("strategy holding period exceeds label horizon")
        bars = _validate_inputs(
            split_plan=split_plan,
            dataset=dataset,
            instruments=instruments,
            initial_capital=initial_capital,
            maximum_holding_sessions=config.maximum_holding_sessions,
        )
        session_positions = {session: index for index, session in enumerate(dataset.sessions)}
        instrument_by_id = {value.instrument_id: value for value in instruments}
        slot_notional = (
            initial_capital * config.gross_exposure_fraction / config.maximum_positions
        )
        decisions: list[GeneratedSignalDecision] = []
        intents: list[EvaluationTradeIntent] = []
        for fold in split_plan.folds:
            signal_session = fold.test_sessions[0]
            entry_session = fold.test_sessions[1]
            signal_index = session_positions[signal_session]
            lookback_index = signal_index - config.lookback_sessions
            candidates: list[tuple[Decimal, Decimal, PointInTimeInstrument, tuple[str, ...]]] = []
            reasons: dict[str, tuple[str, Decimal | None, tuple[str, ...]]] = {}
            for instrument in instruments:
                if signal_session not in instrument.eligible_sessions:
                    reasons[instrument.instrument_id] = ("NOT_POINT_IN_TIME_ELIGIBLE", None, ())
                    continue
                if lookback_index < 0:
                    reasons[instrument.instrument_id] = ("INSUFFICIENT_LOOKBACK", None, ())
                    continue
                start_bar = bars.get((dataset.sessions[lookback_index], instrument.symbol))
                signal_bar = bars.get((signal_session, instrument.symbol))
                if start_bar is None or signal_bar is None:
                    evidence = tuple(
                        value.bar_id for value in (start_bar, signal_bar) if value is not None
                    )
                    reasons[instrument.instrument_id] = ("MISSING_AS_OF_BAR", None, evidence)
                    continue
                evidence = (start_bar.bar_id, signal_bar.bar_id)
                momentum = signal_bar.close / start_bar.close - ONE
                turnover = signal_bar.close * signal_bar.volume
                if not signal_bar.tradable or signal_bar.volume <= 0:
                    reasons[instrument.instrument_id] = (
                        "NOT_TRADABLE_AT_SIGNAL_CLOSE",
                        momentum,
                        evidence,
                    )
                    continue
                if momentum < config.minimum_momentum:
                    reasons[instrument.instrument_id] = (
                        "BELOW_MINIMUM_MOMENTUM",
                        momentum,
                        evidence,
                    )
                    continue
                candidates.append((momentum, turnover, instrument, evidence))
            candidates.sort(key=lambda value: (-value[0], -value[1], value[2].symbol))
            selected: set[str] = set()
            for momentum, _, instrument, evidence in candidates:
                signal_bar = bars[(signal_session, instrument.symbol)]
                limit_price = _on_tick_floor(signal_bar.close, instrument.tick_size)
                if limit_price <= ZERO or int(slot_notional / limit_price) <= 0:
                    reasons[instrument.instrument_id] = (
                        "INSUFFICIENT_SLOT_NOTIONAL",
                        momentum,
                        evidence,
                    )
                    continue
                if len(selected) >= config.maximum_positions:
                    reasons[instrument.instrument_id] = (
                        "OUTSIDE_POSITION_RANK",
                        momentum,
                        evidence,
                    )
                    continue
                selected.add(instrument.instrument_id)
                reasons[instrument.instrument_id] = ("SELECTED", momentum, evidence)
            fold_decisions: list[GeneratedSignalDecision] = []
            for instrument in instruments:
                reason, score, evidence = reasons[instrument.instrument_id]
                decision = GeneratedSignalDecision(
                    generator_id=config.strategy_id,
                    role=GeneratedIntentRole.STRATEGY,
                    fold_id=fold.fold_id,
                    signal_session=signal_session,
                    instrument_id=instrument.instrument_id,
                    symbol=instrument.symbol,
                    score_name="CLOSE_MOMENTUM",
                    score=score,
                    selected=reason == "SELECTED",
                    reason=reason,
                    evidence_bar_ids=evidence,
                )
                fold_decisions.append(decision)
                decisions.append(decision)
            for decision in fold_decisions:
                if not decision.selected:
                    continue
                instrument = instrument_by_id[decision.instrument_id]
                intents.append(
                    _intent(
                        decision=decision,
                        instrument=instrument,
                        signal_bar=bars[(signal_session, instrument.symbol)],
                        entry_session=entry_session,
                        slot_notional=slot_notional,
                        stop_loss_fraction=config.stop_loss_fraction,
                        target_gain_fraction=config.target_gain_fraction,
                        maximum_holding_sessions=config.maximum_holding_sessions,
                        maximum_participation=execution_policy.maximum_participation,
                    )
                )
        return GeneratedIntentBatch(
            generator_id=config.strategy_id,
            role=GeneratedIntentRole.STRATEGY,
            split_plan_id=split_plan.plan_id,
            source_snapshot_ids=dataset.source_snapshot_ids,
            decisions=tuple(sorted(decisions, key=lambda item: (item.signal_session, item.symbol))),
            intents=tuple(
                sorted(
                    intents,
                    key=lambda item: (item.entry_order.signal_session, item.intent_id),
                )
            ),
        )


class DeterministicEqualWeightBenchmarkGenerator:
    def generate(
        self,
        *,
        config: EqualWeightBenchmarkConfig,
        split_plan: PurgedWalkForwardPlan,
        dataset: EvaluationDataset,
        instruments: tuple[PointInTimeInstrument, ...],
        execution_policy: DailyExecutionPolicy,
        initial_capital: Decimal,
    ) -> GeneratedIntentBatch:
        if type(config) is not EqualWeightBenchmarkConfig:
            raise TypeError("config must be exact")
        config.verify_content_identity()
        if type(execution_policy) is not DailyExecutionPolicy:
            raise TypeError("execution_policy must be exact")
        execution_policy.verify_content_identity()
        if config.maximum_holding_sessions > split_plan.label_horizon_sessions:
            raise DeterministicBaselineError("benchmark holding period exceeds label horizon")
        bars = _validate_inputs(
            split_plan=split_plan,
            dataset=dataset,
            instruments=instruments,
            initial_capital=initial_capital,
            maximum_holding_sessions=config.maximum_holding_sessions,
        )
        instrument_by_id = {value.instrument_id: value for value in instruments}
        slot_notional = (
            initial_capital * config.gross_exposure_fraction / config.maximum_constituents
        )
        decisions: list[GeneratedSignalDecision] = []
        intents: list[EvaluationTradeIntent] = []
        for fold in split_plan.folds:
            signal_session = fold.test_sessions[0]
            entry_session = fold.test_sessions[1]
            candidates: list[tuple[Decimal, PointInTimeInstrument, tuple[str, ...]]] = []
            reasons: dict[str, tuple[str, Decimal | None, tuple[str, ...]]] = {}
            for instrument in instruments:
                if signal_session not in instrument.eligible_sessions:
                    reasons[instrument.instrument_id] = ("NOT_POINT_IN_TIME_ELIGIBLE", None, ())
                    continue
                signal_bar = bars.get((signal_session, instrument.symbol))
                if signal_bar is None:
                    reasons[instrument.instrument_id] = ("MISSING_AS_OF_BAR", None, ())
                    continue
                turnover = signal_bar.close * signal_bar.volume
                evidence = (signal_bar.bar_id,)
                if not signal_bar.tradable or signal_bar.volume <= 0:
                    reasons[instrument.instrument_id] = (
                        "NOT_TRADABLE_AT_SIGNAL_CLOSE",
                        turnover,
                        evidence,
                    )
                    continue
                limit_price = _on_tick_floor(signal_bar.close, instrument.tick_size)
                if limit_price <= ZERO or int(slot_notional / limit_price) <= 0:
                    reasons[instrument.instrument_id] = (
                        "INSUFFICIENT_SLOT_NOTIONAL",
                        turnover,
                        evidence,
                    )
                    continue
                candidates.append((turnover, instrument, evidence))
            candidates.sort(key=lambda value: (-value[0], value[1].symbol))
            selected_ids = {
                instrument.instrument_id
                for _, instrument, _ in candidates[: config.maximum_constituents]
            }
            for turnover, instrument, evidence in candidates:
                reasons[instrument.instrument_id] = (
                    "SELECTED" if instrument.instrument_id in selected_ids else "OUTSIDE_CONSTITUENT_RANK",
                    turnover,
                    evidence,
                )
            fold_decisions: list[GeneratedSignalDecision] = []
            for instrument in instruments:
                reason, score, evidence = reasons[instrument.instrument_id]
                decision = GeneratedSignalDecision(
                    generator_id=config.benchmark_id,
                    role=GeneratedIntentRole.BENCHMARK,
                    fold_id=fold.fold_id,
                    signal_session=signal_session,
                    instrument_id=instrument.instrument_id,
                    symbol=instrument.symbol,
                    score_name="SIGNAL_TURNOVER",
                    score=score,
                    selected=reason == "SELECTED",
                    reason=reason,
                    evidence_bar_ids=evidence,
                )
                fold_decisions.append(decision)
                decisions.append(decision)
            for decision in fold_decisions:
                if not decision.selected:
                    continue
                instrument = instrument_by_id[decision.instrument_id]
                intents.append(
                    _intent(
                        decision=decision,
                        instrument=instrument,
                        signal_bar=bars[(signal_session, instrument.symbol)],
                        entry_session=entry_session,
                        slot_notional=slot_notional,
                        stop_loss_fraction=config.stop_loss_fraction,
                        target_gain_fraction=config.target_gain_fraction,
                        maximum_holding_sessions=config.maximum_holding_sessions,
                        maximum_participation=execution_policy.maximum_participation,
                    )
                )
        return GeneratedIntentBatch(
            generator_id=config.benchmark_id,
            role=GeneratedIntentRole.BENCHMARK,
            split_plan_id=split_plan.plan_id,
            source_snapshot_ids=dataset.source_snapshot_ids,
            decisions=tuple(sorted(decisions, key=lambda item: (item.signal_session, item.symbol))),
            intents=tuple(
                sorted(
                    intents,
                    key=lambda item: (item.entry_order.signal_session, item.intent_id),
                )
            ),
        )


class DeterministicBaselineEvaluationEngine:
    def evaluate(
        self,
        *,
        registration: TrialRegistration,
        strategy_config: MomentumBaselineConfig,
        benchmark_config: EqualWeightBenchmarkConfig,
        split_plan: PurgedWalkForwardPlan,
        dataset: EvaluationDataset,
        instruments: tuple[PointInTimeInstrument, ...],
        execution_policy: DailyExecutionPolicy,
        cost_schedule: NseDeliveryCostSchedule,
        initial_capital: Decimal,
    ) -> DeterministicComparisonRun:
        if type(registration) is not TrialRegistration:
            raise TypeError("registration must be exact")
        registration.verify_content_identity()
        if type(strategy_config) is not MomentumBaselineConfig:
            raise TypeError("strategy_config must be exact")
        if type(benchmark_config) is not EqualWeightBenchmarkConfig:
            raise TypeError("benchmark_config must be exact")
        strategy_config.verify_content_identity()
        benchmark_config.verify_content_identity()
        if registration.model_bundle_id != strategy_config.strategy_id:
            raise DeterministicBaselineError("registration does not bind the strategy generator")
        if registration.benchmark_id != benchmark_config.benchmark_id:
            raise DeterministicBaselineError("registration does not bind the benchmark generator")
        strategy_batch = DeterministicMomentumIntentGenerator().generate(
            config=strategy_config,
            split_plan=split_plan,
            dataset=dataset,
            instruments=instruments,
            execution_policy=execution_policy,
            initial_capital=initial_capital,
        )
        benchmark_batch = DeterministicEqualWeightBenchmarkGenerator().generate(
            config=benchmark_config,
            split_plan=split_plan,
            dataset=dataset,
            instruments=instruments,
            execution_policy=execution_policy,
            initial_capital=initial_capital,
        )
        comparison = TrialEvaluationComparisonEngine().evaluate(
            registration=registration,
            split_plan=split_plan,
            dataset=dataset,
            strategy_intents=strategy_batch.intents,
            benchmark_intents=benchmark_batch.intents,
            execution_policy=execution_policy,
            cost_schedule=cost_schedule,
            initial_capital=initial_capital,
        )
        return DeterministicComparisonRun(
            strategy_batch=strategy_batch,
            benchmark_batch=benchmark_batch,
            comparison=comparison,
            fold_summaries=_fold_summaries(
                split_plan=split_plan,
                strategy_batch=strategy_batch,
                benchmark_batch=benchmark_batch,
                comparison=comparison,
            ),
        )
