"""Walk-forward evaluation orchestration for promoted research intents."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Protocol

from india_swing.evaluation.baselines import (
    DeterministicComparisonRun,
    DeterministicEqualWeightBenchmarkGenerator,
    EqualWeightBenchmarkConfig,
    GeneratedIntentBatch,
    GeneratedIntentRole,
    GeneratedSignalDecision,
    PointInTimeInstrument,
    build_fold_comparison_summaries,
)
from india_swing.evaluation.engine import (
    DailyExecutionPolicy,
    EvaluationDataset,
    EvaluationTradeIntent,
    TrialEvaluationComparisonEngine,
)
from india_swing.evaluation.models import PurgedWalkForwardPlan
from india_swing.evaluation.promoted_intent_store import (
    LocalPromotedResearchIntentStore,
)
from india_swing.evaluation.promoted_intents import (
    PromotedIntentPolicyConfig,
    PromotedResearchIntentService,
    VerifiedPromotedResearchIntentBatch,
)
from india_swing.evaluation.trials import TrialRegistration
from india_swing.execution.costs import NseDeliveryCostSchedule
from india_swing.execution.simulator import LimitEntryOrder
from india_swing.features.promoted_cross_section import (
    PromotedCrossSectionResult,
    VerifiedPromotedCrossSectionPanel,
)
from india_swing.identity import content_id


class PromotedWalkForwardError(ValueError):
    pass


PROMOTED_WALK_FORWARD_POLICY_VERSION = "promoted-walk-forward/v1"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_ZERO = Decimal("0")


class ExactCrossSectionResolver(Protocol):
    def get(
        self,
        panel_id: str,
    ) -> VerifiedPromotedCrossSectionPanel: ...


@dataclass(frozen=True, slots=True)
class PromotedFoldCrossSectionBinding:
    fold_id: str
    signal_session: date
    cross_section_panel_id: str
    binding_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            type(self.fold_id) is not str
            or _SHA256.fullmatch(self.fold_id) is None
            or type(self.signal_session) is not date
            or type(self.cross_section_panel_id) is not str
            or _SHA256.fullmatch(self.cross_section_panel_id) is None
        ):
            raise PromotedWalkForwardError(
                "promoted fold binding is invalid"
            )
        object.__setattr__(
            self,
            "binding_id",
            content_id(
                {
                    "schema": "promoted-fold-cross-section-binding/v1",
                    "fold_id": self.fold_id,
                    "signal_session": self.signal_session,
                    "cross_section_panel_id": (
                        self.cross_section_panel_id
                    ),
                },
                length=64,
            ),
        )

    def verify_content_identity(self) -> None:
        expected = PromotedFoldCrossSectionBinding(
            fold_id=self.fold_id,
            signal_session=self.signal_session,
            cross_section_panel_id=self.cross_section_panel_id,
        )
        if self.binding_id != expected.binding_id:
            raise PromotedWalkForwardError(
                "promoted fold binding identity failed"
            )


def _panel_signal_session(
    panel: VerifiedPromotedCrossSectionPanel,
) -> date:
    return (
        panel.source_panel.source_panel.adjustment_panel.signal_session
    )


def _result_symbol(result: PromotedCrossSectionResult) -> str:
    history = (
        result.source_result.source_result.source_adjustment_result
        .source_history
    )
    for observation in reversed(history.observations):
        if observation.tick_entry is not None:
            symbol = (
                observation.tick_entry.frame_entry.universe_entry.symbol
            )
            if (
                isinstance(symbol, str)
                and symbol
                and symbol == symbol.strip().upper()
            ):
                return symbol
    raise PromotedWalkForwardError(
        "promoted decision symbol could not be resolved"
    )


def _rebind_intent(
    intent: EvaluationTradeIntent,
    signal_id: str,
) -> EvaluationTradeIntent:
    order = intent.entry_order
    return EvaluationTradeIntent(
        signal_id=signal_id,
        universe_snapshot_id=intent.universe_snapshot_id,
        isin=intent.isin,
        entry_order=LimitEntryOrder(
            symbol=order.symbol,
            signal_session=order.signal_session,
            first_eligible_session=order.first_eligible_session,
            expiry_session=order.expiry_session,
            quantity=order.quantity,
            limit_price=order.limit_price,
            tick_size=order.tick_size,
            maximum_participation=order.maximum_participation,
        ),
        stop_price=intent.stop_price,
        target_price=intent.target_price,
        max_holding_sessions=intent.max_holding_sessions,
    )


@dataclass(frozen=True, slots=True)
class PromotedWalkForwardStrategyRun:
    policy_version: str
    split_plan_id: str
    config_id: str
    bindings: tuple[PromotedFoldCrossSectionBinding, ...]
    research_batches: tuple[
        VerifiedPromotedResearchIntentBatch,
        ...,
    ]
    strategy_batch: GeneratedIntentBatch
    run_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            self.policy_version != PROMOTED_WALK_FORWARD_POLICY_VERSION
            or _SHA256.fullmatch(self.split_plan_id) is None
            or _SHA256.fullmatch(self.config_id) is None
            or type(self.bindings) is not tuple
            or not self.bindings
            or any(
                type(value) is not PromotedFoldCrossSectionBinding
                for value in self.bindings
            )
            or len({value.binding_id for value in self.bindings})
            != len(self.bindings)
            or len({value.fold_id for value in self.bindings})
            != len(self.bindings)
            or len({value.signal_session for value in self.bindings})
            != len(self.bindings)
            or type(self.research_batches) is not tuple
            or len(self.research_batches) != len(self.bindings)
            or any(
                type(value) is not VerifiedPromotedResearchIntentBatch
                for value in self.research_batches
            )
            or len({value.batch_id for value in self.research_batches})
            != len(self.research_batches)
            or any(
                value.config_id != self.config_id
                for value in self.research_batches
            )
            or type(self.strategy_batch) is not GeneratedIntentBatch
            or self.strategy_batch.role is not GeneratedIntentRole.STRATEGY
            or self.strategy_batch.generator_id != self.config_id
            or self.strategy_batch.split_plan_id != self.split_plan_id
        ):
            raise PromotedWalkForwardError(
                "promoted strategy run graph is invalid"
            )
        for binding, batch in zip(
            self.bindings,
            self.research_batches,
        ):
            binding.verify_content_identity()
            batch.verify_content_identity()
            if (
                binding.cross_section_panel_id
                != batch.source_panel_id
                or binding.signal_session != batch.signal_session
            ):
                raise PromotedWalkForwardError(
                    "promoted strategy run binding differs"
                )
        self.strategy_batch.verify_content_identity()
        research_selected = sum(
            value.selected_count for value in self.research_batches
        )
        if (
            research_selected != len(self.strategy_batch.intents)
            or {
                value.signal_session
                for value in self.strategy_batch.decisions
            }
            != {value.signal_session for value in self.bindings}
        ):
            raise PromotedWalkForwardError(
                "promoted strategy decisions differ from research batches"
            )
        object.__setattr__(self, "run_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": PROMOTED_WALK_FORWARD_POLICY_VERSION,
                "split_plan_id": self.split_plan_id,
                "config_id": self.config_id,
                "binding_ids": tuple(
                    value.binding_id for value in self.bindings
                ),
                "research_batch_ids": tuple(
                    value.batch_id for value in self.research_batches
                ),
                "strategy_batch_id": self.strategy_batch.batch_id,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.run_id != self._calculated_id():
            raise PromotedWalkForwardError(
                "promoted strategy run identity failed"
            )
        for value in self.bindings:
            value.verify_content_identity()
        for value in self.research_batches:
            value.verify_content_identity()
        self.strategy_batch.verify_content_identity()


class PromotedWalkForwardStrategyGenerator:
    """Generates one exact research decision batch per walk-forward fold."""

    def generate(
        self,
        *,
        config: PromotedIntentPolicyConfig,
        split_plan: PurgedWalkForwardPlan,
        dataset: EvaluationDataset,
        bindings: tuple[PromotedFoldCrossSectionBinding, ...],
        cross_section_resolver: ExactCrossSectionResolver,
        intent_store: LocalPromotedResearchIntentStore,
        initial_capital: Decimal,
    ) -> PromotedWalkForwardStrategyRun:
        if (
            type(config) is not PromotedIntentPolicyConfig
            or type(split_plan) is not PurgedWalkForwardPlan
            or type(dataset) is not EvaluationDataset
            or type(bindings) is not tuple
            or type(intent_store) is not LocalPromotedResearchIntentStore
            or type(initial_capital) is not Decimal
            or not initial_capital.is_finite()
            or initial_capital <= _ZERO
            or not callable(getattr(cross_section_resolver, "get", None))
        ):
            raise PromotedWalkForwardError(
                "promoted strategy generation input is invalid"
            )
        try:
            config.verify_content_identity()
            split_plan.verify_content_identity()
            dataset.verify_content_identity()
        except Exception:
            raise PromotedWalkForwardError(
                "promoted strategy generation source failed verification"
            ) from None
        if config.maximum_holding_sessions > (
            split_plan.label_horizon_sessions
        ):
            raise PromotedWalkForwardError(
                "promoted holding period exceeds label horizon"
            )
        if (
            len(bindings) != len(split_plan.folds)
            or any(
                type(value) is not PromotedFoldCrossSectionBinding
                for value in bindings
            )
        ):
            raise PromotedWalkForwardError(
                "promoted fold bindings are incomplete"
            )

        research_batches: list[
            VerifiedPromotedResearchIntentBatch
        ] = []
        generated_decisions: list[GeneratedSignalDecision] = []
        generated_intents: list[EvaluationTradeIntent] = []
        for fold, binding in zip(split_plan.folds, bindings):
            binding.verify_content_identity()
            if (
                len(fold.test_sessions) < 2
                or binding.fold_id != fold.fold_id
                or binding.signal_session != fold.test_sessions[0]
            ):
                raise PromotedWalkForwardError(
                    "promoted fold binding does not select test start"
                )
            entry_session = fold.test_sessions[1]
            if (
                binding.signal_session not in dataset.sessions
                or entry_session not in dataset.sessions
            ):
                raise PromotedWalkForwardError(
                    "promoted fold sessions are absent from dataset"
                )
            try:
                panel = cross_section_resolver.get(
                    binding.cross_section_panel_id
                )
            except Exception:
                raise PromotedWalkForwardError(
                    "promoted cross-section could not be resolved"
                ) from None
            if (
                type(panel) is not VerifiedPromotedCrossSectionPanel
                or panel.panel_id != binding.cross_section_panel_id
                or _panel_signal_session(panel)
                != binding.signal_session
            ):
                raise PromotedWalkForwardError(
                    "promoted cross-section differs from fold binding"
                )
            research_batch = PromotedResearchIntentService().generate(
                source_panel=panel,
                config=config,
                entry_session=entry_session,
                initial_capital=initial_capital,
            )
            persisted = intent_store.put(research_batch, config)
            if persisted != research_batch:
                raise PromotedWalkForwardError(
                    "persisted promoted research batch differs"
                )
            research_batches.append(research_batch)
            result_by_id = {
                value.result_id: value for value in panel.results
            }
            research_intent_by_decision = {
                value.decision_id: value
                for value in research_batch.intents
            }
            for decision in research_batch.decisions:
                result = result_by_id.get(decision.source_result_id)
                if result is None:
                    raise PromotedWalkForwardError(
                        "promoted decision source result is absent"
                    )
                source_history = (
                    result.source_result.source_result
                    .source_adjustment_result.source_history
                )
                vector = result.source_result.feature_vector
                generated = GeneratedSignalDecision(
                    generator_id=config.config_id,
                    role=GeneratedIntentRole.STRATEGY,
                    fold_id=fold.fold_id,
                    signal_session=binding.signal_session,
                    instrument_id=source_history.stable_instrument_id,
                    symbol=_result_symbol(result),
                    score_name="PROMOTED_ENSEMBLE_SCORE",
                    score=decision.ensemble_score,
                    selected=decision.selected,
                    reason=(
                        "SELECTED"
                        if decision.selected
                        else decision.status.value
                    ),
                    evidence_bar_ids=(
                        ()
                        if vector is None
                        else vector.input_bar_ids
                    ),
                )
                generated_decisions.append(generated)
                if decision.selected:
                    research_intent = research_intent_by_decision.get(
                        decision.decision_id
                    )
                    if research_intent is None:
                        raise PromotedWalkForwardError(
                            "selected research intent is absent"
                        )
                    generated_intents.append(
                        _rebind_intent(
                            research_intent.evaluation_intent,
                            generated.decision_id,
                        )
                    )
        strategy_batch = GeneratedIntentBatch(
            generator_id=config.config_id,
            role=GeneratedIntentRole.STRATEGY,
            split_plan_id=split_plan.plan_id,
            source_snapshot_ids=dataset.source_snapshot_ids,
            decisions=tuple(
                sorted(
                    generated_decisions,
                    key=lambda value: (
                        value.signal_session,
                        value.symbol,
                    ),
                )
            ),
            intents=tuple(
                sorted(
                    generated_intents,
                    key=lambda value: (
                        value.entry_order.signal_session,
                        value.intent_id,
                    ),
                )
            ),
        )
        return PromotedWalkForwardStrategyRun(
            policy_version=PROMOTED_WALK_FORWARD_POLICY_VERSION,
            split_plan_id=split_plan.plan_id,
            config_id=config.config_id,
            bindings=bindings,
            research_batches=tuple(research_batches),
            strategy_batch=strategy_batch,
        )


@dataclass(frozen=True, slots=True)
class PromotedWalkForwardEvaluationRun:
    strategy_run: PromotedWalkForwardStrategyRun
    deterministic_run: DeterministicComparisonRun
    run_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            type(self.strategy_run) is not PromotedWalkForwardStrategyRun
            or type(self.deterministic_run) is not DeterministicComparisonRun
        ):
            raise PromotedWalkForwardError(
                "promoted evaluation run graph is invalid"
            )
        self.strategy_run.verify_content_identity()
        self.deterministic_run.verify_content_identity()
        if (
            self.deterministic_run.strategy_batch
            != self.strategy_run.strategy_batch
        ):
            raise PromotedWalkForwardError(
                "promoted evaluation strategy batch differs"
            )
        object.__setattr__(self, "run_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "promoted-walk-forward-evaluation-run/v1",
                "strategy_run_id": self.strategy_run.run_id,
                "deterministic_run_id": self.deterministic_run.run_id,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self.strategy_run.verify_content_identity()
        self.deterministic_run.verify_content_identity()
        if self.run_id != self._calculated_id():
            raise PromotedWalkForwardError(
                "promoted evaluation run identity failed"
            )


class PromotedWalkForwardEvaluationEngine:
    """Runs promoted strategy and equal-weight benchmark under one simulator."""

    def evaluate(
        self,
        *,
        registration: TrialRegistration,
        strategy_config: PromotedIntentPolicyConfig,
        benchmark_config: EqualWeightBenchmarkConfig,
        split_plan: PurgedWalkForwardPlan,
        dataset: EvaluationDataset,
        instruments: tuple[PointInTimeInstrument, ...],
        bindings: tuple[PromotedFoldCrossSectionBinding, ...],
        cross_section_resolver: ExactCrossSectionResolver,
        intent_store: LocalPromotedResearchIntentStore,
        execution_policy: DailyExecutionPolicy,
        cost_schedule: NseDeliveryCostSchedule,
        initial_capital: Decimal,
    ) -> PromotedWalkForwardEvaluationRun:
        if type(registration) is not TrialRegistration:
            raise TypeError("registration must be exact")
        registration.verify_content_identity()
        if type(strategy_config) is not PromotedIntentPolicyConfig:
            raise TypeError("strategy_config must be exact")
        if type(benchmark_config) is not EqualWeightBenchmarkConfig:
            raise TypeError("benchmark_config must be exact")
        strategy_config.verify_content_identity()
        benchmark_config.verify_content_identity()
        if registration.model_bundle_id != strategy_config.config_id:
            raise PromotedWalkForwardError(
                "trial does not bind promoted strategy configuration"
            )
        if registration.benchmark_id != benchmark_config.benchmark_id:
            raise PromotedWalkForwardError(
                "trial does not bind equal-weight benchmark"
            )
        strategy_run = PromotedWalkForwardStrategyGenerator().generate(
            config=strategy_config,
            split_plan=split_plan,
            dataset=dataset,
            bindings=bindings,
            cross_section_resolver=cross_section_resolver,
            intent_store=intent_store,
            initial_capital=initial_capital,
        )
        benchmark_batch = (
            DeterministicEqualWeightBenchmarkGenerator().generate(
                config=benchmark_config,
                split_plan=split_plan,
                dataset=dataset,
                instruments=instruments,
                execution_policy=execution_policy,
                initial_capital=initial_capital,
            )
        )
        comparison = TrialEvaluationComparisonEngine().evaluate(
            registration=registration,
            split_plan=split_plan,
            dataset=dataset,
            strategy_intents=strategy_run.strategy_batch.intents,
            benchmark_intents=benchmark_batch.intents,
            execution_policy=execution_policy,
            cost_schedule=cost_schedule,
            initial_capital=initial_capital,
        )
        deterministic_run = DeterministicComparisonRun(
            strategy_batch=strategy_run.strategy_batch,
            benchmark_batch=benchmark_batch,
            comparison=comparison,
            fold_summaries=build_fold_comparison_summaries(
                split_plan=split_plan,
                strategy_batch=strategy_run.strategy_batch,
                benchmark_batch=benchmark_batch,
                comparison=comparison,
            ),
        )
        return PromotedWalkForwardEvaluationRun(
            strategy_run=strategy_run,
            deterministic_run=deterministic_run,
        )
