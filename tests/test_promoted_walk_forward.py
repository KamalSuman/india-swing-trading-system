from __future__ import annotations

import json
import tempfile
import unittest
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from india_swing.evaluation.baselines import (
    EqualWeightBenchmarkConfig,
    PointInTimeInstrument,
)
from india_swing.evaluation.engine import (
    DailyExecutionPolicy,
    EvaluationDataReadiness,
    EvaluationDataset,
)
from india_swing.evaluation.models import (
    PurgedWalkForwardPlan,
    WalkForwardFold,
)
from india_swing.evaluation.promoted_intent_store import (
    LocalPromotedResearchIntentStore,
    PromotedIntentStoreConflict,
    decode_promoted_intent_record,
    encode_promoted_intent_batch,
)
from india_swing.evaluation.promoted_intents import (
    PromotedIntentPolicyConfig,
    PromotedResearchIntentService,
)
from india_swing.evaluation.promoted_walk_forward import (
    PromotedFoldCrossSectionBinding,
    PromotedWalkForwardError,
    PromotedWalkForwardEvaluationEngine,
    PromotedWalkForwardStrategyGenerator,
)
from india_swing.evaluation.trials import (
    TrialRegistration,
    TrialStage,
)
from india_swing.execution import (
    SimulationBar,
    zerodha_nse_delivery_schedule_2026,
)
from india_swing.features.promoted_cross_section import (
    PromotedCrossSectionConfig,
    PromotedCrossSectionService,
)
from india_swing.identity import content_id
from tests.test_promoted_technical_features import _feature_panel


DATA_ID = "7" * 64


class _Resolver:
    def __init__(self, panels) -> None:
        self.panels = {value.panel_id: value for value in panels}
        self.calls: list[str] = []

    def get(self, panel_id: str):
        self.calls.append(panel_id)
        return self.panels[panel_id]


def _panel(root: Path, *, minimum: int = 1):
    _, _, technical = _feature_panel(root)
    return PromotedCrossSectionService().materialize(
        source_panel=technical,
        config=PromotedCrossSectionConfig(
            minimum_computed_instruments=minimum
        ),
        cutoff=technical.cutoff,
    )


def _strategy_config() -> PromotedIntentPolicyConfig:
    return PromotedIntentPolicyConfig(
        minimum_ensemble_score=Decimal("0.01"),
        minimum_median_traded_value=Decimal("1"),
        minimum_signal_traded_value_ratio=Decimal("0.01"),
        maximum_tick_fraction=Decimal("0.50"),
        minimum_average_true_range_ticks=Decimal("0.01"),
        maximum_annualized_volatility=Decimal("10"),
        maximum_zero_volume_fraction=Decimal("1"),
        maximum_holding_sessions=3,
    )


def _benchmark_config() -> EqualWeightBenchmarkConfig:
    return EqualWeightBenchmarkConfig(
        maximum_constituents=1,
        gross_exposure_fraction=Decimal("0.50"),
        stop_loss_fraction=Decimal("0.50"),
        target_gain_fraction=Decimal("0.50"),
        maximum_holding_sessions=3,
    )


def _policy() -> DailyExecutionPolicy:
    return DailyExecutionPolicy(
        slippage_bps=Decimal("10"),
        stressed_slippage_bps=Decimal("25"),
        maximum_participation=Decimal("0.0025"),
    )


def _plan(signal_session: date) -> PurgedWalkForwardPlan:
    sessions = tuple(
        signal_session - timedelta(days=45 - index)
        for index in range(50)
    )
    fold = WalkForwardFold(
        training_sessions=sessions[:20],
        validation_sessions=sessions[30:35],
        test_sessions=sessions[45:50],
    )
    return PurgedWalkForwardPlan(
        calendar_version="synthetic-promoted-walk-forward-calendar/v1",
        ordered_sessions=sessions,
        label_horizon_sessions=10,
        embargo_sessions=10,
        folds=(fold,),
    )


def _universe_id(panel) -> str:
    result = panel.results[0]
    return (
        result.source_result.source_result.source_adjustment_result
        .identity_bindings[-1].identity_snapshot_id
    )


def _dataset(plan: PurgedWalkForwardPlan, universe_id: str):
    bars = tuple(
        SimulationBar(
            session=session,
            symbol="RELIANCE",
            open=Decimal("99") + Decimal(index) / Decimal("10"),
            high=Decimal("101") + Decimal(index) / Decimal("10"),
            low=Decimal("98") + Decimal(index) / Decimal("10"),
            close=Decimal("100") + Decimal(index) / Decimal("10"),
            volume=1_000_000,
        )
        for index, session in enumerate(plan.ordered_sessions)
    )
    return EvaluationDataset(
        sessions=plan.ordered_sessions,
        bars=bars,
        source_snapshot_ids=(DATA_ID,),
        universe_snapshot_ids=(universe_id,),
        readiness=EvaluationDataReadiness.SYNTHETIC,
    )


def _instrument(plan: PurgedWalkForwardPlan, universe_id: str):
    return PointInTimeInstrument(
        symbol="RELIANCE",
        isin="INE002A01018",
        universe_snapshot_id=universe_id,
        eligible_sessions=plan.ordered_sessions,
        tick_size=Decimal("0.05"),
    )


def _registration(
    *,
    plan: PurgedWalkForwardPlan,
    dataset: EvaluationDataset,
    strategy: PromotedIntentPolicyConfig,
    benchmark: EqualWeightBenchmarkConfig,
) -> TrialRegistration:
    execution = _policy()
    costs = zerodha_nse_delivery_schedule_2026()
    return TrialRegistration(
        registered_at=datetime(2026, 7, 1, 12, tzinfo=UTC),
        stage=TrialStage.EXPLORATORY,
        hypothesis=(
            "Promoted point-in-time opportunity tiers exceed a liquid "
            "equal-weight benchmark."
        ),
        strategy_family_id="promoted-research-intent-v1",
        parent_trial_id=None,
        evaluation_start=plan.ordered_sessions[0],
        evaluation_end=plan.ordered_sessions[-1],
        universe_snapshot_ids=dataset.universe_snapshot_ids,
        data_snapshot_ids=dataset.source_snapshot_ids,
        split_plan_id=plan.plan_id,
        label_horizon_sessions=plan.label_horizon_sessions,
        benchmark_id=benchmark.benchmark_id,
        primary_metric="net_return",
        secondary_metrics=(
            "max_drawdown",
            "net_profit",
            "trade_count",
            "turnover",
        ),
        model_bundle_id=strategy.config_id,
        source_commit="60abef9",
        dependency_hash="a" * 64,
        configuration_hash=content_id(
            (strategy, benchmark),
            length=64,
        ),
        exclusions_hash="b" * 64,
        risk_policy_hash="c" * 64,
        execution_policy_version=execution.version,
        execution_policy_hash=execution.policy_id,
        cost_schedule_version=costs.policy_version,
        cost_schedule_hash=costs.schedule_id,
        base_slippage_bps=execution.slippage_bps,
        stressed_slippage_bps=execution.stressed_slippage_bps,
        pass_thresholds=(("net_return", Decimal("-1")),),
        multiple_testing_policy="single-synthetic-promoted-policy-v1",
        random_seed=1729,
        repetition_count=1,
        holdout_id=None,
        holdout_sealed=False,
        synthetic=True,
    )


def _binding(panel, plan: PurgedWalkForwardPlan):
    fold = plan.folds[0]
    return PromotedFoldCrossSectionBinding(
        fold_id=fold.fold_id,
        signal_session=fold.test_sessions[0],
        cross_section_panel_id=panel.panel_id,
    )


class PromotedIntentStoreTests(unittest.TestCase):
    def test_create_once_store_replays_exact_source_and_policy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            panel = _panel(root / "evidence")
            config = _strategy_config()
            signal = (
                panel.source_panel.source_panel.adjustment_panel
                .signal_session
            )
            batch = PromotedResearchIntentService().generate(
                source_panel=panel,
                config=config,
                entry_session=signal + timedelta(days=1),
                initial_capital=Decimal("100000"),
            )
            resolver = _Resolver((panel,))
            store = LocalPromotedResearchIntentStore(
                root / "store",
                resolver,
            )
            first = store.put(batch, config)
            second = store.put(batch, config)
            restored = store.get(batch.batch_id)
        self.assertEqual(first, batch)
        self.assertEqual(second, batch)
        self.assertEqual(restored, batch)
        self.assertEqual(len(resolver.calls), 5)
        self.assertEqual(set(resolver.calls), {panel.panel_id})

    def test_tampered_projection_fails_independent_replay(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            panel = _panel(root / "evidence")
            config = _strategy_config()
            signal = (
                panel.source_panel.source_panel.adjustment_panel
                .signal_session
            )
            batch = PromotedResearchIntentService().generate(
                source_panel=panel,
                config=config,
                entry_session=signal + timedelta(days=1),
                initial_capital=Decimal("100000"),
            )
            store = LocalPromotedResearchIntentStore(
                root / "store",
                _Resolver((panel,)),
            )
            store.put(batch, config)
            path = store.path_for(batch.batch_id)
            raw = json.loads(path.read_bytes())
            raw["batch"]["blocked_count"] = 999
            path.write_bytes(
                (
                    json.dumps(
                        raw,
                        separators=(",", ":"),
                        sort_keys=True,
                    )
                    + "\n"
                ).encode()
            )
            with self.assertRaises(PromotedIntentStoreConflict):
                store.get(batch.batch_id)

    def test_codec_rejects_duplicate_keys_and_float_tokens(self) -> None:
        with self.assertRaises(PromotedIntentStoreConflict):
            decode_promoted_intent_record(b'{"x":1,"x":2}')
        with self.assertRaises(PromotedIntentStoreConflict):
            decode_promoted_intent_record(b'{"x":1.5}')

    def test_store_exposes_no_list_or_latest_operation(self) -> None:
        public = {
            value
            for value in dir(LocalPromotedResearchIntentStore)
            if not value.startswith("_")
        }
        self.assertEqual(
            public,
            {"batches_root", "get", "path_for", "put"},
        )

    def test_encoded_manifest_contains_no_nested_source_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = _panel(Path(tmp))
            config = _strategy_config()
            signal = (
                panel.source_panel.source_panel.adjustment_panel
                .signal_session
            )
            batch = PromotedResearchIntentService().generate(
                source_panel=panel,
                config=config,
                entry_session=signal + timedelta(days=1),
                initial_capital=Decimal("100000"),
            )
            payload = encode_promoted_intent_batch(batch, config)
        self.assertNotIn(b"source_adjustment_result", payload)
        self.assertIn(panel.panel_id.encode(), payload)


class PromotedWalkForwardStrategyTests(unittest.TestCase):
    def test_exact_fold_session_generates_and_persists_strategy_batch(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            panel = _panel(root / "evidence")
            signal = (
                panel.source_panel.source_panel.adjustment_panel
                .signal_session
            )
            plan = _plan(signal)
            dataset = _dataset(plan, _universe_id(panel))
            resolver = _Resolver((panel,))
            store = LocalPromotedResearchIntentStore(
                root / "store",
                resolver,
            )
            run = PromotedWalkForwardStrategyGenerator().generate(
                config=_strategy_config(),
                split_plan=plan,
                dataset=dataset,
                bindings=(_binding(panel, plan),),
                cross_section_resolver=resolver,
                intent_store=store,
                initial_capital=Decimal("100000"),
            )
        run.verify_content_identity()
        self.assertEqual(len(run.research_batches), 1)
        self.assertEqual(
            run.research_batches[0].entry_session,
            plan.folds[0].test_sessions[1],
        )
        self.assertEqual(run.strategy_batch.intents, ())
        self.assertEqual(
            run.strategy_batch.source_snapshot_ids,
            dataset.source_snapshot_ids,
        )

    def test_binding_cannot_substitute_calendar_day_for_test_successor(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            panel = _panel(root / "evidence")
            signal = (
                panel.source_panel.source_panel.adjustment_panel
                .signal_session
            )
            plan = _plan(signal)
            dataset = _dataset(plan, _universe_id(panel))
            resolver = _Resolver((panel,))
            store = LocalPromotedResearchIntentStore(
                root / "store",
                resolver,
            )
            wrong = PromotedFoldCrossSectionBinding(
                fold_id=plan.folds[0].fold_id,
                signal_session=signal + timedelta(days=1),
                cross_section_panel_id=panel.panel_id,
            )
            with self.assertRaises(PromotedWalkForwardError):
                PromotedWalkForwardStrategyGenerator().generate(
                    config=_strategy_config(),
                    split_plan=plan,
                    dataset=dataset,
                    bindings=(wrong,),
                    cross_section_resolver=resolver,
                    intent_store=store,
                    initial_capital=Decimal("100000"),
                )

    def test_missing_fold_binding_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            panel = _panel(root / "evidence")
            signal = (
                panel.source_panel.source_panel.adjustment_panel
                .signal_session
            )
            plan = _plan(signal)
            resolver = _Resolver((panel,))
            with self.assertRaises(PromotedWalkForwardError):
                PromotedWalkForwardStrategyGenerator().generate(
                    config=_strategy_config(),
                    split_plan=plan,
                    dataset=_dataset(plan, _universe_id(panel)),
                    bindings=(),
                    cross_section_resolver=resolver,
                    intent_store=LocalPromotedResearchIntentStore(
                        root / "store",
                        resolver,
                    ),
                    initial_capital=Decimal("100000"),
                )


class PromotedWalkForwardEvaluationTests(unittest.TestCase):
    def test_runs_same_simulator_and_benchmark_with_stress_case(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            panel = _panel(root / "evidence")
            signal = (
                panel.source_panel.source_panel.adjustment_panel
                .signal_session
            )
            plan = _plan(signal)
            universe_id = _universe_id(panel)
            dataset = _dataset(plan, universe_id)
            strategy = _strategy_config()
            benchmark = _benchmark_config()
            resolver = _Resolver((panel,))
            run = PromotedWalkForwardEvaluationEngine().evaluate(
                registration=_registration(
                    plan=plan,
                    dataset=dataset,
                    strategy=strategy,
                    benchmark=benchmark,
                ),
                strategy_config=strategy,
                benchmark_config=benchmark,
                split_plan=plan,
                dataset=dataset,
                instruments=(_instrument(plan, universe_id),),
                bindings=(_binding(panel, plan),),
                cross_section_resolver=resolver,
                intent_store=LocalPromotedResearchIntentStore(
                    root / "store",
                    resolver,
                ),
                execution_policy=_policy(),
                cost_schedule=zerodha_nse_delivery_schedule_2026(),
                initial_capital=Decimal("100000"),
            )
        run.verify_content_identity()
        comparison = run.deterministic_run.comparison
        self.assertIsNotNone(comparison.strategy_stressed)
        self.assertIsNotNone(comparison.benchmark_stressed)
        self.assertEqual(
            run.strategy_run.strategy_batch.intents,
            (),
        )
        self.assertGreater(
            len(run.deterministic_run.benchmark_batch.intents),
            0,
        )
        self.assertEqual(
            len(run.deterministic_run.fold_summaries),
            len(plan.folds),
        )

    def test_registration_must_bind_exact_strategy_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            panel = _panel(root / "evidence")
            signal = (
                panel.source_panel.source_panel.adjustment_panel
                .signal_session
            )
            plan = _plan(signal)
            universe_id = _universe_id(panel)
            dataset = _dataset(plan, universe_id)
            strategy = _strategy_config()
            benchmark = _benchmark_config()
            wrong_strategy = PromotedIntentPolicyConfig(
                maximum_positions=4
            )
            resolver = _Resolver((panel,))
            with self.assertRaises(PromotedWalkForwardError):
                PromotedWalkForwardEvaluationEngine().evaluate(
                    registration=_registration(
                        plan=plan,
                        dataset=dataset,
                        strategy=strategy,
                        benchmark=benchmark,
                    ),
                    strategy_config=wrong_strategy,
                    benchmark_config=benchmark,
                    split_plan=plan,
                    dataset=dataset,
                    instruments=(_instrument(plan, universe_id),),
                    bindings=(_binding(panel, plan),),
                    cross_section_resolver=resolver,
                    intent_store=LocalPromotedResearchIntentStore(
                        root / "store",
                        resolver,
                    ),
                    execution_policy=_policy(),
                    cost_schedule=(
                        zerodha_nse_delivery_schedule_2026()
                    ),
                    initial_capital=Decimal("100000"),
                )


if __name__ == "__main__":
    unittest.main()
