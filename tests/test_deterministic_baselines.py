from __future__ import annotations

import unittest
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from india_swing.evaluation import (
    DailyExecutionPolicy,
    DeterministicBaselineError,
    DeterministicBaselineEvaluationEngine,
    DeterministicEqualWeightBenchmarkGenerator,
    DeterministicMomentumIntentGenerator,
    EqualWeightBenchmarkConfig,
    EvaluationDataReadiness,
    EvaluationDataset,
    GeneratedIntentRole,
    MomentumBaselineConfig,
    PointInTimeInstrument,
    TrialRegistration,
    TrialStage,
    build_expanding_purged_walk_forward_plan,
)
from india_swing.execution import SimulationBar, zerodha_nse_delivery_schedule_2026
from india_swing.identity import content_id


def D(value: str) -> Decimal:
    return Decimal(value)


SESSIONS = tuple(date(2026, 7, 1) + timedelta(days=index) for index in range(60))
DATA_ID = "a" * 64
UNIVERSE_ID = "b" * 64


def split_plan():
    return build_expanding_purged_walk_forward_plan(
        calendar_version="synthetic-deterministic-baseline-calendar-v1",
        ordered_sessions=SESSIONS,
        initial_training_sessions=15,
        validation_sessions=5,
        test_sessions=5,
        step_sessions=5,
        label_horizon_sessions=10,
        embargo_sessions=10,
    )


def bar(session: date, symbol: str, close: Decimal, volume: int) -> SimulationBar:
    return SimulationBar(
        session=session,
        symbol=symbol,
        open=close - D("0.50"),
        high=close + D("1.00"),
        low=close - D("1.00"),
        close=close,
        volume=volume,
    )


def dataset(*, changed_bar: SimulationBar | None = None) -> EvaluationDataset:
    specifications = (
        ("ALPHA", D("1.00"), 1_000_000),
        ("BETA", D("0.50"), 2_000_000),
        ("GAMMA", D("0.10"), 3_000_000),
    )
    values = [
        bar(session, symbol, D("100") + slope * index, volume)
        for index, session in enumerate(SESSIONS)
        for symbol, slope, volume in specifications
    ]
    if changed_bar is not None:
        values = [
            changed_bar
            if (value.session, value.symbol) == (changed_bar.session, changed_bar.symbol)
            else value
            for value in values
        ]
    return EvaluationDataset(
        sessions=SESSIONS,
        bars=tuple(sorted(values, key=lambda value: (value.session, value.symbol))),
        source_snapshot_ids=(DATA_ID,),
        universe_snapshot_ids=(UNIVERSE_ID,),
        readiness=EvaluationDataReadiness.SYNTHETIC,
    )


def instruments(*, gamma_sessions: tuple[date, ...] = SESSIONS):
    values = (
        PointInTimeInstrument(
            symbol="ALPHA",
            isin="INE000A01001",
            universe_snapshot_id=UNIVERSE_ID,
            eligible_sessions=SESSIONS,
            tick_size=D("0.05"),
        ),
        PointInTimeInstrument(
            symbol="BETA",
            isin="INE000A01002",
            universe_snapshot_id=UNIVERSE_ID,
            eligible_sessions=SESSIONS,
            tick_size=D("0.05"),
        ),
        PointInTimeInstrument(
            symbol="GAMMA",
            isin="INE000A01003",
            universe_snapshot_id=UNIVERSE_ID,
            eligible_sessions=gamma_sessions,
            tick_size=D("0.05"),
        ),
    )
    return tuple(sorted(values, key=lambda value: value.symbol))


def strategy_config() -> MomentumBaselineConfig:
    return MomentumBaselineConfig(
        lookback_sessions=10,
        maximum_positions=2,
        gross_exposure_fraction=D("0.80"),
        minimum_momentum=D("0.01"),
        stop_loss_fraction=D("0.50"),
        target_gain_fraction=D("0.50"),
        maximum_holding_sessions=3,
    )


def benchmark_config() -> EqualWeightBenchmarkConfig:
    return EqualWeightBenchmarkConfig(
        maximum_constituents=2,
        gross_exposure_fraction=D("0.80"),
        stop_loss_fraction=D("0.50"),
        target_gain_fraction=D("0.50"),
        maximum_holding_sessions=3,
    )


def policy() -> DailyExecutionPolicy:
    return DailyExecutionPolicy(
        slippage_bps=D("10"),
        stressed_slippage_bps=D("25"),
        maximum_participation=D("0.0025"),
    )


def registration() -> TrialRegistration:
    strategy = strategy_config()
    benchmark = benchmark_config()
    execution = policy()
    costs = zerodha_nse_delivery_schedule_2026()
    return TrialRegistration(
        registered_at=datetime(2026, 7, 1, 12, tzinfo=UTC),
        stage=TrialStage.EXPLORATORY,
        hypothesis="Point-in-time close momentum exceeds a liquid equal-weight basket.",
        strategy_family_id="deterministic-momentum-baseline-v1",
        parent_trial_id=None,
        evaluation_start=SESSIONS[0],
        evaluation_end=SESSIONS[-1],
        universe_snapshot_ids=(UNIVERSE_ID,),
        data_snapshot_ids=(DATA_ID,),
        split_plan_id=split_plan().plan_id,
        label_horizon_sessions=10,
        benchmark_id=benchmark.benchmark_id,
        primary_metric="net_return",
        secondary_metrics=("max_drawdown", "net_profit", "trade_count", "turnover"),
        model_bundle_id=strategy.strategy_id,
        source_commit="2f1a0b8",
        dependency_hash="c" * 64,
        configuration_hash=content_id((strategy, benchmark), length=64),
        exclusions_hash="d" * 64,
        risk_policy_hash="e" * 64,
        execution_policy_version=execution.version,
        execution_policy_hash=execution.policy_id,
        cost_schedule_version=costs.policy_version,
        cost_schedule_hash=costs.schedule_id,
        base_slippage_bps=execution.slippage_bps,
        stressed_slippage_bps=execution.stressed_slippage_bps,
        pass_thresholds=(("net_return", D("-1")),),
        multiple_testing_policy="single-synthetic-baseline-v1",
        random_seed=1729,
        repetition_count=1,
        holdout_id=None,
        holdout_sealed=False,
        synthetic=True,
    )


class DeterministicBaselineTests(unittest.TestCase):
    def test_momentum_generator_is_reproducible_and_explains_every_candidate(self) -> None:
        common = dict(
            config=strategy_config(),
            split_plan=split_plan(),
            dataset=dataset(),
            instruments=instruments(),
            execution_policy=policy(),
            initial_capital=D("100000"),
        )

        first = DeterministicMomentumIntentGenerator().generate(**common)
        second = DeterministicMomentumIntentGenerator().generate(**common)

        self.assertEqual(first.batch_id, second.batch_id)
        self.assertEqual(first.intents, second.intents)
        self.assertIs(first.role, GeneratedIntentRole.STRATEGY)
        self.assertEqual(len(first.decisions), len(split_plan().folds) * 3)
        self.assertEqual(len(first.intents), len(split_plan().folds) * 2)
        first_fold = split_plan().folds[0]
        selected = [
            value.symbol
            for value in first.decisions
            if value.signal_session == first_fold.test_sessions[0] and value.selected
        ]
        self.assertEqual(selected, ["ALPHA", "BETA"])
        self.assertTrue(all(value.evidence_bar_ids for value in first.decisions))

    def test_future_bar_change_does_not_change_prior_signal_or_intent(self) -> None:
        plan = split_plan()
        signal_session = plan.folds[0].test_sessions[0]
        future_session = plan.folds[0].test_sessions[2]
        original = dataset()
        changed = dataset(
            changed_bar=bar(future_session, "ALPHA", D("50"), 1_000_000)
        )
        common = dict(
            config=strategy_config(),
            split_plan=plan,
            instruments=instruments(),
            execution_policy=policy(),
            initial_capital=D("100000"),
        )

        before = DeterministicMomentumIntentGenerator().generate(dataset=original, **common)
        after = DeterministicMomentumIntentGenerator().generate(dataset=changed, **common)
        before_decision = next(
            value
            for value in before.decisions
            if value.signal_session == signal_session and value.symbol == "ALPHA"
        )
        after_decision = next(
            value
            for value in after.decisions
            if value.signal_session == signal_session and value.symbol == "ALPHA"
        )
        before_intent = next(
            value
            for value in before.intents
            if value.entry_order.signal_session == signal_session
            and value.entry_order.symbol == "ALPHA"
        )
        after_intent = next(
            value
            for value in after.intents
            if value.entry_order.signal_session == signal_session
            and value.entry_order.symbol == "ALPHA"
        )

        self.assertNotEqual(original.dataset_id, changed.dataset_id)
        self.assertEqual(before_decision.decision_id, after_decision.decision_id)
        self.assertEqual(before_intent.intent_id, after_intent.intent_id)

    def test_later_eligibility_cannot_retroactively_enter_earlier_universe(self) -> None:
        plan = split_plan()
        first_signal = plan.folds[0].test_sessions[0]
        later_only = instruments(gamma_sessions=SESSIONS[43:])

        batch = DeterministicEqualWeightBenchmarkGenerator().generate(
            config=benchmark_config(),
            split_plan=plan,
            dataset=dataset(),
            instruments=later_only,
            execution_policy=policy(),
            initial_capital=D("100000"),
        )

        gamma = next(
            value
            for value in batch.decisions
            if value.signal_session == first_signal and value.symbol == "GAMMA"
        )
        self.assertFalse(gamma.selected)
        self.assertEqual(gamma.reason, "NOT_POINT_IN_TIME_ELIGIBLE")
        self.assertFalse(
            any(
                value.entry_order.signal_session == first_signal
                and value.entry_order.symbol == "GAMMA"
                for value in batch.intents
            )
        )

    def test_registered_generators_run_through_base_and_stressed_comparison(self) -> None:
        run = DeterministicBaselineEvaluationEngine().evaluate(
            registration=registration(),
            strategy_config=strategy_config(),
            benchmark_config=benchmark_config(),
            split_plan=split_plan(),
            dataset=dataset(),
            instruments=instruments(),
            execution_policy=policy(),
            cost_schedule=zerodha_nse_delivery_schedule_2026(),
            initial_capital=D("100000"),
        )

        self.assertGreater(len(run.strategy_batch.intents), 0)
        self.assertGreater(len(run.benchmark_batch.intents), 0)
        self.assertIsNotNone(run.comparison.strategy_stressed)
        self.assertIsNotNone(run.comparison.benchmark_stressed)
        self.assertEqual(run.comparison.strategy_id, strategy_config().strategy_id)
        self.assertEqual(run.comparison.benchmark_id, benchmark_config().benchmark_id)

    def test_registration_must_bind_both_generator_configs(self) -> None:
        different_strategy = MomentumBaselineConfig(
            lookback_sessions=9,
            maximum_positions=2,
            gross_exposure_fraction=D("0.80"),
            minimum_momentum=D("0.01"),
            stop_loss_fraction=D("0.50"),
            target_gain_fraction=D("0.50"),
            maximum_holding_sessions=3,
        )

        with self.assertRaisesRegex(
            DeterministicBaselineError, "bind the strategy generator"
        ):
            DeterministicBaselineEvaluationEngine().evaluate(
                registration=registration(),
                strategy_config=different_strategy,
                benchmark_config=benchmark_config(),
                split_plan=split_plan(),
                dataset=dataset(),
                instruments=instruments(),
                execution_policy=policy(),
                cost_schedule=zerodha_nse_delivery_schedule_2026(),
                initial_capital=D("100000"),
            )


if __name__ == "__main__":
    unittest.main()
