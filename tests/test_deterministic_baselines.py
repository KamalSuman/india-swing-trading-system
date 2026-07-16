from __future__ import annotations

import unittest
import json
import io
import tempfile
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from india_swing.evaluation import (
    DailyExecutionPolicy,
    DeterministicBaselineError,
    DeterministicComparisonRunConflict,
    DeterministicBaselineEvaluationEngine,
    DeterministicEqualWeightBenchmarkGenerator,
    DeterministicMomentumIntentGenerator,
    EqualWeightBenchmarkConfig,
    EvaluationDataReadiness,
    EvaluationEvidenceConfig,
    EvaluationDataset,
    GeneratedIntentRole,
    GeneratedIntentBatchConflict,
    LocalDeterministicComparisonRunStore,
    LocalGeneratedIntentBatchStore,
    LocalTrialFamilyAggregateStore,
    LocalTrialLifecycleStore,
    LocalTrialFamilyReportStore,
    LocalTrialEvaluationComparisonStore,
    LocalTrialEvaluationResultStore,
    LocalTrialRegistry,
    FOLD_SIGN_HOLM_POLICY,
    MomentumBaselineConfig,
    PointInTimeInstrument,
    TrialRegistration,
    TrialFamilyAggregationError,
    TrialFamilyReportConflict,
    TrialLifecycleConflict,
    TrialLifecycleEventType,
    TrialFamilyEvaluationAggregator,
    TrialStage,
    build_expanding_purged_walk_forward_plan,
    decode_generated_intent_batch,
    decode_trial_family_aggregate,
    encode_generated_intent_batch,
    encode_trial_family_aggregate,
    build_trial_family_evaluation_report,
)
from india_swing.evaluation.cli import main as evaluation_cli_main
from india_swing.execution import SimulationBar, zerodha_nse_delivery_schedule_2026
from india_swing.identity import content_id


def D(value: str) -> Decimal:
    return Decimal(value)


SESSIONS = tuple(date(2026, 7, 1) + timedelta(days=index) for index in range(65))
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


def registration(
    *,
    strategy: MomentumBaselineConfig | None = None,
    benchmark: EqualWeightBenchmarkConfig | None = None,
    **overrides: object,
) -> TrialRegistration:
    strategy = strategy or strategy_config()
    benchmark = benchmark or benchmark_config()
    execution = policy()
    costs = zerodha_nse_delivery_schedule_2026()
    values: dict[str, object] = {
        "registered_at": datetime(2026, 7, 1, 12, tzinfo=UTC),
        "stage": TrialStage.EXPLORATORY,
        "hypothesis": "Point-in-time close momentum exceeds a liquid equal-weight basket.",
        "strategy_family_id": "deterministic-momentum-baseline-v1",
        "parent_trial_id": None,
        "evaluation_start": SESSIONS[0],
        "evaluation_end": SESSIONS[-1],
        "universe_snapshot_ids": (UNIVERSE_ID,),
        "data_snapshot_ids": (DATA_ID,),
        "split_plan_id": split_plan().plan_id,
        "label_horizon_sessions": 10,
        "benchmark_id": benchmark.benchmark_id,
        "primary_metric": "net_return",
        "secondary_metrics": ("max_drawdown", "net_profit", "trade_count", "turnover"),
        "model_bundle_id": strategy.strategy_id,
        "source_commit": "2937d01",
        "dependency_hash": "c" * 64,
        "configuration_hash": content_id((strategy, benchmark), length=64),
        "exclusions_hash": "d" * 64,
        "risk_policy_hash": "e" * 64,
        "execution_policy_version": execution.version,
        "execution_policy_hash": execution.policy_id,
        "cost_schedule_version": costs.policy_version,
        "cost_schedule_hash": costs.schedule_id,
        "base_slippage_bps": execution.slippage_bps,
        "stressed_slippage_bps": execution.stressed_slippage_bps,
        "pass_thresholds": (("net_return", D("-1")),),
        "multiple_testing_policy": "single-synthetic-baseline-v1",
        "random_seed": 1729,
        "repetition_count": 1,
        "holdout_id": None,
        "holdout_sealed": False,
        "synthetic": True,
    }
    values.update(overrides)
    return TrialRegistration(**values)


def evaluate_run(
    registered: TrialRegistration | None = None,
    strategy: MomentumBaselineConfig | None = None,
    data: EvaluationDataset | None = None,
):
    return DeterministicBaselineEvaluationEngine().evaluate(
        registration=registered or registration(strategy=strategy),
        strategy_config=strategy or strategy_config(),
        benchmark_config=benchmark_config(),
        split_plan=split_plan(),
        dataset=data or dataset(),
        instruments=instruments(),
        execution_policy=policy(),
        cost_schedule=zerodha_nse_delivery_schedule_2026(),
        initial_capital=D("100000"),
    )


class DeterministicBaselineTests(unittest.TestCase):
    def test_evaluation_evidence_config_has_safe_default_and_override(self) -> None:
        self.assertEqual(
            EvaluationEvidenceConfig.from_env({}).data_root,
            Path("var/evaluation"),
        )
        self.assertEqual(
            EvaluationEvidenceConfig.from_env(
                {"INDIA_SWING_EVALUATION_ROOT": "D:/sealed/evaluation"}
            ).data_root,
            Path("D:/sealed/evaluation"),
        )

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
        run = evaluate_run()

        self.assertGreater(len(run.strategy_batch.intents), 0)
        self.assertGreater(len(run.benchmark_batch.intents), 0)
        self.assertIsNotNone(run.comparison.strategy_stressed)
        self.assertIsNotNone(run.comparison.benchmark_stressed)
        self.assertEqual(run.comparison.strategy_id, strategy_config().strategy_id)
        self.assertEqual(run.comparison.benchmark_id, benchmark_config().benchmark_id)
        self.assertEqual(len(run.fold_summaries), len(split_plan().folds))
        self.assertTrue(
            all("base_primary_excess" in dict(value.comparison_metrics) for value in run.fold_summaries)
        )

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


class DeterministicBaselineStoreAndFamilyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.registry = LocalTrialRegistry(self.root / "trials")
        self.result_store = LocalTrialEvaluationResultStore(
            self.root / "evidence", self.registry
        )
        self.comparison_store = LocalTrialEvaluationComparisonStore(
            self.root / "evidence", self.registry, self.result_store
        )
        self.batch_store = LocalGeneratedIntentBatchStore(
            self.root / "evidence", self.registry
        )
        self.run_store = LocalDeterministicComparisonRunStore(
            self.batch_store, self.comparison_store
        )
        self.family_store = LocalTrialFamilyAggregateStore(
            self.root / "evidence", self.registry, self.run_store
        )
        self.lifecycle_store = LocalTrialLifecycleStore(
            self.root / "lifecycle",
            self.registry,
            self.comparison_store,
            self.family_store,
        )
        self.report_store = LocalTrialFamilyReportStore(
            self.root / "evidence", self.family_store
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_batch_codec_and_create_once_run_store_round_trip(self) -> None:
        registered = registration()
        self.registry.register(registered)
        run = evaluate_run(registered)

        decoded = decode_generated_intent_batch(
            encode_generated_intent_batch(run.strategy_batch)
        )
        stored = self.run_store.publish(run)

        self.assertEqual(decoded, run.strategy_batch)
        self.assertEqual(stored, run)
        self.assertEqual(
            self.batch_store.get(registered.trial_id, GeneratedIntentRole.STRATEGY),
            run.strategy_batch,
        )
        self.assertEqual(
            self.batch_store.get(registered.trial_id, GeneratedIntentRole.BENCHMARK),
            run.benchmark_batch,
        )

    def test_trial_role_cannot_be_replaced_by_a_different_batch(self) -> None:
        registered = registration()
        self.registry.register(registered)
        first = evaluate_run(registered)
        self.batch_store.publish(registered.trial_id, first.strategy_batch)
        signal_session = split_plan().folds[0].test_sessions[0]
        changed = dataset(
            changed_bar=bar(signal_session, "ALPHA", D("80"), 1_000_000)
        )
        second_batch = DeterministicMomentumIntentGenerator().generate(
            config=strategy_config(),
            split_plan=split_plan(),
            dataset=changed,
            instruments=instruments(),
            execution_policy=policy(),
            initial_capital=D("100000"),
        )

        with self.assertRaisesRegex(
            GeneratedIntentBatchConflict, "different generated batch"
        ):
            self.batch_store.publish(registered.trial_id, second_batch)

    def test_stored_batch_tampering_is_detected(self) -> None:
        registered = registration()
        self.registry.register(registered)
        run = evaluate_run(registered)
        self.batch_store.publish(registered.trial_id, run.strategy_batch)
        path = (
            self.root
            / "evidence"
            / "intent_batches"
            / registered.trial_id
            / "strategy.json"
        )
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["batch"]["decisions"][0]["reason"] = "RESULT_INFORMED_REWRITE"
        path.write_text(json.dumps(payload), encoding="utf-8")

        with self.assertRaises(GeneratedIntentBatchConflict):
            self.batch_store.get(registered.trial_id, GeneratedIntentRole.STRATEGY)

    def test_holm_gate_covers_entire_family_and_blocks_two_marginal_variants(self) -> None:
        parent = registration(multiple_testing_policy=FOLD_SIGN_HOLM_POLICY)
        revised_strategy = MomentumBaselineConfig(
            lookback_sessions=9,
            maximum_positions=2,
            gross_exposure_fraction=D("0.80"),
            minimum_momentum=D("0.01"),
            stop_loss_fraction=D("0.50"),
            target_gain_fraction=D("0.50"),
            maximum_holding_sessions=3,
        )
        child = registration(
            strategy=revised_strategy,
            multiple_testing_policy=FOLD_SIGN_HOLM_POLICY,
            registered_at=parent.registered_at + timedelta(seconds=1),
            parent_trial_id=parent.trial_id,
        )
        self.registry.register(parent)
        self.registry.register(child)
        parent_run = evaluate_run(parent)
        child_run = evaluate_run(child, revised_strategy)
        self.run_store.publish(parent_run)
        self.run_store.publish(child_run)

        aggregate = TrialFamilyEvaluationAggregator(
            self.registry, self.run_store
        ).aggregate(
            strategy_family_id=parent.strategy_family_id,
            runs=(parent_run, child_run),
        )

        self.assertEqual(len(aggregate.decisions), 2)
        self.assertEqual(aggregate.decisions[0].holm_threshold, D("0.025"))
        self.assertFalse(aggregate.passed)
        self.assertEqual(aggregate.eligible_trial_ids, ())

    def test_single_preregistered_variant_can_clear_fold_sign_gate(self) -> None:
        registered = registration(multiple_testing_policy=FOLD_SIGN_HOLM_POLICY)
        self.registry.register(registered)
        run = evaluate_run(registered)
        self.run_store.publish(run)

        aggregate = TrialFamilyEvaluationAggregator(
            self.registry, self.run_store
        ).aggregate(
            strategy_family_id=registered.strategy_family_id,
            runs=(run,),
        )

        decision = aggregate.decisions[0]
        self.assertEqual(decision.fold_count, 5)
        self.assertEqual(decision.base_wins, 5)
        self.assertEqual(decision.stressed_wins, 5)
        self.assertEqual(decision.raw_p_value, D("0.03125"))
        self.assertEqual(decision.holm_threshold, D("0.05"))
        self.assertTrue(aggregate.passed)
        self.assertEqual(aggregate.eligible_trial_ids, (registered.trial_id,))

    def test_family_aggregation_rejects_selective_trial_omission(self) -> None:
        parent = registration(multiple_testing_policy=FOLD_SIGN_HOLM_POLICY)
        child_strategy = MomentumBaselineConfig(
            lookback_sessions=9,
            maximum_positions=2,
            gross_exposure_fraction=D("0.80"),
            minimum_momentum=D("0.01"),
            stop_loss_fraction=D("0.50"),
            target_gain_fraction=D("0.50"),
            maximum_holding_sessions=3,
        )
        child = registration(
            strategy=child_strategy,
            multiple_testing_policy=FOLD_SIGN_HOLM_POLICY,
            registered_at=parent.registered_at + timedelta(seconds=1),
            parent_trial_id=parent.trial_id,
        )
        self.registry.register(parent)
        self.registry.register(child)
        parent_run = evaluate_run(parent)
        self.run_store.publish(parent_run)

        with self.assertRaisesRegex(TrialFamilyAggregationError, "every registered"):
            TrialFamilyEvaluationAggregator(self.registry, self.run_store).aggregate(
                strategy_family_id=parent.strategy_family_id,
                runs=(parent_run,),
            )

    def test_family_aggregate_store_and_human_report_are_deterministic(self) -> None:
        registered = registration(multiple_testing_policy=FOLD_SIGN_HOLM_POLICY)
        self.registry.register(registered)
        run = evaluate_run(registered)
        self.run_store.publish(run)
        aggregate = TrialFamilyEvaluationAggregator(
            self.registry, self.run_store
        ).aggregate(
            strategy_family_id=registered.strategy_family_id,
            runs=(run,),
        )

        decoded = decode_trial_family_aggregate(
            encode_trial_family_aggregate(aggregate)
        )
        stored = self.family_store.publish(aggregate, runs=(run,))
        first_report = build_trial_family_evaluation_report(
            aggregate=stored, runs=(run,)
        )
        second_report = build_trial_family_evaluation_report(
            aggregate=stored, runs=(run,)
        )
        persisted_report = self.report_store.publish(
            first_report,
            aggregate=stored,
            runs=(run,),
        )

        self.assertEqual(decoded, aggregate)
        self.assertEqual(stored, aggregate)
        self.assertEqual(first_report.report_id, second_report.report_id)
        self.assertEqual(persisted_report, first_report)
        self.assertEqual(self.report_store.get(aggregate.aggregate_id), first_report)
        self.assertEqual(self.report_store.list_reports(), (first_report,))
        self.assertIn("## Family decisions", first_report.markdown)
        self.assertIn("## Fold evidence", first_report.markdown)
        self.assertIn("not a profit forecast or trade alert", first_report.markdown)

    def test_promotion_requires_current_persisted_eligible_family_aggregate(self) -> None:
        registered = registration(multiple_testing_policy=FOLD_SIGN_HOLM_POLICY)
        self.registry.register(registered)
        run = evaluate_run(registered)
        self.run_store.publish(run)
        aggregate = TrialFamilyEvaluationAggregator(
            self.registry, self.run_store
        ).aggregate(
            strategy_family_id=registered.strategy_family_id,
            runs=(run,),
        )
        self.family_store.publish(aggregate, runs=(run,))
        started = self.lifecycle_store.append(
            trial_id=registered.trial_id,
            event_type=TrialLifecycleEventType.TRIAL_STARTED,
            occurred_at=registered.registered_at + timedelta(seconds=1),
            actor_id="evaluation-runner",
            reason="Start the registered synthetic family evaluation.",
        )
        completed = self.lifecycle_store.append(
            trial_id=registered.trial_id,
            event_type=TrialLifecycleEventType.TRIAL_COMPLETED,
            occurred_at=registered.registered_at + timedelta(seconds=2),
            actor_id="evaluation-runner",
            reason="Record the persisted strategy and benchmark comparison.",
            evaluation_comparison=run.comparison,
        )
        promoted = self.lifecycle_store.append(
            trial_id=registered.trial_id,
            event_type=TrialLifecycleEventType.TRIAL_PROMOTED,
            occurred_at=registered.registered_at + timedelta(seconds=3),
            actor_id="research-owner",
            reason="Promote only after the complete registered family passed.",
            family_aggregate=aggregate,
        )

        self.assertEqual(promoted.family_aggregate_id, aggregate.aggregate_id)
        self.assertEqual(
            self.lifecycle_store.outcomes(registered.trial_id),
            (completed, promoted),
        )
        self.assertEqual(promoted.previous_event_id, completed.event_id)
        self.assertNotEqual(started.event_id, promoted.event_id)

    def test_new_family_variant_makes_older_aggregate_ineligible_for_promotion(self) -> None:
        parent = registration(multiple_testing_policy=FOLD_SIGN_HOLM_POLICY)
        self.registry.register(parent)
        run = evaluate_run(parent)
        self.run_store.publish(run)
        aggregate = TrialFamilyEvaluationAggregator(
            self.registry, self.run_store
        ).aggregate(
            strategy_family_id=parent.strategy_family_id,
            runs=(run,),
        )
        self.family_store.publish(aggregate, runs=(run,))
        child_strategy = MomentumBaselineConfig(
            lookback_sessions=9,
            maximum_positions=2,
            gross_exposure_fraction=D("0.80"),
            minimum_momentum=D("0.01"),
            stop_loss_fraction=D("0.50"),
            target_gain_fraction=D("0.50"),
            maximum_holding_sessions=3,
        )
        child = registration(
            strategy=child_strategy,
            multiple_testing_policy=FOLD_SIGN_HOLM_POLICY,
            registered_at=parent.registered_at + timedelta(seconds=1),
            parent_trial_id=parent.trial_id,
        )
        self.registry.register(child)
        self.lifecycle_store.append(
            trial_id=parent.trial_id,
            event_type=TrialLifecycleEventType.TRIAL_STARTED,
            occurred_at=parent.registered_at + timedelta(seconds=2),
            actor_id="evaluation-runner",
            reason="Start the parent trial lifecycle.",
        )
        self.lifecycle_store.append(
            trial_id=parent.trial_id,
            event_type=TrialLifecycleEventType.TRIAL_COMPLETED,
            occurred_at=parent.registered_at + timedelta(seconds=3),
            actor_id="evaluation-runner",
            reason="Record the parent comparison before family promotion.",
            evaluation_comparison=run.comparison,
        )

        with self.assertRaisesRegex(TrialLifecycleConflict, "current family"):
            self.lifecycle_store.append(
                trial_id=parent.trial_id,
                event_type=TrialLifecycleEventType.TRIAL_PROMOTED,
                occurred_at=parent.registered_at + timedelta(seconds=4),
                actor_id="research-owner",
                reason="Attempt promotion using a stale family snapshot.",
                family_aggregate=aggregate,
            )

    def test_promotion_must_match_the_comparison_recorded_at_completion(self) -> None:
        registered = registration(multiple_testing_policy=FOLD_SIGN_HOLM_POLICY)
        self.registry.register(registered)
        aggregate_run = evaluate_run(registered)
        self.run_store.publish(aggregate_run)
        aggregate = TrialFamilyEvaluationAggregator(
            self.registry, self.run_store
        ).aggregate(
            strategy_family_id=registered.strategy_family_id,
            runs=(aggregate_run,),
        )
        self.family_store.publish(aggregate, runs=(aggregate_run,))
        changed_session = split_plan().folds[0].test_sessions[2]
        completion_run = evaluate_run(
            registered,
            data=dataset(
                changed_bar=bar(changed_session, "ALPHA", D("135"), 1_000_000)
            ),
        )
        self.comparison_store.publish(completion_run.comparison)
        self.lifecycle_store.append(
            trial_id=registered.trial_id,
            event_type=TrialLifecycleEventType.TRIAL_STARTED,
            occurred_at=registered.registered_at + timedelta(seconds=1),
            actor_id="evaluation-runner",
            reason="Start the registered comparison-matching test.",
        )
        self.lifecycle_store.append(
            trial_id=registered.trial_id,
            event_type=TrialLifecycleEventType.TRIAL_COMPLETED,
            occurred_at=registered.registered_at + timedelta(seconds=2),
            actor_id="evaluation-runner",
            reason="Complete using a different persisted comparison.",
            evaluation_comparison=completion_run.comparison,
        )

        with self.assertRaisesRegex(TrialLifecycleConflict, "completed comparison"):
            self.lifecycle_store.append(
                trial_id=registered.trial_id,
                event_type=TrialLifecycleEventType.TRIAL_PROMOTED,
                occurred_at=registered.registered_at + timedelta(seconds=3),
                actor_id="research-owner",
                reason="Attempt to promote a different comparison.",
                family_aggregate=aggregate,
            )

    def test_trial_cannot_publish_a_second_deterministic_run(self) -> None:
        registered = registration(multiple_testing_policy=FOLD_SIGN_HOLM_POLICY)
        self.registry.register(registered)
        first = evaluate_run(registered)
        self.run_store.publish(first)
        changed_session = split_plan().folds[0].test_sessions[2]
        second = evaluate_run(
            registered,
            data=dataset(
                changed_bar=bar(changed_session, "ALPHA", D("135"), 1_000_000)
            ),
        )

        with self.assertRaisesRegex(
            DeterministicComparisonRunConflict, "different deterministic run"
        ):
            self.run_store.publish(second)

    def test_evaluation_report_cli_publishes_lists_and_shows_persisted_report(self) -> None:
        registered = registration(multiple_testing_policy=FOLD_SIGN_HOLM_POLICY)
        self.registry.register(registered)
        run = evaluate_run(registered)
        self.run_store.publish(run)
        aggregate = TrialFamilyEvaluationAggregator(
            self.registry, self.run_store
        ).aggregate(
            strategy_family_id=registered.strategy_family_id,
            runs=(run,),
        )
        self.family_store.publish(aggregate, runs=(run,))
        environment = {
            "INDIA_SWING_TRIAL_REGISTRY_ROOT": str(self.root / "trials"),
            "INDIA_SWING_EVALUATION_ROOT": str(self.root / "evidence"),
        }
        publish_output = io.StringIO()
        with patch.dict("os.environ", environment, clear=False), patch(
            "sys.stdout", publish_output
        ):
            publish_code = evaluation_cli_main(
                [
                    "report",
                    "publish",
                    "--strategy-family-id",
                    registered.strategy_family_id,
                ]
            )
        response = json.loads(publish_output.getvalue())
        show_output = io.StringIO()
        with patch.dict("os.environ", environment, clear=False), patch(
            "sys.stdout", show_output
        ):
            show_code = evaluation_cli_main(
                ["report", "show", "--aggregate-id", aggregate.aggregate_id]
            )
        list_output = io.StringIO()
        with patch.dict("os.environ", environment, clear=False), patch(
            "sys.stdout", list_output
        ):
            list_code = evaluation_cli_main(["report", "list"])
        listed = json.loads(list_output.getvalue())

        self.assertEqual(publish_code, 0)
        self.assertEqual(response["report_id"], self.report_store.get(aggregate.aggregate_id).report_id)
        self.assertEqual(show_code, 0)
        self.assertIn("# Trial family evaluation", show_output.getvalue())
        self.assertEqual(list_code, 0)
        self.assertEqual(listed["count"], 1)

    def test_persisted_report_tampering_is_detected(self) -> None:
        registered = registration(multiple_testing_policy=FOLD_SIGN_HOLM_POLICY)
        self.registry.register(registered)
        run = evaluate_run(registered)
        self.run_store.publish(run)
        aggregate = TrialFamilyEvaluationAggregator(
            self.registry, self.run_store
        ).aggregate(
            strategy_family_id=registered.strategy_family_id,
            runs=(run,),
        )
        self.family_store.publish(aggregate, runs=(run,))
        report = build_trial_family_evaluation_report(
            aggregate=aggregate, runs=(run,)
        )
        self.report_store.publish(report, aggregate=aggregate, runs=(run,))
        path = self.report_store.path_for(aggregate.aggregate_id)
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["report"]["markdown"] = "# Result-informed rewrite\n"
        path.write_text(json.dumps(payload), encoding="utf-8")

        with self.assertRaises(TrialFamilyReportConflict):
            self.report_store.get(aggregate.aggregate_id)


if __name__ == "__main__":
    unittest.main()
