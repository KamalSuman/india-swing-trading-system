from __future__ import annotations

import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from india_swing.evaluation import (
    DailyExecutionPolicy,
    EvaluationDataReadiness,
    EvaluationDataset,
    EvaluationTradeIntent,
    TrialEvaluationEngine,
    TrialEvaluationError,
    TrialRegistration,
    TrialStage,
    build_expanding_purged_walk_forward_plan,
)
from india_swing.execution import (
    ExitReason,
    LimitEntryOrder,
    SimulationBar,
    zerodha_nse_delivery_schedule_2026,
)


D = Decimal
UTC = timezone.utc
DATA_ID = "1" * 64
UNIVERSE_ID = "2" * 64
SIGNAL_ID = "3" * 64
SESSIONS = tuple(date(2026, 7, 1) + timedelta(days=value) for value in range(36))
SIGNAL_INDEX = 28


def simulation_bar(session: date, **overrides: object) -> SimulationBar:
    values: dict[str, object] = {
        "session": session,
        "symbol": "RELIANCE",
        "open": D("99"),
        "high": D("101"),
        "low": D("98"),
        "close": D("100"),
        "volume": 1_000_000,
    }
    values.update(overrides)
    return SimulationBar(**values)


def policy() -> DailyExecutionPolicy:
    return DailyExecutionPolicy(
        slippage_bps=D("10"),
        maximum_participation=D("0.0025"),
    )


def split_plan():
    return build_expanding_purged_walk_forward_plan(
        calendar_version="synthetic-calendar-v1",
        ordered_sessions=SESSIONS,
        initial_training_sessions=5,
        validation_sessions=3,
        test_sessions=4,
        step_sessions=4,
        label_horizon_sessions=10,
        embargo_sessions=10,
    )


def dataset(
    *,
    readiness: EvaluationDataReadiness = EvaluationDataReadiness.SYNTHETIC,
    bars: tuple[SimulationBar, ...] | None = None,
) -> EvaluationDataset:
    values = bars or (
        simulation_bar(SESSIONS[SIGNAL_INDEX + 1]),
        simulation_bar(
            SESSIONS[SIGNAL_INDEX + 2],
            open=D("102"),
            high=D("105"),
            low=D("101"),
            close=D("104"),
        ),
        simulation_bar(
            SESSIONS[SIGNAL_INDEX + 3],
            open=D("108"),
            high=D("111"),
            low=D("107"),
            close=D("110"),
        ),
    )
    return EvaluationDataset(
        sessions=SESSIONS,
        bars=tuple(sorted(values, key=lambda value: (value.session, value.symbol))),
        source_snapshot_ids=(DATA_ID,),
        universe_snapshot_ids=(UNIVERSE_ID,),
        readiness=readiness,
    )


def intent(**overrides: object) -> EvaluationTradeIntent:
    values: dict[str, object] = {
        "signal_id": SIGNAL_ID,
        "universe_snapshot_id": UNIVERSE_ID,
        "isin": "INE002A01018",
        "entry_order": LimitEntryOrder(
            symbol="RELIANCE",
            signal_session=SESSIONS[SIGNAL_INDEX],
            first_eligible_session=SESSIONS[SIGNAL_INDEX + 1],
            expiry_session=SESSIONS[SIGNAL_INDEX + 1],
            quantity=100,
            limit_price=D("100"),
            tick_size=D("0.05"),
            maximum_participation=D("0.0025"),
        ),
        "stop_price": D("95"),
        "target_price": D("110"),
        "max_holding_sessions": 3,
    }
    values.update(overrides)
    return EvaluationTradeIntent(**values)


def registration(
    *,
    readiness: EvaluationDataReadiness = EvaluationDataReadiness.SYNTHETIC,
    **overrides: object,
) -> TrialRegistration:
    execution = policy()
    costs = zerodha_nse_delivery_schedule_2026()
    synthetic = readiness is EvaluationDataReadiness.SYNTHETIC
    values: dict[str, object] = {
        "registered_at": datetime(2026, 7, 14, 12, tzinfo=UTC),
        "stage": TrialStage.EXPLORATORY,
        "hypothesis": "Synthetic deterministic fills validate evaluation arithmetic.",
        "strategy_family_id": "synthetic-evaluation-engine-v1",
        "parent_trial_id": None,
        "evaluation_start": SESSIONS[0],
        "evaluation_end": SESSIONS[-1],
        "universe_snapshot_ids": (UNIVERSE_ID,),
        "data_snapshot_ids": (DATA_ID,),
        "split_plan_id": split_plan().plan_id,
        "label_horizon_sessions": 10,
        "benchmark_id": "synthetic-cash-benchmark-v1",
        "primary_metric": "net_return",
        "secondary_metrics": (
            "max_drawdown",
            "net_profit",
            "trade_count",
            "turnover",
        ),
        "model_bundle_id": "synthetic-deterministic-strategy-v1",
        "source_commit": "2f26f0e",
        "dependency_hash": "5" * 64,
        "configuration_hash": "6" * 64,
        "exclusions_hash": "7" * 64,
        "risk_policy_hash": "8" * 64,
        "execution_policy_version": execution.version,
        "execution_policy_hash": execution.policy_id,
        "cost_schedule_version": costs.policy_version,
        "cost_schedule_hash": costs.schedule_id,
        "base_slippage_bps": execution.slippage_bps,
        "stressed_slippage_bps": None,
        "pass_thresholds": (
            ("max_drawdown", D("-0.10")),
            ("net_return", D("0.005")),
        ),
        "multiple_testing_policy": "synthetic-single-trial-v1",
        "random_seed": 1729,
        "repetition_count": 1,
        "holdout_id": None,
        "holdout_sealed": False,
        "synthetic": synthetic,
    }
    values.update(overrides)
    return TrialRegistration(**values)


class TrialEvaluationEngineTests(unittest.TestCase):
    def evaluate(self, **overrides: object):
        values: dict[str, object] = {
            "registration": registration(),
            "split_plan": split_plan(),
            "dataset": dataset(),
            "intents": (intent(),),
            "execution_policy": policy(),
            "cost_schedule": zerodha_nse_delivery_schedule_2026(),
            "initial_capital": D("100000"),
        }
        values.update(overrides)
        return TrialEvaluationEngine().evaluate(**values)

    def test_engine_generates_fills_costs_equity_and_metrics(self) -> None:
        result = self.evaluate()

        self.assertEqual(len(result.trades), 1)
        trade = result.trades[0]
        self.assertEqual(trade.entry_fill.fill_price, D("99.10"))
        self.assertEqual(trade.exit_fill.fill_price, D("110"))
        self.assertIs(trade.exit_fill.exit_reason, ExitReason.TARGET)
        self.assertEqual(trade.gross_pnl, D("1090.00"))
        self.assertIsNotNone(result.charges)
        assert result.charges is not None
        self.assertEqual(result.charges.total, D("38.12"))
        self.assertEqual(result.equity_curve[-1].equity, D("101051.88"))
        metrics = dict(result.metrics)
        self.assertEqual(metrics["net_profit"], D("1051.88"))
        self.assertEqual(metrics["net_return"], D("0.0105188"))
        self.assertEqual(metrics["trade_count"], D("1"))
        self.assertEqual(metrics["turnover"], D("0.2091"))
        self.assertTrue(result.passed)
        self.assertEqual(len(result.result_id), 64)

    def test_no_limit_fill_is_a_valid_zero_trade_result(self) -> None:
        no_touch = dataset(
            bars=(
                simulation_bar(
                    SESSIONS[SIGNAL_INDEX + 1],
                    open=D("102"),
                    high=D("104"),
                    low=D("101"),
                    close=D("103"),
                ),
            )
        )
        value = registration(
            pass_thresholds=(("net_return", D("0")),),
        )

        result = self.evaluate(registration=value, dataset=no_touch)

        self.assertEqual(result.trades, ())
        self.assertIsNone(result.charges)
        self.assertEqual(dict(result.metrics)["net_return"], D("0"))
        self.assertTrue(result.passed)

    def test_registration_must_bind_exact_cost_and_execution_policies(self) -> None:
        cases = (
            (
                "cost",
                registration(cost_schedule_hash="9" * 64),
                "cost schedule content binding",
            ),
            (
                "execution",
                registration(execution_policy_hash="a" * 64),
                "execution policy content binding",
            ),
        )
        for label, value, message in cases:
            with self.subTest(binding=label):
                with self.assertRaisesRegex(TrialEvaluationError, message):
                    self.evaluate(registration=value)

    def test_signal_from_training_partition_cannot_be_evaluated(self) -> None:
        training_intent = intent(
            entry_order=LimitEntryOrder(
                symbol="RELIANCE",
                signal_session=SESSIONS[0],
                first_eligible_session=SESSIONS[1],
                expiry_session=SESSIONS[1],
                quantity=100,
                limit_price=D("100"),
                tick_size=D("0.05"),
                maximum_participation=D("0.0025"),
            )
        )

        with self.assertRaisesRegex(TrialEvaluationError, "test windows"):
            self.evaluate(intents=(training_intent,))

    def test_registration_must_bind_exact_walk_forward_plan(self) -> None:
        mismatched = registration(split_plan_id="a" * 64)

        with self.assertRaisesRegex(TrialEvaluationError, "split-plan binding"):
            self.evaluate(registration=mismatched)

    def test_collection_only_data_cannot_produce_reportable_metrics(self) -> None:
        collection = dataset(readiness=EvaluationDataReadiness.COLLECTION_ONLY)
        reportable = registration(readiness=EvaluationDataReadiness.COLLECTION_ONLY)

        with self.assertRaisesRegex(TrialEvaluationError, "point-in-time verified"):
            self.evaluate(registration=reportable, dataset=collection)

    def test_missing_holding_bar_fails_instead_of_skipping_session(self) -> None:
        missing = dataset(
            bars=(
                simulation_bar(SESSIONS[SIGNAL_INDEX + 1]),
                simulation_bar(
                    SESSIONS[SIGNAL_INDEX + 3],
                    open=D("108"),
                    high=D("111"),
                    low=D("107"),
                    close=D("110"),
                ),
            )
        )

        with self.assertRaisesRegex(TrialEvaluationError, "missing daily bar"):
            self.evaluate(dataset=missing)

    def test_locked_horizon_fails_instead_of_inventing_time_exit(self) -> None:
        locked = dataset(
            bars=(
                simulation_bar(SESSIONS[SIGNAL_INDEX + 1]),
                simulation_bar(SESSIONS[SIGNAL_INDEX + 2]),
                simulation_bar(
                    SESSIONS[SIGNAL_INDEX + 3],
                    open=D("90"),
                    high=D("90"),
                    low=D("90"),
                    close=D("90"),
                    lower_circuit_sell_locked=True,
                ),
            )
        )

        with self.assertRaisesRegex(TrialEvaluationError, "could not be liquidated"):
            self.evaluate(dataset=locked)

    def test_cash_account_cannot_use_implicit_leverage(self) -> None:
        oversized_order = LimitEntryOrder(
            symbol="RELIANCE",
            signal_session=SESSIONS[SIGNAL_INDEX],
            first_eligible_session=SESSIONS[SIGNAL_INDEX + 1],
            expiry_session=SESSIONS[SIGNAL_INDEX + 1],
            quantity=2000,
            limit_price=D("100"),
            tick_size=D("0.05"),
            maximum_participation=D("0.0025"),
        )

        with self.assertRaisesRegex(TrialEvaluationError, "available cash"):
            self.evaluate(intents=(intent(entry_order=oversized_order),))

    def test_same_day_stop_is_priced_as_intraday(self) -> None:
        same_day_stop = dataset(
            bars=(
                simulation_bar(
                    SESSIONS[SIGNAL_INDEX + 1],
                    open=D("99"),
                    high=D("101"),
                    low=D("94"),
                    close=D("96"),
                ),
                simulation_bar(SESSIONS[SIGNAL_INDEX + 2]),
                simulation_bar(SESSIONS[SIGNAL_INDEX + 3]),
            )
        )

        result = self.evaluate(dataset=same_day_stop)

        self.assertEqual(len(result.trades), 1)
        assert result.charges is not None
        self.assertEqual(result.charges.total, D("9.60"))
        self.assertEqual(result.equity_curve[-1].equity, D("99570.40"))

    def test_changed_bar_content_changes_dataset_and_result_identity(self) -> None:
        original = self.evaluate()
        changed_data = dataset(
            bars=(
                simulation_bar(SESSIONS[SIGNAL_INDEX + 1], close=D("100.05")),
                simulation_bar(
                    SESSIONS[SIGNAL_INDEX + 2],
                    open=D("102"),
                    high=D("105"),
                    low=D("101"),
                    close=D("104"),
                ),
                simulation_bar(
                    SESSIONS[SIGNAL_INDEX + 3],
                    open=D("108"),
                    high=D("111"),
                    low=D("107"),
                    close=D("110"),
                ),
            )
        )

        changed = self.evaluate(dataset=changed_data)

        self.assertNotEqual(original.dataset_id, changed.dataset_id)
        self.assertNotEqual(original.result_id, changed.result_id)

    def test_sequential_same_symbol_trades_in_later_test_fold_are_allowed(self) -> None:
        second_signal_index = 32
        second_order = LimitEntryOrder(
            symbol="RELIANCE",
            signal_session=SESSIONS[second_signal_index],
            first_eligible_session=SESSIONS[second_signal_index + 1],
            expiry_session=SESSIONS[second_signal_index + 1],
            quantity=100,
            limit_price=D("100"),
            tick_size=D("0.05"),
            maximum_participation=D("0.0025"),
        )
        second_intent = intent(
            signal_id="b" * 64,
            entry_order=second_order,
        )
        combined_bars = dataset().bars + (
            simulation_bar(SESSIONS[second_signal_index + 1]),
            simulation_bar(
                SESSIONS[second_signal_index + 2],
                open=D("102"),
                high=D("105"),
                low=D("101"),
                close=D("104"),
            ),
            simulation_bar(
                SESSIONS[second_signal_index + 3],
                open=D("108"),
                high=D("111"),
                low=D("107"),
                close=D("110"),
            ),
        )
        combined = dataset(bars=combined_bars)
        intents = tuple(
            sorted(
                (intent(), second_intent),
                key=lambda value: (value.entry_order.signal_session, value.intent_id),
            )
        )

        result = self.evaluate(dataset=combined, intents=intents)

        self.assertEqual(len(result.trades), 2)
        self.assertEqual(dict(result.metrics)["trade_count"], D("2"))

    def test_result_identity_detects_post_calculation_tampering(self) -> None:
        result = self.evaluate()
        object.__setattr__(result, "passed", False)

        with self.assertRaisesRegex(TrialEvaluationError, "content identity"):
            result.verify_content_identity()

    def test_dataset_identity_is_rechecked_before_evaluation(self) -> None:
        value = dataset()
        object.__setattr__(value, "bars", value.bars[:-1])

        with self.assertRaisesRegex(TrialEvaluationError, "dataset content identity"):
            self.evaluate(dataset=value)


if __name__ == "__main__":
    unittest.main()
