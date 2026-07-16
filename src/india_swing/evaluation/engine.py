from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, localcontext
from enum import Enum

from india_swing.execution.costs import (
    CostScheduleError,
    DeliveryChargeBreakdown,
    DeliveryFill,
    FillSide,
    NseDeliveryCostSchedule,
    calculate_equity_cash_charges,
)
from india_swing.execution.simulator import (
    LimitEntryOrder,
    ProtectiveExitOrder,
    SimulatedFill,
    SimulationBar,
    simulate_limit_entry,
    simulate_protective_exit,
    simulate_time_exit,
)
from india_swing.identity import content_id

from .models import PurgedWalkForwardPlan
from .trials import TrialRegistration


ZERO = Decimal("0")
ONE = Decimal("1")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
SUPPORTED_METRICS = frozenset(
    {
        "max_drawdown",
        "net_cagr",
        "net_profit",
        "net_return",
        "trade_count",
        "turnover",
    }
)
THRESHOLD_METRICS = SUPPORTED_METRICS - {"turnover"}


class TrialEvaluationError(ValueError):
    pass


class EvaluationDataReadiness(str, Enum):
    SYNTHETIC = "SYNTHETIC"
    COLLECTION_ONLY = "COLLECTION_ONLY"
    POINT_IN_TIME_VERIFIED = "POINT_IN_TIME_VERIFIED"


def _sha(value: str, name: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise TrialEvaluationError(f"{name} must be a full lowercase SHA-256")


def _decimal(value: Decimal, name: str, *, positive: bool = False) -> None:
    if type(value) is not Decimal or not value.is_finite():
        raise TrialEvaluationError(f"{name} must be a finite Decimal")
    if positive and value <= ZERO:
        raise TrialEvaluationError(f"{name} must be positive")


def _id_tuple(values: tuple[str, ...], name: str) -> None:
    if type(values) is not tuple or not values or values != tuple(sorted(set(values))):
        raise TrialEvaluationError(f"{name} must be a sorted unique tuple")
    for value in values:
        _sha(value, name)


@dataclass(frozen=True, slots=True)
class DailyExecutionPolicy:
    slippage_bps: Decimal
    maximum_participation: Decimal
    version: str = "daily-ohlcv-pessimistic/v1"
    policy_id: str = field(init=False)

    def __post_init__(self) -> None:
        _decimal(self.slippage_bps, "slippage_bps")
        _decimal(self.maximum_participation, "maximum_participation", positive=True)
        if self.maximum_participation > ONE:
            raise TrialEvaluationError("maximum_participation cannot exceed one")
        if not isinstance(self.version, str) or not self.version:
            raise TrialEvaluationError("execution policy version is required")
        object.__setattr__(
            self,
            "policy_id",
            self._calculated_id(),
        )

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "daily-execution-policy/v1",
                "slippage_bps": self.slippage_bps,
                "maximum_participation": self.maximum_participation,
                "version": self.version,
                "entry_timing": "STRICTLY_AFTER_SIGNAL_SESSION",
                "same_bar": "STOP_ASSUMED_TARGET_NOT_ASSUMED",
                "dual_touch": "STOP_FIRST",
                "gap_stop": "OPEN_THEN_ADVERSE_SLIPPAGE",
                "locked_sell": "NO_FILL",
                "partial_fills": "DISABLED",
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.policy_id != self._calculated_id():
            raise TrialEvaluationError("execution policy content identity failed")


@dataclass(frozen=True, slots=True)
class EvaluationDataset:
    sessions: tuple[date, ...]
    bars: tuple[SimulationBar, ...]
    source_snapshot_ids: tuple[str, ...]
    universe_snapshot_ids: tuple[str, ...]
    readiness: EvaluationDataReadiness
    dataset_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            type(self.sessions) is not tuple
            or not self.sessions
            or any(type(value) is not date for value in self.sessions)
            or self.sessions != tuple(sorted(set(self.sessions)))
        ):
            raise TrialEvaluationError("sessions must be sorted unique dates")
        if (
            type(self.bars) is not tuple
            or any(type(value) is not SimulationBar for value in self.bars)
            or self.bars
            != tuple(sorted(self.bars, key=lambda value: (value.session, value.symbol)))
        ):
            raise TrialEvaluationError("bars must be an ordered exact tuple")
        keys = tuple((value.session, value.symbol) for value in self.bars)
        if len(set(keys)) != len(keys):
            raise TrialEvaluationError("dataset cannot contain duplicate symbol/session bars")
        if any(value.session not in self.sessions for value in self.bars):
            raise TrialEvaluationError("bar session is outside the dataset calendar")
        _id_tuple(self.source_snapshot_ids, "source_snapshot_ids")
        _id_tuple(self.universe_snapshot_ids, "universe_snapshot_ids")
        if type(self.readiness) is not EvaluationDataReadiness:
            raise TrialEvaluationError("readiness must be exact")
        object.__setattr__(
            self,
            "dataset_id",
            self._calculated_id(),
        )

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "trial-evaluation-dataset/v1",
                "sessions": self.sessions,
                "bars": self.bars,
                "source_snapshot_ids": self.source_snapshot_ids,
                "universe_snapshot_ids": self.universe_snapshot_ids,
                "readiness": self.readiness,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.dataset_id != self._calculated_id():
            raise TrialEvaluationError("evaluation dataset content identity failed")


@dataclass(frozen=True, slots=True)
class EvaluationTradeIntent:
    signal_id: str
    universe_snapshot_id: str
    isin: str
    entry_order: LimitEntryOrder
    stop_price: Decimal
    target_price: Decimal
    max_holding_sessions: int
    intent_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha(self.signal_id, "signal_id")
        _sha(self.universe_snapshot_id, "universe_snapshot_id")
        if (
            not isinstance(self.isin, str)
            or len(self.isin) != 12
            or self.isin != self.isin.strip().upper()
        ):
            raise TrialEvaluationError("isin must be normalized 12-character text")
        if type(self.entry_order) is not LimitEntryOrder:
            raise TrialEvaluationError("entry_order must be exact")
        for value, name in (
            (self.stop_price, "stop_price"),
            (self.target_price, "target_price"),
        ):
            _decimal(value, name, positive=True)
            if value % self.entry_order.tick_size != ZERO:
                raise TrialEvaluationError(f"{name} must be an exact tick multiple")
        if not self.stop_price < self.entry_order.limit_price < self.target_price:
            raise TrialEvaluationError("intent prices must satisfy stop < limit < target")
        if type(self.max_holding_sessions) is not int or self.max_holding_sessions <= 0:
            raise TrialEvaluationError("max_holding_sessions must be positive")
        object.__setattr__(
            self,
            "intent_id",
            self._calculated_id(),
        )

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "trial-evaluation-trade-intent/v1",
                "signal_id": self.signal_id,
                "universe_snapshot_id": self.universe_snapshot_id,
                "isin": self.isin,
                "entry_order": self.entry_order,
                "stop_price": self.stop_price,
                "target_price": self.target_price,
                "max_holding_sessions": self.max_holding_sessions,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.intent_id != self._calculated_id():
            raise TrialEvaluationError("trade intent content identity failed")


@dataclass(frozen=True, slots=True)
class EvaluatedTrade:
    intent_id: str
    isin: str
    entry_fill: SimulatedFill
    exit_fill: SimulatedFill
    gross_pnl: Decimal = field(init=False)
    trade_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha(self.intent_id, "intent_id")
        if len(self.isin) != 12:
            raise TrialEvaluationError("isin must contain 12 characters")
        if self.entry_fill.side is not FillSide.BUY or self.exit_fill.side is not FillSide.SELL:
            raise TrialEvaluationError("evaluated trade requires buy then sell fills")
        if (
            self.entry_fill.symbol != self.exit_fill.symbol
            or self.entry_fill.quantity != self.exit_fill.quantity
            or self.exit_fill.session < self.entry_fill.session
        ):
            raise TrialEvaluationError("evaluated trade fills are inconsistent")
        gross = (
            self.exit_fill.fill_price - self.entry_fill.fill_price
        ) * self.entry_fill.quantity
        object.__setattr__(self, "gross_pnl", gross)
        object.__setattr__(
            self,
            "trade_id",
            content_id(
                {
                    "schema": "evaluated-swing-trade/v1",
                    "intent_id": self.intent_id,
                    "isin": self.isin,
                    "entry_fill": self.entry_fill,
                    "exit_fill": self.exit_fill,
                    "gross_pnl": gross,
                },
                length=64,
            ),
        )


@dataclass(frozen=True, slots=True)
class EquityPoint:
    session: date
    equity: Decimal
    drawdown: Decimal

    def __post_init__(self) -> None:
        if type(self.session) is not date:
            raise TrialEvaluationError("equity session must be a date")
        _decimal(self.equity, "equity")
        _decimal(self.drawdown, "drawdown")
        if self.drawdown > ZERO:
            raise TrialEvaluationError("drawdown cannot be positive")


@dataclass(frozen=True, slots=True)
class TrialEvaluationResult:
    trial_id: str
    split_plan_id: str
    dataset_id: str
    execution_policy_id: str
    cost_schedule_id: str
    initial_capital: Decimal
    trades: tuple[EvaluatedTrade, ...]
    charges: DeliveryChargeBreakdown | None
    equity_curve: tuple[EquityPoint, ...]
    metrics: tuple[tuple[str, Decimal], ...]
    pass_thresholds: tuple[tuple[str, Decimal], ...]
    passed: bool
    result_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.trial_id, "trial_id"),
            (self.split_plan_id, "split_plan_id"),
            (self.dataset_id, "dataset_id"),
            (self.execution_policy_id, "execution_policy_id"),
            (self.cost_schedule_id, "cost_schedule_id"),
        ):
            _sha(value, name)
        _decimal(self.initial_capital, "initial_capital", positive=True)
        if type(self.trades) is not tuple or self.trades != tuple(
            sorted(self.trades, key=lambda value: (value.entry_fill.session, value.intent_id))
        ):
            raise TrialEvaluationError("trades must be an ordered exact tuple")
        if self.charges is None:
            if self.trades:
                raise TrialEvaluationError("executed trades require calculated charges")
        elif type(self.charges) is not DeliveryChargeBreakdown:
            raise TrialEvaluationError("charges must be an exact breakdown")
        if (
            type(self.equity_curve) is not tuple
            or not self.equity_curve
            or tuple(point.session for point in self.equity_curve)
            != tuple(sorted({point.session for point in self.equity_curve}))
        ):
            raise TrialEvaluationError("equity curve must be ordered and unique")
        if (
            type(self.metrics) is not tuple
            or tuple(name for name, _ in self.metrics) != tuple(sorted(SUPPORTED_METRICS))
        ):
            raise TrialEvaluationError("result must contain the complete metric set")
        for _, value in self.metrics:
            _decimal(value, "metric")
        if (
            type(self.pass_thresholds) is not tuple
            or not self.pass_thresholds
            or tuple(name for name, _ in self.pass_thresholds)
            != tuple(sorted({name for name, _ in self.pass_thresholds}))
        ):
            raise TrialEvaluationError("pass thresholds must be sorted and unique")
        for name, value in self.pass_thresholds:
            if name not in THRESHOLD_METRICS:
                raise TrialEvaluationError("pass threshold has no supported direction")
            _decimal(value, "pass threshold")
        expected_final = self.initial_capital + sum(
            (trade.gross_pnl for trade in self.trades), ZERO
        ) - (self.charges.total if self.charges is not None else ZERO)
        if self.equity_curve[-1].equity != expected_final:
            raise TrialEvaluationError("final equity is inconsistent with fills and charges")
        peak = self.initial_capital
        for point in self.equity_curve:
            peak = max(peak, point.equity)
            expected_drawdown = point.equity / peak - ONE if peak > ZERO else Decimal("-1")
            if point.drawdown != expected_drawdown:
                raise TrialEvaluationError("equity drawdown is inconsistent")
        expected_metrics = _metric_values(
            self.initial_capital,
            self.trades,
            self.equity_curve,
        )
        if self.metrics != expected_metrics:
            raise TrialEvaluationError("metrics were not generated from evaluation evidence")
        expected_passed = all(
            dict(expected_metrics)[name] >= threshold
            for name, threshold in self.pass_thresholds
        )
        if type(self.passed) is not bool:
            raise TrialEvaluationError("passed must be bool")
        if self.passed != expected_passed:
            raise TrialEvaluationError("pass result is inconsistent with registered thresholds")
        object.__setattr__(self, "result_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "trial-evaluation-result/v1",
                "trial_id": self.trial_id,
                "split_plan_id": self.split_plan_id,
                "dataset_id": self.dataset_id,
                "execution_policy_id": self.execution_policy_id,
                "cost_schedule_id": self.cost_schedule_id,
                "initial_capital": self.initial_capital,
                "trades": self.trades,
                "charges": self.charges,
                "equity_curve": self.equity_curve,
                "metrics": self.metrics,
                "pass_thresholds": self.pass_thresholds,
                "passed": self.passed,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.result_id != self._calculated_id():
            raise TrialEvaluationError("evaluation result content identity failed")


def _metric_values(
    initial_capital: Decimal,
    trades: tuple[EvaluatedTrade, ...],
    equity_curve: tuple[EquityPoint, ...],
) -> tuple[tuple[str, Decimal], ...]:
    final_equity = equity_curve[-1].equity
    net_profit = final_equity - initial_capital
    net_return = net_profit / initial_capital
    turnover_value = sum(
        (
            trade.entry_fill.fill_price * trade.entry_fill.quantity
            + trade.exit_fill.fill_price * trade.exit_fill.quantity
            for trade in trades
        ),
        ZERO,
    )
    elapsed_days = max(1, (equity_curve[-1].session - equity_curve[0].session).days)
    if final_equity <= ZERO:
        net_cagr = Decimal("-1")
    else:
        with localcontext() as context:
            context.prec = 28
            net_cagr = (
                ((final_equity / initial_capital).ln() * Decimal(365) / elapsed_days).exp()
                - ONE
            )
    values = {
        "max_drawdown": min(point.drawdown for point in equity_curve),
        "net_cagr": net_cagr,
        "net_profit": net_profit,
        "net_return": net_return,
        "trade_count": Decimal(len(trades)),
        "turnover": turnover_value / initial_capital,
    }
    return tuple(sorted(values.items()))


class TrialEvaluationEngine:
    def evaluate(
        self,
        *,
        registration: TrialRegistration,
        split_plan: PurgedWalkForwardPlan,
        dataset: EvaluationDataset,
        intents: tuple[EvaluationTradeIntent, ...],
        execution_policy: DailyExecutionPolicy,
        cost_schedule: NseDeliveryCostSchedule,
        initial_capital: Decimal,
    ) -> TrialEvaluationResult:
        if type(registration) is not TrialRegistration:
            raise TypeError("registration must be exact")
        registration.verify_content_identity()
        if type(split_plan) is not PurgedWalkForwardPlan:
            raise TypeError("split_plan must be exact")
        split_plan.verify_content_identity()
        if type(dataset) is not EvaluationDataset:
            raise TypeError("dataset must be exact")
        dataset.verify_content_identity()
        if type(execution_policy) is not DailyExecutionPolicy:
            raise TypeError("execution_policy must be exact")
        execution_policy.verify_content_identity()
        if type(cost_schedule) is not NseDeliveryCostSchedule:
            raise TypeError("cost_schedule must be exact")
        _decimal(initial_capital, "initial_capital", positive=True)
        self._validate_bindings(
            registration,
            split_plan,
            dataset,
            execution_policy,
            cost_schedule,
        )
        if (
            type(intents) is not tuple
            or any(type(value) is not EvaluationTradeIntent for value in intents)
            or intents
            != tuple(
                sorted(
                    intents,
                    key=lambda value: (
                        value.entry_order.signal_session,
                        value.intent_id,
                    ),
                )
            )
        ):
            raise TrialEvaluationError("intents must be an ordered exact tuple")

        bars_by_symbol: dict[str, tuple[SimulationBar, ...]] = {}
        for symbol in {intent.entry_order.symbol for intent in intents}:
            bars_by_symbol[symbol] = tuple(
                bar for bar in dataset.bars if bar.symbol == symbol
            )
        trades: list[EvaluatedTrade] = []
        for intent in intents:
            intent.verify_content_identity()
            if intent.universe_snapshot_id not in dataset.universe_snapshot_ids:
                raise TrialEvaluationError("intent universe is absent from dataset binding")
            if intent.max_holding_sessions > registration.label_horizon_sessions:
                raise TrialEvaluationError(
                    "intent holding period exceeds registered label horizon"
                )
            fold = next(
                (
                    value
                    for value in split_plan.folds
                    if intent.entry_order.signal_session in value.test_sessions
                ),
                None,
            )
            if fold is None:
                raise TrialEvaluationError(
                    "intent signal session is outside preregistered test windows"
                )
            if (
                intent.entry_order.first_eligible_session not in fold.test_sessions
                or intent.entry_order.expiry_session not in fold.test_sessions
            ):
                raise TrialEvaluationError(
                    "entry window must remain inside its preregistered test fold"
                )
            if (
                intent.entry_order.maximum_participation
                != execution_policy.maximum_participation
            ):
                raise TrialEvaluationError(
                    "entry participation differs from execution policy"
                )
            trade = self._evaluate_intent(
                intent,
                bars_by_symbol.get(intent.entry_order.symbol, ()),
                dataset.sessions,
                execution_policy,
            )
            if trade is not None:
                if (
                    trade.entry_fill.session not in fold.test_sessions
                    or trade.exit_fill.session not in fold.test_sessions
                ):
                    raise TrialEvaluationError(
                        "evaluated trade crossed its preregistered test-fold boundary"
                    )
                trades.append(trade)

        ordered_trades = tuple(
            sorted(trades, key=lambda value: (value.entry_fill.session, value.intent_id))
        )
        last_exit_by_symbol: dict[str, date] = {}
        for trade in ordered_trades:
            symbol = trade.entry_fill.symbol
            if trade.entry_fill.session <= last_exit_by_symbol.get(symbol, date.min):
                raise TrialEvaluationError("same-symbol evaluated positions overlap")
            last_exit_by_symbol[symbol] = trade.exit_fill.session
        delivery_fills = tuple(
            fill
            for trade in ordered_trades
            for fill in (
                DeliveryFill(
                    trade_date=trade.entry_fill.session,
                    symbol=trade.entry_fill.symbol,
                    isin=trade.isin,
                    side=FillSide.BUY,
                    quantity=trade.entry_fill.quantity,
                    price=trade.entry_fill.fill_price,
                    order_id=trade.entry_fill.order_id,
                ),
                DeliveryFill(
                    trade_date=trade.exit_fill.session,
                    symbol=trade.exit_fill.symbol,
                    isin=trade.isin,
                    side=FillSide.SELL,
                    quantity=trade.exit_fill.quantity,
                    price=trade.exit_fill.fill_price,
                    order_id=trade.exit_fill.order_id,
                ),
            )
        )
        try:
            charges = (
                calculate_equity_cash_charges(delivery_fills, cost_schedule)
                if delivery_fills
                else None
            )
        except CostScheduleError as exc:
            raise TrialEvaluationError(
                "execution cannot be priced by the registered delivery schedule"
            ) from exc
        test_sessions = tuple(
            session
            for fold in split_plan.folds
            for session in fold.test_sessions
        )
        first_test_index = dataset.sessions.index(min(test_sessions))
        last_test_index = dataset.sessions.index(max(test_sessions))
        performance_sessions = dataset.sessions[first_test_index : last_test_index + 1]
        equity_curve = self._equity_curve(
            dataset,
            performance_sessions,
            ordered_trades,
            charges,
            initial_capital,
        )
        metrics = _metric_values(initial_capital, ordered_trades, equity_curve)
        metric_map = dict(metrics)
        passed = all(
            metric_map[name] >= threshold
            for name, threshold in registration.pass_thresholds
        )
        return TrialEvaluationResult(
            trial_id=registration.trial_id,
            split_plan_id=split_plan.plan_id,
            dataset_id=dataset.dataset_id,
            execution_policy_id=execution_policy.policy_id,
            cost_schedule_id=cost_schedule.schedule_id,
            initial_capital=initial_capital,
            trades=ordered_trades,
            charges=charges,
            equity_curve=equity_curve,
            metrics=metrics,
            pass_thresholds=registration.pass_thresholds,
            passed=passed,
        )

    @staticmethod
    def _validate_bindings(
        registration: TrialRegistration,
        split_plan: PurgedWalkForwardPlan,
        dataset: EvaluationDataset,
        execution_policy: DailyExecutionPolicy,
        cost_schedule: NseDeliveryCostSchedule,
    ) -> None:
        requested = {registration.primary_metric, *registration.secondary_metrics}
        if not requested.issubset(SUPPORTED_METRICS):
            raise TrialEvaluationError("registration requests an unsupported metric")
        if any(
            name not in THRESHOLD_METRICS
            for name, _ in registration.pass_thresholds
        ):
            raise TrialEvaluationError(
                "registration uses an unsupported threshold direction"
            )
        if registration.execution_policy_version != execution_policy.version:
            raise TrialEvaluationError("execution policy version binding failed")
        if registration.execution_policy_hash != execution_policy.policy_id:
            raise TrialEvaluationError("execution policy content binding failed")
        if registration.base_slippage_bps != execution_policy.slippage_bps:
            raise TrialEvaluationError("registered base slippage binding failed")
        if registration.cost_schedule_version != cost_schedule.policy_version:
            raise TrialEvaluationError("cost schedule version binding failed")
        if registration.cost_schedule_hash != cost_schedule.schedule_id:
            raise TrialEvaluationError("cost schedule content binding failed")
        if registration.data_snapshot_ids != dataset.source_snapshot_ids:
            raise TrialEvaluationError("data snapshot binding failed")
        if registration.universe_snapshot_ids != dataset.universe_snapshot_ids:
            raise TrialEvaluationError("universe snapshot binding failed")
        if registration.synthetic:
            if dataset.readiness is not EvaluationDataReadiness.SYNTHETIC:
                raise TrialEvaluationError("synthetic trial requires a synthetic dataset")
        elif dataset.readiness is not EvaluationDataReadiness.POINT_IN_TIME_VERIFIED:
            raise TrialEvaluationError(
                "reportable trial requires point-in-time verified data"
            )
        if registration.split_plan_id != split_plan.plan_id:
            raise TrialEvaluationError("walk-forward split-plan binding failed")
        if registration.label_horizon_sessions != split_plan.label_horizon_sessions:
            raise TrialEvaluationError("registered label horizon differs from split plan")
        if dataset.sessions != split_plan.ordered_sessions:
            raise TrialEvaluationError("dataset calendar differs from split plan")
        if (
            dataset.sessions[0] < registration.evaluation_start
            or dataset.sessions[-1] > registration.evaluation_end
        ):
            raise TrialEvaluationError(
                "dataset sessions exceed registered evaluation dates"
            )

    @staticmethod
    def _evaluate_intent(
        intent: EvaluationTradeIntent,
        bars: tuple[SimulationBar, ...],
        sessions: tuple[date, ...],
        policy: DailyExecutionPolicy,
    ) -> EvaluatedTrade | None:
        by_session = {bar.session: bar for bar in bars}
        entry_fill: SimulatedFill | None = None
        for session in sessions:
            if session < intent.entry_order.first_eligible_session:
                continue
            if session > intent.entry_order.expiry_session:
                break
            bar = by_session.get(session)
            if bar is None:
                raise TrialEvaluationError("entry window contains a missing daily bar")
            entry_fill = simulate_limit_entry(
                intent.entry_order,
                bar,
                slippage_bps=policy.slippage_bps,
            )
            if entry_fill is not None:
                break
        if entry_fill is None:
            return None
        entry_index = sessions.index(entry_fill.session)
        horizon_index = entry_index + intent.max_holding_sessions - 1
        if horizon_index >= len(sessions):
            raise TrialEvaluationError("dataset does not mature the full holding horizon")
        exit_order = ProtectiveExitOrder(
            symbol=entry_fill.symbol,
            quantity=entry_fill.quantity,
            entry_session=entry_fill.session,
            entry_price=entry_fill.fill_price,
            stop_price=intent.stop_price,
            target_price=intent.target_price,
            tick_size=intent.entry_order.tick_size,
            maximum_participation=policy.maximum_participation,
        )
        exit_fill: SimulatedFill | None = None
        for index in range(entry_index, horizon_index + 1):
            session = sessions[index]
            bar = by_session.get(session)
            if bar is None:
                raise TrialEvaluationError("holding window contains a missing daily bar")
            exit_fill = simulate_protective_exit(
                exit_order,
                bar,
                slippage_bps=policy.slippage_bps,
            )
            if exit_fill is not None:
                break
            if index == horizon_index:
                exit_fill = simulate_time_exit(
                    exit_order,
                    bar,
                    slippage_bps=policy.slippage_bps,
                )
        if exit_fill is None:
            raise TrialEvaluationError("position could not be liquidated by its horizon")
        return EvaluatedTrade(
            intent_id=intent.intent_id,
            isin=intent.isin,
            entry_fill=entry_fill,
            exit_fill=exit_fill,
        )

    @staticmethod
    def _equity_curve(
        dataset: EvaluationDataset,
        performance_sessions: tuple[date, ...],
        trades: tuple[EvaluatedTrade, ...],
        charges: DeliveryChargeBreakdown | None,
        initial_capital: Decimal,
    ) -> tuple[EquityPoint, ...]:
        entries: dict[date, list[EvaluatedTrade]] = {}
        exits: dict[date, list[EvaluatedTrade]] = {}
        for trade in trades:
            entries.setdefault(trade.entry_fill.session, []).append(trade)
            exits.setdefault(trade.exit_fill.session, []).append(trade)
        charges_by_session = (
            {leg.trade_date: leg.total for leg in charges.legs}
            if charges is not None
            else {}
        )
        bar_map = {(bar.session, bar.symbol): bar for bar in dataset.bars}
        cash = initial_capital
        positions: dict[str, tuple[int, str]] = {}
        peak = initial_capital
        points: list[EquityPoint] = []
        for session in performance_sessions:
            for trade in entries.get(session, ()):
                symbol = trade.entry_fill.symbol
                if symbol in positions:
                    raise TrialEvaluationError("overlapping positions are unsupported")
                cash -= trade.entry_fill.fill_price * trade.entry_fill.quantity
                positions[symbol] = (trade.entry_fill.quantity, trade.intent_id)
            for trade in exits.get(session, ()):
                symbol = trade.exit_fill.symbol
                held = positions.pop(symbol, None)
                if held != (trade.exit_fill.quantity, trade.intent_id):
                    raise TrialEvaluationError("exit does not match an open position")
                cash += trade.exit_fill.fill_price * trade.exit_fill.quantity
            cash -= charges_by_session.get(session, ZERO)
            if cash < ZERO:
                raise TrialEvaluationError(
                    "executed entries and charges exceed available cash"
                )
            market_value = ZERO
            for symbol, (quantity, _) in positions.items():
                bar = bar_map.get((session, symbol))
                if bar is None:
                    raise TrialEvaluationError(
                        "open position has no mark-to-market bar"
                    )
                market_value += bar.close * quantity
            equity = cash + market_value
            peak = max(peak, equity)
            drawdown = equity / peak - ONE if peak > ZERO else Decimal("-1")
            points.append(EquityPoint(session=session, equity=equity, drawdown=drawdown))
        if positions:
            raise TrialEvaluationError("evaluation ended with open positions")
        return tuple(points)
