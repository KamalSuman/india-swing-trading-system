from __future__ import annotations

import re
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum

from india_swing.identity import content_id


ZERO = Decimal("0")
ONE = Decimal("1")
INDIA_STANDARD_TIME = timezone(timedelta(hours=5, minutes=30))
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class CalibrationError(ValueError):
    pass


def _sha(value: str, name: str) -> None:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise CalibrationError(f"{name} must be a lowercase SHA-256")


def _utc(value: datetime, name: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is None:
        raise CalibrationError(f"{name} must be timezone-aware")
    try:
        offset = value.utcoffset()
    except Exception:
        raise CalibrationError(f"{name} has invalid timezone behavior") from None
    if offset is None:
        raise CalibrationError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _decimal(value: Decimal, name: str) -> None:
    if type(value) is not Decimal or not value.is_finite():
        raise CalibrationError(f"{name} must be a finite Decimal")


class CalibrationPartition(str, Enum):
    TRAIN = "TRAIN"
    VALIDATION = "VALIDATION"
    TEST = "TEST"


class CalibrationOutcome(str, Enum):
    TARGET = "TARGET"
    STOP = "STOP"
    TIME = "TIME"


@dataclass(frozen=True, slots=True)
class CalibrationObservation:
    signal_config_id: str
    source_trial_id: str
    source_result_id: str
    source_completion_event_id: str
    source_trade_id: str
    signal_id: str
    signal_session: date
    resolved_session: date
    known_at: datetime
    partition: CalibrationPartition
    outcome: CalibrationOutcome
    realized_time_exit_r: Decimal | None
    observation_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.signal_config_id, "signal_config_id"),
            (self.source_trial_id, "source_trial_id"),
            (self.source_result_id, "source_result_id"),
            (self.source_completion_event_id, "source_completion_event_id"),
            (self.source_trade_id, "source_trade_id"),
            (self.signal_id, "signal_id"),
        ):
            _sha(value, name)
        if type(self.signal_session) is not date or type(self.resolved_session) is not date:
            raise CalibrationError("calibration sessions must be dates")
        if self.resolved_session < self.signal_session:
            raise CalibrationError("outcome cannot resolve before its signal")
        object.__setattr__(self, "known_at", _utc(self.known_at, "known_at"))
        if self.resolved_session > self.known_at.astimezone(INDIA_STANDARD_TIME).date():
            raise CalibrationError("outcome cannot be known before its resolved session")
        if type(self.partition) is not CalibrationPartition:
            raise CalibrationError("calibration partition must be exact")
        if type(self.outcome) is not CalibrationOutcome:
            raise CalibrationError("calibration outcome must be exact")
        if self.outcome is CalibrationOutcome.TIME:
            if self.realized_time_exit_r is None:
                raise CalibrationError("time exits require realized_time_exit_r")
            _decimal(self.realized_time_exit_r, "realized_time_exit_r")
        elif self.realized_time_exit_r is not None:
            raise CalibrationError("target and stop outcomes cannot carry time-exit R")
        object.__setattr__(self, "observation_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "observation_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.observation_id != self._calculated_id():
            raise CalibrationError("calibration observation content identity failed")


@dataclass(frozen=True, slots=True)
class WalkForwardCalibrationPlan:
    registered_at: datetime
    signal_config_id: str
    source_trial_ids: tuple[str, ...]
    minimum_sample_size: int = 100
    adverse_stop_prior_trades: int = 10
    method: str = "TEST_ONLY_ADVERSE_STOP_PRIOR/v1"
    plan_id: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "registered_at", _utc(self.registered_at, "registered_at"))
        _sha(self.signal_config_id, "signal_config_id")
        if (
            type(self.source_trial_ids) is not tuple
            or not self.source_trial_ids
            or self.source_trial_ids != tuple(sorted(set(self.source_trial_ids)))
        ):
            raise CalibrationError("source trial IDs must be sorted, unique, and non-empty")
        for value in self.source_trial_ids:
            _sha(value, "source_trial_id")
        if type(self.minimum_sample_size) is not int or self.minimum_sample_size < 100:
            raise CalibrationError("minimum sample size cannot be below 100")
        if type(self.adverse_stop_prior_trades) is not int or self.adverse_stop_prior_trades < 0:
            raise CalibrationError("adverse stop prior must be a non-negative integer")
        if self.method != "TEST_ONLY_ADVERSE_STOP_PRIOR/v1":
            raise CalibrationError("unsupported calibration method")
        object.__setattr__(self, "plan_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "plan_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.plan_id != self._calculated_id():
            raise CalibrationError("calibration plan content identity failed")


@dataclass(frozen=True, slots=True)
class WalkForwardCalibration:
    plan: WalkForwardCalibrationPlan
    plan_id: str
    signal_config_id: str
    cutoff: datetime
    source_trial_ids: tuple[str, ...]
    observations: tuple[CalibrationObservation, ...]
    observation_ids: tuple[str, ...]
    sample_size: int
    target_count: int
    stop_count: int
    time_count: int
    adverse_stop_prior_trades: int
    target_probability: Decimal
    stop_probability: Decimal
    expected_time_exit_r: Decimal
    method: str = "TEST_ONLY_ADVERSE_STOP_PRIOR/v1"
    schema_version: str = "walk-forward-calibration/v1"
    calibration_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.plan) is not WalkForwardCalibrationPlan:
            raise CalibrationError("calibration plan must be exact")
        self.plan.verify_content_identity()
        for value, name in (
            (self.plan_id, "plan_id"),
            (self.signal_config_id, "signal_config_id"),
        ):
            _sha(value, name)
        object.__setattr__(self, "cutoff", _utc(self.cutoff, "calibration cutoff"))
        if (
            self.plan_id != self.plan.plan_id
            or self.signal_config_id != self.plan.signal_config_id
            or self.source_trial_ids != self.plan.source_trial_ids
            or self.adverse_stop_prior_trades != self.plan.adverse_stop_prior_trades
            or self.method != self.plan.method
        ):
            raise CalibrationError("calibration differs from its preregistered plan")
        if self.plan.registered_at >= self.cutoff:
            raise CalibrationError("calibration plan must predate its cutoff")
        if self.source_trial_ids != tuple(sorted(set(self.source_trial_ids))):
            raise CalibrationError("calibration source trials must be sorted and unique")
        for value in self.source_trial_ids:
            _sha(value, "source_trial_id")
        if (
            type(self.observations) is not tuple
            or not self.observations
            or any(type(value) is not CalibrationObservation for value in self.observations)
            or self.observations
            != tuple(sorted(self.observations, key=lambda value: value.observation_id))
        ):
            raise CalibrationError("calibration observations must be an ordered exact tuple")
        for value in self.observations:
            value.verify_content_identity()
            if (
                value.signal_config_id != self.signal_config_id
                or value.partition is not CalibrationPartition.TEST
                or value.known_at > self.cutoff
                or value.known_at <= self.plan.registered_at
            ):
                raise CalibrationError("calibration observation violates its plan or cutoff")
        if tuple(sorted({value.source_trial_id for value in self.observations})) != self.source_trial_ids:
            raise CalibrationError("calibration observations omit a source trial")
        if len({value.source_trade_id for value in self.observations}) != len(self.observations):
            raise CalibrationError("calibration contains a duplicate trade")
        if len({value.signal_id for value in self.observations}) != len(self.observations):
            raise CalibrationError("calibration contains a duplicate signal")
        for trial_id in self.source_trial_ids:
            trial_values = tuple(
                value for value in self.observations if value.source_trial_id == trial_id
            )
            if (
                len({value.source_result_id for value in trial_values}) != 1
                or len({value.source_completion_event_id for value in trial_values}) != 1
            ):
                raise CalibrationError(
                    "each source trial requires one completed evaluation result"
                )
        if (
            type(self.observation_ids) is not tuple
            or self.observation_ids != tuple(sorted(set(self.observation_ids)))
        ):
            raise CalibrationError("observation IDs must be sorted and unique")
        for value in self.observation_ids:
            _sha(value, "observation_id")
        if self.observation_ids != tuple(value.observation_id for value in self.observations):
            raise CalibrationError("observation IDs differ from calibration observations")
        for value, name in (
            (self.sample_size, "sample_size"),
            (self.target_count, "target_count"),
            (self.stop_count, "stop_count"),
            (self.time_count, "time_count"),
            (self.adverse_stop_prior_trades, "adverse_stop_prior_trades"),
        ):
            if type(value) is not int or value < 0:
                raise CalibrationError(f"{name} must be a non-negative integer")
        if self.sample_size < self.plan.minimum_sample_size:
            raise CalibrationError("validated calibration is below its planned sample size")
        if self.sample_size != len(self.observations):
            raise CalibrationError("sample size differs from calibration observations")
        if self.target_count + self.stop_count + self.time_count != self.sample_size:
            raise CalibrationError("calibration outcome counts differ from sample size")
        actual_target = sum(
            value.outcome is CalibrationOutcome.TARGET for value in self.observations
        )
        actual_stop = sum(
            value.outcome is CalibrationOutcome.STOP for value in self.observations
        )
        actual_time_values = tuple(
            value.realized_time_exit_r
            for value in self.observations
            if value.outcome is CalibrationOutcome.TIME
        )
        if (self.target_count, self.stop_count, self.time_count) != (
            actual_target,
            actual_stop,
            len(actual_time_values),
        ):
            raise CalibrationError("calibration counts differ from its observations")
        denominator = Decimal(self.sample_size + self.adverse_stop_prior_trades)
        expected_target = Decimal(self.target_count) / denominator
        expected_stop = Decimal(self.stop_count + self.adverse_stop_prior_trades) / denominator
        for value, expected, name in (
            (self.target_probability, expected_target, "target_probability"),
            (self.stop_probability, expected_stop, "stop_probability"),
        ):
            _decimal(value, name)
            if value != expected or not ZERO <= value <= ONE:
                raise CalibrationError(f"{name} differs from the registered method")
        if self.target_probability + self.stop_probability > ONE:
            raise CalibrationError("calibrated target and stop probabilities exceed one")
        _decimal(self.expected_time_exit_r, "expected_time_exit_r")
        actual_time_r = (
            sum(actual_time_values, ZERO) / Decimal(len(actual_time_values))
            if actual_time_values
            else ZERO
        )
        if self.expected_time_exit_r != actual_time_r:
            raise CalibrationError("time-exit R differs from calibration observations")
        if self.method != "TEST_ONLY_ADVERSE_STOP_PRIOR/v1":
            raise CalibrationError("unsupported calibration method")
        if self.schema_version != "walk-forward-calibration/v1":
            raise CalibrationError("unsupported calibration schema")
        object.__setattr__(self, "calibration_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "calibration_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self.plan.verify_content_identity()
        for value in self.observations:
            value.verify_content_identity()
        if self.calibration_id != self._calculated_id():
            raise CalibrationError("walk-forward calibration content identity failed")


def build_walk_forward_calibration(
    *,
    plan: WalkForwardCalibrationPlan,
    observations: tuple[CalibrationObservation, ...],
    cutoff: datetime,
) -> WalkForwardCalibration:
    if type(plan) is not WalkForwardCalibrationPlan:
        raise CalibrationError("plan must be exact")
    if (
        type(observations) is not tuple
        or not observations
        or any(type(value) is not CalibrationObservation for value in observations)
    ):
        raise CalibrationError("observations must be a non-empty exact tuple")
    plan.verify_content_identity()
    cutoff = _utc(cutoff, "calibration cutoff")
    if plan.registered_at >= cutoff:
        raise CalibrationError("calibration plan must predate its cutoff")
    for value in observations:
        value.verify_content_identity()
        if value.signal_config_id != plan.signal_config_id:
            raise CalibrationError("observation uses another signal configuration")
        if value.partition is not CalibrationPartition.TEST:
            raise CalibrationError("only untouched test-partition outcomes may calibrate")
        if value.known_at > cutoff:
            raise CalibrationError("calibration contains future-known outcomes")
        if value.known_at <= plan.registered_at:
            raise CalibrationError("calibration plan was registered after an outcome was known")
    if len({value.observation_id for value in observations}) != len(observations):
        raise CalibrationError("calibration observations contain duplicates")
    if len({value.source_trade_id for value in observations}) != len(observations):
        raise CalibrationError("one evaluated trade cannot be counted twice")
    if len({value.signal_id for value in observations}) != len(observations):
        raise CalibrationError("one signal cannot be counted twice")
    observed_trials = tuple(sorted({value.source_trial_id for value in observations}))
    if observed_trials != plan.source_trial_ids:
        raise CalibrationError("calibration does not cover every preregistered source trial")
    for trial_id in plan.source_trial_ids:
        trial_values = tuple(
            value for value in observations if value.source_trial_id == trial_id
        )
        if (
            len({value.source_result_id for value in trial_values}) != 1
            or len({value.source_completion_event_id for value in trial_values}) != 1
        ):
            raise CalibrationError(
                "each source trial requires one completed evaluation result"
            )
    if len(observations) < plan.minimum_sample_size:
        raise CalibrationError("calibration sample is smaller than its preregistered minimum")

    target_count = sum(value.outcome is CalibrationOutcome.TARGET for value in observations)
    stop_count = sum(value.outcome is CalibrationOutcome.STOP for value in observations)
    time_values = tuple(
        value.realized_time_exit_r
        for value in observations
        if value.outcome is CalibrationOutcome.TIME
    )
    time_count = len(time_values)
    expected_time_exit_r = (
        sum(time_values, ZERO) / Decimal(time_count) if time_values else ZERO
    )
    denominator = Decimal(len(observations) + plan.adverse_stop_prior_trades)
    return WalkForwardCalibration(
        plan=plan,
        plan_id=plan.plan_id,
        signal_config_id=plan.signal_config_id,
        cutoff=cutoff,
        source_trial_ids=plan.source_trial_ids,
        observations=tuple(sorted(observations, key=lambda value: value.observation_id)),
        observation_ids=tuple(sorted(value.observation_id for value in observations)),
        sample_size=len(observations),
        target_count=target_count,
        stop_count=stop_count,
        time_count=time_count,
        adverse_stop_prior_trades=plan.adverse_stop_prior_trades,
        target_probability=Decimal(target_count) / denominator,
        stop_probability=Decimal(stop_count + plan.adverse_stop_prior_trades) / denominator,
        expected_time_exit_r=expected_time_exit_r,
    )


def observations_from_evaluation_comparison(
    *,
    signal_config_id: str,
    comparison: object,
    strategy_batch: object,
    split_plan: object,
    completion: object,
) -> tuple[CalibrationObservation, ...]:
    """Derive test-only outcomes from one completed engine comparison.

    Local imports keep the signal contract independent from evaluation storage.
    The exact type and identity checks prevent callers from relabeling arbitrary
    outcomes as test observations.
    """

    from india_swing.evaluation.baselines import (
        GeneratedIntentBatch,
        GeneratedIntentRole,
    )
    from india_swing.evaluation.engine import TrialEvaluationComparisonResult
    from india_swing.evaluation.lifecycle import (
        TrialLifecycleEvent,
        TrialLifecycleEventType,
    )
    from india_swing.evaluation.models import PurgedWalkForwardPlan
    from india_swing.execution.simulator import ExitReason

    _sha(signal_config_id, "signal_config_id")
    if type(comparison) is not TrialEvaluationComparisonResult:
        raise CalibrationError("comparison must be exact")
    if type(strategy_batch) is not GeneratedIntentBatch:
        raise CalibrationError("strategy batch must be exact")
    if type(split_plan) is not PurgedWalkForwardPlan:
        raise CalibrationError("split plan must be exact")
    if type(completion) is not TrialLifecycleEvent:
        raise CalibrationError("completion must be exact")
    comparison.verify_content_identity()
    strategy_batch.verify_content_identity()
    split_plan.verify_content_identity()
    completion.verify_content_identity()
    result = comparison.strategy_base
    if (
        comparison.strategy_id != signal_config_id
        or strategy_batch.generator_id != signal_config_id
        or strategy_batch.role is not GeneratedIntentRole.STRATEGY
    ):
        raise CalibrationError("evaluation source uses another signal configuration")
    if (
        result.trial_id != comparison.trial_id
        or completion.trial_id != comparison.trial_id
        or completion.event_type is not TrialLifecycleEventType.TRIAL_COMPLETED
        or completion.evaluation_result_id != comparison.comparison_id
        or completion.passed != comparison.passed
    ):
        raise CalibrationError("completion does not bind the evaluation comparison")
    expected_metrics = tuple(sorted(result.metrics + comparison.comparison_metrics))
    if completion.metrics != expected_metrics:
        raise CalibrationError("completion metrics differ from the comparison")
    if (
        result.split_plan_id != split_plan.plan_id
        or strategy_batch.split_plan_id != split_plan.plan_id
    ):
        raise CalibrationError("evaluation source uses another split plan")
    test_sessions = {
        session for fold in split_plan.folds for session in fold.test_sessions
    }
    intents = {value.intent_id: value for value in strategy_batch.intents}
    if len(intents) != len(strategy_batch.intents):
        raise CalibrationError("strategy batch contains duplicate intent IDs")
    if not result.trades:
        return ()
    if result.charges is None:
        raise CalibrationError("executed calibration trades require exact charges")
    total_turnover = sum(
        (
            (trade.entry_fill.fill_price + trade.exit_fill.fill_price)
            * Decimal(trade.entry_fill.quantity)
            for trade in result.trades
        ),
        ZERO,
    )
    if total_turnover <= ZERO:
        raise CalibrationError("calibration turnover must be positive")

    observations: list[CalibrationObservation] = []
    for trade in result.trades:
        intent = intents.get(trade.intent_id)
        if intent is None:
            raise CalibrationError("evaluated trade is absent from the strategy batch")
        signal_session = intent.entry_order.signal_session
        if signal_session not in test_sessions:
            raise CalibrationError("evaluated signal is outside every test partition")
        reason = trade.exit_fill.exit_reason
        if reason is ExitReason.TARGET:
            outcome = CalibrationOutcome.TARGET
            time_r = None
        elif reason is ExitReason.STOP:
            outcome = CalibrationOutcome.STOP
            time_r = None
        elif reason is ExitReason.TIME:
            outcome = CalibrationOutcome.TIME
            trade_turnover = (
                trade.entry_fill.fill_price + trade.exit_fill.fill_price
            ) * Decimal(trade.entry_fill.quantity)
            allocated_cost = result.charges.total * trade_turnover / total_turnover
            gross_risk = (
                trade.entry_fill.fill_price - intent.stop_price
            ) * Decimal(trade.entry_fill.quantity)
            net_risk = gross_risk + allocated_cost
            if net_risk <= ZERO:
                raise CalibrationError("time-exit trade has non-positive calibrated risk")
            time_r = (trade.gross_pnl - allocated_cost) / net_risk
        else:
            raise CalibrationError("evaluated exit reason is unavailable")
        if trade.exit_fill.session > completion.occurred_at.astimezone(
            INDIA_STANDARD_TIME
        ).date():
            raise CalibrationError("completion predates an evaluated exit")
        observations.append(
            CalibrationObservation(
                signal_config_id=signal_config_id,
                source_trial_id=comparison.trial_id,
                source_result_id=comparison.comparison_id,
                source_completion_event_id=completion.event_id,
                source_trade_id=trade.trade_id,
                signal_id=intent.signal_id,
                signal_session=signal_session,
                resolved_session=trade.exit_fill.session,
                known_at=completion.occurred_at,
                partition=CalibrationPartition.TEST,
                outcome=outcome,
                realized_time_exit_r=time_r,
            )
        )
    return tuple(sorted(observations, key=lambda value: value.observation_id))
