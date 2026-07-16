from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from dataclasses import fields, is_dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.execution.costs import (
    DeliveryChargeBreakdown,
    DeliveryLegCharges,
    FillSide,
)
from india_swing.execution.simulator import ExitReason, SimulatedFill

from .engine import (
    EquityPoint,
    EvaluatedTrade,
    TrialEvaluationError,
    TrialEvaluationResult,
)
from .trial_store import LocalTrialRegistry


TRIAL_EVALUATION_RESULT_STORE_SCHEMA_VERSION = "local-trial-evaluation-result/v1"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAX_RESULT_BYTES = 32 * 1024 * 1024


class TrialEvaluationResultConflict(TrialEvaluationError):
    pass


class TrialEvaluationResultNotFound(TrialEvaluationError):
    pass


def _is_link_like(path: Path) -> bool:
    try:
        status = os.lstat(path)
    except OSError:
        return path.is_symlink()
    reparse_attribute = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return path.is_symlink() or bool(
        getattr(status, "st_file_attributes", 0) & reparse_attribute
    )


def _json_value(value: object) -> object:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if is_dataclass(value):
        return {item.name: _json_value(getattr(value, item.name)) for item in fields(value)}
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    if value is None or isinstance(value, (str, int, bool)):
        return value
    raise TypeError(f"unsupported evaluation-result value: {type(value).__name__}")


def encode_trial_evaluation_result(result: TrialEvaluationResult) -> bytes:
    if type(result) is not TrialEvaluationResult:
        raise TypeError("result must be an exact TrialEvaluationResult")
    result.verify_content_identity()
    payload = {
        "store_schema_version": TRIAL_EVALUATION_RESULT_STORE_SCHEMA_VERSION,
        "result": _json_value(result),
    }
    return (
        json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise TrialEvaluationError("evaluation result contains a duplicate JSON key")
        result[key] = value
    return result


def _object(value: object, expected: set[str], name: str) -> dict[str, object]:
    if type(value) is not dict or set(value) != expected:
        raise TrialEvaluationError(f"stored {name} has invalid fields")
    return value


def _decimal(value: object, name: str) -> Decimal:
    if type(value) is not str:
        raise TrialEvaluationError(f"stored {name} must be a Decimal string")
    try:
        result = Decimal(value)
    except InvalidOperation as exc:
        raise TrialEvaluationError(f"stored {name} is not a Decimal") from exc
    if not result.is_finite():
        raise TrialEvaluationError(f"stored {name} must be finite")
    return result


def _date(value: object, name: str) -> date:
    if type(value) is not str:
        raise TrialEvaluationError(f"stored {name} must be an ISO date")
    try:
        result = date.fromisoformat(value)
    except ValueError as exc:
        raise TrialEvaluationError(f"stored {name} is not an ISO date") from exc
    if result.isoformat() != value:
        raise TrialEvaluationError(f"stored {name} is not a canonical ISO date")
    return result


def _pairs(value: object, name: str) -> tuple[tuple[str, Decimal], ...]:
    if type(value) is not list:
        raise TrialEvaluationError(f"stored {name} must be a list")
    result: list[tuple[str, Decimal]] = []
    for item in value:
        if type(item) is not list or len(item) != 2 or type(item[0]) is not str:
            raise TrialEvaluationError(f"stored {name} entry is invalid")
        result.append((item[0], _decimal(item[1], name)))
    return tuple(result)


def _decode_fill(value: object) -> SimulatedFill:
    raw = _object(
        value,
        {item.name for item in fields(SimulatedFill)},
        "simulated fill",
    )
    stored_id = raw["fill_id"]
    fill = SimulatedFill(
        order_id=raw["order_id"],
        bar_id=raw["bar_id"],
        session=_date(raw["session"], "fill.session"),
        symbol=raw["symbol"],
        side=FillSide(raw["side"]),
        quantity=raw["quantity"],
        trigger_price=_decimal(raw["trigger_price"], "fill.trigger_price"),
        fill_price=_decimal(raw["fill_price"], "fill.fill_price"),
        slippage_bps=_decimal(raw["slippage_bps"], "fill.slippage_bps"),
        exit_reason=(
            None if raw["exit_reason"] is None else ExitReason(raw["exit_reason"])
        ),
    )
    if fill.fill_id != stored_id:
        raise TrialEvaluationError("stored simulated fill ID does not match content")
    return fill


def _decode_trade(value: object) -> EvaluatedTrade:
    raw = _object(
        value,
        {item.name for item in fields(EvaluatedTrade)},
        "evaluated trade",
    )
    stored_gross_pnl = _decimal(raw["gross_pnl"], "trade.gross_pnl")
    stored_id = raw["trade_id"]
    trade = EvaluatedTrade(
        intent_id=raw["intent_id"],
        isin=raw["isin"],
        entry_fill=_decode_fill(raw["entry_fill"]),
        exit_fill=_decode_fill(raw["exit_fill"]),
    )
    if trade.gross_pnl != stored_gross_pnl or trade.trade_id != stored_id:
        raise TrialEvaluationError("stored evaluated trade does not match content")
    return trade


def _decode_leg(value: object) -> DeliveryLegCharges:
    raw = _object(
        value,
        {item.name for item in fields(DeliveryLegCharges)},
        "charge leg",
    )
    return DeliveryLegCharges(
        trade_date=_date(raw["trade_date"], "charge.trade_date"),
        turnover=_decimal(raw["turnover"], "charge.turnover"),
        brokerage=_decimal(raw["brokerage"], "charge.brokerage"),
        stt=_decimal(raw["stt"], "charge.stt"),
        exchange_and_ipft=_decimal(
            raw["exchange_and_ipft"], "charge.exchange_and_ipft"
        ),
        sebi=_decimal(raw["sebi"], "charge.sebi"),
        stamp=_decimal(raw["stamp"], "charge.stamp"),
        gst=_decimal(raw["gst"], "charge.gst"),
        dp_base=_decimal(raw["dp_base"], "charge.dp_base"),
        dp_gst=_decimal(raw["dp_gst"], "charge.dp_gst"),
    )


def _decode_charges(value: object) -> DeliveryChargeBreakdown | None:
    if value is None:
        return None
    raw = _object(
        value,
        {item.name for item in fields(DeliveryChargeBreakdown)},
        "charge breakdown",
    )
    if type(raw["legs"]) is not list:
        raise TrialEvaluationError("stored charge legs must be a list")
    stored_id = raw["calculation_id"]
    charges = DeliveryChargeBreakdown(
        schedule_id=raw["schedule_id"],
        legs=tuple(_decode_leg(item) for item in raw["legs"]),
    )
    if charges.calculation_id != stored_id:
        raise TrialEvaluationError("stored charge calculation ID does not match content")
    return charges


def _decode_equity_point(value: object) -> EquityPoint:
    raw = _object(
        value,
        {item.name for item in fields(EquityPoint)},
        "equity point",
    )
    return EquityPoint(
        session=_date(raw["session"], "equity.session"),
        equity=_decimal(raw["equity"], "equity.value"),
        drawdown=_decimal(raw["drawdown"], "equity.drawdown"),
    )


def decode_trial_evaluation_result(payload: bytes) -> TrialEvaluationResult:
    try:
        value = json.loads(
            payload.decode("utf-8", errors="strict"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        top = _object(
            value,
            {"store_schema_version", "result"},
            "evaluation-result envelope",
        )
        if top["store_schema_version"] != TRIAL_EVALUATION_RESULT_STORE_SCHEMA_VERSION:
            raise TrialEvaluationError("unsupported evaluation-result store schema")
        raw = _object(
            top["result"],
            {item.name for item in fields(TrialEvaluationResult)},
            "evaluation result",
        )
        if type(raw["trades"]) is not list or type(raw["equity_curve"]) is not list:
            raise TrialEvaluationError("stored result collections are invalid")
        stored_id = raw["result_id"]
        result = TrialEvaluationResult(
            trial_id=raw["trial_id"],
            split_plan_id=raw["split_plan_id"],
            dataset_id=raw["dataset_id"],
            execution_policy_id=raw["execution_policy_id"],
            cost_schedule_id=raw["cost_schedule_id"],
            initial_capital=_decimal(raw["initial_capital"], "initial_capital"),
            trades=tuple(_decode_trade(item) for item in raw["trades"]),
            charges=_decode_charges(raw["charges"]),
            equity_curve=tuple(
                _decode_equity_point(item) for item in raw["equity_curve"]
            ),
            metrics=_pairs(raw["metrics"], "metrics"),
            pass_thresholds=_pairs(raw["pass_thresholds"], "pass_thresholds"),
            passed=raw["passed"],
        )
        if result.result_id != stored_id:
            raise TrialEvaluationError("stored evaluation result ID does not match content")
        result.verify_content_identity()
        return result
    except TrialEvaluationError:
        raise
    except (
        InvalidOperation,
        KeyError,
        TypeError,
        ValueError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:
        raise TrialEvaluationError("stored evaluation result is invalid") from exc


class LocalTrialEvaluationResultStore:
    """Create-once full evidence artifacts for engine-generated trial results."""

    def __init__(self, root: Path, registry: LocalTrialRegistry) -> None:
        self.root = Path(root)
        if type(registry) is not LocalTrialRegistry:
            raise TypeError("registry must be an exact LocalTrialRegistry")
        self.registry = registry

    @property
    def results_root(self) -> Path:
        return self.root / "results"

    def _trial_dir(self, trial_id: str) -> Path:
        if not isinstance(trial_id, str) or _SHA256.fullmatch(trial_id) is None:
            raise TrialEvaluationError("trial_id must be a full lowercase SHA-256")
        return self.results_root / trial_id

    def _path(self, trial_id: str, result_id: str) -> Path:
        if not isinstance(result_id, str) or _SHA256.fullmatch(result_id) is None:
            raise TrialEvaluationError("result_id must be a full lowercase SHA-256")
        return self._trial_dir(trial_id) / f"{result_id}.json"

    def _validate_registration_binding(self, result: TrialEvaluationResult) -> None:
        registration = self.registry.require_registered(result.trial_id)
        if (
            result.split_plan_id != registration.split_plan_id
            or result.execution_policy_id != registration.execution_policy_hash
            or result.cost_schedule_id != registration.cost_schedule_hash
            or result.pass_thresholds != registration.pass_thresholds
        ):
            raise TrialEvaluationResultConflict(
                "evaluation result does not match registered trial bindings"
            )

    def publish(self, result: TrialEvaluationResult) -> TrialEvaluationResult:
        if type(result) is not TrialEvaluationResult:
            raise TypeError("result must be an exact TrialEvaluationResult")
        result.verify_content_identity()
        self._validate_registration_binding(result)
        self.results_root.mkdir(parents=True, exist_ok=True)
        if _is_link_like(self.results_root):
            raise TrialEvaluationResultConflict("evaluation-results root cannot be a link")
        trial_dir = self._trial_dir(result.trial_id)
        trial_dir.mkdir(exist_ok=True)
        if _is_link_like(trial_dir):
            raise TrialEvaluationResultConflict("evaluation-result path cannot be a link")
        target = self._path(result.trial_id, result.result_id)
        payload = encode_trial_evaluation_result(result)
        lock = self.results_root / ".evaluation-results.lock"
        try:
            with advisory_file_lock(lock):
                if target.exists():
                    stored = self.get(result.trial_id, result.result_id)
                    if stored != result:
                        raise TrialEvaluationResultConflict(
                            "stored evaluation result differs from proposed content"
                        )
                    return stored
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".evaluation-result-",
                    suffix=".tmp",
                    dir=trial_dir,
                )
                temporary = Path(temporary_name)
                try:
                    with os.fdopen(descriptor, "wb") as handle:
                        handle.write(payload)
                        handle.flush()
                        os.fsync(handle.fileno())
                    os.link(temporary, target)
                finally:
                    temporary.unlink(missing_ok=True)
        except (FileLockUnavailable, FileSafetyError) as exc:
            raise TrialEvaluationResultConflict(
                "evaluation-result store is unavailable"
            ) from exc
        return self.get(result.trial_id, result.result_id)

    def get(self, trial_id: str, result_id: str) -> TrialEvaluationResult:
        self.registry.require_registered(trial_id)
        path = self._path(trial_id, result_id)
        if not path.exists():
            raise TrialEvaluationResultNotFound(result_id)
        if not path.is_file() or _is_link_like(path):
            raise TrialEvaluationResultConflict(
                "evaluation result must be a regular file"
            )
        try:
            payload = read_stable_regular_file(path, maximum_bytes=_MAX_RESULT_BYTES)
        except FileSafetyError as exc:
            raise TrialEvaluationResultConflict(
                "evaluation result could not be read safely"
            ) from exc
        result = decode_trial_evaluation_result(payload)
        if result.trial_id != trial_id or result.result_id != result_id:
            raise TrialEvaluationResultConflict(
                "evaluation-result filename identity mismatch"
            )
        self._validate_registration_binding(result)
        return result

    def require_persisted(
        self,
        result: TrialEvaluationResult,
    ) -> TrialEvaluationResult:
        stored = self.get(result.trial_id, result.result_id)
        if stored != result:
            raise TrialEvaluationResultConflict(
                "persisted evaluation result differs from completion evidence"
            )
        return stored

    def results_for_trial(self, trial_id: str) -> tuple[TrialEvaluationResult, ...]:
        self.registry.require_registered(trial_id)
        trial_dir = self._trial_dir(trial_id)
        if not trial_dir.exists():
            return ()
        if not trial_dir.is_dir() or _is_link_like(trial_dir):
            raise TrialEvaluationResultConflict(
                "evaluation-result trial path must be a regular directory"
            )
        paths = tuple(sorted(trial_dir.iterdir(), key=lambda value: value.name))
        results: list[TrialEvaluationResult] = []
        for path in paths:
            if (
                not path.name.endswith(".json")
                or _SHA256.fullmatch(path.stem) is None
                or not path.is_file()
                or _is_link_like(path)
            ):
                raise TrialEvaluationResultConflict(
                    "evaluation-result file set is invalid"
                )
            results.append(self.get(trial_id, path.stem))
        return tuple(results)
