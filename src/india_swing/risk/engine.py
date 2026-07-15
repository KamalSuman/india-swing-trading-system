from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, fields, is_dataclass
from datetime import date, datetime
from decimal import Decimal, ROUND_FLOOR
from enum import Enum
from os import PathLike
from typing import Any

from india_swing.domain.models import (
    Candidate,
    DecisionAction,
    PortfolioState,
    ResearchAssessment,
    ResearchVerdict,
    ProbabilityStatus,
    RiskPolicy,
    TradeDecision,
)


ZERO = Decimal("0")
_SENSITIVE_ATTRIBUTE_PARTS = (
    "api_key",
    "apikey",
    "authorization",
    "credential",
    "password",
    "private_key",
    "secret",
    "token",
)


def _type_name(value: object) -> str:
    value_type = type(value)
    return f"{value_type.__module__}.{value_type.__qualname__}"


def _is_sensitive_attribute(name: str) -> bool:
    normalized = name.casefold().replace("-", "_")
    return any(part in normalized for part in _SENSITIVE_ATTRIBUTE_PARTS)


def _identity_value(value: object, stack: set[int]) -> object:
    """Return a JSON-safe, deterministic representation without using repr addresses."""

    if isinstance(value, Enum):
        return {"$enum": _type_name(value), "value": _identity_value(value.value, stack)}
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, Decimal):
        return {"$decimal": str(value)}
    if isinstance(value, float):
        return {"$float": value.hex()}
    if isinstance(value, datetime):
        return {"$datetime": value.isoformat(timespec="microseconds")}
    if isinstance(value, date):
        return {"$date": value.isoformat()}
    if isinstance(value, bytes):
        return {"$bytes_sha256": hashlib.sha256(value).hexdigest(), "length": len(value)}
    if isinstance(value, PathLike):
        return {"$path": str(value)}
    if isinstance(value, type):
        return {"$type": f"{value.__module__}.{value.__qualname__}"}

    marker = id(value)
    if marker in stack:
        return {"$cycle": _type_name(value)}
    stack.add(marker)
    try:
        if is_dataclass(value):
            return {
                "$dataclass": _type_name(value),
                "fields": {
                    field.name: _identity_value(getattr(value, field.name), stack)
                    for field in fields(value)
                },
            }
        if isinstance(value, Mapping):
            pairs = [
                (_identity_value(key, stack), _identity_value(item, stack))
                for key, item in value.items()
            ]
            pairs.sort(key=lambda pair: _canonical_json(pair[0]))
            return {"$mapping": [[key, item] for key, item in pairs]}
        if isinstance(value, tuple):
            return {"$tuple": [_identity_value(item, stack) for item in value]}
        if isinstance(value, list):
            return {"$list": [_identity_value(item, stack) for item in value]}
        if isinstance(value, (set, frozenset)):
            items = [_identity_value(item, stack) for item in value]
            items.sort(key=_canonical_json)
            return {"$set": items}
        if callable(value):
            return {
                "$callable": f"{getattr(value, '__module__', type(value).__module__)}."
                f"{getattr(value, '__qualname__', type(value).__qualname__)}"
            }

        explicit_material = getattr(value, "identity_material", None)
        if explicit_material is not None:
            if callable(explicit_material):
                explicit_material = explicit_material()
            return {
                "$object": _type_name(value),
                "identity_material": _identity_value(explicit_material, stack),
            }

        try:
            attributes = vars(value)
        except TypeError:
            attributes = {}
        public_attributes = {
            name: item
            for name, item in attributes.items()
            if not name.startswith("_")
            and not _is_sensitive_attribute(name)
            and not callable(item)
        }
        return {
            "$object": _type_name(value),
            "attributes": _identity_value(public_attributes, stack),
        }
    finally:
        stack.remove(marker)


def canonical_identity(value: object) -> object:
    return _identity_value(value, set())


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def content_id(material: object, length: int = 20) -> str:
    payload = _canonical_json(canonical_identity(material)).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:length]


def component_identity(component: object) -> object:
    versions: dict[str, object] = {}
    for name in ("version", "model_version", "model_name", "model_id"):
        value = getattr(component, name, None)
        if value is not None and not callable(value):
            versions[name] = value
    return {
        "component_type": _type_name(component),
        "versions": versions,
        "configuration": canonical_identity(component),
    }


def floor_units(value: Decimal) -> int:
    if value <= ZERO:
        return 0
    return int(value.to_integral_value(rounding=ROUND_FLOOR))


@dataclass(frozen=True, slots=True)
class RiskEvaluation:
    approved: bool
    decision: TradeDecision | None
    reasons: tuple[str, ...]


class RiskEngine:
    def __init__(self, policy: RiskPolicy) -> None:
        self.policy = policy

    def evaluate(
        self,
        candidate: Candidate,
        research: ResearchAssessment,
        portfolio: PortfolioState,
        rank: int,
        *,
        identity_context: object | None = None,
    ) -> RiskEvaluation:
        setup = candidate.setup
        instrument = candidate.instrument
        reasons: list[str] = []

        if research.verdict is not ResearchVerdict.APPROVE:
            reasons.append(f"research verdict is {research.verdict.value}")
        if setup.earliest_entry_at <= setup.decision_time:
            reasons.append("entry is not strictly after the decision time")
        if portfolio.open_risk >= self.policy.max_open_risk:
            reasons.append("portfolio open-risk limit is already exhausted")
        if portfolio.open_positions >= self.policy.max_open_positions:
            reasons.append("maximum number of open positions is already reached")
        if portfolio.daily_realized_pnl <= -self.policy.max_daily_loss:
            reasons.append("daily loss halt is active")
        if portfolio.pilot_realized_pnl <= -self.policy.max_pilot_drawdown:
            reasons.append("pilot drawdown halt is active")
        if setup.entry_expires_at is None:
            reasons.append("entry validity window is missing")
        if (
            self.policy.require_validated_probabilities
            and setup.probability_status is not ProbabilityStatus.VALIDATED
        ):
            reasons.append("probability estimate is not validated")
        if (
            self.policy.require_validated_probabilities
            and setup.calibration_sample_size < self.policy.min_calibration_sample_size
        ):
            reasons.append("probability calibration sample is too small")

        cost_bps = max(
            self.policy.estimated_round_trip_cost_bps,
            candidate.signals.estimated_cost_bps,
        )
        cost_per_share = setup.entry_high * cost_bps / Decimal("10000")
        net_loss_per_share = setup.entry_high - setup.stop + cost_per_share
        net_reward_per_share = setup.target - setup.entry_high - cost_per_share

        if net_loss_per_share <= ZERO or net_reward_per_share <= ZERO:
            reasons.append("setup has non-positive net risk or reward")
            net_reward_risk = ZERO
        else:
            net_reward_risk = net_reward_per_share / net_loss_per_share
            if net_reward_risk < self.policy.min_net_reward_risk:
                reasons.append("net reward-to-risk is below the policy minimum")

        time_probability = Decimal("1") - setup.target_probability - setup.stop_probability
        expected_r = (
            setup.target_probability * net_reward_risk
            - setup.stop_probability
            + time_probability * setup.expected_time_exit_r
        )
        if expected_r < self.policy.min_expected_r:
            reasons.append("cost-adjusted expected R is below the policy minimum")

        remaining_open_risk = self.policy.max_open_risk - portfolio.open_risk
        remaining_exposure = self.policy.max_gross_exposure - portfolio.gross_exposure
        remaining_cash = portfolio.capital - portfolio.gross_exposure
        liquidity_notional = (
            instrument.median_daily_traded_value * self.policy.max_turnover_participation
        )
        notional_cap = min(
            self.policy.max_position_notional,
            remaining_exposure,
            remaining_cash,
            liquidity_notional,
        )
        risk_budget = min(self.policy.per_trade_risk, remaining_open_risk)
        quantity = min(
            floor_units(risk_budget / net_loss_per_share) if net_loss_per_share > ZERO else 0,
            floor_units(notional_cap / setup.entry_high) if setup.entry_high > ZERO else 0,
        )
        if quantity < 1:
            reasons.append("risk, exposure, or liquidity caps produce zero quantity")

        if reasons:
            return RiskEvaluation(False, None, tuple(reasons))

        planned_max_loss = net_loss_per_share * quantity
        estimated_cost = cost_per_share * quantity
        decision_material: dict[str, Any] = dict(
            action=DecisionAction.BUY,
            decision_time=setup.decision_time,
            symbol=instrument.symbol,
            quantity=quantity,
            entry_low=setup.entry_low,
            entry_high=setup.entry_high,
            stop=setup.stop,
            target=setup.target,
            planned_max_loss=planned_max_loss,
            estimated_cost=estimated_cost,
            net_reward_risk=net_reward_risk,
            expected_r=expected_r,
            reasons=(setup.setup_reason, setup.stop_reason, setup.target_reason),
            thesis=research.thesis,
            bear_case=research.bear_case,
            cancel_conditions=setup.cancel_conditions,
            metadata=(
                ("rank", str(rank)),
                ("risk_policy", self.policy.policy_version),
                ("forecast_model", candidate.forecast.model_version),
                ("research_model", research.model_version),
            ),
            target_probability=setup.target_probability,
            stop_probability=setup.stop_probability,
            probability_status=setup.probability_status,
            calibration_sample_size=setup.calibration_sample_size,
            earliest_entry_at=setup.earliest_entry_at,
            entry_expires_at=setup.entry_expires_at,
            max_holding_sessions=setup.max_holding_sessions,
            order_type="LIMIT",
        )
        signal_id = content_id(
            {
                "identity_schema": "trade-signal-v2",
                "pipeline_context": identity_context,
                "candidate": candidate,
                "research": research,
                "portfolio": portfolio,
                "risk_policy": self.policy,
                "rank": rank,
                "final_decision": decision_material,
            }
        )
        decision = TradeDecision(signal_id=signal_id, **decision_material)
        return RiskEvaluation(True, decision, ())
