from __future__ import annotations

import re
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum

from india_swing.identity import content_id
from india_swing.shadow_alerts import ShadowAlert, ShadowAlertKind


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_PUBLIC_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,63}\Z")
_EXIT_REASON_CODES = frozenset({"STOP_EXIT", "TARGET_EXIT", "TIME_EXIT"})


class PaperTradeError(ValueError):
    pass


class PaperTradeIntegrityError(PaperTradeError):
    pass


class PaperTradeConflict(PaperTradeError):
    pass


class PaperTradeEventType(str, Enum):
    ENTRY_RECORDED = "ENTRY_RECORDED"
    EXIT_RECORDED = "EXIT_RECORDED"
    EXPIRED = "EXPIRED"
    INVALIDATED = "INVALIDATED"


class PaperTradeStatus(str, Enum):
    ALERTED = "ALERTED"
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    EXPIRED = "EXPIRED"
    INVALIDATED = "INVALIDATED"


def _utc(value: datetime, name: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is None:
        raise PaperTradeError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _positive_decimal(value: Decimal, name: str) -> None:
    if type(value) is not Decimal or not value.is_finite() or value <= 0:
        raise PaperTradeError(f"{name} must be a positive finite Decimal")


@dataclass(frozen=True, slots=True)
class PaperTradeRegistration:
    alert_id: str
    source_run_id: str
    source_pipeline_integrity_hash: str
    source_decision_integrity_hash: str
    signal_id: str
    symbol: str
    quantity: int
    decision_time: datetime
    earliest_entry_at: datetime
    entry_expires_at: datetime
    entry_low: Decimal
    entry_high: Decimal
    stop: Decimal
    target: Decimal
    max_holding_sessions: int
    estimated_round_trip_cost: Decimal
    mode: str = "PAPER_ONLY"
    actionable: bool = False
    schema_version: str = "paper-trade-registration/v1"
    registration_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.alert_id, "alert_id"),
            (self.source_pipeline_integrity_hash, "source_pipeline_integrity_hash"),
            (self.source_decision_integrity_hash, "source_decision_integrity_hash"),
        ):
            if type(value) is not str or _SHA256.fullmatch(value) is None:
                raise PaperTradeError(f"{name} must be a lowercase SHA-256")
        if type(self.source_run_id) is not str or not self.source_run_id:
            raise PaperTradeError("source_run_id is required")
        if type(self.signal_id) is not str or not self.signal_id.strip():
            raise PaperTradeError("signal_id is required")
        if type(self.symbol) is not str or not self.symbol.strip():
            raise PaperTradeError("symbol is required")
        if type(self.quantity) is not int or self.quantity <= 0:
            raise PaperTradeError("quantity must be a positive exact integer")
        object.__setattr__(self, "decision_time", _utc(self.decision_time, "decision_time"))
        object.__setattr__(
            self, "earliest_entry_at", _utc(self.earliest_entry_at, "earliest_entry_at")
        )
        object.__setattr__(
            self, "entry_expires_at", _utc(self.entry_expires_at, "entry_expires_at")
        )
        if not self.decision_time <= self.earliest_entry_at < self.entry_expires_at:
            raise PaperTradeError("entry window is invalid")
        for value, name in (
            (self.entry_low, "entry_low"),
            (self.entry_high, "entry_high"),
            (self.stop, "stop"),
            (self.target, "target"),
        ):
            _positive_decimal(value, name)
        if not self.stop < self.entry_low <= self.entry_high < self.target:
            raise PaperTradeError("paper trade price levels are invalid")
        if type(self.max_holding_sessions) is not int or self.max_holding_sessions <= 0:
            raise PaperTradeError("max_holding_sessions must be positive")
        if (
            type(self.estimated_round_trip_cost) is not Decimal
            or not self.estimated_round_trip_cost.is_finite()
            or self.estimated_round_trip_cost < 0
        ):
            raise PaperTradeError("estimated_round_trip_cost is invalid")
        if self.mode != "PAPER_ONLY" or self.actionable:
            raise PaperTradeError("paper trade authority boundary is invalid")
        if self.schema_version != "paper-trade-registration/v1":
            raise PaperTradeError("unsupported paper trade registration schema")
        object.__setattr__(self, "registration_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "registration_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        try:
            fresh = PaperTradeRegistration(
                **{
                    item.name: getattr(self, item.name)
                    for item in fields(self)
                    if item.name != "registration_id"
                }
            )
        except Exception:
            raise PaperTradeIntegrityError("paper registration identity failed") from None
        if self.registration_id != fresh.registration_id:
            raise PaperTradeIntegrityError("paper registration identity failed")


def registration_from_shadow_alert(alert: ShadowAlert) -> PaperTradeRegistration:
    if type(alert) is not ShadowAlert:
        raise PaperTradeError("shadow alert must be exact")
    try:
        alert.verify_integrity()
    except Exception:
        raise PaperTradeError("shadow alert integrity failed") from None
    if alert.kind is not ShadowAlertKind.CANDIDATE:
        raise PaperTradeError("only candidate alerts can register paper trades")
    decision = alert.decision
    if decision.execution_eligible:
        raise PaperTradeError("executable decisions cannot register paper trades")
    required = (
        decision.symbol,
        decision.earliest_entry_at,
        decision.entry_expires_at,
        decision.entry_low,
        decision.entry_high,
        decision.stop,
        decision.target,
    )
    if any(value is None for value in required):
        raise PaperTradeError("candidate alert omits paper trade terms")
    return PaperTradeRegistration(
        alert_id=alert.alert_id,
        source_run_id=alert.source_run_id,
        source_pipeline_integrity_hash=alert.source_pipeline_integrity_hash,
        source_decision_integrity_hash=decision.integrity_hash,
        signal_id=decision.signal_id,
        symbol=decision.symbol,
        quantity=decision.quantity,
        decision_time=decision.decision_time,
        earliest_entry_at=decision.earliest_entry_at,
        entry_expires_at=decision.entry_expires_at,
        entry_low=decision.entry_low,
        entry_high=decision.entry_high,
        stop=decision.stop,
        target=decision.target,
        max_holding_sessions=decision.max_holding_sessions,
        estimated_round_trip_cost=decision.estimated_cost,
    )


@dataclass(frozen=True, slots=True)
class PaperTradeEvent:
    registration_id: str
    alert_id: str
    sequence: int
    previous_event_id: str | None
    event_type: PaperTradeEventType
    occurred_at: datetime
    observed_price: Decimal | None = None
    evidence_id: str | None = None
    reason_code: str | None = None
    market_session: date | None = None
    replay_id: str | None = None
    outcome_policy_id: str | None = None
    instrument_binding_id: str | None = None
    calendar_snapshot_id: str | None = None
    schema_version: str = "paper-trade-event/v2"
    event_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.registration_id, "registration_id"),
            (self.alert_id, "alert_id"),
        ):
            if type(value) is not str or _SHA256.fullmatch(value) is None:
                raise PaperTradeError(f"{name} must be a lowercase SHA-256")
        if type(self.sequence) is not int or self.sequence <= 0:
            raise PaperTradeError("event sequence must be a positive integer")
        if self.sequence == 1:
            if self.previous_event_id is not None:
                raise PaperTradeError("first event cannot have a predecessor")
        elif (
            type(self.previous_event_id) is not str
            or _SHA256.fullmatch(self.previous_event_id) is None
        ):
            raise PaperTradeError("later event requires a predecessor")
        if type(self.event_type) is not PaperTradeEventType:
            raise PaperTradeError("paper event type must be exact")
        object.__setattr__(self, "occurred_at", _utc(self.occurred_at, "occurred_at"))
        is_fill = self.event_type in {
            PaperTradeEventType.ENTRY_RECORDED,
            PaperTradeEventType.EXIT_RECORDED,
        }
        if is_fill:
            _positive_decimal(self.observed_price, "observed_price")
            if type(self.evidence_id) is not str or _SHA256.fullmatch(self.evidence_id) is None:
                raise PaperTradeError("fill events require an evidence SHA-256")
            if type(self.market_session) is not date:
                raise PaperTradeError("fill events require a market_session date")
            if self.event_type is PaperTradeEventType.ENTRY_RECORDED:
                if self.reason_code is not None:
                    raise PaperTradeError("entry events cannot carry a reason code")
            elif self.reason_code is not None and self.reason_code not in _EXIT_REASON_CODES:
                raise PaperTradeError("exit reason code must be an exact exit reason")
        else:
            if self.observed_price is not None or self.evidence_id is not None:
                raise PaperTradeError("non-fill events cannot carry fill evidence")
            if self.market_session is not None:
                raise PaperTradeError("non-fill events cannot carry a market_session")
            if type(self.reason_code) is not str or _PUBLIC_CODE.fullmatch(self.reason_code) is None:
                raise PaperTradeError("non-fill events require a public reason code")
        lineage = (
            self.replay_id,
            self.outcome_policy_id,
            self.instrument_binding_id,
            self.calendar_snapshot_id,
        )
        present = [value is not None for value in lineage]
        if any(present) and not all(present):
            raise PaperTradeError(
                "automated replay lineage must be fully present or fully absent"
            )
        if all(present):
            for value, name in zip(
                lineage,
                (
                    "replay_id",
                    "outcome_policy_id",
                    "instrument_binding_id",
                    "calendar_snapshot_id",
                ),
            ):
                if type(value) is not str or _SHA256.fullmatch(value) is None:
                    raise PaperTradeError(f"{name} must be a lowercase SHA-256")
            if self.event_type is PaperTradeEventType.INVALIDATED:
                raise PaperTradeError("invalidation cannot carry automated replay lineage")
        if self.schema_version != "paper-trade-event/v2":
            raise PaperTradeError("unsupported paper trade event schema")
        object.__setattr__(self, "event_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "event_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        try:
            fresh = PaperTradeEvent(
                **{
                    item.name: getattr(self, item.name)
                    for item in fields(self)
                    if item.name != "event_id"
                }
            )
        except Exception:
            raise PaperTradeIntegrityError("paper event identity failed") from None
        if self.event_id != fresh.event_id:
            raise PaperTradeIntegrityError("paper event identity failed")


@dataclass(frozen=True, slots=True)
class PaperTradeSummary:
    registration_id: str
    alert_id: str
    status: PaperTradeStatus
    entry_price: Decimal | None
    exit_price: Decimal | None
    gross_pnl: Decimal | None
    estimated_net_pnl: Decimal | None
    event_ids: tuple[str, ...]
    mode: str = "PAPER_ONLY"
    actionable: bool = False
    schema_version: str = "paper-trade-summary/v1"
