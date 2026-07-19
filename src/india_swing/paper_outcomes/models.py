from __future__ import annotations

import re
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum

from india_swing.historical_prices import NseEodSessionArtifact, RAW_UNADJUSTED
from india_swing.identity import content_id
from india_swing.paper_trades import PaperTradeRegistration
from india_swing.reference.calendar import CalendarSnapshot
from india_swing.tick_sizes import CollectionTickSizeSnapshot


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_ISIN = re.compile(r"[A-Z]{2}[A-Z0-9]{9}[0-9]\Z")
_CODE = re.compile(r"[A-Z][A-Z0-9_]{2,127}\Z")
_BLOCKERS = (
    "COLLECTION_ONLY_NON_ACTIONABLE",
    "CORPORATE_ACTIONS_UNAPPLIED",
    "RAW_UNADJUSTED_PRICES",
    "SELL_CIRCUIT_STATUS_UNAVAILABLE",
)


class PaperOutcomeError(ValueError):
    pass


class PaperOutcomeIntegrityError(PaperOutcomeError):
    pass


class PaperOutcomeStatus(str, Enum):
    WAITING = "WAITING"
    EXPIRED = "EXPIRED"
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    BLOCKED = "BLOCKED"


class PaperOutcomeExitReason(str, Enum):
    STOP = "STOP"
    TARGET = "TARGET"
    TIME = "TIME"


def _sha(value: str, name: str) -> None:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise PaperOutcomeError(f"{name} must be a lowercase SHA-256")


def _utc(value: datetime, name: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is None:
        raise PaperOutcomeError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _positive(value: Decimal, name: str) -> None:
    if type(value) is not Decimal or not value.is_finite() or value <= 0:
        raise PaperOutcomeError(f"{name} must be a positive finite Decimal")


@dataclass(frozen=True, slots=True)
class PaperOutcomePolicy:
    slippage_bps: Decimal = Decimal("10")
    maximum_participation: Decimal = Decimal("0.0025")
    policy_version: str = "paper-outcome-conservative-eod/v1"
    policy_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            type(self.slippage_bps) is not Decimal
            or not self.slippage_bps.is_finite()
            or self.slippage_bps < 0
        ):
            raise PaperOutcomeError("slippage_bps must be non-negative")
        if (
            type(self.maximum_participation) is not Decimal
            or not self.maximum_participation.is_finite()
            or not Decimal("0") < self.maximum_participation <= Decimal("1")
        ):
            raise PaperOutcomeError("maximum_participation must be in (0, 1]")
        if self.policy_version != "paper-outcome-conservative-eod/v1":
            raise PaperOutcomeError("unsupported paper outcome policy")
        object.__setattr__(self, "policy_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "policy_version": self.policy_version,
                "slippage_bps": self.slippage_bps,
                "maximum_participation": self.maximum_participation,
                "entry": "LIMIT_AT_ENTRY_HIGH_REJECT_GAP_BELOW_ENTRY_LOW",
                "same_bar": "STOP_FIRST_TARGET_DEFERRED",
                "gap_stop": "OPEN_WITH_ADVERSE_SLIPPAGE",
                "time_exit": "HORIZON_CLOSE_WITH_ADVERSE_SLIPPAGE",
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        try:
            fresh = PaperOutcomePolicy(
                slippage_bps=self.slippage_bps,
                maximum_participation=self.maximum_participation,
                policy_version=self.policy_version,
            )
        except Exception:
            raise PaperOutcomeIntegrityError("paper outcome policy identity failed") from None
        if self.policy_id != fresh.policy_id:
            raise PaperOutcomeIntegrityError("paper outcome policy identity failed")


@dataclass(frozen=True, slots=True)
class PaperInstrumentBinding:
    registration_id: str
    symbol: str
    series: str
    validated_isin: str
    financial_instrument_id: int
    tick_size: Decimal
    tick_snapshot_id: str
    tick_observation_id: str
    tick_market_session_claim: date
    tick_knowledge_time: datetime
    schema_version: str = "paper-instrument-binding/v1"
    binding_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.registration_id, "registration_id"),
            (self.tick_snapshot_id, "tick_snapshot_id"),
            (self.tick_observation_id, "tick_observation_id"),
        ):
            _sha(value, name)
        if type(self.symbol) is not str or not self.symbol or self.symbol != self.symbol.strip().upper():
            raise PaperOutcomeError("binding symbol must be normalized")
        if type(self.series) is not str or not self.series or self.series != self.series.strip().upper():
            raise PaperOutcomeError("binding series must be normalized")
        if type(self.validated_isin) is not str or _ISIN.fullmatch(self.validated_isin) is None:
            raise PaperOutcomeError("binding ISIN is invalid")
        if type(self.financial_instrument_id) is not int or self.financial_instrument_id <= 0:
            raise PaperOutcomeError("financial_instrument_id must be positive")
        _positive(self.tick_size, "tick_size")
        if type(self.tick_market_session_claim) is not date:
            raise PaperOutcomeError("tick_market_session_claim must be a date")
        object.__setattr__(
            self,
            "tick_knowledge_time",
            _utc(self.tick_knowledge_time, "tick_knowledge_time"),
        )
        if self.schema_version != "paper-instrument-binding/v1":
            raise PaperOutcomeError("unsupported instrument binding schema")
        object.__setattr__(self, "binding_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "binding_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        try:
            fresh = PaperInstrumentBinding(
                **{
                    item.name: getattr(self, item.name)
                    for item in fields(self)
                    if item.name != "binding_id"
                }
            )
        except Exception:
            raise PaperOutcomeIntegrityError("paper instrument binding identity failed") from None
        if self.binding_id != fresh.binding_id:
            raise PaperOutcomeIntegrityError("paper instrument binding identity failed")


def bind_paper_instrument(
    registration: PaperTradeRegistration,
    snapshot: CollectionTickSizeSnapshot,
    *,
    series: str,
    validated_isin: str,
) -> PaperInstrumentBinding:
    if type(registration) is not PaperTradeRegistration or type(snapshot) is not CollectionTickSizeSnapshot:
        raise PaperOutcomeError("instrument binding inputs must be exact")
    try:
        registration.verify_content_identity()
        snapshot.verify_content_identity()
    except Exception:
        raise PaperOutcomeIntegrityError("instrument binding input identity failed") from None
    if snapshot.knowledge_time > registration.decision_time:
        raise PaperOutcomeError("tick snapshot was unknown at decision time")
    matches = tuple(
        value
        for value in snapshot.observations
        if (value.symbol, value.series, value.validated_isin)
        == (registration.symbol, series, validated_isin)
    )
    if len(matches) != 1:
        raise PaperOutcomeError("instrument binding requires one exact tick observation")
    value = matches[0]
    for price in (
        registration.entry_low,
        registration.entry_high,
        registration.stop,
        registration.target,
    ):
        if price % value.tick_size_rupees != 0:
            raise PaperOutcomeError("registered price is not on the bound tick size")
    return PaperInstrumentBinding(
        registration_id=registration.registration_id,
        symbol=registration.symbol,
        series=series,
        validated_isin=validated_isin,
        financial_instrument_id=value.financial_instrument_id,
        tick_size=value.tick_size_rupees,
        tick_snapshot_id=snapshot.snapshot_id,
        tick_observation_id=value.observation_id,
        tick_market_session_claim=value.market_session_claim,
        tick_knowledge_time=value.knowledge_time,
    )


@dataclass(frozen=True, slots=True)
class PaperOutcomeObservation:
    artifact_id: str
    calendar_snapshot_id: str
    market_session: date
    session_close_at: datetime
    knowledge_time: datetime
    symbol: str
    series: str
    validated_isin: str
    bar_id: str | None
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    volume: int | None
    price_basis: str = RAW_UNADJUSTED
    schema_version: str = "paper-outcome-observation/v1"
    observation_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha(self.artifact_id, "artifact_id")
        _sha(self.calendar_snapshot_id, "calendar_snapshot_id")
        if type(self.market_session) is not date:
            raise PaperOutcomeError("market_session must be a date")
        object.__setattr__(self, "session_close_at", _utc(self.session_close_at, "session_close_at"))
        object.__setattr__(self, "knowledge_time", _utc(self.knowledge_time, "knowledge_time"))
        if self.knowledge_time <= self.session_close_at:
            raise PaperOutcomeError("observation knowledge must follow session close")
        if type(self.symbol) is not str or self.symbol != self.symbol.strip().upper() or not self.symbol:
            raise PaperOutcomeError("observation symbol must be normalized")
        if type(self.series) is not str or self.series != self.series.strip().upper() or not self.series:
            raise PaperOutcomeError("observation series must be normalized")
        if type(self.validated_isin) is not str or _ISIN.fullmatch(self.validated_isin) is None:
            raise PaperOutcomeError("observation ISIN is invalid")
        values = (self.open, self.high, self.low, self.close)
        if self.bar_id is None:
            if any(value is not None for value in values) or self.volume is not None:
                raise PaperOutcomeError("missing-bar observation cannot carry market values")
        else:
            _sha(self.bar_id, "bar_id")
            for value, name in zip(values, ("open", "high", "low", "close"), strict=True):
                _positive(value, name)
            if self.high < max(self.open, self.low, self.close) or self.low > min(self.open, self.high, self.close):
                raise PaperOutcomeError("observation OHLC is inconsistent")
            if type(self.volume) is not int or self.volume <= 0:
                raise PaperOutcomeError("observation volume must be positive")
        if self.price_basis != RAW_UNADJUSTED:
            raise PaperOutcomeError("unsupported paper observation price basis")
        if self.schema_version != "paper-outcome-observation/v1":
            raise PaperOutcomeError("unsupported paper observation schema")
        object.__setattr__(self, "observation_id", self._calculated_id())

    @property
    def traded(self) -> bool:
        return self.bar_id is not None

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
        try:
            fresh = PaperOutcomeObservation(
                **{
                    item.name: getattr(self, item.name)
                    for item in fields(self)
                    if item.name != "observation_id"
                }
            )
        except Exception:
            raise PaperOutcomeIntegrityError("paper outcome observation identity failed") from None
        if self.observation_id != fresh.observation_id:
            raise PaperOutcomeIntegrityError("paper outcome observation identity failed")


def observe_paper_session(
    artifact: NseEodSessionArtifact,
    calendar: CalendarSnapshot,
    binding: PaperInstrumentBinding,
) -> PaperOutcomeObservation:
    if type(artifact) is not NseEodSessionArtifact or type(calendar) is not CalendarSnapshot or type(binding) is not PaperInstrumentBinding:
        raise PaperOutcomeError("paper observation inputs must be exact")
    try:
        artifact.verify_content_identity()
        calendar.verify_content_identity()
        binding.verify_content_identity()
    except Exception:
        raise PaperOutcomeIntegrityError("paper observation input identity failed") from None
    if (artifact.exchange, artifact.segment) != (calendar.exchange, calendar.segment) or (artifact.exchange, artifact.segment) != ("NSE", "CM"):
        raise PaperOutcomeError("paper observation market binding differs")
    day = next((value for value in calendar.days if value.day == artifact.market_session), None)
    if day is None or not day.is_session:
        raise PaperOutcomeError("artifact session is not an open calendar session")
    executable = tuple(value for value in day.session_windows if value.is_executable)
    if not executable:
        raise PaperOutcomeError("calendar session has no executable window")
    matches = tuple(
        value
        for value in artifact.bars
        if (value.symbol, value.series, value.validated_isin)
        == (binding.symbol, binding.series, binding.validated_isin)
    )
    if len(matches) > 1:
        raise PaperOutcomeIntegrityError("artifact contains duplicate bound listings")
    bar = matches[0] if matches else None
    return PaperOutcomeObservation(
        artifact_id=artifact.artifact_id,
        calendar_snapshot_id=calendar.snapshot_id,
        market_session=artifact.market_session,
        session_close_at=executable[-1].closes_at,
        knowledge_time=artifact.knowledge_time,
        symbol=binding.symbol,
        series=binding.series,
        validated_isin=binding.validated_isin,
        bar_id=None if bar is None else bar.bar_id,
        open=None if bar is None else bar.open,
        high=None if bar is None else bar.high,
        low=None if bar is None else bar.low,
        close=None if bar is None else bar.close,
        volume=None if bar is None else bar.volume,
    )


@dataclass(frozen=True, slots=True)
class PaperOutcomeFill:
    market_session: date
    observed_at: datetime
    price: Decimal
    evidence_id: str
    reason: PaperOutcomeExitReason | None
    schema_version: str = "paper-outcome-fill/v1"

    def __post_init__(self) -> None:
        if type(self.market_session) is not date:
            raise PaperOutcomeError("fill market_session must be a date")
        object.__setattr__(self, "observed_at", _utc(self.observed_at, "fill observed_at"))
        _positive(self.price, "fill price")
        _sha(self.evidence_id, "fill evidence_id")
        if self.reason is not None and type(self.reason) is not PaperOutcomeExitReason:
            raise PaperOutcomeError("fill reason must be exact")
        if self.schema_version != "paper-outcome-fill/v1":
            raise PaperOutcomeError("unsupported paper outcome fill schema")

    def verify_integrity(self) -> None:
        try:
            fresh = PaperOutcomeFill(
                market_session=self.market_session,
                observed_at=self.observed_at,
                price=self.price,
                evidence_id=self.evidence_id,
                reason=self.reason,
                schema_version=self.schema_version,
            )
        except Exception:
            raise PaperOutcomeIntegrityError("paper outcome fill integrity failed") from None
        if self != fresh:
            raise PaperOutcomeIntegrityError("paper outcome fill integrity failed")


@dataclass(frozen=True, slots=True)
class PaperOutcomeReplay:
    registration_id: str
    binding_id: str
    policy_id: str
    calendar_snapshot_id: str
    as_of: datetime
    status: PaperOutcomeStatus
    entry: PaperOutcomeFill | None
    exit: PaperOutcomeFill | None
    reason_code: str
    source_observation_ids: tuple[str, ...]
    blockers: tuple[str, ...] = _BLOCKERS
    mode: str = "PAPER_ONLY"
    actionable: bool = False
    provisional: bool = True
    schema_version: str = "paper-outcome-replay/v1"
    replay_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.registration_id, "registration_id"),
            (self.binding_id, "binding_id"),
            (self.policy_id, "policy_id"),
            (self.calendar_snapshot_id, "calendar_snapshot_id"),
        ):
            _sha(value, name)
        object.__setattr__(self, "as_of", _utc(self.as_of, "as_of"))
        if type(self.status) is not PaperOutcomeStatus:
            raise PaperOutcomeError("paper outcome status must be exact")
        if self.entry is not None and type(self.entry) is not PaperOutcomeFill:
            raise PaperOutcomeError("entry fill must be exact")
        if self.exit is not None and type(self.exit) is not PaperOutcomeFill:
            raise PaperOutcomeError("exit fill must be exact")
        if self.entry is not None:
            self.entry.verify_integrity()
        if self.exit is not None:
            self.exit.verify_integrity()
        if self.status is PaperOutcomeStatus.CLOSED and (self.entry is None or self.exit is None):
            raise PaperOutcomeError("closed replay requires entry and exit")
        if self.status is PaperOutcomeStatus.OPEN and (self.entry is None or self.exit is not None):
            raise PaperOutcomeError("open replay requires only an entry")
        if self.status in {PaperOutcomeStatus.WAITING, PaperOutcomeStatus.EXPIRED} and (self.entry is not None or self.exit is not None):
            raise PaperOutcomeError("unentered replay cannot carry fills")
        if type(self.reason_code) is not str or _CODE.fullmatch(self.reason_code) is None:
            raise PaperOutcomeError("replay reason_code is invalid")
        if (
            type(self.source_observation_ids) is not tuple
            or len(set(self.source_observation_ids)) != len(self.source_observation_ids)
        ):
            raise PaperOutcomeError("source observations must be a unique tuple")
        for value in self.source_observation_ids:
            _sha(value, "source_observation_id")
        if self.blockers != tuple(sorted(set(_BLOCKERS))):
            raise PaperOutcomeError("paper replay blockers are invalid")
        if self.mode != "PAPER_ONLY" or self.actionable or not self.provisional:
            raise PaperOutcomeError("paper replay authority boundary is invalid")
        if self.schema_version != "paper-outcome-replay/v1":
            raise PaperOutcomeError("unsupported paper outcome replay schema")
        object.__setattr__(self, "replay_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "replay_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        try:
            fresh = PaperOutcomeReplay(
                **{
                    item.name: getattr(self, item.name)
                    for item in fields(self)
                    if item.name != "replay_id"
                }
            )
        except Exception:
            raise PaperOutcomeIntegrityError("paper outcome replay identity failed") from None
        if self.replay_id != fresh.replay_id:
            raise PaperOutcomeIntegrityError("paper outcome replay identity failed")


DEFAULT_PAPER_OUTCOME_BLOCKERS = tuple(sorted(_BLOCKERS))
