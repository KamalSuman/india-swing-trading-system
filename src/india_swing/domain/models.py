from __future__ import annotations

from dataclasses import dataclass, field, fields
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum

from india_swing.identity import content_id


ZERO = Decimal("0")
ONE = Decimal("1")
INDIA_STANDARD_TIME = timezone(timedelta(hours=5, minutes=30))


def require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


def require_decimal(value: Decimal, field_name: str) -> None:
    if type(value) is not Decimal:
        raise TypeError(f"{field_name} must be a Decimal")
    if not value.is_finite():
        raise ValueError(f"{field_name} must be finite")


def require_probability(value: Decimal, field_name: str) -> None:
    require_decimal(value, field_name)
    if value < ZERO or value > ONE:
        raise ValueError(f"{field_name} must be between 0 and 1")


class Board(str, Enum):
    MAIN = "MAIN"
    SME = "SME"
    UNKNOWN = "UNKNOWN"


class MarketCapBucket(str, Enum):
    LARGE = "LARGE"
    MID = "MID"
    SMALL = "SMALL"
    MICRO = "MICRO"
    UNKNOWN = "UNKNOWN"


class Surveillance(str, Enum):
    NONE = "NONE"
    ASM = "ASM"
    GSM = "GSM"
    TRADE_TO_TRADE = "TRADE_TO_TRADE"
    UNKNOWN = "UNKNOWN"


class ResearchVerdict(str, Enum):
    APPROVE = "APPROVE"
    VETO = "VETO"
    UNCERTAIN = "UNCERTAIN"


class DecisionAction(str, Enum):
    BUY = "BUY"
    NO_TRADE = "NO_TRADE"


class ProbabilityStatus(str, Enum):
    PROVISIONAL = "PROVISIONAL"
    VALIDATED = "VALIDATED"


class RunStatus(str, Enum):
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class EvidenceItem:
    evidence_id: str
    source: str
    published_at: datetime
    available_at: datetime
    content_hash: str
    event_time: datetime | None = None

    def __post_init__(self) -> None:
        if not self.evidence_id.strip():
            raise ValueError("evidence_id is required")
        if not self.content_hash.strip():
            raise ValueError("content_hash is required")
        require_aware(self.published_at, "published_at")
        require_aware(self.available_at, "available_at")
        if self.event_time is not None:
            require_aware(self.event_time, "event_time")
        if self.available_at < self.published_at:
            raise ValueError("available_at cannot be earlier than published_at")


@dataclass(frozen=True, slots=True)
class DataSnapshot:
    snapshot_id: str
    decision_time: datetime
    market_session: date
    evidence: tuple[EvidenceItem, ...]
    session_finalized_at: datetime
    universe_snapshot_id: str
    calendar_version: str
    trial_id: str
    model_bundle_id: str
    data_content_hash: str
    source_revision: str
    execution_policy_version: str
    cost_schedule_version: str
    content_fingerprint: str = field(init=False)

    def __post_init__(self) -> None:
        if not self.snapshot_id.strip():
            raise ValueError("snapshot_id is required")
        require_aware(self.decision_time, "decision_time")
        require_aware(self.session_finalized_at, "session_finalized_at")
        if type(self.evidence) is not tuple or any(
            type(item) is not EvidenceItem for item in self.evidence
        ):
            raise TypeError("snapshot evidence must be an immutable EvidenceItem tuple")
        if self.market_session > self.decision_time.astimezone(INDIA_STANDARD_TIME).date():
            raise ValueError("market_session cannot be after the decision date")
        if self.session_finalized_at > self.decision_time:
            raise ValueError("session data must be finalized before the decision cutoff")
        if (
            self.session_finalized_at.astimezone(INDIA_STANDARD_TIME).date()
            != self.market_session
        ):
            raise ValueError("session_finalized_at must belong to market_session in India")
        required_lineage = (
            "universe_snapshot_id",
            "calendar_version",
            "trial_id",
            "model_bundle_id",
            "data_content_hash",
            "source_revision",
            "execution_policy_version",
            "cost_schedule_version",
        )
        for name in required_lineage:
            if not getattr(self, name).strip():
                raise ValueError(f"{name} is required")
        evidence_ids = [item.evidence_id for item in self.evidence]
        if len(evidence_ids) != len(set(evidence_ids)):
            raise ValueError("evidence IDs must be unique within a snapshot")
        object.__setattr__(
            self,
            "content_fingerprint",
            self._calculated_content_fingerprint(),
        )

    def _calculated_content_fingerprint(self) -> str:
        return content_id(
            {
                "snapshot_id": self.snapshot_id,
                "decision_time": self.decision_time,
                "market_session": self.market_session,
                "evidence": self.evidence,
                "session_finalized_at": self.session_finalized_at,
                "universe_snapshot_id": self.universe_snapshot_id,
                "calendar_version": self.calendar_version,
                "trial_id": self.trial_id,
                "model_bundle_id": self.model_bundle_id,
                "data_content_hash": self.data_content_hash,
                "source_revision": self.source_revision,
                "execution_policy_version": self.execution_policy_version,
                "cost_schedule_version": self.cost_schedule_version,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.content_fingerprint != self._calculated_content_fingerprint():
            raise ValueError("data snapshot content identity verification failed")


@dataclass(frozen=True, slots=True)
class InstrumentSnapshot:
    instrument_id: str
    listing_id: str
    universe_snapshot_id: str
    exchange: str
    segment: str
    symbol: str
    board: Board
    market_cap_bucket: MarketCapBucket
    active: bool
    suspended: bool
    surveillance: Surveillance
    last_price: Decimal
    median_daily_traded_value: Decimal
    quoted_spread_bps: Decimal
    lower_circuit_locked: bool
    history_sessions: int
    price_session: date
    data_available_at: datetime
    content_fingerprint: str = field(init=False)

    def __post_init__(self) -> None:
        for name in (
            "instrument_id",
            "listing_id",
            "universe_snapshot_id",
            "exchange",
            "segment",
            "symbol",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} is required")
        for name in ("exchange", "segment", "symbol"):
            value = getattr(self, name)
            if value != value.strip().upper():
                raise ValueError(f"{name} must be normalized uppercase text")
        for name in (
            "last_price",
            "median_daily_traded_value",
            "quoted_spread_bps",
        ):
            require_decimal(getattr(self, name), name)
        if self.last_price <= ZERO:
            raise ValueError("last_price must be positive")
        if self.median_daily_traded_value < ZERO:
            raise ValueError("median_daily_traded_value cannot be negative")
        if self.quoted_spread_bps < ZERO:
            raise ValueError("quoted_spread_bps cannot be negative")
        if self.history_sessions < 0:
            raise ValueError("history_sessions cannot be negative")
        require_aware(self.data_available_at, "data_available_at")
        object.__setattr__(
            self,
            "content_fingerprint",
            self._calculated_content_fingerprint(),
        )

    def _calculated_content_fingerprint(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "content_fingerprint"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.content_fingerprint != self._calculated_content_fingerprint():
            raise ValueError("instrument snapshot content identity verification failed")


@dataclass(frozen=True, slots=True)
class ForecastSummary:
    symbol: str
    as_of: datetime
    horizon_sessions: int
    median_return_pct: Decimal
    downside_return_pct: Decimal
    uncertainty: Decimal
    sample_count: int
    model_version: str
    instrument_id: str
    listing_id: str
    universe_snapshot_id: str
    data_snapshot_id: str
    data_snapshot_fingerprint: str
    instrument_fingerprint: str

    def __post_init__(self) -> None:
        require_aware(self.as_of, "forecast.as_of")
        for name in (
            "symbol",
            "model_version",
            "instrument_id",
            "listing_id",
            "universe_snapshot_id",
            "data_snapshot_id",
            "data_snapshot_fingerprint",
            "instrument_fingerprint",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"forecast {name} is required")
        for name in (
            "median_return_pct",
            "downside_return_pct",
            "uncertainty",
        ):
            require_decimal(getattr(self, name), f"forecast.{name}")
        if self.horizon_sessions <= 0:
            raise ValueError("horizon_sessions must be positive")
        if self.sample_count <= 0:
            raise ValueError("sample_count must be positive")
        require_probability(self.uncertainty, "uncertainty")


@dataclass(frozen=True, slots=True)
class SignalFeatures:
    relative_strength: Decimal
    trend_quality: Decimal
    volume_confirmation: Decimal
    liquidity_quality: Decimal
    news_score: Decimal
    estimated_cost_bps: Decimal
    instrument_id: str
    listing_id: str
    universe_snapshot_id: str
    data_snapshot_id: str
    data_snapshot_fingerprint: str
    instrument_fingerprint: str
    provider_version: str

    def __post_init__(self) -> None:
        for name in (
            "instrument_id",
            "listing_id",
            "universe_snapshot_id",
            "data_snapshot_id",
            "data_snapshot_fingerprint",
            "instrument_fingerprint",
            "provider_version",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"signals {name} is required")
        for name in (
            "relative_strength",
            "trend_quality",
            "volume_confirmation",
            "liquidity_quality",
            "news_score",
            "estimated_cost_bps",
        ):
            require_decimal(getattr(self, name), f"signals.{name}")
        for name in (
            "relative_strength",
            "trend_quality",
            "volume_confirmation",
            "liquidity_quality",
        ):
            require_probability(getattr(self, name), name)
        if self.news_score < Decimal("-1") or self.news_score > ONE:
            raise ValueError("news_score must be between -1 and 1")
        if self.estimated_cost_bps < ZERO:
            raise ValueError("estimated_cost_bps cannot be negative")


@dataclass(frozen=True, slots=True)
class TradeSetup:
    symbol: str
    decision_time: datetime
    earliest_entry_at: datetime
    entry_low: Decimal
    entry_high: Decimal
    stop: Decimal
    target: Decimal
    target_probability: Decimal
    stop_probability: Decimal
    expected_time_exit_r: Decimal
    max_holding_sessions: int
    setup_reason: str
    stop_reason: str
    target_reason: str
    cancel_conditions: tuple[str, ...] = field(default_factory=tuple)
    probability_status: ProbabilityStatus = ProbabilityStatus.PROVISIONAL
    calibration_sample_size: int = 0
    entry_expires_at: datetime | None = None
    instrument_id: str = ""
    listing_id: str = ""
    universe_snapshot_id: str = ""
    data_snapshot_id: str = ""
    data_snapshot_fingerprint: str = ""
    instrument_fingerprint: str = ""
    provider_version: str = ""

    def __post_init__(self) -> None:
        for name in (
            "symbol",
            "instrument_id",
            "listing_id",
            "universe_snapshot_id",
            "data_snapshot_id",
            "data_snapshot_fingerprint",
            "instrument_fingerprint",
            "provider_version",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"setup {name} is required")
        require_aware(self.decision_time, "setup.decision_time")
        require_aware(self.earliest_entry_at, "earliest_entry_at")
        if self.entry_expires_at is not None:
            require_aware(self.entry_expires_at, "entry_expires_at")
            if self.entry_expires_at <= self.earliest_entry_at:
                raise ValueError("entry_expires_at must be after earliest_entry_at")
        if type(self.cancel_conditions) is not tuple or any(
            not isinstance(condition, str) or not condition.strip()
            for condition in self.cancel_conditions
        ):
            raise TypeError("cancel_conditions must be an immutable text tuple")
        for name in (
            "entry_low",
            "entry_high",
            "stop",
            "target",
            "target_probability",
            "stop_probability",
            "expected_time_exit_r",
        ):
            require_decimal(getattr(self, name), f"setup.{name}")
        if not (ZERO < self.stop < self.entry_low <= self.entry_high < self.target):
            raise ValueError("long setup must satisfy 0 < stop < entry_low <= entry_high < target")
        require_probability(self.target_probability, "target_probability")
        require_probability(self.stop_probability, "stop_probability")
        if self.target_probability + self.stop_probability > ONE:
            raise ValueError("target and stop probabilities cannot sum above 1")
        if self.max_holding_sessions <= 0:
            raise ValueError("max_holding_sessions must be positive")
        if self.calibration_sample_size < 0:
            raise ValueError("calibration_sample_size cannot be negative")
        if (
            self.probability_status is ProbabilityStatus.VALIDATED
            and self.calibration_sample_size <= 0
        ):
            raise ValueError("validated probabilities require a positive calibration sample")


@dataclass(frozen=True, slots=True)
class Candidate:
    instrument: InstrumentSnapshot
    forecast: ForecastSummary
    signals: SignalFeatures
    setup: TradeSetup
    evidence_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        expected_types = (
            ("instrument", self.instrument, InstrumentSnapshot),
            ("forecast", self.forecast, ForecastSummary),
            ("signals", self.signals, SignalFeatures),
            ("setup", self.setup, TradeSetup),
        )
        for name, value, expected_type in expected_types:
            if type(value) is not expected_type:
                raise TypeError(
                    f"candidate {name} must be an exact {expected_type.__name__}"
                )
        if type(self.evidence_ids) is not tuple or any(
            not isinstance(evidence_id, str) or not evidence_id.strip()
            for evidence_id in self.evidence_ids
        ):
            raise TypeError("candidate evidence_ids must be an immutable text tuple")
        if len(self.evidence_ids) != len(set(self.evidence_ids)):
            raise ValueError("candidate evidence IDs must be unique")
        symbols = {self.instrument.symbol, self.forecast.symbol, self.setup.symbol}
        if len(symbols) != 1:
            raise ValueError("candidate components must refer to the same symbol")
        instrument_ids = {
            self.instrument.instrument_id,
            self.forecast.instrument_id,
            self.signals.instrument_id,
            self.setup.instrument_id,
        }
        if len(instrument_ids) != 1:
            raise ValueError("candidate components must refer to the same instrument ID")
        listing_ids = {
            self.instrument.listing_id,
            self.forecast.listing_id,
            self.signals.listing_id,
            self.setup.listing_id,
        }
        if len(listing_ids) != 1:
            raise ValueError("candidate components must refer to the same listing ID")
        universe_ids = {
            self.instrument.universe_snapshot_id,
            self.forecast.universe_snapshot_id,
            self.signals.universe_snapshot_id,
            self.setup.universe_snapshot_id,
        }
        if len(universe_ids) != 1:
            raise ValueError(
                "candidate components must refer to the same universe snapshot"
            )
        if len(
            {
                self.forecast.data_snapshot_id,
                self.signals.data_snapshot_id,
                self.setup.data_snapshot_id,
            }
        ) != 1:
            raise ValueError(
                "forecast, signals, and setup must refer to the same data snapshot"
            )
        if len(
            {
                self.forecast.data_snapshot_fingerprint,
                self.signals.data_snapshot_fingerprint,
                self.setup.data_snapshot_fingerprint,
            }
        ) != 1:
            raise ValueError(
                "forecast, signals, and setup must bind the same snapshot content"
            )
        component_instrument_fingerprints = {
            self.instrument.content_fingerprint,
            self.forecast.instrument_fingerprint,
            self.signals.instrument_fingerprint,
            self.setup.instrument_fingerprint,
        }
        if len(component_instrument_fingerprints) != 1:
            raise ValueError(
                "candidate components must bind the same instrument content"
            )


@dataclass(frozen=True, slots=True)
class ResearchAssessment:
    symbol: str
    verdict: ResearchVerdict
    thesis: str
    bear_case: str
    risks: tuple[str, ...]
    evidence_ids: tuple[str, ...]
    model_version: str
    instrument_id: str
    listing_id: str
    universe_snapshot_id: str
    data_snapshot_id: str
    data_snapshot_fingerprint: str
    instrument_fingerprint: str

    def __post_init__(self) -> None:
        required_text = (
            "symbol",
            "thesis",
            "bear_case",
            "model_version",
            "instrument_id",
            "listing_id",
            "universe_snapshot_id",
            "data_snapshot_id",
            "data_snapshot_fingerprint",
            "instrument_fingerprint",
        )
        for name in required_text:
            if not getattr(self, name).strip():
                raise ValueError(f"research {name} is required")
        if type(self.risks) is not tuple or any(
            not isinstance(risk, str) or not risk.strip() for risk in self.risks
        ):
            raise TypeError("research risks must be an immutable text tuple")
        if type(self.evidence_ids) is not tuple or any(
            not isinstance(evidence_id, str) or not evidence_id.strip()
            for evidence_id in self.evidence_ids
        ):
            raise TypeError("research evidence IDs must be an immutable text tuple")
        if not self.evidence_ids:
            raise ValueError("research assessment must cite curated evidence")
        if len(self.evidence_ids) != len(set(self.evidence_ids)):
            raise ValueError("research evidence IDs must be unique")


@dataclass(frozen=True, slots=True)
class PortfolioState:
    capital: Decimal
    open_risk: Decimal
    gross_exposure: Decimal
    open_positions: int = 0
    daily_realized_pnl: Decimal = ZERO
    pilot_realized_pnl: Decimal = ZERO

    def __post_init__(self) -> None:
        for name in (
            "capital",
            "open_risk",
            "gross_exposure",
            "daily_realized_pnl",
            "pilot_realized_pnl",
        ):
            require_decimal(getattr(self, name), f"portfolio.{name}")
        if self.capital <= ZERO:
            raise ValueError("capital must be positive")
        if self.open_risk < ZERO or self.gross_exposure < ZERO:
            raise ValueError("portfolio values cannot be negative")
        if self.open_positions < 0:
            raise ValueError("open_positions cannot be negative")


@dataclass(frozen=True, slots=True)
class RiskPolicy:
    policy_version: str = "pilot-v1"
    per_trade_risk: Decimal = Decimal("250")
    max_open_risk: Decimal = Decimal("500")
    max_position_notional: Decimal = Decimal("20000")
    max_gross_exposure: Decimal = Decimal("40000")
    max_turnover_participation: Decimal = Decimal("0.0025")
    min_net_reward_risk: Decimal = Decimal("2.5")
    min_expected_r: Decimal = Decimal("0.20")
    estimated_round_trip_cost_bps: Decimal = Decimal("30")
    max_spread_bps: Decimal = Decimal("50")
    min_history_sessions: int = 120
    require_validated_probabilities: bool = True
    min_calibration_sample_size: int = 100
    max_open_positions: int = 2
    max_daily_loss: Decimal = Decimal("750")
    max_pilot_drawdown: Decimal = Decimal("1500")
    banned_surveillance: tuple[Surveillance, ...] = (
        Surveillance.ASM,
        Surveillance.GSM,
        Surveillance.TRADE_TO_TRADE,
    )

    def __post_init__(self) -> None:
        decimal_fields = (
            "per_trade_risk",
            "max_open_risk",
            "max_position_notional",
            "max_gross_exposure",
            "max_turnover_participation",
            "min_net_reward_risk",
            "min_expected_r",
            "estimated_round_trip_cost_bps",
            "max_spread_bps",
            "max_daily_loss",
            "max_pilot_drawdown",
        )
        for name in decimal_fields:
            require_decimal(getattr(self, name), f"risk_policy.{name}")
        positive_fields = (
            "per_trade_risk",
            "max_open_risk",
            "max_position_notional",
            "max_gross_exposure",
            "max_turnover_participation",
            "min_net_reward_risk",
            "max_daily_loss",
            "max_pilot_drawdown",
        )
        for name in positive_fields:
            if getattr(self, name) <= ZERO:
                raise ValueError(f"{name} must be positive")
        if self.estimated_round_trip_cost_bps < ZERO or self.max_spread_bps < ZERO:
            raise ValueError("cost and spread limits cannot be negative")
        if self.min_history_sessions <= 0:
            raise ValueError("min_history_sessions must be positive")
        if self.min_calibration_sample_size < 0:
            raise ValueError("min_calibration_sample_size cannot be negative")
        if self.max_open_positions <= 0:
            raise ValueError("max_open_positions must be positive")


@dataclass(frozen=True, slots=True)
class TradeDecision:
    action: DecisionAction
    signal_id: str
    decision_time: datetime
    symbol: str | None
    quantity: int
    entry_low: Decimal | None
    entry_high: Decimal | None
    stop: Decimal | None
    target: Decimal | None
    planned_max_loss: Decimal
    estimated_cost: Decimal
    net_reward_risk: Decimal
    expected_r: Decimal
    reasons: tuple[str, ...]
    thesis: str = ""
    bear_case: str = ""
    cancel_conditions: tuple[str, ...] = field(default_factory=tuple)
    metadata: tuple[tuple[str, str], ...] = field(default_factory=tuple)
    target_probability: Decimal = Decimal("0")
    stop_probability: Decimal = Decimal("0")
    probability_status: ProbabilityStatus = ProbabilityStatus.PROVISIONAL
    calibration_sample_size: int = 0
    earliest_entry_at: datetime | None = None
    entry_expires_at: datetime | None = None
    max_holding_sessions: int = 0
    order_type: str = ""
    reference_readiness: str = "UNBOUND"
    execution_eligible: bool = False
    integrity_hash: str = field(init=False)

    def __post_init__(self) -> None:
        require_aware(self.decision_time, "decision.decision_time")
        for name in (
            "planned_max_loss",
            "estimated_cost",
            "net_reward_risk",
            "expected_r",
            "target_probability",
            "stop_probability",
        ):
            require_decimal(getattr(self, name), f"decision.{name}")
        for name in ("entry_low", "entry_high", "stop", "target"):
            value = getattr(self, name)
            if value is not None:
                require_decimal(value, f"decision.{name}")
        if type(self.reasons) is not tuple or any(
            not isinstance(reason, str) or not reason.strip() for reason in self.reasons
        ):
            raise TypeError("decision reasons must be an immutable text tuple")
        if type(self.cancel_conditions) is not tuple or any(
            not isinstance(condition, str) or not condition.strip()
            for condition in self.cancel_conditions
        ):
            raise TypeError(
                "decision cancel_conditions must be an immutable text tuple"
            )
        if type(self.metadata) is not tuple or any(
            type(item) is not tuple
            or len(item) != 2
            or not all(isinstance(value, str) for value in item)
            for item in self.metadata
        ):
            raise TypeError("decision metadata must be an immutable text-pair tuple")
        if self.quantity < 0:
            raise ValueError("quantity cannot be negative")
        if self.action is DecisionAction.BUY and self.quantity <= 0:
            raise ValueError("BUY decisions require a positive quantity")
        if self.action is DecisionAction.BUY:
            if self.earliest_entry_at is None or self.entry_expires_at is None:
                raise ValueError("BUY decisions require an explicit entry validity window")
            require_aware(self.earliest_entry_at, "decision.earliest_entry_at")
            require_aware(self.entry_expires_at, "decision.entry_expires_at")
            if self.entry_expires_at <= self.earliest_entry_at:
                raise ValueError("decision entry expiry must follow its earliest entry")
            if self.max_holding_sessions <= 0:
                raise ValueError("BUY decisions require a positive holding horizon")
            if not self.order_type.strip():
                raise ValueError("BUY decisions require an order type")
            if not self.reference_readiness.strip():
                raise ValueError("BUY decisions require explicit reference readiness")
        if type(self.execution_eligible) is not bool:
            raise TypeError("execution_eligible must be a bool")
        if self.execution_eligible and self.reference_readiness != "POINT_IN_TIME_VERIFIED":
            raise ValueError(
                "only point-in-time-verified decisions can be execution eligible"
            )
        require_probability(self.target_probability, "decision.target_probability")
        require_probability(self.stop_probability, "decision.stop_probability")
        if self.calibration_sample_size < 0:
            raise ValueError("calibration_sample_size cannot be negative")
        object.__setattr__(self, "integrity_hash", self._calculated_integrity_hash())

    def _calculated_integrity_hash(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "integrity_hash"
            },
            length=64,
        )

    def verify_integrity(self) -> None:
        if self.integrity_hash != self._calculated_integrity_hash():
            raise ValueError("trade decision integrity verification failed")
