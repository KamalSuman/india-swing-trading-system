from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal

from india_swing.domain.models import INDIA_STANDARD_TIME, require_aware


ZERO = Decimal("0")
SHA256_IDENTIFIER = re.compile(r"[0-9a-f]{64}\Z")
NSE_REGULAR_MARKET_CLOSE = time(15, 30)
NSE_REGULAR_DATA_READY = time(16, 0)
NSE_REGULAR_FINALITY_POLICY_VERSION = "nse-regular-eod-collection-guard/v1"


def _require_decimal(value: object, field_name: str) -> None:
    if not isinstance(value, Decimal):
        raise TypeError(f"{field_name} must be a Decimal")


@dataclass(frozen=True, slots=True)
class NseSessionFinality:
    """Non-overridable regular-session guard used only for data collection.

    This is deliberately not a trading-calendar assertion. It blocks collection
    before 16:00 IST and marks the result non-actionable until a dated official
    NSE calendar confirms the session and its actual close.
    """

    session: date
    market_close_at: datetime
    data_ready_at: datetime
    policy_version: str
    actionable: bool

    @classmethod
    def regular_collection_guard(cls, session: date) -> NseSessionFinality:
        return cls(
            session=session,
            market_close_at=datetime.combine(
                session,
                NSE_REGULAR_MARKET_CLOSE,
                tzinfo=INDIA_STANDARD_TIME,
            ),
            data_ready_at=datetime.combine(
                session,
                NSE_REGULAR_DATA_READY,
                tzinfo=INDIA_STANDARD_TIME,
            ),
            policy_version=NSE_REGULAR_FINALITY_POLICY_VERSION,
            actionable=False,
        )

    def __post_init__(self) -> None:
        require_aware(self.market_close_at, "session_finality.market_close_at")
        require_aware(self.data_ready_at, "session_finality.data_ready_at")
        if self.market_close_at.utcoffset() != INDIA_STANDARD_TIME.utcoffset(None):
            raise ValueError("market_close_at must use the Asia/Kolkata offset")
        if self.data_ready_at.utcoffset() != INDIA_STANDARD_TIME.utcoffset(None):
            raise ValueError("data_ready_at must use the Asia/Kolkata offset")
        expected_close = datetime.combine(
            self.session,
            NSE_REGULAR_MARKET_CLOSE,
            tzinfo=INDIA_STANDARD_TIME,
        )
        expected_ready = datetime.combine(
            self.session,
            NSE_REGULAR_DATA_READY,
            tzinfo=INDIA_STANDARD_TIME,
        )
        if self.market_close_at != expected_close or self.data_ready_at != expected_ready:
            raise ValueError("regular-session finality must use the fixed 15:30/16:00 IST guard")
        if self.policy_version != NSE_REGULAR_FINALITY_POLICY_VERSION:
            raise ValueError("unsupported regular-session finality policy")
        if self.actionable:
            raise ValueError("the unversioned regular-session guard is collection-only")


@dataclass(frozen=True, slots=True)
class KiteInstrument:
    instrument_token: int
    exchange_token: str
    tradingsymbol: str
    name: str
    dump_last_price: Decimal
    expiry: date | None
    strike: Decimal | None
    tick_size: Decimal
    lot_size: int
    instrument_type: str
    segment: str
    exchange: str

    def __post_init__(self) -> None:
        if type(self.instrument_token) is not int or self.instrument_token <= 0:
            raise ValueError("instrument_token must be a positive integer")
        if type(self.lot_size) is not int or self.lot_size <= 0:
            raise ValueError("lot_size must be a positive integer")
        required_text = (
            "exchange_token",
            "tradingsymbol",
            "instrument_type",
            "segment",
            "exchange",
        )
        for name in required_text:
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} is required")
        if not isinstance(self.name, str):
            raise TypeError("name must be text")
        _require_decimal(self.dump_last_price, "dump_last_price")
        _require_decimal(self.tick_size, "tick_size")
        if self.strike is not None:
            _require_decimal(self.strike, "strike")
        if self.dump_last_price < ZERO:
            raise ValueError("dump_last_price cannot be negative")
        if self.strike is not None and self.strike < ZERO:
            raise ValueError("strike cannot be negative")
        if self.tick_size <= ZERO:
            raise ValueError("tick_size must be positive")

    @property
    def listing_key(self) -> str:
        """Current listing key, not a permanent economic-security identifier."""

        return f"{self.exchange}:{self.tradingsymbol}"

    @property
    def is_nse_eq_record(self) -> bool:
        """Kite EQ classification only; not proof of main-board eligibility."""

        return (
            self.exchange == "NSE"
            and self.segment == "NSE"
            and self.instrument_type == "EQ"
        )


@dataclass(frozen=True, slots=True)
class InstrumentBatch:
    exchange: str
    observed_at: datetime
    provider_version: str
    instruments: tuple[KiteInstrument, ...]

    def __post_init__(self) -> None:
        require_aware(self.observed_at, "instrument_batch.observed_at")
        if not self.exchange.strip() or not self.provider_version.strip():
            raise ValueError("exchange and provider_version are required")
        listing_keys = [item.listing_key for item in self.instruments]
        if len(listing_keys) != len(set(listing_keys)):
            raise ValueError("instrument batch contains duplicate listing keys")
        tokens = [item.instrument_token for item in self.instruments]
        if len(tokens) != len(set(tokens)):
            raise ValueError("instrument batch contains duplicate instrument tokens")
        exchange_tokens = [item.exchange_token for item in self.instruments]
        if len(exchange_tokens) != len(set(exchange_tokens)):
            raise ValueError("instrument batch contains duplicate exchange tokens")
        wrong_exchange = [
            item.listing_key for item in self.instruments if item.exchange != self.exchange
        ]
        if wrong_exchange:
            raise ValueError("instrument batch contains records from another exchange")


@dataclass(frozen=True, slots=True)
class DailyCandle:
    instrument_token: int
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    open_interest: int | None = None

    def __post_init__(self) -> None:
        if type(self.instrument_token) is not int or self.instrument_token <= 0:
            raise ValueError("instrument_token must be a positive integer")
        require_aware(self.timestamp, "candle.timestamp")
        if self.timestamp.utcoffset() != INDIA_STANDARD_TIME.utcoffset(None):
            raise ValueError("daily candle timestamp must use the Asia/Kolkata offset")
        for name in ("open", "high", "low", "close"):
            _require_decimal(getattr(self, name), name)
        if min(self.open, self.high, self.low, self.close) <= ZERO:
            raise ValueError("OHLC values must be positive")
        if self.high < max(self.open, self.low, self.close):
            raise ValueError("candle high is inconsistent with OHLC values")
        if self.low > min(self.open, self.high, self.close):
            raise ValueError("candle low is inconsistent with OHLC values")
        if type(self.volume) is not int or self.volume < 0:
            raise ValueError("volume must be a non-negative integer")
        if self.open_interest is not None and (
            type(self.open_interest) is not int or self.open_interest < 0
        ):
            raise ValueError("open_interest must be a non-negative integer")

    @property
    def session(self) -> date:
        return self.timestamp.date()


@dataclass(frozen=True, slots=True)
class DailyCandleBatch:
    instrument_token: int
    session_finality: NseSessionFinality
    observed_at: datetime
    provider_version: str
    candles: tuple[DailyCandle, ...]

    def __post_init__(self) -> None:
        if type(self.instrument_token) is not int or self.instrument_token <= 0:
            raise ValueError("instrument_token must be a positive integer")
        require_aware(self.observed_at, "candle_batch.observed_at")
        if self.observed_at < self.session_finality.data_ready_at:
            raise ValueError("end-session candles were requested before session finalization")
        if not self.provider_version.strip():
            raise ValueError("provider_version is required")
        if len(self.candles) != 1:
            raise ValueError("the collection-only contract requires exactly one session candle")
        candle = self.candles[0]
        if candle.instrument_token != self.instrument_token:
            raise ValueError("candle instrument token does not match its batch")
        if candle.session != self.session_finality.session:
            raise ValueError("candle session does not match its finality contract")

    @property
    def session(self) -> date:
        return self.session_finality.session


@dataclass(frozen=True, slots=True)
class DailyCandleArchive:
    """A daily candle bound to the exact instrument-master vintage used."""

    instrument_master_snapshot_id: str
    instrument_master_observed_at: datetime
    listing_key: str
    batch: DailyCandleBatch

    def __post_init__(self) -> None:
        if SHA256_IDENTIFIER.fullmatch(self.instrument_master_snapshot_id) is None:
            raise ValueError("instrument_master_snapshot_id must be a full SHA-256 identifier")
        require_aware(
            self.instrument_master_observed_at,
            "daily_archive.instrument_master_observed_at",
        )
        if not self.listing_key.strip():
            raise ValueError("listing_key is required")
        if self.instrument_master_observed_at > self.batch.observed_at:
            raise ValueError("instrument master cannot be observed after its candle archive")
        master_session = self.instrument_master_observed_at.astimezone(
            INDIA_STANDARD_TIME
        ).date()
        if self.batch.session < master_session:
            raise ValueError(
                "current instrument tokens cannot be used for pre-vintage historical backfills"
            )
