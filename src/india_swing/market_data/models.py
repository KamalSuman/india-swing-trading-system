from __future__ import annotations

import re
from dataclasses import dataclass, field, fields
from datetime import date, datetime, time, timezone
from decimal import Decimal

from india_swing.domain.models import INDIA_STANDARD_TIME, require_aware
from india_swing.identity import content_id


ZERO = Decimal("0")
SHA256_IDENTIFIER = re.compile(r"[0-9a-f]{64}\Z")
NSE_REGULAR_MARKET_CLOSE = time(15, 30)
NSE_REGULAR_DATA_READY = time(16, 0)
NSE_REGULAR_FINALITY_POLICY_VERSION = "nse-regular-eod-collection-guard/v1"
LISTING_KEY_PATTERN = re.compile(r"NSE:[A-Z0-9][A-Z0-9&\-]{0,31}\Z")
NSE_SECURITY_SERIES_PATTERN = re.compile(r"[A-Z0-9]{1,8}\Z")
MARKET_DATA_PROVIDER_PATTERN = re.compile(r"[A-Z][A-Z0-9_]{1,31}\Z")
NSE_EQUITY_ISIN_PATTERN = re.compile(r"INE[A-Z0-9]{9}\Z")
MAXIMUM_QUOTE_KEYS = 500
MAXIMUM_HISTORICAL_SESSIONS = 10000


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
class HistoricalInstrumentBinding:
    """Provider key bound to one point-in-time NSE listing identity interval."""

    provider: str
    provider_instrument_id: str
    exchange: str
    listing_key: str
    security_series: str
    isin: str
    valid_from: date
    valid_through: date
    source_snapshot_ids: tuple[str, ...]
    binding_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "binding_id", self._calculated_id())

    def _validate(self) -> None:
        if type(self.provider) is not str or MARKET_DATA_PROVIDER_PATTERN.fullmatch(
            self.provider
        ) is None:
            raise ValueError("provider must be canonical uppercase provider text")
        if (
            type(self.provider_instrument_id) is not str
            or not self.provider_instrument_id
            or len(self.provider_instrument_id) > 128
            or self.provider_instrument_id != self.provider_instrument_id.strip()
            or any(ord(character) < 32 for character in self.provider_instrument_id)
        ):
            raise ValueError("provider_instrument_id must be bounded canonical text")
        if self.exchange != "NSE":
            raise ValueError("historical binding currently supports NSE only")
        if type(self.listing_key) is not str or LISTING_KEY_PATTERN.fullmatch(
            self.listing_key
        ) is None:
            raise ValueError("listing_key must be canonical NSE:TRADINGSYMBOL text")
        if (
            type(self.security_series) is not str
            or NSE_SECURITY_SERIES_PATTERN.fullmatch(self.security_series) is None
        ):
            raise ValueError("security_series must be canonical NSE series text")
        if type(self.isin) is not str or NSE_EQUITY_ISIN_PATTERN.fullmatch(self.isin) is None:
            raise ValueError("isin must be a canonical Indian equity ISIN")
        if type(self.valid_from) is not date or type(self.valid_through) is not date:
            raise TypeError("binding validity values must be exact dates")
        if self.valid_through < self.valid_from:
            raise ValueError("binding validity interval is reversed")
        if (
            type(self.source_snapshot_ids) is not tuple
            or not self.source_snapshot_ids
            or self.source_snapshot_ids != tuple(sorted(set(self.source_snapshot_ids)))
            or any(
                type(value) is not str or SHA256_IDENTIFIER.fullmatch(value) is None
                for value in self.source_snapshot_ids
            )
        ):
            raise ValueError(
                "source_snapshot_ids must be a non-empty sorted unique SHA-256 tuple"
            )

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
        self._validate()
        if self.binding_id != self._calculated_id():
            raise ValueError("historical instrument binding identity failed")


@dataclass(frozen=True, slots=True)
class HistoricalDailyRequest:
    """Exact historical sessions requested for one point-in-time listing binding."""

    binding: HistoricalInstrumentBinding
    sessions: tuple[date, ...]
    requested_at: datetime
    request_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "requested_at", self.requested_at.astimezone(timezone.utc))
        object.__setattr__(self, "request_id", self._calculated_id())

    def _validate(self) -> None:
        if type(self.binding) is not HistoricalInstrumentBinding:
            raise TypeError("binding must be an exact HistoricalInstrumentBinding")
        self.binding.verify_content_identity()
        require_aware(self.requested_at, "historical_request.requested_at")
        if (
            type(self.sessions) is not tuple
            or not self.sessions
            or len(self.sessions) > MAXIMUM_HISTORICAL_SESSIONS
            or any(type(value) is not date for value in self.sessions)
            or self.sessions != tuple(sorted(set(self.sessions)))
        ):
            raise ValueError(
                "sessions must be a non-empty sorted unique bounded exact-date tuple"
            )
        if (
            self.sessions[0] < self.binding.valid_from
            or self.sessions[-1] > self.binding.valid_through
        ):
            raise ValueError("requested sessions exceed the listing binding interval")
        request_session = self.requested_at.astimezone(INDIA_STANDARD_TIME).date()
        if self.sessions[-1] >= request_session:
            raise ValueError("historical requests cannot include the current or a future session")

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "historical-daily-request/v1",
                "binding_id": self.binding.binding_id,
                "sessions": self.sessions,
                "requested_at": self.requested_at,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.request_id != self._calculated_id():
            raise ValueError("historical daily request identity failed")


@dataclass(frozen=True, slots=True)
class HistoricalDailyCandle:
    session: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    open_interest: int | None = None

    def __post_init__(self) -> None:
        if type(self.session) is not date:
            raise TypeError("historical candle session must be an exact date")
        for name in ("open", "high", "low", "close"):
            value = getattr(self, name)
            if type(value) is not Decimal or not value.is_finite() or value <= ZERO:
                raise ValueError(f"historical candle {name} must be a positive finite Decimal")
        if self.high < max(self.open, self.low, self.close):
            raise ValueError("historical candle high is inconsistent with OHLC")
        if self.low > min(self.open, self.high, self.close):
            raise ValueError("historical candle low is inconsistent with OHLC")
        if type(self.volume) is not int or self.volume < 0:
            raise ValueError("historical candle volume must be a non-negative exact integer")
        if self.open_interest is not None and (
            type(self.open_interest) is not int or self.open_interest < 0
        ):
            raise ValueError(
                "historical candle open_interest must be a non-negative exact integer"
            )


@dataclass(frozen=True, slots=True)
class HistoricalResponsePage:
    first_session: date
    last_session: date
    payload_sha256: str
    row_count: int

    def __post_init__(self) -> None:
        if type(self.first_session) is not date or type(self.last_session) is not date:
            raise TypeError("response page boundaries must be exact dates")
        if self.last_session < self.first_session:
            raise ValueError("response page boundaries are reversed")
        if type(self.payload_sha256) is not str or SHA256_IDENTIFIER.fullmatch(
            self.payload_sha256
        ) is None:
            raise ValueError("response page payload_sha256 must be lowercase SHA-256")
        if type(self.row_count) is not int or self.row_count <= 0:
            raise ValueError("response page row_count must be a positive exact integer")


@dataclass(frozen=True, slots=True)
class HistoricalDailyCandleBatch:
    request: HistoricalDailyRequest
    observed_at: datetime
    provider_version: str
    candles: tuple[HistoricalDailyCandle, ...]
    response_pages: tuple[HistoricalResponsePage, ...]
    batch_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "observed_at", self.observed_at.astimezone(timezone.utc))
        object.__setattr__(self, "batch_id", self._calculated_id())

    def _validate(self) -> None:
        if type(self.request) is not HistoricalDailyRequest:
            raise TypeError("request must be an exact HistoricalDailyRequest")
        self.request.verify_content_identity()
        require_aware(self.observed_at, "historical_batch.observed_at")
        if self.observed_at < self.request.requested_at:
            raise ValueError("historical batch cannot predate its request")
        if type(self.provider_version) is not str or not self.provider_version.strip():
            raise ValueError("provider_version is required")
        if type(self.candles) is not tuple or any(
            type(value) is not HistoricalDailyCandle for value in self.candles
        ):
            raise TypeError("candles must be an exact HistoricalDailyCandle tuple")
        if tuple(value.session for value in self.candles) != self.request.sessions:
            raise ValueError("historical candles must exactly cover the requested sessions")
        if type(self.response_pages) is not tuple or not self.response_pages or any(
            type(value) is not HistoricalResponsePage for value in self.response_pages
        ):
            raise TypeError("response_pages must be a non-empty exact tuple")
        covered_sessions: list[date] = []
        previous_last: date | None = None
        for page in self.response_pages:
            if previous_last is not None and page.first_session <= previous_last:
                raise ValueError("historical response pages overlap or are unordered")
            page_sessions = tuple(
                session
                for session in self.request.sessions
                if page.first_session <= session <= page.last_session
            )
            if (
                not page_sessions
                or page_sessions[0] != page.first_session
                or page_sessions[-1] != page.last_session
                or len(page_sessions) != page.row_count
            ):
                raise ValueError("historical response page coverage is inconsistent")
            covered_sessions.extend(page_sessions)
            previous_last = page.last_session
        if tuple(covered_sessions) != self.request.sessions:
            raise ValueError("historical response pages do not cover the full request")

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "batch_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.batch_id != self._calculated_id():
            raise ValueError("historical daily batch identity failed")

    @property
    def provider(self) -> str:
        return self.request.binding.provider

    @property
    def record_count(self) -> int:
        return len(self.candles)


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


MAXIMUM_AGGREGATED_QUOTE_KEYS = 10000


def require_canonical_listing_keys(
    listing_keys: object,
    *,
    maximum_keys: int = MAXIMUM_QUOTE_KEYS,
) -> None:
    """Validate an exact, non-empty, sorted, unique, canonical-uppercase key tuple."""

    if type(maximum_keys) is not int or maximum_keys <= 0:
        raise ValueError("maximum_keys must be a positive exact integer")
    if type(listing_keys) is not tuple or not listing_keys:
        raise ValueError("listing_keys must be a non-empty exact tuple")
    if len(listing_keys) > maximum_keys:
        raise ValueError(f"listing_keys cannot exceed {maximum_keys} keys")
    for key in listing_keys:
        if type(key) is not str or LISTING_KEY_PATTERN.fullmatch(key) is None:
            raise ValueError(
                "listing_keys must be canonical uppercase NSE:TRADINGSYMBOL text"
            )
    if len(set(listing_keys)) != len(listing_keys):
        raise ValueError("listing_keys must be unique")
    if listing_keys != tuple(sorted(listing_keys)):
        raise ValueError("listing_keys must already be in sorted canonical order")


def _require_ordered_positive_depth(
    levels: tuple[KiteDepthLevel, ...], *, descending: bool, side_name: str
) -> None:
    positive_prices = [level.price for level in levels if level.price > ZERO]
    for previous, current in zip(positive_prices, positive_prices[1:]):
        if descending and current > previous:
            raise ValueError(f"{side_name} positive prices must be non-increasing")
        if not descending and current < previous:
            raise ValueError(f"{side_name} positive prices must be non-decreasing")


@dataclass(frozen=True, slots=True)
class KiteDepthLevel:
    """One order-book depth level. Zero price is only valid when empty."""

    price: Decimal
    quantity: int
    orders: int

    def __post_init__(self) -> None:
        self._validate()

    def _validate(self) -> None:
        if type(self.price) is not Decimal or not self.price.is_finite():
            raise ValueError("depth price must be a finite Decimal")
        if self.price < ZERO:
            raise ValueError("depth price cannot be negative")
        if type(self.quantity) is not int or self.quantity < 0:
            raise ValueError("depth quantity must be a non-negative integer")
        if type(self.orders) is not int or self.orders < 0:
            raise ValueError("depth orders must be a non-negative integer")
        if self.price == ZERO and (self.quantity != 0 or self.orders != 0):
            raise ValueError("a zero-priced depth level must have zero quantity and orders")

    def verify_content_identity(self) -> None:
        self._validate()


@dataclass(frozen=True, slots=True)
class KiteFullQuote:
    """One point-in-time full quote snapshot for exactly one requested listing key."""

    listing_key: str
    instrument_token: int
    exchange_timestamp: datetime
    last_trade_time: datetime | None
    last_price: Decimal
    lower_circuit_limit: Decimal
    upper_circuit_limit: Decimal
    depth_buy: tuple[KiteDepthLevel, ...]
    depth_sell: tuple[KiteDepthLevel, ...]

    def __post_init__(self) -> None:
        self._validate()

    def _validate(self) -> None:
        if type(self.listing_key) is not str or LISTING_KEY_PATTERN.fullmatch(
            self.listing_key
        ) is None:
            raise ValueError("quote listing_key must be canonical NSE:TRADINGSYMBOL text")
        if type(self.instrument_token) is not int or self.instrument_token <= 0:
            raise ValueError("quote instrument_token must be a positive integer")
        require_aware(self.exchange_timestamp, "quote.exchange_timestamp")
        if self.last_trade_time is not None:
            require_aware(self.last_trade_time, "quote.last_trade_time")
            if self.last_trade_time > self.exchange_timestamp:
                raise ValueError("last_trade_time cannot be after the exchange timestamp")
        for name in ("last_price", "lower_circuit_limit", "upper_circuit_limit"):
            value = getattr(self, name)
            if type(value) is not Decimal or not value.is_finite():
                raise ValueError(f"quote.{name} must be a finite Decimal")
            if value <= ZERO:
                raise ValueError(f"quote.{name} must be positive")
        if not (self.lower_circuit_limit <= self.last_price <= self.upper_circuit_limit):
            raise ValueError("last_price must be within its circuit limits")
        for side_name, side in (
            ("depth_buy", self.depth_buy),
            ("depth_sell", self.depth_sell),
        ):
            if type(side) is not tuple or any(
                type(level) is not KiteDepthLevel for level in side
            ):
                raise TypeError(f"{side_name} must be an exact KiteDepthLevel tuple")
            for level in side:
                level.verify_content_identity()
        _require_ordered_positive_depth(self.depth_buy, descending=True, side_name="depth_buy")
        _require_ordered_positive_depth(
            self.depth_sell, descending=False, side_name="depth_sell"
        )
        best_bid = self.best_bid
        best_ask = self.best_ask
        if best_bid is not None and best_ask is not None and best_ask < best_bid:
            raise ValueError("crossed depth: best ask is below best bid")

    def verify_content_identity(self) -> None:
        self._validate()

    @property
    def best_bid(self) -> Decimal | None:
        for level in self.depth_buy:
            if level.price > ZERO:
                return level.price
        return None

    @property
    def best_ask(self) -> Decimal | None:
        for level in self.depth_sell:
            if level.price > ZERO:
                return level.price
        return None

    @property
    def has_two_sided_depth(self) -> bool:
        return self.best_bid is not None and self.best_ask is not None

    @property
    def mid_price(self) -> Decimal | None:
        best_bid = self.best_bid
        best_ask = self.best_ask
        if best_bid is None or best_ask is None:
            return None
        return (best_bid + best_ask) / Decimal("2")

    @property
    def spread_bps(self) -> Decimal | None:
        best_bid = self.best_bid
        best_ask = self.best_ask
        mid = self.mid_price
        if best_bid is None or best_ask is None or mid is None:
            return None
        return (best_ask - best_bid) / mid * Decimal("10000")

    @property
    def at_lower_circuit(self) -> bool:
        return self.last_price == self.lower_circuit_limit

    @property
    def at_upper_circuit(self) -> bool:
        return self.last_price == self.upper_circuit_limit


@dataclass(frozen=True, slots=True)
class FullQuoteBatch:
    """Exact, content-addressed coverage of one canonical full-quote request."""

    requested_keys: tuple[str, ...]
    requested_at: datetime
    observed_at: datetime
    provider_version: str
    quotes: tuple[KiteFullQuote, ...]
    batch_id: str = field(init=False)

    def __post_init__(self) -> None:
        require_canonical_listing_keys(
            self.requested_keys,
            maximum_keys=MAXIMUM_AGGREGATED_QUOTE_KEYS,
        )
        require_aware(self.requested_at, "quote_batch.requested_at")
        require_aware(self.observed_at, "quote_batch.observed_at")
        object.__setattr__(self, "requested_at", self.requested_at.astimezone(timezone.utc))
        object.__setattr__(self, "observed_at", self.observed_at.astimezone(timezone.utc))
        if self.requested_at > self.observed_at:
            raise ValueError("requested_at cannot be after observed_at")
        if not self.provider_version.strip():
            raise ValueError("provider_version is required")
        if type(self.quotes) is not tuple or any(
            type(value) is not KiteFullQuote for value in self.quotes
        ):
            raise TypeError("quotes must be an exact KiteFullQuote tuple")
        for value in self.quotes:
            value.verify_content_identity()
        if tuple(value.listing_key for value in self.quotes) != self.requested_keys:
            raise ValueError("quotes must exactly cover requested_keys in request order")
        tokens = [value.instrument_token for value in self.quotes]
        if len(tokens) != len(set(tokens)):
            raise ValueError("quote batch contains duplicate instrument tokens")
        if any(value.exchange_timestamp > self.observed_at for value in self.quotes):
            raise ValueError("quote batch contains a future-known exchange timestamp")
        object.__setattr__(self, "batch_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "batch_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        for value in self.quotes:
            if type(value) is not KiteFullQuote:
                raise TypeError("quote batch contains an invalid quote")
            value.verify_content_identity()
        if self.batch_id != self._calculated_id():
            raise ValueError("quote batch content identity verification failed")
