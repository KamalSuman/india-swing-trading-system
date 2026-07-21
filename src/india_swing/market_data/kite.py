from __future__ import annotations

import random
import re
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation
from importlib import metadata
from time import monotonic, sleep
from typing import Any, Protocol

from india_swing.domain.models import INDIA_STANDARD_TIME

from .config import KiteCredentials
from .models import (
    DailyCandle,
    DailyCandleBatch,
    FullQuoteBatch,
    InstrumentBatch,
    KiteDepthLevel,
    KiteFullQuote,
    KiteInstrument,
    NseSessionFinality,
    MAXIMUM_QUOTE_KEYS,
    require_canonical_listing_keys,
)


PINNED_KITE_SDK_VERSION = "5.2.0"


class KiteReadClient(Protocol):
    """The read-only subset of the official SDK used by this application."""

    def instruments(self, exchange: str | None = None) -> list[dict[str, Any]]: ...

    def historical_data(
        self,
        instrument_token: int,
        from_date: str,
        to_date: str,
        interval: str,
        continuous: bool = False,
        oi: bool = False,
    ) -> list[dict[str, Any]]: ...

    def quote(self, *instruments: str) -> dict[str, dict[str, Any]]: ...


class RequestRateLimiter(Protocol):
    def wait(self, operation: str) -> None: ...


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay_seconds: float = 0.5
    maximum_delay_seconds: float = 4.0
    jitter_seconds: float = 0.25

    def __post_init__(self) -> None:
        if self.max_attempts <= 0:
            raise ValueError("max_attempts must be positive")
        if min(
            self.base_delay_seconds,
            self.maximum_delay_seconds,
            self.jitter_seconds,
        ) < 0:
            raise ValueError("retry delays cannot be negative")


class EndpointRateLimiter:
    """Process-local pacing for the currently documented Kite endpoint limits."""

    def __init__(
        self,
        *,
        monotonic_clock: Callable[[], float] = monotonic,
        sleeper: Callable[[float], None] = sleep,
    ) -> None:
        self._clock = monotonic_clock
        self._sleep = sleeper
        self._lock = threading.Lock()
        self._last_request: dict[str, float] = {}
        self._requests_per_second = {
            "historical_data": 3.0,
            "instruments": 10.0,
            "quote": 1.0,
        }

    def wait(self, operation: str) -> None:
        rate = self._requests_per_second.get(operation, 10.0)
        minimum_interval = 1.0 / rate
        with self._lock:
            now = self._clock()
            previous = self._last_request.get(operation)
            if previous is not None:
                remaining = minimum_interval - (now - previous)
                if remaining > 0:
                    self._sleep(remaining)
                    now = self._clock()
            self._last_request[operation] = now


class KiteMarketDataError(RuntimeError):
    def __init__(self, operation: str, upstream_type: str) -> None:
        self.operation = operation
        self.upstream_type = upstream_type
        super().__init__(f"Kite {operation} failed ({upstream_type})")


class KiteAuthenticationError(KiteMarketDataError):
    pass


class KitePermissionError(KiteMarketDataError):
    pass


class KiteRateLimitError(KiteMarketDataError):
    pass


class KiteRequestError(KiteMarketDataError):
    pass


class KiteAvailabilityError(KiteMarketDataError):
    pass


class KiteDataIntegrityError(KiteMarketDataError):
    pass


class KiteDependencyError(RuntimeError):
    pass


class MarketSessionNotFinalError(ValueError):
    pass


def _decimal(value: Any, field_name: str, *, allow_blank: bool = False) -> Decimal | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        if allow_blank:
            return None
        raise ValueError(f"{field_name} is required")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} is not a valid decimal") from exc
    if not parsed.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return parsed


def _date(value: Any, field_name: str) -> date | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value.strip())
        except ValueError as exc:
            raise ValueError(f"{field_name} is not an ISO date") from exc
    raise ValueError(f"{field_name} has an unsupported type")


def _integer(value: Any, field_name: str, *, minimum: int = 0) -> int:
    if type(value) is int:
        parsed = value
    elif isinstance(value, str) and re.fullmatch(r"[+-]?\d+", value.strip()):
        parsed = int(value.strip())
    else:
        raise ValueError(f"{field_name} must be an integer")
    if parsed < minimum:
        raise ValueError(f"{field_name} must be at least {minimum}")
    return parsed


def _required_text(value: Any, field_name: str, *, uppercase: bool = False) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required text")
    parsed = value.strip()
    return parsed.upper() if uppercase else parsed


def _optional_text(value: Any, field_name: str) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be text")
    return value.strip()


def _identifier_text(value: Any, field_name: str) -> str:
    if type(value) is int:
        if value < 0:
            raise ValueError(f"{field_name} cannot be negative")
        return str(value)
    return _required_text(value, field_name)


def _datetime(value: Any, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"{field_name} is not an ISO timestamp") from exc
    else:
        raise ValueError(f"{field_name} has an unsupported type")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return parsed


def _exchange_timestamp(value: Any, field_name: str) -> datetime:
    """Interpret a Kite exchange timestamp, attaching IST only to a naive SDK value.

    Kite documents exchange timestamps as IST. The pinned SDK (5.2.0) converts
    19-character timestamp strings into naive datetimes; that exact naive form
    is what gets Asia/Kolkata attached here. Any already-aware value must
    already carry the IST offset -- a different offset is rejected rather than
    silently reinterpreted.
    """

    if type(value) is not datetime:
        raise ValueError(f"{field_name} must be the pinned SDK datetime type")
    parsed = value
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=INDIA_STANDARD_TIME)
    if parsed.utcoffset() != INDIA_STANDARD_TIME.utcoffset(None):
        raise ValueError(f"{field_name} has an inconsistent timezone offset")
    return parsed.astimezone(INDIA_STANDARD_TIME)


class KiteMarketDataAdapter:
    """Validated read-only wrapper around Zerodha's official Python SDK."""

    def __init__(
        self,
        client: KiteReadClient,
        *,
        sdk_version: str,
        clock: Callable[[], datetime] | None = None,
        rate_limiter: RequestRateLimiter | None = None,
        retry_policy: RetryPolicy | None = None,
        sleeper: Callable[[float], None] = sleep,
        jitter: Callable[[float, float], float] = random.uniform,
    ) -> None:
        if not sdk_version.strip():
            raise ValueError("sdk_version is required")
        self._client = client
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._rate_limiter = rate_limiter or EndpointRateLimiter()
        self._retry_policy = retry_policy or RetryPolicy()
        self._sleep = sleeper
        self._jitter = jitter
        self.sdk_version = sdk_version
        self.model_version = f"kiteconnect/{sdk_version}"

    @classmethod
    def from_official_sdk(
        cls,
        credentials: KiteCredentials,
        *,
        required_version: str = PINNED_KITE_SDK_VERSION,
        clock: Callable[[], datetime] | None = None,
    ) -> KiteMarketDataAdapter:
        try:
            installed_version = metadata.version("kiteconnect")
            from kiteconnect import KiteConnect
        except (metadata.PackageNotFoundError, ImportError) as exc:
            raise KiteDependencyError(
                "install the pinned market-data extra: pip install -e .[kite]"
            ) from exc
        if installed_version != required_version:
            raise KiteDependencyError(
                f"kiteconnect {required_version} is required; found {installed_version}"
            )
        client = KiteConnect(api_key=credentials.api_key())
        client.set_access_token(credentials.access_token())
        return cls(client, sdk_version=installed_version, clock=clock)

    @property
    def identity_material(self) -> dict[str, object]:
        return {
            "provider": "ZERODHA_KITE",
            "sdk_version": self.sdk_version,
            "api_version": 3,
            "capabilities": ("instruments", "historical_data", "quote"),
            "order_access": False,
        }

    def fetch_instruments(self, exchange: str = "NSE") -> InstrumentBatch:
        exchange = exchange.strip().upper()
        if not exchange:
            raise ValueError("exchange is required")
        raw_rows = self._call("instruments", lambda: self._client.instruments(exchange))
        if not isinstance(raw_rows, Sequence) or isinstance(raw_rows, (str, bytes)):
            raise KiteDataIntegrityError("instruments", "InvalidResponseType")
        if not raw_rows:
            raise KiteDataIntegrityError("instruments", "EmptyInstrumentDump")

        instruments: list[KiteInstrument] = []
        for index, row in enumerate(raw_rows):
            if not isinstance(row, Mapping):
                raise KiteDataIntegrityError("instruments", f"InvalidRow[{index}]")
            try:
                instrument = self._instrument(row)
            except (KeyError, TypeError, ValueError) as exc:
                raise KiteDataIntegrityError(
                    "instruments",
                    f"InvalidRow[{index}]:{type(exc).__name__}",
                ) from None
            if instrument.exchange != exchange:
                raise KiteDataIntegrityError("instruments", "UnexpectedExchange")
            instruments.append(instrument)

        instruments.sort(key=lambda item: (item.listing_key, item.instrument_token))
        observed_at = self._observed_at()
        try:
            return InstrumentBatch(
                exchange=exchange,
                observed_at=observed_at,
                provider_version=self.model_version,
                instruments=tuple(instruments),
            )
        except ValueError as exc:
            raise KiteDataIntegrityError(
                "instruments",
                f"InvalidBatch:{type(exc).__name__}",
            ) from None

    def fetch_daily_candle(
        self,
        instrument_token: int,
        session: date,
        *,
        session_finality: NseSessionFinality,
    ) -> DailyCandleBatch:
        if type(instrument_token) is not int or instrument_token <= 0:
            raise ValueError("instrument_token must be a positive integer")
        if session_finality.session != session:
            raise ValueError("session finality does not belong to the requested session")
        request_started_at = self._observed_at()
        if request_started_at < session_finality.data_ready_at:
            raise MarketSessionNotFinalError(
                "daily candles cannot be collected before the session data-ready floor"
            )

        from_value = datetime.combine(session, time.min).isoformat(sep=" ")
        to_value = datetime.combine(session, time.max.replace(microsecond=0)).isoformat(
            sep=" "
        )
        raw_rows = self._call(
            "historical_data",
            lambda: self._client.historical_data(
                instrument_token,
                from_value,
                to_value,
                "day",
                continuous=False,
                oi=False,
            ),
        )
        if not isinstance(raw_rows, Sequence) or isinstance(raw_rows, (str, bytes)):
            raise KiteDataIntegrityError("historical_data", "InvalidResponseType")

        candles: list[DailyCandle] = []
        for index, row in enumerate(raw_rows):
            if not isinstance(row, Mapping):
                raise KiteDataIntegrityError("historical_data", f"InvalidRow[{index}]")
            try:
                candle = self._candle(instrument_token, row)
            except (KeyError, TypeError, ValueError) as exc:
                raise KiteDataIntegrityError(
                    "historical_data",
                    f"InvalidRow[{index}]:{type(exc).__name__}",
                ) from None
            candles.append(candle)

        if len(candles) != 1 or candles[0].session != session:
            raise KiteDataIntegrityError("historical_data", "MissingOrAmbiguousSession")
        observed_at = self._observed_at()
        if observed_at < request_started_at:
            raise KiteDataIntegrityError("historical_data", "NonMonotonicAcquisitionClock")
        try:
            return DailyCandleBatch(
                instrument_token=instrument_token,
                session_finality=session_finality,
                observed_at=observed_at,
                provider_version=self.model_version,
                candles=tuple(candles),
            )
        except ValueError as exc:
            raise KiteDataIntegrityError(
                "historical_data",
                f"InvalidBatch:{type(exc).__name__}",
            ) from None

    def fetch_full_quotes(self, listing_keys: tuple[str, ...]) -> FullQuoteBatch:
        try:
            require_canonical_listing_keys(
                listing_keys,
                maximum_keys=MAXIMUM_QUOTE_KEYS,
            )
        except ValueError as exc:
            raise KiteDataIntegrityError(
                "quote",
                f"InvalidRequest:{type(exc).__name__}",
            ) from None

        request_started_at = self._observed_at()
        raw_response = self._call("quote", lambda: self._client.quote(*listing_keys))
        if not isinstance(raw_response, Mapping):
            raise KiteDataIntegrityError("quote", "InvalidResponseType")
        if not raw_response:
            raise KiteDataIntegrityError("quote", "EmptyQuoteResponse")
        if set(raw_response.keys()) != set(listing_keys):
            raise KiteDataIntegrityError("quote", "IncompleteQuoteCoverage")

        quotes: list[KiteFullQuote] = []
        for key in listing_keys:
            row = raw_response[key]
            if not isinstance(row, Mapping):
                raise KiteDataIntegrityError("quote", "InvalidRowType")
            try:
                quote = self._full_quote(key, row)
            except (KeyError, TypeError, ValueError) as exc:
                raise KiteDataIntegrityError(
                    "quote",
                    f"InvalidRow:{type(exc).__name__}",
                ) from None
            quotes.append(quote)

        observed_at = self._observed_at()
        if observed_at < request_started_at:
            raise KiteDataIntegrityError("quote", "NonMonotonicAcquisitionClock")
        try:
            return FullQuoteBatch(
                requested_keys=listing_keys,
                requested_at=request_started_at,
                observed_at=observed_at,
                provider_version=self.model_version,
                quotes=tuple(quotes),
            )
        except ValueError as exc:
            raise KiteDataIntegrityError(
                "quote",
                f"InvalidBatch:{type(exc).__name__}",
            ) from None

    def _observed_at(self) -> datetime:
        observed_at = self._clock()
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("market-data clock must return a timezone-aware datetime")
        return observed_at

    @staticmethod
    def _instrument(row: Mapping[str, Any]) -> KiteInstrument:
        strike = _decimal(row.get("strike"), "strike", allow_blank=True)
        return KiteInstrument(
            instrument_token=_integer(row["instrument_token"], "instrument_token", minimum=1),
            exchange_token=_identifier_text(row["exchange_token"], "exchange_token"),
            tradingsymbol=_required_text(
                row["tradingsymbol"],
                "tradingsymbol",
                uppercase=True,
            ),
            name=_optional_text(row.get("name"), "name"),
            dump_last_price=_decimal(row.get("last_price", 0), "last_price")
            or Decimal("0"),
            expiry=_date(row.get("expiry"), "expiry"),
            strike=strike,
            tick_size=_decimal(row["tick_size"], "tick_size") or Decimal("0"),
            lot_size=_integer(row["lot_size"], "lot_size", minimum=1),
            instrument_type=_required_text(
                row["instrument_type"],
                "instrument_type",
                uppercase=True,
            ),
            segment=_required_text(row["segment"], "segment", uppercase=True),
            exchange=_required_text(row["exchange"], "exchange", uppercase=True),
        )

    @staticmethod
    def _candle(instrument_token: int, row: Mapping[str, Any]) -> DailyCandle:
        open_interest = row.get("oi")
        candle = DailyCandle(
            instrument_token=instrument_token,
            timestamp=_datetime(row["date"], "candle.date"),
            open=_decimal(row["open"], "open") or Decimal("0"),
            high=_decimal(row["high"], "high") or Decimal("0"),
            low=_decimal(row["low"], "low") or Decimal("0"),
            close=_decimal(row["close"], "close") or Decimal("0"),
            volume=_integer(row["volume"], "volume", minimum=0),
            open_interest=(
                _integer(open_interest, "open_interest", minimum=0)
                if open_interest is not None
                else None
            ),
        )
        return candle

    @staticmethod
    def _full_quote(listing_key: str, row: Mapping[str, Any]) -> KiteFullQuote:
        depth = row.get("depth")
        if not isinstance(depth, Mapping):
            raise ValueError("quote.depth is required")
        last_trade_time = row.get("last_trade_time")
        return KiteFullQuote(
            listing_key=listing_key,
            instrument_token=_integer(row["instrument_token"], "instrument_token", minimum=1),
            exchange_timestamp=_exchange_timestamp(row["timestamp"], "quote.timestamp"),
            last_trade_time=(
                _exchange_timestamp(last_trade_time, "quote.last_trade_time")
                if last_trade_time is not None
                else None
            ),
            last_price=_decimal(row["last_price"], "last_price") or Decimal("0"),
            lower_circuit_limit=_decimal(row["lower_circuit_limit"], "lower_circuit_limit")
            or Decimal("0"),
            upper_circuit_limit=_decimal(row["upper_circuit_limit"], "upper_circuit_limit")
            or Decimal("0"),
            depth_buy=KiteMarketDataAdapter._depth_levels(depth.get("buy"), "depth.buy"),
            depth_sell=KiteMarketDataAdapter._depth_levels(depth.get("sell"), "depth.sell"),
        )

    @staticmethod
    def _depth_levels(rows: Any, field_name: str) -> tuple[KiteDepthLevel, ...]:
        if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
            raise ValueError(f"{field_name} must be a list")
        levels: list[KiteDepthLevel] = []
        for row in rows:
            if not isinstance(row, Mapping):
                raise ValueError(f"{field_name} entry must be a mapping")
            levels.append(
                KiteDepthLevel(
                    price=_decimal(row.get("price", 0), "depth.price") or Decimal("0"),
                    quantity=_integer(row.get("quantity", 0), "depth.quantity", minimum=0),
                    orders=_integer(row.get("orders", 0), "depth.orders", minimum=0),
                )
            )
        return tuple(levels)

    def _call(self, operation: str, callback: Callable[[], Any]) -> Any:
        for attempt in range(1, self._retry_policy.max_attempts + 1):
            self._rate_limiter.wait(operation)
            try:
                return callback()
            except KiteMarketDataError:
                raise
            except Exception as exc:
                error = self._classify_error(operation, exc)
                retryable = isinstance(
                    error,
                    (KiteRateLimitError, KiteAvailabilityError),
                )
                if not retryable or attempt == self._retry_policy.max_attempts:
                    raise error from None
                exponential = self._retry_policy.base_delay_seconds * (2 ** (attempt - 1))
                delay = min(exponential, self._retry_policy.maximum_delay_seconds)
                if self._retry_policy.jitter_seconds:
                    delay += self._jitter(0, self._retry_policy.jitter_seconds)
                self._sleep(delay)
        raise AssertionError("retry loop ended unexpectedly")

    @staticmethod
    def _classify_error(operation: str, exc: Exception) -> KiteMarketDataError:
        upstream_type = type(exc).__name__
        status = getattr(exc, "code", None)
        if upstream_type == "TokenException":
            error_type = KiteAuthenticationError
        elif upstream_type == "PermissionException" or status == 403:
            error_type = KitePermissionError
        elif upstream_type == "DataException":
            error_type = KiteDataIntegrityError
        elif status == 429 or upstream_type == "TooManyRequestsException":
            error_type = KiteRateLimitError
        elif upstream_type in {"InputException", "UserException"} or status in {
            400,
            404,
            405,
            410,
        }:
            error_type = KiteRequestError
        else:
            error_type = KiteAvailabilityError
        return error_type(operation, upstream_type)
