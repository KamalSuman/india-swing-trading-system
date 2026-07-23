from __future__ import annotations

import hashlib
import json
import random
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from time import monotonic, sleep
from typing import Protocol
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

from india_swing.domain.models import INDIA_STANDARD_TIME

from .config import UpstoxCredentials
from .models import (
    HistoricalDailyCandle,
    HistoricalDailyCandleBatch,
    HistoricalDailyRequest,
    HistoricalResponsePage,
)
from .provider import RequestRateLimiter, RetryPolicy


UPSTOX_PROVIDER = "UPSTOX"
UPSTOX_HISTORICAL_API_BASE_URL = "https://api.upstox.com/v3/historical-candle"
UPSTOX_HISTORICAL_PROVIDER_VERSION = "upstox-rest/v3-historical-candles"
MAXIMUM_UPSTOX_RESPONSE_BYTES = 8 * 1024 * 1024
MAXIMUM_UPSTOX_DAILY_RANGE_DAYS = 3650
UPSTOX_HTTP_TIMEOUT_SECONDS = 30.0


@dataclass(frozen=True, slots=True)
class UpstoxHttpResponse:
    status_code: int
    body: bytes

    def __post_init__(self) -> None:
        if type(self.status_code) is not int or not 100 <= self.status_code <= 599:
            raise ValueError("status_code must be an exact HTTP status")
        if type(self.body) is not bytes:
            raise TypeError("body must be exact bytes")


class UpstoxHttpTransport(Protocol):
    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        timeout_seconds: float,
        maximum_bytes: int,
    ) -> UpstoxHttpResponse: ...


class UrllibUpstoxHttpTransport:
    """Small real HTTPS transport; tests inject a fake and never contact Upstox."""

    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        timeout_seconds: float,
        maximum_bytes: int,
    ) -> UpstoxHttpResponse:
        request = Request(url, headers=dict(headers), method="GET")
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                return UpstoxHttpResponse(
                    status_code=response.status,
                    body=response.read(maximum_bytes + 1),
                )
        except HTTPError as exc:
            return UpstoxHttpResponse(
                status_code=exc.code,
                body=exc.read(maximum_bytes + 1),
            )


class UpstoxEndpointRateLimiter:
    """Conservative one-request/second pacing for long historical backfills."""

    def __init__(
        self,
        *,
        monotonic_clock: Callable[[], float] = monotonic,
        sleeper: Callable[[float], None] = sleep,
    ) -> None:
        self._clock = monotonic_clock
        self._sleep = sleeper
        self._lock = threading.Lock()
        self._last_request: float | None = None

    def wait(self, operation: str) -> None:
        if operation != "historical_data":
            raise ValueError("unsupported Upstox rate-limit operation")
        with self._lock:
            now = self._clock()
            if self._last_request is not None:
                remaining = 1.0 - (now - self._last_request)
                if remaining > 0:
                    self._sleep(remaining)
                    now = self._clock()
            self._last_request = now


class UpstoxMarketDataError(RuntimeError):
    def __init__(self, operation: str, upstream_type: str) -> None:
        self.operation = operation
        self.upstream_type = upstream_type
        super().__init__(f"Upstox {operation} failed ({upstream_type})")


class UpstoxAuthenticationError(UpstoxMarketDataError):
    pass


class UpstoxPermissionError(UpstoxMarketDataError):
    pass


class UpstoxRateLimitError(UpstoxMarketDataError):
    pass


class UpstoxRequestError(UpstoxMarketDataError):
    pass


class UpstoxAvailabilityError(UpstoxMarketDataError):
    pass


class UpstoxDataIntegrityError(UpstoxMarketDataError):
    pass


def _reject_json_constant(_: str) -> None:
    raise ValueError("non-finite JSON number")


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON key")
        value[key] = item
    return value


def _decimal(value: object, field_name: str) -> Decimal:
    if type(value) is Decimal:
        parsed = value
    elif type(value) is int:
        parsed = Decimal(value)
    else:
        raise ValueError(f"{field_name} must be a decimal") from None
    if not parsed.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return parsed


def _integer(value: object, field_name: str) -> int:
    if type(value) is not int:
        raise ValueError(f"{field_name} must be an integer")
    if value < 0:
        raise ValueError(f"{field_name} cannot be negative")
    return value


def _daily_session(value: object) -> date:
    if type(value) is not str:
        raise ValueError("daily candle timestamp must be text")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("daily candle timestamp is not ISO-8601") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("daily candle timestamp must be timezone-aware")
    if parsed.utcoffset() != INDIA_STANDARD_TIME.utcoffset(None):
        raise ValueError("daily candle timestamp must use the Asia/Kolkata offset")
    if parsed.timetz().replace(tzinfo=None) != time.min:
        raise ValueError("daily candle timestamp must be the session boundary")
    return parsed.date()


def _session_chunks(sessions: tuple[date, ...]) -> tuple[tuple[date, ...], ...]:
    chunks: list[tuple[date, ...]] = []
    start = 0
    while start < len(sessions):
        end = start + 1
        while (
            end < len(sessions)
            and (sessions[end] - sessions[start]).days
            < MAXIMUM_UPSTOX_DAILY_RANGE_DAYS
        ):
            end += 1
        chunks.append(sessions[start:end])
        start = end
    return tuple(chunks)


class UpstoxHistoricalDataAdapter:
    """Read-only Upstox V3 daily history translated to canonical market models."""

    def __init__(
        self,
        credentials: UpstoxCredentials,
        *,
        transport: UpstoxHttpTransport | None = None,
        clock: Callable[[], datetime] | None = None,
        rate_limiter: RequestRateLimiter | None = None,
        retry_policy: RetryPolicy | None = None,
        sleeper: Callable[[float], None] = sleep,
        jitter: Callable[[float, float], float] = random.uniform,
    ) -> None:
        if type(credentials) is not UpstoxCredentials:
            raise TypeError("credentials must be exact UpstoxCredentials")
        self._credentials = credentials
        self._transport = transport or UrllibUpstoxHttpTransport()
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._rate_limiter = rate_limiter or UpstoxEndpointRateLimiter()
        self._retry_policy = retry_policy or RetryPolicy()
        self._sleep = sleeper
        self._jitter = jitter

    @property
    def provider(self) -> str:
        return UPSTOX_PROVIDER

    @property
    def provider_version(self) -> str:
        return UPSTOX_HISTORICAL_PROVIDER_VERSION

    @property
    def identity_material(self) -> dict[str, object]:
        return {
            "provider": self.provider,
            "api_version": 3,
            "capabilities": ("historical_daily",),
            "order_access": False,
        }

    def fetch_historical_daily(
        self,
        request: HistoricalDailyRequest,
    ) -> HistoricalDailyCandleBatch:
        if type(request) is not HistoricalDailyRequest:
            raise TypeError("request must be an exact HistoricalDailyRequest")
        try:
            request.verify_content_identity()
        except (TypeError, ValueError):
            raise UpstoxDataIntegrityError(
                "historical_data",
                "InvalidCanonicalRequest",
            ) from None
        if request.binding.provider != self.provider:
            raise UpstoxDataIntegrityError(
                "historical_data",
                "ProviderBindingMismatch",
            )
        expected_instrument_id = f"NSE_EQ|{request.binding.isin}"
        if request.binding.provider_instrument_id != expected_instrument_id:
            raise UpstoxDataIntegrityError(
                "historical_data",
                "InstrumentIdentityMismatch",
            )

        started_at = self._observed_at()
        if started_at < request.requested_at:
            raise UpstoxDataIntegrityError("historical_data", "RequestClockMismatch")

        candles: list[HistoricalDailyCandle] = []
        pages: list[HistoricalResponsePage] = []
        for chunk in _session_chunks(request.sessions):
            body = self._request_page(
                instrument_id=request.binding.provider_instrument_id,
                from_session=chunk[0],
                to_session=chunk[-1],
            )
            chunk_candles = self._parse_page(body, expected_sessions=chunk)
            candles.extend(chunk_candles)
            pages.append(
                HistoricalResponsePage(
                    first_session=chunk[0],
                    last_session=chunk[-1],
                    payload_sha256=hashlib.sha256(body).hexdigest(),
                    row_count=len(chunk_candles),
                )
            )

        observed_at = self._observed_at()
        if observed_at < started_at:
            raise UpstoxDataIntegrityError(
                "historical_data",
                "NonMonotonicAcquisitionClock",
            )
        try:
            return HistoricalDailyCandleBatch(
                request=request,
                observed_at=observed_at,
                provider_version=self.provider_version,
                candles=tuple(candles),
                response_pages=tuple(pages),
            )
        except (TypeError, ValueError):
            raise UpstoxDataIntegrityError(
                "historical_data",
                "InvalidCanonicalBatch",
            ) from None

    def _request_page(
        self,
        *,
        instrument_id: str,
        from_session: date,
        to_session: date,
    ) -> bytes:
        encoded_instrument = quote(instrument_id, safe="")
        url = (
            f"{UPSTOX_HISTORICAL_API_BASE_URL}/{encoded_instrument}/days/1/"
            f"{to_session.isoformat()}/{from_session.isoformat()}"
        )
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._credentials.access_token()}",
            "Content-Type": "application/json",
        }
        for attempt in range(1, self._retry_policy.max_attempts + 1):
            self._rate_limiter.wait("historical_data")
            try:
                response = self._transport.get(
                    url,
                    headers=headers,
                    timeout_seconds=UPSTOX_HTTP_TIMEOUT_SECONDS,
                    maximum_bytes=MAXIMUM_UPSTOX_RESPONSE_BYTES,
                )
                if type(response) is not UpstoxHttpResponse:
                    raise UpstoxDataIntegrityError(
                        "historical_data",
                        "InvalidTransportResponse",
                    )
                if len(response.body) > MAXIMUM_UPSTOX_RESPONSE_BYTES:
                    raise UpstoxDataIntegrityError(
                        "historical_data",
                        "OversizedResponse",
                    )
                if response.status_code == 200:
                    return response.body
                error = self._http_error(response.status_code)
            except UpstoxMarketDataError as exc:
                error = exc
            except Exception as exc:
                error = UpstoxAvailabilityError(
                    "historical_data",
                    type(exc).__name__,
                )
            retryable = isinstance(
                error,
                (UpstoxRateLimitError, UpstoxAvailabilityError),
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
    def _http_error(status_code: int) -> UpstoxMarketDataError:
        upstream_type = f"HTTP{status_code}"
        if status_code == 401:
            return UpstoxAuthenticationError("historical_data", upstream_type)
        if status_code == 403:
            return UpstoxPermissionError("historical_data", upstream_type)
        if status_code == 429:
            return UpstoxRateLimitError("historical_data", upstream_type)
        if status_code in {400, 404, 405, 406, 410}:
            return UpstoxRequestError("historical_data", upstream_type)
        return UpstoxAvailabilityError("historical_data", upstream_type)

    @staticmethod
    def _parse_page(
        body: bytes,
        *,
        expected_sessions: tuple[date, ...],
    ) -> tuple[HistoricalDailyCandle, ...]:
        if not body:
            raise UpstoxDataIntegrityError("historical_data", "EmptyResponse")
        try:
            value = json.loads(
                body.decode("utf-8"),
                object_pairs_hook=_reject_duplicate_keys,
                parse_float=Decimal,
                parse_int=int,
                parse_constant=_reject_json_constant,
            )
            if type(value) is not dict or set(value) != {"status", "data"}:
                raise ValueError("invalid response envelope")
            if value["status"] != "success":
                raise ValueError("unsuccessful response status")
            data = value["data"]
            if type(data) is not dict or set(data) != {"candles"}:
                raise ValueError("invalid response data")
            rows = data["candles"]
            if type(rows) is not list or not rows:
                raise ValueError("empty candle response")
            parsed: list[HistoricalDailyCandle] = []
            for row in rows:
                if type(row) is not list or len(row) != 7:
                    raise ValueError("invalid candle row")
                parsed.append(
                    HistoricalDailyCandle(
                        session=_daily_session(row[0]),
                        open=_decimal(row[1], "open"),
                        high=_decimal(row[2], "high"),
                        low=_decimal(row[3], "low"),
                        close=_decimal(row[4], "close"),
                        volume=_integer(row[5], "volume"),
                        open_interest=_integer(row[6], "open_interest"),
                    )
                )
            parsed.sort(key=lambda item: item.session)
            if tuple(item.session for item in parsed) != expected_sessions:
                raise ValueError("response sessions differ from the exact request")
            return tuple(parsed)
        except UpstoxDataIntegrityError:
            raise
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
            raise UpstoxDataIntegrityError(
                "historical_data",
                "MalformedResponse",
            ) from None

    def _observed_at(self) -> datetime:
        observed_at = self._clock()
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("market-data clock must return a timezone-aware datetime")
        return observed_at.astimezone(timezone.utc)
