from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import re
from typing import Protocol

from .models import (
    HistoricalDailyCandleBatch,
    HistoricalDailyRequest,
    MARKET_DATA_PROVIDER_PATTERN,
    SHA256_IDENTIFIER,
)


class HistoricalEmptyProviderResponseError(ValueError):
    """A structurally valid provider response contained zero rows for one session.

    This proves only EMPTY_PROVIDER_RESPONSE -- never zero trades, suspension,
    delisting, a stale access token, a corporate action, or a provider backfill
    failure. A connector must raise this only for a clean, unambiguous empty
    response; any malformed, partial, or otherwise-suspect response must still
    raise the connector's own integrity error and fail closed.
    """

    def __init__(
        self,
        *,
        provider: str,
        provider_version: str,
        provider_instrument_id: str,
        session: date,
        observed_at: datetime,
        normalized_response_sha256: str,
    ) -> None:
        if (
            type(provider) is not str
            or MARKET_DATA_PROVIDER_PATTERN.fullmatch(provider) is None
        ):
            raise ValueError("provider must be canonical uppercase provider text")
        if (
            type(provider_version) is not str
            or not provider_version.strip()
            or len(provider_version) > 128
        ):
            raise ValueError("provider_version must be bounded non-empty text")
        if (
            type(provider_instrument_id) is not str
            or not provider_instrument_id
            or provider_instrument_id != provider_instrument_id.strip()
            or len(provider_instrument_id) > 128
        ):
            raise ValueError(
                "provider_instrument_id must be non-empty canonical text"
            )
        if type(session) is not date:
            raise TypeError("session must be an exact date")
        if type(observed_at) is not datetime:
            raise TypeError("observed_at must be an exact datetime")
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        if (
            type(normalized_response_sha256) is not str
            or SHA256_IDENTIFIER.fullmatch(normalized_response_sha256) is None
        ):
            raise ValueError(
                "normalized_response_sha256 must be a lowercase SHA-256"
            )
        self.provider = provider
        self.provider_version = provider_version
        self.provider_instrument_id = provider_instrument_id
        self.session = session
        self.observed_at = observed_at.astimezone(timezone.utc)
        self.normalized_response_sha256 = normalized_response_sha256
        super().__init__("historical provider returned a valid empty response")


_UPSTREAM_ERROR_TYPE_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9_]{0,127}")


class HistoricalProviderRequestRejectedError(ValueError):
    """One exact historical request was rejected before a response existed.

    This is unresolved provider evidence. It never proves that the instrument
    was invalid, delisted, suspended, or untraded, and it cannot authorize a
    synthetic candle or completion.
    """

    def __init__(
        self,
        *,
        provider: str,
        provider_version: str,
        provider_instrument_id: str,
        session: date,
        observed_at: datetime,
        upstream_error_type: str,
        normalized_response_sha256: str,
    ) -> None:
        if (
            type(provider) is not str
            or MARKET_DATA_PROVIDER_PATTERN.fullmatch(provider) is None
        ):
            raise ValueError("provider must be canonical uppercase provider text")
        if (
            type(provider_version) is not str
            or not provider_version.strip()
            or len(provider_version) > 128
        ):
            raise ValueError("provider_version must be bounded non-empty text")
        if (
            type(provider_instrument_id) is not str
            or not provider_instrument_id
            or provider_instrument_id != provider_instrument_id.strip()
            or len(provider_instrument_id) > 128
        ):
            raise ValueError(
                "provider_instrument_id must be non-empty canonical text"
            )
        if type(session) is not date:
            raise TypeError("session must be an exact date")
        if type(observed_at) is not datetime:
            raise TypeError("observed_at must be an exact datetime")
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        if (
            type(upstream_error_type) is not str
            or _UPSTREAM_ERROR_TYPE_PATTERN.fullmatch(upstream_error_type) is None
        ):
            raise ValueError("upstream_error_type must be bounded canonical text")
        if (
            type(normalized_response_sha256) is not str
            or SHA256_IDENTIFIER.fullmatch(normalized_response_sha256) is None
        ):
            raise ValueError(
                "normalized_response_sha256 must be a lowercase SHA-256"
            )
        self.provider = provider
        self.provider_version = provider_version
        self.provider_instrument_id = provider_instrument_id
        self.session = session
        self.observed_at = observed_at.astimezone(timezone.utc)
        self.upstream_error_type = upstream_error_type
        self.normalized_response_sha256 = normalized_response_sha256
        super().__init__("historical provider rejected one request")


class RequestRateLimiter(Protocol):
    def wait(self, operation: str) -> None: ...


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay_seconds: float = 0.5
    maximum_delay_seconds: float = 4.0
    jitter_seconds: float = 0.25

    def __post_init__(self) -> None:
        if type(self.max_attempts) is not int or self.max_attempts <= 0:
            raise ValueError("max_attempts must be a positive exact integer")
        for value, name in (
            (self.base_delay_seconds, "base_delay_seconds"),
            (self.maximum_delay_seconds, "maximum_delay_seconds"),
            (self.jitter_seconds, "jitter_seconds"),
        ):
            if type(value) not in (int, float) or value < 0:
                raise ValueError(f"{name} must be a non-negative number")
        if self.maximum_delay_seconds < self.base_delay_seconds:
            raise ValueError("maximum_delay_seconds cannot be below base_delay_seconds")


class HistoricalDailyDataConnector(Protocol):
    """Provider-neutral daily-history boundary used by collectors and stores."""

    @property
    def provider(self) -> str: ...

    @property
    def provider_version(self) -> str: ...

    def fetch_historical_daily(
        self,
        request: HistoricalDailyRequest,
    ) -> HistoricalDailyCandleBatch: ...
