from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .models import HistoricalDailyCandleBatch, HistoricalDailyRequest


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
