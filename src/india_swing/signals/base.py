from __future__ import annotations

from typing import Protocol

from india_swing.domain.models import (
    DataSnapshot,
    ForecastSummary,
    InstrumentSnapshot,
    SignalFeatures,
    TradeSetup,
)


class SignalProvider(Protocol):
    """Builds deterministic features and a proposed long setup from curated inputs."""

    version: str

    def generate(
        self,
        instrument: InstrumentSnapshot,
        forecast: ForecastSummary,
        snapshot: DataSnapshot,
    ) -> tuple[SignalFeatures, TradeSetup, tuple[str, ...]]: ...
