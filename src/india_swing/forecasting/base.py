from __future__ import annotations

from typing import Protocol

from india_swing.domain.models import DataSnapshot, ForecastSummary, InstrumentSnapshot


class ForecastProvider(Protocol):
    """Boundary implemented later by the pinned Kronos inference adapter."""

    model_version: str

    def forecast(
        self, instrument: InstrumentSnapshot, snapshot: DataSnapshot
    ) -> ForecastSummary: ...
