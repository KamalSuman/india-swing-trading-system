from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


HISTORICAL_PRICES_ROOT_ENV = "INDIA_SWING_HISTORICAL_PRICES_ROOT"
DAILY_REPORTS_ROOT_ENV = "INDIA_SWING_DAILY_REPORTS_ROOT"


@dataclass(frozen=True, slots=True)
class HistoricalPricesConfig:
    data_root: Path = Path("var/historical_prices")
    daily_reports_root: Path = Path("var/daily_reports")

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> HistoricalPricesConfig:
        values = os.environ if environ is None else environ
        historical_root = values.get(
            HISTORICAL_PRICES_ROOT_ENV,
            "var/historical_prices",
        )
        daily_root = values.get(DAILY_REPORTS_ROOT_ENV, "var/daily_reports")
        for value, name in (
            (historical_root, HISTORICAL_PRICES_ROOT_ENV),
            (daily_root, DAILY_REPORTS_ROOT_ENV),
        ):
            if (
                not isinstance(value, str)
                or not value.strip()
                or "\x00" in value
            ):
                raise ValueError(f"{name} is invalid")
        return cls(
            data_root=Path(historical_root),
            daily_reports_root=Path(daily_root),
        )
