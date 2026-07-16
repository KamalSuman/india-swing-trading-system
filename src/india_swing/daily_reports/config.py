from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class DailyReportsConfig:
    data_root: Path

    @classmethod
    def from_env(cls) -> "DailyReportsConfig":
        value = os.environ.get(
            "INDIA_SWING_DAILY_REPORTS_ROOT",
            "var/daily_reports",
        )
        if not value or "\x00" in value:
            raise ValueError("INDIA_SWING_DAILY_REPORTS_ROOT is invalid")
        return cls(data_root=Path(value))
