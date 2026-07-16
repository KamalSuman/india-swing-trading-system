from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


CALENDAR_DATA_ROOT_ENV = "INDIA_SWING_CALENDAR_DATA_ROOT"


@dataclass(frozen=True, slots=True)
class CalendarDataConfig:
    data_root: Path = Path("var/calendar_data")

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> CalendarDataConfig:
        values = os.environ if environ is None else environ
        root = values.get(CALENDAR_DATA_ROOT_ENV, "").strip()
        if "\x00" in root:
            raise ValueError(f"{CALENDAR_DATA_ROOT_ENV} is invalid")
        return cls(data_root=Path(root) if root else Path("var/calendar_data"))
