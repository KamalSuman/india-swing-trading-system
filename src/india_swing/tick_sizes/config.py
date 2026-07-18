from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


TICK_SIZE_ROOT_ENV = "INDIA_SWING_TICK_SIZE_ROOT"


@dataclass(frozen=True, slots=True)
class TickSizeConfig:
    data_root: Path = Path("var/tick_sizes")

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> TickSizeConfig:
        values = os.environ if environ is None else environ
        root = values.get(TICK_SIZE_ROOT_ENV, "var/tick_sizes")
        if not isinstance(root, str) or not root.strip() or "\x00" in root:
            raise ValueError(f"{TICK_SIZE_ROOT_ENV} is invalid")
        return cls(data_root=Path(root))
