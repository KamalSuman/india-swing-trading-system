from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


LIQUIDITY_ROOT_ENV = "INDIA_SWING_LIQUIDITY_ROOT"


@dataclass(frozen=True, slots=True)
class LiquidityConfig:
    data_root: Path = Path("var/liquidity")

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> LiquidityConfig:
        values = os.environ if environ is None else environ
        root = values.get(LIQUIDITY_ROOT_ENV, "var/liquidity")
        if not isinstance(root, str) or not root.strip() or "\x00" in root:
            raise ValueError(f"{LIQUIDITY_ROOT_ENV} is invalid")
        return cls(data_root=Path(root))
