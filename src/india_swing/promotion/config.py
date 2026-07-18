from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


PROMOTION_ROOT_ENV = "INDIA_SWING_PROMOTION_ROOT"


@dataclass(frozen=True, slots=True)
class PromotionConfig:
    data_root: Path = Path("var/promotion")

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> PromotionConfig:
        values = os.environ if environ is None else environ
        root = values.get(PROMOTION_ROOT_ENV, "var/promotion")
        if not isinstance(root, str) or not root.strip() or "\x00" in root:
            raise ValueError(f"{PROMOTION_ROOT_ENV} is invalid")
        return cls(data_root=Path(root))
