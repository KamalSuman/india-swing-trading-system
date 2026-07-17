from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


COLLECTION_UNIVERSE_ROOT_ENV = "INDIA_SWING_UNIVERSE_ROOT"


@dataclass(frozen=True, slots=True)
class CollectionUniverseConfig:
    data_root: Path = Path("var/universe")

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> CollectionUniverseConfig:
        values = os.environ if environ is None else environ
        root = values.get(COLLECTION_UNIVERSE_ROOT_ENV, "var/universe")
        if not isinstance(root, str) or not root.strip() or "\x00" in root:
            raise ValueError(f"{COLLECTION_UNIVERSE_ROOT_ENV} is invalid")
        return cls(data_root=Path(root))
