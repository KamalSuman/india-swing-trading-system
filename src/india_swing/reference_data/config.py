from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


REFERENCE_DATA_ROOT_ENV = "INDIA_SWING_REFERENCE_DATA_ROOT"


@dataclass(frozen=True, slots=True)
class ReferenceDataConfig:
    data_root: Path = Path("var/reference_data")

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> ReferenceDataConfig:
        values = os.environ if environ is None else environ
        root = values.get(REFERENCE_DATA_ROOT_ENV, "").strip()
        return cls(data_root=Path(root) if root else Path("var/reference_data"))
