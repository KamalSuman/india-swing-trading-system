from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


SHADOW_SCAN_ROOT_ENV = "INDIA_SWING_SHADOW_SCAN_ROOT"


@dataclass(frozen=True, slots=True)
class ShadowScanStoreConfig:
    data_root: Path = Path("var/shadow_scans")

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> ShadowScanStoreConfig:
        values = os.environ if environ is None else environ
        root = values.get(SHADOW_SCAN_ROOT_ENV, "var/shadow_scans")
        if type(root) is not str or not root.strip() or "\x00" in root:
            raise ValueError(f"{SHADOW_SCAN_ROOT_ENV} is invalid")
        return cls(data_root=Path(root))
