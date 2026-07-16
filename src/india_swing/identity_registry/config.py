from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


IDENTITY_REGISTRY_ROOT_ENV = "INDIA_SWING_IDENTITY_REGISTRY_ROOT"


@dataclass(frozen=True, slots=True)
class IdentityRegistryConfig:
    data_root: Path = Path("var/identity_registry")

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> IdentityRegistryConfig:
        values = os.environ if environ is None else environ
        root = values.get(IDENTITY_REGISTRY_ROOT_ENV, "").strip()
        return cls(data_root=Path(root) if root else Path("var/identity_registry"))
