from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


IDENTITY_EVIDENCE_ROOT_ENV = "INDIA_SWING_IDENTITY_EVIDENCE_ROOT"


@dataclass(frozen=True, slots=True)
class IdentityEvidenceConfig:
    data_root: Path = Path("var/identity_evidence")

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> IdentityEvidenceConfig:
        values = os.environ if environ is None else environ
        root = values.get(IDENTITY_EVIDENCE_ROOT_ENV, "").strip()
        if "\x00" in root:
            raise ValueError(f"{IDENTITY_EVIDENCE_ROOT_ENV} is invalid")
        return cls(Path(root) if root else Path("var/identity_evidence"))
