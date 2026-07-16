from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


TRIAL_REGISTRY_ROOT_ENV = "INDIA_SWING_TRIAL_REGISTRY_ROOT"
EVALUATION_EVIDENCE_ROOT_ENV = "INDIA_SWING_EVALUATION_ROOT"


@dataclass(frozen=True, slots=True)
class TrialRegistryConfig:
    data_root: Path = Path("var/trial_registry")

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> TrialRegistryConfig:
        values = os.environ if environ is None else environ
        root = values.get(TRIAL_REGISTRY_ROOT_ENV, "").strip()
        return cls(data_root=Path(root) if root else Path("var/trial_registry"))


@dataclass(frozen=True, slots=True)
class EvaluationEvidenceConfig:
    data_root: Path = Path("var/evaluation")

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> EvaluationEvidenceConfig:
        values = os.environ if environ is None else environ
        root = values.get(EVALUATION_EVIDENCE_ROOT_ENV, "").strip()
        return cls(data_root=Path(root) if root else Path("var/evaluation"))
