from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


DAILY_PIPELINE_ROOT_ENV = "INDIA_SWING_DAILY_PIPELINE_ROOT"


@dataclass(frozen=True, slots=True)
class DailyPipelineConfig:
    data_root: Path = Path("var/daily_pipeline")

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> DailyPipelineConfig:
        values = os.environ if environ is None else environ
        root = values.get(DAILY_PIPELINE_ROOT_ENV, "var/daily_pipeline")
        if not isinstance(root, str) or not root.strip() or "\x00" in root:
            raise ValueError(f"{DAILY_PIPELINE_ROOT_ENV} is invalid")
        return cls(data_root=Path(root))


STATE_PUBLICATION_BUCKET_ENV = "INDIA_SWING_STATE_PUBLICATION_BUCKET"


@dataclass(frozen=True, slots=True)
class StatePublicationConfig:
    bucket: str

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> StatePublicationConfig:
        values = os.environ if environ is None else environ
        bucket = values.get(STATE_PUBLICATION_BUCKET_ENV)
        if (
            type(bucket) is not str
            or not bucket
            or bucket != bucket.strip()
            or "\x00" in bucket
        ):
            raise ValueError(f"{STATE_PUBLICATION_BUCKET_ENV} is invalid")
        return cls(bucket=bucket)
