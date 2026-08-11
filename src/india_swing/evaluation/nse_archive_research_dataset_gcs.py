"""Exact-generation GCS reader for a published NSE archive research dataset.

The dataset builder publishes one canonical manifest but intentionally does not
copy it back into the mounted market-snapshot store.  This adapter closes that
boundary without listing a bucket or selecting a latest object: callers must
pin the dataset identity, object generation, and object SHA-256 independently.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

from india_swing.daily_pipeline.acquisition import GCSObjectPayload, GCSObjectReader
from india_swing.evaluation.nse_archive_research_dataset import (
    NseArchiveResearchDataset,
)
from india_swing.evaluation.nse_archive_research_dataset_store import (
    MAXIMUM_MANIFEST_BYTES,
    decode_nse_archive_research_dataset,
)


class NseArchiveResearchDatasetGCSError(ValueError):
    """Static, sanitized failure at the pinned dataset read boundary."""


_ERROR = "pinned NSE archive research dataset read failed"
_BUCKET = re.compile(r"[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAXIMUM_GENERATION = 9_223_372_036_854_775_807
_OBJECT_PREFIX = "research/nse-archive-datasets/v1"


def nse_archive_research_dataset_object_name(dataset_id: str) -> str:
    if type(dataset_id) is not str or _SHA256.fullmatch(dataset_id) is None:
        raise NseArchiveResearchDatasetGCSError(_ERROR)
    return f"{_OBJECT_PREFIX}/{dataset_id}.json"


@dataclass(frozen=True, slots=True)
class PinnedNseArchiveResearchDatasetRequest:
    bucket: str
    dataset_id: str
    generation: int
    expected_sha256: str

    def __post_init__(self) -> None:
        if type(self.bucket) is not str or _BUCKET.fullmatch(self.bucket) is None:
            raise NseArchiveResearchDatasetGCSError(_ERROR)
        nse_archive_research_dataset_object_name(self.dataset_id)
        if (
            type(self.generation) is not int
            or self.generation <= 0
            or self.generation > _MAXIMUM_GENERATION
        ):
            raise NseArchiveResearchDatasetGCSError(_ERROR)
        if (
            type(self.expected_sha256) is not str
            or _SHA256.fullmatch(self.expected_sha256) is None
        ):
            raise NseArchiveResearchDatasetGCSError(_ERROR)

    @property
    def object_name(self) -> str:
        return nse_archive_research_dataset_object_name(self.dataset_id)


def read_pinned_nse_archive_research_dataset(
    *,
    request: PinnedNseArchiveResearchDatasetRequest,
    reader: GCSObjectReader,
) -> NseArchiveResearchDataset:
    """Read and independently verify one exact published dataset manifest."""

    if type(request) is not PinnedNseArchiveResearchDatasetRequest:
        raise NseArchiveResearchDatasetGCSError(_ERROR)
    failed = False
    dataset: NseArchiveResearchDataset | None = None
    try:
        payload = reader.read_generation(
            bucket=request.bucket,
            object_name=request.object_name,
            generation=request.generation,
            maximum_bytes=MAXIMUM_MANIFEST_BYTES,
        )
        if (
            type(payload) is not GCSObjectPayload
            or type(payload.generation) is not int
            or payload.generation != request.generation
            or type(payload.content_bytes) is not bytes
            or not (0 < len(payload.content_bytes) <= MAXIMUM_MANIFEST_BYTES)
            or hashlib.sha256(payload.content_bytes).hexdigest()
            != request.expected_sha256
        ):
            raise ValueError
        dataset = decode_nse_archive_research_dataset(payload.content_bytes)
        if dataset.dataset_id != request.dataset_id:
            raise ValueError
        dataset.verify_content_identity()
    except Exception:
        failed = True
    if failed or dataset is None:
        raise NseArchiveResearchDatasetGCSError(_ERROR)
    return dataset


@dataclass(frozen=True, slots=True)
class ExactNseArchiveResearchDatasetResolver:
    """One-object resolver used after the pinned GCS read succeeds."""

    dataset: NseArchiveResearchDataset

    def __post_init__(self) -> None:
        failed = False
        try:
            if type(self.dataset) is not NseArchiveResearchDataset:
                raise ValueError
            self.dataset.verify_content_identity()
        except Exception:
            failed = True
        if failed:
            raise NseArchiveResearchDatasetGCSError(_ERROR)

    def get(self, dataset_id: str) -> NseArchiveResearchDataset:
        if type(dataset_id) is not str or dataset_id != self.dataset.dataset_id:
            raise NseArchiveResearchDatasetGCSError(_ERROR)
        self.dataset.verify_content_identity()
        return self.dataset
