from __future__ import annotations

import hashlib
from dataclasses import dataclass

from .state_blob_acquisition import VerifiedPipelineStateBlobs
from .state_inventory import PipelineStateEntry


_ERROR_BLOBS = "pipeline state hydration blob verification failed"
_ERROR_ENTRY = "pipeline state hydration entry verification failed"
_ERROR_PLAN = "pipeline state hydration plan verification failed"
_ERROR_AGGREGATE = "pipeline state hydration aggregate verification failed"


class PipelineStateHydrationError(Exception):
    pass


def _reconstructed_state_entry(value: object) -> PipelineStateEntry:
    if type(value) is not PipelineStateEntry:
        raise PipelineStateHydrationError(_ERROR_ENTRY)

    failed = False
    reconstructed: PipelineStateEntry | None = None
    try:
        reconstructed = PipelineStateEntry(
            root_name=value.root_name,
            relative_path=value.relative_path,
            byte_count=value.byte_count,
            sha256=value.sha256,
        )
    except Exception:
        failed = True
    if failed or reconstructed is None:
        raise PipelineStateHydrationError(_ERROR_ENTRY)
    return reconstructed


def _reconstructed_blobs(value: object) -> VerifiedPipelineStateBlobs:
    if type(value) is not VerifiedPipelineStateBlobs:
        raise PipelineStateHydrationError(_ERROR_BLOBS)

    failed = False
    reconstructed: VerifiedPipelineStateBlobs | None = None
    try:
        reconstructed = VerifiedPipelineStateBlobs(
            control=value.control,
            blobs=value.blobs,
        )
    except Exception:
        failed = True
    if failed or reconstructed is None:
        raise PipelineStateHydrationError(_ERROR_BLOBS)
    return reconstructed


@dataclass(frozen=True, slots=True)
class HydratedPipelineStateEntry:
    inventory_entry: PipelineStateEntry
    content_bytes: bytes

    def __post_init__(self) -> None:
        inventory_entry = _reconstructed_state_entry(self.inventory_entry)
        if (
            type(self.content_bytes) is not bytes
            or len(self.content_bytes) != inventory_entry.byte_count
            or hashlib.sha256(self.content_bytes).hexdigest() != inventory_entry.sha256
        ):
            raise PipelineStateHydrationError(_ERROR_ENTRY)
        object.__setattr__(self, "inventory_entry", inventory_entry)


@dataclass(frozen=True, slots=True)
class VerifiedHydratedPipelineState:
    acquired_blobs: VerifiedPipelineStateBlobs
    entries: tuple[HydratedPipelineStateEntry, ...]

    def __post_init__(self) -> None:
        acquired_blobs = _reconstructed_blobs(self.acquired_blobs)
        if type(self.entries) is not tuple:
            raise PipelineStateHydrationError(_ERROR_AGGREGATE)

        failed = False
        reconstructed_entries: tuple[HydratedPipelineStateEntry, ...] = ()
        try:
            reconstructed_entries = tuple(
                HydratedPipelineStateEntry(
                    inventory_entry=item.inventory_entry,
                    content_bytes=item.content_bytes,
                )
                for item in self.entries
                if type(item) is HydratedPipelineStateEntry
            )
            if len(reconstructed_entries) != len(self.entries):
                raise PipelineStateHydrationError(_ERROR_AGGREGATE)
        except Exception:
            failed = True
        if failed:
            raise PipelineStateHydrationError(_ERROR_AGGREGATE)

        expected_entries = acquired_blobs.control.inventory.entries
        expected_keys = tuple(
            (item.root_name, item.relative_path, item.byte_count, item.sha256)
            for item in expected_entries
        )
        observed_keys = tuple(
            (
                item.inventory_entry.root_name,
                item.inventory_entry.relative_path,
                item.inventory_entry.byte_count,
                item.inventory_entry.sha256,
            )
            for item in reconstructed_entries
        )
        if observed_keys != expected_keys:
            raise PipelineStateHydrationError(_ERROR_AGGREGATE)

        blob_by_hash = {
            item.published_object.sha256: item.content_bytes
            for item in acquired_blobs.blobs
        }
        if len(blob_by_hash) != len(acquired_blobs.blobs):
            raise PipelineStateHydrationError(_ERROR_AGGREGATE)
        for item in reconstructed_entries:
            if blob_by_hash.get(item.inventory_entry.sha256) != item.content_bytes:
                raise PipelineStateHydrationError(_ERROR_AGGREGATE)

        if sum(len(item.content_bytes) for item in reconstructed_entries) != (
            acquired_blobs.control.inventory.total_bytes
        ):
            raise PipelineStateHydrationError(_ERROR_AGGREGATE)

        object.__setattr__(self, "acquired_blobs", acquired_blobs)
        object.__setattr__(self, "entries", reconstructed_entries)


def hydrate_verified_pipeline_state(
    acquired_blobs: VerifiedPipelineStateBlobs,
) -> VerifiedHydratedPipelineState:
    """Binds every canonical inventory path to its verified content bytes.

    This is an in-memory, all-or-nothing boundary. It performs no reads,
    writes, path resolution, codec guessing, retries, or fallback selection.
    Duplicate inventory entries that intentionally share one SHA-256 reuse
    the same immutable bytes object. No aggregate is returned until every
    entry and the complete inventory total have been independently checked.
    """

    acquired_blobs = _reconstructed_blobs(acquired_blobs)

    content_by_hash: dict[str, bytes] = {}
    for item in acquired_blobs.blobs:
        sha256_hash = item.published_object.sha256
        if sha256_hash in content_by_hash:
            raise PipelineStateHydrationError(_ERROR_PLAN)
        content_by_hash[sha256_hash] = item.content_bytes

    hydrated: list[HydratedPipelineStateEntry] = []
    hydration_failed = False
    try:
        for inventory_entry in acquired_blobs.control.inventory.entries:
            content_bytes = content_by_hash[inventory_entry.sha256]
            hydrated.append(
                HydratedPipelineStateEntry(
                    inventory_entry=inventory_entry,
                    content_bytes=content_bytes,
                )
            )
    except Exception:
        hydration_failed = True
    if hydration_failed:
        raise PipelineStateHydrationError(_ERROR_PLAN)

    aggregate_failed = False
    result: VerifiedHydratedPipelineState | None = None
    try:
        result = VerifiedHydratedPipelineState(
            acquired_blobs=acquired_blobs,
            entries=tuple(hydrated),
        )
    except Exception:
        aggregate_failed = True
    if aggregate_failed or result is None:
        raise PipelineStateHydrationError(_ERROR_AGGREGATE)
    return result
