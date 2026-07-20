from __future__ import annotations

from pathlib import Path

from india_swing._filesystem import read_stable_regular_file

from .acquisition import GCSObjectReader
from .pinned_gcs_state_restoration_service import (
    CompletedPinnedGCSStateRestore,
    restore_pipeline_state_from_pinned_gcs,
)
from .pinned_gcs_state_restore_spec import (
    MAXIMUM_PINNED_GCS_STATE_RESTORE_SPEC_BYTES,
    PinnedGCSStateRestoreSpec,
    parse_pinned_gcs_state_restore_spec,
)


_ERROR_LOAD = "pinned state restoration spec file could not be loaded"
_ERROR_SCHEMA = "pinned state restoration file boundary schema verification failed"
_CONCRETE_PATH_TYPE: type = type(Path())


class PinnedGCSStateRestoreFileBoundaryError(Exception):
    pass


def _validated_spec_path(value: object) -> Path:
    if (
        type(value) is not _CONCRETE_PATH_TYPE
        or not value.is_absolute()
        or ".." in value.parts
        or value.parent == value
    ):
        raise PinnedGCSStateRestoreFileBoundaryError(_ERROR_LOAD)
    return value


def load_pinned_gcs_state_restore_spec_file(
    spec_path: Path,
) -> PinnedGCSStateRestoreSpec:
    """Stably reads and reconstructs one canonical restore specification.

    The caller must provide an exact absolute Path. The shared stable-file
    boundary rejects links/reparse points, non-regular files, path swaps,
    concurrent mutation, empty content, and over-sized content. No directory
    listing, latest selection, environment lookup, or client construction is
    available here.
    """

    path = _validated_spec_path(spec_path)
    load_failed = False
    loaded: PinnedGCSStateRestoreSpec | None = None
    try:
        spec_bytes = read_stable_regular_file(
            path,
            maximum_bytes=MAXIMUM_PINNED_GCS_STATE_RESTORE_SPEC_BYTES,
        )
        loaded = parse_pinned_gcs_state_restore_spec(spec_bytes)
    except Exception:
        load_failed = True
    if load_failed or loaded is None:
        raise PinnedGCSStateRestoreFileBoundaryError(_ERROR_LOAD)

    reconstruction_failed = False
    reconstructed: PinnedGCSStateRestoreSpec | None = None
    try:
        if type(loaded) is not PinnedGCSStateRestoreSpec:
            raise PinnedGCSStateRestoreFileBoundaryError(_ERROR_SCHEMA)
        reconstructed = PinnedGCSStateRestoreSpec(
            schema_version=loaded.schema_version,
            publication_request=loaded.publication_request,
            destination=loaded.destination,
        )
    except Exception:
        reconstruction_failed = True
    if reconstruction_failed or reconstructed is None:
        raise PinnedGCSStateRestoreFileBoundaryError(_ERROR_SCHEMA)
    return reconstructed


def restore_pipeline_state_from_pinned_gcs_spec_file(
    spec_path: Path,
    *,
    reader: GCSObjectReader,
) -> CompletedPinnedGCSStateRestore:
    """Loads one canonical restore spec and delegates exactly once.

    File-load/schema failures are sanitized by the loader. The composed
    restoration service result and all service/BaseException failures are
    forwarded unchanged; this boundary performs no retry or rollback.
    """

    spec = load_pinned_gcs_state_restore_spec_file(spec_path)
    return restore_pipeline_state_from_pinned_gcs(
        spec.publication_request,
        reader=reader,
        destination=spec.destination,
    )
