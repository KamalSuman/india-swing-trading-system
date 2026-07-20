from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .acquisition import GCSObjectReader
from .state_blob_acquisition import (
    VerifiedPipelineStateBlobs,
    acquire_verified_pipeline_state_blobs,
)
from .state_hydration import (
    VerifiedHydratedPipelineState,
    hydrate_verified_pipeline_state,
)
from .state_publication_acquisition import (
    PinnedStatePublicationRequest,
    VerifiedPipelineStateControl,
    acquire_verified_pipeline_state_control,
)
from .state_restoration import (
    CompletedPipelineStateRestore,
    restore_verified_pipeline_state,
)


_ERROR_INPUT = "pinned state restoration service input verification failed"
_ERROR_CONTROL = "pinned state restoration service control acquisition failed"
_ERROR_BLOBS = "pinned state restoration service blob acquisition failed"
_ERROR_HYDRATION = "pinned state restoration service hydration failed"
_ERROR_RESTORATION = "pinned state restoration service restoration failed"
_ERROR_AGGREGATE = "pinned state restoration service aggregate verification failed"

_CONCRETE_PATH_TYPE: type = type(Path())


class PinnedGCSStateRestorationServiceError(Exception):
    pass


def _reconstructed_request(value: object) -> PinnedStatePublicationRequest:
    if type(value) is not PinnedStatePublicationRequest:
        raise PinnedGCSStateRestorationServiceError(_ERROR_INPUT)
    failed = False
    reconstructed: PinnedStatePublicationRequest | None = None
    try:
        reconstructed = PinnedStatePublicationRequest(
            bucket=value.bucket,
            publication_object_name=value.publication_object_name,
            generation=value.generation,
            expected_sha256=value.expected_sha256,
            expected_run_id=value.expected_run_id,
        )
    except Exception:
        failed = True
    if failed or reconstructed is None:
        raise PinnedGCSStateRestorationServiceError(_ERROR_INPUT)
    return reconstructed


def _validated_destination(value: object, expected_run_id: str) -> Path:
    if (
        type(value) is not _CONCRETE_PATH_TYPE
        or not value.is_absolute()
        or ".." in value.parts
        or value.name != expected_run_id
        or value.parent == value
    ):
        raise PinnedGCSStateRestorationServiceError(_ERROR_INPUT)
    return value


def _request_key(value: PinnedStatePublicationRequest) -> tuple[object, ...]:
    return (
        value.bucket,
        value.publication_object_name,
        value.generation,
        value.expected_sha256,
        value.expected_run_id,
    )


@dataclass(frozen=True, slots=True)
class CompletedPinnedGCSStateRestore:
    request: PinnedStatePublicationRequest
    destination: Path
    state: VerifiedHydratedPipelineState
    restoration: CompletedPipelineStateRestore

    def __post_init__(self) -> None:
        input_failed = False
        request: PinnedStatePublicationRequest | None = None
        destination: Path | None = None
        try:
            request = _reconstructed_request(self.request)
            destination = _validated_destination(
                self.destination,
                request.expected_run_id,
            )
        except Exception:
            input_failed = True
        if input_failed or request is None or destination is None:
            raise PinnedGCSStateRestorationServiceError(_ERROR_AGGREGATE)

        if type(self.state) is not VerifiedHydratedPipelineState:
            raise PinnedGCSStateRestorationServiceError(_ERROR_AGGREGATE)
        state_failed = False
        state: VerifiedHydratedPipelineState | None = None
        try:
            state = VerifiedHydratedPipelineState(
                acquired_blobs=self.state.acquired_blobs,
                entries=self.state.entries,
            )
        except Exception:
            state_failed = True
        if state_failed or state is None:
            raise PinnedGCSStateRestorationServiceError(_ERROR_AGGREGATE)

        if type(self.restoration) is not CompletedPipelineStateRestore:
            raise PinnedGCSStateRestorationServiceError(_ERROR_AGGREGATE)
        restoration_failed = False
        restoration: CompletedPipelineStateRestore | None = None
        try:
            restoration = CompletedPipelineStateRestore(
                snapshot_root=self.restoration.snapshot_root,
                roots=self.restoration.roots,
                run_id=self.restoration.run_id,
                inventory_id=self.restoration.inventory_id,
            )
        except Exception:
            restoration_failed = True
        if restoration_failed or restoration is None:
            raise PinnedGCSStateRestorationServiceError(_ERROR_AGGREGATE)

        control = state.acquired_blobs.control
        inventory = control.inventory
        if (
            _request_key(control.request) != _request_key(request)
            or inventory.run_id != request.expected_run_id
            or restoration.snapshot_root != destination
            or restoration.run_id != inventory.run_id
            or restoration.inventory_id != inventory.inventory_id
        ):
            raise PinnedGCSStateRestorationServiceError(_ERROR_AGGREGATE)

        object.__setattr__(self, "request", request)
        object.__setattr__(self, "destination", destination)
        object.__setattr__(self, "state", state)
        object.__setattr__(self, "restoration", restoration)


def restore_pipeline_state_from_pinned_gcs(
    request: PinnedStatePublicationRequest,
    *,
    reader: GCSObjectReader,
    destination: Path,
) -> CompletedPinnedGCSStateRestore:
    """Restores one exact pinned cloud publication into a local snapshot.

    The service preflights an exact request and run-ID-named destination,
    then delegates exactly once and in order to verified control acquisition,
    unique blob acquisition, in-memory hydration, and atomic filesystem
    restoration. It constructs no client, selects no latest object, retries
    nothing, and never accesses environment configuration or a clock.

    Ordinary failures collapse to static stage-specific errors. BaseException
    propagates. If the final aggregate wrapper fails after restoration, the
    already verified create-once snapshot remains intact; it is never rolled
    back or deleted through this composition seam.
    """

    request = _reconstructed_request(request)
    destination = _validated_destination(destination, request.expected_run_id)

    control_failed = False
    control: VerifiedPipelineStateControl | None = None
    try:
        control = acquire_verified_pipeline_state_control(request, reader=reader)
        if type(control) is not VerifiedPipelineStateControl:
            raise PinnedGCSStateRestorationServiceError(_ERROR_CONTROL)
    except Exception:
        control_failed = True
    if control_failed or control is None:
        raise PinnedGCSStateRestorationServiceError(_ERROR_CONTROL)

    blobs_failed = False
    blobs: VerifiedPipelineStateBlobs | None = None
    try:
        blobs = acquire_verified_pipeline_state_blobs(control, reader=reader)
        if type(blobs) is not VerifiedPipelineStateBlobs:
            raise PinnedGCSStateRestorationServiceError(_ERROR_BLOBS)
    except Exception:
        blobs_failed = True
    if blobs_failed or blobs is None:
        raise PinnedGCSStateRestorationServiceError(_ERROR_BLOBS)

    hydration_failed = False
    state: VerifiedHydratedPipelineState | None = None
    try:
        state = hydrate_verified_pipeline_state(blobs)
        if type(state) is not VerifiedHydratedPipelineState:
            raise PinnedGCSStateRestorationServiceError(_ERROR_HYDRATION)
    except Exception:
        hydration_failed = True
    if hydration_failed or state is None:
        raise PinnedGCSStateRestorationServiceError(_ERROR_HYDRATION)

    restoration_failed = False
    restoration: CompletedPipelineStateRestore | None = None
    try:
        restoration = restore_verified_pipeline_state(
            state,
            destination=destination,
        )
        if type(restoration) is not CompletedPipelineStateRestore:
            raise PinnedGCSStateRestorationServiceError(_ERROR_RESTORATION)
    except Exception:
        restoration_failed = True
    if restoration_failed or restoration is None:
        raise PinnedGCSStateRestorationServiceError(_ERROR_RESTORATION)

    aggregate_failed = False
    completed: CompletedPinnedGCSStateRestore | None = None
    try:
        completed = CompletedPinnedGCSStateRestore(
            request=request,
            destination=destination,
            state=state,
            restoration=restoration,
        )
    except Exception:
        aggregate_failed = True
    if aggregate_failed or completed is None:
        raise PinnedGCSStateRestorationServiceError(_ERROR_AGGREGATE)
    return completed
