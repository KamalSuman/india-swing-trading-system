from __future__ import annotations

import hashlib
from dataclasses import dataclass

from .acquisition import GCSObjectPayload, GCSObjectReader, _MAXIMUM_GENERATION
from .state_inventory import (
    MAXIMUM_ENCODED_BYTES,
    PipelineStateInventory,
    parse_pipeline_state_inventory,
)
from .state_publication import (
    MAXIMUM_PUBLICATION_MANIFEST_BYTES,
    CompletedPipelineStatePublication,
    PipelineStatePublicationManifest,
    PublishedStateObject,
    _validate_bucket,
    parse_pipeline_state_publication_manifest,
)


_SHA256_CHARS = frozenset("0123456789abcdef")
_ERROR_REQUEST = "pinned state publication acquisition request is invalid"
_ERROR_PUBLICATION_READ = "pinned state publication acquisition publication read failed"
_ERROR_PUBLICATION_VERIFY = (
    "pinned state publication acquisition publication verification failed"
)
_ERROR_INVENTORY_READ = "pinned state publication acquisition inventory read failed"
_ERROR_INVENTORY_VERIFY = "pinned state publication acquisition inventory verification failed"
_ERROR_AGGREGATE = "pinned state publication acquisition aggregate verification failed"


class StatePublicationAcquisitionError(Exception):
    pass


def _validate_sha256(value: object) -> None:
    if type(value) is not str or len(value) != 64 or not _SHA256_CHARS.issuperset(value):
        raise StatePublicationAcquisitionError(_ERROR_REQUEST)


@dataclass(frozen=True, slots=True)
class PinnedStatePublicationRequest:
    """One fully pinned, mandatory reference to a prior state-publication
    manifest object.

    There is no default, no path/prefix/session, no bucket listing, no
    "latest" marker, and no fallback object or alternate bucket -- every
    field must be supplied and is independently re-verified against the
    existing state-publication bucket authority and canonical hash/
    generation rules.
    """

    bucket: str
    publication_object_name: str
    generation: int
    expected_sha256: str
    expected_run_id: str

    def __post_init__(self) -> None:
        bucket_failed = False
        try:
            _validate_bucket(self.bucket)
        except Exception:
            bucket_failed = True
        if bucket_failed:
            raise StatePublicationAcquisitionError(_ERROR_REQUEST)

        if (
            type(self.publication_object_name) is not str
            or not self.publication_object_name
        ):
            raise StatePublicationAcquisitionError(_ERROR_REQUEST)
        if (
            type(self.generation) is not int
            or self.generation <= 0
            or self.generation > _MAXIMUM_GENERATION
        ):
            raise StatePublicationAcquisitionError(_ERROR_REQUEST)
        _validate_sha256(self.expected_sha256)
        _validate_sha256(self.expected_run_id)


def _reconstructed_request(value: object) -> PinnedStatePublicationRequest:
    if type(value) is not PinnedStatePublicationRequest:
        raise StatePublicationAcquisitionError(_ERROR_REQUEST)
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
    except StatePublicationAcquisitionError:
        raise
    except Exception:
        failed = True
    if failed or reconstructed is None:
        raise StatePublicationAcquisitionError(_ERROR_REQUEST)
    return reconstructed


@dataclass(frozen=True, slots=True)
class VerifiedPipelineStateControl:
    """One independently cross-verified aggregate binding a fully pinned
    restoration request, its verified immutable publication manifest, and
    its verified canonical inventory.

    Runtime evidence only: a self-consistent graph is integrity evidence,
    not independent provenance or proof that upstream operator inputs
    were truthful.
    """

    request: PinnedStatePublicationRequest
    publication: CompletedPipelineStatePublication
    inventory: PipelineStateInventory

    def __post_init__(self) -> None:
        reconstructed_request = _reconstructed_request(self.request)
        object.__setattr__(self, "request", reconstructed_request)

        if type(self.publication) is not CompletedPipelineStatePublication:
            raise StatePublicationAcquisitionError(_ERROR_AGGREGATE)
        publication_failed = False
        reconstructed_publication: CompletedPipelineStatePublication | None = None
        try:
            reconstructed_publication = CompletedPipelineStatePublication(
                manifest=self.publication.manifest,
                publication_object=self.publication.publication_object,
            )
        except Exception:
            publication_failed = True
        if publication_failed or reconstructed_publication is None:
            raise StatePublicationAcquisitionError(_ERROR_AGGREGATE)
        object.__setattr__(self, "publication", reconstructed_publication)

        publication_object = self.publication.publication_object
        if (
            publication_object.object_name != self.request.publication_object_name
            or publication_object.generation != self.request.generation
            or publication_object.sha256 != self.request.expected_sha256
        ):
            raise StatePublicationAcquisitionError(_ERROR_AGGREGATE)

        if type(self.inventory) is not PipelineStateInventory:
            raise StatePublicationAcquisitionError(_ERROR_AGGREGATE)
        inventory_failed = False
        reconstructed_inventory: PipelineStateInventory | None = None
        try:
            reconstructed_inventory = PipelineStateInventory(
                schema_version=self.inventory.schema_version,
                run_id=self.inventory.run_id,
                previous_run_id=self.inventory.previous_run_id,
                market_session=self.inventory.market_session,
                cutoff=self.inventory.cutoff,
                entries=self.inventory.entries,
                entry_count=self.inventory.entry_count,
                total_bytes=self.inventory.total_bytes,
            )
            if reconstructed_inventory.inventory_id != self.inventory.inventory_id:
                raise StatePublicationAcquisitionError(_ERROR_AGGREGATE)
        except Exception:
            inventory_failed = True
        if inventory_failed or reconstructed_inventory is None:
            raise StatePublicationAcquisitionError(_ERROR_AGGREGATE)
        object.__setattr__(self, "inventory", reconstructed_inventory)

        manifest = self.publication.manifest
        if manifest.bucket != self.request.bucket:
            raise StatePublicationAcquisitionError(_ERROR_AGGREGATE)
        if manifest.run_id != self.request.expected_run_id:
            raise StatePublicationAcquisitionError(_ERROR_AGGREGATE)
        if manifest.inventory_id != self.inventory.inventory_id:
            raise StatePublicationAcquisitionError(_ERROR_AGGREGATE)
        if (
            manifest.run_id != self.inventory.run_id
            or manifest.previous_run_id != self.inventory.previous_run_id
            or manifest.market_session != self.inventory.market_session
            or manifest.cutoff != self.inventory.cutoff
        ):
            raise StatePublicationAcquisitionError(_ERROR_AGGREGATE)


def acquire_verified_pipeline_state_control(
    request: PinnedStatePublicationRequest,
    *,
    reader: GCSObjectReader,
) -> VerifiedPipelineStateControl:
    """Acquires and independently cross-verifies exactly one pinned prior
    publication manifest and its exact manifest-referenced inventory
    object through the injected, generation-pinned GCSObjectReader.

    A pure read-only trust boundary: it never constructs a storage
    client, never lists a bucket or selects "latest", never downloads a
    blob object, and never writes/creates/opens/resolves/stats any local
    file, directory, or path. reader.read_generation is called exactly
    twice, in order -- once for the publication manifest (bounded by
    MAXIMUM_PUBLICATION_MANIFEST_BYTES) and, only after full publication
    verification succeeds, once for manifest.inventory_object (bounded by
    MAXIMUM_ENCODED_BYTES).

    Every ordinary failure (never BaseException) -- including one that
    happens to already be a StatePublicationAcquisitionError -- collapses
    into one fresh, static, stage-specific StatePublicationAcquisitionError
    raised only after its guarding try/except has fully exited, so both
    __cause__ and __context__ are always None and no bucket, path, run
    ID, hash, generation, date, content, or nested exception text can
    leak through this boundary.
    """

    request = _reconstructed_request(request)

    publication_read_failed = False
    publication_payload: GCSObjectPayload | None = None
    try:
        publication_payload = reader.read_generation(
            bucket=request.bucket,
            object_name=request.publication_object_name,
            generation=request.generation,
            maximum_bytes=MAXIMUM_PUBLICATION_MANIFEST_BYTES,
        )
    except Exception:
        publication_read_failed = True
    if publication_read_failed:
        raise StatePublicationAcquisitionError(_ERROR_PUBLICATION_READ)

    publication_verify_failed = False
    publication_bytes = b""
    observed_publication_sha256 = ""
    try:
        if type(publication_payload) is not GCSObjectPayload:
            raise StatePublicationAcquisitionError(_ERROR_PUBLICATION_READ)
        if (
            type(publication_payload.generation) is not int
            or publication_payload.generation != request.generation
        ):
            raise StatePublicationAcquisitionError(_ERROR_PUBLICATION_READ)
        publication_bytes = publication_payload.content_bytes
        if (
            type(publication_bytes) is not bytes
            or not (0 < len(publication_bytes) <= MAXIMUM_PUBLICATION_MANIFEST_BYTES)
        ):
            raise StatePublicationAcquisitionError(_ERROR_PUBLICATION_READ)
        observed_publication_sha256 = hashlib.sha256(publication_bytes).hexdigest()
        if observed_publication_sha256 != request.expected_sha256:
            raise StatePublicationAcquisitionError(_ERROR_PUBLICATION_READ)
    except Exception:
        publication_verify_failed = True
    if publication_verify_failed:
        raise StatePublicationAcquisitionError(_ERROR_PUBLICATION_READ)

    publication_construct_failed = False
    manifest: PipelineStatePublicationManifest | None = None
    completed_publication: CompletedPipelineStatePublication | None = None
    try:
        manifest = parse_pipeline_state_publication_manifest(publication_bytes)
        publication_object = PublishedStateObject(
            object_name=request.publication_object_name,
            generation=request.generation,
            byte_count=len(publication_bytes),
            sha256=observed_publication_sha256,
        )
        completed_publication = CompletedPipelineStatePublication(
            manifest=manifest,
            publication_object=publication_object,
        )
    except Exception:
        publication_construct_failed = True
    if publication_construct_failed or manifest is None or completed_publication is None:
        raise StatePublicationAcquisitionError(_ERROR_PUBLICATION_VERIFY)

    if manifest.bucket != request.bucket or manifest.run_id != request.expected_run_id:
        raise StatePublicationAcquisitionError(_ERROR_PUBLICATION_VERIFY)

    inventory_object = manifest.inventory_object

    inventory_read_failed = False
    inventory_payload: GCSObjectPayload | None = None
    try:
        inventory_payload = reader.read_generation(
            bucket=request.bucket,
            object_name=inventory_object.object_name,
            generation=inventory_object.generation,
            maximum_bytes=MAXIMUM_ENCODED_BYTES,
        )
    except Exception:
        inventory_read_failed = True
    if inventory_read_failed:
        raise StatePublicationAcquisitionError(_ERROR_INVENTORY_READ)

    inventory_verify_failed = False
    inventory_bytes = b""
    try:
        if type(inventory_payload) is not GCSObjectPayload:
            raise StatePublicationAcquisitionError(_ERROR_INVENTORY_READ)
        if (
            type(inventory_payload.generation) is not int
            or inventory_payload.generation != inventory_object.generation
        ):
            raise StatePublicationAcquisitionError(_ERROR_INVENTORY_READ)
        inventory_bytes = inventory_payload.content_bytes
        if (
            type(inventory_bytes) is not bytes
            or not (0 < len(inventory_bytes) <= MAXIMUM_ENCODED_BYTES)
        ):
            raise StatePublicationAcquisitionError(_ERROR_INVENTORY_READ)
        if len(inventory_bytes) != inventory_object.byte_count:
            raise StatePublicationAcquisitionError(_ERROR_INVENTORY_READ)
        if hashlib.sha256(inventory_bytes).hexdigest() != inventory_object.sha256:
            raise StatePublicationAcquisitionError(_ERROR_INVENTORY_READ)
    except Exception:
        inventory_verify_failed = True
    if inventory_verify_failed:
        raise StatePublicationAcquisitionError(_ERROR_INVENTORY_READ)

    inventory_construct_failed = False
    inventory: PipelineStateInventory | None = None
    try:
        inventory = parse_pipeline_state_inventory(inventory_bytes)
        if (
            inventory.inventory_id != manifest.inventory_id
            or inventory.run_id != manifest.run_id
            or inventory.previous_run_id != manifest.previous_run_id
            or inventory.market_session != manifest.market_session
            or inventory.cutoff != manifest.cutoff
        ):
            raise StatePublicationAcquisitionError(_ERROR_INVENTORY_VERIFY)
    except Exception:
        inventory_construct_failed = True
    if inventory_construct_failed or inventory is None:
        raise StatePublicationAcquisitionError(_ERROR_INVENTORY_VERIFY)

    aggregate_failed = False
    result: VerifiedPipelineStateControl | None = None
    try:
        result = VerifiedPipelineStateControl(
            request=request,
            publication=completed_publication,
            inventory=inventory,
        )
    except Exception:
        aggregate_failed = True
    if aggregate_failed or result is None:
        raise StatePublicationAcquisitionError(_ERROR_AGGREGATE)
    return result
