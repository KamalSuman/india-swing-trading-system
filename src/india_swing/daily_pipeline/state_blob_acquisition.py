from __future__ import annotations

import hashlib
from dataclasses import dataclass

from .acquisition import GCSObjectPayload, GCSObjectReader
from .state_inventory import MAXIMUM_FILE_BYTES
from .state_publication import PublishedStateObject
from .state_publication_acquisition import VerifiedPipelineStateControl


_ERROR_CONTROL = "state blob acquisition control verification failed"
_ERROR_PLAN = "state blob acquisition plan verification failed"
_ERROR_READ = "state blob acquisition object read failed"
_ERROR_VERIFY = "state blob acquisition object verification failed"
_ERROR_AGGREGATE = "state blob acquisition aggregate verification failed"


class StateBlobAcquisitionError(Exception):
    pass


def _reconstructed_control(value: object) -> VerifiedPipelineStateControl:
    if type(value) is not VerifiedPipelineStateControl:
        raise StateBlobAcquisitionError(_ERROR_CONTROL)

    failed = False
    reconstructed: VerifiedPipelineStateControl | None = None
    try:
        reconstructed = VerifiedPipelineStateControl(
            request=value.request,
            publication=value.publication,
            inventory=value.inventory,
        )
    except Exception:
        failed = True
    if failed or reconstructed is None:
        raise StateBlobAcquisitionError(_ERROR_CONTROL)
    return reconstructed


def _reconstructed_published_object(value: object) -> PublishedStateObject:
    if type(value) is not PublishedStateObject:
        raise StateBlobAcquisitionError(_ERROR_AGGREGATE)

    failed = False
    reconstructed: PublishedStateObject | None = None
    try:
        reconstructed = PublishedStateObject(
            object_name=value.object_name,
            generation=value.generation,
            byte_count=value.byte_count,
            sha256=value.sha256,
        )
    except Exception:
        failed = True
    if failed or reconstructed is None:
        raise StateBlobAcquisitionError(_ERROR_AGGREGATE)
    return reconstructed


@dataclass(frozen=True, slots=True)
class AcquiredStateBlob:
    published_object: PublishedStateObject
    content_bytes: bytes

    def __post_init__(self) -> None:
        published_object = _reconstructed_published_object(self.published_object)
        if (
            type(self.content_bytes) is not bytes
            or not (0 < len(self.content_bytes) <= MAXIMUM_FILE_BYTES)
            or len(self.content_bytes) != published_object.byte_count
            or hashlib.sha256(self.content_bytes).hexdigest() != published_object.sha256
        ):
            raise StateBlobAcquisitionError(_ERROR_AGGREGATE)
        object.__setattr__(self, "published_object", published_object)


def _expected_blob_objects(
    control: VerifiedPipelineStateControl,
) -> tuple[PublishedStateObject, ...]:
    expected_sizes: dict[str, int] = {}
    failed = False
    try:
        for entry in control.inventory.entries:
            prior_size = expected_sizes.get(entry.sha256)
            if prior_size is not None and prior_size != entry.byte_count:
                raise StateBlobAcquisitionError(_ERROR_PLAN)
            expected_sizes[entry.sha256] = entry.byte_count

        blob_objects = control.publication.manifest.blob_objects
        if {item.sha256 for item in blob_objects} != set(expected_sizes):
            raise StateBlobAcquisitionError(_ERROR_PLAN)
        for item in blob_objects:
            if item.byte_count != expected_sizes[item.sha256]:
                raise StateBlobAcquisitionError(_ERROR_PLAN)
    except Exception:
        failed = True
    if failed:
        raise StateBlobAcquisitionError(_ERROR_PLAN)
    return blob_objects


@dataclass(frozen=True, slots=True)
class VerifiedPipelineStateBlobs:
    control: VerifiedPipelineStateControl
    blobs: tuple[AcquiredStateBlob, ...]

    def __post_init__(self) -> None:
        control = _reconstructed_control(self.control)
        expected = _expected_blob_objects(control)
        if type(self.blobs) is not tuple:
            raise StateBlobAcquisitionError(_ERROR_AGGREGATE)

        failed = False
        reconstructed: tuple[AcquiredStateBlob, ...] = ()
        try:
            reconstructed_items: list[AcquiredStateBlob] = []
            for item in self.blobs:
                if type(item) is not AcquiredStateBlob:
                    raise StateBlobAcquisitionError(_ERROR_AGGREGATE)
                reconstructed_items.append(
                    AcquiredStateBlob(
                        published_object=item.published_object,
                        content_bytes=item.content_bytes,
                    )
                )
            reconstructed = tuple(reconstructed_items)
        except Exception:
            failed = True
        if failed:
            raise StateBlobAcquisitionError(_ERROR_AGGREGATE)

        expected_keys = tuple(
            (item.object_name, item.generation, item.byte_count, item.sha256)
            for item in expected
        )
        observed_keys = tuple(
            (
                item.published_object.object_name,
                item.published_object.generation,
                item.published_object.byte_count,
                item.published_object.sha256,
            )
            for item in reconstructed
        )
        if observed_keys != expected_keys:
            raise StateBlobAcquisitionError(_ERROR_AGGREGATE)

        object.__setattr__(self, "control", control)
        object.__setattr__(self, "blobs", reconstructed)


def acquire_verified_pipeline_state_blobs(
    control: VerifiedPipelineStateControl,
    *,
    reader: GCSObjectReader,
) -> VerifiedPipelineStateBlobs:
    """Reads every unique state blob from one verified publication.

    The publication manifest and inventory must describe the same unique
    SHA-256/byte-count set before any read occurs. Each manifest blob is
    then read exactly once, in canonical manifest order, through the
    caller-injected generation-pinned reader and independently checked for
    exact payload type, generation, size, and SHA-256. No filesystem,
    listing, latest selection, retry, fallback, client construction, or
    cloud/local mutation capability exists here.
    """

    control = _reconstructed_control(control)
    expected_objects = _expected_blob_objects(control)
    acquired: list[AcquiredStateBlob] = []

    for published_object in expected_objects:
        read_failed = False
        payload: GCSObjectPayload | None = None
        try:
            payload = reader.read_generation(
                bucket=control.request.bucket,
                object_name=published_object.object_name,
                generation=published_object.generation,
                maximum_bytes=MAXIMUM_FILE_BYTES,
            )
        except Exception:
            read_failed = True
        if read_failed:
            raise StateBlobAcquisitionError(_ERROR_READ)

        verify_failed = False
        content_bytes = b""
        try:
            if type(payload) is not GCSObjectPayload:
                raise StateBlobAcquisitionError(_ERROR_VERIFY)
            if (
                type(payload.generation) is not int
                or payload.generation != published_object.generation
            ):
                raise StateBlobAcquisitionError(_ERROR_VERIFY)
            content_bytes = payload.content_bytes
            if (
                type(content_bytes) is not bytes
                or not (0 < len(content_bytes) <= MAXIMUM_FILE_BYTES)
                or len(content_bytes) != published_object.byte_count
                or hashlib.sha256(content_bytes).hexdigest() != published_object.sha256
            ):
                raise StateBlobAcquisitionError(_ERROR_VERIFY)
        except Exception:
            verify_failed = True
        if verify_failed:
            raise StateBlobAcquisitionError(_ERROR_VERIFY)

        acquired.append(
            AcquiredStateBlob(
                published_object=published_object,
                content_bytes=content_bytes,
            )
        )

    aggregate_failed = False
    result: VerifiedPipelineStateBlobs | None = None
    try:
        result = VerifiedPipelineStateBlobs(control=control, blobs=tuple(acquired))
    except Exception:
        aggregate_failed = True
    if aggregate_failed or result is None:
        raise StateBlobAcquisitionError(_ERROR_AGGREGATE)
    return result
