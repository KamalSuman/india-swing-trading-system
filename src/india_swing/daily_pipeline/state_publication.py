from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Protocol

from india_swing._filesystem import FileSafetyError, read_stable_regular_file

from .models import DailyPipelineRun
from .state_inventory import (
    MAXIMUM_ENCODED_BYTES,
    MAXIMUM_FILE_BYTES,
    PipelineStateInventory,
    PipelineStateRoots,
    build_pipeline_state_inventory,
    encode_pipeline_state_inventory,
)

try:
    from google.cloud import storage
except ImportError:  # pragma: no cover - optional dependency
    storage = None

try:
    from google.api_core.exceptions import PreconditionFailed
except ImportError:  # pragma: no cover - optional dependency
    PreconditionFailed = None


MAXIMUM_PUBLICATION_MANIFEST_BYTES = 32 * 1024 * 1024
_MAXIMUM_GENERATION = 9223372036854775807  # 2**63 - 1, positive signed 64-bit max

_BLOB_PATH_PREFIX = "state/v1/blobs"
_INVENTORY_PATH_PREFIX = "state/v1/inventories"
_PUBLICATION_PATH_PREFIX = "state/v1/publications"

_BUCKET_PATTERN = re.compile(r"[a-z0-9][a-z0-9\-_.]{1,61}[a-z0-9]\Z")
_IP_SHAPED_PATTERN = re.compile(r"\d+\.\d+\.\d+\.\d+\Z")
_SHA256_CHARS = frozenset("0123456789abcdef")

_MANIFEST_TOP_LEVEL_KEYS = frozenset(
    {
        "schema_version",
        "bucket",
        "run_id",
        "previous_run_id",
        "market_session",
        "cutoff",
        "inventory_id",
        "inventory_object",
        "blob_objects",
        "publication_id",
    }
)
_PUBLISHED_OBJECT_KEYS = frozenset({"object_name", "generation", "byte_count", "sha256"})

_ERROR_RUN_TYPE = "run must be an exact DailyPipelineRun"
_ERROR_INVENTORY_TYPE = "inventory must be an exact PipelineStateInventory"
_ERROR_ROOTS_TYPE = "roots must be an exact PipelineStateRoots"
_ERROR_WRITER_TYPE = "writer must expose create_or_verify"
_ERROR_BUCKET = "bucket name is invalid"
_ERROR_INVENTORY_VERIFICATION_FAILED = "supplied inventory content identity verification failed"
_ERROR_REBUILD_FAILED = "pipeline state could not be rebuilt from the supplied run and roots"
_ERROR_INVENTORY_MISMATCH = "rebuilt pipeline state does not match the supplied inventory"
_ERROR_LOCAL_FILE_UNREADABLE = "local pipeline state file could not be read safely"
_ERROR_LOCAL_FILE_MISMATCH = "local pipeline state file content does not match the inventory"
_ERROR_WRITER_FAILED = "state object writer failed"
_ERROR_WRITER_RETURNED_INVALID = "state object writer returned an unexpected result"
_ERROR_BLOB_SET_MISMATCH = "published blob set does not match the inventory"
_ERROR_SCHEMA_VERSION = "pipeline state publication manifest schema version is unsupported"
_ERROR_RUN_ID = "pipeline state publication manifest run identifier is invalid"
_ERROR_PREVIOUS_RUN_ID = "pipeline state publication manifest previous run identifier is invalid"
_ERROR_MARKET_SESSION = "pipeline state publication manifest market session is invalid"
_ERROR_CUTOFF = "pipeline state publication manifest cutoff is invalid"
_ERROR_INVENTORY_ID_FIELD = "pipeline state publication manifest inventory identifier is invalid"
_ERROR_INVENTORY_OBJECT_NAME = "pipeline state publication manifest inventory object name is invalid"
_ERROR_PUBLISHED_OBJECT_TYPE = "published state object must be exact"
_ERROR_PUBLISHED_OBJECT = "published state object is invalid"
_ERROR_BLOB_OBJECTS_TYPE = "pipeline state publication manifest blob objects must be an exact tuple"
_ERROR_BLOB_OBJECTS_DUPLICATE = "pipeline state publication manifest contains a duplicate blob hash"
_ERROR_BLOB_OBJECTS_ORDER = (
    "pipeline state publication manifest blob objects are not canonically ordered"
)
_ERROR_BLOB_OBJECT_NAME = "pipeline state publication manifest blob object name is invalid"
_ERROR_MANIFEST_TYPE = "manifest must be an exact PipelineStatePublicationManifest"
_ERROR_PUBLICATION_ID = "pipeline state publication manifest identity verification failed"
_ERROR_MANIFEST_ENCODE_FAILED = "pipeline state publication manifest could not be encoded"
_ERROR_MANIFEST_TOO_LARGE = "pipeline state publication manifest exceeds the encoded-byte ceiling"
_ERROR_MANIFEST_VERIFICATION_FAILED = "pipeline state publication manifest verification failed"
_ERROR_PUBLICATION_OBJECT_NAME = "pipeline state publication object name is invalid"
_ERROR_PUBLICATION_OBJECT_MISMATCH = "pipeline state publication object does not match the manifest"
_ERROR_PAYLOAD_TYPE = "pipeline state publication manifest payload must be bytes"
_ERROR_PAYLOAD_EMPTY = "pipeline state publication manifest payload is empty"
_ERROR_PAYLOAD_TOO_LARGE = (
    "pipeline state publication manifest payload exceeds the encoded-byte ceiling"
)
_ERROR_PAYLOAD_MALFORMED = "pipeline state publication manifest payload is not valid canonical JSON"
_ERROR_PAYLOAD_SHAPE = "pipeline state publication manifest payload has an invalid shape"
_ERROR_PAYLOAD_NONCANONICAL = "pipeline state publication manifest payload is not canonical"
_ERROR_SDK_NOT_INSTALLED = "google-cloud-storage is not installed"
_ERROR_SDK_INITIALIZATION_FAILED = "google-cloud-storage client initialization failed"
_ERROR_WRITER_ARGUMENT = "state object writer argument is invalid"
_ERROR_UPLOAD_FAILED = "state object upload failed"
_ERROR_INVALID_GENERATION = "state object generation is invalid"
_ERROR_CONFLICT_VERIFICATION_FAILED = "state object conflict verification failed"
_ERROR_CONFLICT_CONTENT_MISMATCH = "state object conflict content does not match"


class StatePublicationError(Exception):
    pass


def _validate_bucket(value: object) -> str:
    if (
        type(value) is not str
        or len(value) < 3
        or len(value) > 63
        or _BUCKET_PATTERN.fullmatch(value) is None
        or ".." in value
        or _IP_SHAPED_PATTERN.fullmatch(value) is not None
    ):
        raise StatePublicationError(_ERROR_BUCKET)
    return value


def _validate_sha256(value: object, error_message: str) -> None:
    if (
        type(value) is not str
        or len(value) != 64
        or not _SHA256_CHARS.issuperset(value)
    ):
        raise StatePublicationError(error_message)


def _blob_object_name(sha256_hash: str) -> str:
    return f"{_BLOB_PATH_PREFIX}/{sha256_hash[:2]}/{sha256_hash}"


def _inventory_object_name(market_session: date, run_id: str, inventory_id: str) -> str:
    return f"{_INVENTORY_PATH_PREFIX}/{market_session.isoformat()}/{run_id}/{inventory_id}.json"


def _publication_object_name(market_session: date, run_id: str, publication_id: str) -> str:
    return f"{_PUBLICATION_PATH_PREFIX}/{market_session.isoformat()}/{run_id}/{publication_id}.json"


@dataclass(frozen=True, slots=True)
class PublishedStateObject:
    object_name: str
    generation: int
    byte_count: int
    sha256: str

    def __post_init__(self) -> None:
        if type(self.object_name) is not str or not self.object_name:
            raise StatePublicationError(_ERROR_PUBLISHED_OBJECT)
        if (
            type(self.generation) is not int
            or self.generation <= 0
            or self.generation > _MAXIMUM_GENERATION
        ):
            raise StatePublicationError(_ERROR_PUBLISHED_OBJECT)
        if type(self.byte_count) is not int or self.byte_count <= 0:
            raise StatePublicationError(_ERROR_PUBLISHED_OBJECT)
        _validate_sha256(self.sha256, _ERROR_PUBLISHED_OBJECT)


def _reconstructed_published_object(value: object) -> PublishedStateObject:
    if type(value) is not PublishedStateObject:
        raise StatePublicationError(_ERROR_PUBLISHED_OBJECT_TYPE)
    failed = False
    reconstructed: PublishedStateObject | None = None
    try:
        reconstructed = PublishedStateObject(
            object_name=value.object_name,
            generation=value.generation,
            byte_count=value.byte_count,
            sha256=value.sha256,
        )
    except StatePublicationError:
        raise
    except Exception:
        failed = True
    if failed or reconstructed is None:
        raise StatePublicationError(_ERROR_PUBLISHED_OBJECT)
    return reconstructed


class StateObjectWriter(Protocol):
    def create_or_verify(
        self,
        *,
        bucket: str,
        object_name: str,
        content_bytes: bytes,
        content_type: str,
        maximum_bytes: int,
    ) -> PublishedStateObject: ...


def _published_object_body(value: PublishedStateObject) -> dict[str, object]:
    return {
        "byte_count": value.byte_count,
        "generation": value.generation,
        "object_name": value.object_name,
        "sha256": value.sha256,
    }


def _manifest_body(
    manifest: "PipelineStatePublicationManifest", *, include_publication_id: bool
) -> dict[str, object]:
    body: dict[str, object] = {
        "blob_objects": [_published_object_body(item) for item in manifest.blob_objects],
        "bucket": manifest.bucket,
        "cutoff": manifest.cutoff.isoformat(),
        "inventory_id": manifest.inventory_id,
        "inventory_object": _published_object_body(manifest.inventory_object),
        "market_session": manifest.market_session.isoformat(),
        "previous_run_id": manifest.previous_run_id,
        "run_id": manifest.run_id,
        "schema_version": manifest.schema_version,
    }
    if include_publication_id:
        body["publication_id"] = manifest.publication_id
    return body


def _canonical_manifest_json_bytes(value: dict[str, object]) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _validate_manifest_state(
    candidate: "PipelineStatePublicationManifest",
) -> tuple[PublishedStateObject, tuple[PublishedStateObject, ...]]:
    if type(candidate.schema_version) is not int or candidate.schema_version != 1:
        raise StatePublicationError(_ERROR_SCHEMA_VERSION)
    _validate_bucket(candidate.bucket)
    _validate_sha256(candidate.run_id, _ERROR_RUN_ID)
    if candidate.previous_run_id is not None:
        _validate_sha256(candidate.previous_run_id, _ERROR_PREVIOUS_RUN_ID)
    if type(candidate.market_session) is not date:
        raise StatePublicationError(_ERROR_MARKET_SESSION)
    if type(candidate.cutoff) is not datetime:
        raise StatePublicationError(_ERROR_CUTOFF)

    failed = False
    offset = None
    try:
        offset = candidate.cutoff.utcoffset()
    except Exception:
        failed = True
    if failed:
        raise StatePublicationError(_ERROR_CUTOFF)
    if candidate.cutoff.tzinfo is None or offset != timedelta(0):
        raise StatePublicationError(_ERROR_CUTOFF)

    _validate_sha256(candidate.inventory_id, _ERROR_INVENTORY_ID_FIELD)

    inventory_object = _reconstructed_published_object(candidate.inventory_object)
    expected_inventory_name = _inventory_object_name(
        candidate.market_session, candidate.run_id, candidate.inventory_id
    )
    if inventory_object.object_name != expected_inventory_name:
        raise StatePublicationError(_ERROR_INVENTORY_OBJECT_NAME)

    if type(candidate.blob_objects) is not tuple:
        raise StatePublicationError(_ERROR_BLOB_OBJECTS_TYPE)
    reconstructed_blobs = tuple(
        _reconstructed_published_object(item) for item in candidate.blob_objects
    )
    previous_sha: str | None = None
    seen: set[str] = set()
    for blob in reconstructed_blobs:
        if blob.sha256 in seen:
            raise StatePublicationError(_ERROR_BLOB_OBJECTS_DUPLICATE)
        seen.add(blob.sha256)
        if previous_sha is not None and not previous_sha < blob.sha256:
            raise StatePublicationError(_ERROR_BLOB_OBJECTS_ORDER)
        previous_sha = blob.sha256
        if blob.object_name != _blob_object_name(blob.sha256):
            raise StatePublicationError(_ERROR_BLOB_OBJECT_NAME)

    return (inventory_object, reconstructed_blobs)


@dataclass(frozen=True, slots=True)
class PipelineStatePublicationManifest:
    schema_version: int
    bucket: str
    run_id: str
    previous_run_id: str | None
    market_session: date
    cutoff: datetime
    inventory_id: str
    inventory_object: PublishedStateObject
    blob_objects: tuple[PublishedStateObject, ...]
    publication_id: str = field(init=False)

    def __post_init__(self) -> None:
        reconstructed_inventory_object, reconstructed_blob_objects = _validate_manifest_state(
            self
        )
        object.__setattr__(self, "inventory_object", reconstructed_inventory_object)
        object.__setattr__(self, "blob_objects", reconstructed_blob_objects)
        object.__setattr__(self, "publication_id", self._calculated_publication_id())

    def _calculated_publication_id(self) -> str:
        failed = False
        digest = ""
        try:
            body_bytes = _canonical_manifest_json_bytes(
                _manifest_body(self, include_publication_id=False)
            )
            digest = hashlib.sha256(body_bytes).hexdigest()
        except Exception:
            failed = True
        if failed:
            raise StatePublicationError(_ERROR_PUBLICATION_ID)
        return digest

    def verify_content_identity(self) -> None:
        failed = False
        try:
            if type(self) is not PipelineStatePublicationManifest:
                raise StatePublicationError(_ERROR_MANIFEST_TYPE)
            _validate_manifest_state(self)
            if self.publication_id != self._calculated_publication_id():
                raise StatePublicationError(_ERROR_PUBLICATION_ID)
        except Exception:
            failed = True
        if failed:
            raise StatePublicationError(_ERROR_PUBLICATION_ID)


@dataclass(frozen=True, slots=True)
class CompletedPipelineStatePublication:
    manifest: PipelineStatePublicationManifest
    publication_object: PublishedStateObject

    def __post_init__(self) -> None:
        if type(self.manifest) is not PipelineStatePublicationManifest:
            raise StatePublicationError(_ERROR_MANIFEST_TYPE)

        failed = False
        reconstructed_manifest: PipelineStatePublicationManifest | None = None
        try:
            reconstructed_manifest = PipelineStatePublicationManifest(
                schema_version=self.manifest.schema_version,
                bucket=self.manifest.bucket,
                run_id=self.manifest.run_id,
                previous_run_id=self.manifest.previous_run_id,
                market_session=self.manifest.market_session,
                cutoff=self.manifest.cutoff,
                inventory_id=self.manifest.inventory_id,
                inventory_object=self.manifest.inventory_object,
                blob_objects=self.manifest.blob_objects,
            )
        except StatePublicationError:
            raise
        except Exception:
            failed = True
        if failed or reconstructed_manifest is None:
            raise StatePublicationError(_ERROR_MANIFEST_VERIFICATION_FAILED)
        if reconstructed_manifest.publication_id != self.manifest.publication_id:
            raise StatePublicationError(_ERROR_MANIFEST_VERIFICATION_FAILED)
        object.__setattr__(self, "manifest", reconstructed_manifest)

        reconstructed_publication_object = _reconstructed_published_object(
            self.publication_object
        )
        object.__setattr__(self, "publication_object", reconstructed_publication_object)

        expected_publication_name = _publication_object_name(
            self.manifest.market_session, self.manifest.run_id, self.manifest.publication_id
        )
        if reconstructed_publication_object.object_name != expected_publication_name:
            raise StatePublicationError(_ERROR_PUBLICATION_OBJECT_NAME)

        encode_failed = False
        expected_bytes = b""
        try:
            expected_bytes = encode_pipeline_state_publication_manifest(self.manifest)
        except Exception:
            encode_failed = True
        if encode_failed:
            raise StatePublicationError(_ERROR_MANIFEST_ENCODE_FAILED)

        if (
            reconstructed_publication_object.byte_count != len(expected_bytes)
            or reconstructed_publication_object.sha256
            != hashlib.sha256(expected_bytes).hexdigest()
        ):
            raise StatePublicationError(_ERROR_PUBLICATION_OBJECT_MISMATCH)


def encode_pipeline_state_publication_manifest(
    manifest: PipelineStatePublicationManifest,
) -> bytes:
    if type(manifest) is not PipelineStatePublicationManifest:
        raise StatePublicationError(_ERROR_MANIFEST_TYPE)
    manifest.verify_content_identity()

    failed = False
    encoded = b""
    try:
        encoded = _canonical_manifest_json_bytes(
            _manifest_body(manifest, include_publication_id=True)
        )
    except Exception:
        failed = True
    if failed:
        raise StatePublicationError(_ERROR_MANIFEST_ENCODE_FAILED)

    if len(encoded) > MAXIMUM_PUBLICATION_MANIFEST_BYTES:
        raise StatePublicationError(_ERROR_MANIFEST_TOO_LARGE)
    return encoded


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def _reject_constant(_value: str) -> None:
    raise ValueError("non-canonical numeric constant")


def _published_object_from_raw(value: object) -> PublishedStateObject:
    failed = False
    object_name = sha256_value = None
    generation: object = None
    byte_count: object = None
    try:
        if type(value) is not dict or set(value) != _PUBLISHED_OBJECT_KEYS:
            raise ValueError
        object_name = value["object_name"]
        generation = value["generation"]
        byte_count = value["byte_count"]
        sha256_value = value["sha256"]
        if type(generation) is not int or type(byte_count) is not int:
            raise ValueError
    except ValueError:
        failed = True
    if failed:
        raise StatePublicationError(_ERROR_PAYLOAD_SHAPE)
    return PublishedStateObject(
        object_name=object_name,
        generation=generation,
        byte_count=byte_count,
        sha256=sha256_value,
    )


def _manifest_from_raw(raw: object) -> PipelineStatePublicationManifest:
    failed = False
    schema_version: object = None
    bucket = run_id = previous_run_id = None
    inventory_id: object = None
    inventory_object_raw: object = None
    blob_objects_raw: object = None
    declared_publication_id = None
    market_session = cutoff = None
    try:
        if type(raw) is not dict or set(raw) != _MANIFEST_TOP_LEVEL_KEYS:
            raise ValueError
        schema_version = raw["schema_version"]
        bucket = raw["bucket"]
        run_id = raw["run_id"]
        previous_run_id = raw["previous_run_id"]
        market_session_raw = raw["market_session"]
        cutoff_raw = raw["cutoff"]
        inventory_id = raw["inventory_id"]
        inventory_object_raw = raw["inventory_object"]
        blob_objects_raw = raw["blob_objects"]
        declared_publication_id = raw["publication_id"]
        if type(market_session_raw) is not str or type(cutoff_raw) is not str:
            raise ValueError
        if type(blob_objects_raw) is not list:
            raise ValueError
        market_session = date.fromisoformat(market_session_raw)
        cutoff = datetime.fromisoformat(cutoff_raw)
    except ValueError:
        failed = True
    if failed:
        raise StatePublicationError(_ERROR_PAYLOAD_SHAPE)

    inventory_object = _published_object_from_raw(inventory_object_raw)
    blob_objects = tuple(_published_object_from_raw(item) for item in blob_objects_raw)

    construction_failed = False
    manifest: PipelineStatePublicationManifest | None = None
    try:
        manifest = PipelineStatePublicationManifest(
            schema_version=schema_version,
            bucket=bucket,
            run_id=run_id,
            previous_run_id=previous_run_id,
            market_session=market_session,
            cutoff=cutoff,
            inventory_id=inventory_id,
            inventory_object=inventory_object,
            blob_objects=blob_objects,
        )
    except StatePublicationError:
        raise
    except Exception:
        construction_failed = True
    if construction_failed or manifest is None:
        raise StatePublicationError(_ERROR_PAYLOAD_SHAPE)

    if manifest.publication_id != declared_publication_id:
        raise StatePublicationError(_ERROR_PUBLICATION_ID)
    return manifest


def parse_pipeline_state_publication_manifest(
    payload: bytes,
) -> PipelineStatePublicationManifest:
    if type(payload) is not bytes:
        raise StatePublicationError(_ERROR_PAYLOAD_TYPE)
    if not payload:
        raise StatePublicationError(_ERROR_PAYLOAD_EMPTY)
    if len(payload) > MAXIMUM_PUBLICATION_MANIFEST_BYTES:
        raise StatePublicationError(_ERROR_PAYLOAD_TOO_LARGE)

    failed = False
    raw: object = None
    try:
        text = payload.decode("utf-8", errors="strict")
        raw = json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_float=lambda value: _reject_constant(value),
            parse_constant=lambda value: _reject_constant(value),
        )
    except (UnicodeDecodeError, ValueError):
        failed = True
    if failed:
        raise StatePublicationError(_ERROR_PAYLOAD_MALFORMED)

    manifest = _manifest_from_raw(raw)

    reencoded = encode_pipeline_state_publication_manifest(manifest)
    if reencoded != payload:
        raise StatePublicationError(_ERROR_PAYLOAD_NONCANONICAL)
    return manifest


class GoogleCloudStorageStateObjectWriter:
    """Production StateObjectWriter backed by google-cloud-storage.

    Exercised by tests/test_pipeline_state_publication.py via an injected
    fake SDK client; only real GCP/network access is absent from those
    tests. Never lists a bucket, never selects a "latest" object, never
    overwrites, and never deletes.
    """

    def __init__(self, client: object | None = None) -> None:
        if client is not None:
            self._client = client
        elif storage is not None:
            initialization_failed = False
            initialized_client: object | None = None
            try:
                initialized_client = storage.Client()
            except Exception:
                initialization_failed = True
            if initialization_failed or initialized_client is None:
                raise StatePublicationError(_ERROR_SDK_INITIALIZATION_FAILED)
            self._client = initialized_client
        else:
            raise StatePublicationError(_ERROR_SDK_NOT_INSTALLED)

    def create_or_verify(
        self,
        *,
        bucket: str,
        object_name: str,
        content_bytes: bytes,
        content_type: str,
        maximum_bytes: int,
    ) -> PublishedStateObject:
        _validate_bucket(bucket)
        if type(object_name) is not str or not object_name:
            raise StatePublicationError(_ERROR_WRITER_ARGUMENT)
        if type(content_type) is not str or not content_type:
            raise StatePublicationError(_ERROR_WRITER_ARGUMENT)
        if type(maximum_bytes) is not int or maximum_bytes <= 0:
            raise StatePublicationError(_ERROR_WRITER_ARGUMENT)
        if type(content_bytes) is not bytes or not (0 < len(content_bytes) <= maximum_bytes):
            raise StatePublicationError(_ERROR_WRITER_ARGUMENT)

        expected_sha256 = hashlib.sha256(content_bytes).hexdigest()

        setup_failed = False
        blob: object | None = None
        try:
            blob = self._client.bucket(bucket).blob(object_name)
        except Exception:
            setup_failed = True
        if setup_failed or blob is None:
            raise StatePublicationError(_ERROR_UPLOAD_FAILED)

        create_failed = False
        conflict = False
        try:
            blob.upload_from_string(
                content_bytes,
                content_type=content_type,
                if_generation_match=0,
                checksum="auto",
                retry=None,
            )
        except Exception as error:
            if PreconditionFailed is not None and isinstance(error, PreconditionFailed):
                conflict = True
            else:
                create_failed = True
        if create_failed:
            raise StatePublicationError(_ERROR_UPLOAD_FAILED)

        if not conflict:
            generation_read_failed = False
            observed_generation: object = None
            try:
                observed_generation = blob.generation
            except Exception:
                generation_read_failed = True
            if generation_read_failed:
                raise StatePublicationError(_ERROR_INVALID_GENERATION)
            if (
                observed_generation is None
                or type(observed_generation) is bool
                or type(observed_generation) is not int
                or observed_generation <= 0
                or observed_generation > _MAXIMUM_GENERATION
            ):
                raise StatePublicationError(_ERROR_INVALID_GENERATION)
            return PublishedStateObject(
                object_name=object_name,
                generation=observed_generation,
                byte_count=len(content_bytes),
                sha256=expected_sha256,
            )

        reload_failed = False
        generation: object = None
        try:
            blob.reload(retry=None)
            generation = blob.generation
        except Exception:
            reload_failed = True
        if reload_failed:
            raise StatePublicationError(_ERROR_CONFLICT_VERIFICATION_FAILED)

        if (
            generation is None
            or type(generation) is bool
            or type(generation) is not int
            or generation <= 0
            or generation > _MAXIMUM_GENERATION
        ):
            raise StatePublicationError(_ERROR_INVALID_GENERATION)

        download_failed = False
        downloaded = b""
        pinned_generation: object = None
        try:
            pinned_blob = self._client.bucket(bucket).blob(
                object_name, generation=generation
            )
            downloaded = pinned_blob.download_as_bytes(
                end=maximum_bytes,
                raw_download=True,
                if_generation_match=generation,
                retry=None,
            )
            pinned_generation = pinned_blob.generation
        except Exception:
            download_failed = True
        if download_failed:
            raise StatePublicationError(_ERROR_CONFLICT_VERIFICATION_FAILED)

        if (
            type(pinned_generation) is not int
            or pinned_generation <= 0
            or pinned_generation > _MAXIMUM_GENERATION
            or pinned_generation != generation
        ):
            raise StatePublicationError(_ERROR_INVALID_GENERATION)

        if (
            type(downloaded) is not bytes
            or len(downloaded) != len(content_bytes)
            or downloaded != content_bytes
        ):
            raise StatePublicationError(_ERROR_CONFLICT_CONTENT_MISMATCH)

        return PublishedStateObject(
            object_name=object_name,
            generation=generation,
            byte_count=len(content_bytes),
            sha256=expected_sha256,
        )


def _create_or_verify_checked(
    writer: StateObjectWriter,
    *,
    bucket: str,
    object_name: str,
    content_bytes: bytes,
    content_type: str,
    maximum_bytes: int,
) -> PublishedStateObject:
    expected_byte_count = len(content_bytes)
    expected_sha256 = hashlib.sha256(content_bytes).hexdigest()

    failed = False
    result: object = None
    try:
        result = writer.create_or_verify(
            bucket=bucket,
            object_name=object_name,
            content_bytes=content_bytes,
            content_type=content_type,
            maximum_bytes=maximum_bytes,
        )
    except Exception:
        failed = True
    if failed:
        raise StatePublicationError(_ERROR_WRITER_FAILED)

    reconstruction_failed = False
    reconstructed: PublishedStateObject | None = None
    try:
        reconstructed = _reconstructed_published_object(result)
    except Exception:
        reconstruction_failed = True
    if reconstruction_failed or reconstructed is None:
        raise StatePublicationError(_ERROR_WRITER_RETURNED_INVALID)
    if (
        reconstructed.object_name != object_name
        or reconstructed.byte_count != expected_byte_count
        or reconstructed.sha256 != expected_sha256
    ):
        raise StatePublicationError(_ERROR_WRITER_RETURNED_INVALID)
    return reconstructed


def publish_pipeline_state(
    run: DailyPipelineRun,
    inventory: PipelineStateInventory,
    roots: PipelineStateRoots,
    bucket: str,
    writer: StateObjectWriter,
) -> CompletedPipelineStatePublication:
    if type(run) is not DailyPipelineRun:
        raise StatePublicationError(_ERROR_RUN_TYPE)
    if type(inventory) is not PipelineStateInventory:
        raise StatePublicationError(_ERROR_INVENTORY_TYPE)
    if type(roots) is not PipelineStateRoots:
        raise StatePublicationError(_ERROR_ROOTS_TYPE)
    bucket = _validate_bucket(bucket)
    writer_validation_failed = False
    writer_method: object = None
    try:
        writer_method = getattr(writer, "create_or_verify")
    except Exception:
        writer_validation_failed = True
    if writer_validation_failed or not callable(writer_method):
        raise StatePublicationError(_ERROR_WRITER_TYPE)

    verify_failed = False
    supplied_canonical_bytes = b""
    try:
        inventory.verify_content_identity()
        supplied_canonical_bytes = encode_pipeline_state_inventory(inventory)
    except Exception:
        verify_failed = True
    if verify_failed:
        raise StatePublicationError(_ERROR_INVENTORY_VERIFICATION_FAILED)

    rebuild_failed = False
    rebuilt_canonical_bytes = b""
    try:
        rebuilt_inventory = build_pipeline_state_inventory(run, roots)
        rebuilt_canonical_bytes = encode_pipeline_state_inventory(rebuilt_inventory)
    except Exception:
        rebuild_failed = True
    if rebuild_failed:
        raise StatePublicationError(_ERROR_REBUILD_FAILED)
    if rebuilt_canonical_bytes != supplied_canonical_bytes:
        raise StatePublicationError(_ERROR_INVENTORY_MISMATCH)

    unique_hashes = sorted({entry.sha256 for entry in inventory.entries})
    uploaded_by_hash: dict[str, PublishedStateObject] = {}

    entries_in_publication_order = sorted(
        inventory.entries,
        key=lambda entry: (entry.sha256, entry.root_name, entry.relative_path),
    )
    for entry in entries_in_publication_order:
        root_path: Path = getattr(roots, entry.root_name)
        file_path = root_path.joinpath(*entry.relative_path.split("/"))

        read_failed = False
        payload = b""
        try:
            payload = read_stable_regular_file(file_path, maximum_bytes=MAXIMUM_FILE_BYTES)
        except FileSafetyError:
            read_failed = True
        if read_failed:
            raise StatePublicationError(_ERROR_LOCAL_FILE_UNREADABLE)

        if len(payload) != entry.byte_count:
            raise StatePublicationError(_ERROR_LOCAL_FILE_MISMATCH)
        observed_sha256 = hashlib.sha256(payload).hexdigest()
        if observed_sha256 != entry.sha256:
            raise StatePublicationError(_ERROR_LOCAL_FILE_MISMATCH)

        if entry.sha256 in uploaded_by_hash:
            payload = b""
            continue

        object_name = _blob_object_name(entry.sha256)
        published = _create_or_verify_checked(
            writer,
            bucket=bucket,
            object_name=object_name,
            content_bytes=payload,
            content_type="application/octet-stream",
            maximum_bytes=MAXIMUM_FILE_BYTES,
        )
        uploaded_by_hash[entry.sha256] = published
        payload = b""

    if set(uploaded_by_hash) != set(unique_hashes):
        raise StatePublicationError(_ERROR_BLOB_SET_MISMATCH)

    blob_objects = tuple(uploaded_by_hash[sha] for sha in unique_hashes)

    inventory_object_name = _inventory_object_name(
        inventory.market_session, inventory.run_id, inventory.inventory_id
    )
    inventory_object = _create_or_verify_checked(
        writer,
        bucket=bucket,
        object_name=inventory_object_name,
        content_bytes=supplied_canonical_bytes,
        content_type="application/json",
        maximum_bytes=MAXIMUM_ENCODED_BYTES,
    )

    manifest = PipelineStatePublicationManifest(
        schema_version=1,
        bucket=bucket,
        run_id=inventory.run_id,
        previous_run_id=inventory.previous_run_id,
        market_session=inventory.market_session,
        cutoff=inventory.cutoff,
        inventory_id=inventory.inventory_id,
        inventory_object=inventory_object,
        blob_objects=blob_objects,
    )
    manifest_bytes = encode_pipeline_state_publication_manifest(manifest)

    publication_object_name = _publication_object_name(
        manifest.market_session, manifest.run_id, manifest.publication_id
    )
    publication_object = _create_or_verify_checked(
        writer,
        bucket=bucket,
        object_name=publication_object_name,
        content_bytes=manifest_bytes,
        content_type="application/json",
        maximum_bytes=MAXIMUM_PUBLICATION_MANIFEST_BYTES,
    )

    return CompletedPipelineStatePublication(
        manifest=manifest,
        publication_object=publication_object,
    )
