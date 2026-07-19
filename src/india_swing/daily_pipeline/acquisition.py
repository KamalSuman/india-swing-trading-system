from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date
from enum import Enum
from types import MappingProxyType
from typing import Mapping, Protocol

try:
    from google.cloud import storage
except ImportError:  # pragma: no cover - optional dependency
    storage = None


class AcquisitionError(ValueError):
    pass


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_BUCKET_NAME = re.compile(r"[a-z0-9][a-z0-9\-_.]{1,61}[a-z0-9]\Z")
_MAXIMUM_GENERATION = 9223372036854775807  # 2**63 - 1, positive signed 64-bit max


class AcquisitionFileType(str, Enum):
    SECURITY_MASTER = "SECURITY_MASTER"
    DAILY_BUNDLE = "DAILY_BUNDLE"


_MAXIMUM_OBJECT_BYTES: Mapping[AcquisitionFileType, int] = MappingProxyType(
    {
        AcquisitionFileType.SECURITY_MASTER: 32 * 1024 * 1024,
        AcquisitionFileType.DAILY_BUNDLE: 128 * 1024 * 1024,
    }
)


def _maximum_bytes_for(file_type: AcquisitionFileType) -> int:
    try:
        return _MAXIMUM_OBJECT_BYTES[file_type]
    except KeyError:
        raise AcquisitionError("unsupported acquisition file type") from None


def _security_master_filename(target_session: date) -> str:
    return f"NSE_CM_security_{target_session.strftime('%d%m%Y')}.csv.gz"


def _daily_bundle_filename() -> str:
    return "Reports-Daily-Multiple.zip"


def _expected_object_name(file_type: AcquisitionFileType, target_session: date) -> str:
    if file_type is AcquisitionFileType.SECURITY_MASTER:
        filename = _security_master_filename(target_session)
    elif file_type is AcquisitionFileType.DAILY_BUNDLE:
        filename = _daily_bundle_filename()
    else:
        raise AcquisitionError("unsupported acquisition file type")
    return f"landing/{target_session.isoformat()}/{filename}"


def _landing_manifest_filename() -> str:
    return "landing-manifest.json"


def _expected_manifest_object_name(target_session: date) -> str:
    return f"landing/{target_session.isoformat()}/{_landing_manifest_filename()}"


@dataclass(frozen=True, slots=True)
class LandingObjectRequest:
    """One explicit, generation-pinned GCS landing object to read.

    Every field is required and independently verified. There is no
    default, no bucket listing, and no "latest object" resolution
    anywhere in this module.
    """

    bucket: str
    object_name: str
    generation: int
    expected_sha256: str
    target_session: date
    file_type: AcquisitionFileType

    def __post_init__(self) -> None:
        if not isinstance(self.bucket, str) or _BUCKET_NAME.fullmatch(self.bucket) is None:
            raise AcquisitionError("acquisition bucket name is invalid")
        if type(self.target_session) is not date:
            raise AcquisitionError("acquisition target session must be a date")
        if not isinstance(self.file_type, AcquisitionFileType):
            raise AcquisitionError("acquisition file type is invalid")
        if (
            type(self.generation) is not int
            or self.generation <= 0
            or self.generation > _MAXIMUM_GENERATION
        ):
            raise AcquisitionError(
                "acquisition generation must be a positive integer within the supported range"
            )
        if (
            not isinstance(self.expected_sha256, str)
            or _SHA256.fullmatch(self.expected_sha256) is None
        ):
            raise AcquisitionError("acquisition expected SHA-256 is invalid")
        expected_object_name = _expected_object_name(self.file_type, self.target_session)
        if not isinstance(self.object_name, str) or self.object_name != expected_object_name:
            raise AcquisitionError(
                "acquisition object name does not match the expected session-bound landing path"
            )


@dataclass(frozen=True, slots=True)
class LandingManifestObjectRequest:
    """One explicit, generation-pinned GCS landing-manifest object to read.

    Unlike LandingObjectRequest, this carries no expected hash and no
    file_type: TrustedLandingManifestBinding (in landing_manifest.py) is the
    single independent authority for the manifest's expected hash. There is
    no default, no bucket listing, and no "latest object" resolution
    anywhere in this module.
    """

    bucket: str
    object_name: str
    generation: int
    target_session: date

    def __post_init__(self) -> None:
        if not isinstance(self.bucket, str) or _BUCKET_NAME.fullmatch(self.bucket) is None:
            raise AcquisitionError("acquisition bucket name is invalid")
        if type(self.target_session) is not date:
            raise AcquisitionError("acquisition target session must be a date")
        if (
            type(self.generation) is not int
            or self.generation <= 0
            or self.generation > _MAXIMUM_GENERATION
        ):
            raise AcquisitionError(
                "acquisition generation must be a positive integer within the supported range"
            )
        expected_object_name = _expected_manifest_object_name(self.target_session)
        if not isinstance(self.object_name, str) or self.object_name != expected_object_name:
            raise AcquisitionError(
                "acquisition object name does not match the expected session-bound "
                "landing-manifest path"
            )


@dataclass(frozen=True, slots=True)
class AcquiredFile:
    """One verified, generation-pinned GCS landing object."""

    bucket: str
    object_name: str
    generation: int
    target_session: date
    file_type: AcquisitionFileType
    content_bytes: bytes
    sha256_hash: str


@dataclass(frozen=True, slots=True)
class GCSObjectPayload:
    content_bytes: bytes
    generation: int


class GCSObjectReader(Protocol):
    """Reads exactly one explicit object generation.

    Implementations must never list a bucket and must never resolve a
    "latest" object; every read is pinned to the caller-supplied
    generation, bounded by maximum_bytes.
    """

    def read_generation(
        self, *, bucket: str, object_name: str, generation: int, maximum_bytes: int
    ) -> GCSObjectPayload: ...


class GoogleCloudStorageObjectReader:
    """Production GCSObjectReader backed by google-cloud-storage.

    Exercised by tests/test_acquisition.py via an injected fake SDK client
    (FakeStorageClient/FakeBucket/FakeBlob); only real GCP/network access is
    absent from those tests.
    """

    def __init__(self, client: object | None = None) -> None:
        if client is not None:
            self._client = client
        elif storage is not None:
            self._client = storage.Client()
        else:
            raise AcquisitionError("google-cloud-storage is not installed")

    def read_generation(
        self, *, bucket: str, object_name: str, generation: int, maximum_bytes: int
    ) -> GCSObjectPayload:
        if (
            type(generation) is bool
            or type(generation) is not int
            or generation <= 0
            or generation > _MAXIMUM_GENERATION
        ):
            raise AcquisitionError(
                "acquisition requested generation must be a positive integer within the supported range"
            )
        blob = self._client.bucket(bucket).blob(object_name, generation=generation)
        content_bytes = blob.download_as_bytes(
            end=maximum_bytes,
            raw_download=True,
            if_generation_match=generation,
            retry=None,
        )
        observed_generation = blob.generation
        if observed_generation is None:
            raise AcquisitionError("acquisition observed generation is missing")
        if type(observed_generation) is bool or type(observed_generation) is not int:
            raise AcquisitionError("acquisition observed generation must be an integer")
        if observed_generation <= 0 or observed_generation > _MAXIMUM_GENERATION:
            raise AcquisitionError(
                "acquisition observed generation must be positive and within the supported range"
            )
        if observed_generation != generation:
            raise AcquisitionError(
                "acquisition observed generation does not match the requested generation"
            )
        return GCSObjectPayload(content_bytes=content_bytes, generation=observed_generation)


class GCSLandingObjectReader:
    """Reads one explicit, generation-pinned NSE landing object from GCS.

    Never lists a bucket and never selects a "latest" object. Both the
    generation and the content hash returned by the underlying client are
    re-verified against the request after download; the client is not
    trusted to have pinned correctly on its own.
    """

    def __init__(self, client: GCSObjectReader) -> None:
        self._client = client

    def read(self, request: LandingObjectRequest) -> AcquiredFile:
        if type(request) is not LandingObjectRequest:
            raise AcquisitionError("acquisition request must be exact")
        maximum_bytes = _maximum_bytes_for(request.file_type)
        payload = self._client.read_generation(
            bucket=request.bucket,
            object_name=request.object_name,
            generation=request.generation,
            maximum_bytes=maximum_bytes,
        )
        if type(payload) is not GCSObjectPayload:
            raise AcquisitionError("acquisition reader returned an invalid payload")
        if type(payload.generation) is not int or payload.generation != request.generation:
            raise AcquisitionError(
                "acquisition object generation does not match the requested generation"
            )
        content_bytes = payload.content_bytes
        if not isinstance(content_bytes, bytes):
            raise AcquisitionError("acquisition object content must be bytes")
        if len(content_bytes) == 0:
            raise AcquisitionError("acquisition object is empty")
        if len(content_bytes) > maximum_bytes:
            raise AcquisitionError("acquisition object exceeds the maximum allowed size")
        observed_sha256 = hashlib.sha256(content_bytes).hexdigest()
        if observed_sha256 != request.expected_sha256:
            raise AcquisitionError(
                "acquisition object SHA-256 does not match the expected digest"
            )
        return AcquiredFile(
            bucket=request.bucket,
            object_name=request.object_name,
            generation=request.generation,
            target_session=request.target_session,
            file_type=request.file_type,
            content_bytes=content_bytes,
            sha256_hash=observed_sha256,
        )
