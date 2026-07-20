from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .state_publication_acquisition import (
    PinnedStatePublicationRequest,
)


MAXIMUM_PINNED_GCS_STATE_RESTORE_SPEC_BYTES = 64 * 1024
PINNED_GCS_STATE_RESTORE_SPEC_SCHEMA_VERSION = 1

_CONCRETE_PATH_TYPE: type = type(Path())
_MAXIMUM_INTEGER_DIGITS = 20
_MAXIMUM_DESTINATION_CHARACTERS = 4096
_TOP_LEVEL_KEYS = frozenset(
    {"schema_version", "publication_request", "destination"}
)
_REQUEST_KEYS = frozenset(
    {
        "bucket",
        "publication_object_name",
        "generation",
        "expected_sha256",
        "expected_run_id",
    }
)

_ERROR_SPEC = "pinned state restoration spec bytes are invalid"
_ERROR_UTF8 = "pinned state restoration spec is not valid UTF-8"
_ERROR_JSON = "pinned state restoration spec is not valid JSON"
_ERROR_DUPLICATE = "pinned state restoration spec contains a duplicate key"
_ERROR_NUMERIC = "pinned state restoration spec numeric value is invalid"
_ERROR_SHAPE = "pinned state restoration spec shape is invalid"
_ERROR_SCHEMA = "pinned state restoration spec schema version is unsupported"
_ERROR_REQUEST = "pinned state restoration spec publication request is invalid"
_ERROR_DESTINATION = "pinned state restoration spec destination is invalid"
_ERROR_NONCANONICAL = "pinned state restoration spec is not canonical"
_ERROR_ENCODE = "pinned state restoration spec could not be encoded"


class PinnedGCSStateRestoreSpecError(Exception):
    pass


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise PinnedGCSStateRestoreSpecError(_ERROR_DUPLICATE)
        result[key] = value
    return result


def _reject_numeric(_token: str) -> None:
    raise PinnedGCSStateRestoreSpecError(_ERROR_NUMERIC)


def _parse_int(token: str) -> int:
    digits = token[1:] if token[:1] == "-" else token
    if len(digits) > _MAXIMUM_INTEGER_DIGITS:
        raise PinnedGCSStateRestoreSpecError(_ERROR_NUMERIC)
    return int(token)


def _reconstructed_request(value: object) -> PinnedStatePublicationRequest:
    if type(value) is not PinnedStatePublicationRequest:
        raise PinnedGCSStateRestoreSpecError(_ERROR_REQUEST)
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
        raise PinnedGCSStateRestoreSpecError(_ERROR_REQUEST)
    return reconstructed


def _validated_destination(value: object, expected_run_id: str) -> Path:
    if (
        type(value) is not _CONCRETE_PATH_TYPE
        or not value.is_absolute()
        or ".." in value.parts
        or value.name != expected_run_id
        or value.parent == value
        or len(str(value)) > _MAXIMUM_DESTINATION_CHARACTERS
        or "\x00" in str(value)
    ):
        raise PinnedGCSStateRestoreSpecError(_ERROR_DESTINATION)
    return value


@dataclass(frozen=True, slots=True)
class PinnedGCSStateRestoreSpec:
    schema_version: int
    publication_request: PinnedStatePublicationRequest
    destination: Path

    def __post_init__(self) -> None:
        if (
            type(self.schema_version) is not int
            or self.schema_version != PINNED_GCS_STATE_RESTORE_SPEC_SCHEMA_VERSION
        ):
            raise PinnedGCSStateRestoreSpecError(_ERROR_SCHEMA)
        request = _reconstructed_request(self.publication_request)
        destination = _validated_destination(
            self.destination,
            request.expected_run_id,
        )
        object.__setattr__(self, "publication_request", request)
        object.__setattr__(self, "destination", destination)


def _spec_body(spec: PinnedGCSStateRestoreSpec) -> dict[str, object]:
    request = spec.publication_request
    return {
        "destination": str(spec.destination),
        "publication_request": {
            "bucket": request.bucket,
            "expected_run_id": request.expected_run_id,
            "expected_sha256": request.expected_sha256,
            "generation": request.generation,
            "publication_object_name": request.publication_object_name,
        },
        "schema_version": spec.schema_version,
    }


def _canonical_bytes(spec: PinnedGCSStateRestoreSpec) -> bytes:
    return (
        json.dumps(
            _spec_body(spec),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def encode_pinned_gcs_state_restore_spec(
    spec: PinnedGCSStateRestoreSpec,
) -> bytes:
    if type(spec) is not PinnedGCSStateRestoreSpec:
        raise PinnedGCSStateRestoreSpecError(_ERROR_ENCODE)
    failed = False
    encoded = b""
    try:
        reconstructed = PinnedGCSStateRestoreSpec(
            schema_version=spec.schema_version,
            publication_request=spec.publication_request,
            destination=spec.destination,
        )
        encoded = _canonical_bytes(reconstructed)
    except Exception:
        failed = True
    if (
        failed
        or not encoded
        or len(encoded) > MAXIMUM_PINNED_GCS_STATE_RESTORE_SPEC_BYTES
    ):
        raise PinnedGCSStateRestoreSpecError(_ERROR_ENCODE)
    return encoded


def parse_pinned_gcs_state_restore_spec(
    spec_bytes: bytes,
) -> PinnedGCSStateRestoreSpec:
    """Parses one exact canonical operator-governed restore instruction.

    This pure boundary performs no filesystem, GCS, environment, clock, or
    client access. It rejects duplicate keys at every depth, unsupported
    numeric forms, shape variation, and any byte representation that does not
    exactly equal the canonical encoding of the reconstructed value. The
    expected publication hash is supplied by the control plane and is never
    inferred from publication content here.
    """

    if (
        type(spec_bytes) is not bytes
        or not spec_bytes
        or len(spec_bytes) > MAXIMUM_PINNED_GCS_STATE_RESTORE_SPEC_BYTES
    ):
        raise PinnedGCSStateRestoreSpecError(_ERROR_SPEC)
    try:
        text = spec_bytes.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        raise PinnedGCSStateRestoreSpecError(_ERROR_UTF8) from None

    try:
        raw = json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_float=_reject_numeric,
            parse_constant=_reject_numeric,
            parse_int=_parse_int,
        )
    except PinnedGCSStateRestoreSpecError:
        raise
    except (json.JSONDecodeError, RecursionError):
        raise PinnedGCSStateRestoreSpecError(_ERROR_JSON) from None

    if type(raw) is not dict or set(raw) != _TOP_LEVEL_KEYS:
        raise PinnedGCSStateRestoreSpecError(_ERROR_SHAPE)
    schema_version = raw["schema_version"]
    if (
        type(schema_version) is not int
        or schema_version != PINNED_GCS_STATE_RESTORE_SPEC_SCHEMA_VERSION
    ):
        raise PinnedGCSStateRestoreSpecError(_ERROR_SCHEMA)

    request_raw = raw["publication_request"]
    if type(request_raw) is not dict or set(request_raw) != _REQUEST_KEYS:
        raise PinnedGCSStateRestoreSpecError(_ERROR_REQUEST)
    try:
        request = PinnedStatePublicationRequest(
            bucket=request_raw["bucket"],
            publication_object_name=request_raw["publication_object_name"],
            generation=request_raw["generation"],
            expected_sha256=request_raw["expected_sha256"],
            expected_run_id=request_raw["expected_run_id"],
        )
    except Exception:
        raise PinnedGCSStateRestoreSpecError(_ERROR_REQUEST) from None

    destination_raw = raw["destination"]
    if (
        type(destination_raw) is not str
        or not destination_raw
        or len(destination_raw) > _MAXIMUM_DESTINATION_CHARACTERS
        or "\x00" in destination_raw
    ):
        raise PinnedGCSStateRestoreSpecError(_ERROR_DESTINATION)
    try:
        spec = PinnedGCSStateRestoreSpec(
            schema_version=schema_version,
            publication_request=request,
            destination=Path(destination_raw),
        )
    except PinnedGCSStateRestoreSpecError:
        raise
    except Exception:
        raise PinnedGCSStateRestoreSpecError(_ERROR_DESTINATION) from None

    canonical_failed = False
    canonical = b""
    try:
        canonical = _canonical_bytes(spec)
    except Exception:
        canonical_failed = True
    if canonical_failed or canonical != spec_bytes:
        raise PinnedGCSStateRestoreSpecError(_ERROR_NONCANONICAL)
    return spec
