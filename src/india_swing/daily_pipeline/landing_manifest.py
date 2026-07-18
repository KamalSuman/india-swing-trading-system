from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from .acquisition import AcquisitionError, AcquisitionFileType, LandingObjectRequest


class LandingManifestError(ValueError):
    pass


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_BUCKET_NAME = re.compile(r"[a-z0-9][a-z0-9\-_.]{1,61}[a-z0-9]\Z")
_CANONICAL_DATE = re.compile(r"\d{4}-\d{2}-\d{2}\Z")

_MAXIMUM_MANIFEST_BYTES = 64 * 1024

_TOP_LEVEL_KEYS = frozenset({"schema_version", "knowledge_time", "target_session", "objects"})
_OBJECT_KEYS = frozenset({"file_type", "bucket", "object_name", "generation", "sha256"})

_ERR_BINDING = "landing manifest trust binding is invalid"
_ERR_MANIFEST_BYTES = "landing manifest bytes are invalid"
_ERR_HASH_MISMATCH = "landing manifest hash does not match the trusted binding"
_ERR_UTF8 = "landing manifest is not valid UTF-8"
_ERR_JSON = "landing manifest is not valid JSON"
_ERR_DUPLICATE_KEY = "landing manifest contains a duplicate key"
_ERR_NUMERIC = "landing manifest contains an unsupported numeric value"
_ERR_TOP_LEVEL_SHAPE = "landing manifest shape is invalid"
_ERR_SCHEMA_VERSION = "landing manifest schema version is unsupported"
_ERR_KNOWLEDGE_TIME = "landing manifest knowledge time is invalid"
_ERR_KNOWLEDGE_TIME_BOUNDS = "landing manifest knowledge time is outside the trusted bounds"
_ERR_TARGET_SESSION = "landing manifest target session is invalid"
_ERR_TARGET_SESSION_MISMATCH = "landing manifest target session does not match the trusted binding"
_ERR_OBJECTS_SHAPE = "landing manifest objects are invalid"
_ERR_OBJECT_SHAPE = "landing manifest object shape is invalid"
_ERR_OBJECT_BUCKET = "landing manifest object bucket is not allowed"
_ERR_OBJECT_INVALID = "landing manifest object could not be verified"
_ERR_OBJECTS_INCOMPLETE = "landing manifest is missing a required object"

_MAXIMUM_INTEGER_DIGITS = 20  # generous headroom over the 19-digit int64 max


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise LandingManifestError(_ERR_DUPLICATE_KEY)
        result[key] = value
    return result


def _reject_numeric(_token: str) -> None:
    raise LandingManifestError(_ERR_NUMERIC)


def _parse_int(token: str) -> int:
    digits = token[1:] if token[:1] == "-" else token
    if len(digits) > _MAXIMUM_INTEGER_DIGITS:
        raise LandingManifestError(_ERR_NUMERIC)
    return int(token)


@dataclass(frozen=True, slots=True)
class TrustedLandingManifestBinding:
    """The only source of trust this verifier accepts.

    expected_manifest_sha256 must come from an independently governed
    record, never from anything inside the manifest itself. not_before and
    cutoff together bound knowledge_time; neither is inferred from
    target_session or the system clock.
    """

    expected_manifest_sha256: str
    allowed_bucket: str
    target_session: date
    not_before: datetime
    cutoff: datetime

    def __post_init__(self) -> None:
        if (
            not isinstance(self.expected_manifest_sha256, str)
            or _SHA256.fullmatch(self.expected_manifest_sha256) is None
        ):
            raise LandingManifestError(_ERR_BINDING)
        if not isinstance(self.allowed_bucket, str) or _BUCKET_NAME.fullmatch(self.allowed_bucket) is None:
            raise LandingManifestError(_ERR_BINDING)
        if type(self.target_session) is not date:
            raise LandingManifestError(_ERR_BINDING)
        if type(self.not_before) is not datetime:
            raise LandingManifestError(_ERR_BINDING)
        if self.not_before.tzinfo is None or self.not_before.utcoffset() is None:
            raise LandingManifestError(_ERR_BINDING)
        if type(self.cutoff) is not datetime:
            raise LandingManifestError(_ERR_BINDING)
        if self.cutoff.tzinfo is None or self.cutoff.utcoffset() is None:
            raise LandingManifestError(_ERR_BINDING)
        if self.not_before > self.cutoff:
            raise LandingManifestError(_ERR_BINDING)


@dataclass(frozen=True, slots=True)
class VerifiedLandingManifest:
    """One landing manifest whose bytes matched an externally trusted hash.

    Requires and retains the exact TrustedLandingManifestBinding it was
    verified against; there is no default, so this cannot be constructed
    without one. __post_init__ defensively re-derives and re-checks the
    manifest hash, target session, allowed bucket, and knowledge-time
    bounds against that binding, and re-checks the existing object
    invariants, rather than trusting that a caller assembled a
    self-consistent instance correctly.
    """

    schema_version: int
    manifest_sha256: str
    manifest_bytes: bytes
    knowledge_time: datetime
    target_session: date
    security_master: LandingObjectRequest
    daily_bundle: LandingObjectRequest
    binding: TrustedLandingManifestBinding

    def __post_init__(self) -> None:
        if type(self.binding) is not TrustedLandingManifestBinding:
            raise LandingManifestError(_ERR_BINDING)
        if type(self.schema_version) is not int or self.schema_version != 1:
            raise LandingManifestError(_ERR_SCHEMA_VERSION)
        if not isinstance(self.manifest_sha256, str) or _SHA256.fullmatch(self.manifest_sha256) is None:
            raise LandingManifestError(_ERR_HASH_MISMATCH)
        if type(self.manifest_bytes) is not bytes or len(self.manifest_bytes) == 0:
            raise LandingManifestError(_ERR_MANIFEST_BYTES)
        if hashlib.sha256(self.manifest_bytes).hexdigest() != self.manifest_sha256:
            raise LandingManifestError(_ERR_HASH_MISMATCH)
        if self.manifest_sha256 != self.binding.expected_manifest_sha256:
            raise LandingManifestError(_ERR_HASH_MISMATCH)
        if type(self.knowledge_time) is not datetime:
            raise LandingManifestError(_ERR_KNOWLEDGE_TIME)
        if self.knowledge_time.tzinfo is None or self.knowledge_time.utcoffset() != timedelta(0):
            raise LandingManifestError(_ERR_KNOWLEDGE_TIME)
        if self.knowledge_time < self.binding.not_before or self.knowledge_time > self.binding.cutoff:
            raise LandingManifestError(_ERR_KNOWLEDGE_TIME_BOUNDS)
        if type(self.target_session) is not date:
            raise LandingManifestError(_ERR_TARGET_SESSION)
        if self.target_session != self.binding.target_session:
            raise LandingManifestError(_ERR_TARGET_SESSION_MISMATCH)
        if (
            type(self.security_master) is not LandingObjectRequest
            or self.security_master.file_type is not AcquisitionFileType.SECURITY_MASTER
        ):
            raise LandingManifestError(_ERR_OBJECT_SHAPE)
        if (
            type(self.daily_bundle) is not LandingObjectRequest
            or self.daily_bundle.file_type is not AcquisitionFileType.DAILY_BUNDLE
        ):
            raise LandingManifestError(_ERR_OBJECT_SHAPE)
        if (
            self.security_master.target_session != self.target_session
            or self.daily_bundle.target_session != self.target_session
        ):
            raise LandingManifestError(_ERR_TARGET_SESSION_MISMATCH)
        if (
            self.security_master.bucket != self.binding.allowed_bucket
            or self.daily_bundle.bucket != self.binding.allowed_bucket
        ):
            raise LandingManifestError(_ERR_OBJECT_BUCKET)


class LandingManifestVerifier:
    """Verifies one landing manifest against an externally trusted binding.

    Performs no GCS access, network access, bucket listing, "latest"
    selection, filesystem reads, environment variable reads, or
    current-clock calls. Every error is a generic, sanitized
    LandingManifestError; no manifest contents, bucket names, object
    paths, hashes, or nested exception text are ever included in a
    raised message.
    """

    def verify(
        self, manifest_bytes: bytes, binding: TrustedLandingManifestBinding
    ) -> VerifiedLandingManifest:
        if type(binding) is not TrustedLandingManifestBinding:
            raise LandingManifestError(_ERR_BINDING)
        if type(manifest_bytes) is not bytes:
            raise LandingManifestError(_ERR_MANIFEST_BYTES)
        if len(manifest_bytes) == 0:
            raise LandingManifestError(_ERR_MANIFEST_BYTES)
        if len(manifest_bytes) > _MAXIMUM_MANIFEST_BYTES:
            raise LandingManifestError(_ERR_MANIFEST_BYTES)

        observed_manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
        if observed_manifest_sha256 != binding.expected_manifest_sha256:
            raise LandingManifestError(_ERR_HASH_MISMATCH)

        try:
            text = manifest_bytes.decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            raise LandingManifestError(_ERR_UTF8) from None

        try:
            raw = json.loads(
                text,
                object_pairs_hook=_unique_object,
                parse_float=_reject_numeric,
                parse_constant=_reject_numeric,
                parse_int=_parse_int,
            )
        except LandingManifestError:
            raise
        except (json.JSONDecodeError, RecursionError):
            raise LandingManifestError(_ERR_JSON) from None

        if type(raw) is not dict or set(raw) != _TOP_LEVEL_KEYS:
            raise LandingManifestError(_ERR_TOP_LEVEL_SHAPE)

        schema_version = raw["schema_version"]
        if type(schema_version) is not int or schema_version != 1:
            raise LandingManifestError(_ERR_SCHEMA_VERSION)

        knowledge_time_raw = raw["knowledge_time"]
        if type(knowledge_time_raw) is not str:
            raise LandingManifestError(_ERR_KNOWLEDGE_TIME)
        try:
            knowledge_time = datetime.fromisoformat(knowledge_time_raw)
        except ValueError:
            raise LandingManifestError(_ERR_KNOWLEDGE_TIME) from None
        if knowledge_time.tzinfo is None or knowledge_time.utcoffset() != timedelta(0):
            raise LandingManifestError(_ERR_KNOWLEDGE_TIME)
        knowledge_time = knowledge_time.astimezone(timezone.utc)
        if knowledge_time < binding.not_before or knowledge_time > binding.cutoff:
            raise LandingManifestError(_ERR_KNOWLEDGE_TIME_BOUNDS)

        target_session_raw = raw["target_session"]
        if type(target_session_raw) is not str or _CANONICAL_DATE.fullmatch(target_session_raw) is None:
            raise LandingManifestError(_ERR_TARGET_SESSION)
        try:
            target_session = date.fromisoformat(target_session_raw)
        except ValueError:
            raise LandingManifestError(_ERR_TARGET_SESSION) from None
        if target_session != binding.target_session:
            raise LandingManifestError(_ERR_TARGET_SESSION_MISMATCH)

        objects_raw = raw["objects"]
        if type(objects_raw) is not list or len(objects_raw) != 2:
            raise LandingManifestError(_ERR_OBJECTS_SHAPE)

        by_file_type: dict[AcquisitionFileType, LandingObjectRequest] = {}
        for entry in objects_raw:
            if type(entry) is not dict or set(entry) != _OBJECT_KEYS:
                raise LandingManifestError(_ERR_OBJECT_SHAPE)

            file_type_raw = entry["file_type"]
            if type(file_type_raw) is not str:
                raise LandingManifestError(_ERR_OBJECT_SHAPE)
            try:
                file_type = AcquisitionFileType(file_type_raw)
            except ValueError:
                raise LandingManifestError(_ERR_OBJECT_SHAPE) from None
            if file_type in by_file_type:
                raise LandingManifestError(_ERR_OBJECTS_SHAPE)

            bucket_raw = entry["bucket"]
            if type(bucket_raw) is not str or bucket_raw != binding.allowed_bucket:
                raise LandingManifestError(_ERR_OBJECT_BUCKET)

            try:
                request = LandingObjectRequest(
                    bucket=bucket_raw,
                    object_name=entry["object_name"],
                    generation=entry["generation"],
                    expected_sha256=entry["sha256"],
                    target_session=target_session,
                    file_type=file_type,
                )
            except AcquisitionError:
                raise LandingManifestError(_ERR_OBJECT_INVALID) from None

            by_file_type[file_type] = request

        if (
            AcquisitionFileType.SECURITY_MASTER not in by_file_type
            or AcquisitionFileType.DAILY_BUNDLE not in by_file_type
        ):
            raise LandingManifestError(_ERR_OBJECTS_INCOMPLETE)

        return VerifiedLandingManifest(
            schema_version=schema_version,
            manifest_sha256=observed_manifest_sha256,
            manifest_bytes=manifest_bytes,
            knowledge_time=knowledge_time,
            target_session=target_session,
            security_master=by_file_type[AcquisitionFileType.SECURITY_MASTER],
            daily_bundle=by_file_type[AcquisitionFileType.DAILY_BUNDLE],
            binding=binding,
        )
