from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone

from .acquisition import AcquisitionError, LandingManifestObjectRequest
from .landing_manifest import LandingManifestError, TrustedLandingManifestBinding


MAXIMUM_PINNED_GCS_RUN_SPEC_BYTES = 32 * 1024
PINNED_GCS_RUN_SPEC_SCHEMA_VERSION = 1

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_CANONICAL_DATE = re.compile(r"\d{4}-\d{2}-\d{2}\Z")
_RFC3339 = re.compile(
    r"\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?(Z|[+-]\d{2}:\d{2})\Z"
)
_MAXIMUM_INTEGER_DIGITS = 20  # generous headroom over the 19-digit int64 max

_TOP_LEVEL_KEYS = frozenset({"schema_version", "manifest_request", "trusted_binding", "run"})
_MANIFEST_REQUEST_KEYS = frozenset({"bucket", "object_name", "generation", "target_session"})
_TRUSTED_BINDING_KEYS = frozenset(
    {"expected_manifest_sha256", "allowed_bucket", "target_session", "not_before", "cutoff"}
)
_RUN_KEYS = frozenset(
    {"market_session", "cutoff", "calendar_materialization_id", "previous_run_id"}
)

_ERR_SPEC_BYTES = "pinned gcs run spec bytes are invalid"
_ERR_UTF8 = "pinned gcs run spec is not valid UTF-8"
_ERR_JSON = "pinned gcs run spec is not valid JSON"
_ERR_DUPLICATE_KEY = "pinned gcs run spec contains a duplicate key"
_ERR_NUMERIC = "pinned gcs run spec contains an unsupported numeric value"
_ERR_TOP_LEVEL_SHAPE = "pinned gcs run spec shape is invalid"
_ERR_SCHEMA_VERSION = "pinned gcs run spec schema version is unsupported"
_ERR_MANIFEST_REQUEST_SHAPE = "pinned gcs run spec manifest request is invalid"
_ERR_TRUSTED_BINDING_SHAPE = "pinned gcs run spec trusted binding is invalid"
_ERR_RUN_SHAPE = "pinned gcs run spec run section is invalid"
_ERR_SESSION = "pinned gcs run spec session is invalid"
_ERR_SESSION_MISMATCH = "pinned gcs run spec sessions do not agree"
_ERR_BUCKET_MISMATCH = "pinned gcs run spec buckets do not agree"
_ERR_TIME = "pinned gcs run spec time is invalid"
_ERR_TIME_SEQUENCE = "pinned gcs run spec time ordering is invalid"
_ERR_CALENDAR_ID = "pinned gcs run spec calendar materialization id is invalid"
_ERR_PREVIOUS_RUN_ID = "pinned gcs run spec previous run id is invalid"


class PinnedGCSRunSpecError(Exception):
    pass


def _unique_object(pairs) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise PinnedGCSRunSpecError(_ERR_DUPLICATE_KEY)
        result[key] = value
    return result


def _reject_numeric(_token: str) -> None:
    raise PinnedGCSRunSpecError(_ERR_NUMERIC)


def _parse_int(token: str) -> int:
    digits = token[1:] if token[:1] == "-" else token
    if len(digits) > _MAXIMUM_INTEGER_DIGITS:
        raise PinnedGCSRunSpecError(_ERR_NUMERIC)
    return int(token)


def _parse_canonical_date(value: object, error_message: str) -> date:
    if type(value) is not str or _CANONICAL_DATE.fullmatch(value) is None:
        raise PinnedGCSRunSpecError(error_message)
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise PinnedGCSRunSpecError(error_message) from None


def _parse_rfc3339_utc(value: object, error_message: str) -> datetime:
    if type(value) is not str or _RFC3339.fullmatch(value) is None:
        raise PinnedGCSRunSpecError(error_message)
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        raise PinnedGCSRunSpecError(error_message) from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise PinnedGCSRunSpecError(error_message)
    return parsed.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class PinnedGCSRunSpec:
    """One immutable, operator-governed pinned-GCS daily-run specification.

    Carries exactly the inputs run_daily_pipeline_from_pinned_gcs_manifest
    needs: schema_version, manifest_request, trusted_binding,
    market_session, cutoff, calendar_materialization_id, and
    previous_run_id. Retains no mapping/list, credential, project ID,
    client setting, local path, raw manifest bytes, or other mutable
    configuration.

    This value cannot prove who authored the underlying JSON document; it
    is operator-governed input, not self-authenticating evidence.
    Authenticity, IAM, and distribution of that document remain an
    operational control outside this module's scope.

    __post_init__ is exactly as strict as parse_pinned_gcs_run_spec: it
    independently requires every field's exact type, reconstructs fresh
    LandingManifestObjectRequest and TrustedLandingManifestBinding
    snapshots from primitive fields (so post-construction mutation, or a
    caller assembling a self-consistent-looking-but-wrong instance,
    cannot bypass validation), cross-checks every retained field and
    temporal relationship, normalizes binding/run datetimes to UTC, and
    replaces both nested values with the freshly reconstructed snapshots.
    """

    schema_version: int
    manifest_request: LandingManifestObjectRequest
    trusted_binding: TrustedLandingManifestBinding
    market_session: date
    cutoff: datetime
    calendar_materialization_id: str
    previous_run_id: str | None

    def __post_init__(self) -> None:
        if (
            type(self.schema_version) is not int
            or self.schema_version != PINNED_GCS_RUN_SPEC_SCHEMA_VERSION
        ):
            raise PinnedGCSRunSpecError(_ERR_SCHEMA_VERSION)

        manifest_request = self.manifest_request
        trusted_binding = self.trusted_binding
        if type(manifest_request) is not LandingManifestObjectRequest:
            raise PinnedGCSRunSpecError(_ERR_MANIFEST_REQUEST_SHAPE)
        if type(trusted_binding) is not TrustedLandingManifestBinding:
            raise PinnedGCSRunSpecError(_ERR_TRUSTED_BINDING_SHAPE)

        if (
            type(manifest_request.bucket) is not str
            or type(manifest_request.object_name) is not str
            or type(manifest_request.generation) is not int
            or type(manifest_request.target_session) is not date
        ):
            raise PinnedGCSRunSpecError(_ERR_MANIFEST_REQUEST_SHAPE)

        if (
            type(trusted_binding.expected_manifest_sha256) is not str
            or type(trusted_binding.allowed_bucket) is not str
            or type(trusted_binding.target_session) is not date
            or type(trusted_binding.not_before) is not datetime
            or type(trusted_binding.cutoff) is not datetime
        ):
            raise PinnedGCSRunSpecError(_ERR_TRUSTED_BINDING_SHAPE)

        try:
            request_snapshot = LandingManifestObjectRequest(
                bucket=manifest_request.bucket,
                object_name=manifest_request.object_name,
                generation=manifest_request.generation,
                target_session=manifest_request.target_session,
            )
        except Exception:
            raise PinnedGCSRunSpecError(_ERR_MANIFEST_REQUEST_SHAPE) from None

        try:
            binding_snapshot = TrustedLandingManifestBinding(
                expected_manifest_sha256=trusted_binding.expected_manifest_sha256,
                allowed_bucket=trusted_binding.allowed_bucket,
                target_session=trusted_binding.target_session,
                not_before=trusted_binding.not_before,
                cutoff=trusted_binding.cutoff,
            )
            binding_not_before_utc = binding_snapshot.not_before.astimezone(timezone.utc)
            binding_cutoff_utc = binding_snapshot.cutoff.astimezone(timezone.utc)
        except Exception:
            raise PinnedGCSRunSpecError(_ERR_TRUSTED_BINDING_SHAPE) from None
        object.__setattr__(binding_snapshot, "not_before", binding_not_before_utc)
        object.__setattr__(binding_snapshot, "cutoff", binding_cutoff_utc)

        if type(self.market_session) is not date:
            raise PinnedGCSRunSpecError(_ERR_SESSION)

        if type(self.cutoff) is not datetime:
            raise PinnedGCSRunSpecError(_ERR_TIME)
        try:
            if self.cutoff.tzinfo is None or self.cutoff.utcoffset() is None:
                raise ValueError("cutoff must be timezone-aware")
            run_cutoff_utc = self.cutoff.astimezone(timezone.utc)
        except Exception:
            raise PinnedGCSRunSpecError(_ERR_TIME) from None

        if (
            type(self.calendar_materialization_id) is not str
            or _SHA256.fullmatch(self.calendar_materialization_id) is None
        ):
            raise PinnedGCSRunSpecError(_ERR_CALENDAR_ID)

        if self.previous_run_id is not None and (
            type(self.previous_run_id) is not str
            or _SHA256.fullmatch(self.previous_run_id) is None
        ):
            raise PinnedGCSRunSpecError(_ERR_PREVIOUS_RUN_ID)

        if request_snapshot.bucket != binding_snapshot.allowed_bucket:
            raise PinnedGCSRunSpecError(_ERR_BUCKET_MISMATCH)
        if (
            request_snapshot.target_session != binding_snapshot.target_session
            or request_snapshot.target_session != self.market_session
        ):
            raise PinnedGCSRunSpecError(_ERR_SESSION_MISMATCH)

        if binding_snapshot.cutoff > run_cutoff_utc:
            raise PinnedGCSRunSpecError(_ERR_TIME_SEQUENCE)

        object.__setattr__(self, "manifest_request", request_snapshot)
        object.__setattr__(self, "trusted_binding", binding_snapshot)
        object.__setattr__(self, "cutoff", run_cutoff_utc)


def parse_pinned_gcs_run_spec(spec_bytes: bytes) -> PinnedGCSRunSpec:
    """Strictly parses one operator-governed pinned-GCS daily-run
    specification from bytes.

    This is a pure parser and value-model boundary only: it never reads a
    file, environment variable, or the current clock, never constructs a
    GCS/storage client, never lists or selects a "latest" object, never
    infers a previous run, and never computes or derives
    expected_manifest_sha256 from manifest content -- that hash must
    already be present in spec_bytes, supplied through an independently
    governed operator/control-plane channel. This function cannot prove
    who authored spec_bytes; treat the result as operator-governed input,
    not self-authenticating evidence.

    Rejects empty or over-sized input before decoding, requires strict
    UTF-8, rejects duplicate JSON keys at every nesting level, rejects
    floats/NaN/Infinity and overlong integer tokens, requires the exact
    key set at all four object levels, parses only the canonical date and
    RFC3339-like datetime forms this contract defines, and delegates
    canonical manifest-request and trusted-binding field validation to
    their existing constructors rather than reimplementing it. Every
    ordinary failure (never BaseException) collapses to one static,
    sanitized PinnedGCSRunSpecError with chaining suppressed; none of them
    expose spec content, bucket/path, generation, hashes, dates, IDs, or
    nested exception text.
    """

    if type(spec_bytes) is not bytes:
        raise PinnedGCSRunSpecError(_ERR_SPEC_BYTES)
    if len(spec_bytes) == 0 or len(spec_bytes) > MAXIMUM_PINNED_GCS_RUN_SPEC_BYTES:
        raise PinnedGCSRunSpecError(_ERR_SPEC_BYTES)

    try:
        text = spec_bytes.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        raise PinnedGCSRunSpecError(_ERR_UTF8) from None

    try:
        raw = json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_float=_reject_numeric,
            parse_constant=_reject_numeric,
            parse_int=_parse_int,
        )
    except PinnedGCSRunSpecError:
        raise
    except (json.JSONDecodeError, RecursionError):
        raise PinnedGCSRunSpecError(_ERR_JSON) from None

    if type(raw) is not dict or set(raw) != _TOP_LEVEL_KEYS:
        raise PinnedGCSRunSpecError(_ERR_TOP_LEVEL_SHAPE)

    schema_version = raw["schema_version"]
    if type(schema_version) is not int or schema_version != PINNED_GCS_RUN_SPEC_SCHEMA_VERSION:
        raise PinnedGCSRunSpecError(_ERR_SCHEMA_VERSION)

    manifest_request_raw = raw["manifest_request"]
    if (
        type(manifest_request_raw) is not dict
        or set(manifest_request_raw) != _MANIFEST_REQUEST_KEYS
    ):
        raise PinnedGCSRunSpecError(_ERR_MANIFEST_REQUEST_SHAPE)

    trusted_binding_raw = raw["trusted_binding"]
    if type(trusted_binding_raw) is not dict or set(trusted_binding_raw) != _TRUSTED_BINDING_KEYS:
        raise PinnedGCSRunSpecError(_ERR_TRUSTED_BINDING_SHAPE)

    run_raw = raw["run"]
    if type(run_raw) is not dict or set(run_raw) != _RUN_KEYS:
        raise PinnedGCSRunSpecError(_ERR_RUN_SHAPE)

    manifest_target_session = _parse_canonical_date(
        manifest_request_raw["target_session"], _ERR_SESSION
    )
    try:
        manifest_request = LandingManifestObjectRequest(
            bucket=manifest_request_raw["bucket"],
            object_name=manifest_request_raw["object_name"],
            generation=manifest_request_raw["generation"],
            target_session=manifest_target_session,
        )
    except AcquisitionError:
        raise PinnedGCSRunSpecError(_ERR_MANIFEST_REQUEST_SHAPE) from None

    trusted_target_session = _parse_canonical_date(
        trusted_binding_raw["target_session"], _ERR_SESSION
    )
    not_before = _parse_rfc3339_utc(trusted_binding_raw["not_before"], _ERR_TIME)
    trusted_cutoff = _parse_rfc3339_utc(trusted_binding_raw["cutoff"], _ERR_TIME)
    try:
        trusted_binding = TrustedLandingManifestBinding(
            expected_manifest_sha256=trusted_binding_raw["expected_manifest_sha256"],
            allowed_bucket=trusted_binding_raw["allowed_bucket"],
            target_session=trusted_target_session,
            not_before=not_before,
            cutoff=trusted_cutoff,
        )
    except LandingManifestError:
        raise PinnedGCSRunSpecError(_ERR_TRUSTED_BINDING_SHAPE) from None

    market_session = _parse_canonical_date(run_raw["market_session"], _ERR_SESSION)
    run_cutoff = _parse_rfc3339_utc(run_raw["cutoff"], _ERR_TIME)

    calendar_materialization_id = run_raw["calendar_materialization_id"]
    if (
        type(calendar_materialization_id) is not str
        or _SHA256.fullmatch(calendar_materialization_id) is None
    ):
        raise PinnedGCSRunSpecError(_ERR_CALENDAR_ID)

    previous_run_id = run_raw["previous_run_id"]
    if previous_run_id is not None and (
        type(previous_run_id) is not str or _SHA256.fullmatch(previous_run_id) is None
    ):
        raise PinnedGCSRunSpecError(_ERR_PREVIOUS_RUN_ID)

    return PinnedGCSRunSpec(
        schema_version=schema_version,
        manifest_request=manifest_request,
        trusted_binding=trusted_binding,
        market_session=market_session,
        cutoff=run_cutoff,
        calendar_materialization_id=calendar_materialization_id,
        previous_run_id=previous_run_id,
    )
