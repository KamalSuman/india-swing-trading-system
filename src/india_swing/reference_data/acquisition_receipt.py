from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

from typing import TYPE_CHECKING

from india_swing.domain.models import INDIA_STANDARD_TIME

from .models import NSE_CM_SECURITY_DATASET

if TYPE_CHECKING:
    from india_swing.daily_pipeline.acquisition import LandingObjectRequest


class ReferenceAcquisitionReceiptError(ValueError):
    pass


def _daily_pipeline_acquisition():
    """Import lazily to avoid a package-init-time circular import.

    india_swing/__init__.py eagerly imports a chain that reaches
    daily_reports -> reference_data.models before reference_data's own
    package body finishes; importing daily_pipeline.acquisition (which
    triggers daily_pipeline/__init__.py, which reaches back into
    daily_reports) at reference_data-module-load time completes that cycle.
    Deferring the import to first actual use breaks it without touching any
    file outside this task's allowed_writes.
    """

    from india_swing.daily_pipeline import acquisition

    return acquisition


def _reference_data_artifact_store():
    """Import lazily, for the same reason as _daily_pipeline_acquisition.

    Resolves the one canonical NSE download-root authority
    (reference_data.artifact_store.NSE_CM_CLAIMED_DOWNLOAD_ROOT) instead of
    duplicating it in this module, without risking reintroducing a
    package-init-time circular import.
    """

    from . import artifact_store

    return artifact_store


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_BUCKET_NAME = re.compile(r"[a-z0-9][a-z0-9\-_.]{1,61}[a-z0-9]\Z")
_CANONICAL_DATE = re.compile(r"\d{4}-\d{2}-\d{2}\Z")
_CANONICAL_ACQUIRED_AT = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\Z")

MAXIMUM_RECEIPT_BYTES = 64 * 1024
MAXIMUM_RAW_BYTES = 32 * 1024 * 1024

_TOP_LEVEL_KEYS = frozenset(
    {
        "schema_version",
        "dataset",
        "authority",
        "acquirer_id",
        "acquired_at",
        "report_date",
        "requested_url",
        "response_status",
        "response_media_type",
        "raw_byte_count",
        "raw_sha256",
        "landing_object",
    }
)
_LANDING_OBJECT_KEYS = frozenset(
    {"file_type", "bucket", "object_name", "generation", "sha256"}
)

_ERR_BINDING = "reference acquisition trust binding is invalid"
_ERR_RECEIPT_BYTES = "reference acquisition receipt bytes are invalid"
_ERR_RECEIPT_HASH = "reference acquisition receipt hash does not match the trusted binding"
_ERR_UTF8 = "reference acquisition receipt is not valid UTF-8"
_ERR_JSON = "reference acquisition receipt is not valid JSON"
_ERR_DUPLICATE_KEY = "reference acquisition receipt contains a duplicate key"
_ERR_NUMERIC = "reference acquisition receipt contains an unsupported numeric value"
_ERR_TOP_LEVEL_SHAPE = "reference acquisition receipt shape is invalid"
_ERR_SCHEMA_VERSION = "reference acquisition receipt schema version is unsupported"
_ERR_SOURCE = "reference acquisition receipt source is invalid"
_ERR_ACQUIRER = "reference acquisition receipt acquirer identity is invalid"
_ERR_REPORT_DATE = "reference acquisition receipt report date is invalid"
_ERR_REPORT_DATE_FUTURE = (
    "reference acquisition receipt report date is not yet known at acquisition time"
)
_ERR_ACQUIRED_AT = "reference acquisition receipt acquisition time is invalid"
_ERR_ACQUIRED_AT_BOUNDS = (
    "reference acquisition receipt acquisition time is outside the trusted bounds"
)
_ERR_URL = "reference acquisition receipt requested URL is invalid"
_ERR_RESPONSE_STATUS = "reference acquisition receipt response status is invalid"
_ERR_RESPONSE_MEDIA_TYPE = "reference acquisition receipt response media type is invalid"
_ERR_RAW_BYTE_COUNT = "reference acquisition receipt raw byte count is invalid"
_ERR_RAW_HASH = "reference acquisition receipt raw hash does not match the trusted binding"
_ERR_LANDING_OBJECT_SHAPE = "reference acquisition receipt landing object is invalid"
_ERR_LANDING_OBJECT_BUCKET = (
    "reference acquisition receipt landing object bucket is not allowed"
)
_ERR_LANDING_OBJECT_HASH = (
    "reference acquisition receipt landing object hash disagrees with the raw hash"
)
_ERR_LANDING_OBJECT_INVALID = (
    "reference acquisition receipt landing object could not be verified"
)
_ERR_CONTENT_IDENTITY = (
    "reference acquisition receipt retained facts disagree with independently "
    "verified receipt content"
)
_ERR_BINDING_IDENTITY = (
    "reference acquisition trust binding no longer matches its construction-time identity"
)

_MAXIMUM_INTEGER_DIGITS = 20  # generous headroom over the 19-digit int64 max


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ReferenceAcquisitionReceiptError(_ERR_DUPLICATE_KEY)
        result[key] = value
    return result


def _reject_numeric(_token: str) -> None:
    raise ReferenceAcquisitionReceiptError(_ERR_NUMERIC)


def _parse_int(token: str) -> int:
    digits = token[1:] if token[:1] == "-" else token
    if len(digits) > _MAXIMUM_INTEGER_DIGITS:
        raise ReferenceAcquisitionReceiptError(_ERR_NUMERIC)
    return int(token)


def _parse_canonical_acquired_at(raw: object) -> datetime:
    """Accept exactly one canonical spelling: UTC RFC3339 whole seconds.

    `YYYY-MM-DDTHH:MM:SSZ` is the only accepted text. `+00:00`, a space
    separator, lowercase `z`, omitted seconds, fractional seconds, and
    nonzero offsets are all rejected even when they represent the same
    instant, so exactly one text spelling maps to a given acquisition
    knowledge time.
    """

    if type(raw) is not str or _CANONICAL_ACQUIRED_AT.fullmatch(raw) is None:
        raise ReferenceAcquisitionReceiptError(_ERR_ACQUIRED_AT)
    try:
        naive = datetime.strptime(raw, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        raise ReferenceAcquisitionReceiptError(_ERR_ACQUIRED_AT) from None
    return naive.replace(tzinfo=timezone.utc)


def _expected_requested_url(report_date: date) -> str:
    artifact_store = _reference_data_artifact_store()
    filename = f"NSE_CM_security_{report_date.strftime('%d%m%Y')}.csv.gz"
    return artifact_store.NSE_CM_CLAIMED_DOWNLOAD_ROOT + filename


@dataclass(frozen=True, slots=True)
class TrustedReferenceAcquisitionBinding:
    """The only source of trust this verifier accepts.

    Every field here must come from an independently governed record, never
    from anything inside the receipt itself. Neither hash, the acquirer ID,
    nor the time bounds are inferred or recomputed from receipt content, an
    environment variable, a clock, GCS, a filename, or a network response.
    """

    expected_receipt_sha256: str
    expected_raw_sha256: str
    allowed_bucket: str
    target_report_date: date
    not_before: datetime
    cutoff: datetime
    trusted_acquirer_id: str

    def __post_init__(self) -> None:
        for value in (self.expected_receipt_sha256, self.expected_raw_sha256):
            if type(value) is not str or _SHA256.fullmatch(value) is None:
                raise ReferenceAcquisitionReceiptError(_ERR_BINDING)
        if (
            type(self.allowed_bucket) is not str
            or _BUCKET_NAME.fullmatch(self.allowed_bucket) is None
        ):
            raise ReferenceAcquisitionReceiptError(_ERR_BINDING)
        if type(self.target_report_date) is not date:
            raise ReferenceAcquisitionReceiptError(_ERR_BINDING)
        for value in (self.not_before, self.cutoff):
            if type(value) is not datetime:
                raise ReferenceAcquisitionReceiptError(_ERR_BINDING)
            if value.tzinfo is None or value.utcoffset() != timedelta(0):
                raise ReferenceAcquisitionReceiptError(_ERR_BINDING)
        if self.not_before > self.cutoff:
            raise ReferenceAcquisitionReceiptError(_ERR_BINDING)
        if (
            type(self.trusted_acquirer_id) is not str
            or _SHA256.fullmatch(self.trusted_acquirer_id) is None
        ):
            raise ReferenceAcquisitionReceiptError(_ERR_BINDING)


def _binding_identity(binding: TrustedReferenceAcquisitionBinding) -> tuple:
    """The exact type and value of all seven trusted binding fields.

    Used to bind a VerifiedReferenceAcquisitionReceipt to its
    construction-time trust boundary: a still-individually-valid
    post-construction change to any binding field (for example widening
    not_before or cutoff while keeping both aware-UTC and ordered) must be
    detected as a redefinition of trust, not merely re-validated as if it
    were the original binding.
    """

    if type(binding) is not TrustedReferenceAcquisitionBinding:
        raise ReferenceAcquisitionReceiptError(_ERR_BINDING)
    return (
        type(binding.expected_receipt_sha256),
        binding.expected_receipt_sha256,
        type(binding.expected_raw_sha256),
        binding.expected_raw_sha256,
        type(binding.allowed_bucket),
        binding.allowed_bucket,
        type(binding.target_report_date),
        binding.target_report_date,
        type(binding.not_before),
        binding.not_before,
        type(binding.cutoff),
        binding.cutoff,
        type(binding.trusted_acquirer_id),
        binding.trusted_acquirer_id,
    )


@dataclass(frozen=True, slots=True)
class _ParsedReceiptFacts:
    """Plain normalized receipt facts returned by the single strict decoder.

    Never a VerifiedReferenceAcquisitionReceipt: this is the one receipt
    decoder that both ReferenceAcquisitionReceiptVerifier.verify and
    VerifiedReferenceAcquisitionReceipt's own defensive checks call, so
    defensive validation always compares against these plain values rather
    than trusting a caller-assembled typed instance.
    """

    schema_version: int
    receipt_bytes: bytes
    receipt_sha256: str
    dataset: str
    authority: str
    acquirer_id: str
    acquired_at: datetime
    report_date: date
    requested_url: str
    response_status: int
    response_media_type: str
    raw_byte_count: int
    raw_sha256: str
    landing_object: LandingObjectRequest


def _parse_receipt(
    receipt_bytes: bytes, binding: TrustedReferenceAcquisitionBinding
) -> _ParsedReceiptFacts:
    """The single strict receipt decoder.

    Accepts exact receipt bytes plus an exact TrustedReferenceAcquisitionBinding,
    performs the complete strict parse/validation pipeline, and returns plain
    normalized facts plus the exact LandingObjectRequest. Performs no
    network, GCS, filesystem, environment, clock, listing, or
    latest-selection access, and never constructs
    VerifiedReferenceAcquisitionReceipt.
    """

    if type(binding) is not TrustedReferenceAcquisitionBinding:
        raise ReferenceAcquisitionReceiptError(_ERR_BINDING)
    if type(receipt_bytes) is not bytes:
        raise ReferenceAcquisitionReceiptError(_ERR_RECEIPT_BYTES)
    if len(receipt_bytes) == 0:
        raise ReferenceAcquisitionReceiptError(_ERR_RECEIPT_BYTES)
    if len(receipt_bytes) > MAXIMUM_RECEIPT_BYTES:
        raise ReferenceAcquisitionReceiptError(_ERR_RECEIPT_BYTES)

    observed_receipt_sha256 = hashlib.sha256(receipt_bytes).hexdigest()
    if observed_receipt_sha256 != binding.expected_receipt_sha256:
        raise ReferenceAcquisitionReceiptError(_ERR_RECEIPT_HASH)

    try:
        text = receipt_bytes.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        raise ReferenceAcquisitionReceiptError(_ERR_UTF8) from None

    try:
        raw = json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_float=_reject_numeric,
            parse_constant=_reject_numeric,
            parse_int=_parse_int,
        )
    except ReferenceAcquisitionReceiptError:
        raise
    except (json.JSONDecodeError, RecursionError):
        raise ReferenceAcquisitionReceiptError(_ERR_JSON) from None

    if type(raw) is not dict or set(raw) != _TOP_LEVEL_KEYS:
        raise ReferenceAcquisitionReceiptError(_ERR_TOP_LEVEL_SHAPE)

    schema_version = raw["schema_version"]
    if type(schema_version) is not int or schema_version != 1:
        raise ReferenceAcquisitionReceiptError(_ERR_SCHEMA_VERSION)

    dataset = raw["dataset"]
    if type(dataset) is not str or dataset != NSE_CM_SECURITY_DATASET:
        raise ReferenceAcquisitionReceiptError(_ERR_SOURCE)

    authority = raw["authority"]
    if type(authority) is not str or authority != "NSE":
        raise ReferenceAcquisitionReceiptError(_ERR_SOURCE)

    acquirer_id = raw["acquirer_id"]
    if type(acquirer_id) is not str or _SHA256.fullmatch(acquirer_id) is None:
        raise ReferenceAcquisitionReceiptError(_ERR_ACQUIRER)
    if acquirer_id != binding.trusted_acquirer_id:
        raise ReferenceAcquisitionReceiptError(_ERR_ACQUIRER)

    report_date_raw = raw["report_date"]
    if (
        type(report_date_raw) is not str
        or _CANONICAL_DATE.fullmatch(report_date_raw) is None
    ):
        raise ReferenceAcquisitionReceiptError(_ERR_REPORT_DATE)
    try:
        report_date = date.fromisoformat(report_date_raw)
    except ValueError:
        raise ReferenceAcquisitionReceiptError(_ERR_REPORT_DATE) from None
    if report_date != binding.target_report_date:
        raise ReferenceAcquisitionReceiptError(_ERR_REPORT_DATE)

    acquired_at = _parse_canonical_acquired_at(raw["acquired_at"])
    if acquired_at < binding.not_before or acquired_at > binding.cutoff:
        raise ReferenceAcquisitionReceiptError(_ERR_ACQUIRED_AT_BOUNDS)
    if report_date > acquired_at.astimezone(INDIA_STANDARD_TIME).date():
        raise ReferenceAcquisitionReceiptError(_ERR_REPORT_DATE_FUTURE)

    requested_url_raw = raw["requested_url"]
    expected_url = _expected_requested_url(report_date)
    if type(requested_url_raw) is not str or requested_url_raw != expected_url:
        raise ReferenceAcquisitionReceiptError(_ERR_URL)

    response_status = raw["response_status"]
    if type(response_status) is not int or response_status != 200:
        raise ReferenceAcquisitionReceiptError(_ERR_RESPONSE_STATUS)

    response_media_type = raw["response_media_type"]
    if (
        type(response_media_type) is not str
        or response_media_type != "application/gzip"
    ):
        raise ReferenceAcquisitionReceiptError(_ERR_RESPONSE_MEDIA_TYPE)

    raw_byte_count = raw["raw_byte_count"]
    if (
        type(raw_byte_count) is not int
        or not 1 <= raw_byte_count <= MAXIMUM_RAW_BYTES
    ):
        raise ReferenceAcquisitionReceiptError(_ERR_RAW_BYTE_COUNT)

    raw_sha256 = raw["raw_sha256"]
    if type(raw_sha256) is not str or _SHA256.fullmatch(raw_sha256) is None:
        raise ReferenceAcquisitionReceiptError(_ERR_RAW_HASH)
    if raw_sha256 != binding.expected_raw_sha256:
        raise ReferenceAcquisitionReceiptError(_ERR_RAW_HASH)

    landing_object_raw = raw["landing_object"]
    if (
        type(landing_object_raw) is not dict
        or set(landing_object_raw) != _LANDING_OBJECT_KEYS
    ):
        raise ReferenceAcquisitionReceiptError(_ERR_LANDING_OBJECT_SHAPE)

    acquisition = _daily_pipeline_acquisition()
    file_type_raw = landing_object_raw["file_type"]
    if type(file_type_raw) is not str:
        raise ReferenceAcquisitionReceiptError(_ERR_LANDING_OBJECT_SHAPE)
    try:
        file_type = acquisition.AcquisitionFileType(file_type_raw)
    except ValueError:
        raise ReferenceAcquisitionReceiptError(_ERR_LANDING_OBJECT_SHAPE) from None
    if file_type is not acquisition.AcquisitionFileType.SECURITY_MASTER:
        raise ReferenceAcquisitionReceiptError(_ERR_LANDING_OBJECT_SHAPE)

    bucket_raw = landing_object_raw["bucket"]
    if type(bucket_raw) is not str or bucket_raw != binding.allowed_bucket:
        raise ReferenceAcquisitionReceiptError(_ERR_LANDING_OBJECT_BUCKET)

    landing_sha256 = landing_object_raw["sha256"]
    if (
        type(landing_sha256) is not str
        or landing_sha256 != raw_sha256
        or landing_sha256 != binding.expected_raw_sha256
    ):
        raise ReferenceAcquisitionReceiptError(_ERR_LANDING_OBJECT_HASH)

    try:
        landing_object = acquisition.LandingObjectRequest(
            bucket=bucket_raw,
            object_name=landing_object_raw["object_name"],
            generation=landing_object_raw["generation"],
            expected_sha256=landing_sha256,
            target_session=report_date,
            file_type=file_type,
        )
    except acquisition.AcquisitionError:
        raise ReferenceAcquisitionReceiptError(_ERR_LANDING_OBJECT_INVALID) from None

    return _ParsedReceiptFacts(
        schema_version=schema_version,
        receipt_bytes=receipt_bytes,
        receipt_sha256=observed_receipt_sha256,
        dataset=dataset,
        authority=authority,
        acquirer_id=acquirer_id,
        acquired_at=acquired_at,
        report_date=report_date,
        requested_url=requested_url_raw,
        response_status=response_status,
        response_media_type=response_media_type,
        raw_byte_count=raw_byte_count,
        raw_sha256=raw_sha256,
        landing_object=landing_object,
    )


def _require_identical_facts(
    retained: "VerifiedReferenceAcquisitionReceipt", parsed: _ParsedReceiptFacts
) -> None:
    pairs = (
        (retained.schema_version, parsed.schema_version),
        (retained.receipt_bytes, parsed.receipt_bytes),
        (retained.receipt_sha256, parsed.receipt_sha256),
        (retained.dataset, parsed.dataset),
        (retained.authority, parsed.authority),
        (retained.acquirer_id, parsed.acquirer_id),
        (retained.acquired_at, parsed.acquired_at),
        (retained.report_date, parsed.report_date),
        (retained.requested_url, parsed.requested_url),
        (retained.response_status, parsed.response_status),
        (retained.response_media_type, parsed.response_media_type),
        (retained.raw_byte_count, parsed.raw_byte_count),
        (retained.raw_sha256, parsed.raw_sha256),
        (retained.landing_object, parsed.landing_object),
    )
    for retained_value, parsed_value in pairs:
        if type(retained_value) is not type(parsed_value) or retained_value != parsed_value:
            raise ReferenceAcquisitionReceiptError(_ERR_CONTENT_IDENTITY)


@dataclass(frozen=True, slots=True)
class VerifiedReferenceAcquisitionReceipt:
    """One acquisition receipt whose bytes matched an externally trusted hash.

    Requires and retains the exact TrustedReferenceAcquisitionBinding it was
    verified against; there is no default, so this cannot be constructed
    without one. __post_init__ independently replays the single strict
    receipt decoder (_parse_receipt) on self.receipt_bytes and self.binding
    and requires exact type-and-value agreement with every retained field,
    so a caller cannot bypass verification by hand-assembling an instance
    whose typed fields disagree with its own receipt bytes, and
    verify_content_identity() lets any later consumer replay that same check
    to detect post-construction mutation.
    """

    schema_version: int
    receipt_bytes: bytes
    receipt_sha256: str
    dataset: str
    authority: str
    acquirer_id: str
    acquired_at: datetime
    report_date: date
    requested_url: str
    response_status: int
    response_media_type: str
    raw_byte_count: int
    raw_sha256: str
    landing_object: LandingObjectRequest
    binding: TrustedReferenceAcquisitionBinding
    _construction_time_binding_identity: tuple = field(
        init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        # Captures the exact type and value of every trusted binding field at
        # construction time, before any defensive check runs, so that a later
        # still-individually-valid mutation of the binding (for example
        # widening not_before or cutoff) is detected as a redefinition of
        # trust rather than silently re-validated as if it were original.
        object.__setattr__(
            self, "_construction_time_binding_identity", _binding_identity(self.binding)
        )
        self.verify_content_identity()

    def verify_content_identity(self) -> None:
        """Replay the strict receipt decoder and require exact agreement.

        First requires the current binding to match the exact type and value
        of every field captured at construction time, so a still-valid
        post-construction change to the trust boundary itself (not just an
        invalid one) fails closed. Then independently re-derives every fact
        from self.receipt_bytes and self.binding via the same private
        _parse_receipt routine ReferenceAcquisitionReceiptVerifier.verify
        uses, and requires exact type-and-value equality against every
        retained field, including receipt_sha256 and the complete
        LandingObjectRequest. Detects post-construction object.__setattr__
        mutation of any top-level field, nested binding field, or nested
        LandingObjectRequest field.
        """

        if _binding_identity(self.binding) != self._construction_time_binding_identity:
            raise ReferenceAcquisitionReceiptError(_ERR_BINDING_IDENTITY)
        parsed = _parse_receipt(self.receipt_bytes, self.binding)
        _require_identical_facts(self, parsed)


class ReferenceAcquisitionReceiptVerifier:
    """Verifies one acquisition receipt against an externally trusted binding.

    Performs no network, GCS, filesystem, environment, clock, listing, or
    latest-selection access. Every failure is one static, sanitized
    ReferenceAcquisitionReceiptError; no receipt content, URL, bucket, object
    name, hash, acquirer ID, nested exception type/text, path, or
    credential-like text is ever included in a raised message.
    """

    def verify(
        self,
        receipt_bytes: bytes,
        binding: TrustedReferenceAcquisitionBinding,
    ) -> VerifiedReferenceAcquisitionReceipt:
        parsed = _parse_receipt(receipt_bytes, binding)
        return VerifiedReferenceAcquisitionReceipt(
            schema_version=parsed.schema_version,
            receipt_bytes=parsed.receipt_bytes,
            receipt_sha256=parsed.receipt_sha256,
            dataset=parsed.dataset,
            authority=parsed.authority,
            acquirer_id=parsed.acquirer_id,
            acquired_at=parsed.acquired_at,
            report_date=parsed.report_date,
            requested_url=parsed.requested_url,
            response_status=parsed.response_status,
            response_media_type=parsed.response_media_type,
            raw_byte_count=parsed.raw_byte_count,
            raw_sha256=parsed.raw_sha256,
            landing_object=parsed.landing_object,
            binding=binding,
        )
