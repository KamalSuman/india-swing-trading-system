from __future__ import annotations

import hashlib
from dataclasses import dataclass

from india_swing.calendar_data.materialization import CollectionCalendarMaterialization
from india_swing.calendar_data.materialization_codec import (
    MAXIMUM_CALENDAR_MATERIALIZATION_BYTES,
    decode_calendar_materialization,
    encode_calendar_materialization,
)
from india_swing.reference.models import ReferenceReadiness

from .acquisition import GCSObjectPayload, GCSObjectReader


_MAXIMUM_GENERATION = 9223372036854775807  # 2**63 - 1, positive signed 64-bit max
_HEX_DIGITS = frozenset("0123456789abcdef")
_BUCKET_ALNUM = frozenset("abcdefghijklmnopqrstuvwxyz0123456789")
_BUCKET_MIDDLE = frozenset("abcdefghijklmnopqrstuvwxyz0123456789-_.")
_CALENDAR_MATERIALIZATION_OBJECT_FILENAME = "materialization.json"

_ERR = (
    "calendar materialization acquisition failed generation-pinned "
    "read or verification"
)


class CalendarMaterializationAcquisitionError(ValueError):
    pass


def _is_lowercase_hex64(value: object) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and all(character in _HEX_DIGITS for character in value)
    )


def _is_valid_bucket_name(value: object) -> bool:
    if type(value) is not str or len(value) < 3 or len(value) > 63:
        return False
    if value[0] not in _BUCKET_ALNUM or value[-1] not in _BUCKET_ALNUM:
        return False
    return all(character in _BUCKET_MIDDLE for character in value[1:-1])


def _expected_object_name(materialization_id: str) -> str:
    return f"calendar-materializations/{materialization_id}/{_CALENDAR_MATERIALIZATION_OBJECT_FILENAME}"


@dataclass(frozen=True, slots=True)
class CalendarMaterializationObjectRequest:
    """One explicit, generation-pinned GCS calendar-materialization object
    to read.

    Every field is required and independently verified. There is no
    default, no bucket listing, and no "latest object" resolution anywhere
    in this module: object_name must equal the exact canonical path this
    request's own materialization_id derives, with no alternate prefix,
    traversal, normalization, URL, or local path accepted.
    """

    bucket: str
    object_name: str
    generation: int
    expected_sha256: str
    materialization_id: str

    def __post_init__(self) -> None:
        if not _is_valid_bucket_name(self.bucket):
            raise CalendarMaterializationAcquisitionError(_ERR)
        if not _is_lowercase_hex64(self.materialization_id):
            raise CalendarMaterializationAcquisitionError(_ERR)
        if not _is_lowercase_hex64(self.expected_sha256):
            raise CalendarMaterializationAcquisitionError(_ERR)
        if (
            type(self.generation) is bool
            or type(self.generation) is not int
            or self.generation <= 0
            or self.generation > _MAXIMUM_GENERATION
        ):
            raise CalendarMaterializationAcquisitionError(_ERR)
        expected_object_name = _expected_object_name(self.materialization_id)
        if type(self.object_name) is not str or self.object_name != expected_object_name:
            raise CalendarMaterializationAcquisitionError(_ERR)


@dataclass(frozen=True, slots=True)
class AcquiredCalendarMaterialization:
    """One verified, generation-pinned calendar materialization read from
    GCS.

    request is a defensively reconstructed snapshot, never the caller's
    original object. observed_generation/observed_sha256 are the values
    actually returned by the reader for this read, independently
    re-verified against the request rather than trusted at face value.
    Neither observed_sha256 nor materialization.materialization_id proves
    external authorship by itself; both remain internal integrity checks
    only.
    """

    request: CalendarMaterializationObjectRequest
    observed_generation: int
    observed_sha256: str
    materialization: CollectionCalendarMaterialization

    def __post_init__(self) -> None:
        # Every ordinary failure below -- including a CalendarMaterialization
        # AcquisitionError raised by a nested call (e.g. an untrusted
        # reader/decoder injecting that exact class with attacker-controlled
        # text) -- is collected into `ordinary_failure` and re-raised as a
        # single fresh, static error only after this try/except has fully
        # exited. There is no same-class bare-reraise privilege: a
        # CalendarMaterializationAcquisitionError caught here is discarded
        # exactly like any other Exception, so injected content can never
        # reach the caller unchanged. Raising a *new* exception from inside
        # an except clause still attaches the caught exception as
        # __context__ even with `from None` (which only clears __cause__
        # and suppresses display); raising after the block exits leaves no
        # currently-handled exception, so __context__ is genuinely None.
        ordinary_failure = False
        request_snapshot: CalendarMaterializationObjectRequest | None = None
        try:
            if type(self.request) is not CalendarMaterializationObjectRequest:
                raise CalendarMaterializationAcquisitionError(_ERR)
            request_snapshot = CalendarMaterializationObjectRequest(
                bucket=self.request.bucket,
                object_name=self.request.object_name,
                generation=self.request.generation,
                expected_sha256=self.request.expected_sha256,
                materialization_id=self.request.materialization_id,
            )

            if (
                type(self.observed_generation) is bool
                or type(self.observed_generation) is not int
                or self.observed_generation != request_snapshot.generation
            ):
                raise CalendarMaterializationAcquisitionError(_ERR)
            if not _is_lowercase_hex64(self.observed_sha256):
                raise CalendarMaterializationAcquisitionError(_ERR)

            if type(self.materialization) is not CollectionCalendarMaterialization:
                raise CalendarMaterializationAcquisitionError(_ERR)
            self.materialization.verify_content_identity()
            if self.materialization.materialization_id != request_snapshot.materialization_id:
                raise CalendarMaterializationAcquisitionError(_ERR)
            if self.materialization.readiness is not ReferenceReadiness.COLLECTION_ONLY:
                raise CalendarMaterializationAcquisitionError(_ERR)
            if (
                type(self.materialization.actionable) is not bool
                or self.materialization.actionable
            ):
                raise CalendarMaterializationAcquisitionError(_ERR)

            # Bind the hash pair to the exact canonical stored bytes this
            # materialization re-encodes to, rather than trusting a
            # caller-supplied observed_sha256/expected_sha256 pair that
            # merely agree with each other but not with the materialization
            # actually being wrapped.
            canonical_bytes = encode_calendar_materialization(self.materialization)
            canonical_sha256 = hashlib.sha256(canonical_bytes).hexdigest()
            if (
                canonical_sha256 != self.observed_sha256
                or canonical_sha256 != request_snapshot.expected_sha256
            ):
                raise CalendarMaterializationAcquisitionError(_ERR)
        except Exception:
            ordinary_failure = True

        if ordinary_failure:
            raise CalendarMaterializationAcquisitionError(_ERR)

        object.__setattr__(self, "request", request_snapshot)


def acquire_calendar_materialization(
    request: CalendarMaterializationObjectRequest,
    *,
    reader: GCSObjectReader,
) -> AcquiredCalendarMaterialization:
    """Reads and verifies exactly one generation-pinned calendar
    materialization object from GCS.

    This is a pure acquisition boundary only: it never constructs a
    storage client, never lists a bucket, never selects a "latest" object,
    and calls reader.read_generation exactly once, with the request's own
    bucket/object_name/generation and maximum_bytes=
    MAXIMUM_CALENDAR_MATERIALIZATION_BYTES. The returned payload's
    generation and content are independently re-verified rather than
    trusted; SHA-256 is computed on the exact returned stored bytes and
    checked against request.expected_sha256 before decode_calendar_
    materialization is ever called, so hash verification always precedes
    decoding. The decoded value's own content identity and
    materialization_id are then explicitly re-checked against the
    request. None of this proves external provenance -- decoded bytes
    remain exactly as collection-only and non-actionable as
    decode_calendar_materialization's own constructors require.

    Every ordinary failure (never BaseException), including a reader,
    hashing, or decoder failure, collapses to one static, sanitized
    CalendarMaterializationAcquisitionError with chaining suppressed. A
    failure never triggers a second read. The message never includes the
    bucket, object path, generation, hashes, IDs, dates, raw bytes, or any
    nested exception's type or text; neither is it retained via __cause__
    or __context__, since the fresh error is only ever raised after this
    function's own try/except has fully exited -- see
    AcquiredCalendarMaterialization.__post_init__ for why `from None`
    alone is not sufficient. There is likewise no same-class bare-reraise
    privilege for CalendarMaterializationAcquisitionError: an untrusted
    reader (or a decoder call) that raises that exact class with
    attacker-controlled text is discarded exactly like any other
    Exception, so it can never escape to the caller unchanged.
    """

    ordinary_failure = False
    acquired: AcquiredCalendarMaterialization | None = None
    try:
        if type(request) is not CalendarMaterializationObjectRequest:
            raise CalendarMaterializationAcquisitionError(_ERR)
        fresh_request = CalendarMaterializationObjectRequest(
            bucket=request.bucket,
            object_name=request.object_name,
            generation=request.generation,
            expected_sha256=request.expected_sha256,
            materialization_id=request.materialization_id,
        )

        payload = reader.read_generation(
            bucket=fresh_request.bucket,
            object_name=fresh_request.object_name,
            generation=fresh_request.generation,
            maximum_bytes=MAXIMUM_CALENDAR_MATERIALIZATION_BYTES,
        )
        if type(payload) is not GCSObjectPayload:
            raise CalendarMaterializationAcquisitionError(_ERR)
        if (
            type(payload.generation) is bool
            or type(payload.generation) is not int
            or payload.generation != fresh_request.generation
        ):
            raise CalendarMaterializationAcquisitionError(_ERR)

        content_bytes = payload.content_bytes
        if (
            type(content_bytes) is not bytes
            or len(content_bytes) == 0
            or len(content_bytes) > MAXIMUM_CALENDAR_MATERIALIZATION_BYTES
        ):
            raise CalendarMaterializationAcquisitionError(_ERR)

        observed_sha256 = hashlib.sha256(content_bytes).hexdigest()
        if observed_sha256 != fresh_request.expected_sha256:
            raise CalendarMaterializationAcquisitionError(_ERR)

        materialization = decode_calendar_materialization(content_bytes)
        if type(materialization) is not CollectionCalendarMaterialization:
            raise CalendarMaterializationAcquisitionError(_ERR)
        materialization.verify_content_identity()
        if materialization.materialization_id != fresh_request.materialization_id:
            raise CalendarMaterializationAcquisitionError(_ERR)

        acquired = AcquiredCalendarMaterialization(
            request=fresh_request,
            observed_generation=payload.generation,
            observed_sha256=observed_sha256,
            materialization=materialization,
        )
    except Exception:
        ordinary_failure = True

    if ordinary_failure:
        raise CalendarMaterializationAcquisitionError(_ERR)
    return acquired
