"""HYP-002 quality pilot: canonical_response_v1 trust boundary.

Models, canonically encodes, and replay-verifies one scheduled provider
request/response observation for the founder-authorized HYP-002 20-confirmed
-session quality-only Kite capture pilot -- catalog, full quote, or daily
OHLCV. Every artifact this module produces is permanently ineligible for
O0, research partitions, features, labels, signals, paper trades,
notifications, execution, or capital: this is a foundation-only trust
boundary, not a data collector, scheduler, or strategy input.

This module performs no filesystem, environment, clock, network, GCP, Kite
SDK, broker, order, signal, feature, label, return, paper-trade,
notification, or deployment capability. It never lists, discovers, selects
a "latest" artifact, or persists anything -- every model here is a pure,
deterministic, in-memory value built and verified entirely from
caller-supplied data.

Canonical wire format
----------------------
``canonical_response_v1`` is a strict, hand-rolled JSON codec, distinct
from :func:`india_swing.identity.content_id` (which is an internal-only,
type-tagged content-identity hash, not a clean wire format). Every encoded
object is UTF-8 JSON with lexicographically sorted keys and no
insignificant whitespace. ``Decimal`` values are canonical non-exponent
strings with no signed zero; timestamps are RFC3339 UTC with a literal
``Z`` and exactly six fractional digits; dates are ``YYYY-MM-DD``. The
strict decoder rejects duplicate object keys at every nesting level, JSON
floats/``NaN``/``Infinity``, noncanonical spellings, and any structure
outside the fixed schema.

``canonical_response_sha256`` is SHA-256 over the UTF-8 bytes of
``canonical_response_v1|request_identity_json|canonical_payload_json``.
``normalized_content_sha256`` is SHA-256 over
``canonical_response_v1|window_json|request_identity_json|canonical_payload_json``
-- the same canonical trees, in the same deterministic field/array order,
additionally binding the observation window. ``observation_id`` binds the
schema/policy version, the pinned protocol hash, both nested identities,
both hashes, response classification, correction lineage, record counts,
and the fixed permanent quality-only posture.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from enum import Enum
from hashlib import sha256
from typing import Union

from india_swing.market_data.models import (
    DailyCandle,
    DailyCandleBatch,
    FullQuoteBatch,
    InstrumentBatch,
    KiteDepthLevel,
    KiteFullQuote,
    KiteInstrument,
    LISTING_KEY_PATTERN,
    MAXIMUM_QUOTE_KEYS,
    NseSessionFinality,
    require_canonical_listing_keys,
)


# ---------------------------------------------------------------------------
# Pinned constants
# ---------------------------------------------------------------------------

PILOT_PROTOCOL_SHA256 = "b29e34fdc3f62134034fbf032cf67d39ce06acd5082f9d1de680fcac2260f8e5"
CANONICAL_RESPONSE_SCHEMA_VERSION = "canonical_response_v1"
NORMALIZED_CONTENT_SCHEMA_VERSION = "normalized_content_v1"
QUALITY_PILOT_POLICY_VERSION = "hyp002-quality-pilot/positive-only-v1"

MAXIMUM_CATALOG_INSTRUMENTS = 20000
# One observation represents exactly one provider request/chunk, so this is
# pinned to the real per-request Kite quote endpoint ceiling -- never the
# larger MAXIMUM_AGGREGATED_QUOTE_KEYS ceiling FullQuoteBatch itself allows
# for a caller-assembled multi-request aggregate.
MAXIMUM_QUOTE_REQUEST_KEYS = MAXIMUM_QUOTE_KEYS
MAXIMUM_ENCODED_BYTES = 32 * 1024 * 1024
MAXIMUM_TEXT_FIELD_LENGTH = 128
MAXIMUM_CHUNK_COUNT = 10000

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_CANONICAL_DECIMAL_PATTERN = re.compile(r"-?(0|[1-9]\d*)(\.\d+)?\Z")
_CANONICAL_DATETIME_PATTERN = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z\Z"
)
_CANONICAL_DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}\Z")
_IST = timezone(timedelta(hours=5, minutes=30))


def _is_sha256(value: object) -> bool:
    return type(value) is str and _SHA256_PATTERN.fullmatch(value) is not None


def _is_bounded_text(value: object, *, maximum_length: int = MAXIMUM_TEXT_FIELD_LENGTH) -> bool:
    return (
        type(value) is str
        and 0 < len(value) <= maximum_length
        and value == value.strip()
        and all(ord(character) >= 32 for character in value)
    )


class QualityPilotCanonicalResponseError(ValueError):
    """A quality-pilot canonical-response input, capability, or artifact failed a static safety rule."""


def _fail(message: str) -> None:
    raise QualityPilotCanonicalResponseError(message)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class EndpointFamily(Enum):
    CATALOG = "CATALOG"
    FULL_QUOTE = "FULL_QUOTE"
    DAILY_OHLCV = "DAILY_OHLCV"


class ScheduledWindowKind(Enum):
    CATALOG_PREOPEN = "CATALOG_PREOPEN"
    QUOTE_0920 = "QUOTE_0920"
    QUOTE_CLOSE = "QUOTE_CLOSE"
    OHLCV_CLOSE = "OHLCV_CLOSE"


class ResponseClassification(Enum):
    SUCCESS = "SUCCESS"
    CATALOG_GAP = "CATALOG_GAP"
    PROVIDER_GAP = "PROVIDER_GAP"
    CANONICALIZATION_FAILURE = "CANONICALIZATION_FAILURE"
    REQUEST_REJECTED = "REQUEST_REJECTED"
    RATE_LIMITED = "RATE_LIMITED"
    PARTIAL_KEY_COVERAGE = "PARTIAL_KEY_COVERAGE"
    TIMESTAMP_VIOLATION = "TIMESTAMP_VIOLATION"


_SUCCESS_CLASSIFICATIONS = frozenset({ResponseClassification.SUCCESS})

# Pinned response-classification/endpoint compatibility table. An absence
# must be immutable AND semantically correctly classified: an incompatible
# pair (e.g. a FULL_QUOTE request recorded as CATALOG_GAP) is malformed,
# never silently accepted or remapped.
_UNIVERSAL_CLASSIFICATIONS = frozenset(
    {
        ResponseClassification.SUCCESS,
        ResponseClassification.CANONICALIZATION_FAILURE,
        ResponseClassification.REQUEST_REJECTED,
        ResponseClassification.RATE_LIMITED,
        ResponseClassification.TIMESTAMP_VIOLATION,
    }
)
_CLASSIFICATION_COMPATIBILITY = {
    EndpointFamily.CATALOG: _UNIVERSAL_CLASSIFICATIONS
    | frozenset({ResponseClassification.CATALOG_GAP}),
    EndpointFamily.FULL_QUOTE: _UNIVERSAL_CLASSIFICATIONS
    | frozenset(
        {ResponseClassification.PROVIDER_GAP, ResponseClassification.PARTIAL_KEY_COVERAGE}
    ),
    EndpointFamily.DAILY_OHLCV: _UNIVERSAL_CLASSIFICATIONS
    | frozenset({ResponseClassification.PROVIDER_GAP}),
}

_WINDOW_KIND_ENDPOINT_FAMILY = {
    ScheduledWindowKind.CATALOG_PREOPEN: EndpointFamily.CATALOG,
    ScheduledWindowKind.QUOTE_0920: EndpointFamily.FULL_QUOTE,
    ScheduledWindowKind.QUOTE_CLOSE: EndpointFamily.FULL_QUOTE,
    ScheduledWindowKind.OHLCV_CLOSE: EndpointFamily.DAILY_OHLCV,
}


# ---------------------------------------------------------------------------
# Canonical scalar codecs
# ---------------------------------------------------------------------------


def _decimal_to_canonical_str(value: Decimal, field_name: str) -> str:
    if type(value) is not Decimal or not value.is_finite():
        _fail(f"{field_name} must be a finite Decimal")
    text = format(value, "f")
    if text.startswith("-") and Decimal(text) == 0:
        text = text[1:]
    return text


def _parse_canonical_decimal(text: object, field_name: str) -> Decimal:
    if type(text) is not str or _CANONICAL_DECIMAL_PATTERN.fullmatch(text) is None:
        _fail(f"{field_name} is not a canonical decimal string")
    if text.startswith("-") and Decimal(text) == 0:
        _fail(f"{field_name} is a signed zero")
    value = Decimal(text)
    if _decimal_to_canonical_str(value, field_name) != text:
        _fail(f"{field_name} is not a canonical decimal spelling")
    return value


def _datetime_to_canonical_str(value: datetime, field_name: str) -> str:
    if type(value) is not datetime or value.tzinfo is None or value.utcoffset() is None:
        _fail(f"{field_name} must be a timezone-aware datetime")
    normalized = value.astimezone(timezone.utc)
    return (
        f"{normalized.year:04d}-{normalized.month:02d}-{normalized.day:02d}T"
        f"{normalized.hour:02d}:{normalized.minute:02d}:{normalized.second:02d}."
        f"{normalized.microsecond:06d}Z"
    )


def _parse_canonical_datetime(text: object, field_name: str) -> datetime:
    if type(text) is not str or _CANONICAL_DATETIME_PATTERN.fullmatch(text) is None:
        _fail(f"{field_name} is not a canonical RFC3339 UTC timestamp")
    parse_failed = False
    parsed = None
    try:
        parsed = datetime.strptime(text[:-1], "%Y-%m-%dT%H:%M:%S.%f")
    except ValueError:
        parse_failed = True
    if parse_failed:
        _fail(f"{field_name} is not a valid timestamp")
    value = parsed.replace(tzinfo=timezone.utc)
    if _datetime_to_canonical_str(value, field_name) != text:
        _fail(f"{field_name} is not a canonical timestamp spelling")
    return value


def _date_to_canonical_str(value: date, field_name: str) -> str:
    if type(value) is not date:
        _fail(f"{field_name} must be an exact date")
    return value.isoformat()


def _parse_canonical_date(text: object, field_name: str) -> date:
    if type(text) is not str or _CANONICAL_DATE_PATTERN.fullmatch(text) is None:
        _fail(f"{field_name} is not a canonical YYYY-MM-DD date")
    parse_failed = False
    parsed = None
    try:
        parsed = date.fromisoformat(text)
    except ValueError:
        parse_failed = True
    if parse_failed:
        _fail(f"{field_name} is not a valid date")
    return parsed


# ---------------------------------------------------------------------------
# Canonical JSON encode/decode primitives
# ---------------------------------------------------------------------------


def _canonical_dumps(tree: object) -> str:
    return json.dumps(
        tree, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True
    )


def _reject_float(_text: str) -> float:
    _fail("canonical JSON must not contain a float literal")


def _reject_constant(_constant: str) -> float:
    _fail("canonical JSON must not contain NaN/Infinity")


def _strict_object_pairs_hook(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if type(key) is not str:
            _fail("canonical JSON object keys must be strings")
        if key in result:
            _fail("canonical JSON object contains a duplicate key")
        result[key] = value
    return result


def _strict_loads(text: str) -> object:
    parse_failed = False
    result: object = None
    try:
        result = json.loads(
            text,
            object_pairs_hook=_strict_object_pairs_hook,
            parse_float=_reject_float,
            parse_constant=_reject_constant,
        )
    except QualityPilotCanonicalResponseError:
        raise
    except Exception:
        parse_failed = True
    if parse_failed:
        _fail("canonical JSON could not be parsed")
    return result


def _require_exact_keys(tree: object, expected_keys: frozenset[str], field_name: str) -> dict:
    if type(tree) is not dict:
        _fail(f"{field_name} must be a canonical JSON object")
    if set(tree.keys()) != expected_keys:
        _fail(f"{field_name} has missing or extra keys")
    return tree


def _require_str(tree: dict, key: str, field_name: str) -> str:
    value = tree[key]
    if type(value) is not str:
        _fail(f"{field_name} must be text")
    return value


def _require_int(tree: dict, key: str, field_name: str) -> int:
    value = tree[key]
    if type(value) is not int:
        _fail(f"{field_name} must be an exact integer")
    return value


def _require_optional_int(tree: dict, key: str, field_name: str) -> int | None:
    value = tree[key]
    if value is None:
        return None
    if type(value) is not int:
        _fail(f"{field_name} must be an exact integer or null")
    return value


def _require_str_tuple(tree: dict, key: str, field_name: str) -> tuple[str, ...]:
    value = tree[key]
    if type(value) is not list or any(type(item) is not str for item in value):
        _fail(f"{field_name} must be a JSON array of strings")
    return tuple(value)


def _require_array(tree: dict, key: str, field_name: str) -> list:
    value = tree[key]
    if type(value) is not list:
        _fail(f"{field_name} must be a JSON array")
    return value


# ---------------------------------------------------------------------------
# ObservationWindowSpec
# ---------------------------------------------------------------------------

_WINDOW_KEYS = frozenset(
    {
        "closes_at",
        "endpoint_family",
        "market_session",
        "opens_at",
        "pilot_run_id",
        "protocol_sha256",
        "window_id",
        "window_kind",
    }
)


@dataclass(frozen=True, slots=True)
class ObservationWindowSpec:
    """One caller-pinned scheduled observation window. Never reads a clock or a calendar."""

    pilot_run_id: str
    market_session: date
    window_kind: ScheduledWindowKind
    endpoint_family: EndpointFamily
    opens_at: datetime
    closes_at: datetime
    protocol_sha256: str
    window_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "window_id", self._calculated_id())

    def _validate(self) -> None:
        if not _is_sha256(self.pilot_run_id):
            _fail("observation window pilot run id is invalid")
        if type(self.market_session) is not date:
            _fail("observation window market session is invalid")
        if type(self.window_kind) is not ScheduledWindowKind:
            _fail("observation window kind is invalid")
        if type(self.endpoint_family) is not EndpointFamily:
            _fail("observation window endpoint family is invalid")
        if self.protocol_sha256 != PILOT_PROTOCOL_SHA256:
            _fail("observation window protocol hash is invalid")
        if (
            type(self.opens_at) is not datetime
            or self.opens_at.tzinfo is None
            or self.opens_at.utcoffset() is None
        ):
            _fail("observation window opens_at is invalid")
        if (
            type(self.closes_at) is not datetime
            or self.closes_at.tzinfo is None
            or self.closes_at.utcoffset() is None
        ):
            _fail("observation window closes_at is invalid")
        if self.opens_at > self.closes_at:
            _fail("observation window opens_at must not be after closes_at")
        if self.opens_at.astimezone(_IST).date() != self.market_session:
            _fail("observation window opens_at does not map to its market session")
        if self.closes_at.astimezone(_IST).date() != self.market_session:
            _fail("observation window closes_at does not map to its market session")
        if _WINDOW_KIND_ENDPOINT_FAMILY[self.window_kind] != self.endpoint_family:
            _fail("observation window kind and endpoint family do not pair")

    def _calculated_id(self) -> str:
        return sha256(_canonical_dumps(self._tree()).encode("utf-8")).hexdigest()

    def _tree(self) -> dict:
        return {
            "closes_at": _datetime_to_canonical_str(self.closes_at, "closes_at"),
            "endpoint_family": self.endpoint_family.value,
            "market_session": _date_to_canonical_str(self.market_session, "market_session"),
            "opens_at": _datetime_to_canonical_str(self.opens_at, "opens_at"),
            "pilot_run_id": self.pilot_run_id,
            "protocol_sha256": self.protocol_sha256,
            "window_kind": self.window_kind.value,
        }

    def verify_content_identity(self) -> None:
        self._validate()
        if self.window_id != self._calculated_id():
            _fail("observation window identity failed")

    def canonical_json(self) -> str:
        tree = dict(self._tree())
        tree["window_id"] = self.window_id
        return _canonical_dumps(tree)

    @staticmethod
    def _decode(tree: object) -> "ObservationWindowSpec":
        record = _require_exact_keys(tree, _WINDOW_KEYS, "window")
        endpoint_family = _decode_enum(record["endpoint_family"], EndpointFamily, "window.endpoint_family")
        window_kind = _decode_enum(record["window_kind"], ScheduledWindowKind, "window.window_kind")
        spec = ObservationWindowSpec(
            pilot_run_id=_require_str(record, "pilot_run_id", "window.pilot_run_id"),
            market_session=_parse_canonical_date(record["market_session"], "window.market_session"),
            window_kind=window_kind,
            endpoint_family=endpoint_family,
            opens_at=_parse_canonical_datetime(record["opens_at"], "window.opens_at"),
            closes_at=_parse_canonical_datetime(record["closes_at"], "window.closes_at"),
            protocol_sha256=_require_str(record, "protocol_sha256", "window.protocol_sha256"),
        )
        wire_window_id = _require_str(record, "window_id", "window.window_id")
        if wire_window_id != spec.window_id:
            _fail("window id does not match its canonical content")
        return spec

    # Read-only, fixed fail-closed posture. Not dataclass fields -- no
    # per-instance state, never enters window_id.
    @property
    def quality_only(self) -> bool:
        return True

    @property
    def counts_toward_o0(self) -> bool:
        return False

    @property
    def counts_toward_clean_accumulation(self) -> bool:
        return False

    @property
    def research_partition_eligible(self) -> bool:
        return False

    @property
    def training_eligible(self) -> bool:
        return False

    @property
    def feature_eligible(self) -> bool:
        return False

    @property
    def label_eligible(self) -> bool:
        return False

    @property
    def signal_eligible(self) -> bool:
        return False

    @property
    def paper_trade_eligible(self) -> bool:
        return False

    @property
    def notification_eligible(self) -> bool:
        return False

    @property
    def execution_eligible(self) -> bool:
        return False

    @property
    def capital_eligible(self) -> bool:
        return False


def _decode_enum(value: object, enum_type, field_name: str):
    if type(value) is not str:
        _fail(f"{field_name} must be text")
    lookup_failed = False
    parsed = None
    try:
        parsed = enum_type(value)
    except ValueError:
        lookup_failed = True
    if lookup_failed:
        _fail(f"{field_name} is not a recognized value")
    return parsed


# ---------------------------------------------------------------------------
# ObservationRequestIdentity
# ---------------------------------------------------------------------------

_REQUEST_KEYS = frozenset(
    {
        "chunk_count",
        "chunk_index",
        "endpoint_family",
        "exchange",
        "protocol_sha256",
        "provider",
        "provider_version",
        "request_ended_at",
        "request_id",
        "request_started_at",
        "requested_keys",
        "requested_range_end",
        "requested_range_start",
        "requested_session",
        "response_classification",
        "window_id",
    }
)

PROVIDER_ZERODHA_KITE = "ZERODHA_KITE"
EXCHANGE_NSE = "NSE"


@dataclass(frozen=True, slots=True)
class ObservationRequestIdentity:
    """One immutable, independently re-verifiable scheduled provider request."""

    provider: str
    provider_version: str
    endpoint_family: EndpointFamily
    exchange: str
    window_id: str
    requested_session: date
    requested_keys: tuple[str, ...]
    requested_range_start: date | None
    requested_range_end: date | None
    request_started_at: datetime
    request_ended_at: datetime
    chunk_index: int
    chunk_count: int
    response_classification: ResponseClassification
    protocol_sha256: str
    request_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "request_id", self._calculated_id())

    def _validate(self) -> None:
        if self.provider != PROVIDER_ZERODHA_KITE:
            _fail("observation request provider is invalid")
        if not _is_bounded_text(self.provider_version):
            _fail("observation request provider version is invalid")
        if type(self.endpoint_family) is not EndpointFamily:
            _fail("observation request endpoint family is invalid")
        if self.exchange != EXCHANGE_NSE:
            _fail("observation request exchange is invalid")
        if not _is_sha256(self.window_id):
            _fail("observation request window id is invalid")
        if type(self.requested_session) is not date:
            _fail("observation request requested_session is invalid")
        if self.protocol_sha256 != PILOT_PROTOCOL_SHA256:
            _fail("observation request protocol hash is invalid")
        if (
            type(self.request_started_at) is not datetime
            or self.request_started_at.tzinfo is None
            or self.request_started_at.utcoffset() is None
        ):
            _fail("observation request request_started_at is invalid")
        if (
            type(self.request_ended_at) is not datetime
            or self.request_ended_at.tzinfo is None
            or self.request_ended_at.utcoffset() is None
        ):
            _fail("observation request request_ended_at is invalid")
        if self.request_started_at > self.request_ended_at:
            _fail("observation request started_at must not be after ended_at")
        if type(self.chunk_index) is not int or type(self.chunk_count) is not int:
            _fail("observation request chunk fields must be exact integers")
        if self.chunk_count < 1 or self.chunk_count > MAXIMUM_CHUNK_COUNT:
            _fail("observation request chunk_count is out of range")
        if self.chunk_index < 1 or self.chunk_index > self.chunk_count:
            _fail("observation request chunk_index is out of range")
        if type(self.response_classification) is not ResponseClassification:
            _fail("observation request response classification is invalid")
        if self.response_classification not in _CLASSIFICATION_COMPATIBILITY[self.endpoint_family]:
            _fail("observation request response classification is incompatible with its endpoint")
        self._validate_endpoint_shape()

    def _validate_endpoint_shape(self) -> None:
        if self.endpoint_family is EndpointFamily.CATALOG:
            if self.requested_keys != ():
                _fail("catalog requests must not carry requested keys")
            if self.requested_range_start is not None or self.requested_range_end is not None:
                _fail("catalog requests must not carry a requested range")
        elif self.endpoint_family is EndpointFamily.FULL_QUOTE:
            keys_invalid = False
            try:
                require_canonical_listing_keys(
                    self.requested_keys, maximum_keys=MAXIMUM_QUOTE_REQUEST_KEYS
                )
            except QualityPilotCanonicalResponseError:
                raise
            except Exception:
                keys_invalid = True
            if keys_invalid:
                _fail("observation request requested keys are invalid")
            if self.requested_range_start is not None or self.requested_range_end is not None:
                _fail("full quote requests must not carry a requested range")
        else:
            if type(self.requested_keys) is not tuple or len(self.requested_keys) != 1:
                _fail("daily OHLCV requests must name exactly one requested key")
            if LISTING_KEY_PATTERN.fullmatch(self.requested_keys[0]) is None:
                _fail("daily OHLCV requested key is not a canonical listing key")
            if (
                self.requested_range_start != self.requested_session
                or self.requested_range_end != self.requested_session
            ):
                _fail(
                    "daily OHLCV requests must carry a requested range equal to the "
                    "requested session"
                )

    def _tree(self) -> dict:
        return {
            "chunk_count": self.chunk_count,
            "chunk_index": self.chunk_index,
            "endpoint_family": self.endpoint_family.value,
            "exchange": self.exchange,
            "protocol_sha256": self.protocol_sha256,
            "provider": self.provider,
            "provider_version": self.provider_version,
            "request_ended_at": _datetime_to_canonical_str(
                self.request_ended_at, "request_ended_at"
            ),
            "request_started_at": _datetime_to_canonical_str(
                self.request_started_at, "request_started_at"
            ),
            "requested_keys": list(self.requested_keys),
            "requested_range_end": (
                None
                if self.requested_range_end is None
                else _date_to_canonical_str(self.requested_range_end, "requested_range_end")
            ),
            "requested_range_start": (
                None
                if self.requested_range_start is None
                else _date_to_canonical_str(self.requested_range_start, "requested_range_start")
            ),
            "requested_session": _date_to_canonical_str(
                self.requested_session, "requested_session"
            ),
            "response_classification": self.response_classification.value,
            "window_id": self.window_id,
        }

    def _calculated_id(self) -> str:
        return sha256(_canonical_dumps(self._tree()).encode("utf-8")).hexdigest()

    def verify_content_identity(self) -> None:
        self._validate()
        if self.request_id != self._calculated_id():
            _fail("observation request identity failed")

    def canonical_json(self) -> str:
        tree = dict(self._tree())
        tree["request_id"] = self.request_id
        return _canonical_dumps(tree)

    @staticmethod
    def _decode(tree: object) -> "ObservationRequestIdentity":
        record = _require_exact_keys(tree, _REQUEST_KEYS, "request")
        endpoint_family = _decode_enum(
            record["endpoint_family"], EndpointFamily, "request.endpoint_family"
        )
        classification = _decode_enum(
            record["response_classification"],
            ResponseClassification,
            "request.response_classification",
        )
        range_start_text = record["requested_range_start"]
        range_end_text = record["requested_range_end"]
        identity = ObservationRequestIdentity(
            provider=_require_str(record, "provider", "request.provider"),
            provider_version=_require_str(record, "provider_version", "request.provider_version"),
            endpoint_family=endpoint_family,
            exchange=_require_str(record, "exchange", "request.exchange"),
            window_id=_require_str(record, "window_id", "request.window_id"),
            requested_session=_parse_canonical_date(
                record["requested_session"], "request.requested_session"
            ),
            requested_keys=_require_str_tuple(record, "requested_keys", "request.requested_keys"),
            requested_range_start=(
                None
                if range_start_text is None
                else _parse_canonical_date(range_start_text, "request.requested_range_start")
            ),
            requested_range_end=(
                None
                if range_end_text is None
                else _parse_canonical_date(range_end_text, "request.requested_range_end")
            ),
            request_started_at=_parse_canonical_datetime(
                record["request_started_at"], "request.request_started_at"
            ),
            request_ended_at=_parse_canonical_datetime(
                record["request_ended_at"], "request.request_ended_at"
            ),
            chunk_index=_require_int(record, "chunk_index", "request.chunk_index"),
            chunk_count=_require_int(record, "chunk_count", "request.chunk_count"),
            response_classification=classification,
            protocol_sha256=_require_str(record, "protocol_sha256", "request.protocol_sha256"),
        )
        wire_request_id = _require_str(record, "request_id", "request.request_id")
        if wire_request_id != identity.request_id:
            _fail("request id does not match its canonical content")
        return identity

    # Read-only, fixed fail-closed posture. Not dataclass fields -- no
    # per-instance state, never enters request_id.
    @property
    def quality_only(self) -> bool:
        return True

    @property
    def counts_toward_o0(self) -> bool:
        return False

    @property
    def counts_toward_clean_accumulation(self) -> bool:
        return False

    @property
    def research_partition_eligible(self) -> bool:
        return False

    @property
    def training_eligible(self) -> bool:
        return False

    @property
    def feature_eligible(self) -> bool:
        return False

    @property
    def label_eligible(self) -> bool:
        return False

    @property
    def signal_eligible(self) -> bool:
        return False

    @property
    def paper_trade_eligible(self) -> bool:
        return False

    @property
    def notification_eligible(self) -> bool:
        return False

    @property
    def execution_eligible(self) -> bool:
        return False

    @property
    def capital_eligible(self) -> bool:
        return False


# ---------------------------------------------------------------------------
# Canonical payload trees (success payloads + fixed gap payload)
# ---------------------------------------------------------------------------

_GAP_PAYLOAD_TREE = {"kind": "GAP"}


def _reconstruct_instrument(source: object) -> KiteInstrument:
    """Independently rebuild and revalidate one instrument from primitive fields.

    Never trusts a caller-supplied ``KiteInstrument`` as pre-verified --
    accessing its own fields and re-running construction (which re-runs
    ``__post_init__``) is the only way to catch a frozen-dataclass
    ``object.__setattr__`` tamper, since ``KiteInstrument`` exposes no
    ``verify_content_identity`` of its own.
    """

    if type(source) is not KiteInstrument:
        _fail("catalog instrument type is invalid")
    reconstruct_failed = False
    result: KiteInstrument | None = None
    try:
        result = KiteInstrument(
            instrument_token=source.instrument_token,
            exchange_token=source.exchange_token,
            tradingsymbol=source.tradingsymbol,
            name=source.name,
            dump_last_price=source.dump_last_price,
            expiry=source.expiry,
            strike=source.strike,
            tick_size=source.tick_size,
            lot_size=source.lot_size,
            instrument_type=source.instrument_type,
            segment=source.segment,
            exchange=source.exchange,
        )
    except QualityPilotCanonicalResponseError:
        raise
    except Exception:
        reconstruct_failed = True
    if reconstruct_failed or result is None:
        _fail("catalog instrument could not be independently reverified")
    return result


def _reconstruct_instrument_batch(source: object) -> InstrumentBatch:
    """Independently rebuild and revalidate a non-empty, exact Kite NSE/EQ catalog.

    The quality pilot's prospective catalog is defined as the non-empty
    Kite K_EQ cohort: every reconstructed row must satisfy
    ``is_nse_eq_record`` (exchange=NSE, segment=NSE, instrument_type=EQ)
    exactly. This never infers ordinary-equity/main-board status beyond
    that Kite classification.
    """

    if type(source) is not InstrumentBatch:
        _fail("catalog payload type is invalid")
    if type(source.instruments) is not tuple:
        _fail("catalog payload instruments are invalid")
    if len(source.instruments) == 0:
        _fail("catalog payload must contain at least one instrument")
    reconstructed_instruments = tuple(_reconstruct_instrument(item) for item in source.instruments)
    for item in reconstructed_instruments:
        if not item.is_nse_eq_record:
            _fail("catalog payload contains a non-NSE-EQ instrument")
    reconstruct_failed = False
    result: InstrumentBatch | None = None
    try:
        result = InstrumentBatch(
            exchange=source.exchange,
            observed_at=source.observed_at,
            provider_version=source.provider_version,
            instruments=reconstructed_instruments,
        )
    except QualityPilotCanonicalResponseError:
        raise
    except Exception:
        reconstruct_failed = True
    if reconstruct_failed or result is None:
        _fail("catalog payload could not be independently reverified")
    if len(result.instruments) == 0:
        _fail("catalog payload must contain at least one instrument")
    return result


def _reconstruct_session_finality(source: object) -> NseSessionFinality:
    """Independently rebuild the expected regular guard and require exact equality.

    Never normalizes a tampered/noncanonical finality into a valid one --
    ``source``'s own market_close_at/data_ready_at/policy_version/actionable
    must exactly match what ``regular_collection_guard(source.session)``
    itself produces, or the whole finality is rejected.
    """

    if type(source) is not NseSessionFinality:
        _fail("daily OHLCV session finality type is invalid")
    reconstruct_failed = False
    expected: NseSessionFinality | None = None
    try:
        expected = NseSessionFinality.regular_collection_guard(source.session)
    except QualityPilotCanonicalResponseError:
        raise
    except Exception:
        reconstruct_failed = True
    if reconstruct_failed or expected is None:
        _fail("daily OHLCV session finality could not be independently reverified")
    if (
        source.session != expected.session
        or source.market_close_at != expected.market_close_at
        or source.data_ready_at != expected.data_ready_at
        or source.policy_version != expected.policy_version
        or source.actionable != expected.actionable
    ):
        _fail("daily OHLCV session finality does not match its exact regular guard")
    return expected


def _reconstruct_daily_candle(source: object) -> DailyCandle:
    if type(source) is not DailyCandle:
        _fail("daily OHLCV candle type is invalid")
    reconstruct_failed = False
    result: DailyCandle | None = None
    try:
        result = DailyCandle(
            instrument_token=source.instrument_token,
            timestamp=source.timestamp,
            open=source.open,
            high=source.high,
            low=source.low,
            close=source.close,
            volume=source.volume,
            open_interest=source.open_interest,
        )
    except QualityPilotCanonicalResponseError:
        raise
    except Exception:
        reconstruct_failed = True
    if reconstruct_failed or result is None:
        _fail("daily OHLCV candle could not be independently reverified")
    return result


def _reconstruct_daily_candle_batch(source: object) -> DailyCandleBatch:
    """Independently rebuild and revalidate a current-session daily candle batch.

    ``DailyCandleBatch``/``DailyCandle``/``NseSessionFinality`` expose no
    ``verify_content_identity`` of their own -- the same reconstruction
    defense used for catalog instruments is applied here for symmetry.
    """

    if type(source) is not DailyCandleBatch:
        _fail("daily OHLCV payload type is invalid")
    if type(source.candles) is not tuple or len(source.candles) != 1:
        _fail("daily OHLCV payload must contain exactly one candle")
    finality = _reconstruct_session_finality(source.session_finality)
    candle = _reconstruct_daily_candle(source.candles[0])
    reconstruct_failed = False
    result: DailyCandleBatch | None = None
    try:
        result = DailyCandleBatch(
            instrument_token=source.instrument_token,
            session_finality=finality,
            observed_at=source.observed_at,
            provider_version=source.provider_version,
            candles=(candle,),
        )
    except QualityPilotCanonicalResponseError:
        raise
    except Exception:
        reconstruct_failed = True
    if reconstruct_failed or result is None:
        _fail("daily OHLCV payload could not be independently reverified")
    return result


def _instrument_tree(instrument: KiteInstrument) -> dict:
    return {
        "dump_last_price": _decimal_to_canonical_str(
            instrument.dump_last_price, "instrument.dump_last_price"
        ),
        "exchange": instrument.exchange,
        "exchange_token": instrument.exchange_token,
        "expiry": (
            None
            if instrument.expiry is None
            else _date_to_canonical_str(instrument.expiry, "instrument.expiry")
        ),
        "instrument_token": instrument.instrument_token,
        "instrument_type": instrument.instrument_type,
        "lot_size": instrument.lot_size,
        "name": instrument.name,
        "segment": instrument.segment,
        "strike": (
            None
            if instrument.strike is None
            else _decimal_to_canonical_str(instrument.strike, "instrument.strike")
        ),
        "tick_size": _decimal_to_canonical_str(instrument.tick_size, "instrument.tick_size"),
        "tradingsymbol": instrument.tradingsymbol,
    }


def _catalog_payload_tree(payload: InstrumentBatch) -> dict:
    ordered = sorted(
        payload.instruments,
        key=lambda item: (item.exchange, item.instrument_token, item.tradingsymbol),
    )
    return {
        "exchange": payload.exchange,
        "instruments": [_instrument_tree(item) for item in ordered],
        "kind": "CATALOG",
        "observed_at": _datetime_to_canonical_str(payload.observed_at, "catalog.observed_at"),
        "provider_version": payload.provider_version,
    }


def _depth_level_tree(level: KiteDepthLevel) -> dict:
    return {
        "orders": level.orders,
        "price": _decimal_to_canonical_str(level.price, "depth.price"),
        "quantity": level.quantity,
    }


def _quote_tree(quote: KiteFullQuote) -> dict:
    return {
        "depth_buy": [_depth_level_tree(level) for level in quote.depth_buy],
        "depth_sell": [_depth_level_tree(level) for level in quote.depth_sell],
        "exchange_timestamp": _datetime_to_canonical_str(
            quote.exchange_timestamp, "quote.exchange_timestamp"
        ),
        "instrument_token": quote.instrument_token,
        "last_price": _decimal_to_canonical_str(quote.last_price, "quote.last_price"),
        "last_trade_time": (
            None
            if quote.last_trade_time is None
            else _datetime_to_canonical_str(quote.last_trade_time, "quote.last_trade_time")
        ),
        "listing_key": quote.listing_key,
        "lower_circuit_limit": _decimal_to_canonical_str(
            quote.lower_circuit_limit, "quote.lower_circuit_limit"
        ),
        "upper_circuit_limit": _decimal_to_canonical_str(
            quote.upper_circuit_limit, "quote.upper_circuit_limit"
        ),
    }


def _full_quote_payload_tree(payload: FullQuoteBatch) -> dict:
    return {
        "kind": "FULL_QUOTE",
        "observed_at": _datetime_to_canonical_str(payload.observed_at, "full_quote.observed_at"),
        "provider_version": payload.provider_version,
        "quotes": [_quote_tree(quote) for quote in payload.quotes],
        "requested_at": _datetime_to_canonical_str(payload.requested_at, "full_quote.requested_at"),
        "requested_keys": list(payload.requested_keys),
    }


def _daily_ohlcv_payload_tree(payload: DailyCandleBatch, listing_key: str) -> dict:
    candle = payload.candles[0]
    finality = payload.session_finality
    return {
        "actionable": finality.actionable,
        "close": _decimal_to_canonical_str(candle.close, "daily_ohlcv.close"),
        "data_ready_at": _datetime_to_canonical_str(
            finality.data_ready_at, "daily_ohlcv.data_ready_at"
        ),
        "high": _decimal_to_canonical_str(candle.high, "daily_ohlcv.high"),
        "instrument_token": payload.instrument_token,
        "kind": "DAILY_OHLCV",
        "listing_key": listing_key,
        "low": _decimal_to_canonical_str(candle.low, "daily_ohlcv.low"),
        "market_close_at": _datetime_to_canonical_str(
            finality.market_close_at, "daily_ohlcv.market_close_at"
        ),
        "observed_at": _datetime_to_canonical_str(payload.observed_at, "daily_ohlcv.observed_at"),
        "open": _decimal_to_canonical_str(candle.open, "daily_ohlcv.open"),
        "open_interest": candle.open_interest,
        "policy_version": finality.policy_version,
        "provider_version": payload.provider_version,
        "session": _date_to_canonical_str(finality.session, "daily_ohlcv.session"),
        "volume": candle.volume,
    }


def _daily_ohlcv_normalized_tree(payload: DailyCandleBatch, listing_key: str) -> dict:
    candle = payload.candles[0]
    return {
        "close": _decimal_to_canonical_str(candle.close, "daily_ohlcv.close"),
        "high": _decimal_to_canonical_str(candle.high, "daily_ohlcv.high"),
        "instrument_token": payload.instrument_token,
        "listing_key": listing_key,
        "low": _decimal_to_canonical_str(candle.low, "daily_ohlcv.low"),
        "open": _decimal_to_canonical_str(candle.open, "daily_ohlcv.open"),
        "open_interest": candle.open_interest,
        "session": _date_to_canonical_str(
            payload.session_finality.session, "daily_ohlcv.session"
        ),
        "volume": candle.volume,
    }


def _canonical_payload_tree(
    request: "ObservationRequestIdentity",
    payload: Union[InstrumentBatch, FullQuoteBatch, DailyCandleBatch, None],
) -> dict:
    classification = request.response_classification
    endpoint_family = request.endpoint_family
    if classification not in _SUCCESS_CLASSIFICATIONS:
        if payload is not None:
            _fail("non-success observations must not carry a success payload")
        return dict(_GAP_PAYLOAD_TREE)
    if payload is None:
        _fail("success observations must carry a success payload")
    if endpoint_family is EndpointFamily.CATALOG:
        reconstructed = _reconstruct_instrument_batch(payload)
        if len(reconstructed.instruments) > MAXIMUM_CATALOG_INSTRUMENTS:
            _fail("catalog success payload exceeds the instrument ceiling")
        return _catalog_payload_tree(reconstructed)
    if endpoint_family is EndpointFamily.FULL_QUOTE:
        if type(payload) is not FullQuoteBatch:
            _fail("full quote success payload type is invalid")
        return _full_quote_payload_tree(payload)
    reconstructed = _reconstruct_daily_candle_batch(payload)
    return _daily_ohlcv_payload_tree(reconstructed, request.requested_keys[0])


def _normalized_content_tree(
    request: "ObservationRequestIdentity",
    payload: Union[InstrumentBatch, FullQuoteBatch, DailyCandleBatch, None],
) -> dict:
    """Build the exact normalized_content_v1 record tree.

    Deliberately excludes window/request/chunk identities and timestamps,
    and excludes batch-level requested_at/observed_at/provider_version
    metadata -- only economic/identity record fields, exact record
    ordering, and the requested listing key/provider instrument token
    needed to distinguish content enter this tree. The fixed response
    classification is bound so two different non-success reasons (or a
    non-success and a SUCCESS with zero records) never collide.
    """

    classification = request.response_classification
    if classification not in _SUCCESS_CLASSIFICATIONS:
        return {"classification": classification.value, "records": []}
    if payload is None:
        _fail("success observations must carry a success payload")
    family = request.endpoint_family
    if family is EndpointFamily.CATALOG:
        reconstructed = _reconstruct_instrument_batch(payload)
        ordered = sorted(
            reconstructed.instruments,
            key=lambda item: (item.exchange, item.instrument_token, item.tradingsymbol),
        )
        records = [_instrument_tree(item) for item in ordered]
    elif family is EndpointFamily.FULL_QUOTE:
        if type(payload) is not FullQuoteBatch:
            _fail("full quote success payload type is invalid")
        records = [_quote_tree(quote) for quote in payload.quotes]
    else:
        reconstructed = _reconstruct_daily_candle_batch(payload)
        records = [_daily_ohlcv_normalized_tree(reconstructed, request.requested_keys[0])]
    return {"classification": classification.value, "records": records}


# ---------------------------------------------------------------------------
# QualityPilotObservation
# ---------------------------------------------------------------------------

_ENVELOPE_KEYS = frozenset(
    {
        "canonical_response_sha256",
        "corrects_observation_id",
        "normalized_content_sha256",
        "observation_id",
        "payload",
        "request",
        "window",
    }
)

_POSTURE_NAMES = (
    "quality_only",
    "counts_toward_o0",
    "counts_toward_clean_accumulation",
    "research_partition_eligible",
    "training_eligible",
    "feature_eligible",
    "label_eligible",
    "signal_eligible",
    "paper_trade_eligible",
    "notification_eligible",
    "execution_eligible",
    "capital_eligible",
)


@dataclass(frozen=True, slots=True)
class QualityPilotObservation:
    """One immutable, complete canonical_response_v1 observation envelope.

    Independently re-verifies its nested window and request identities,
    cross-checks their lineage and the request interval against the exact
    window, cross-checks the payload (if any) against the request, and
    reproduces both canonical hashes and its own observation_id
    deterministically from its own fields -- never from caller-supplied
    hash inputs.
    """

    window: ObservationWindowSpec
    request: ObservationRequestIdentity
    payload: Union[InstrumentBatch, FullQuoteBatch, DailyCandleBatch, None]
    corrects_observation_id: str | None
    canonical_response_sha256: str = field(init=False)
    normalized_content_sha256: str = field(init=False)
    quality_only: bool = field(init=False)
    counts_toward_o0: bool = field(init=False)
    counts_toward_clean_accumulation: bool = field(init=False)
    research_partition_eligible: bool = field(init=False)
    training_eligible: bool = field(init=False)
    feature_eligible: bool = field(init=False)
    label_eligible: bool = field(init=False)
    signal_eligible: bool = field(init=False)
    paper_trade_eligible: bool = field(init=False)
    notification_eligible: bool = field(init=False)
    execution_eligible: bool = field(init=False)
    capital_eligible: bool = field(init=False)
    observation_id: str = field(init=False)

    def __post_init__(self) -> None:
        for name in _POSTURE_NAMES:
            object.__setattr__(self, name, name == "quality_only")
        self._validate()
        payload_tree = _canonical_payload_tree(self.request, self.payload)
        request_identity_json = self.request.canonical_json()
        canonical_payload_json = _canonical_dumps(payload_tree)
        object.__setattr__(
            self,
            "canonical_response_sha256",
            _response_hash(request_identity_json, canonical_payload_json),
        )
        object.__setattr__(
            self,
            "normalized_content_sha256",
            _normalized_hash(self.request, self.payload),
        )
        object.__setattr__(self, "observation_id", self._calculated_id())

    def _validate(self) -> None:
        if type(self.window) is not ObservationWindowSpec:
            _fail("observation window type is invalid")
        self.window.verify_content_identity()
        if type(self.request) is not ObservationRequestIdentity:
            _fail("observation request type is invalid")
        self.request.verify_content_identity()
        if self.request.window_id != self.window.window_id:
            _fail("observation request does not reference its exact window")
        if self.request.endpoint_family != self.window.endpoint_family:
            _fail("observation request endpoint family disagrees with its window")
        if self.request.requested_session != self.window.market_session:
            _fail("observation request requested_session disagrees with its window")
        if (
            self.request.request_started_at < self.window.opens_at
            or self.request.request_ended_at > self.window.closes_at
        ):
            _fail("observation request interval does not lie inside its window")
        if self.corrects_observation_id is not None and not _is_sha256(
            self.corrects_observation_id
        ):
            _fail("observation correction lineage is invalid")
        self._validate_payload_cross_check()

    def _validate_payload_cross_check(self) -> None:
        classification = self.request.response_classification
        payload = self.payload
        if classification not in _SUCCESS_CLASSIFICATIONS:
            if payload is not None:
                _fail("non-success observations must not carry a success payload")
            return
        if payload is None:
            _fail("success observations must carry a success payload")
        family = self.request.endpoint_family
        if family is EndpointFamily.CATALOG:
            # InstrumentBatch has no batch_id/verify_content_identity of its
            # own (unlike FullQuoteBatch) -- independently reconstruct every
            # nested KiteInstrument and the batch itself from primitive
            # field values so a frozen-dataclass object.__setattr__ tamper
            # cannot survive to canonicalization.
            reconstructed = _reconstruct_instrument_batch(payload)
            if reconstructed.exchange != self.request.exchange:
                _fail("catalog payload exchange disagrees with its request")
            if reconstructed.provider_version != self.request.provider_version:
                _fail("catalog payload provider version disagrees with its request")
            if (
                reconstructed.observed_at < self.request.request_started_at
                or reconstructed.observed_at > self.request.request_ended_at
            ):
                _fail("catalog payload was observed outside its request window")
            if len(reconstructed.instruments) > MAXIMUM_CATALOG_INSTRUMENTS:
                _fail("catalog payload exceeds the instrument ceiling")
        elif family is EndpointFamily.FULL_QUOTE:
            if type(payload) is not FullQuoteBatch:
                _fail("full quote payload type is invalid")
            verify_failed = False
            try:
                payload.verify_content_identity()
            except Exception:
                verify_failed = True
            if verify_failed:
                _fail("full quote payload failed independent verification")
            if payload.requested_keys != self.request.requested_keys:
                _fail("full quote payload requested keys disagree with its request")
            if payload.provider_version != self.request.provider_version:
                _fail("full quote payload provider version disagrees with its request")
            if (
                payload.requested_at < self.request.request_started_at
                or payload.observed_at > self.request.request_ended_at
            ):
                _fail("full quote payload timestamps disagree with its request interval")
        else:
            # DailyCandleBatch also has no verify_content_identity of its
            # own -- apply the same independent-reconstruction defense used
            # for catalog.
            reconstructed = _reconstruct_daily_candle_batch(payload)
            candle = reconstructed.candles[0]
            if candle.session != self.request.requested_session:
                _fail("daily OHLCV payload session disagrees with its requested session")
            if candle.session != self.window.market_session:
                _fail("daily OHLCV payload session disagrees with its window")
            if reconstructed.provider_version != self.request.provider_version:
                _fail("daily OHLCV payload provider version disagrees with its request")
            if (
                reconstructed.observed_at < self.request.request_started_at
                or reconstructed.observed_at > self.request.request_ended_at
            ):
                _fail("daily OHLCV payload timestamps disagree with its request interval")

    @property
    def record_count(self) -> int:
        payload = self.payload
        if payload is None:
            return 0
        if type(payload) is InstrumentBatch:
            return len(payload.instruments)
        if type(payload) is FullQuoteBatch:
            return len(payload.quotes)
        return len(payload.candles)

    def _calculated_id(self) -> str:
        material = {
            "canonical_response_sha256": self.canonical_response_sha256,
            "corrects_observation_id": self.corrects_observation_id,
            "normalized_content_sha256": self.normalized_content_sha256,
            "policy_version": QUALITY_PILOT_POLICY_VERSION,
            "posture": {name: getattr(self, name) for name in _POSTURE_NAMES},
            "protocol_sha256": self.request.protocol_sha256,
            "record_count": self.record_count,
            "request_id": self.request.request_id,
            "response_classification": self.request.response_classification.value,
            "schema": CANONICAL_RESPONSE_SCHEMA_VERSION,
            "window_id": self.window.window_id,
        }
        return sha256(_canonical_dumps(material).encode("utf-8")).hexdigest()

    def verify_content_identity(self) -> None:
        self._validate()
        payload_tree = _canonical_payload_tree(self.request, self.payload)
        request_identity_json = self.request.canonical_json()
        canonical_payload_json = _canonical_dumps(payload_tree)
        if self.canonical_response_sha256 != _response_hash(
            request_identity_json, canonical_payload_json
        ):
            _fail("observation canonical response hash failed")
        if self.normalized_content_sha256 != _normalized_hash(self.request, self.payload):
            _fail("observation normalized content hash failed")
        if any(getattr(self, name) != (name == "quality_only") for name in _POSTURE_NAMES):
            _fail("observation safety posture is invalid")
        if self.observation_id != self._calculated_id():
            _fail("observation identity failed")


def _response_hash(request_identity_json: str, canonical_payload_json: str) -> str:
    payload = "|".join(
        (CANONICAL_RESPONSE_SCHEMA_VERSION, request_identity_json, canonical_payload_json)
    )
    return sha256(payload.encode("utf-8")).hexdigest()


def _normalized_hash(
    request: "ObservationRequestIdentity",
    payload: Union[InstrumentBatch, FullQuoteBatch, DailyCandleBatch, None],
) -> str:
    normalized_tree = _normalized_content_tree(request, payload)
    normalized_json = _canonical_dumps(normalized_tree)
    payload_str = "|".join((NORMALIZED_CONTENT_SCHEMA_VERSION, normalized_json))
    return sha256(payload_str.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Strict encode / decode / replay
# ---------------------------------------------------------------------------


def encode_observation(observation: QualityPilotObservation) -> bytes:
    """Encode one observation as canonical, strictly reproducible UTF-8 JSON bytes."""

    if type(observation) is not QualityPilotObservation:
        _fail("encode input must be an exact QualityPilotObservation")
    observation.verify_content_identity()
    payload_tree = _canonical_payload_tree(observation.request, observation.payload)
    envelope = {
        "canonical_response_sha256": observation.canonical_response_sha256,
        "corrects_observation_id": observation.corrects_observation_id,
        "normalized_content_sha256": observation.normalized_content_sha256,
        "observation_id": observation.observation_id,
        "payload": payload_tree,
        "request": json.loads(observation.request.canonical_json()),
        "window": json.loads(observation.window.canonical_json()),
    }
    encoded = _canonical_dumps(envelope).encode("utf-8")
    if len(encoded) > MAXIMUM_ENCODED_BYTES:
        _fail("encoded observation exceeds the maximum encoded byte ceiling")
    return encoded


def decode_observation(data: bytes) -> QualityPilotObservation:
    """Strictly decode canonical observation bytes back into a QualityPilotObservation.

    Rejects duplicate keys at every nesting level, floats/NaN/Infinity,
    unknown/missing/extra keys, wrong types, noncanonical spellings, forged
    nested IDs/hashes, and unsafe posture. Re-encoding the result reproduces
    the exact input bytes.
    """

    if type(data) is not bytes or not data:
        _fail("encoded observation must be non-empty bytes")
    if len(data) > MAXIMUM_ENCODED_BYTES:
        _fail("encoded observation exceeds the maximum encoded byte ceiling")
    decode_failed = False
    text = ""
    try:
        text = data.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        decode_failed = True
    if decode_failed:
        _fail("encoded observation is not valid UTF-8")
    tree = _strict_loads(text)
    record = _require_exact_keys(tree, _ENVELOPE_KEYS, "observation")

    decode_nested_failed = False
    window: ObservationWindowSpec | None = None
    request: ObservationRequestIdentity | None = None
    payload = None
    try:
        window = ObservationWindowSpec._decode(record["window"])
        request = ObservationRequestIdentity._decode(record["request"])
        payload = _decode_payload(request, record["payload"])
    except QualityPilotCanonicalResponseError:
        raise
    except Exception:
        decode_nested_failed = True
    if decode_nested_failed or window is None or request is None:
        _fail("observation nested fields could not be decoded")
    corrects = record["corrects_observation_id"]
    if corrects is not None and type(corrects) is not str:
        _fail("corrects_observation_id must be text or null")

    reconstruct_failed = False
    observation: QualityPilotObservation | None = None
    try:
        observation = QualityPilotObservation(
            window=window,
            request=request,
            payload=payload,
            corrects_observation_id=corrects,
        )
    except QualityPilotCanonicalResponseError:
        raise
    except Exception:
        reconstruct_failed = True
    if reconstruct_failed or observation is None:
        _fail("observation could not be reconstructed")

    for wire_key, actual_value in (
        ("canonical_response_sha256", observation.canonical_response_sha256),
        ("normalized_content_sha256", observation.normalized_content_sha256),
        ("observation_id", observation.observation_id),
    ):
        wire_value = record[wire_key]
        if type(wire_value) is not str or wire_value != actual_value:
            _fail(f"{wire_key} does not match its canonical content")

    observation.verify_content_identity()
    return observation


def _decode_payload(
    request: "ObservationRequestIdentity",
    tree: object,
) -> Union[InstrumentBatch, FullQuoteBatch, DailyCandleBatch, None]:
    classification = request.response_classification
    if classification not in _SUCCESS_CLASSIFICATIONS:
        if tree != _GAP_PAYLOAD_TREE:
            _fail("non-success payload must be the fixed gap payload")
        return None
    if type(tree) is not dict:
        _fail("success payload must be a canonical JSON object")
    endpoint_family = request.endpoint_family
    if endpoint_family is EndpointFamily.CATALOG:
        return _decode_catalog_payload(tree)
    if endpoint_family is EndpointFamily.FULL_QUOTE:
        return _decode_full_quote_payload(tree)
    return _decode_daily_ohlcv_payload(tree, request.requested_keys[0])


_INSTRUMENT_KEYS = frozenset(
    {
        "dump_last_price",
        "exchange",
        "exchange_token",
        "expiry",
        "instrument_token",
        "instrument_type",
        "lot_size",
        "name",
        "segment",
        "strike",
        "tick_size",
        "tradingsymbol",
    }
)

_CATALOG_KEYS = frozenset(
    {"exchange", "instruments", "kind", "observed_at", "provider_version"}
)


def _decode_instrument(tree: object) -> KiteInstrument:
    record = _require_exact_keys(tree, _INSTRUMENT_KEYS, "instrument")
    expiry_text = record["expiry"]
    strike_text = record["strike"]
    return KiteInstrument(
        instrument_token=_require_int(record, "instrument_token", "instrument.instrument_token"),
        exchange_token=_require_str(record, "exchange_token", "instrument.exchange_token"),
        tradingsymbol=_require_str(record, "tradingsymbol", "instrument.tradingsymbol"),
        name=_require_str(record, "name", "instrument.name"),
        dump_last_price=_parse_canonical_decimal(
            record["dump_last_price"], "instrument.dump_last_price"
        ),
        expiry=None if expiry_text is None else _parse_canonical_date(expiry_text, "instrument.expiry"),
        strike=None if strike_text is None else _parse_canonical_decimal(strike_text, "instrument.strike"),
        tick_size=_parse_canonical_decimal(record["tick_size"], "instrument.tick_size"),
        lot_size=_require_int(record, "lot_size", "instrument.lot_size"),
        instrument_type=_require_str(record, "instrument_type", "instrument.instrument_type"),
        segment=_require_str(record, "segment", "instrument.segment"),
        exchange=_require_str(record, "exchange", "instrument.exchange"),
    )


def _decode_catalog_payload(tree: dict) -> InstrumentBatch:
    record = _require_exact_keys(tree, _CATALOG_KEYS, "catalog payload")
    if record["kind"] != "CATALOG":
        _fail("catalog payload kind is invalid")
    instruments_tree = _require_array(record, "instruments", "catalog.instruments")
    if len(instruments_tree) > MAXIMUM_CATALOG_INSTRUMENTS:
        _fail("catalog payload exceeds the instrument ceiling")
    instruments = tuple(_decode_instrument(item) for item in instruments_tree)
    return InstrumentBatch(
        exchange=_require_str(record, "exchange", "catalog.exchange"),
        observed_at=_parse_canonical_datetime(record["observed_at"], "catalog.observed_at"),
        provider_version=_require_str(record, "provider_version", "catalog.provider_version"),
        instruments=instruments,
    )


_DEPTH_KEYS = frozenset({"orders", "price", "quantity"})
_QUOTE_KEYS = frozenset(
    {
        "depth_buy",
        "depth_sell",
        "exchange_timestamp",
        "instrument_token",
        "last_price",
        "last_trade_time",
        "listing_key",
        "lower_circuit_limit",
        "upper_circuit_limit",
    }
)
_FULL_QUOTE_KEYS = frozenset(
    {"kind", "observed_at", "provider_version", "quotes", "requested_at", "requested_keys"}
)


def _decode_depth_level(tree: object) -> KiteDepthLevel:
    record = _require_exact_keys(tree, _DEPTH_KEYS, "depth level")
    return KiteDepthLevel(
        price=_parse_canonical_decimal(record["price"], "depth.price"),
        quantity=_require_int(record, "quantity", "depth.quantity"),
        orders=_require_int(record, "orders", "depth.orders"),
    )


def _decode_quote(tree: object) -> KiteFullQuote:
    record = _require_exact_keys(tree, _QUOTE_KEYS, "quote")
    last_trade_text = record["last_trade_time"]
    depth_buy_tree = _require_array(record, "depth_buy", "quote.depth_buy")
    depth_sell_tree = _require_array(record, "depth_sell", "quote.depth_sell")
    return KiteFullQuote(
        listing_key=_require_str(record, "listing_key", "quote.listing_key"),
        instrument_token=_require_int(record, "instrument_token", "quote.instrument_token"),
        exchange_timestamp=_parse_canonical_datetime(
            record["exchange_timestamp"], "quote.exchange_timestamp"
        ),
        last_trade_time=(
            None
            if last_trade_text is None
            else _parse_canonical_datetime(last_trade_text, "quote.last_trade_time")
        ),
        last_price=_parse_canonical_decimal(record["last_price"], "quote.last_price"),
        lower_circuit_limit=_parse_canonical_decimal(
            record["lower_circuit_limit"], "quote.lower_circuit_limit"
        ),
        upper_circuit_limit=_parse_canonical_decimal(
            record["upper_circuit_limit"], "quote.upper_circuit_limit"
        ),
        depth_buy=tuple(_decode_depth_level(item) for item in depth_buy_tree),
        depth_sell=tuple(_decode_depth_level(item) for item in depth_sell_tree),
    )


def _decode_full_quote_payload(tree: dict) -> FullQuoteBatch:
    record = _require_exact_keys(tree, _FULL_QUOTE_KEYS, "full quote payload")
    if record["kind"] != "FULL_QUOTE":
        _fail("full quote payload kind is invalid")
    quotes_tree = _require_array(record, "quotes", "full_quote.quotes")
    requested_keys = _require_str_tuple(record, "requested_keys", "full_quote.requested_keys")
    if len(requested_keys) > MAXIMUM_QUOTE_REQUEST_KEYS:
        _fail("full quote payload exceeds the requested key ceiling")
    return FullQuoteBatch(
        requested_keys=requested_keys,
        requested_at=_parse_canonical_datetime(record["requested_at"], "full_quote.requested_at"),
        observed_at=_parse_canonical_datetime(record["observed_at"], "full_quote.observed_at"),
        provider_version=_require_str(record, "provider_version", "full_quote.provider_version"),
        quotes=tuple(_decode_quote(item) for item in quotes_tree),
    )


_DAILY_OHLCV_KEYS = frozenset(
    {
        "actionable",
        "close",
        "data_ready_at",
        "high",
        "instrument_token",
        "kind",
        "listing_key",
        "low",
        "market_close_at",
        "observed_at",
        "open",
        "open_interest",
        "policy_version",
        "provider_version",
        "session",
        "volume",
    }
)


def _decode_daily_ohlcv_payload(tree: dict, expected_listing_key: str) -> DailyCandleBatch:
    record = _require_exact_keys(tree, _DAILY_OHLCV_KEYS, "daily OHLCV payload")
    if record["kind"] != "DAILY_OHLCV":
        _fail("daily OHLCV payload kind is invalid")
    listing_key = _require_str(record, "listing_key", "daily_ohlcv.listing_key")
    if listing_key != expected_listing_key:
        _fail("daily OHLCV payload listing key does not match its request")
    session = _parse_canonical_date(record["session"], "daily_ohlcv.session")
    instrument_token = _require_int(record, "instrument_token", "daily_ohlcv.instrument_token")

    finality_failed = False
    session_finality: NseSessionFinality | None = None
    try:
        session_finality = NseSessionFinality.regular_collection_guard(session)
    except QualityPilotCanonicalResponseError:
        raise
    except Exception:
        finality_failed = True
    if finality_failed or session_finality is None:
        _fail("daily OHLCV session could not be reconstructed")

    # The wire format retains the complete exact finality contract -- never
    # normalize a noncanonical/tampered guard into a valid one. Every wire
    # finality field must exactly match what the deterministic regular
    # guard for the decoded session itself produces.
    wire_actionable = record["actionable"]
    if type(wire_actionable) is not bool or wire_actionable != session_finality.actionable:
        _fail("daily OHLCV finality actionable does not match its exact regular guard")
    wire_policy_version = _require_str(record, "policy_version", "daily_ohlcv.policy_version")
    if wire_policy_version != session_finality.policy_version:
        _fail("daily OHLCV finality policy_version does not match its exact regular guard")
    wire_market_close_at = _parse_canonical_datetime(
        record["market_close_at"], "daily_ohlcv.market_close_at"
    )
    if wire_market_close_at != session_finality.market_close_at:
        _fail("daily OHLCV finality market_close_at does not match its exact regular guard")
    wire_data_ready_at = _parse_canonical_datetime(
        record["data_ready_at"], "daily_ohlcv.data_ready_at"
    )
    if wire_data_ready_at != session_finality.data_ready_at:
        _fail("daily OHLCV finality data_ready_at does not match its exact regular guard")

    # The wire format retains only the session date for the candle's own
    # bar (a fixed, canonical daily-bar convention), never a caller-chosen
    # intraday time-of-day -- candle.timestamp never enters
    # canonical_payload_json/observation_id, only its derived .session
    # (== session_finality.session) does, so a deterministic midnight-IST
    # anchor reproduces byte-identical hashes.
    timestamp = datetime.combine(session, time.min, tzinfo=_IST)
    candle = DailyCandle(
        instrument_token=instrument_token,
        timestamp=timestamp,
        open=_parse_canonical_decimal(record["open"], "daily_ohlcv.open"),
        high=_parse_canonical_decimal(record["high"], "daily_ohlcv.high"),
        low=_parse_canonical_decimal(record["low"], "daily_ohlcv.low"),
        close=_parse_canonical_decimal(record["close"], "daily_ohlcv.close"),
        volume=_require_int(record, "volume", "daily_ohlcv.volume"),
        open_interest=_require_optional_int(record, "open_interest", "daily_ohlcv.open_interest"),
    )
    return DailyCandleBatch(
        instrument_token=instrument_token,
        session_finality=session_finality,
        observed_at=_parse_canonical_datetime(record["observed_at"], "daily_ohlcv.observed_at"),
        provider_version=_require_str(record, "provider_version", "daily_ohlcv.provider_version"),
        candles=(candle,),
    )


def replay_verify(data: bytes) -> QualityPilotObservation:
    """Decode canonical observation bytes and re-verify every hash and identity.

    A thin, explicit alias over :func:`decode_observation` -- decoding
    itself already reconstructs both canonical JSON strings and both
    hashes from the retained typed envelope and fails closed on any
    mismatch. Exists as its own name so callers can express replay intent
    without implying they are merely parsing untrusted bytes.
    """

    return decode_observation(data)
