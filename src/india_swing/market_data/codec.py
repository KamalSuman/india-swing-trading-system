from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any

from .models import (
    DailyCandle,
    DailyCandleArchive,
    DailyCandleBatch,
    HistoricalDailyCandle,
    HistoricalDailyCandleBatch,
    HistoricalDailyRequest,
    HistoricalInstrumentBinding,
    HistoricalResponsePage,
    InstrumentBatch,
    KiteInstrument,
    NseSessionFinality,
)
from .reconciliation import (
    HistoricalCandleDifference,
    HistoricalCandleReconciliationReport,
    HistoricalCandleReconciliationRow,
    HistoricalReconciliationStatus,
)


MARKET_PAYLOAD_CODEC_VERSION = "market-data-json/v1"

_SENSITIVE_KEY_PARTS = (
    "api_key",
    "apikey",
    "authorization",
    "client_secret",
    "credential",
    "password",
    "private_key",
    "secret",
    "access_token",
    "refresh_token",
    "request_token",
    "session_token",
    "auth_token",
    "cookie",
    "set_cookie",
)
_SENSITIVE_VALUE_MARKERS = re.compile(
    r"(?:authorization\s*:|set-cookie\s*:|cookie\s*:|"
    r"(?:access[_-]?token|refresh[_-]?token|request[_-]?token|auth[_-]?token|"
    r"api[_-]?key|client[_-]?secret|password)\s*[=:]|"
    r"-----BEGIN (?:RSA |EC )?PRIVATE KEY-----)",
    re.IGNORECASE,
)

_ALLOWED_DATACLASSES = (
    KiteInstrument,
    InstrumentBatch,
    NseSessionFinality,
    DailyCandle,
    DailyCandleBatch,
    DailyCandleArchive,
    HistoricalInstrumentBinding,
    HistoricalDailyRequest,
    HistoricalDailyCandle,
    HistoricalResponsePage,
    HistoricalDailyCandleBatch,
    HistoricalCandleDifference,
    HistoricalCandleReconciliationRow,
    HistoricalCandleReconciliationReport,
)
_TYPE_TO_TAG = {
    value_type: f"{value_type.__module__}.{value_type.__qualname__}"
    for value_type in _ALLOWED_DATACLASSES
}
_TAG_TO_TYPE = {tag: value_type for value_type, tag in _TYPE_TO_TAG.items()}
_ALLOWED_ENUMS = (HistoricalReconciliationStatus,)
_ENUM_TO_TAG = {
    value_type: f"{value_type.__module__}.{value_type.__qualname__}"
    for value_type in _ALLOWED_ENUMS
}
_TAG_TO_ENUM = {tag: value_type for value_type, tag in _ENUM_TO_TAG.items()}


class MarketPayloadCodecError(ValueError):
    pass


class MarketPayloadSecretError(MarketPayloadCodecError):
    pass


def _is_sensitive_key(name: str) -> bool:
    normalized = name.casefold().replace("-", "_").replace(" ", "_")
    return any(part in normalized for part in _SENSITIVE_KEY_PARTS)


def _check_text(value: str) -> None:
    if _SENSITIVE_VALUE_MARKERS.search(value):
        raise MarketPayloadSecretError("market payload contains a secret-bearing marker")


def _encode(value: object, stack: set[int]) -> object:
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, Enum):
        tag = _ENUM_TO_TAG.get(type(value))
        if tag is None:
            raise MarketPayloadCodecError(
                f"unsupported market payload enum: {type(value).__name__}"
            )
        _check_text(str(value.value))
        return {"$enum": tag, "value": value.value}
    if isinstance(value, str):
        _check_text(value)
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise MarketPayloadCodecError("market payload decimals must be finite")
        return {"$decimal": str(value)}
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise MarketPayloadCodecError("market payload datetimes must be timezone-aware")
        return {"$datetime": value.isoformat(timespec="microseconds")}
    if isinstance(value, date):
        return {"$date": value.isoformat()}
    if isinstance(value, float):
        raise MarketPayloadCodecError("normalized market payloads cannot contain floats")
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise MarketPayloadCodecError("arbitrary byte payloads are not supported")

    marker = id(value)
    if marker in stack:
        raise MarketPayloadCodecError("market payload contains a cycle")
    stack.add(marker)
    try:
        if is_dataclass(value):
            value_type = type(value)
            tag = _TYPE_TO_TAG.get(value_type)
            if tag is None:
                raise MarketPayloadCodecError(
                    f"unsupported market payload dataclass: {value_type.__name__}"
                )
            encoded_fields: dict[str, object] = {}
            for field in fields(value):
                if _is_sensitive_key(field.name):
                    raise MarketPayloadSecretError(
                        "market payload dataclass contains a sensitive field"
                    )
                encoded_fields[field.name] = _encode(getattr(value, field.name), stack)
            return {"$dataclass": tag, "fields": encoded_fields}
        if isinstance(value, Mapping):
            encoded_items: list[list[object]] = []
            for key, item in value.items():
                if not isinstance(key, str):
                    raise MarketPayloadCodecError("market payload mapping keys must be text")
                if _is_sensitive_key(key):
                    raise MarketPayloadSecretError(
                        "market payload contains a sensitive mapping key"
                    )
                _check_text(key)
                encoded_items.append([key, _encode(item, stack)])
            encoded_items.sort(key=lambda pair: pair[0])
            return {"$mapping": encoded_items}
        if isinstance(value, tuple):
            return {"$tuple": [_encode(item, stack) for item in value]}
        if isinstance(value, list):
            return {"$list": [_encode(item, stack) for item in value]}
    finally:
        stack.remove(marker)
    raise MarketPayloadCodecError(f"unsupported market payload type: {type(value).__name__}")


def encode_market_payload(value: object) -> bytes:
    encoded = {
        "codec_version": MARKET_PAYLOAD_CODEC_VERSION,
        "payload": _encode(value, set()),
    }
    return json.dumps(
        encoded,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _decode(value: object) -> object:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, list):
        raise MarketPayloadCodecError("untagged market payload list")
    if not isinstance(value, dict):
        raise MarketPayloadCodecError("invalid market payload value")

    if set(value) == {"$decimal"}:
        try:
            parsed = Decimal(str(value["$decimal"]))
        except (InvalidOperation, ValueError) as exc:
            raise MarketPayloadCodecError("invalid encoded decimal") from exc
        if not parsed.is_finite():
            raise MarketPayloadCodecError("encoded decimal must be finite")
        return parsed
    if set(value) == {"$datetime"}:
        try:
            parsed_datetime = datetime.fromisoformat(str(value["$datetime"]))
        except ValueError as exc:
            raise MarketPayloadCodecError("invalid encoded datetime") from exc
        if parsed_datetime.tzinfo is None or parsed_datetime.utcoffset() is None:
            raise MarketPayloadCodecError("encoded datetime must be timezone-aware")
        return parsed_datetime
    if set(value) == {"$date"}:
        try:
            return date.fromisoformat(str(value["$date"]))
        except ValueError as exc:
            raise MarketPayloadCodecError("invalid encoded date") from exc
    if set(value) == {"$enum", "value"}:
        tag = value["$enum"]
        raw_value = value["value"]
        if not isinstance(tag, str) or not isinstance(raw_value, str):
            raise MarketPayloadCodecError("invalid encoded enum")
        value_type = _TAG_TO_ENUM.get(tag)
        if value_type is None:
            raise MarketPayloadCodecError("unsupported encoded enum")
        try:
            return value_type(raw_value)
        except ValueError as exc:
            raise MarketPayloadCodecError("invalid encoded enum value") from exc
    if set(value) == {"$tuple"}:
        items = value["$tuple"]
        if not isinstance(items, list):
            raise MarketPayloadCodecError("invalid encoded tuple")
        return tuple(_decode(item) for item in items)
    if set(value) == {"$list"}:
        items = value["$list"]
        if not isinstance(items, list):
            raise MarketPayloadCodecError("invalid encoded list")
        return [_decode(item) for item in items]
    if set(value) == {"$mapping"}:
        items = value["$mapping"]
        if not isinstance(items, list):
            raise MarketPayloadCodecError("invalid encoded mapping")
        decoded: dict[str, object] = {}
        for pair in items:
            if not isinstance(pair, list) or len(pair) != 2 or not isinstance(pair[0], str):
                raise MarketPayloadCodecError("invalid encoded mapping entry")
            key = pair[0]
            if key in decoded or _is_sensitive_key(key):
                raise MarketPayloadCodecError("invalid or sensitive encoded mapping key")
            decoded[key] = _decode(pair[1])
        return decoded
    if set(value) == {"$dataclass", "fields"}:
        tag = value["$dataclass"]
        encoded_fields = value["fields"]
        if not isinstance(tag, str) or not isinstance(encoded_fields, dict):
            raise MarketPayloadCodecError("invalid encoded dataclass")
        value_type = _TAG_TO_TYPE.get(tag)
        if value_type is None:
            raise MarketPayloadCodecError("unsupported encoded dataclass")
        dataclass_fields = fields(value_type)
        expected_fields = {field.name for field in dataclass_fields}
        if set(encoded_fields) != expected_fields:
            raise MarketPayloadCodecError("encoded dataclass fields do not match its schema")
        decoded_fields = {name: _decode(item) for name, item in encoded_fields.items()}
        try:
            reconstructed = value_type(
                **{
                    field.name: decoded_fields[field.name]
                    for field in dataclass_fields
                    if field.init
                }
            )
        except (TypeError, ValueError) as exc:
            raise MarketPayloadCodecError("encoded dataclass violates its schema") from exc
        if any(
            getattr(reconstructed, field.name) != decoded_fields[field.name]
            for field in dataclass_fields
            if not field.init
        ):
            raise MarketPayloadCodecError(
                "encoded dataclass derived identity does not match its content"
            )
        return reconstructed
    raise MarketPayloadCodecError("unknown market payload tag")


def decode_market_payload(payload: bytes) -> object:
    try:
        envelope = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MarketPayloadCodecError("market payload is not valid UTF-8 JSON") from exc
    if not isinstance(envelope, dict) or set(envelope) != {"codec_version", "payload"}:
        raise MarketPayloadCodecError("invalid market payload envelope")
    if envelope["codec_version"] != MARKET_PAYLOAD_CODEC_VERSION:
        raise MarketPayloadCodecError("unsupported market payload codec")
    return _decode(envelope["payload"])


def market_payload_record_count(value: object) -> int:
    if isinstance(value, InstrumentBatch):
        return len(value.instruments)
    if isinstance(value, DailyCandleArchive):
        return len(value.batch.candles)
    if isinstance(value, DailyCandleBatch):
        return len(value.candles)
    if isinstance(value, HistoricalDailyCandleBatch):
        return value.record_count
    if isinstance(value, HistoricalCandleReconciliationReport):
        return value.record_count
    if isinstance(value, Mapping):
        records = value.get("records")
        if isinstance(records, (list, tuple)):
            return len(records)
    raise MarketPayloadCodecError(
        "record count must be derivable from a supported typed payload or records list"
    )
