from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from os import PathLike


_SENSITIVE_ATTRIBUTE_PARTS = (
    "api_key",
    "apikey",
    "authorization",
    "credential",
    "password",
    "private_key",
    "secret",
    "access_token",
    "refresh_token",
    "request_token",
    "session_token",
    "auth_token",
)


def _type_name(value: object) -> str:
    value_type = type(value)
    return f"{value_type.__module__}.{value_type.__qualname__}"


def _is_sensitive_attribute(name: str) -> bool:
    normalized = name.casefold().replace("-", "_")
    return any(part in normalized for part in _SENSITIVE_ATTRIBUTE_PARTS)


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _identity_value(value: object, stack: set[int]) -> object:
    """Return a deterministic JSON value without addresses or secret attributes."""

    if isinstance(value, Enum):
        return {"$enum": _type_name(value), "value": _identity_value(value.value, stack)}
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, Decimal):
        return {"$decimal": str(value)}
    if isinstance(value, float):
        return {"$float": value.hex()}
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("identity datetimes must be timezone-aware")
        normalized = value.astimezone(timezone.utc)
        return {"$datetime": normalized.isoformat(timespec="microseconds")}
    if isinstance(value, date):
        return {"$date": value.isoformat()}
    if isinstance(value, bytes):
        return {"$bytes_sha256": hashlib.sha256(value).hexdigest(), "length": len(value)}
    if isinstance(value, PathLike):
        return {"$path": str(value)}
    if isinstance(value, type):
        return {"$type": f"{value.__module__}.{value.__qualname__}"}

    marker = id(value)
    if marker in stack:
        return {"$cycle": _type_name(value)}
    stack.add(marker)
    try:
        if is_dataclass(value):
            safe_fields = {
                field.name: _identity_value(getattr(value, field.name), stack)
                for field in fields(value)
                if not _is_sensitive_attribute(field.name)
            }
            return {"$dataclass": _type_name(value), "fields": safe_fields}
        if isinstance(value, Mapping):
            pairs = [
                (_identity_value(key, stack), _identity_value(item, stack))
                for key, item in value.items()
                if not (isinstance(key, str) and _is_sensitive_attribute(key))
            ]
            pairs.sort(key=lambda pair: _canonical_json(pair[0]))
            return {"$mapping": [[key, item] for key, item in pairs]}
        if isinstance(value, tuple):
            return {"$tuple": [_identity_value(item, stack) for item in value]}
        if isinstance(value, list):
            return {"$list": [_identity_value(item, stack) for item in value]}
        if isinstance(value, (set, frozenset)):
            items = [_identity_value(item, stack) for item in value]
            items.sort(key=_canonical_json)
            return {"$set": items}
        if callable(value):
            return {
                "$callable": f"{getattr(value, '__module__', type(value).__module__)}."
                f"{getattr(value, '__qualname__', type(value).__qualname__)}"
            }

        explicit_material = getattr(value, "identity_material", None)
        if explicit_material is not None:
            if callable(explicit_material):
                explicit_material = explicit_material()
            return {
                "$object": _type_name(value),
                "identity_material": _identity_value(explicit_material, stack),
            }

        try:
            attributes = vars(value)
        except TypeError:
            attributes = {}
        public_attributes = {
            name: item
            for name, item in attributes.items()
            if not name.startswith("_")
            and not _is_sensitive_attribute(name)
            and not callable(item)
        }
        return {
            "$object": _type_name(value),
            "attributes": _identity_value(public_attributes, stack),
        }
    finally:
        stack.remove(marker)


def canonical_identity(value: object) -> object:
    return _identity_value(value, set())


def canonical_identity_json(value: object) -> str:
    return _canonical_json(canonical_identity(value))


def content_id(material: object, length: int = 20) -> str:
    if length <= 0 or length > 64:
        raise ValueError("content ID length must be between 1 and 64")
    payload = canonical_identity_json(material).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:length]


def component_identity(component: object) -> object:
    versions: dict[str, object] = {}
    for name in ("version", "model_version", "model_name", "model_id"):
        value = getattr(component, name, None)
        if value is not None and not callable(value):
            versions[name] = value
    return {
        "component_type": _type_name(component),
        "versions": versions,
        "configuration": canonical_identity(component),
    }
