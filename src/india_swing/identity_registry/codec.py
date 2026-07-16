from __future__ import annotations

import json
from dataclasses import fields, is_dataclass
from datetime import date, datetime
from enum import Enum

from .models import (
    IDENTITY_REGISTRY_CODEC_VERSION,
    IDENTITY_REGISTRY_DATASET,
    CrossVintageIdentityRegistry,
)


def _json_value(value: object) -> object:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if is_dataclass(value):
        return {
            item.name: _json_value(getattr(value, item.name))
            for item in fields(value)
        }
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    if value is None or isinstance(value, (str, int, bool)):
        return value
    raise TypeError(f"unsupported identity-registry codec value: {type(value).__name__}")


def encode_identity_registry(registry: CrossVintageIdentityRegistry) -> bytes:
    if type(registry) is not CrossVintageIdentityRegistry:
        raise TypeError("registry must be an exact CrossVintageIdentityRegistry")
    registry.verify_content_identity()
    payload = {
        "codec_version": IDENTITY_REGISTRY_CODEC_VERSION,
        "dataset": IDENTITY_REGISTRY_DATASET,
        "registry": _json_value(registry),
    }
    return json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")

