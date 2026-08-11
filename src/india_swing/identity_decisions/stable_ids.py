"""Canonical public constructors for NSE CM stable identity IDs."""

from __future__ import annotations

import re

from india_swing.identity import content_id
from india_swing.reference_data.models import validated_isin_or_none

from .models import STABLE_INSTRUMENT_ID_SCHEME, STABLE_LISTING_ID_SCHEME


_SERIES = re.compile(r"[A-Z0-9]{1,4}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def stable_instrument_id_for_isin(validated_isin: str) -> str:
    if (
        type(validated_isin) is not str
        or validated_isin_or_none(validated_isin) != validated_isin
    ):
        raise ValueError("validated ISIN is invalid")
    return content_id(
        {
            "scheme": STABLE_INSTRUMENT_ID_SCHEME,
            "exchange": "NSE",
            "segment": "CM",
            "validated_isin": validated_isin,
        },
        length=64,
    )


def stable_listing_id_for_series(stable_instrument_id: str, series: str) -> str:
    if (
        type(stable_instrument_id) is not str
        or _SHA256.fullmatch(stable_instrument_id) is None
        or type(series) is not str
        or _SERIES.fullmatch(series) is None
    ):
        raise ValueError("stable listing identity input is invalid")
    return content_id(
        {
            "scheme": STABLE_LISTING_ID_SCHEME,
            "stable_instrument_id": stable_instrument_id,
            "exchange": "NSE",
            "segment": "CM",
            "series": series,
        },
        length=64,
    )
