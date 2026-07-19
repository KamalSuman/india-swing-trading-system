from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal

from .models import (
    CollectionShadowCandidate,
    CollectionShadowScanResult,
    ShadowScanError,
    ShadowScanStatus,
)


SHADOW_SCAN_CODEC_VERSION = "collection-shadow-scan-json/v1"


class ShadowScanCodecError(ShadowScanError):
    pass


def _candidate(value: CollectionShadowCandidate) -> dict[str, object]:
    value.verify_content_identity()
    return {
        "bar_ids": list(value.bar_ids),
        "candidate_id": value.candidate_id,
        "current_close": str(value.current_close),
        "evidence_ids": list(value.evidence_ids),
        "financial_instrument_id": value.financial_instrument_id,
        "lookback_return_pct": str(value.lookback_return_pct),
        "lookback_sessions": [item.isoformat() for item in value.lookback_sessions],
        "market_session": value.market_session.isoformat(),
        "median_daily_traded_value": str(value.median_daily_traded_value),
        "median_daily_volume": str(value.median_daily_volume),
        "median_delivery_percent": str(value.median_delivery_percent),
        "positive_session_fraction": str(value.positive_session_fraction),
        "schema_version": value.schema_version,
        "series": value.series,
        "symbol": value.symbol,
        "tick_size_rupees": str(value.tick_size_rupees),
        "validated_isin": value.validated_isin,
        "warnings": list(value.warnings),
    }


def encode_shadow_scan_result(value: CollectionShadowScanResult) -> bytes:
    if type(value) is not CollectionShadowScanResult:
        raise TypeError("shadow scan result must be exact")
    value.verify_content_identity()
    payload = {
        "codec_schema_version": SHADOW_SCAN_CODEC_VERSION,
        "result": {
            "actionable": value.actionable,
            "blockers": list(value.blockers),
            "candidates": [_candidate(item) for item in value.candidates],
            "config_id": value.config_id,
            "cutoff": value.cutoff.isoformat(),
            "derived_evidence_id": value.derived_evidence_id,
            "exclusion_counts": [list(item) for item in value.exclusion_counts],
            "historical_price_artifact_ids": list(
                value.historical_price_artifact_ids
            ),
            "liquidity_snapshot_id": value.liquidity_snapshot_id,
            "market_session": value.market_session.isoformat(),
            "mode": value.mode,
            "result_id": value.result_id,
            "schema_version": value.schema_version,
            "status": value.status.value,
            "tick_size_snapshot_id": value.tick_size_snapshot_id,
            "universe_snapshot_id": value.universe_snapshot_id,
        },
    }
    return (
        json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ShadowScanCodecError("shadow scan JSON contains duplicate keys")
        value[key] = item
    return value


def _exact(value: object, keys: set[str], name: str) -> dict[str, object]:
    if type(value) is not dict or set(value) != keys:
        raise ShadowScanCodecError(f"stored {name} fields are invalid")
    return value


def _decimal_text(value: object, name: str) -> Decimal:
    if type(value) is not str:
        raise ShadowScanCodecError(f"stored {name} must be decimal text")
    try:
        return Decimal(value)
    except Exception:
        raise ShadowScanCodecError(f"stored {name} is invalid") from None


def _decode_candidate(raw: object) -> CollectionShadowCandidate:
    value = _exact(
        raw,
        {
            "bar_ids",
            "candidate_id",
            "current_close",
            "evidence_ids",
            "financial_instrument_id",
            "lookback_return_pct",
            "lookback_sessions",
            "market_session",
            "median_daily_traded_value",
            "median_daily_volume",
            "median_delivery_percent",
            "positive_session_fraction",
            "schema_version",
            "series",
            "symbol",
            "tick_size_rupees",
            "validated_isin",
            "warnings",
        },
        "candidate",
    )
    result = CollectionShadowCandidate(
        market_session=date.fromisoformat(value["market_session"]),
        symbol=value["symbol"],
        series=value["series"],
        validated_isin=value["validated_isin"],
        financial_instrument_id=value["financial_instrument_id"],
        current_close=_decimal_text(value["current_close"], "current_close"),
        tick_size_rupees=_decimal_text(
            value["tick_size_rupees"], "tick_size_rupees"
        ),
        lookback_sessions=tuple(
            date.fromisoformat(item) for item in value["lookback_sessions"]
        ),
        bar_ids=tuple(value["bar_ids"]),
        lookback_return_pct=_decimal_text(
            value["lookback_return_pct"], "lookback_return_pct"
        ),
        positive_session_fraction=_decimal_text(
            value["positive_session_fraction"], "positive_session_fraction"
        ),
        median_daily_traded_value=_decimal_text(
            value["median_daily_traded_value"], "median_daily_traded_value"
        ),
        median_daily_volume=_decimal_text(
            value["median_daily_volume"], "median_daily_volume"
        ),
        median_delivery_percent=_decimal_text(
            value["median_delivery_percent"], "median_delivery_percent"
        ),
        evidence_ids=tuple(value["evidence_ids"]),
        warnings=tuple(value["warnings"]),
        schema_version=value["schema_version"],
    )
    if result.candidate_id != value["candidate_id"]:
        raise ShadowScanCodecError("stored candidate identity differs")
    return result


def decode_shadow_scan_result(payload: bytes) -> CollectionShadowScanResult:
    try:
        raw = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        root = _exact(
            raw,
            {"codec_schema_version", "result"},
            "scan envelope",
        )
        if root["codec_schema_version"] != SHADOW_SCAN_CODEC_VERSION:
            raise ShadowScanCodecError("unsupported shadow scan codec")
        value = _exact(
            root["result"],
            {
                "actionable",
                "blockers",
                "candidates",
                "config_id",
                "cutoff",
                "derived_evidence_id",
                "exclusion_counts",
                "historical_price_artifact_ids",
                "liquidity_snapshot_id",
                "market_session",
                "mode",
                "result_id",
                "schema_version",
                "status",
                "tick_size_snapshot_id",
                "universe_snapshot_id",
            },
            "scan result",
        )
        if type(value["candidates"]) is not list:
            raise ShadowScanCodecError("stored candidates must be a list")
        result = CollectionShadowScanResult(
            market_session=date.fromisoformat(value["market_session"]),
            cutoff=datetime.fromisoformat(value["cutoff"]),
            derived_evidence_id=value["derived_evidence_id"],
            universe_snapshot_id=value["universe_snapshot_id"],
            liquidity_snapshot_id=value["liquidity_snapshot_id"],
            tick_size_snapshot_id=value["tick_size_snapshot_id"],
            historical_price_artifact_ids=tuple(
                value["historical_price_artifact_ids"]
            ),
            config_id=value["config_id"],
            candidates=tuple(
                _decode_candidate(item) for item in value["candidates"]
            ),
            exclusion_counts=tuple(
                tuple(item) for item in value["exclusion_counts"]
            ),
            blockers=tuple(value["blockers"]),
            status=ShadowScanStatus(value["status"]),
            mode=value["mode"],
            actionable=value["actionable"],
            schema_version=value["schema_version"],
        )
        if result.result_id != value["result_id"]:
            raise ShadowScanCodecError("stored scan result identity differs")
        return result
    except ShadowScanCodecError:
        raise
    except Exception:
        raise ShadowScanCodecError("stored shadow scan is invalid") from None
