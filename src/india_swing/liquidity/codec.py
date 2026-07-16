from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal

from india_swing.reference.models import ReferenceReadiness

from .models import (
    LIQUIDITY_CODEC_VERSION,
    CollectedLiquidityObservation,
    CollectionLiquiditySnapshot,
    LiquidityIntegrityError,
    LiquiditySourceSession,
)


def encode_liquidity_snapshot(value: CollectionLiquiditySnapshot) -> bytes:
    if type(value) is not CollectionLiquiditySnapshot:
        raise TypeError("liquidity snapshot must be exact")
    value.verify_content_identity()
    payload = {
        "actionable": value.actionable,
        "codec_version": LIQUIDITY_CODEC_VERSION,
        "decision_cutoff": value.decision_cutoff.isoformat(),
        "minimum_history_sessions": value.minimum_history_sessions,
        "observations": [
            {
                "bar_ids": list(item.bar_ids),
                "candidate_id": item.candidate_id,
                "median_daily_traded_value": str(item.median_daily_traded_value),
                "median_daily_volume": str(item.median_daily_volume),
                "median_delivery_percent": (
                    str(item.median_delivery_percent)
                    if item.median_delivery_percent is not None
                    else None
                ),
                "minimum_history_sessions": item.minimum_history_sessions,
                "observation_id": item.observation_id,
                "observed_sessions": [
                    session.isoformat() for session in item.observed_sessions
                ],
                "schema_version": item.schema_version,
                "series": item.series,
                "supplied_session_count": item.supplied_session_count,
                "symbols": list(item.symbols),
                "validated_isin": item.validated_isin,
            }
            for item in value.observations
        ],
        "policy_version": value.policy_version,
        "readiness": value.readiness.value,
        "reason_codes": list(value.reason_codes),
        "schema_version": value.schema_version,
        "snapshot_id": value.snapshot_id,
        "source_sessions": [
            {
                "artifact_id": item.artifact_id,
                "binding_id": item.binding_id,
                "cutoff": item.cutoff.isoformat(),
                "knowledge_time": item.knowledge_time.isoformat(),
                "market_session": item.market_session.isoformat(),
                "schema_version": item.schema_version,
            }
            for item in value.source_sessions
        ],
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
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise LiquidityIntegrityError("liquidity JSON contains a duplicate key")
        result[key] = value
    return result


def decode_liquidity_snapshot(payload: bytes) -> CollectionLiquiditySnapshot:
    root_fields = {
        "actionable",
        "codec_version",
        "decision_cutoff",
        "minimum_history_sessions",
        "observations",
        "policy_version",
        "readiness",
        "reason_codes",
        "schema_version",
        "snapshot_id",
        "source_sessions",
    }
    source_fields = {
        "artifact_id",
        "binding_id",
        "cutoff",
        "knowledge_time",
        "market_session",
        "schema_version",
    }
    observation_fields = {
        "bar_ids",
        "candidate_id",
        "median_daily_traded_value",
        "median_daily_volume",
        "median_delivery_percent",
        "minimum_history_sessions",
        "observation_id",
        "observed_sessions",
        "schema_version",
        "series",
        "supplied_session_count",
        "symbols",
        "validated_isin",
    }
    try:
        raw = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        if type(raw) is not dict or set(raw) != root_fields:
            raise LiquidityIntegrityError("liquidity payload has invalid fields")
        if raw["codec_version"] != LIQUIDITY_CODEC_VERSION:
            raise LiquidityIntegrityError("unsupported liquidity codec")
        sources = []
        for item in raw["source_sessions"]:
            if type(item) is not dict or set(item) != source_fields:
                raise LiquidityIntegrityError("liquidity source has invalid fields")
            source = LiquiditySourceSession(
                market_session=date.fromisoformat(item["market_session"]),
                artifact_id=item["artifact_id"],
                cutoff=datetime.fromisoformat(item["cutoff"]),
                knowledge_time=datetime.fromisoformat(item["knowledge_time"]),
                schema_version=item["schema_version"],
            )
            if source.binding_id != item["binding_id"]:
                raise LiquidityIntegrityError("liquidity source identity differs")
            sources.append(source)
        observations = []
        for item in raw["observations"]:
            if type(item) is not dict or set(item) != observation_fields:
                raise LiquidityIntegrityError(
                    "liquidity observation has invalid fields"
                )
            observation = CollectedLiquidityObservation(
                candidate_id=item["candidate_id"],
                validated_isin=item["validated_isin"],
                series=item["series"],
                symbols=tuple(item["symbols"]),
                observed_sessions=tuple(
                    date.fromisoformat(value) for value in item["observed_sessions"]
                ),
                bar_ids=tuple(item["bar_ids"]),
                supplied_session_count=item["supplied_session_count"],
                minimum_history_sessions=item["minimum_history_sessions"],
                median_daily_traded_value=Decimal(
                    item["median_daily_traded_value"]
                ),
                median_daily_volume=Decimal(item["median_daily_volume"]),
                median_delivery_percent=(
                    Decimal(item["median_delivery_percent"])
                    if item["median_delivery_percent"] is not None
                    else None
                ),
                schema_version=item["schema_version"],
            )
            if observation.observation_id != item["observation_id"]:
                raise LiquidityIntegrityError(
                    "liquidity observation identity differs"
                )
            observations.append(observation)
        result = CollectionLiquiditySnapshot(
            decision_cutoff=datetime.fromisoformat(raw["decision_cutoff"]),
            minimum_history_sessions=raw["minimum_history_sessions"],
            source_sessions=tuple(sources),
            observations=tuple(observations),
            reason_codes=tuple(raw["reason_codes"]),
            readiness=ReferenceReadiness(raw["readiness"]),
            actionable=raw["actionable"],
            policy_version=raw["policy_version"],
            schema_version=raw["schema_version"],
        )
        if result.snapshot_id != raw["snapshot_id"]:
            raise LiquidityIntegrityError("liquidity snapshot identity differs")
        return result
    except LiquidityIntegrityError:
        raise
    except (
        json.JSONDecodeError,
        KeyError,
        TypeError,
        UnicodeDecodeError,
        ValueError,
    ) as exc:
        raise LiquidityIntegrityError("liquidity payload is invalid") from exc
