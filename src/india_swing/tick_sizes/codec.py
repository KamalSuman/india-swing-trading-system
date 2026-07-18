from __future__ import annotations

import json
from datetime import date, datetime

from india_swing.reference.models import ReferenceReadiness

from .models import (
    TICK_SIZE_CODEC_VERSION,
    CollectedTickSizeObservation,
    CollectionTickSizeSnapshot,
    TickSizeIntegrityError,
)


def encode_tick_size_snapshot(value: CollectionTickSizeSnapshot) -> bytes:
    if type(value) is not CollectionTickSizeSnapshot:
        raise TypeError("tick-size snapshot must be exact")
    value.verify_content_identity()
    payload = {
        "actionable": value.actionable,
        "codec_version": TICK_SIZE_CODEC_VERSION,
        "cutoff": value.cutoff.isoformat(),
        "knowledge_time": value.knowledge_time.isoformat(),
        "market_session_claim": value.market_session_claim.isoformat(),
        "observations": [
            {
                "bid_interval_paise": item.bid_interval_paise,
                "financial_instrument_id": item.financial_instrument_id,
                "knowledge_time": item.knowledge_time.isoformat(),
                "market_session_claim": item.market_session_claim.isoformat(),
                "observation_id": item.observation_id,
                "schema_version": item.schema_version,
                "series": item.series,
                "source_artifact_id": item.source_artifact_id,
                "source_manifest_id": item.source_manifest_id,
                "source_record_id": item.source_record_id,
                "symbol": item.symbol,
                "validated_isin": item.validated_isin,
            }
            for item in value.observations
        ],
        "policy_version": value.policy_version,
        "readiness": value.readiness.value,
        "reason_codes": list(value.reason_codes),
        "schema_version": value.schema_version,
        "snapshot_id": value.snapshot_id,
        "source_artifact_id": value.source_artifact_id,
        "source_manifest_id": value.source_manifest_id,
        "source_normalized_sha256": value.source_normalized_sha256,
        "source_raw_sha256": value.source_raw_sha256,
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
            raise TickSizeIntegrityError("tick-size payload has a duplicate JSON key")
        result[key] = value
    return result


def decode_tick_size_snapshot(payload: bytes) -> CollectionTickSizeSnapshot:
    expected = {
        "actionable",
        "codec_version",
        "cutoff",
        "knowledge_time",
        "market_session_claim",
        "observations",
        "policy_version",
        "readiness",
        "reason_codes",
        "schema_version",
        "snapshot_id",
        "source_artifact_id",
        "source_manifest_id",
        "source_normalized_sha256",
        "source_raw_sha256",
    }
    observation_fields = {
        "bid_interval_paise",
        "financial_instrument_id",
        "knowledge_time",
        "market_session_claim",
        "observation_id",
        "schema_version",
        "series",
        "source_artifact_id",
        "source_manifest_id",
        "source_record_id",
        "symbol",
        "validated_isin",
    }
    try:
        raw = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        if type(raw) is not dict or set(raw) != expected:
            raise TickSizeIntegrityError("tick-size payload has invalid fields")
        if raw["codec_version"] != TICK_SIZE_CODEC_VERSION:
            raise TickSizeIntegrityError("unsupported tick-size codec")
        if type(raw["observations"]) is not list:
            raise TickSizeIntegrityError("tick-size observations must be a list")
        observations = []
        for item in raw["observations"]:
            if type(item) is not dict or set(item) != observation_fields:
                raise TickSizeIntegrityError("tick-size observation has invalid fields")
            observation = CollectedTickSizeObservation(
                market_session_claim=date.fromisoformat(item["market_session_claim"]),
                knowledge_time=datetime.fromisoformat(item["knowledge_time"]),
                source_artifact_id=item["source_artifact_id"],
                source_manifest_id=item["source_manifest_id"],
                source_record_id=item["source_record_id"],
                financial_instrument_id=item["financial_instrument_id"],
                symbol=item["symbol"],
                series=item["series"],
                validated_isin=item["validated_isin"],
                bid_interval_paise=item["bid_interval_paise"],
                schema_version=item["schema_version"],
            )
            if observation.observation_id != item["observation_id"]:
                raise TickSizeIntegrityError("tick-size observation ID differs")
            observations.append(observation)
        result = CollectionTickSizeSnapshot(
            market_session_claim=date.fromisoformat(raw["market_session_claim"]),
            cutoff=datetime.fromisoformat(raw["cutoff"]),
            knowledge_time=datetime.fromisoformat(raw["knowledge_time"]),
            source_artifact_id=raw["source_artifact_id"],
            source_manifest_id=raw["source_manifest_id"],
            source_raw_sha256=raw["source_raw_sha256"],
            source_normalized_sha256=raw["source_normalized_sha256"],
            observations=tuple(observations),
            reason_codes=tuple(raw["reason_codes"]),
            readiness=ReferenceReadiness(raw["readiness"]),
            actionable=raw["actionable"],
            policy_version=raw["policy_version"],
            schema_version=raw["schema_version"],
        )
        if result.snapshot_id != raw["snapshot_id"]:
            raise TickSizeIntegrityError("tick-size snapshot ID differs")
        return result
    except TickSizeIntegrityError:
        raise
    except (
        json.JSONDecodeError,
        KeyError,
        TypeError,
        UnicodeDecodeError,
        ValueError,
    ) as exc:
        raise TickSizeIntegrityError("tick-size payload is invalid") from exc
