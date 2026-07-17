from __future__ import annotations

import json
from datetime import date, datetime

from india_swing.reference.models import ReferenceReadiness

from .models import (
    COLLECTION_UNIVERSE_CODEC_VERSION,
    CollectedUniverseObservation,
    CollectionUniverseDisposition,
    CollectionUniverseIntegrityError,
    CollectionUniverseSnapshot,
)


def encode_collection_universe_snapshot(
    value: CollectionUniverseSnapshot,
) -> bytes:
    if type(value) is not CollectionUniverseSnapshot:
        raise TypeError("universe snapshot must be exact")
    value.verify_content_identity()
    payload = {
        "actionable": value.actionable,
        "calendar_snapshot_id": value.calendar_snapshot_id,
        "codec_version": COLLECTION_UNIVERSE_CODEC_VERSION,
        "cutoff": value.cutoff.isoformat(),
        "knowledge_time": value.knowledge_time.isoformat(),
        "market_session_claim": value.market_session_claim.isoformat(),
        "observations": [
            {
                "delete_flag": item.delete_flag,
                "disposition": item.disposition.value,
                "financial_instrument_id": item.financial_instrument_id,
                "included_in_broad_equity_scope": (
                    item.included_in_broad_equity_scope
                ),
                "knowledge_time": item.knowledge_time.isoformat(),
                "listing_timestamp": item.listing_timestamp,
                "market_session_claim": item.market_session_claim.isoformat(),
                "normal_market_eligible": item.normal_market_eligible,
                "normal_market_status": item.normal_market_status,
                "observation_id": item.observation_id,
                "permitted_to_trade": item.permitted_to_trade,
                "readmission_timestamp": item.readmission_timestamp,
                "removal_timestamp": item.removal_timestamp,
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
            raise CollectionUniverseIntegrityError(
                "universe payload has a duplicate JSON key"
            )
        result[key] = value
    return result


def decode_collection_universe_snapshot(
    payload: bytes,
) -> CollectionUniverseSnapshot:
    expected = {
        "actionable",
        "calendar_snapshot_id",
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
        "delete_flag",
        "disposition",
        "financial_instrument_id",
        "included_in_broad_equity_scope",
        "knowledge_time",
        "listing_timestamp",
        "market_session_claim",
        "normal_market_eligible",
        "normal_market_status",
        "observation_id",
        "permitted_to_trade",
        "readmission_timestamp",
        "removal_timestamp",
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
            raise CollectionUniverseIntegrityError(
                "universe payload has invalid fields"
            )
        if raw["codec_version"] != COLLECTION_UNIVERSE_CODEC_VERSION:
            raise CollectionUniverseIntegrityError("unsupported universe codec")
        if type(raw["observations"]) is not list:
            raise CollectionUniverseIntegrityError(
                "universe observations must be a list"
            )
        observations = []
        for item in raw["observations"]:
            if type(item) is not dict or set(item) != observation_fields:
                raise CollectionUniverseIntegrityError(
                    "universe observation has invalid fields"
                )
            observation = CollectedUniverseObservation(
                market_session_claim=date.fromisoformat(
                    item["market_session_claim"]
                ),
                knowledge_time=datetime.fromisoformat(item["knowledge_time"]),
                source_artifact_id=item["source_artifact_id"],
                source_manifest_id=item["source_manifest_id"],
                source_record_id=item["source_record_id"],
                financial_instrument_id=item["financial_instrument_id"],
                symbol=item["symbol"],
                series=item["series"],
                validated_isin=item["validated_isin"],
                disposition=CollectionUniverseDisposition(item["disposition"]),
                included_in_broad_equity_scope=(
                    item["included_in_broad_equity_scope"]
                ),
                permitted_to_trade=item["permitted_to_trade"],
                normal_market_status=item["normal_market_status"],
                normal_market_eligible=item["normal_market_eligible"],
                delete_flag=item["delete_flag"],
                listing_timestamp=item["listing_timestamp"],
                removal_timestamp=item["removal_timestamp"],
                readmission_timestamp=item["readmission_timestamp"],
                schema_version=item["schema_version"],
            )
            if observation.observation_id != item["observation_id"]:
                raise CollectionUniverseIntegrityError(
                    "universe observation ID differs"
                )
            observations.append(observation)
        result = CollectionUniverseSnapshot(
            market_session_claim=date.fromisoformat(raw["market_session_claim"]),
            cutoff=datetime.fromisoformat(raw["cutoff"]),
            knowledge_time=datetime.fromisoformat(raw["knowledge_time"]),
            calendar_snapshot_id=raw["calendar_snapshot_id"],
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
            raise CollectionUniverseIntegrityError("universe snapshot ID differs")
        return result
    except CollectionUniverseIntegrityError:
        raise
    except (
        json.JSONDecodeError,
        KeyError,
        TypeError,
        UnicodeDecodeError,
        ValueError,
    ) as exc:
        raise CollectionUniverseIntegrityError(
            "collection universe payload is invalid"
        ) from exc
