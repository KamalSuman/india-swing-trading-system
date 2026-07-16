from __future__ import annotations

import json
from datetime import date, datetime

from india_swing.reference.models import ReferenceReadiness

from .models import (
    PromotionCapability,
    PromotionDecision,
    PromotionEvidence,
    PromotionIntegrityError,
    PromotionStage,
)


PROMOTION_CODEC_SCHEMA_VERSION = "promotion-normalized-json/v1"


class PromotionCodecError(PromotionIntegrityError):
    pass


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise PromotionCodecError("promotion payload contains a duplicate JSON key")
        result[key] = value
    return result


def _keys(value: object, expected: set[str], name: str) -> dict[str, object]:
    if type(value) is not dict or set(value) != expected:
        raise PromotionCodecError(f"stored {name} has invalid fields")
    return value


def _evidence_value(value: PromotionEvidence) -> dict[str, object]:
    return {
        "actionable": value.actionable,
        "capability": value.capability.value,
        "complete": value.complete,
        "coverage_end": value.coverage_end.isoformat(),
        "coverage_start": value.coverage_start.isoformat(),
        "cutoff": value.cutoff.isoformat(),
        "evidence_id": value.evidence_id,
        "readiness": value.readiness.value,
        "reason_codes": list(value.reason_codes),
        "schema_version": value.schema_version,
        "source_snapshot_ids": list(value.source_snapshot_ids),
    }


def encode_promotion_decision(value: PromotionDecision) -> bytes:
    if type(value) is not PromotionDecision:
        raise TypeError("promotion decision must be exact")
    value.verify_content_identity()
    payload = {
        "codec_schema_version": PROMOTION_CODEC_SCHEMA_VERSION,
        "decision": {
            "achieved_stage": value.achieved_stage.value,
            "alert_blockers": list(value.alert_blockers),
            "backtest_blockers": list(value.backtest_blockers),
            "decision_cutoff": value.decision_cutoff.isoformat(),
            "decision_id": value.decision_id,
            "evidence": [_evidence_value(item) for item in value.evidence],
            "history_start": value.history_start.isoformat(),
            "market_session": value.market_session.isoformat(),
            "policy_version": value.policy_version,
            "research_blockers": list(value.research_blockers),
            "schema_version": value.schema_version,
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


def _decode_evidence(raw: object) -> PromotionEvidence:
    value = _keys(
        raw,
        {
            "actionable",
            "capability",
            "complete",
            "coverage_end",
            "coverage_start",
            "cutoff",
            "evidence_id",
            "readiness",
            "reason_codes",
            "schema_version",
            "source_snapshot_ids",
        },
        "promotion evidence",
    )
    result = PromotionEvidence(
        capability=PromotionCapability(value["capability"]),
        cutoff=datetime.fromisoformat(value["cutoff"]),
        coverage_start=date.fromisoformat(value["coverage_start"]),
        coverage_end=date.fromisoformat(value["coverage_end"]),
        source_snapshot_ids=tuple(value["source_snapshot_ids"]),
        readiness=ReferenceReadiness(value["readiness"]),
        complete=value["complete"],
        actionable=value["actionable"],
        reason_codes=tuple(value["reason_codes"]),
        schema_version=value["schema_version"],
    )
    if result.evidence_id != value["evidence_id"]:
        raise PromotionCodecError("stored promotion evidence identity differs")
    return result


def decode_promotion_decision(payload: bytes) -> PromotionDecision:
    try:
        raw = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        root = _keys(
            raw,
            {"codec_schema_version", "decision"},
            "promotion envelope",
        )
        if root["codec_schema_version"] != PROMOTION_CODEC_SCHEMA_VERSION:
            raise PromotionCodecError("unsupported promotion codec schema")
        value = _keys(
            root["decision"],
            {
                "achieved_stage",
                "alert_blockers",
                "backtest_blockers",
                "decision_cutoff",
                "decision_id",
                "evidence",
                "history_start",
                "market_session",
                "policy_version",
                "research_blockers",
                "schema_version",
            },
            "promotion decision",
        )
        if type(value["evidence"]) is not list:
            raise PromotionCodecError("stored promotion evidence must be a list")
        result = PromotionDecision(
            market_session=date.fromisoformat(value["market_session"]),
            history_start=date.fromisoformat(value["history_start"]),
            decision_cutoff=datetime.fromisoformat(value["decision_cutoff"]),
            evidence=tuple(_decode_evidence(item) for item in value["evidence"]),
            achieved_stage=PromotionStage(value["achieved_stage"]),
            research_blockers=tuple(value["research_blockers"]),
            backtest_blockers=tuple(value["backtest_blockers"]),
            alert_blockers=tuple(value["alert_blockers"]),
            policy_version=value["policy_version"],
            schema_version=value["schema_version"],
        )
        if result.decision_id != value["decision_id"]:
            raise PromotionCodecError("stored promotion decision identity differs")
        return result
    except PromotionCodecError:
        raise
    except PromotionIntegrityError as exc:
        raise PromotionCodecError("stored promotion decision violates invariants") from exc
    except (
        json.JSONDecodeError,
        KeyError,
        TypeError,
        UnicodeDecodeError,
        ValueError,
    ) as exc:
        raise PromotionCodecError("stored promotion decision is invalid") from exc
