"""Canonical manifests for replayable promoted feature-input panels."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime

from india_swing.evaluation.promoted_feature_inputs import (
    PROMOTED_FEATURE_INPUT_POLICY_VERSION,
    PROMOTED_FEATURE_INPUT_SCHEMA_VERSION,
    VerifiedPromotedFeatureInputPanel,
)
from india_swing.reference.models import ReferenceReadiness


PROMOTED_FEATURE_INPUT_CODEC_VERSION = (
    "promoted-feature-input-artifact-json/v1"
)
_KIND = "PROMOTED_FEATURE_INPUT_PANEL"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAXIMUM_BYTES = 64 * 1024 * 1024


class PromotedFeatureInputCodecError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class DecodedPromotedFeatureInputRecord:
    adjustment_bridge_id: str
    tick_panel_id: str
    cutoff: datetime
    panel_id: str


def _canonical(value: dict[str, object]) -> bytes:
    return (
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _projection(
    value: VerifiedPromotedFeatureInputPanel,
) -> dict[str, object]:
    return {
        "schema_version": value.schema_version,
        "policy_version": value.policy_version,
        "knowledge_time": value.knowledge_time.isoformat(),
        "result_ids": tuple(item.result_id for item in value.results),
        "status_counts": tuple(
            tuple(item) for item in value.status_counts
        ),
        "resolved_histories_input_complete": (
            value.resolved_histories_input_complete
        ),
        "unassigned_entry_count": value.unassigned_entry_count,
        "readiness": value.readiness.value,
        "actionable": value.actionable,
        "training_eligible": value.training_eligible,
        "feature_eligible": value.feature_eligible,
        "cross_sectional_ranking_eligible": (
            value.cross_sectional_ranking_eligible
        ),
        "alert_eligible": value.alert_eligible,
        "execution_eligible": value.execution_eligible,
        "panel_id": value.panel_id,
    }


def encode_promoted_feature_input_panel(
    value: VerifiedPromotedFeatureInputPanel,
) -> bytes:
    if type(value) is not VerifiedPromotedFeatureInputPanel:
        raise TypeError("promoted feature-input panel must be exact")
    value.verify_content_identity()
    return _canonical(
        {
            "codec_schema_version": (
                PROMOTED_FEATURE_INPUT_CODEC_VERSION
            ),
            "kind": _KIND,
            "adjustment_bridge_id": (
                value.adjustment_panel.bridge_id
            ),
            "tick_panel_id": value.tick_panel.panel_id,
            "cutoff": value.cutoff.isoformat(),
            "panel": _projection(value),
        }
    )


def _unique_object(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise PromotedFeatureInputCodecError(
                "promoted feature-input artifact contains duplicate keys"
            )
        result[key] = value
    return result


def _reject_number(_: str) -> object:
    raise PromotedFeatureInputCodecError(
        "promoted feature-input artifact contains a forbidden number"
    )


def _object(
    value: object,
    expected: set[str],
) -> dict[str, object]:
    if type(value) is not dict or set(value) != expected:
        raise PromotedFeatureInputCodecError(
            "promoted feature-input artifact fields are invalid"
        )
    return value


def _sha(value: object) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise PromotedFeatureInputCodecError(
            "promoted feature-input artifact ID is invalid"
        )
    return value


def _datetime(value: object) -> datetime:
    if type(value) is not str:
        raise PromotedFeatureInputCodecError(
            "promoted feature-input artifact time is invalid"
        )
    try:
        result = datetime.fromisoformat(value)
        offset = result.utcoffset()
    except Exception:
        raise PromotedFeatureInputCodecError(
            "promoted feature-input artifact time is invalid"
        ) from None
    if (
        result.tzinfo is None
        or offset is None
        or result.isoformat() != value
    ):
        raise PromotedFeatureInputCodecError(
            "promoted feature-input artifact time is invalid"
        )
    return result


def _sequence(value: object) -> tuple[object, ...]:
    if type(value) is not list:
        raise PromotedFeatureInputCodecError(
            "promoted feature-input artifact sequence is invalid"
        )
    return tuple(value)


def _validate_projection(
    value: object,
) -> str:
    raw = _object(
        value,
        {
            "schema_version",
            "policy_version",
            "knowledge_time",
            "result_ids",
            "status_counts",
            "resolved_histories_input_complete",
            "unassigned_entry_count",
            "readiness",
            "actionable",
            "training_eligible",
            "feature_eligible",
            "cross_sectional_ranking_eligible",
            "alert_eligible",
            "execution_eligible",
            "panel_id",
        },
    )
    result_ids = _sequence(raw["result_ids"])
    status_counts = _sequence(raw["status_counts"])
    flags = (
        raw["resolved_histories_input_complete"],
        raw["actionable"],
        raw["training_eligible"],
        raw["feature_eligible"],
        raw["cross_sectional_ranking_eligible"],
        raw["alert_eligible"],
        raw["execution_eligible"],
    )
    if (
        raw["schema_version"]
        != PROMOTED_FEATURE_INPUT_SCHEMA_VERSION
        or raw["policy_version"]
        != PROMOTED_FEATURE_INPUT_POLICY_VERSION
        or any(
            type(item) is not str
            or _SHA256.fullmatch(item) is None
            for item in result_ids
        )
        or any(
            type(item) is not list
            or len(item) != 2
            or type(item[0]) is not str
            or type(item[1]) is not int
            or item[1] < 0
            for item in status_counts
        )
        or any(type(item) is not bool for item in flags)
        or type(raw["unassigned_entry_count"]) is not int
        or raw["unassigned_entry_count"] < 0
        or raw["readiness"] != ReferenceReadiness.COLLECTION_ONLY.value
        or any(
            item is not False
            for item in (
                raw["actionable"],
                raw["training_eligible"],
                raw["feature_eligible"],
                raw["cross_sectional_ranking_eligible"],
                raw["alert_eligible"],
                raw["execution_eligible"],
            )
        )
    ):
        raise PromotedFeatureInputCodecError(
            "promoted feature-input projection is invalid"
        )
    _datetime(raw["knowledge_time"])
    return _sha(raw["panel_id"])


def decode_promoted_feature_input_record(
    payload: bytes,
) -> DecodedPromotedFeatureInputRecord:
    if (
        type(payload) is not bytes
        or not payload
        or len(payload) > _MAXIMUM_BYTES
    ):
        raise PromotedFeatureInputCodecError(
            "promoted feature-input artifact bytes are invalid"
        )
    try:
        decoded = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
        raw = _object(
            decoded,
            {
                "codec_schema_version",
                "kind",
                "adjustment_bridge_id",
                "tick_panel_id",
                "cutoff",
                "panel",
            },
        )
        if (
            raw["codec_schema_version"]
            != PROMOTED_FEATURE_INPUT_CODEC_VERSION
            or raw["kind"] != _KIND
        ):
            raise PromotedFeatureInputCodecError(
                "promoted feature-input artifact kind is invalid"
            )
        panel_id = _validate_projection(raw["panel"])
        return DecodedPromotedFeatureInputRecord(
            adjustment_bridge_id=_sha(
                raw["adjustment_bridge_id"]
            ),
            tick_panel_id=_sha(raw["tick_panel_id"]),
            cutoff=_datetime(raw["cutoff"]),
            panel_id=panel_id,
        )
    except PromotedFeatureInputCodecError:
        raise
    except Exception:
        raise PromotedFeatureInputCodecError(
            "promoted feature-input artifact is invalid"
        ) from None
