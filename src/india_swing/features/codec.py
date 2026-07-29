"""Canonical manifests for replayable promoted feature artifacts."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from india_swing.features.promoted_cross_section import (
    PROMOTED_CROSS_SECTION_POLICY_VERSION,
    PROMOTED_CROSS_SECTION_SCHEMA_VERSION,
    PromotedCrossSectionConfig,
    VerifiedPromotedCrossSectionPanel,
)
from india_swing.features.promoted_technical import (
    PROMOTED_TECHNICAL_FEATURE_POLICY_VERSION,
    PROMOTED_TECHNICAL_FEATURE_SCHEMA_VERSION,
    PromotedTechnicalFeatureConfig,
    VerifiedPromotedTechnicalFeaturePanel,
)
from india_swing.forecasting.regime_ensemble import (
    AlphaRegimeWeighting,
    MarketRegime,
)
from india_swing.reference.models import ReferenceReadiness


class PromotedFeatureCodecError(ValueError):
    pass


PROMOTED_FEATURE_ARTIFACT_CODEC_VERSION = "promoted-feature-artifact-json/v1"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_TECHNICAL_KIND = "TECHNICAL_FEATURE_PANEL"
_CROSS_SECTION_KIND = "CROSS_SECTION_PANEL"


@dataclass(frozen=True, slots=True)
class DecodedTechnicalFeatureRecord:
    source_panel_id: str
    config: PromotedTechnicalFeatureConfig
    cutoff: datetime
    panel_id: str


@dataclass(frozen=True, slots=True)
class DecodedCrossSectionRecord:
    source_panel_id: str
    config: PromotedCrossSectionConfig
    cutoff: datetime
    panel_id: str


def _canonical(value: dict[str, object]) -> bytes:
    return (
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _technical_config(
    value: PromotedTechnicalFeatureConfig,
) -> dict[str, object]:
    value.verify_content_identity()
    return {
        "annualization_sessions": value.annualization_sessions,
        "atr_sessions": value.atr_sessions,
        "breakout_sessions": value.breakout_sessions,
        "config_id": value.config_id,
        "contraction_long_sessions": value.contraction_long_sessions,
        "contraction_short_sessions": value.contraction_short_sessions,
        "drawdown_sessions": value.drawdown_sessions,
        "liquidity_sessions": value.liquidity_sessions,
        "long_return_sessions": value.long_return_sessions,
        "long_trend_sessions": value.long_trend_sessions,
        "medium_return_sessions": value.medium_return_sessions,
        "minimum_history_sessions": value.minimum_history_sessions,
        "policy_version": value.policy_version,
        "schema_version": value.schema_version,
        "short_return_sessions": value.short_return_sessions,
        "short_trend_sessions": value.short_trend_sessions,
        "tick_history_sessions": value.tick_history_sessions,
        "volatility_sessions": value.volatility_sessions,
    }


def _weighting(value: AlphaRegimeWeighting) -> dict[str, object]:
    value.verify_content_identity()
    return {
        "liquidity_quality": str(value.liquidity_quality),
        "momentum_breakout": str(value.momentum_breakout),
        "pullback_continuation": str(value.pullback_continuation),
        "regime": value.regime.value,
        "volatility_contraction": str(value.volatility_contraction),
        "weighting_id": value.weighting_id,
    }


def _cross_config(value: PromotedCrossSectionConfig) -> dict[str, object]:
    value.verify_content_identity()
    return {
        "breakout_reference_distance": str(
            value.breakout_reference_distance
        ),
        "config_id": value.config_id,
        "contraction_zero_quality_ratio": str(
            value.contraction_zero_quality_ratio
        ),
        "high_volatility_threshold": str(value.high_volatility_threshold),
        "ideal_pullback_depth": str(value.ideal_pullback_depth),
        "minimum_computed_instruments": value.minimum_computed_instruments,
        "policy_version": value.policy_version,
        "pullback_tolerance": str(value.pullback_tolerance),
        "risk_off_breadth_threshold": str(value.risk_off_breadth_threshold),
        "schema_version": value.schema_version,
        "trend_gate_distance": str(value.trend_gate_distance),
        "trending_breadth_threshold": str(
            value.trending_breadth_threshold
        ),
        "trending_momentum_threshold": str(
            value.trending_momentum_threshold
        ),
        "volume_ratio_full_confirmation": str(
            value.volume_ratio_full_confirmation
        ),
        "weightings": [_weighting(item) for item in value.weightings],
    }


def _panel_projection(
    value: VerifiedPromotedTechnicalFeaturePanel,
) -> dict[str, object]:
    return {
        "actionable": value.actionable,
        "alert_eligible": value.alert_eligible,
        "blocked_history_count": value.blocked_history_count,
        "computed_history_count": value.computed_history_count,
        "cross_sectional_ranking_eligible": (
            value.cross_sectional_ranking_eligible
        ),
        "execution_eligible": value.execution_eligible,
        "feature_eligible": value.feature_eligible,
        "knowledge_time": value.knowledge_time.isoformat(),
        "panel_id": value.panel_id,
        "policy_version": value.policy_version,
        "readiness": value.readiness.value,
        "resolved_histories_feature_complete": (
            value.resolved_histories_feature_complete
        ),
        "result_ids": [item.result_id for item in value.results],
        "status_counts": [list(item) for item in value.status_counts],
        "schema_version": value.schema_version,
        "training_eligible": value.training_eligible,
        "unassigned_entry_count": value.unassigned_entry_count,
    }


def _cross_projection(
    value: VerifiedPromotedCrossSectionPanel,
) -> dict[str, object]:
    return {
        "actionable": value.actionable,
        "alert_eligible": value.alert_eligible,
        "blocked_history_count": value.blocked_history_count,
        "execution_eligible": value.execution_eligible,
        "feature_eligible": value.feature_eligible,
        "knowledge_time": value.knowledge_time.isoformat(),
        "orphan_bar_count": value.orphan_bar_count,
        "panel_id": value.panel_id,
        "policy_version": value.policy_version,
        "ranking_eligible": value.ranking_eligible,
        "readiness": value.readiness.value,
        "regime_evidence_id": (
            None
            if value.regime_evidence is None
            else value.regime_evidence.evidence_id
        ),
        "resolved_histories_scoring_complete": (
            value.resolved_histories_scoring_complete
        ),
        "result_ids": [item.result_id for item in value.results],
        "scored_history_count": value.scored_history_count,
        "source_universe_cross_section_complete": (
            value.source_universe_cross_section_complete
        ),
        "status_counts": [list(item) for item in value.status_counts],
        "schema_version": value.schema_version,
        "training_eligible": value.training_eligible,
        "unassigned_entry_count": value.unassigned_entry_count,
    }


def encode_technical_feature_panel(
    value: VerifiedPromotedTechnicalFeaturePanel,
) -> bytes:
    if type(value) is not VerifiedPromotedTechnicalFeaturePanel:
        raise TypeError("technical feature panel must be exact")
    value.verify_content_identity()
    return _canonical(
        {
            "codec_schema_version": PROMOTED_FEATURE_ARTIFACT_CODEC_VERSION,
            "config": _technical_config(value.config),
            "cutoff": value.cutoff.isoformat(),
            "kind": _TECHNICAL_KIND,
            "panel": _panel_projection(value),
            "source_panel_id": value.source_panel.panel_id,
        }
    )


def encode_cross_section_panel(
    value: VerifiedPromotedCrossSectionPanel,
) -> bytes:
    if type(value) is not VerifiedPromotedCrossSectionPanel:
        raise TypeError("cross-section panel must be exact")
    value.verify_content_identity()
    return _canonical(
        {
            "codec_schema_version": PROMOTED_FEATURE_ARTIFACT_CODEC_VERSION,
            "config": _cross_config(value.config),
            "cutoff": value.cutoff.isoformat(),
            "kind": _CROSS_SECTION_KIND,
            "panel": _cross_projection(value),
            "source_panel_id": value.source_panel.panel_id,
        }
    )


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise PromotedFeatureCodecError(
                "promoted feature artifact contains duplicate keys"
            )
        result[key] = value
    return result


def _exact(
    value: object,
    keys: set[str],
    name: str,
) -> dict[str, object]:
    if type(value) is not dict or set(value) != keys:
        raise PromotedFeatureCodecError(
            f"stored promoted feature {name} fields are invalid"
        )
    return value


def _decimal(value: object, name: str) -> Decimal:
    if type(value) is not str:
        raise PromotedFeatureCodecError(
            f"stored promoted feature {name} must be decimal text"
        )
    try:
        result = Decimal(value)
    except Exception:
        raise PromotedFeatureCodecError(
            f"stored promoted feature {name} is invalid"
        ) from None
    if not result.is_finite():
        raise PromotedFeatureCodecError(
            f"stored promoted feature {name} is invalid"
        )
    return result


def _sha(value: object, name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise PromotedFeatureCodecError(
            f"stored promoted feature {name} is invalid"
        )
    return value


def _integer(value: object, name: str) -> int:
    if type(value) is not int or value < 0:
        raise PromotedFeatureCodecError(
            f"stored promoted feature {name} is invalid"
        )
    return value


def _datetime(value: object, name: str) -> datetime:
    if type(value) is not str:
        raise PromotedFeatureCodecError(
            f"stored promoted feature {name} is invalid"
        )
    try:
        result = datetime.fromisoformat(value)
        offset = result.utcoffset()
    except Exception:
        raise PromotedFeatureCodecError(
            f"stored promoted feature {name} is invalid"
        ) from None
    if result.tzinfo is None or offset is None:
        raise PromotedFeatureCodecError(
            f"stored promoted feature {name} is invalid"
        )
    return result


def _root(payload: bytes, kind: str) -> dict[str, object]:
    if type(payload) is not bytes or not payload:
        raise PromotedFeatureCodecError(
            "stored promoted feature artifact is invalid"
        )
    try:
        raw = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        result = _exact(
            raw,
            {
                "codec_schema_version",
                "config",
                "cutoff",
                "kind",
                "panel",
                "source_panel_id",
            },
            "envelope",
        )
        if (
            result["codec_schema_version"]
            != PROMOTED_FEATURE_ARTIFACT_CODEC_VERSION
            or result["kind"] != kind
        ):
            raise PromotedFeatureCodecError(
                "stored promoted feature artifact kind is invalid"
            )
        return result
    except PromotedFeatureCodecError:
        raise
    except Exception:
        raise PromotedFeatureCodecError(
            "stored promoted feature artifact is invalid"
        ) from None


_TECHNICAL_CONFIG_KEYS = {
    "annualization_sessions",
    "atr_sessions",
    "breakout_sessions",
    "config_id",
    "contraction_long_sessions",
    "contraction_short_sessions",
    "drawdown_sessions",
    "liquidity_sessions",
    "long_return_sessions",
    "long_trend_sessions",
    "medium_return_sessions",
    "minimum_history_sessions",
    "policy_version",
    "schema_version",
    "short_return_sessions",
    "short_trend_sessions",
    "tick_history_sessions",
    "volatility_sessions",
}

_TECHNICAL_PANEL_KEYS = {
    "actionable",
    "alert_eligible",
    "blocked_history_count",
    "computed_history_count",
    "cross_sectional_ranking_eligible",
    "execution_eligible",
    "feature_eligible",
    "knowledge_time",
    "panel_id",
    "policy_version",
    "readiness",
    "resolved_histories_feature_complete",
    "result_ids",
    "status_counts",
    "schema_version",
    "training_eligible",
    "unassigned_entry_count",
}


def _decode_technical_config(raw: object) -> PromotedTechnicalFeatureConfig:
    value = _exact(raw, _TECHNICAL_CONFIG_KEYS, "technical config")
    integer_names = _TECHNICAL_CONFIG_KEYS - {
        "config_id",
        "policy_version",
        "schema_version",
    }
    kwargs = {
        name: _integer(value[name], name)
        for name in integer_names
    }
    if any(kwargs[name] <= 0 for name in kwargs):
        raise PromotedFeatureCodecError(
            "stored promoted feature technical config is invalid"
        )
    try:
        result = PromotedTechnicalFeatureConfig(
            **kwargs,
            policy_version=value["policy_version"],
            schema_version=value["schema_version"],
        )
    except Exception:
        raise PromotedFeatureCodecError(
            "stored promoted feature technical config is invalid"
        ) from None
    if result.config_id != _sha(value["config_id"], "config_id"):
        raise PromotedFeatureCodecError(
            "stored promoted feature technical config identity differs"
        )
    return result


_CROSS_CONFIG_KEYS = {
    "breakout_reference_distance",
    "config_id",
    "contraction_zero_quality_ratio",
    "high_volatility_threshold",
    "ideal_pullback_depth",
    "minimum_computed_instruments",
    "policy_version",
    "pullback_tolerance",
    "risk_off_breadth_threshold",
    "schema_version",
    "trend_gate_distance",
    "trending_breadth_threshold",
    "trending_momentum_threshold",
    "volume_ratio_full_confirmation",
    "weightings",
}

_WEIGHTING_KEYS = {
    "liquidity_quality",
    "momentum_breakout",
    "pullback_continuation",
    "regime",
    "volatility_contraction",
    "weighting_id",
}

_CROSS_PANEL_KEYS = {
    "actionable",
    "alert_eligible",
    "blocked_history_count",
    "execution_eligible",
    "feature_eligible",
    "knowledge_time",
    "orphan_bar_count",
    "panel_id",
    "policy_version",
    "ranking_eligible",
    "readiness",
    "regime_evidence_id",
    "resolved_histories_scoring_complete",
    "result_ids",
    "scored_history_count",
    "source_universe_cross_section_complete",
    "status_counts",
    "schema_version",
    "training_eligible",
    "unassigned_entry_count",
}


def _decode_weighting(raw: object) -> AlphaRegimeWeighting:
    value = _exact(raw, _WEIGHTING_KEYS, "regime weighting")
    try:
        result = AlphaRegimeWeighting(
            regime=MarketRegime(value["regime"]),
            momentum_breakout=_decimal(
                value["momentum_breakout"],
                "momentum_breakout",
            ),
            pullback_continuation=_decimal(
                value["pullback_continuation"],
                "pullback_continuation",
            ),
            volatility_contraction=_decimal(
                value["volatility_contraction"],
                "volatility_contraction",
            ),
            liquidity_quality=_decimal(
                value["liquidity_quality"],
                "liquidity_quality",
            ),
        )
    except PromotedFeatureCodecError:
        raise
    except Exception:
        raise PromotedFeatureCodecError(
            "stored promoted feature regime weighting is invalid"
        ) from None
    if result.weighting_id != _sha(value["weighting_id"], "weighting_id"):
        raise PromotedFeatureCodecError(
            "stored promoted feature regime weighting identity differs"
        )
    return result


def _decode_cross_config(raw: object) -> PromotedCrossSectionConfig:
    value = _exact(raw, _CROSS_CONFIG_KEYS, "cross-section config")
    if type(value["weightings"]) is not list:
        raise PromotedFeatureCodecError(
            "stored promoted feature weightings are invalid"
        )
    try:
        result = PromotedCrossSectionConfig(
            minimum_computed_instruments=_integer(
                value["minimum_computed_instruments"],
                "minimum_computed_instruments",
            ),
            trending_breadth_threshold=_decimal(
                value["trending_breadth_threshold"],
                "trending_breadth_threshold",
            ),
            trending_momentum_threshold=_decimal(
                value["trending_momentum_threshold"],
                "trending_momentum_threshold",
            ),
            risk_off_breadth_threshold=_decimal(
                value["risk_off_breadth_threshold"],
                "risk_off_breadth_threshold",
            ),
            high_volatility_threshold=_decimal(
                value["high_volatility_threshold"],
                "high_volatility_threshold",
            ),
            trend_gate_distance=_decimal(
                value["trend_gate_distance"],
                "trend_gate_distance",
            ),
            ideal_pullback_depth=_decimal(
                value["ideal_pullback_depth"],
                "ideal_pullback_depth",
            ),
            pullback_tolerance=_decimal(
                value["pullback_tolerance"],
                "pullback_tolerance",
            ),
            breakout_reference_distance=_decimal(
                value["breakout_reference_distance"],
                "breakout_reference_distance",
            ),
            volume_ratio_full_confirmation=_decimal(
                value["volume_ratio_full_confirmation"],
                "volume_ratio_full_confirmation",
            ),
            contraction_zero_quality_ratio=_decimal(
                value["contraction_zero_quality_ratio"],
                "contraction_zero_quality_ratio",
            ),
            weightings=tuple(
                _decode_weighting(item) for item in value["weightings"]
            ),
            schema_version=value["schema_version"],
            policy_version=value["policy_version"],
        )
    except PromotedFeatureCodecError:
        raise
    except Exception:
        raise PromotedFeatureCodecError(
            "stored promoted feature cross-section config is invalid"
        ) from None
    if result.config_id != _sha(value["config_id"], "config_id"):
        raise PromotedFeatureCodecError(
            "stored promoted feature cross-section config identity differs"
        )
    return result


def _validate_projection(
    raw: object,
    keys: set[str],
    panel_id: str,
) -> None:
    value = _exact(raw, keys, "panel projection")
    if _sha(value["panel_id"], "panel_id") != panel_id:
        raise PromotedFeatureCodecError(
            "stored promoted feature panel identity differs"
        )
    _datetime(value["knowledge_time"], "knowledge_time")
    if type(value["result_ids"]) is not list or any(
        _SHA256.fullmatch(item) is None
        for item in value["result_ids"]
        if type(item) is str
    ) or any(type(item) is not str for item in value["result_ids"]):
        raise PromotedFeatureCodecError(
            "stored promoted feature result identities are invalid"
        )
    if len(set(value["result_ids"])) != len(value["result_ids"]):
        raise PromotedFeatureCodecError(
            "stored promoted feature result identities are invalid"
        )
    if type(value["status_counts"]) is not list or any(
        type(item) is not list
        or len(item) != 2
        or type(item[0]) is not str
        or type(item[1]) is not int
        for item in value["status_counts"]
    ):
        raise PromotedFeatureCodecError(
            "stored promoted feature status counts are invalid"
        )
    if (
        value["status_counts"]
        != sorted(value["status_counts"], key=lambda item: item[0])
        or len({item[0] for item in value["status_counts"]})
        != len(value["status_counts"])
        or any(item[1] < 0 for item in value["status_counts"])
    ):
        raise PromotedFeatureCodecError(
            "stored promoted feature status counts are invalid"
        )


def _validate_technical_projection(value: dict[str, object]) -> None:
    boolean_names = (
        "actionable",
        "alert_eligible",
        "cross_sectional_ranking_eligible",
        "execution_eligible",
        "feature_eligible",
        "resolved_histories_feature_complete",
        "training_eligible",
    )
    count_names = (
        "blocked_history_count",
        "computed_history_count",
        "unassigned_entry_count",
    )
    if (
        any(type(value[name]) is not bool for name in boolean_names)
        or any(
            type(value[name]) is not int or value[name] < 0
            for name in count_names
        )
        or any(
            type(value[name]) is not str or not value[name]
            for name in ("policy_version", "readiness", "schema_version")
        )
        or value["policy_version"]
        != PROMOTED_TECHNICAL_FEATURE_POLICY_VERSION
        or value["schema_version"]
        != PROMOTED_TECHNICAL_FEATURE_SCHEMA_VERSION
        or value["readiness"] != ReferenceReadiness.COLLECTION_ONLY.value
        or any(
            value[name] is not False
            for name in (
                "actionable",
                "alert_eligible",
                "cross_sectional_ranking_eligible",
                "execution_eligible",
                "feature_eligible",
                "training_eligible",
            )
        )
    ):
        raise PromotedFeatureCodecError(
            "stored promoted technical feature projection is invalid"
        )
    result_count = len(value["result_ids"])
    if (
        value["blocked_history_count"] + value["computed_history_count"]
        != result_count
        or sum(item[1] for item in value["status_counts"]) != result_count
        or value["resolved_histories_feature_complete"]
        is not (result_count > 0 and value["computed_history_count"] == result_count)
    ):
        raise PromotedFeatureCodecError(
            "stored promoted technical feature projection is inconsistent"
        )


def _validate_cross_projection(value: dict[str, object]) -> None:
    boolean_names = (
        "actionable",
        "alert_eligible",
        "execution_eligible",
        "feature_eligible",
        "ranking_eligible",
        "resolved_histories_scoring_complete",
        "source_universe_cross_section_complete",
        "training_eligible",
    )
    count_names = (
        "blocked_history_count",
        "orphan_bar_count",
        "scored_history_count",
        "unassigned_entry_count",
    )
    regime_id = value["regime_evidence_id"]
    if (
        any(type(value[name]) is not bool for name in boolean_names)
        or any(
            type(value[name]) is not int or value[name] < 0
            for name in count_names
        )
        or any(
            type(value[name]) is not str or not value[name]
            for name in ("policy_version", "readiness", "schema_version")
        )
        or value["policy_version"] != PROMOTED_CROSS_SECTION_POLICY_VERSION
        or value["schema_version"] != PROMOTED_CROSS_SECTION_SCHEMA_VERSION
        or value["readiness"] != ReferenceReadiness.COLLECTION_ONLY.value
        or any(
            value[name] is not False
            for name in (
                "actionable",
                "alert_eligible",
                "execution_eligible",
                "feature_eligible",
                "ranking_eligible",
                "training_eligible",
            )
        )
        or (
            regime_id is not None
            and (
                type(regime_id) is not str
                or _SHA256.fullmatch(regime_id) is None
            )
        )
    ):
        raise PromotedFeatureCodecError(
            "stored promoted cross-section projection is invalid"
        )
    result_count = len(value["result_ids"])
    if (
        value["blocked_history_count"] + value["scored_history_count"]
        != result_count
        or sum(item[1] for item in value["status_counts"]) != result_count
        or (
            value["regime_evidence_id"] is None
            and value["scored_history_count"] > 0
        )
        or (
            value["regime_evidence_id"] is not None
            and value["scored_history_count"] == 0
        )
        or value["resolved_histories_scoring_complete"]
        is not (result_count > 0 and value["scored_history_count"] == result_count)
        or (
            value["source_universe_cross_section_complete"]
            and (
                not value["resolved_histories_scoring_complete"]
                or value["unassigned_entry_count"] != 0
                or value["orphan_bar_count"] != 0
            )
        )
    ):
        raise PromotedFeatureCodecError(
            "stored promoted cross-section projection is inconsistent"
        )


def decode_technical_feature_record(
    payload: bytes,
) -> DecodedTechnicalFeatureRecord:
    try:
        root = _root(payload, _TECHNICAL_KIND)
        source_panel_id = _sha(root["source_panel_id"], "source_panel_id")
        config = _decode_technical_config(root["config"])
        cutoff = _datetime(root["cutoff"], "cutoff")
        panel_raw = _exact(
            root["panel"],
            _TECHNICAL_PANEL_KEYS,
            "technical panel",
        )
        panel_id = _sha(panel_raw["panel_id"], "panel_id")
        _validate_projection(panel_raw, _TECHNICAL_PANEL_KEYS, panel_id)
        _validate_technical_projection(panel_raw)
        return DecodedTechnicalFeatureRecord(
            source_panel_id=source_panel_id,
            config=config,
            cutoff=cutoff,
            panel_id=panel_id,
        )
    except PromotedFeatureCodecError:
        raise
    except Exception:
        raise PromotedFeatureCodecError(
            "stored promoted technical feature artifact is invalid"
        ) from None


def decode_cross_section_record(
    payload: bytes,
) -> DecodedCrossSectionRecord:
    try:
        root = _root(payload, _CROSS_SECTION_KIND)
        source_panel_id = _sha(root["source_panel_id"], "source_panel_id")
        config = _decode_cross_config(root["config"])
        cutoff = _datetime(root["cutoff"], "cutoff")
        panel_raw = _exact(
            root["panel"],
            _CROSS_PANEL_KEYS,
            "cross-section panel",
        )
        panel_id = _sha(panel_raw["panel_id"], "panel_id")
        _validate_projection(panel_raw, _CROSS_PANEL_KEYS, panel_id)
        _validate_cross_projection(panel_raw)
        return DecodedCrossSectionRecord(
            source_panel_id=source_panel_id,
            config=config,
            cutoff=cutoff,
            panel_id=panel_id,
        )
    except PromotedFeatureCodecError:
        raise
    except Exception:
        raise PromotedFeatureCodecError(
            "stored promoted cross-section artifact is invalid"
        ) from None
