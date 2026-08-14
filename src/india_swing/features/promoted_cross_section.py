"""Collection-only cross-sectional regime and opportunity scoring.

Only fully computed promoted technical-feature vectors enter the descriptive
cross-section. Blocked histories and unresolved identity evidence remain
visible. Scores are ranks, not probabilities, expected returns, selections, or
trade instructions.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_EVEN, localcontext
from enum import Enum

from india_swing.features.promoted_technical import (
    PromotedTechnicalFeatureConfig,
    PromotedTechnicalFeatureResult,
    PromotedTechnicalFeatureStatus,
    PromotedTechnicalFeatureVector,
    VerifiedPromotedTechnicalFeaturePanel,
)
from india_swing.forecasting.regime_ensemble import (
    AlphaRegimeWeighting,
    AlphaSpecialist,
    MarketRegime,
)
from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness


class PromotedCrossSectionError(ValueError):
    """Raised when the promoted cross-sectional boundary fails closed."""


PROMOTED_CROSS_SECTION_SCHEMA_VERSION = "promoted-cross-section-panel/v1"
PROMOTED_CROSS_SECTION_POLICY_VERSION = (
    "promoted-cross-section/regime-ranked-resolved-subset-v1"
)
PROMOTED_CROSS_SECTION_CONFIG_SCHEMA_VERSION = (
    "promoted-cross-section-config/v1"
)

_ZERO = Decimal("0")
_ONE = Decimal("1")
_HALF = Decimal("0.5")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_COMPONENT = re.compile(r"[a-z][a-z0-9_]{2,63}\Z")
_REASON = re.compile(r"[A-Z][A-Z0-9_]{2,127}\Z")

_ERR_TYPE = "promoted cross-section type is invalid"
_ERR_INPUT = "promoted cross-section source is invalid"
_ERR_VERIFY = "promoted cross-section source could not be verified"
_ERR_CUTOFF = "promoted cross-section cutoff is invalid"
_ERR_FUTURE = "promoted cross-section contains future-known evidence"
_ERR_CONFIG = "promoted cross-section configuration is invalid"
_ERR_GRAPH = "promoted cross-section graph is invalid"
_ERR_DERIVED = "promoted cross-section derived content is invalid"
_ERR_ID = "promoted cross-section identifier is invalid"

_COMMON_REASONS = {
    "COLLECTION_ONLY_NO_DECISION_AUTHORITY",
    "IDENTIFIERS_NOT_USED_TO_BREAK_SCORE_TIES",
    "NO_PROBABILITY_OR_EXPECTED_RETURN_CLAIM",
    "NO_TRADE_SELECTION_AUTHORITY",
}


def _utc(value: datetime) -> datetime:
    if type(value) is not datetime:
        raise PromotedCrossSectionError(_ERR_CUTOFF)
    try:
        offset = value.utcoffset()
    except Exception:
        raise PromotedCrossSectionError(_ERR_CUTOFF) from None
    if value.tzinfo is None or offset is None:
        raise PromotedCrossSectionError(_ERR_CUTOFF)
    return value.astimezone(timezone.utc)


def _finite(value: object) -> bool:
    return type(value) is Decimal and value.is_finite()


def _unit(value: object) -> bool:
    return _finite(value) and _ZERO <= value <= _ONE


def _clamp(value: Decimal) -> Decimal:
    return max(_ZERO, min(value, _ONE))


def _mean(values: tuple[Decimal, ...]) -> Decimal:
    if not values:
        raise PromotedCrossSectionError(_ERR_GRAPH)
    return sum(values, _ZERO) / Decimal(len(values))


def _median(values: tuple[Decimal, ...]) -> Decimal:
    if not values:
        raise PromotedCrossSectionError(_ERR_GRAPH)
    ordered = tuple(sorted(values))
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / Decimal("2")


def _counts(values: tuple[str, ...]) -> tuple[tuple[str, int], ...]:
    totals: dict[str, int] = {}
    for value in values:
        totals[value] = totals.get(value, 0) + 1
    return tuple(sorted(totals.items()))


def _default_weightings() -> tuple[AlphaRegimeWeighting, ...]:
    return tuple(
        sorted(
            (
                AlphaRegimeWeighting(
                    regime=MarketRegime.TRENDING,
                    momentum_breakout=Decimal("0.45"),
                    pullback_continuation=Decimal("0.25"),
                    volatility_contraction=Decimal("0.20"),
                    liquidity_quality=Decimal("0.10"),
                ),
                AlphaRegimeWeighting(
                    regime=MarketRegime.RANGE_BOUND,
                    momentum_breakout=Decimal("0.20"),
                    pullback_continuation=Decimal("0.35"),
                    volatility_contraction=Decimal("0.30"),
                    liquidity_quality=Decimal("0.15"),
                ),
                AlphaRegimeWeighting(
                    regime=MarketRegime.HIGH_VOLATILITY,
                    momentum_breakout=Decimal("0.15"),
                    pullback_continuation=Decimal("0.15"),
                    volatility_contraction=Decimal("0.25"),
                    liquidity_quality=Decimal("0.45"),
                ),
                AlphaRegimeWeighting(
                    regime=MarketRegime.RISK_OFF,
                    momentum_breakout=Decimal("0.10"),
                    pullback_continuation=Decimal("0.10"),
                    volatility_contraction=Decimal("0.20"),
                    liquidity_quality=Decimal("0.60"),
                ),
            ),
            key=lambda value: value.regime.value,
        )
    )


@dataclass(frozen=True, slots=True)
class PromotedCrossSectionConfig:
    minimum_computed_instruments: int = 20
    trending_breadth_threshold: Decimal = Decimal("0.60")
    trending_momentum_threshold: Decimal = Decimal("0.02")
    risk_off_breadth_threshold: Decimal = Decimal("0.35")
    high_volatility_threshold: Decimal = Decimal("0.35")
    trend_gate_distance: Decimal = Decimal("0.10")
    ideal_pullback_depth: Decimal = Decimal("0.04")
    pullback_tolerance: Decimal = Decimal("0.08")
    breakout_reference_distance: Decimal = Decimal("0.10")
    volume_ratio_full_confirmation: Decimal = Decimal("2")
    contraction_zero_quality_ratio: Decimal = Decimal("1.50")
    weightings: tuple[AlphaRegimeWeighting, ...] = field(
        default_factory=_default_weightings
    )
    schema_version: str = PROMOTED_CROSS_SECTION_CONFIG_SCHEMA_VERSION
    policy_version: str = PROMOTED_CROSS_SECTION_POLICY_VERSION
    config_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            type(self.minimum_computed_instruments) is not int
            or self.minimum_computed_instruments <= 0
            or not _unit(self.trending_breadth_threshold)
            or not _unit(self.risk_off_breadth_threshold)
            or not _finite(self.trending_momentum_threshold)
            or not _finite(self.high_volatility_threshold)
            or self.high_volatility_threshold <= _ZERO
            or not _finite(self.trend_gate_distance)
            or self.trend_gate_distance <= _ZERO
            or not _finite(self.ideal_pullback_depth)
            or self.ideal_pullback_depth < _ZERO
            or not _finite(self.pullback_tolerance)
            or self.pullback_tolerance <= _ZERO
            or not _finite(self.breakout_reference_distance)
            or self.breakout_reference_distance <= _ZERO
            or not _finite(self.volume_ratio_full_confirmation)
            or self.volume_ratio_full_confirmation <= _ZERO
            or not _finite(self.contraction_zero_quality_ratio)
            or self.contraction_zero_quality_ratio <= _ONE
            or self.risk_off_breadth_threshold
            >= self.trending_breadth_threshold
            or type(self.weightings) is not tuple
            or any(type(value) is not AlphaRegimeWeighting for value in self.weightings)
            or self.weightings
            != tuple(sorted(self.weightings, key=lambda value: value.regime.value))
            or {value.regime for value in self.weightings} != set(MarketRegime)
            or type(self.schema_version) is not str
            or self.schema_version != PROMOTED_CROSS_SECTION_CONFIG_SCHEMA_VERSION
            or type(self.policy_version) is not str
            or self.policy_version != PROMOTED_CROSS_SECTION_POLICY_VERSION
        ):
            raise PromotedCrossSectionError(_ERR_CONFIG)
        try:
            for value in self.weightings:
                value.verify_content_identity()
        except Exception:
            raise PromotedCrossSectionError(_ERR_CONFIG) from None
        object.__setattr__(self, "config_id", self._calculated_id())

    def _identity(self) -> dict[str, object]:
        return {
            value.name: getattr(self, value.name)
            for value in fields(self)
            if value.name != "config_id"
        }

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": PROMOTED_CROSS_SECTION_CONFIG_SCHEMA_VERSION,
                **self._identity(),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        expected = PromotedCrossSectionConfig(**self._identity())
        if self.config_id != expected.config_id:
            raise PromotedCrossSectionError(_ERR_ID)

    def weighting_for(self, regime: MarketRegime) -> AlphaRegimeWeighting:
        matches = tuple(value for value in self.weightings if value.regime is regime)
        if len(matches) != 1:
            raise PromotedCrossSectionError(_ERR_CONFIG)
        return matches[0]


_CellKey = tuple[str, str]


def _percentile_ranks(
    values: tuple[tuple[_CellKey, Decimal], ...],
    *,
    higher_is_better: bool,
) -> dict[_CellKey, Decimal]:
    """Value-based percentiles: ties are equal and IDs never break a tie."""

    if (
        type(values) is not tuple
        or not values
        or any(
            type(value) is not tuple
            or len(value) != 2
            or type(value[0]) is not tuple
            or len(value[0]) != 2
            or any(
                type(identifier) is not str
                or _SHA256.fullmatch(identifier) is None
                for identifier in value[0]
            )
            or not _finite(value[1])
            for value in values
        )
        or len({key for key, _ in values}) != len(values)
    ):
        raise PromotedCrossSectionError(_ERR_GRAPH)
    unique = tuple(sorted({value for _, value in values}))
    if len(unique) == 1:
        by_value = {unique[0]: _HALF}
    else:
        by_value = {
            value: Decimal(index) / Decimal(len(unique) - 1)
            for index, value in enumerate(unique)
        }
    if not higher_is_better:
        by_value = {value: _ONE - rank for value, rank in by_value.items()}
    return {key: by_value[value] for key, value in values}


def _rank_tiers(
    values: tuple[tuple[_CellKey, Decimal], ...],
) -> dict[_CellKey, tuple[int, int]]:
    """Dense score tiers and tie sizes derived from scores, never IDs."""

    if (
        type(values) is not tuple
        or not values
        or any(
            type(item) is not tuple
            or len(item) != 2
            or type(item[0]) is not tuple
            or len(item[0]) != 2
            or any(
                type(identifier) is not str
                or _SHA256.fullmatch(identifier) is None
                for identifier in item[0]
            )
            or not _finite(item[1])
            for item in values
        )
        or len({key for key, _ in values}) != len(values)
    ):
        raise PromotedCrossSectionError(_ERR_GRAPH)
    unique_scores = tuple(sorted({value for _, value in values}, reverse=True))
    dense_rank_by_score = {
        value: index for index, value in enumerate(unique_scores, start=1)
    }
    tie_count_by_score: dict[Decimal, int] = {}
    for _, value in values:
        tie_count_by_score[value] = tie_count_by_score.get(value, 0) + 1
    return {
        key: (dense_rank_by_score[value], tie_count_by_score[value])
        for key, value in values
    }


def _status_reasons(status: "PromotedCrossSectionResultStatus") -> tuple[str, ...]:
    specific = {
        PromotedCrossSectionResultStatus.SCORED_RESOLVED_SUBSET_COLLECTION_ONLY: {
            "RESOLVED_SUBSET_REGIME_SCORE_COMPUTED",
        },
        PromotedCrossSectionResultStatus.SOURCE_FEATURE_BLOCKED: {
            "SOURCE_TECHNICAL_FEATURE_NOT_COMPUTED",
        },
        PromotedCrossSectionResultStatus.CROSS_SECTION_TOO_SMALL_BLOCKED: {
            "MINIMUM_COMPUTED_CROSS_SECTION_NOT_MET",
        },
    }[status]
    result = tuple(sorted(_COMMON_REASONS | specific))
    if any(type(value) is not str or _REASON.fullmatch(value) is None for value in result):
        raise PromotedCrossSectionError(_ERR_GRAPH)
    return result


@dataclass(frozen=True, slots=True)
class PromotedMarketRegimeEvidence:
    source_feature_panel_id: str
    config_id: str
    regime: MarketRegime
    computed_instrument_count: int
    market_breadth: Decimal
    median_medium_momentum: Decimal
    median_annualized_volatility: Decimal
    feature_ids: tuple[str, ...]
    evidence_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            type(self.source_feature_panel_id) is not str
            or _SHA256.fullmatch(self.source_feature_panel_id) is None
            or type(self.config_id) is not str
            or _SHA256.fullmatch(self.config_id) is None
            or type(self.regime) is not MarketRegime
            or type(self.computed_instrument_count) is not int
            or self.computed_instrument_count <= 0
            or not _unit(self.market_breadth)
            or not _finite(self.median_medium_momentum)
            or not _finite(self.median_annualized_volatility)
            or self.median_annualized_volatility < _ZERO
            or type(self.feature_ids) is not tuple
            or len(self.feature_ids) != self.computed_instrument_count
            or self.feature_ids != tuple(sorted(set(self.feature_ids)))
            or any(
                type(value) is not str or _SHA256.fullmatch(value) is None
                for value in self.feature_ids
            )
        ):
            raise PromotedCrossSectionError(_ERR_GRAPH)
        object.__setattr__(self, "evidence_id", self._calculated_id())

    def _identity(self) -> dict[str, object]:
        return {
            value.name: getattr(self, value.name)
            for value in fields(self)
            if value.name != "evidence_id"
        }

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "promoted-market-regime-evidence/v1",
                **self._identity(),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        expected = PromotedMarketRegimeEvidence(**self._identity())
        if self.evidence_id != expected.evidence_id:
            raise PromotedCrossSectionError(_ERR_ID)


@dataclass(frozen=True, slots=True)
class PromotedSpecialistScore:
    specialist: AlphaSpecialist
    raw_score: Decimal
    regime_weight: Decimal
    weighted_score: Decimal
    components: tuple[tuple[str, Decimal], ...]
    score_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            type(self.specialist) is not AlphaSpecialist
            or not _unit(self.raw_score)
            or not _unit(self.regime_weight)
            or self.weighted_score != self.raw_score * self.regime_weight
            or type(self.components) is not tuple
            or not self.components
            or self.components
            != tuple(sorted(self.components, key=lambda value: value[0]))
            or len({name for name, _ in self.components}) != len(self.components)
            or any(
                type(value) is not tuple
                or len(value) != 2
                or type(value[0]) is not str
                or _COMPONENT.fullmatch(value[0]) is None
                or not _finite(value[1])
                for value in self.components
            )
        ):
            raise PromotedCrossSectionError(_ERR_GRAPH)
        object.__setattr__(self, "score_id", self._calculated_id())

    def _identity(self) -> dict[str, object]:
        return {
            value.name: getattr(self, value.name)
            for value in fields(self)
            if value.name != "score_id"
        }

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "promoted-specialist-score/v1",
                **self._identity(),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        expected = PromotedSpecialistScore(**self._identity())
        if self.score_id != expected.score_id:
            raise PromotedCrossSectionError(_ERR_ID)


@dataclass(frozen=True, slots=True)
class PromotedOpportunityScore:
    source_feature_id: str
    regime_evidence_id: str
    stable_instrument_id: str
    stable_listing_id: str
    specialist_scores: tuple[PromotedSpecialistScore, ...]
    ensemble_score: Decimal
    rank_tier: int
    tie_size: int
    opportunity_id: str = field(init=False)

    def __post_init__(self) -> None:
        expected_specialists = tuple(
            sorted(AlphaSpecialist, key=lambda value: value.value)
        )
        if (
            any(
                type(value) is not str or _SHA256.fullmatch(value) is None
                for value in (
                    self.source_feature_id,
                    self.regime_evidence_id,
                    self.stable_instrument_id,
                    self.stable_listing_id,
                )
            )
            or type(self.specialist_scores) is not tuple
            or tuple(value.specialist for value in self.specialist_scores)
            != expected_specialists
            or any(
                type(value) is not PromotedSpecialistScore
                for value in self.specialist_scores
            )
            or not _unit(self.ensemble_score)
            or self.ensemble_score
            != sum(
                (value.weighted_score for value in self.specialist_scores),
                _ZERO,
            )
            or type(self.rank_tier) is not int
            or self.rank_tier <= 0
            or type(self.tie_size) is not int
            or self.tie_size <= 0
        ):
            raise PromotedCrossSectionError(_ERR_GRAPH)
        try:
            for value in self.specialist_scores:
                value.verify_content_identity()
        except Exception:
            raise PromotedCrossSectionError(_ERR_GRAPH) from None
        object.__setattr__(self, "opportunity_id", self._calculated_id())

    def _identity(self) -> dict[str, object]:
        return {
            "source_feature_id": self.source_feature_id,
            "regime_evidence_id": self.regime_evidence_id,
            "stable_instrument_id": self.stable_instrument_id,
            "stable_listing_id": self.stable_listing_id,
            "specialist_score_ids": tuple(
                value.score_id for value in self.specialist_scores
            ),
            "ensemble_score": self.ensemble_score,
            "rank_tier": self.rank_tier,
            "tie_size": self.tie_size,
        }

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "promoted-opportunity-score/v1",
                **self._identity(),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        expected = PromotedOpportunityScore(
            source_feature_id=self.source_feature_id,
            regime_evidence_id=self.regime_evidence_id,
            stable_instrument_id=self.stable_instrument_id,
            stable_listing_id=self.stable_listing_id,
            specialist_scores=self.specialist_scores,
            ensemble_score=self.ensemble_score,
            rank_tier=self.rank_tier,
            tie_size=self.tie_size,
        )
        if self.opportunity_id != expected.opportunity_id:
            raise PromotedCrossSectionError(_ERR_ID)


class PromotedCrossSectionResultStatus(str, Enum):
    SCORED_RESOLVED_SUBSET_COLLECTION_ONLY = (
        "SCORED_RESOLVED_SUBSET_COLLECTION_ONLY"
    )
    SOURCE_FEATURE_BLOCKED = "SOURCE_FEATURE_BLOCKED"
    CROSS_SECTION_TOO_SMALL_BLOCKED = "CROSS_SECTION_TOO_SMALL_BLOCKED"


@dataclass(frozen=True, slots=True)
class PromotedCrossSectionResult:
    source_result: PromotedTechnicalFeatureResult
    status: PromotedCrossSectionResultStatus
    opportunity_score: PromotedOpportunityScore | None
    reason_codes: tuple[str, ...]
    result_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            type(self.source_result) is not PromotedTechnicalFeatureResult
            or type(self.status) is not PromotedCrossSectionResultStatus
            or (
                self.opportunity_score is not None
                and type(self.opportunity_score) is not PromotedOpportunityScore
            )
            or self.reason_codes != _status_reasons(self.status)
        ):
            raise PromotedCrossSectionError(_ERR_GRAPH)
        try:
            self.source_result.verify_content_identity()
            if self.opportunity_score is not None:
                self.opportunity_score.verify_content_identity()
        except Exception:
            raise PromotedCrossSectionError(_ERR_GRAPH) from None
        vector = self.source_result.feature_vector
        scored = (
            self.status
            is PromotedCrossSectionResultStatus.SCORED_RESOLVED_SUBSET_COLLECTION_ONLY
        )
        if scored:
            if (
                vector is None
                or self.opportunity_score is None
                or self.opportunity_score.source_feature_id != vector.feature_id
                or self.opportunity_score.stable_instrument_id
                != vector.stable_instrument_id
                or self.opportunity_score.stable_listing_id
                != vector.stable_listing_id
            ):
                raise PromotedCrossSectionError(_ERR_GRAPH)
        elif self.opportunity_score is not None:
            raise PromotedCrossSectionError(_ERR_GRAPH)
        if (
            self.status is PromotedCrossSectionResultStatus.SOURCE_FEATURE_BLOCKED
            and vector is not None
        ) or (
            self.status
            is PromotedCrossSectionResultStatus.CROSS_SECTION_TOO_SMALL_BLOCKED
            and vector is None
        ):
            raise PromotedCrossSectionError(_ERR_GRAPH)
        object.__setattr__(self, "result_id", self._calculated_id())

    def _identity(self) -> dict[str, object]:
        return {
            "source_result_id": self.source_result.result_id,
            "status": self.status,
            "opportunity_id": (
                None
                if self.opportunity_score is None
                else self.opportunity_score.opportunity_id
            ),
            "reason_codes": self.reason_codes,
        }

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "promoted-cross-section-result/v1",
                **self._identity(),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        expected = PromotedCrossSectionResult(
            source_result=self.source_result,
            status=self.status,
            opportunity_score=self.opportunity_score,
            reason_codes=self.reason_codes,
        )
        if self.result_id != expected.result_id:
            raise PromotedCrossSectionError(_ERR_ID)


@dataclass(frozen=True, slots=True)
class _ScoreParts:
    vector: PromotedTechnicalFeatureVector
    specialist_scores: tuple[PromotedSpecialistScore, ...]
    ensemble_score: Decimal


def _regime(
    vectors: tuple[PromotedTechnicalFeatureVector, ...],
    config: PromotedCrossSectionConfig,
    source_panel_id: str,
) -> PromotedMarketRegimeEvidence:
    breadth = Decimal(
        sum(value.distance_from_long_average > _ZERO for value in vectors)
    ) / Decimal(len(vectors))
    median_momentum = _median(tuple(value.return_medium for value in vectors))
    median_volatility = _median(
        tuple(value.annualized_realized_volatility for value in vectors)
    )
    if median_volatility >= config.high_volatility_threshold:
        regime = MarketRegime.HIGH_VOLATILITY
    elif (
        breadth <= config.risk_off_breadth_threshold
        and median_momentum <= _ZERO
    ):
        regime = MarketRegime.RISK_OFF
    elif (
        breadth >= config.trending_breadth_threshold
        and median_momentum >= config.trending_momentum_threshold
    ):
        regime = MarketRegime.TRENDING
    else:
        regime = MarketRegime.RANGE_BOUND
    return PromotedMarketRegimeEvidence(
        source_feature_panel_id=source_panel_id,
        config_id=config.config_id,
        regime=regime,
        computed_instrument_count=len(vectors),
        market_breadth=breadth,
        median_medium_momentum=median_momentum,
        median_annualized_volatility=median_volatility,
        feature_ids=tuple(sorted(value.feature_id for value in vectors)),
    )


def _score_parts(
    *,
    vectors: tuple[PromotedTechnicalFeatureVector, ...],
    regime_evidence: PromotedMarketRegimeEvidence,
    config: PromotedCrossSectionConfig,
) -> tuple[_ScoreParts, ...]:
    cells = tuple(
        (
            (
                value.stable_instrument_id,
                value.stable_listing_id,
            ),
            value,
        )
        for value in vectors
    )
    if len({key for key, _ in cells}) != len(vectors):
        raise PromotedCrossSectionError(_ERR_GRAPH)
    short_ranks = _percentile_ranks(
        tuple((key, vector.return_short) for key, vector in cells),
        higher_is_better=True,
    )
    long_ranks = _percentile_ranks(
        tuple((key, vector.return_long) for key, vector in cells),
        higher_is_better=True,
    )
    liquidity_ranks = _percentile_ranks(
        tuple((key, vector.median_prior_traded_value) for key, vector in cells),
        higher_is_better=True,
    )
    low_volatility_ranks = _percentile_ranks(
        tuple(
            (key, vector.annualized_realized_volatility)
            for key, vector in cells
        ),
        higher_is_better=False,
    )
    low_tick_friction_ranks = _percentile_ranks(
        tuple((key, vector.signal_tick_fraction) for key, vector in cells),
        higher_is_better=False,
    )
    weighting = config.weighting_for(regime_evidence.regime)
    output: list[_ScoreParts] = []
    for key, vector in cells:
        breakout_quality = _clamp(
            (vector.breakout_distance + config.breakout_reference_distance)
            / config.breakout_reference_distance
        )
        volume_quality = _clamp(
            vector.signal_volume_ratio / config.volume_ratio_full_confirmation
        )
        contraction_quality = _clamp(
            (
                config.contraction_zero_quality_ratio
                - vector.range_contraction_ratio
            )
            / (config.contraction_zero_quality_ratio - _HALF)
        )
        trend_gate = _clamp(
            vector.distance_from_long_average / config.trend_gate_distance
        )
        pullback_depth = max(_ZERO, -vector.breakout_distance)
        pullback_quality = _clamp(
            _ONE
            - abs(pullback_depth - config.ideal_pullback_depth)
            / config.pullback_tolerance
        )
        raw = {
            AlphaSpecialist.MOMENTUM_BREAKOUT: _clamp(
                Decimal("0.35") * short_ranks[key]
                + Decimal("0.25") * long_ranks[key]
                + Decimal("0.25") * breakout_quality
                + Decimal("0.15") * volume_quality
            ),
            AlphaSpecialist.PULLBACK_CONTINUATION: _clamp(
                trend_gate
                * (
                    Decimal("0.50") * pullback_quality
                    + Decimal("0.25") * short_ranks[key]
                    + Decimal("0.25") * volume_quality
                )
            ),
            AlphaSpecialist.VOLATILITY_CONTRACTION: _clamp(
                Decimal("0.40") * contraction_quality
                + Decimal("0.25") * breakout_quality
                + Decimal("0.20") * low_volatility_ranks[key]
                + Decimal("0.15") * volume_quality
            ),
            AlphaSpecialist.LIQUIDITY_QUALITY: _clamp(
                Decimal("0.75") * liquidity_ranks[key]
                + Decimal("0.25") * low_tick_friction_ranks[key]
            ),
        }
        components = {
            AlphaSpecialist.MOMENTUM_BREAKOUT: (
                ("breakout_quality", breakout_quality),
                ("long_return_percentile", long_ranks[key]),
                ("short_return_percentile", short_ranks[key]),
                ("volume_quality", volume_quality),
            ),
            AlphaSpecialist.PULLBACK_CONTINUATION: (
                ("pullback_quality", pullback_quality),
                ("short_return_percentile", short_ranks[key]),
                ("trend_gate", trend_gate),
                ("volume_quality", volume_quality),
            ),
            AlphaSpecialist.VOLATILITY_CONTRACTION: (
                ("breakout_quality", breakout_quality),
                ("contraction_quality", contraction_quality),
                ("low_volatility_percentile", low_volatility_ranks[key]),
                ("volume_quality", volume_quality),
            ),
            AlphaSpecialist.LIQUIDITY_QUALITY: (
                ("low_tick_friction_percentile", low_tick_friction_ranks[key]),
                ("traded_value_percentile", liquidity_ranks[key]),
            ),
        }
        specialist_scores = tuple(
            PromotedSpecialistScore(
                specialist=specialist,
                raw_score=raw[specialist],
                regime_weight=weighting.weight_for(specialist),
                weighted_score=(
                    raw[specialist] * weighting.weight_for(specialist)
                ),
                components=tuple(
                    sorted(components[specialist], key=lambda value: value[0])
                ),
            )
            for specialist in sorted(AlphaSpecialist, key=lambda value: value.value)
        )
        output.append(
            _ScoreParts(
                vector=vector,
                specialist_scores=specialist_scores,
                ensemble_score=sum(
                    (value.weighted_score for value in specialist_scores),
                    _ZERO,
                ),
            )
        )
    return tuple(output)


def _score_promoted_feature_vectors(
    *,
    vectors: tuple[PromotedTechnicalFeatureVector, ...],
    source_feature_panel_id: str,
    config: PromotedCrossSectionConfig,
    verify_vectors: bool,
) -> tuple[
    PromotedMarketRegimeEvidence | None,
    tuple[PromotedOpportunityScore, ...],
]:
    """Apply the established regime/specialist kernel to exact vectors.

    This is the shared calculation seam used by both the legacy promoted
    panel and the forward-paper graph. It performs no selection and grants
    no ranking, alert, paper-trade, notification, or execution authority.
    """

    if (
        type(vectors) is not tuple
        or any(type(value) is not PromotedTechnicalFeatureVector for value in vectors)
        or type(source_feature_panel_id) is not str
        or _SHA256.fullmatch(source_feature_panel_id) is None
        or type(config) is not PromotedCrossSectionConfig
    ):
        raise PromotedCrossSectionError(_ERR_INPUT)
    try:
        config.verify_content_identity()
        if verify_vectors:
            for value in vectors:
                value.verify_content_identity()
    except Exception:
        raise PromotedCrossSectionError(_ERR_VERIFY) from None
    keys = tuple(
        (value.stable_instrument_id, value.stable_listing_id) for value in vectors
    )
    if len(set(keys)) != len(keys):
        raise PromotedCrossSectionError(_ERR_GRAPH)
    if len(vectors) < config.minimum_computed_instruments:
        return None, ()
    try:
        with localcontext() as context:
            context.prec = 28
            context.rounding = ROUND_HALF_EVEN
            regime_evidence = _regime(vectors, config, source_feature_panel_id)
            parts = _score_parts(
                vectors=vectors,
                regime_evidence=regime_evidence,
                config=config,
            )
            tiers = _rank_tiers(
                tuple(
                    (
                        (
                            value.vector.stable_instrument_id,
                            value.vector.stable_listing_id,
                        ),
                        value.ensemble_score,
                    )
                    for value in parts
                )
            )
            opportunities = tuple(
                sorted(
                    (
                        PromotedOpportunityScore(
                            source_feature_id=value.vector.feature_id,
                            regime_evidence_id=regime_evidence.evidence_id,
                            stable_instrument_id=value.vector.stable_instrument_id,
                            stable_listing_id=value.vector.stable_listing_id,
                            specialist_scores=value.specialist_scores,
                            ensemble_score=value.ensemble_score,
                            rank_tier=tiers[
                                (
                                    value.vector.stable_instrument_id,
                                    value.vector.stable_listing_id,
                                )
                            ][0],
                            tie_size=tiers[
                                (
                                    value.vector.stable_instrument_id,
                                    value.vector.stable_listing_id,
                                )
                            ][1],
                        )
                        for value in parts
                    ),
                    key=lambda value: (
                        value.rank_tier,
                        value.stable_instrument_id,
                        value.stable_listing_id,
                    ),
                )
            )
    except PromotedCrossSectionError:
        raise
    except Exception:
        raise PromotedCrossSectionError(_ERR_GRAPH) from None
    return regime_evidence, opportunities


def score_promoted_feature_vectors(
    *,
    vectors: tuple[PromotedTechnicalFeatureVector, ...],
    source_feature_panel_id: str,
    config: PromotedCrossSectionConfig,
) -> tuple[
    PromotedMarketRegimeEvidence | None,
    tuple[PromotedOpportunityScore, ...],
]:
    """Apply the scoring kernel after independently verifying every vector."""

    return _score_promoted_feature_vectors(
        vectors=vectors,
        source_feature_panel_id=source_feature_panel_id,
        config=config,
        verify_vectors=True,
    )


@dataclass(frozen=True, slots=True)
class _CrossSectionFacts:
    cutoff: datetime
    knowledge_time: datetime
    regime_evidence: PromotedMarketRegimeEvidence | None
    results: tuple[PromotedCrossSectionResult, ...]
    status_counts: tuple[tuple[str, int], ...]
    scored_history_count: int
    blocked_history_count: int
    resolved_histories_scoring_complete: bool
    source_universe_cross_section_complete: bool
    unassigned_entry_count: int
    orphan_bar_count: int
    readiness: ReferenceReadiness
    actionable: bool
    training_eligible: bool
    feature_eligible: bool
    ranking_eligible: bool
    alert_eligible: bool
    execution_eligible: bool
    panel_id: str


def _result(
    source_result: PromotedTechnicalFeatureResult,
    status: PromotedCrossSectionResultStatus,
    opportunity_score: PromotedOpportunityScore | None,
) -> PromotedCrossSectionResult:
    return PromotedCrossSectionResult(
        source_result=source_result,
        status=status,
        opportunity_score=opportunity_score,
        reason_codes=_status_reasons(status),
    )


def _panel_identity(
    *,
    source_panel_id: str,
    config_id: str,
    facts: _CrossSectionFacts,
) -> dict[str, object]:
    return {
        "schema_version": PROMOTED_CROSS_SECTION_SCHEMA_VERSION,
        "policy_version": PROMOTED_CROSS_SECTION_POLICY_VERSION,
        "source_panel_id": source_panel_id,
        "config_id": config_id,
        "cutoff": facts.cutoff,
        "knowledge_time": facts.knowledge_time,
        "regime_evidence_id": (
            None
            if facts.regime_evidence is None
            else facts.regime_evidence.evidence_id
        ),
        "result_ids": tuple(value.result_id for value in facts.results),
        "status_counts": facts.status_counts,
        "scored_history_count": facts.scored_history_count,
        "blocked_history_count": facts.blocked_history_count,
        "resolved_histories_scoring_complete": (
            facts.resolved_histories_scoring_complete
        ),
        "source_universe_cross_section_complete": (
            facts.source_universe_cross_section_complete
        ),
        "unassigned_entry_count": facts.unassigned_entry_count,
        "orphan_bar_count": facts.orphan_bar_count,
        "readiness": facts.readiness,
        "actionable": facts.actionable,
        "training_eligible": facts.training_eligible,
        "feature_eligible": facts.feature_eligible,
        "ranking_eligible": facts.ranking_eligible,
        "alert_eligible": facts.alert_eligible,
        "execution_eligible": facts.execution_eligible,
    }


def _build_facts_exact(
    source_panel: VerifiedPromotedTechnicalFeaturePanel,
    config: PromotedCrossSectionConfig,
    cutoff: datetime,
) -> _CrossSectionFacts:
    computed = tuple(
        value
        for value in source_panel.results
        if value.status
        is PromotedTechnicalFeatureStatus.FEATURE_VECTOR_COMPUTED_COLLECTION_ONLY
        and value.feature_vector is not None
    )
    vectors = tuple(value.feature_vector for value in computed if value.feature_vector)
    regime_evidence, opportunities = score_promoted_feature_vectors(
        vectors=vectors,
        source_feature_panel_id=source_panel.panel_id,
        config=config,
    )
    enough = regime_evidence is not None
    opportunity_by_feature_id = {
        value.source_feature_id: value for value in opportunities
    }

    results: list[PromotedCrossSectionResult] = []
    for source_result in source_panel.results:
        if source_result.feature_vector is None:
            results.append(
                _result(
                    source_result,
                    PromotedCrossSectionResultStatus.SOURCE_FEATURE_BLOCKED,
                    None,
                )
            )
        elif not enough:
            results.append(
                _result(
                    source_result,
                    (
                        PromotedCrossSectionResultStatus
                        .CROSS_SECTION_TOO_SMALL_BLOCKED
                    ),
                    None,
                )
            )
        else:
            results.append(
                _result(
                    source_result,
                    (
                        PromotedCrossSectionResultStatus
                        .SCORED_RESOLVED_SUBSET_COLLECTION_ONLY
                    ),
                    opportunity_by_feature_id[source_result.feature_vector.feature_id],
                )
            )
    results_tuple = tuple(
        sorted(
            results,
            key=lambda value: (
                (
                    value.opportunity_score.rank_tier
                    if value.opportunity_score is not None
                    else 2**31
                ),
                (
                    value.opportunity_score.stable_instrument_id
                    if value.opportunity_score is not None
                    else (
                        value.source_result.source_result.source_adjustment_result
                        .source_history.stable_instrument_id
                    )
                ),
                (
                    value.opportunity_score.stable_listing_id
                    if value.opportunity_score is not None
                    else (
                        value.source_result.source_result.source_adjustment_result
                        .source_history.stable_listing_id
                    )
                ),
            ),
        )
    )
    status_counts = _counts(tuple(value.status.value for value in results_tuple))
    scored_count = len(opportunity_by_feature_id)
    blocked_count = len(results_tuple) - scored_count
    resolved_complete = bool(results_tuple) and scored_count == len(results_tuple)
    orphan_bar_count = len(
        source_panel.source_panel.adjustment_panel.source_panel.orphan_bars
    )
    source_universe_complete = (
        resolved_complete
        and source_panel.unassigned_entry_count == 0
        and orphan_bar_count == 0
    )
    provisional = _CrossSectionFacts(
        cutoff=cutoff,
        knowledge_time=source_panel.knowledge_time,
        regime_evidence=regime_evidence,
        results=results_tuple,
        status_counts=status_counts,
        scored_history_count=scored_count,
        blocked_history_count=blocked_count,
        resolved_histories_scoring_complete=resolved_complete,
        source_universe_cross_section_complete=source_universe_complete,
        unassigned_entry_count=source_panel.unassigned_entry_count,
        orphan_bar_count=orphan_bar_count,
        readiness=ReferenceReadiness.COLLECTION_ONLY,
        actionable=False,
        training_eligible=False,
        feature_eligible=False,
        ranking_eligible=False,
        alert_eligible=False,
        execution_eligible=False,
        panel_id="",
    )
    panel_id = content_id(
        _panel_identity(
            source_panel_id=source_panel.panel_id,
            config_id=config.config_id,
            facts=provisional,
        ),
        length=64,
    )
    return _CrossSectionFacts(
        cutoff=provisional.cutoff,
        knowledge_time=provisional.knowledge_time,
        regime_evidence=provisional.regime_evidence,
        results=provisional.results,
        status_counts=provisional.status_counts,
        scored_history_count=provisional.scored_history_count,
        blocked_history_count=provisional.blocked_history_count,
        resolved_histories_scoring_complete=(
            provisional.resolved_histories_scoring_complete
        ),
        source_universe_cross_section_complete=(
            provisional.source_universe_cross_section_complete
        ),
        unassigned_entry_count=provisional.unassigned_entry_count,
        orphan_bar_count=provisional.orphan_bar_count,
        readiness=provisional.readiness,
        actionable=provisional.actionable,
        training_eligible=provisional.training_eligible,
        feature_eligible=provisional.feature_eligible,
        ranking_eligible=provisional.ranking_eligible,
        alert_eligible=provisional.alert_eligible,
        execution_eligible=provisional.execution_eligible,
        panel_id=panel_id,
    )


def _build_facts(
    source_panel: VerifiedPromotedTechnicalFeaturePanel,
    config: PromotedCrossSectionConfig,
    cutoff: datetime,
) -> _CrossSectionFacts:
    if (
        type(source_panel) is not VerifiedPromotedTechnicalFeaturePanel
        or type(config) is not PromotedCrossSectionConfig
    ):
        raise PromotedCrossSectionError(_ERR_INPUT)
    cutoff = _utc(cutoff)
    try:
        source_panel.verify_content_identity()
        config.verify_content_identity()
    except Exception:
        raise PromotedCrossSectionError(_ERR_VERIFY) from None
    if cutoff < max(source_panel.cutoff, source_panel.knowledge_time):
        raise PromotedCrossSectionError(_ERR_FUTURE)
    if (
        source_panel.readiness is not ReferenceReadiness.COLLECTION_ONLY
        or source_panel.actionable is not False
        or source_panel.training_eligible is not False
        or source_panel.feature_eligible is not False
        or source_panel.cross_sectional_ranking_eligible is not False
        or source_panel.alert_eligible is not False
        or source_panel.execution_eligible is not False
    ):
        raise PromotedCrossSectionError(_ERR_INPUT)
    try:
        with localcontext() as context:
            context.prec = 28
            context.rounding = ROUND_HALF_EVEN
            return _build_facts_exact(source_panel, config, cutoff)
    except PromotedCrossSectionError:
        raise
    except Exception:
        raise PromotedCrossSectionError(_ERR_GRAPH) from None


@dataclass(frozen=True, slots=True)
class VerifiedPromotedCrossSectionPanel:
    schema_version: str
    policy_version: str
    source_panel: VerifiedPromotedTechnicalFeaturePanel
    config: PromotedCrossSectionConfig
    cutoff: datetime
    knowledge_time: datetime
    regime_evidence: PromotedMarketRegimeEvidence | None
    results: tuple[PromotedCrossSectionResult, ...]
    status_counts: tuple[tuple[str, int], ...]
    scored_history_count: int
    blocked_history_count: int
    resolved_histories_scoring_complete: bool
    source_universe_cross_section_complete: bool
    unassigned_entry_count: int
    orphan_bar_count: int
    readiness: ReferenceReadiness
    actionable: bool
    training_eligible: bool
    feature_eligible: bool
    ranking_eligible: bool
    alert_eligible: bool
    execution_eligible: bool
    panel_id: str

    def __post_init__(self) -> None:
        self.verify_content_identity()

    def verify_content_identity(self) -> None:
        if type(self) is not VerifiedPromotedCrossSectionPanel:
            raise PromotedCrossSectionError(_ERR_TYPE)
        if (
            type(self.schema_version) is not str
            or self.schema_version != PROMOTED_CROSS_SECTION_SCHEMA_VERSION
            or type(self.policy_version) is not str
            or self.policy_version != PROMOTED_CROSS_SECTION_POLICY_VERSION
            or type(self.source_panel) is not VerifiedPromotedTechnicalFeaturePanel
            or type(self.config) is not PromotedCrossSectionConfig
            or type(self.cutoff) is not datetime
            or type(self.knowledge_time) is not datetime
            or (
                self.regime_evidence is not None
                and type(self.regime_evidence) is not PromotedMarketRegimeEvidence
            )
            or type(self.results) is not tuple
            or any(type(value) is not PromotedCrossSectionResult for value in self.results)
            or type(self.status_counts) is not tuple
            or any(
                type(value) is not tuple
                or len(value) != 2
                or type(value[0]) is not str
                or type(value[1]) is not int
                for value in self.status_counts
            )
            or any(
                type(value) is not int or value < 0
                for value in (
                    self.scored_history_count,
                    self.blocked_history_count,
                    self.unassigned_entry_count,
                    self.orphan_bar_count,
                )
            )
            or any(
                type(value) is not bool
                for value in (
                    self.resolved_histories_scoring_complete,
                    self.source_universe_cross_section_complete,
                    self.actionable,
                    self.training_eligible,
                    self.feature_eligible,
                    self.ranking_eligible,
                    self.alert_eligible,
                    self.execution_eligible,
                )
            )
            or type(self.readiness) is not ReferenceReadiness
            or type(self.panel_id) is not str
            or _SHA256.fullmatch(self.panel_id) is None
        ):
            raise PromotedCrossSectionError(_ERR_DERIVED)
        try:
            facts = _build_facts(self.source_panel, self.config, self.cutoff)
            comparisons = (
                (self.cutoff, facts.cutoff),
                (self.knowledge_time, facts.knowledge_time),
                (self.regime_evidence, facts.regime_evidence),
                (self.results, facts.results),
                (self.status_counts, facts.status_counts),
                (self.scored_history_count, facts.scored_history_count),
                (self.blocked_history_count, facts.blocked_history_count),
                (
                    self.resolved_histories_scoring_complete,
                    facts.resolved_histories_scoring_complete,
                ),
                (
                    self.source_universe_cross_section_complete,
                    facts.source_universe_cross_section_complete,
                ),
                (self.unassigned_entry_count, facts.unassigned_entry_count),
                (self.orphan_bar_count, facts.orphan_bar_count),
                (self.readiness, facts.readiness),
                (self.actionable, facts.actionable),
                (self.training_eligible, facts.training_eligible),
                (self.feature_eligible, facts.feature_eligible),
                (self.ranking_eligible, facts.ranking_eligible),
                (self.alert_eligible, facts.alert_eligible),
                (self.execution_eligible, facts.execution_eligible),
                (self.panel_id, facts.panel_id),
            )
            if any(left != right for left, right in comparisons):
                raise PromotedCrossSectionError(_ERR_DERIVED)
        except PromotedCrossSectionError:
            raise
        except Exception:
            raise PromotedCrossSectionError(_ERR_DERIVED) from None


class PromotedCrossSectionService:
    """Scores the resolved subset without granting ranking or trade authority."""

    def materialize(
        self,
        *,
        source_panel: VerifiedPromotedTechnicalFeaturePanel,
        config: PromotedCrossSectionConfig,
        cutoff: datetime,
    ) -> VerifiedPromotedCrossSectionPanel:
        facts = _build_facts(source_panel, config, cutoff)
        return VerifiedPromotedCrossSectionPanel(
            schema_version=PROMOTED_CROSS_SECTION_SCHEMA_VERSION,
            policy_version=PROMOTED_CROSS_SECTION_POLICY_VERSION,
            source_panel=source_panel,
            config=config,
            cutoff=facts.cutoff,
            knowledge_time=facts.knowledge_time,
            regime_evidence=facts.regime_evidence,
            results=facts.results,
            status_counts=facts.status_counts,
            scored_history_count=facts.scored_history_count,
            blocked_history_count=facts.blocked_history_count,
            resolved_histories_scoring_complete=(
                facts.resolved_histories_scoring_complete
            ),
            source_universe_cross_section_complete=(
                facts.source_universe_cross_section_complete
            ),
            unassigned_entry_count=facts.unassigned_entry_count,
            orphan_bar_count=facts.orphan_bar_count,
            readiness=facts.readiness,
            actionable=facts.actionable,
            training_eligible=facts.training_eligible,
            feature_eligible=facts.feature_eligible,
            ranking_eligible=facts.ranking_eligible,
            alert_eligible=facts.alert_eligible,
            execution_eligible=facts.execution_eligible,
            panel_id=facts.panel_id,
        )
