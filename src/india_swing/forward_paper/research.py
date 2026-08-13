"""Deterministic baseline/challenger research over one verified graph.

The module reuses the established promoted cross-sectional regime and
specialist-scoring kernel. It does not select trades, estimate probabilities,
or grant ranking, alert, paper-trade, notification, or execution authority.
Every blocked upstream result remains present as immutable evidence.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum

from india_swing.features.promoted_cross_section import (
    PromotedCrossSectionConfig,
    PromotedMarketRegimeEvidence,
    PromotedOpportunityScore,
    score_promoted_feature_vectors,
)
from india_swing.features.promoted_technical import PromotedTechnicalFeatureVector
from india_swing.identity import content_id

from .features import (
    ForwardPaperTechnicalFeatureResult,
    ForwardPaperTechnicalFeatureWindow,
)
from .operational import ForwardPaperOperationalResearchGraph


FORWARD_PAPER_RESEARCH_POLICY_VERSION = (
    "forward-paper-baseline-challenger/shared-promoted-score-kernel-v1"
)
FORWARD_PAPER_RESEARCH_ARM_SCHEMA_VERSION = "forward-paper-research-arm/v1"
FORWARD_PAPER_RESEARCH_COMPARISON_SCHEMA_VERSION = (
    "forward-paper-research-comparison/v1"
)
FORWARD_PAPER_RESEARCH_RUN_SCHEMA_VERSION = "forward-paper-research-run/v1"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class ForwardPaperResearchError(ValueError):
    """One static failure at the forward-paper research boundary."""


def _fail(message: str) -> None:
    raise ForwardPaperResearchError(message)


def _sha(value: object) -> bool:
    return type(value) is str and _SHA256.fullmatch(value) is not None


class ForwardPaperResearchArmName(str, Enum):
    BASELINE = "BASELINE"
    CHALLENGER = "CHALLENGER"


def _vectors(
    source: ForwardPaperTechnicalFeatureWindow,
) -> tuple[PromotedTechnicalFeatureVector, ...]:
    return tuple(
        result.feature_vector
        for result in source.results
        if type(result.feature_vector) is PromotedTechnicalFeatureVector
    )


def _blocked_result_ids(
    source: ForwardPaperTechnicalFeatureWindow,
    *,
    scoring_available: bool,
) -> tuple[str, ...]:
    return tuple(
        sorted(
            result.result_id
            for result in source.results
            if result.feature_vector is None or not scoring_available
        )
    )


@dataclass(frozen=True, slots=True)
class ForwardPaperResearchArmResult:
    name: ForwardPaperResearchArmName
    source_window: ForwardPaperTechnicalFeatureWindow
    config: PromotedCrossSectionConfig
    regime_evidence: PromotedMarketRegimeEvidence | None
    opportunities: tuple[PromotedOpportunityScore, ...]
    blocked_result_ids: tuple[str, ...]
    arm_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "arm_id", self._calculated_id())

    def _validate(self) -> None:
        if (
            type(self.name) is not ForwardPaperResearchArmName
            or type(self.source_window) is not ForwardPaperTechnicalFeatureWindow
            or type(self.config) is not PromotedCrossSectionConfig
            or (
                self.regime_evidence is not None
                and type(self.regime_evidence) is not PromotedMarketRegimeEvidence
            )
            or type(self.opportunities) is not tuple
            or any(type(value) is not PromotedOpportunityScore for value in self.opportunities)
            or type(self.blocked_result_ids) is not tuple
            or self.blocked_result_ids != tuple(sorted(set(self.blocked_result_ids)))
            or any(not _sha(value) for value in self.blocked_result_ids)
        ):
            _fail("forward paper research arm shape is invalid")
        failed = False
        expected_regime = None
        expected_opportunities: tuple[PromotedOpportunityScore, ...] = ()
        try:
            self.source_window.verify_content_identity()
            self.config.verify_content_identity()
            expected_regime, expected_opportunities = score_promoted_feature_vectors(
                vectors=_vectors(self.source_window),
                source_feature_panel_id=self.source_window.window_id,
                config=self.config,
            )
        except Exception:
            failed = True
        if failed:
            _fail("forward paper research arm evidence failed verification")
        expected_blocked = _blocked_result_ids(
            self.source_window,
            scoring_available=expected_regime is not None,
        )
        if (
            self.regime_evidence != expected_regime
            or self.opportunities != expected_opportunities
            or self.blocked_result_ids != expected_blocked
            or len(self.opportunities) + len(self.blocked_result_ids)
            != len(self.source_window.results)
        ):
            _fail("forward paper research arm derivation is invalid")

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": FORWARD_PAPER_RESEARCH_ARM_SCHEMA_VERSION,
                "policy_version": FORWARD_PAPER_RESEARCH_POLICY_VERSION,
                "name": self.name,
                "source_window_id": self.source_window.window_id,
                "config_id": self.config.config_id,
                "regime_evidence_id": (
                    None
                    if self.regime_evidence is None
                    else self.regime_evidence.evidence_id
                ),
                "opportunity_ids": tuple(
                    value.opportunity_id for value in self.opportunities
                ),
                "blocked_result_ids": self.blocked_result_ids,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.arm_id != self._calculated_id():
            _fail("forward paper research arm identity failed")

    @property
    def collection_only(self) -> bool:
        return True

    @property
    def ranking_eligible(self) -> bool:
        return False

    @property
    def paper_trade_eligible(self) -> bool:
        return False

    @property
    def notification_eligible(self) -> bool:
        return False

    @property
    def execution_eligible(self) -> bool:
        return False


@dataclass(frozen=True, slots=True)
class ForwardPaperResearchComparison:
    stable_instrument_id: str
    stable_listing_id: str
    source_feature_id: str
    baseline_opportunity_id: str
    challenger_opportunity_id: str
    baseline_rank_tier: int
    challenger_rank_tier: int
    baseline_score: Decimal
    challenger_score: Decimal
    rank_tier_delta: int
    score_delta: Decimal
    comparison_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            any(
                not _sha(value)
                for value in (
                    self.stable_instrument_id,
                    self.stable_listing_id,
                    self.source_feature_id,
                    self.baseline_opportunity_id,
                    self.challenger_opportunity_id,
                )
            )
            or type(self.baseline_rank_tier) is not int
            or self.baseline_rank_tier <= 0
            or type(self.challenger_rank_tier) is not int
            or self.challenger_rank_tier <= 0
            or type(self.baseline_score) is not Decimal
            or not self.baseline_score.is_finite()
            or type(self.challenger_score) is not Decimal
            or not self.challenger_score.is_finite()
            or self.rank_tier_delta
            != self.challenger_rank_tier - self.baseline_rank_tier
            or self.score_delta != self.challenger_score - self.baseline_score
        ):
            _fail("forward paper research comparison is invalid")
        object.__setattr__(self, "comparison_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": FORWARD_PAPER_RESEARCH_COMPARISON_SCHEMA_VERSION,
                "policy_version": FORWARD_PAPER_RESEARCH_POLICY_VERSION,
                "stable_instrument_id": self.stable_instrument_id,
                "stable_listing_id": self.stable_listing_id,
                "source_feature_id": self.source_feature_id,
                "baseline_opportunity_id": self.baseline_opportunity_id,
                "challenger_opportunity_id": self.challenger_opportunity_id,
                "baseline_rank_tier": self.baseline_rank_tier,
                "challenger_rank_tier": self.challenger_rank_tier,
                "baseline_score": self.baseline_score,
                "challenger_score": self.challenger_score,
                "rank_tier_delta": self.rank_tier_delta,
                "score_delta": self.score_delta,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.comparison_id != self._calculated_id():
            _fail("forward paper research comparison identity failed")


@dataclass(frozen=True, slots=True)
class ForwardPaperBaselineChallengerRun:
    source_graph: ForwardPaperOperationalResearchGraph
    baseline: ForwardPaperResearchArmResult
    challenger: ForwardPaperResearchArmResult
    comparison_top_tiers: int
    comparisons: tuple[ForwardPaperResearchComparison, ...]
    baseline_top_count: int
    challenger_top_count: int
    overlap_count: int
    run_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "run_id", self._calculated_id())

    def _validate(self) -> None:
        if (
            type(self.source_graph) is not ForwardPaperOperationalResearchGraph
            or type(self.baseline) is not ForwardPaperResearchArmResult
            or type(self.challenger) is not ForwardPaperResearchArmResult
            or self.baseline.name is not ForwardPaperResearchArmName.BASELINE
            or self.challenger.name is not ForwardPaperResearchArmName.CHALLENGER
            or self.baseline.config.config_id == self.challenger.config.config_id
            or type(self.comparison_top_tiers) is not int
            or isinstance(self.comparison_top_tiers, bool)
            or self.comparison_top_tiers <= 0
            or type(self.comparisons) is not tuple
            or any(
                type(value) is not ForwardPaperResearchComparison
                for value in self.comparisons
            )
        ):
            _fail("forward paper research run shape is invalid")
        failed = False
        try:
            self.source_graph.verify_content_identity()
            self.baseline.verify_content_identity()
            self.challenger.verify_content_identity()
            for value in self.comparisons:
                value.verify_content_identity()
        except Exception:
            failed = True
        if failed:
            _fail("forward paper research run evidence failed verification")
        source = self.source_graph.technical_feature_window
        if (
            self.baseline.source_window is not source
            or self.challenger.source_window is not source
        ):
            _fail("forward paper research run lineage is invalid")
        expected = _comparisons(
            self.baseline,
            self.challenger,
            self.comparison_top_tiers,
        )
        baseline_top = sum(
            value.rank_tier <= self.comparison_top_tiers
            for value in self.baseline.opportunities
        )
        challenger_top = sum(
            value.rank_tier <= self.comparison_top_tiers
            for value in self.challenger.opportunities
        )
        if (
            self.comparisons != expected
            or self.baseline_top_count != baseline_top
            or self.challenger_top_count != challenger_top
            or self.overlap_count != len(expected)
        ):
            _fail("forward paper research run derivation is invalid")

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": FORWARD_PAPER_RESEARCH_RUN_SCHEMA_VERSION,
                "policy_version": FORWARD_PAPER_RESEARCH_POLICY_VERSION,
                "source_graph_id": self.source_graph.graph_id,
                "baseline_arm_id": self.baseline.arm_id,
                "challenger_arm_id": self.challenger.arm_id,
                "comparison_top_tiers": self.comparison_top_tiers,
                "comparison_ids": tuple(
                    value.comparison_id for value in self.comparisons
                ),
                "baseline_top_count": self.baseline_top_count,
                "challenger_top_count": self.challenger_top_count,
                "overlap_count": self.overlap_count,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.run_id != self._calculated_id():
            _fail("forward paper research run identity failed")

    @property
    def collection_only(self) -> bool:
        return True

    @property
    def promotion_eligible(self) -> bool:
        return False

    @property
    def paper_trade_eligible(self) -> bool:
        return False

    @property
    def notification_eligible(self) -> bool:
        return False

    @property
    def execution_eligible(self) -> bool:
        return False


def _build_arm(
    *,
    name: ForwardPaperResearchArmName,
    source: ForwardPaperTechnicalFeatureWindow,
    config: PromotedCrossSectionConfig,
) -> ForwardPaperResearchArmResult:
    regime, opportunities = score_promoted_feature_vectors(
        vectors=_vectors(source),
        source_feature_panel_id=source.window_id,
        config=config,
    )
    return ForwardPaperResearchArmResult(
        name=name,
        source_window=source,
        config=config,
        regime_evidence=regime,
        opportunities=opportunities,
        blocked_result_ids=_blocked_result_ids(
            source,
            scoring_available=regime is not None,
        ),
    )


def _comparisons(
    baseline: ForwardPaperResearchArmResult,
    challenger: ForwardPaperResearchArmResult,
    top_tiers: int,
) -> tuple[ForwardPaperResearchComparison, ...]:
    baseline_by_key = {
        (value.stable_instrument_id, value.stable_listing_id): value
        for value in baseline.opportunities
        if value.rank_tier <= top_tiers
    }
    challenger_by_key = {
        (value.stable_instrument_id, value.stable_listing_id): value
        for value in challenger.opportunities
        if value.rank_tier <= top_tiers
    }
    rows = []
    for key in sorted(set(baseline_by_key) & set(challenger_by_key)):
        left = baseline_by_key[key]
        right = challenger_by_key[key]
        if left.source_feature_id != right.source_feature_id:
            _fail("forward paper research arm feature lineage differs")
        rows.append(
            ForwardPaperResearchComparison(
                stable_instrument_id=key[0],
                stable_listing_id=key[1],
                source_feature_id=left.source_feature_id,
                baseline_opportunity_id=left.opportunity_id,
                challenger_opportunity_id=right.opportunity_id,
                baseline_rank_tier=left.rank_tier,
                challenger_rank_tier=right.rank_tier,
                baseline_score=left.ensemble_score,
                challenger_score=right.ensemble_score,
                rank_tier_delta=right.rank_tier - left.rank_tier,
                score_delta=right.ensemble_score - left.ensemble_score,
            )
        )
    return tuple(sorted(rows, key=lambda value: value.comparison_id))


def run_forward_paper_baseline_challenger_research(
    *,
    source_graph: ForwardPaperOperationalResearchGraph,
    baseline_config: PromotedCrossSectionConfig,
    challenger_config: PromotedCrossSectionConfig,
    comparison_top_tiers: int = 10,
) -> ForwardPaperBaselineChallengerRun:
    """Run two explicit configurations over the same exact feature graph."""

    if (
        type(source_graph) is not ForwardPaperOperationalResearchGraph
        or type(baseline_config) is not PromotedCrossSectionConfig
        or type(challenger_config) is not PromotedCrossSectionConfig
        or baseline_config.config_id == challenger_config.config_id
        or type(comparison_top_tiers) is not int
        or isinstance(comparison_top_tiers, bool)
        or comparison_top_tiers <= 0
    ):
        _fail("forward paper research request is invalid")
    failed = False
    try:
        source_graph.verify_content_identity()
        baseline_config.verify_content_identity()
        challenger_config.verify_content_identity()
    except Exception:
        failed = True
    if failed:
        _fail("forward paper research request failed verification")
    source = source_graph.technical_feature_window
    baseline = _build_arm(
        name=ForwardPaperResearchArmName.BASELINE,
        source=source,
        config=baseline_config,
    )
    challenger = _build_arm(
        name=ForwardPaperResearchArmName.CHALLENGER,
        source=source,
        config=challenger_config,
    )
    comparisons = _comparisons(baseline, challenger, comparison_top_tiers)
    return ForwardPaperBaselineChallengerRun(
        source_graph=source_graph,
        baseline=baseline,
        challenger=challenger,
        comparison_top_tiers=comparison_top_tiers,
        comparisons=comparisons,
        baseline_top_count=sum(
            value.rank_tier <= comparison_top_tiers
            for value in baseline.opportunities
        ),
        challenger_top_count=sum(
            value.rank_tier <= comparison_top_tiers
            for value in challenger.opportunities
        ),
        overlap_count=len(comparisons),
    )
