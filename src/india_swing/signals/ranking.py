from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from india_swing.domain.models import Candidate


ZERO = Decimal("0")
ONE = Decimal("1")


def clamp(value: Decimal, lower: Decimal = ZERO, upper: Decimal = ONE) -> Decimal:
    return max(lower, min(value, upper))


@dataclass(frozen=True, slots=True)
class RankWeights:
    expected_return: Decimal = Decimal("0.30")
    relative_strength: Decimal = Decimal("0.20")
    trend_quality: Decimal = Decimal("0.15")
    volume_confirmation: Decimal = Decimal("0.10")
    news: Decimal = Decimal("0.10")
    liquidity: Decimal = Decimal("0.10")
    downside_penalty: Decimal = Decimal("0.20")
    uncertainty_penalty: Decimal = Decimal("0.15")
    cost_penalty: Decimal = Decimal("0.05")


@dataclass(frozen=True, slots=True)
class RankedCandidate:
    candidate: Candidate
    score: Decimal
    components: tuple[tuple[str, Decimal], ...]


class WeightedRanker:
    """A provisional, explicit ranking rule; weights must be walk-forward validated."""

    version = "provisional-ranker-v1"

    def __init__(self, weights: RankWeights | None = None) -> None:
        self.weights = weights or RankWeights()

    def score(self, candidate: Candidate) -> RankedCandidate:
        expected = clamp(candidate.forecast.median_return_pct / Decimal("10"), Decimal("-1"), ONE)
        downside = clamp(abs(min(candidate.forecast.downside_return_pct, ZERO)) / Decimal("10"))
        cost = clamp(candidate.signals.estimated_cost_bps / Decimal("100"))
        news = candidate.signals.news_score

        components = (
            ("expected_return", self.weights.expected_return * expected),
            (
                "relative_strength",
                self.weights.relative_strength * candidate.signals.relative_strength,
            ),
            ("trend_quality", self.weights.trend_quality * candidate.signals.trend_quality),
            (
                "volume_confirmation",
                self.weights.volume_confirmation * candidate.signals.volume_confirmation,
            ),
            ("news", self.weights.news * news),
            ("liquidity", self.weights.liquidity * candidate.signals.liquidity_quality),
            ("downside_penalty", -(self.weights.downside_penalty * downside)),
            (
                "uncertainty_penalty",
                -(self.weights.uncertainty_penalty * candidate.forecast.uncertainty),
            ),
            ("cost_penalty", -(self.weights.cost_penalty * cost)),
        )
        return RankedCandidate(candidate, sum((value for _, value in components), ZERO), components)

    def rank(self, candidates: list[Candidate]) -> list[RankedCandidate]:
        ranked = [self.score(candidate) for candidate in candidates]
        return sorted(ranked, key=lambda item: (-item.score, item.candidate.instrument.symbol))
