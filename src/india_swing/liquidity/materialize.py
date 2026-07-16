from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from india_swing.historical_prices.models import NseEodSessionArtifact, RawNseEodBar
from india_swing.identity import content_id

from .models import (
    CollectedLiquidityObservation,
    CollectionLiquiditySnapshot,
    LiquidityIntegrityError,
    LiquiditySourceSession,
)


def _median(values: list[Decimal]) -> Decimal:
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / Decimal(2)


def materialize_collection_liquidity(
    sources: tuple[NseEodSessionArtifact, ...],
    *,
    decision_cutoff: datetime,
    minimum_history_sessions: int = 120,
) -> CollectionLiquiditySnapshot:
    if (
        type(sources) is not tuple
        or not sources
        or any(type(value) is not NseEodSessionArtifact for value in sources)
        or sources != tuple(sorted(sources, key=lambda value: value.market_session))
    ):
        raise LiquidityIntegrityError(
            "liquidity sources must be non-empty, exact, and session ordered"
        )
    if len({value.market_session for value in sources}) != len(sources):
        raise LiquidityIntegrityError("liquidity source sessions must be unique")
    if not isinstance(decision_cutoff, datetime):
        raise TypeError("liquidity decision_cutoff must be a datetime")
    if decision_cutoff.tzinfo is None or decision_cutoff.utcoffset() is None:
        raise LiquidityIntegrityError("liquidity decision_cutoff must be timezone-aware")
    decision_cutoff = decision_cutoff.astimezone(timezone.utc)
    if type(minimum_history_sessions) is not int or minimum_history_sessions <= 0:
        raise LiquidityIntegrityError("minimum history sessions must be positive")
    for value in sources:
        value.verify_content_identity()
        if value.knowledge_time > decision_cutoff:
            raise LiquidityIntegrityError(
                "historical price source was unavailable at the decision cutoff"
            )

    accumulated: dict[tuple[str, str], list[RawNseEodBar]] = {}
    seen_session_candidates: set[tuple[object, ...]] = set()
    for source in sources:
        for bar in source.bars:
            candidate_key = (bar.validated_isin, bar.series)
            session_key = (source.market_session, *candidate_key)
            if session_key in seen_session_candidates:
                raise LiquidityIntegrityError(
                    "one liquidity candidate has duplicate bars in a session"
                )
            seen_session_candidates.add(session_key)
            accumulated.setdefault(candidate_key, []).append(bar)

    observations = []
    for (validated_isin, series), bars in accumulated.items():
        candidate_id = content_id(
            {
                "scheme": "validated-isin-series-liquidity-candidate/v1",
                "validated_isin": validated_isin,
                "series": series,
            },
            length=64,
        )
        delivery = [
            value.delivery_percent
            for value in bars
            if value.delivery_percent is not None
        ]
        observations.append(
            CollectedLiquidityObservation(
                candidate_id=candidate_id,
                validated_isin=validated_isin,
                series=series,
                symbols=tuple(sorted({value.symbol for value in bars})),
                observed_sessions=tuple(value.market_session for value in bars),
                bar_ids=tuple(value.bar_id for value in bars),
                supplied_session_count=len(sources),
                minimum_history_sessions=minimum_history_sessions,
                median_daily_traded_value=_median(
                    [value.traded_value for value in bars]
                ),
                median_daily_volume=_median(
                    [Decimal(value.volume) for value in bars]
                ),
                median_delivery_percent=_median(delivery) if delivery else None,
            )
        )
    reasons = {
        "CALENDAR_CONTINUITY_UNVERIFIED",
        "SOURCE_COVERAGE_TRADED_ROWS_ONLY",
        "STABLE_IDENTITY_UNAVAILABLE",
        "UNVERIFIED_MANUAL_ACQUISITION",
    }
    if any(not value.meets_minimum_history for value in observations):
        reasons.add("INSUFFICIENT_HISTORY")
    return CollectionLiquiditySnapshot(
        decision_cutoff=decision_cutoff,
        minimum_history_sessions=minimum_history_sessions,
        source_sessions=tuple(
            LiquiditySourceSession(
                market_session=value.market_session,
                artifact_id=value.artifact_id,
                cutoff=value.cutoff,
                knowledge_time=value.knowledge_time,
            )
            for value in sources
        ),
        observations=tuple(
            sorted(observations, key=lambda value: value.candidate_id)
        ),
        reason_codes=tuple(sorted(reasons)),
    )
