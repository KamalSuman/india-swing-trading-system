from __future__ import annotations

from datetime import date, datetime

from india_swing.reference.models import ReferenceReadiness

from .models import (
    PromotionCapability,
    PromotionDecision,
    PromotionEvidence,
    PromotionStage,
)


RESEARCH_REQUIREMENTS = frozenset(
    {
        PromotionCapability.CALENDAR,
        PromotionCapability.STABLE_IDENTITY,
        PromotionCapability.UNIVERSE,
        PromotionCapability.RAW_PRICES,
    }
)
BACKTEST_REQUIREMENTS = RESEARCH_REQUIREMENTS | frozenset(
    {
        PromotionCapability.CORPORATE_ACTIONS,
        PromotionCapability.LIQUIDITY,
        PromotionCapability.SURVEILLANCE,
        PromotionCapability.TICK_SIZES,
        PromotionCapability.EXPLICIT_NONTRADING,
        PromotionCapability.RECONCILIATION,
    }
)
ALERT_REQUIREMENTS = BACKTEST_REQUIREMENTS | frozenset(
    {
        PromotionCapability.MODEL_VALIDATION,
        PromotionCapability.RISK_POLICY,
        PromotionCapability.SHADOW_OPERATIONS,
    }
)


def _blockers_for(
    *,
    required: frozenset[PromotionCapability],
    by_capability: dict[PromotionCapability, PromotionEvidence],
    history_start: date,
    market_session: date,
    decision_cutoff: datetime,
) -> tuple[str, ...]:
    blockers: set[str] = set()
    for capability in required:
        evidence = by_capability.get(capability)
        prefix = capability.value
        if evidence is None:
            blockers.add(f"MISSING_{prefix}")
            continue
        if evidence.readiness is ReferenceReadiness.COLLECTION_ONLY:
            blockers.add(f"{prefix}_COLLECTION_ONLY")
        if evidence.readiness is ReferenceReadiness.SYNTHETIC_TEST:
            blockers.add(f"{prefix}_SYNTHETIC_ONLY")
        if not evidence.complete:
            blockers.add(f"{prefix}_INCOMPLETE")
        if not evidence.actionable:
            blockers.add(f"{prefix}_NOT_ACTIONABLE")
        if evidence.cutoff > decision_cutoff:
            blockers.add(f"{prefix}_FUTURE_KNOWLEDGE")
        if (
            evidence.coverage_start > history_start
            or evidence.coverage_end < market_session
        ):
            blockers.add(f"{prefix}_COVERAGE_GAP")
        blockers.update(f"{prefix}_{value}" for value in evidence.reason_codes)
    return tuple(sorted(blockers))


def evaluate_promotion(
    *,
    market_session: date,
    history_start: date,
    decision_cutoff: datetime,
    evidence: tuple[PromotionEvidence, ...],
) -> PromotionDecision:
    """Evaluate all stages independently and fail closed on missing evidence."""

    if type(evidence) is not tuple or any(
        type(value) is not PromotionEvidence for value in evidence
    ):
        raise TypeError("promotion evidence must be an exact tuple")
    ordered = tuple(sorted(evidence, key=lambda value: value.capability.value))
    if len({value.capability for value in ordered}) != len(ordered):
        raise ValueError("promotion evidence capabilities must be unique")
    for value in ordered:
        value.verify_content_identity()
    by_capability = {value.capability: value for value in ordered}
    arguments = {
        "by_capability": by_capability,
        "history_start": history_start,
        "market_session": market_session,
        "decision_cutoff": decision_cutoff,
    }
    research = _blockers_for(required=RESEARCH_REQUIREMENTS, **arguments)
    backtest = _blockers_for(required=BACKTEST_REQUIREMENTS, **arguments)
    alert = _blockers_for(required=ALERT_REQUIREMENTS, **arguments)
    achieved = (
        PromotionStage.ALERT_ELIGIBLE
        if not alert
        else PromotionStage.BACKTEST_ELIGIBLE
        if not backtest
        else PromotionStage.RESEARCH_ELIGIBLE
        if not research
        else PromotionStage.COLLECTION_ONLY
    )
    return PromotionDecision(
        market_session=market_session,
        history_start=history_start,
        decision_cutoff=decision_cutoff,
        evidence=ordered,
        achieved_stage=achieved,
        research_blockers=research,
        backtest_blockers=backtest,
        alert_blockers=alert,
    )
