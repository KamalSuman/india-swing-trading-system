from __future__ import annotations

from india_swing.promotion import PromotionCapability, PromotionEvidence

from .models import CollectionLiquiditySnapshot


def liquidity_promotion_evidence(
    snapshot: CollectionLiquiditySnapshot,
) -> PromotionEvidence:
    if type(snapshot) is not CollectionLiquiditySnapshot:
        raise TypeError("liquidity snapshot must be exact")
    snapshot.verify_content_identity()
    return PromotionEvidence(
        capability=PromotionCapability.LIQUIDITY,
        cutoff=snapshot.decision_cutoff,
        coverage_start=snapshot.coverage_start,
        coverage_end=snapshot.coverage_end,
        source_snapshot_ids=(snapshot.snapshot_id,),
        readiness=snapshot.readiness,
        complete=False,
        actionable=False,
        reason_codes=snapshot.reason_codes,
    )
