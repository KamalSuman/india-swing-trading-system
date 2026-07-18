from __future__ import annotations

from india_swing.promotion import PromotionCapability, PromotionEvidence

from .models import CollectionTickSizeSnapshot


def tick_size_promotion_evidence(
    snapshot: CollectionTickSizeSnapshot,
) -> PromotionEvidence:
    if type(snapshot) is not CollectionTickSizeSnapshot:
        raise TypeError("tick-size snapshot must be exact")
    snapshot.verify_content_identity()
    return PromotionEvidence(
        capability=PromotionCapability.TICK_SIZES,
        cutoff=snapshot.cutoff,
        coverage_start=snapshot.market_session_claim,
        coverage_end=snapshot.market_session_claim,
        source_snapshot_ids=(snapshot.snapshot_id,),
        readiness=snapshot.readiness,
        complete=False,
        actionable=False,
        reason_codes=snapshot.reason_codes,
    )
