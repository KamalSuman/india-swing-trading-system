from __future__ import annotations

from india_swing.promotion import PromotionCapability, PromotionEvidence

from .models import CollectionUniverseSnapshot


def universe_promotion_evidence(
    snapshot: CollectionUniverseSnapshot,
) -> PromotionEvidence:
    if type(snapshot) is not CollectionUniverseSnapshot:
        raise TypeError("universe snapshot must be exact")
    snapshot.verify_content_identity()
    return PromotionEvidence(
        capability=PromotionCapability.UNIVERSE,
        cutoff=snapshot.cutoff,
        coverage_start=snapshot.market_session_claim,
        coverage_end=snapshot.market_session_claim,
        source_snapshot_ids=(snapshot.snapshot_id,),
        readiness=snapshot.readiness,
        complete=False,
        actionable=False,
        reason_codes=snapshot.reason_codes,
    )
