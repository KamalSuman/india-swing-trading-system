from __future__ import annotations

from india_swing.promotion import PromotionCapability, PromotionEvidence

from .models import CorporateActionSnapshot


def corporate_action_promotion_evidence(
    snapshot: CorporateActionSnapshot,
) -> PromotionEvidence:
    if type(snapshot) is not CorporateActionSnapshot:
        raise TypeError("corporate-action snapshot must be exact")
    snapshot.verify_content_identity()
    return PromotionEvidence(
        capability=PromotionCapability.CORPORATE_ACTIONS,
        cutoff=snapshot.cutoff,
        coverage_start=snapshot.coverage_start,
        coverage_end=snapshot.coverage_end,
        source_snapshot_ids=(snapshot.snapshot_id,),
        readiness=snapshot.readiness,
        complete=snapshot.complete,
        actionable=snapshot.actionable,
        reason_codes=snapshot.reason_codes,
    )
