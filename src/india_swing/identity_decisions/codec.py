from __future__ import annotations

import json

from .models import IDENTITY_REVIEW_CODEC_VERSION, ParsedIdentityReviewBundle


def encode_identity_review_bundle(value: ParsedIdentityReviewBundle) -> bytes:
    if type(value) is not ParsedIdentityReviewBundle:
        raise TypeError("identity review codec requires an exact bundle")
    value.verify_content_identity()
    payload = {
        "codec_version": IDENTITY_REVIEW_CODEC_VERSION,
        "declaration_schema_version": value.schema_version,
        "bundle_id": value.bundle_id,
        "queue_id": value.queue_id,
        "source_registry_id": value.source_registry_id,
        "reviewer_id": value.reviewer_id,
        "reviewed_at": value.reviewed_at.isoformat(),
        "decision_count": len(value.decisions),
        "decisions": [
            {
                "decision_id": decision.decision_id,
                "decision_schema_version": decision.schema_version,
                "candidate_id": decision.candidate_id,
                "requirement": decision.requirement.value,
                "outcome": decision.outcome.value,
                "evidence_artifact_id": decision.evidence_artifact_id,
                "evidence_claim_id": decision.evidence_claim_id,
                "rationale": decision.rationale,
            }
            for decision in value.decisions
        ],
    }
    return json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
