from __future__ import annotations

import json

from .models import IDENTITY_EVIDENCE_CODEC_VERSION, ParsedIdentityEvidenceDeclaration


def encode_identity_evidence_declaration(value: ParsedIdentityEvidenceDeclaration) -> bytes:
    if type(value) is not ParsedIdentityEvidenceDeclaration:
        raise TypeError("identity evidence codec requires an exact declaration")
    value.verify_content_identity()
    payload = {
        "codec_version": IDENTITY_EVIDENCE_CODEC_VERSION,
        "declaration_schema_version": value.schema_version,
        "exchange": value.exchange,
        "segment": value.segment,
        "claimed_authority": value.claimed_authority,
        "source_kind": value.source_kind.value,
        "claimed_document_id": value.claimed_document_id,
        "claimed_issue_date": value.claimed_issue_date.isoformat(),
        "claimed_publication_at": None if value.claimed_publication_at is None else value.claimed_publication_at.isoformat(),
        "claimed_source_url": value.claimed_source_url,
        "source": {
            "filename": value.source_filename,
            "media_type": value.source_media_type,
            "byte_count": value.source_byte_count,
            "sha256": value.source_sha256,
        },
        "claim_count": len(value.claims),
        "claims": [
            {
                "claim_id": claim.claim_id,
                "claim_schema_version": claim.schema_version,
                "candidate_id": claim.candidate_id,
                "requirement": claim.requirement.value,
                "effective_date": None if claim.effective_date is None else claim.effective_date.isoformat(),
                "symbol": claim.symbol,
                "series": claim.series,
                "isin": claim.isin,
                "locator": {"page": claim.locator.page, "row": claim.locator.row, "section": claim.locator.section},
                "claim_text": claim.claim_text,
            }
            for claim in value.claims
        ],
    }
    return json.dumps(payload, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
