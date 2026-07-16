from __future__ import annotations

import json

from .models import ObservedMarketDateArtifact


def encode_observed_market_date_artifact(
    artifact: ObservedMarketDateArtifact,
) -> bytes:
    if type(artifact) is not ObservedMarketDateArtifact:
        raise TypeError("calendar-evidence codec requires an exact artifact")
    artifact.verify_content_identity()
    value = {
        "schema_version": artifact.schema_version,
        "policy_version": artifact.policy_version,
        "artifact_id": artifact.artifact_id,
        "exchange": artifact.exchange,
        "segment": artifact.segment,
        "cutoff": artifact.cutoff.isoformat(),
        "source": {
            "bundle_artifact_id": artifact.source_bundle_artifact_id,
            "bundle_manifest_id": artifact.source_bundle_manifest_id,
            "raw_sha256": artifact.source_bundle_raw_sha256,
            "normalized_sha256": artifact.source_bundle_normalized_sha256,
            "acquisition_mode": artifact.source_acquisition_mode.value,
            "readiness": artifact.source_readiness.value,
            "first_seen_at": artifact.source_first_seen_at.isoformat(),
            "validated_at": artifact.source_validated_at.isoformat(),
        },
        "inference_scope": artifact.inference_scope,
        "readiness": artifact.readiness.value,
        "actionable": artifact.actionable,
        "observations": [
            {
                "market_date": observation.market_date.isoformat(),
                "evidence_id": observation.evidence_id,
                "report_refs": [
                    {
                        "bundle_artifact_id": reference.bundle_artifact_id,
                        "bundle_manifest_id": reference.bundle_manifest_id,
                        "family": reference.family.value,
                        "source_entry_name": reference.source_entry_name,
                        "content_name": reference.content_name,
                        "source_entry_sha256": reference.source_entry_sha256,
                        "content_sha256": reference.content_sha256,
                        "header_sha256": reference.header_sha256,
                        "ordered_row_digest": reference.ordered_row_digest,
                        "row_count": reference.row_count,
                        "trade_date": reference.trade_date.isoformat(),
                        "knowledge_time": reference.knowledge_time.isoformat(),
                    }
                    for reference in observation.report_refs
                ],
            }
            for observation in artifact.observations
        ],
    }
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
