from __future__ import annotations

import json

from .materialization import CollectionCalendarMaterialization


def _manifest(value: object) -> dict[str, object]:
    return {
        "schema_version": value.schema_version,
        "manifest_id": value.manifest_id,
        "artifact_id": value.artifact_id,
        "dataset": value.dataset,
        "exchange": value.exchange,
        "segment": value.segment,
        "claimed_authority": value.claimed_authority,
        "acquisition_mode": value.acquisition_mode.value,
        "readiness": value.readiness.value,
        "actionable": value.actionable,
        "publication_time_status": value.publication_time_status,
        "first_seen_at": value.first_seen_at.isoformat(),
        "validated_at": value.validated_at.isoformat(),
        "original_source_filename": value.original_source_filename,
        "original_declaration_filename": value.original_declaration_filename,
        "claimed_document_id": value.claimed_document_id,
        "claimed_issue_date": value.claimed_issue_date.isoformat(),
        "claimed_source_url": value.claimed_source_url,
        "source_media_type": value.source_media_type,
        "source_byte_count": value.source_byte_count,
        "source_sha256": value.source_sha256,
        "declaration_byte_count": value.declaration_byte_count,
        "declaration_sha256": value.declaration_sha256,
        "normalized_byte_count": value.normalized_byte_count,
        "normalized_sha256": value.normalized_sha256,
        "event_count": value.event_count,
        "event_ids": list(value.event_ids),
        "parser_version": value.parser_version,
        "declaration_schema_version": value.declaration_schema_version,
        "event_schema_version": value.event_schema_version,
        "event_policy_version": value.event_policy_version,
        "normalized_codec_version": value.normalized_codec_version,
        "raw_filename": value.raw_filename,
        "declaration_filename": value.declaration_filename,
        "normalized_filename": value.normalized_filename,
    }


def encode_calendar_materialization(
    materialization: CollectionCalendarMaterialization,
) -> bytes:
    if type(materialization) is not CollectionCalendarMaterialization:
        raise TypeError("calendar materialization codec requires an exact artifact")
    materialization.verify_content_identity()
    calendar = materialization.calendar_snapshot
    value = {
        "schema_version": materialization.schema_version,
        "policy_version": materialization.policy_version,
        "materialization_id": materialization.materialization_id,
        "exchange": materialization.exchange,
        "segment": materialization.segment,
        "cutoff": materialization.cutoff.isoformat(),
        "coverage_start": materialization.coverage_start.isoformat(),
        "coverage_end": materialization.coverage_end.isoformat(),
        "readiness": materialization.readiness.value,
        "actionable": materialization.actionable,
        "source_manifests": [
            _manifest(manifest) for manifest in materialization.source_manifests
        ],
        "day_resolutions": [
            {
                "day": resolution.day.isoformat(),
                "state_chain_event_ids": list(resolution.state_chain_event_ids),
                "non_executable_event_ids": list(
                    resolution.non_executable_event_ids
                ),
                "applied_event_ids": list(resolution.applied_event_ids),
                "source_artifact_ids": list(resolution.source_artifact_ids),
                "source_manifest_ids": list(resolution.source_manifest_ids),
                "source_snapshot_id": resolution.source_snapshot_id,
                "resolution_id": resolution.resolution_id,
            }
            for resolution in materialization.day_resolutions
        ],
        "observed_evidence_bindings": [
            {
                "artifact_id": binding.artifact_id,
                "cutoff": binding.cutoff.isoformat(),
                "knowledge_time": binding.knowledge_time.isoformat(),
                "source_bundle_artifact_id": binding.source_bundle_artifact_id,
                "source_bundle_manifest_id": binding.source_bundle_manifest_id,
                "observed_dates": [
                    value.isoformat() for value in binding.observed_dates
                ],
                "binding_id": binding.binding_id,
            }
            for binding in materialization.observed_evidence_bindings
        ],
        "calendar_snapshot": {
            "schema_version": calendar.schema_version,
            "snapshot_id": calendar.snapshot_id,
            "version": calendar.version,
            "exchange": calendar.exchange,
            "segment": calendar.segment,
            "cutoff": calendar.cutoff.isoformat(),
            "coverage_start": calendar.coverage_start.isoformat(),
            "coverage_end": calendar.coverage_end.isoformat(),
            "source_snapshot_ids": list(calendar.source_snapshot_ids),
            "readiness": calendar.readiness.value,
            "days": [
                {
                    "day": day.day.isoformat(),
                    "kind": day.kind.value,
                    "data_ready_at": None,
                    "reference": {
                        "event_time": day.reference.event_time.isoformat(),
                        "knowledge_time": day.reference.knowledge_time.isoformat(),
                        "source": day.reference.source,
                        "content_hash": day.reference.content_hash,
                        "source_snapshot_id": day.reference.source_snapshot_id,
                    },
                    "session_windows": [
                        {
                            "opens_at": window.opens_at.isoformat(),
                            "closes_at": window.closes_at.isoformat(),
                            "phase": window.phase.value,
                        }
                        for window in day.session_windows
                    ],
                }
                for day in calendar.days
            ],
        },
    }
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
