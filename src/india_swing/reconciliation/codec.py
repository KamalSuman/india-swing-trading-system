from __future__ import annotations

import json

from india_swing.daily_reports.models import DailyBundleArtifactManifest
from india_swing.reference_data.models import ReferenceArtifactManifest

from .models import (
    BandChangeObservation,
    BandObservation,
    CollectionReconciliationSnapshot,
    EvidenceRowRef,
    OrphanReportKey,
    RECONCILIATION_CODEC_VERSION,
    Reg1Observation,
    ReportBinding,
    SeriesChangeObservation,
)


def _row_ref(value: EvidenceRowRef) -> dict[str, object]:
    return {
        "binding_id": value.binding_id,
        "family": value.family.value,
        "source_row_number": value.source_row_number,
        "row_sha256": value.row_sha256,
        "listing_keys": [list(key) for key in value.listing_keys],
    }


def _binding(value: ReportBinding) -> dict[str, object]:
    return {
        "binding_id": value.binding_id,
        "artifact_id": value.artifact_id,
        "manifest_id": value.manifest_id,
        "bundle_raw_sha256": value.bundle_raw_sha256,
        "bundle_normalized_sha256": value.bundle_normalized_sha256,
        "family": value.family.value,
        "source_entry_name": value.source_entry_name,
        "source_entry_sha256": value.source_entry_sha256,
        "content_sha256": value.content_sha256,
        "ordered_row_digest": value.ordered_row_digest,
        "claimed_report_date": (
            value.claimed_report_date.isoformat()
            if value.claimed_report_date is not None
            else None
        ),
        "confirmed_row_dates": [
            day.isoformat() for day in value.confirmed_row_dates
        ],
        "date_role": value.date_role.value,
        "effective_session": (
            value.effective_session.isoformat()
            if value.effective_session is not None
            else None
        ),
        "effective_session_resolution": value.effective_session_resolution.value,
        "first_seen_at": value.first_seen_at.isoformat(),
        "validated_at": value.validated_at.isoformat(),
        "row_count": value.row_count,
    }


def _daily_manifest(value: DailyBundleArtifactManifest) -> dict[str, object]:
    return {
        "schema_version": value.schema_version,
        "manifest_id": value.manifest_id,
        "artifact_id": value.artifact_id,
        "dataset": value.dataset,
        "claimed_authority": value.claimed_authority,
        "acquisition_mode": value.acquisition_mode.value,
        "readiness": value.readiness.value,
        "actionable": value.actionable,
        "original_filename": value.original_filename,
        "claimed_source_catalog_url": value.claimed_source_catalog_url,
        "source_media_type": value.source_media_type,
        "first_seen_at": value.first_seen_at.isoformat(),
        "validated_at": value.validated_at.isoformat(),
        "parser_version": value.parser_version,
        "normalized_codec_version": value.normalized_codec_version,
        "raw_sha256": value.raw_sha256,
        "normalized_sha256": value.normalized_sha256,
        "byte_count": value.byte_count,
        "outer_entry_count": value.outer_entry_count,
        "selected_report_count": value.selected_report_count,
        "quarantined_report_count": value.quarantined_report_count,
        "deferred_report_count": value.deferred_report_count,
        "ignored_entry_count": value.ignored_entry_count,
        "selected_row_count": value.selected_row_count,
        "raw_filename": value.raw_filename,
        "normalized_filename": value.normalized_filename,
    }


def _reference_manifest(value: ReferenceArtifactManifest) -> dict[str, object]:
    return {
        "schema_version": value.schema_version,
        "manifest_id": value.manifest_id,
        "artifact_id": value.artifact_id,
        "dataset": value.dataset,
        "claimed_authority": value.claimed_authority,
        "acquisition_mode": value.acquisition_mode.value,
        "readiness": value.readiness.value,
        "actionable": value.actionable,
        "original_filename": value.original_filename,
        "claimed_report_date": value.claimed_report_date.isoformat(),
        "verified_report_date": (
            value.verified_report_date.isoformat()
            if value.verified_report_date is not None
            else None
        ),
        "claimed_source_catalog_url": value.claimed_source_catalog_url,
        "claimed_download_url": value.claimed_download_url,
        "source_media_type": value.source_media_type,
        "publication_time_status": value.publication_time_status,
        "first_seen_at": value.first_seen_at.isoformat(),
        "validated_at": value.validated_at.isoformat(),
        "parser_version": value.parser_version,
        "source_schema_version": value.source_schema_version,
        "scope_policy_version": value.scope_policy_version,
        "normalized_codec_version": value.normalized_codec_version,
        "compressed_byte_count": value.compressed_byte_count,
        "uncompressed_byte_count": value.uncompressed_byte_count,
        "raw_sha256": value.raw_sha256,
        "uncompressed_sha256": value.uncompressed_sha256,
        "normalized_sha256": value.normalized_sha256,
        "header_sha256": value.header_sha256,
        "raw_row_count": value.raw_row_count,
        "parsed_row_count": value.parsed_row_count,
        "retained_unverified_equity_count": (
            value.retained_unverified_equity_count
        ),
        "excluded_non_equity_count": value.excluded_non_equity_count,
        "excluded_test_security_count": value.excluded_test_security_count,
        "excluded_alternative_venue_count": (
            value.excluded_alternative_venue_count
        ),
        "ordered_row_digest": value.ordered_row_digest,
        "raw_filename": value.raw_filename,
        "normalized_filename": value.normalized_filename,
    }


def _reg1(value: Reg1Observation) -> dict[str, object]:
    return {
        "row_ref": _row_ref(value.row_ref),
        "publication_date_claim": value.publication_date_claim.isoformat(),
        "effective_session": (
            value.effective_session.isoformat()
            if value.effective_session is not None
            else None
        ),
        "status": value.status,
        "nse_exclusive": value.nse_exclusive,
        "gsm_code": value.gsm_code,
        "long_term_asm_code": value.long_term_asm_code,
        "short_term_asm_code": value.short_term_asm_code,
        "esm_code": value.esm_code,
        "indicator_codes": [list(item) for item in value.indicator_codes],
    }


def _band(value: BandObservation) -> dict[str, object]:
    return {
        "row_ref": _row_ref(value.row_ref),
        "claimed_date": value.claimed_date.isoformat(),
        "effective_session": (
            value.effective_session.isoformat()
            if value.effective_session is not None
            else None
        ),
        "band": value.band,
    }


def _band_change(value: BandChangeObservation) -> dict[str, object]:
    return {
        "row_ref": _row_ref(value.row_ref),
        "claimed_effective_date": value.claimed_effective_date.isoformat(),
        "from_band": value.from_band,
        "to_band": value.to_band,
    }


def _series_change(value: SeriesChangeObservation) -> dict[str, object]:
    return {
        "row_ref": _row_ref(value.row_ref),
        "symbol": value.symbol,
        "from_series": value.from_series,
        "to_series": value.to_series,
        "effective_date": value.effective_date.isoformat(),
    }


def _orphan(value: OrphanReportKey) -> dict[str, object]:
    return {
        "family": value.family.value,
        "claimed_date": value.claimed_date.isoformat() if value.claimed_date else None,
        "symbol": value.symbol,
        "series": value.series,
        "row_ref": _row_ref(value.row_ref),
    }


def encode_reconciliation(snapshot: CollectionReconciliationSnapshot) -> bytes:
    if type(snapshot) is not CollectionReconciliationSnapshot:
        raise TypeError("reconciliation codec requires an exact snapshot")
    snapshot.verify_content_identity()
    value = {
        "codec_version": RECONCILIATION_CODEC_VERSION,
        "schema_version": snapshot.schema_version,
        "policy_version": snapshot.policy_version,
        "snapshot_id": snapshot.snapshot_id,
        "exchange": snapshot.exchange,
        "segment": snapshot.segment,
        "market_session": snapshot.market_session.isoformat(),
        "cutoff": snapshot.cutoff.isoformat(),
        "calendar_snapshot_id": snapshot.calendar_snapshot_id,
        "security_master_artifact_id": snapshot.security_master_artifact_id,
        "security_master_manifest_id": snapshot.security_master_manifest_id,
        "security_master_claimed_report_date": (
            snapshot.security_master_claimed_report_date.isoformat()
        ),
        "security_master_raw_sha256": snapshot.security_master_raw_sha256,
        "security_master_normalized_sha256": snapshot.security_master_normalized_sha256,
        "security_master_first_seen_at": snapshot.security_master_first_seen_at.isoformat(),
        "security_master_validated_at": snapshot.security_master_validated_at.isoformat(),
        "security_master_manifest": _reference_manifest(
            snapshot.security_master_manifest
        ),
        "daily_bundle_artifact_ids": list(snapshot.daily_bundle_artifact_ids),
        "daily_bundle_manifest_ids": list(snapshot.daily_bundle_manifest_ids),
        "daily_bundle_manifests": [
            _daily_manifest(value) for value in snapshot.daily_bundle_manifests
        ],
        "readiness": snapshot.readiness.value,
        "actionable": snapshot.actionable,
        "global_reason_codes": list(snapshot.global_reason_codes),
        "summary": {
            "retained_row_count": snapshot.retained_row_count,
            "main_scope_count": snapshot.main_scope_count,
            "sme_scope_count": snapshot.sme_scope_count,
            "unsupported_series_count": snapshot.unsupported_series_count,
            "unresolved_count": snapshot.unresolved_count,
            "traded_row_count": snapshot.traded_row_count,
            "orphan_report_key_count": len(snapshot.orphan_report_keys),
        },
        "report_bindings": [_binding(value) for value in snapshot.report_bindings],
        "retained_source_row_ids": list(snapshot.retained_source_row_ids),
        "entries": [
            {
                "source_record_id": entry.source_record_id,
                "master_row_sha256": entry.master_row_sha256,
                "symbol": entry.symbol,
                "series": entry.series,
                "financial_instrument_id": entry.financial_instrument_id,
                "validated_isin": entry.validated_isin,
                "scope": entry.scope.value,
                "disposition": entry.disposition.value,
                "reason_codes": list(entry.reason_codes),
                "reg1_observations": [_reg1(value) for value in entry.reg1_observations],
                "effective_reg1": (
                    _reg1(entry.effective_reg1)
                    if entry.effective_reg1 is not None
                    else None
                ),
                "complete_band_observations": [
                    _band(value) for value in entry.complete_band_observations
                ],
                "effective_complete_band": (
                    _band(entry.effective_complete_band)
                    if entry.effective_complete_band is not None
                    else None
                ),
                "target_sme_band": (
                    _band(entry.target_sme_band)
                    if entry.target_sme_band is not None
                    else None
                ),
                "udiff_trade_row": (
                    _row_ref(entry.udiff_trade_row)
                    if entry.udiff_trade_row is not None
                    else None
                ),
                "full_delivery_row": (
                    _row_ref(entry.full_delivery_row)
                    if entry.full_delivery_row is not None
                    else None
                ),
                "target_band_changes": [
                    _band_change(value) for value in entry.target_band_changes
                ],
                "relevant_series_changes": [
                    _series_change(value) for value in entry.relevant_series_changes
                ],
            }
            for entry in snapshot.entries
        ],
        "orphan_report_keys": [_orphan(value) for value in snapshot.orphan_report_keys],
    }
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
