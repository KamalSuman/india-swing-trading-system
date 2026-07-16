from __future__ import annotations

import json

from india_swing.daily_reports.models import DailyBundleArtifactManifest

from .models import (
    HISTORICAL_PRICE_CODEC_VERSION,
    NseEodSessionArtifact,
    PriceReportRef,
    PriceRowRef,
    RawNseEodBar,
)


def _source_manifest(value: DailyBundleArtifactManifest) -> dict[str, object]:
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


def _report_ref(value: PriceReportRef) -> dict[str, object]:
    return {
        "report_ref_id": value.report_ref_id,
        "bundle_artifact_id": value.bundle_artifact_id,
        "bundle_manifest_id": value.bundle_manifest_id,
        "bundle_raw_sha256": value.bundle_raw_sha256,
        "bundle_normalized_sha256": value.bundle_normalized_sha256,
        "family": value.family.value,
        "source_entry_name": value.source_entry_name,
        "content_name": value.content_name,
        "source_entry_sha256": value.source_entry_sha256,
        "content_sha256": value.content_sha256,
        "header_sha256": value.header_sha256,
        "ordered_row_digest": value.ordered_row_digest,
        "claimed_report_date": value.claimed_report_date.isoformat(),
        "confirmed_row_dates": [item.isoformat() for item in value.confirmed_row_dates],
        "date_status": value.date_status.value,
        "date_role": value.date_role.value,
        "first_seen_at": value.first_seen_at.isoformat(),
        "validated_at": value.validated_at.isoformat(),
        "row_count": value.row_count,
    }


def _row_ref(value: PriceRowRef) -> dict[str, object]:
    return {
        "report_ref_id": value.report_ref_id,
        "family": value.family.value,
        "source_row_number": value.source_row_number,
        "row_sha256": value.row_sha256,
        "listing_key": list(value.listing_key),
    }


def _decimal(value: object) -> str | None:
    return None if value is None else str(value)


def _bar(value: RawNseEodBar) -> dict[str, object]:
    return {
        "bar_id": value.bar_id,
        "market_session": value.market_session.isoformat(),
        "financial_instrument_id": value.financial_instrument_id,
        "validated_isin": value.validated_isin,
        "symbol": value.symbol,
        "series": value.series,
        "session_id": value.session_id,
        "instrument_name": value.instrument_name,
        "open": str(value.open),
        "high": str(value.high),
        "low": str(value.low),
        "close": str(value.close),
        "last": str(value.last),
        "previous_close": str(value.previous_close),
        "volume": value.volume,
        "traded_value": str(value.traded_value),
        "trade_count": value.trade_count,
        "board_lot_quantity": value.board_lot_quantity,
        "full_average_price": _decimal(value.full_average_price),
        "delivery_quantity": value.delivery_quantity,
        "delivery_percent": _decimal(value.delivery_percent),
        "knowledge_time": value.knowledge_time.isoformat(),
        "udiff_row_ref": _row_ref(value.udiff_row_ref),
        "full_delivery_row_ref": (
            _row_ref(value.full_delivery_row_ref)
            if value.full_delivery_row_ref is not None
            else None
        ),
    }


def encode_historical_price_artifact(artifact: NseEodSessionArtifact) -> bytes:
    if type(artifact) is not NseEodSessionArtifact:
        raise TypeError("historical-price codec requires an exact session artifact")
    artifact.verify_content_identity()
    value = {
        "codec_version": HISTORICAL_PRICE_CODEC_VERSION,
        "schema_version": artifact.schema_version,
        "policy_version": artifact.policy_version,
        "artifact_id": artifact.artifact_id,
        "exchange": artifact.exchange,
        "segment": artifact.segment,
        "market_session": artifact.market_session.isoformat(),
        "cutoff": artifact.cutoff.isoformat(),
        "knowledge_time": artifact.knowledge_time.isoformat(),
        "source_bundle_manifest": _source_manifest(artifact.source_bundle_manifest),
        "report_refs": [_report_ref(value) for value in artifact.report_refs],
        "bars": [_bar(value) for value in artifact.bars],
        "price_basis": artifact.price_basis,
        "coverage_scope": artifact.coverage_scope,
        "readiness": artifact.readiness.value,
        "actionable": artifact.actionable,
    }
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
