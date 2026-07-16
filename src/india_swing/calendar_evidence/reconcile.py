from __future__ import annotations

import hashlib
from collections import Counter
from dataclasses import fields
from datetime import date, datetime

from india_swing.daily_reports.codec import encode_daily_bundle
from india_swing.daily_reports.artifact_store import (
    verify_stored_daily_bundle_provenance,
)
from india_swing.daily_reports.models import (
    NSE_DAILY_BUNDLE_ARTIFACT_SCHEMA_VERSION,
    NSE_DAILY_BUNDLE_CODEC_VERSION,
    NSE_DAILY_BUNDLE_DATASET,
    NSE_DAILY_BUNDLE_PARSER_VERSION,
    BundleEntryDisposition,
    DailyBundleArtifactManifest,
    DailyReportIntegrityError,
    DailyReportFamily,
    ParsedDailyReport,
    ParsedNseDailyBundle,
    ReportDateRole,
    ReportDateStatus,
    StoredDailyBundleArtifact,
)
from india_swing.identity import content_id

from .models import (
    CalendarEvidenceIntegrityError,
    DailyReportEvidenceRef,
    ObservedMarketDate,
    ObservedMarketDateArtifact,
)
from .policy import final_report_not_before


_REPORT_FAMILY_ORDER = (
    DailyReportFamily.UDIFF_BHAVCOPY,
    DailyReportFamily.FULL_BHAVCOPY_DELIVERY,
)


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _require_aware(value: datetime, field_name: str) -> None:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


def _source_artifact_identity(
    manifest: DailyBundleArtifactManifest,
) -> dict[str, object]:
    return {
        "schema_version": manifest.schema_version,
        "dataset": manifest.dataset,
        "claimed_authority": manifest.claimed_authority,
        "acquisition_mode": manifest.acquisition_mode,
        "readiness": manifest.readiness,
        "actionable": manifest.actionable,
        "original_filename": manifest.original_filename,
        "claimed_source_catalog_url": manifest.claimed_source_catalog_url,
        "source_media_type": manifest.source_media_type,
        "parser_version": manifest.parser_version,
        "normalized_codec_version": manifest.normalized_codec_version,
        "raw_sha256": manifest.raw_sha256,
        "normalized_sha256": manifest.normalized_sha256,
        "byte_count": manifest.byte_count,
        "outer_entry_count": manifest.outer_entry_count,
        "selected_report_count": manifest.selected_report_count,
        "quarantined_report_count": manifest.quarantined_report_count,
        "deferred_report_count": manifest.deferred_report_count,
        "ignored_entry_count": manifest.ignored_entry_count,
        "selected_row_count": manifest.selected_row_count,
        "raw_filename": manifest.raw_filename,
        "normalized_filename": manifest.normalized_filename,
    }


def _source_manifest_identity(
    manifest: DailyBundleArtifactManifest,
) -> dict[str, object]:
    return {
        item.name: getattr(manifest, item.name)
        for item in fields(DailyBundleArtifactManifest)
        if item.name != "manifest_id"
    }


def _validate_source_artifact(stored: StoredDailyBundleArtifact) -> None:
    if type(stored) is not StoredDailyBundleArtifact:
        raise TypeError("source must be an exact StoredDailyBundleArtifact")
    manifest = stored.manifest
    parsed = stored.parsed
    if type(manifest) is not DailyBundleArtifactManifest:
        raise TypeError("source manifest must be an exact daily-bundle manifest")
    if type(parsed) is not ParsedNseDailyBundle:
        raise TypeError("source payload must be an exact parsed daily bundle")
    if type(stored.raw_bytes) is not bytes or type(stored.normalized_bytes) is not bytes:
        raise TypeError("stored daily-bundle payloads must be exact bytes")

    if (
        manifest.schema_version != NSE_DAILY_BUNDLE_ARTIFACT_SCHEMA_VERSION
        or manifest.dataset != NSE_DAILY_BUNDLE_DATASET
        or manifest.parser_version != NSE_DAILY_BUNDLE_PARSER_VERSION
        or manifest.normalized_codec_version != NSE_DAILY_BUNDLE_CODEC_VERSION
    ):
        raise CalendarEvidenceIntegrityError(
            "daily-bundle source uses an unsupported schema or parser contract"
        )
    if manifest.raw_filename != "bundle.zip" or manifest.normalized_filename != "normalized.json":
        raise CalendarEvidenceIntegrityError(
            "daily-bundle source archive filenames are not canonical"
        )
    if _sha256(stored.raw_bytes) != manifest.raw_sha256:
        raise CalendarEvidenceIntegrityError("daily-bundle raw hash mismatch")
    if len(stored.raw_bytes) != manifest.byte_count:
        raise CalendarEvidenceIntegrityError("daily-bundle byte count mismatch")
    if (
        parsed.raw_sha256 != manifest.raw_sha256
        or parsed.byte_count != manifest.byte_count
        or parsed.original_filename != manifest.original_filename
    ):
        raise CalendarEvidenceIntegrityError(
            "parsed daily bundle and source manifest disagree"
        )
    expected_normalized = encode_daily_bundle(parsed)
    if stored.normalized_bytes != expected_normalized:
        raise CalendarEvidenceIntegrityError(
            "daily-bundle normalized payload is not deterministic"
        )
    if _sha256(expected_normalized) != manifest.normalized_sha256:
        raise CalendarEvidenceIntegrityError("daily-bundle normalized hash mismatch")

    entry_counts = Counter(entry.disposition for entry in parsed.entries)
    if (
        manifest.outer_entry_count != len(parsed.entries)
        or manifest.selected_report_count
        != entry_counts[BundleEntryDisposition.SELECTED_VALIDATED]
        or manifest.quarantined_report_count
        != entry_counts[
            BundleEntryDisposition.QUARANTINED_INTEROPERABILITY_SECURITY_MASTER
        ]
        or manifest.deferred_report_count
        != entry_counts[BundleEntryDisposition.DEFERRED_NSE_ONLY_SECURITY_MASTER]
        or manifest.ignored_entry_count
        != entry_counts[BundleEntryDisposition.IGNORED_UNAPPROVED]
        or manifest.selected_row_count
        != sum(
            report.row_count
            for report in parsed.reports
            if report.disposition is BundleEntryDisposition.SELECTED_VALIDATED
        )
    ):
        raise CalendarEvidenceIntegrityError(
            "daily-bundle manifest disposition counts do not match its payload"
        )

    expected_artifact_id = content_id(_source_artifact_identity(manifest), length=64)
    expected_manifest_id = content_id(_source_manifest_identity(manifest), length=64)
    if manifest.artifact_id != expected_artifact_id:
        raise CalendarEvidenceIntegrityError("daily-bundle artifact ID mismatch")
    if manifest.manifest_id != expected_manifest_id:
        raise CalendarEvidenceIntegrityError("daily-bundle manifest ID mismatch")
    try:
        verify_stored_daily_bundle_provenance(stored)
    except DailyReportIntegrityError as exc:
        raise CalendarEvidenceIntegrityError(
            "daily-bundle source does not match sealed provenance"
        ) from exc


def build_observed_market_date_artifact(
    stored: StoredDailyBundleArtifact,
    *,
    cutoff: datetime,
) -> ObservedMarketDateArtifact:
    """Reconcile positive trade dates without constructing a trading calendar."""

    _require_aware(cutoff, "cutoff")
    _validate_source_artifact(stored)
    manifest = stored.manifest
    if manifest.validated_at > cutoff:
        raise CalendarEvidenceIntegrityError(
            "daily bundle was not validated by the requested cutoff"
        )

    reports_by_date: dict[date, dict[DailyReportFamily, ParsedDailyReport]] = {}
    for report in stored.parsed.reports:
        if type(report) is not ParsedDailyReport:
            raise TypeError("daily-bundle reports must remain exact parsed report values")
        if report.family not in _REPORT_FAMILY_ORDER:
            continue
        if (
            report.disposition is not BundleEntryDisposition.SELECTED_VALIDATED
            or report.date_status is not ReportDateStatus.ROW_CONFIRMED
            or report.date_role is not ReportDateRole.TRADE_DATE
            or report.claimed_report_date is None
            or report.confirmed_row_dates != (report.claimed_report_date,)
            or report.row_count <= 0
        ):
            raise CalendarEvidenceIntegrityError(
                "trade-date evidence must be selected, row-confirmed, and non-empty"
            )
        trade_date = report.confirmed_row_dates[0]
        family_reports = reports_by_date.setdefault(trade_date, {})
        if report.family in family_reports:
            raise CalendarEvidenceIntegrityError(
                "trade date contains a duplicate report family"
            )
        family_reports[report.family] = report

    if not reports_by_date:
        raise CalendarEvidenceIntegrityError(
            "daily bundle contains no positive CM trade-date evidence"
        )

    observations: list[ObservedMarketDate] = []
    for trade_date in sorted(reports_by_date):
        if manifest.validated_at < final_report_not_before(trade_date):
            raise CalendarEvidenceIntegrityError(
                "final trade-date reports were validated before the conservative event boundary"
            )
        family_reports = reports_by_date[trade_date]
        if tuple(family for family in _REPORT_FAMILY_ORDER if family in family_reports) != _REPORT_FAMILY_ORDER:
            raise CalendarEvidenceIntegrityError(
                "each trade date requires exactly one UDiFF/full report pair"
            )
        references = tuple(
            DailyReportEvidenceRef(
                bundle_artifact_id=manifest.artifact_id,
                bundle_manifest_id=manifest.manifest_id,
                family=family,
                source_entry_name=family_reports[family].source_entry_name,
                content_name=family_reports[family].content_name,
                source_entry_sha256=family_reports[family].source_entry_sha256,
                content_sha256=family_reports[family].content_sha256,
                header_sha256=family_reports[family].header_sha256,
                ordered_row_digest=family_reports[family].ordered_row_digest,
                row_count=family_reports[family].row_count,
                trade_date=trade_date,
                knowledge_time=manifest.validated_at,
            )
            for family in _REPORT_FAMILY_ORDER
        )
        observations.append(
            ObservedMarketDate(
                market_date=trade_date,
                report_refs=references,
            )
        )

    return ObservedMarketDateArtifact(
        exchange="NSE",
        segment="CM",
        cutoff=cutoff,
        source_bundle_artifact_id=manifest.artifact_id,
        source_bundle_manifest_id=manifest.manifest_id,
        source_bundle_raw_sha256=manifest.raw_sha256,
        source_bundle_normalized_sha256=manifest.normalized_sha256,
        source_acquisition_mode=manifest.acquisition_mode,
        source_readiness=manifest.readiness,
        source_first_seen_at=manifest.first_seen_at,
        source_validated_at=manifest.validated_at,
        observations=tuple(observations),
    )
