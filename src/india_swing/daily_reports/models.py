from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path, PurePath

from india_swing.reference.models import ReferenceReadiness
from india_swing.reference_data.models import AcquisitionMode


NSE_DAILY_BUNDLE_DATASET = "nse-daily-multiple-reports"
NSE_DAILY_BUNDLE_ARTIFACT_SCHEMA_VERSION = "nse-daily-bundle-artifact/v1"
NSE_DAILY_BUNDLE_PARSER_VERSION = "nse-daily-bundle-parser/v1"
NSE_DAILY_BUNDLE_CODEC_VERSION = "nse-daily-bundle-json/v1"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class DailyReportError(RuntimeError):
    pass


class DailyReportIntegrityError(DailyReportError):
    pass


class DailyReportConflict(DailyReportError):
    pass


class DailyReportNotFound(DailyReportError):
    pass


class DailyReportFamily(str, Enum):
    UDIFF_BHAVCOPY = "UDIFF_BHAVCOPY"
    FULL_BHAVCOPY_DELIVERY = "FULL_BHAVCOPY_DELIVERY"
    SURVEILLANCE_REG1 = "SURVEILLANCE_REG1"
    COMPLETE_PRICE_BANDS = "COMPLETE_PRICE_BANDS"
    SME_PRICE_BANDS = "SME_PRICE_BANDS"
    PRICE_BAND_CHANGES = "PRICE_BAND_CHANGES"
    SERIES_CHANGES = "SERIES_CHANGES"
    SECURITY_MASTER = "SECURITY_MASTER"


class BundleEntryDisposition(str, Enum):
    SELECTED_VALIDATED = "SELECTED_VALIDATED"
    QUARANTINED_INTEROPERABILITY_SECURITY_MASTER = (
        "QUARANTINED_INTEROPERABILITY_SECURITY_MASTER"
    )
    DEFERRED_NSE_ONLY_SECURITY_MASTER = "DEFERRED_NSE_ONLY_SECURITY_MASTER"
    IGNORED_UNAPPROVED = "IGNORED_UNAPPROVED"


class ReportDateStatus(str, Enum):
    ROW_CONFIRMED = "ROW_CONFIRMED"
    FILENAME_CLAIM_ONLY = "FILENAME_CLAIM_ONLY"
    INTERNAL_DATES_ONLY = "INTERNAL_DATES_ONLY"
    NO_DATE_AVAILABLE = "NO_DATE_AVAILABLE"


class ReportDateRole(str, Enum):
    TRADE_DATE = "TRADE_DATE"
    PUBLICATION_DATE_NEXT_SESSION_EFFECTIVE = (
        "PUBLICATION_DATE_NEXT_SESSION_EFFECTIVE"
    )
    CLAIMED_EFFECTIVE_DATE = "CLAIMED_EFFECTIVE_DATE"
    INTERNAL_EFFECTIVE_DATES = "INTERNAL_EFFECTIVE_DATES"
    CLAIMED_REPORT_DATE = "CLAIMED_REPORT_DATE"


def _require_sha256(value: str, field_name: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a full lowercase SHA-256")


def _require_aware(value: datetime, field_name: str) -> None:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


def _require_safe_basename(value: str, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or PurePath(value).name != value
        or "/" in value
        or "\\" in value
    ):
        raise ValueError(f"{field_name} must be a safe basename")


@dataclass(frozen=True, slots=True)
class BundleEntryInventory:
    name: str
    byte_count: int
    compressed_byte_count: int
    compression_method: int
    crc32: int
    sha256: str
    disposition: BundleEntryDisposition
    family: DailyReportFamily | None

    def __post_init__(self) -> None:
        _require_safe_basename(self.name, "bundle entry name")
        for value, field_name in (
            (self.byte_count, "byte_count"),
            (self.compressed_byte_count, "compressed_byte_count"),
        ):
            if type(value) is not int or value < 0:
                raise ValueError(f"{field_name} must be a non-negative integer")
        if type(self.compression_method) is not int or self.compression_method < 0:
            raise ValueError("compression method must be a non-negative integer")
        if type(self.crc32) is not int or not 0 <= self.crc32 <= 0xFFFFFFFF:
            raise ValueError("CRC-32 must be an unsigned 32-bit integer")
        _require_sha256(self.sha256, "entry sha256")
        if not isinstance(self.disposition, BundleEntryDisposition):
            raise TypeError("entry disposition is required")
        if self.disposition is BundleEntryDisposition.IGNORED_UNAPPROVED:
            if self.family is not None:
                raise ValueError("ignored entries cannot claim an approved family")
        elif not isinstance(self.family, DailyReportFamily):
            raise TypeError("approved or quarantined entries require a family")


@dataclass(frozen=True, slots=True)
class ParsedDailyReport:
    source_entry_name: str
    content_name: str
    family: DailyReportFamily
    disposition: BundleEntryDisposition
    claimed_report_date: date | None
    confirmed_row_dates: tuple[date, ...]
    date_status: ReportDateStatus
    date_role: ReportDateRole
    source_entry_sha256: str
    content_sha256: str
    source_entry_byte_count: int
    content_byte_count: int
    header: tuple[str, ...]
    header_sha256: str
    row_count: int
    ordered_row_digest: str
    rows: tuple[tuple[str, ...], ...]

    def __post_init__(self) -> None:
        _require_safe_basename(self.source_entry_name, "source entry name")
        _require_safe_basename(self.content_name, "content name")
        if not isinstance(self.family, DailyReportFamily):
            raise TypeError("report family is required")
        if self.disposition is BundleEntryDisposition.IGNORED_UNAPPROVED:
            raise ValueError("ignored entries cannot be parsed reports")
        if self.claimed_report_date is not None and type(self.claimed_report_date) is not date:
            raise TypeError("claimed report date must be a date or None")
        if type(self.confirmed_row_dates) is not tuple or any(
            type(value) is not date for value in self.confirmed_row_dates
        ):
            raise TypeError("confirmed row dates must be an immutable date tuple")
        if tuple(sorted(set(self.confirmed_row_dates))) != self.confirmed_row_dates:
            raise ValueError("confirmed row dates must be unique and sorted")
        if not isinstance(self.date_status, ReportDateStatus):
            raise TypeError("report date status is required")
        if not isinstance(self.date_role, ReportDateRole):
            raise TypeError("report date role is required")
        if self.date_status is ReportDateStatus.ROW_CONFIRMED:
            if (
                self.claimed_report_date is None
                or self.confirmed_row_dates != (self.claimed_report_date,)
            ):
                raise ValueError("row-confirmed reports must corroborate their filename date")
        elif self.date_status is ReportDateStatus.FILENAME_CLAIM_ONLY:
            if self.claimed_report_date is None or self.confirmed_row_dates:
                raise ValueError("filename-only reports cannot claim confirmed row dates")
        elif self.date_status is ReportDateStatus.INTERNAL_DATES_ONLY:
            if self.claimed_report_date is not None or not self.confirmed_row_dates:
                raise ValueError("internal-date reports require row dates and no filename date")
        elif self.date_status is ReportDateStatus.NO_DATE_AVAILABLE:
            if self.claimed_report_date is not None or self.confirmed_row_dates:
                raise ValueError("date-unavailable reports cannot carry any dates")
        if self.date_status is ReportDateStatus.ROW_CONFIRMED:
            if self.date_role is not ReportDateRole.TRADE_DATE:
                raise ValueError("row-confirmed daily prices must carry the trade-date role")
        elif self.date_status in (
            ReportDateStatus.INTERNAL_DATES_ONLY,
            ReportDateStatus.NO_DATE_AVAILABLE,
        ):
            if self.date_role is not ReportDateRole.INTERNAL_EFFECTIVE_DATES:
                raise ValueError("internal row dates must carry the internal-effective role")
        elif self.date_role in (
            ReportDateRole.TRADE_DATE,
            ReportDateRole.INTERNAL_EFFECTIVE_DATES,
        ):
            raise ValueError("filename-only dates cannot claim confirmed date roles")
        for value, name in (
            (self.source_entry_sha256, "source_entry_sha256"),
            (self.content_sha256, "content_sha256"),
            (self.header_sha256, "header_sha256"),
            (self.ordered_row_digest, "ordered_row_digest"),
        ):
            _require_sha256(value, name)
        for value, name in (
            (self.source_entry_byte_count, "source_entry_byte_count"),
            (self.content_byte_count, "content_byte_count"),
            (self.row_count, "row_count"),
        ):
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if type(self.header) is not tuple or not self.header or any(
            not isinstance(value, str) for value in self.header
        ):
            raise TypeError("report header must be a non-empty immutable text tuple")
        if type(self.rows) is not tuple or any(
            type(row) is not tuple or any(not isinstance(value, str) for value in row)
            for row in self.rows
        ):
            raise TypeError("normalized report rows must be immutable text tuples")
        if self.disposition is BundleEntryDisposition.SELECTED_VALIDATED:
            if len(self.rows) != self.row_count:
                raise ValueError("selected reports must preserve every parsed row")
        elif self.rows:
            raise ValueError("deferred or quarantined reports store summaries only")


@dataclass(frozen=True, slots=True)
class ParsedNseDailyBundle:
    original_filename: str
    raw_sha256: str
    byte_count: int
    entries: tuple[BundleEntryInventory, ...]
    reports: tuple[ParsedDailyReport, ...]

    def __post_init__(self) -> None:
        _require_safe_basename(self.original_filename, "bundle filename")
        _require_sha256(self.raw_sha256, "bundle raw sha256")
        if type(self.byte_count) is not int or self.byte_count <= 0:
            raise ValueError("bundle byte count must be positive")
        if type(self.entries) is not tuple or not self.entries:
            raise ValueError("bundle inventory cannot be empty")
        if any(type(entry) is not BundleEntryInventory for entry in self.entries):
            raise TypeError("bundle entries must be exact inventory values")
        if tuple(sorted(self.entries, key=lambda item: item.name)) != self.entries:
            raise ValueError("bundle inventory must be sorted by name")
        if len({entry.name for entry in self.entries}) != len(self.entries):
            raise ValueError("bundle entry names must be unique")
        if type(self.reports) is not tuple or not self.reports:
            raise ValueError("bundle must contain parsed approved reports")
        if any(type(report) is not ParsedDailyReport for report in self.reports):
            raise TypeError("reports must be exact parsed report values")
        if tuple(sorted(self.reports, key=lambda item: item.source_entry_name)) != self.reports:
            raise ValueError("parsed reports must be sorted by source entry name")
        inventory = {entry.name: entry for entry in self.entries}
        if len({report.source_entry_name for report in self.reports}) != len(self.reports):
            raise ValueError("one parsed report is allowed per source entry")
        for report in self.reports:
            entry = inventory.get(report.source_entry_name)
            if entry is None:
                raise ValueError("parsed report is missing from the outer inventory")
            if entry.family is not report.family or entry.disposition is not report.disposition:
                raise ValueError("parsed report and inventory classification disagree")
            if entry.sha256 != report.source_entry_sha256:
                raise ValueError("parsed report and inventory hash disagree")


@dataclass(frozen=True, slots=True)
class DailyBundleArtifactManifest:
    schema_version: str
    manifest_id: str
    artifact_id: str
    dataset: str
    claimed_authority: str
    acquisition_mode: AcquisitionMode
    readiness: ReferenceReadiness
    actionable: bool
    original_filename: str
    claimed_source_catalog_url: str
    source_media_type: str
    first_seen_at: datetime
    validated_at: datetime
    parser_version: str
    normalized_codec_version: str
    raw_sha256: str
    normalized_sha256: str
    byte_count: int
    outer_entry_count: int
    selected_report_count: int
    quarantined_report_count: int
    deferred_report_count: int
    ignored_entry_count: int
    selected_row_count: int
    raw_filename: str
    normalized_filename: str

    def __post_init__(self) -> None:
        for value, name in (
            (self.manifest_id, "manifest_id"),
            (self.artifact_id, "artifact_id"),
            (self.raw_sha256, "raw_sha256"),
            (self.normalized_sha256, "normalized_sha256"),
        ):
            _require_sha256(value, name)
        if self.dataset != NSE_DAILY_BUNDLE_DATASET or self.claimed_authority != "NSE":
            raise ValueError("unsupported daily-bundle dataset or claimed authority")
        if self.acquisition_mode is not AcquisitionMode.UNVERIFIED_MANUAL_FILE:
            raise ValueError("daily bundle must remain an unverified manual acquisition")
        if self.readiness is not ReferenceReadiness.COLLECTION_ONLY or self.actionable is not False:
            raise ValueError("daily bundle must remain collection-only and non-actionable")
        _require_safe_basename(self.original_filename, "bundle filename")
        if not isinstance(self.claimed_source_catalog_url, str) or not self.claimed_source_catalog_url:
            raise ValueError("claimed source catalog URL is required")
        if self.source_media_type != "application/zip":
            raise ValueError("daily bundle media type must be application/zip")
        _require_aware(self.first_seen_at, "first_seen_at")
        _require_aware(self.validated_at, "validated_at")
        if (
            self.first_seen_at.utcoffset() != timezone.utc.utcoffset(None)
            or self.validated_at.utcoffset() != timezone.utc.utcoffset(None)
        ):
            raise ValueError("daily-bundle manifest timestamps must use UTC")
        object.__setattr__(
            self,
            "first_seen_at",
            self.first_seen_at.astimezone(timezone.utc),
        )
        object.__setattr__(
            self,
            "validated_at",
            self.validated_at.astimezone(timezone.utc),
        )
        if self.validated_at < self.first_seen_at:
            raise ValueError("validated_at cannot precede first_seen_at")
        for value, name in (
            (self.byte_count, "byte_count"),
            (self.outer_entry_count, "outer_entry_count"),
            (self.selected_report_count, "selected_report_count"),
            (self.quarantined_report_count, "quarantined_report_count"),
            (self.deferred_report_count, "deferred_report_count"),
            (self.ignored_entry_count, "ignored_entry_count"),
            (self.selected_row_count, "selected_row_count"),
        ):
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if self.byte_count == 0 or self.outer_entry_count == 0 or self.selected_report_count == 0:
            raise ValueError("daily bundle must contain bytes, entries, and selected reports")
        if (
            self.selected_report_count
            + self.quarantined_report_count
            + self.deferred_report_count
            + self.ignored_entry_count
            != self.outer_entry_count
        ):
            raise ValueError("every outer entry must have exactly one disposition")
        _require_safe_basename(self.raw_filename, "raw archive filename")
        _require_safe_basename(self.normalized_filename, "normalized filename")


@dataclass(frozen=True, slots=True)
class StoredDailyBundleArtifact:
    path: Path
    manifest: DailyBundleArtifactManifest
    parsed: ParsedNseDailyBundle
    raw_bytes: bytes
    normalized_bytes: bytes
