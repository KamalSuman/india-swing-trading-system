from __future__ import annotations

import re
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timezone
from enum import Enum

from india_swing.daily_reports.models import (
    NSE_DAILY_BUNDLE_ARTIFACT_SCHEMA_VERSION,
    NSE_DAILY_BUNDLE_CODEC_VERSION,
    NSE_DAILY_BUNDLE_DATASET,
    NSE_DAILY_BUNDLE_PARSER_VERSION,
    DailyBundleArtifactManifest,
    DailyReportFamily,
    ReportDateRole,
)
from india_swing.calendar_evidence.policy import final_report_not_before
from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference_data.models import (
    NSE_CM_SECURITY_DATASET,
    NSE_CM_SECURITY_PARSER_VERSION,
    NSE_CM_SECURITY_SCOPE_POLICY_VERSION,
    NSE_CM_SECURITY_SOURCE_SCHEMA_VERSION,
    REFERENCE_ARTIFACT_SCHEMA_VERSION,
    REFERENCE_NORMALIZED_CODEC_VERSION,
    ReferenceArtifactManifest,
)


RECONCILIATION_SCHEMA_VERSION = "nse-collection-reconciliation/v1"
RECONCILIATION_POLICY_VERSION = "nse-cm-equity-observation-policy/v1"
RECONCILIATION_CODEC_VERSION = "nse-collection-reconciliation-json/v1"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_REASON = re.compile(r"[A-Z][A-Z0-9_]{1,95}\Z")
_PRICE_BANDS = frozenset({"2", "5", "10", "20", "40", "No Band"})


class ReconciliationError(RuntimeError):
    pass


class ReconciliationIntegrityError(ReconciliationError):
    pass


class EffectiveSessionResolution(str, Enum):
    TRADE_DATE_CONFIRMED = "TRADE_DATE_CONFIRMED"
    CLAIMED_EFFECTIVE_DATE_UNVERIFIED = "CLAIMED_EFFECTIVE_DATE_UNVERIFIED"
    CALENDAR_RESOLVED_FROM_PUBLICATION_CLAIM = (
        "CALENDAR_RESOLVED_FROM_PUBLICATION_CLAIM"
    )
    UNRESOLVED_NO_CALENDAR = "UNRESOLVED_NO_CALENDAR"
    UNRESOLVED_CALENDAR_COVERAGE = "UNRESOLVED_CALENDAR_COVERAGE"
    INTERNAL_EFFECTIVE_DATES = "INTERNAL_EFFECTIVE_DATES"


class ReconciliationScope(str, Enum):
    MAIN_EQ = "MAIN_EQ"
    SME_SM = "SME_SM"
    UNSUPPORTED_SERIES = "UNSUPPORTED_SERIES"


class ReconciliationDisposition(str, Enum):
    UNVERIFIED_MAIN_SCOPE = "UNVERIFIED_MAIN_SCOPE"
    UNVERIFIED_SME_WATCH_SCOPE = "UNVERIFIED_SME_WATCH_SCOPE"
    UNRESOLVED = "UNRESOLVED"
    EXCLUDED_UNSUPPORTED_SERIES = "EXCLUDED_UNSUPPORTED_SERIES"


def _require_sha256(value: str, field_name: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a full lowercase SHA-256")


def _require_aware(value: datetime, field_name: str) -> None:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


def _utc(value: datetime, field_name: str) -> datetime:
    _require_aware(value, field_name)
    return value.astimezone(timezone.utc)


def _require_date(value: date, field_name: str) -> None:
    if type(value) is not date:
        raise TypeError(f"{field_name} must be a date")


def _require_text(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required")


def _require_reasons(values: tuple[str, ...], field_name: str) -> None:
    if type(values) is not tuple:
        raise TypeError(f"{field_name} must be an immutable tuple")
    if tuple(sorted(set(values))) != values:
        raise ValueError(f"{field_name} must be unique and sorted")
    if any(_REASON.fullmatch(value) is None for value in values):
        raise ValueError(f"{field_name} contains an invalid reason code")


def _daily_artifact_identity(
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


def _daily_manifest_identity(
    manifest: DailyBundleArtifactManifest,
) -> dict[str, object]:
    return {
        item.name: getattr(manifest, item.name)
        for item in fields(DailyBundleArtifactManifest)
        if item.name != "manifest_id"
    }


def _daily_manifest_sort_key(
    manifest: DailyBundleArtifactManifest,
) -> tuple[str, str]:
    return (manifest.artifact_id, manifest.manifest_id)


def _reference_artifact_identity(
    manifest: ReferenceArtifactManifest,
) -> dict[str, object]:
    return {
        "schema_version": manifest.schema_version,
        "dataset": manifest.dataset,
        "claimed_authority": manifest.claimed_authority,
        "acquisition_mode": manifest.acquisition_mode,
        "readiness": manifest.readiness,
        "actionable": manifest.actionable,
        "original_filename": manifest.original_filename,
        "claimed_report_date": manifest.claimed_report_date,
        "verified_report_date": manifest.verified_report_date,
        "claimed_source_catalog_url": manifest.claimed_source_catalog_url,
        "claimed_download_url": manifest.claimed_download_url,
        "source_media_type": manifest.source_media_type,
        "parser_version": manifest.parser_version,
        "source_schema_version": manifest.source_schema_version,
        "scope_policy_version": manifest.scope_policy_version,
        "normalized_codec_version": manifest.normalized_codec_version,
        "compressed_byte_count": manifest.compressed_byte_count,
        "uncompressed_byte_count": manifest.uncompressed_byte_count,
        "raw_sha256": manifest.raw_sha256,
        "uncompressed_sha256": manifest.uncompressed_sha256,
        "normalized_sha256": manifest.normalized_sha256,
        "header_sha256": manifest.header_sha256,
        "raw_row_count": manifest.raw_row_count,
        "parsed_row_count": manifest.parsed_row_count,
        "retained_unverified_equity_count": (
            manifest.retained_unverified_equity_count
        ),
        "excluded_non_equity_count": manifest.excluded_non_equity_count,
        "excluded_test_security_count": manifest.excluded_test_security_count,
        "excluded_alternative_venue_count": (
            manifest.excluded_alternative_venue_count
        ),
        "ordered_row_digest": manifest.ordered_row_digest,
        "raw_filename": manifest.raw_filename,
        "normalized_filename": manifest.normalized_filename,
    }


def _reference_manifest_identity(
    manifest: ReferenceArtifactManifest,
) -> dict[str, object]:
    return {
        item.name: getattr(manifest, item.name)
        for item in fields(ReferenceArtifactManifest)
        if item.name != "manifest_id"
    }


@dataclass(frozen=True, slots=True)
class ReportBinding:
    artifact_id: str
    manifest_id: str
    bundle_raw_sha256: str
    bundle_normalized_sha256: str
    family: DailyReportFamily
    source_entry_name: str
    source_entry_sha256: str
    content_sha256: str
    ordered_row_digest: str
    claimed_report_date: date | None
    confirmed_row_dates: tuple[date, ...]
    date_role: ReportDateRole
    effective_session: date | None
    effective_session_resolution: EffectiveSessionResolution
    first_seen_at: datetime
    validated_at: datetime
    row_count: int
    binding_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.artifact_id, "report_binding.artifact_id"),
            (self.manifest_id, "report_binding.manifest_id"),
            (self.bundle_raw_sha256, "report_binding.bundle_raw_sha256"),
            (
                self.bundle_normalized_sha256,
                "report_binding.bundle_normalized_sha256",
            ),
            (self.source_entry_sha256, "report_binding.source_entry_sha256"),
            (self.content_sha256, "report_binding.content_sha256"),
            (self.ordered_row_digest, "report_binding.ordered_row_digest"),
        ):
            _require_sha256(value, name)
        if not isinstance(self.family, DailyReportFamily):
            raise TypeError("report_binding.family must be a DailyReportFamily")
        _require_text(self.source_entry_name, "report_binding.source_entry_name")
        if self.claimed_report_date is not None:
            _require_date(self.claimed_report_date, "report_binding.claimed_report_date")
        if type(self.confirmed_row_dates) is not tuple or any(
            type(value) is not date for value in self.confirmed_row_dates
        ):
            raise TypeError("report binding confirmed dates must be an exact date tuple")
        if tuple(sorted(set(self.confirmed_row_dates))) != self.confirmed_row_dates:
            raise ValueError("report binding confirmed dates must be unique and sorted")
        if not isinstance(self.date_role, ReportDateRole):
            raise TypeError("report_binding.date_role must be a ReportDateRole")
        if self.effective_session is not None:
            _require_date(self.effective_session, "report_binding.effective_session")
        if not isinstance(
            self.effective_session_resolution,
            EffectiveSessionResolution,
        ):
            raise TypeError("report binding requires an effective-session resolution")
        object.__setattr__(
            self,
            "first_seen_at",
            _utc(self.first_seen_at, "report_binding.first_seen_at"),
        )
        object.__setattr__(
            self,
            "validated_at",
            _utc(self.validated_at, "report_binding.validated_at"),
        )
        if self.validated_at < self.first_seen_at:
            raise ValueError("report validation cannot precede first observation")
        if type(self.row_count) is not int or self.row_count < 0:
            raise ValueError("report_binding.row_count must be non-negative")

        resolution = self.effective_session_resolution
        if resolution in {
            EffectiveSessionResolution.TRADE_DATE_CONFIRMED,
            EffectiveSessionResolution.CLAIMED_EFFECTIVE_DATE_UNVERIFIED,
            EffectiveSessionResolution.CALENDAR_RESOLVED_FROM_PUBLICATION_CLAIM,
        } and self.effective_session is None:
            raise ValueError("resolved report bindings require an effective session")
        if resolution in {
            EffectiveSessionResolution.UNRESOLVED_NO_CALENDAR,
            EffectiveSessionResolution.UNRESOLVED_CALENDAR_COVERAGE,
            EffectiveSessionResolution.INTERNAL_EFFECTIVE_DATES,
        } and self.effective_session is not None:
            raise ValueError("unresolved or multi-date report bindings cannot carry one session")

        expected_roles = {
            DailyReportFamily.UDIFF_BHAVCOPY: ReportDateRole.TRADE_DATE,
            DailyReportFamily.FULL_BHAVCOPY_DELIVERY: ReportDateRole.TRADE_DATE,
            DailyReportFamily.SURVEILLANCE_REG1: (
                ReportDateRole.PUBLICATION_DATE_NEXT_SESSION_EFFECTIVE
            ),
            DailyReportFamily.COMPLETE_PRICE_BANDS: (
                ReportDateRole.PUBLICATION_DATE_NEXT_SESSION_EFFECTIVE
            ),
            DailyReportFamily.SME_PRICE_BANDS: ReportDateRole.CLAIMED_EFFECTIVE_DATE,
            DailyReportFamily.PRICE_BAND_CHANGES: ReportDateRole.CLAIMED_EFFECTIVE_DATE,
            DailyReportFamily.SERIES_CHANGES: ReportDateRole.INTERNAL_EFFECTIVE_DATES,
        }
        if expected_roles.get(self.family) is not self.date_role:
            raise ValueError("report family and date role disagree")
        if self.date_role is ReportDateRole.TRADE_DATE:
            if (
                self.claimed_report_date is None
                or self.confirmed_row_dates != (self.claimed_report_date,)
                or resolution is not EffectiveSessionResolution.TRADE_DATE_CONFIRMED
                or self.effective_session != self.claimed_report_date
            ):
                raise ValueError("trade-date binding has inconsistent session semantics")
            if self.validated_at < final_report_not_before(self.claimed_report_date):
                raise ValueError(
                    "final report binding predates its conservative event boundary"
                )
        elif self.date_role is ReportDateRole.CLAIMED_EFFECTIVE_DATE:
            if (
                self.claimed_report_date is None
                or self.confirmed_row_dates
                or resolution
                is not EffectiveSessionResolution.CLAIMED_EFFECTIVE_DATE_UNVERIFIED
                or self.effective_session != self.claimed_report_date
            ):
                raise ValueError("claimed-effective binding has inconsistent semantics")
        elif self.date_role is ReportDateRole.PUBLICATION_DATE_NEXT_SESSION_EFFECTIVE:
            if self.claimed_report_date is None or self.confirmed_row_dates or resolution not in {
                EffectiveSessionResolution.CALENDAR_RESOLVED_FROM_PUBLICATION_CLAIM,
                EffectiveSessionResolution.UNRESOLVED_NO_CALENDAR,
                EffectiveSessionResolution.UNRESOLVED_CALENDAR_COVERAGE,
            }:
                raise ValueError("publication-date binding has inconsistent semantics")
            if (
                resolution
                is EffectiveSessionResolution.CALENDAR_RESOLVED_FROM_PUBLICATION_CLAIM
                and (
                    self.effective_session is None
                    or self.effective_session <= self.claimed_report_date
                )
            ):
                raise ValueError("resolved publication state must use a later session")
        elif self.date_role is ReportDateRole.INTERNAL_EFFECTIVE_DATES:
            if (
                self.claimed_report_date is not None
                or resolution is not EffectiveSessionResolution.INTERNAL_EFFECTIVE_DATES
            ):
                raise ValueError("internal-date binding has inconsistent semantics")

        object.__setattr__(self, "binding_id", self._calculated_binding_id())

    def _calculated_binding_id(self) -> str:
        return content_id(
            {
                "artifact_id": self.artifact_id,
                "manifest_id": self.manifest_id,
                "bundle_raw_sha256": self.bundle_raw_sha256,
                "bundle_normalized_sha256": self.bundle_normalized_sha256,
                "family": self.family,
                "source_entry_name": self.source_entry_name,
                "source_entry_sha256": self.source_entry_sha256,
                "content_sha256": self.content_sha256,
                "ordered_row_digest": self.ordered_row_digest,
                "claimed_report_date": self.claimed_report_date,
                "confirmed_row_dates": self.confirmed_row_dates,
                "date_role": self.date_role,
                "effective_session": self.effective_session,
                "effective_session_resolution": self.effective_session_resolution,
                "first_seen_at": self.first_seen_at,
                "validated_at": self.validated_at,
                "row_count": self.row_count,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.binding_id != self._calculated_binding_id():
            raise ReconciliationIntegrityError("report binding identity verification failed")


@dataclass(frozen=True, slots=True)
class EvidenceRowRef:
    binding_id: str
    family: DailyReportFamily
    source_row_number: int
    row_sha256: str
    listing_keys: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        _require_sha256(self.binding_id, "evidence_row.binding_id")
        if not isinstance(self.family, DailyReportFamily):
            raise TypeError("evidence_row.family must be a DailyReportFamily")
        if type(self.source_row_number) is not int or self.source_row_number < 2:
            raise ValueError("evidence row number must include the header offset")
        _require_sha256(self.row_sha256, "evidence_row.row_sha256")
        if type(self.listing_keys) is not tuple or not self.listing_keys:
            raise TypeError("evidence row listing keys must be a non-empty tuple")
        if any(
            type(key) is not tuple
            or len(key) != 2
            or any(not isinstance(value, str) or not value for value in key)
            for key in self.listing_keys
        ):
            raise TypeError("evidence row listing keys must be exact text pairs")
        if tuple(sorted(set(self.listing_keys))) != self.listing_keys:
            raise ValueError("evidence row listing keys must be unique and sorted")


@dataclass(frozen=True, slots=True)
class Reg1Observation:
    row_ref: EvidenceRowRef
    publication_date_claim: date
    effective_session: date | None
    status: str
    nse_exclusive: str
    gsm_code: str
    long_term_asm_code: str
    short_term_asm_code: str
    esm_code: str
    indicator_codes: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        if type(self.row_ref) is not EvidenceRowRef:
            raise TypeError("REG1 observation requires an exact row reference")
        if self.row_ref.family is not DailyReportFamily.SURVEILLANCE_REG1:
            raise ValueError("REG1 observation row family is invalid")
        _require_date(self.publication_date_claim, "REG1 publication date")
        if self.effective_session is not None:
            _require_date(self.effective_session, "REG1 effective session")
        if self.status not in {"A", "I", "S"}:
            raise ValueError("REG1 status is outside the pinned domain")
        if self.nse_exclusive not in {"N", "Y"}:
            raise ValueError("REG1 NSE-exclusive flag is outside the pinned domain")
        for value, name in (
            (self.gsm_code, "gsm_code"),
            (self.long_term_asm_code, "long_term_asm_code"),
            (self.short_term_asm_code, "short_term_asm_code"),
            (self.esm_code, "esm_code"),
        ):
            _require_text(value, f"REG1 {name}")
        if type(self.indicator_codes) is not tuple or any(
            type(item) is not tuple
            or len(item) != 2
            or any(not isinstance(value, str) for value in item)
            for item in self.indicator_codes
        ):
            raise TypeError("REG1 indicators must be immutable text pairs")
        names = tuple(item[0] for item in self.indicator_codes)
        if names != tuple(sorted(set(names))):
            raise ValueError("REG1 indicator names must be unique and sorted")


@dataclass(frozen=True, slots=True)
class BandObservation:
    row_ref: EvidenceRowRef
    claimed_date: date
    effective_session: date | None
    band: str

    def __post_init__(self) -> None:
        if type(self.row_ref) is not EvidenceRowRef:
            raise TypeError("band observation requires an exact row reference")
        if self.row_ref.family not in {
            DailyReportFamily.COMPLETE_PRICE_BANDS,
            DailyReportFamily.SME_PRICE_BANDS,
        }:
            raise ValueError("band observation row family is invalid")
        _require_date(self.claimed_date, "band claimed date")
        if self.effective_session is not None:
            _require_date(self.effective_session, "band effective session")
        if self.band not in _PRICE_BANDS:
            raise ValueError("band value is outside the pinned domain")


@dataclass(frozen=True, slots=True)
class BandChangeObservation:
    row_ref: EvidenceRowRef
    claimed_effective_date: date
    from_band: str
    to_band: str

    def __post_init__(self) -> None:
        if type(self.row_ref) is not EvidenceRowRef:
            raise TypeError("band-change observation requires an exact row reference")
        if self.row_ref.family is not DailyReportFamily.PRICE_BAND_CHANGES:
            raise ValueError("band-change observation row family is invalid")
        _require_date(
            self.claimed_effective_date,
            "band-change claimed effective date",
        )
        if self.from_band not in _PRICE_BANDS or self.to_band not in _PRICE_BANDS:
            raise ValueError("band-change value is outside the pinned domain")
        if self.from_band == self.to_band:
            raise ValueError("band-change observation must change the band")


@dataclass(frozen=True, slots=True)
class SeriesChangeObservation:
    row_ref: EvidenceRowRef
    symbol: str
    from_series: str
    to_series: str
    effective_date: date

    def __post_init__(self) -> None:
        if type(self.row_ref) is not EvidenceRowRef:
            raise TypeError("series-change observation requires an exact row reference")
        if self.row_ref.family is not DailyReportFamily.SERIES_CHANGES:
            raise ValueError("series-change observation row family is invalid")
        for value, name in (
            (self.symbol, "series-change symbol"),
            (self.from_series, "series-change from series"),
            (self.to_series, "series-change to series"),
        ):
            _require_text(value, name)
        if self.from_series == self.to_series:
            raise ValueError("series-change observation must change series")
        _require_date(self.effective_date, "series-change effective date")


@dataclass(frozen=True, slots=True)
class OrphanReportKey:
    family: DailyReportFamily
    claimed_date: date | None
    symbol: str
    series: str
    row_ref: EvidenceRowRef

    def __post_init__(self) -> None:
        if not isinstance(self.family, DailyReportFamily):
            raise TypeError("orphan family must be a DailyReportFamily")
        if self.claimed_date is not None:
            _require_date(self.claimed_date, "orphan claimed date")
        _require_text(self.symbol, "orphan symbol")
        _require_text(self.series, "orphan series")
        if type(self.row_ref) is not EvidenceRowRef:
            raise TypeError("orphan requires an exact row reference")
        if self.row_ref.family is not self.family:
            raise ValueError("orphan family and row reference disagree")


@dataclass(frozen=True, slots=True)
class ReconciledListingEvidence:
    source_record_id: str
    master_row_sha256: str
    symbol: str
    series: str
    financial_instrument_id: int
    validated_isin: str | None
    scope: ReconciliationScope
    disposition: ReconciliationDisposition
    reason_codes: tuple[str, ...]
    reg1_observations: tuple[Reg1Observation, ...]
    effective_reg1: Reg1Observation | None
    complete_band_observations: tuple[BandObservation, ...]
    effective_complete_band: BandObservation | None
    target_sme_band: BandObservation | None
    udiff_trade_row: EvidenceRowRef | None
    full_delivery_row: EvidenceRowRef | None
    target_band_changes: tuple[BandChangeObservation, ...]
    relevant_series_changes: tuple[SeriesChangeObservation, ...]

    def __post_init__(self) -> None:
        _require_sha256(self.source_record_id, "reconciled source_record_id")
        _require_sha256(self.master_row_sha256, "reconciled master row hash")
        _require_text(self.symbol, "reconciled symbol")
        _require_text(self.series, "reconciled series")
        if type(self.financial_instrument_id) is not int or self.financial_instrument_id <= 0:
            raise ValueError("reconciled financial instrument ID must be positive")
        if self.validated_isin is not None:
            _require_text(self.validated_isin, "reconciled validated ISIN")
        if not isinstance(self.scope, ReconciliationScope):
            raise TypeError("reconciled scope is required")
        expected_scope = (
            ReconciliationScope.MAIN_EQ
            if self.series == "EQ"
            else (
                ReconciliationScope.SME_SM
                if self.series == "SM"
                else ReconciliationScope.UNSUPPORTED_SERIES
            )
        )
        if self.scope is not expected_scope:
            raise ValueError("reconciliation scope disagrees with the security series")
        if not isinstance(self.disposition, ReconciliationDisposition):
            raise TypeError("reconciled disposition is required")
        _require_reasons(self.reason_codes, "reconciled reason_codes")
        if not self.reason_codes:
            raise ValueError("collection-only entries require at least one reason code")

        for value, exact_type, name in (
            (self.reg1_observations, Reg1Observation, "reg1_observations"),
            (self.complete_band_observations, BandObservation, "complete_band_observations"),
            (self.target_band_changes, BandChangeObservation, "target_band_changes"),
            (self.relevant_series_changes, SeriesChangeObservation, "relevant_series_changes"),
        ):
            if type(value) is not tuple or any(type(item) is not exact_type for item in value):
                raise TypeError(f"{name} must be an immutable exact-value tuple")
        reg1_sort_key = lambda value: (
            value.publication_date_claim,
            value.row_ref.binding_id,
            value.row_ref.source_row_number,
            value.row_ref.row_sha256,
        )
        band_sort_key = lambda value: (
            value.claimed_date,
            value.row_ref.binding_id,
            value.row_ref.source_row_number,
            value.row_ref.row_sha256,
        )
        band_change_sort_key = lambda value: (
            value.claimed_effective_date,
            value.row_ref.binding_id,
            value.row_ref.source_row_number,
            value.row_ref.row_sha256,
        )
        series_sort_key = lambda value: (
            value.effective_date,
            value.row_ref.binding_id,
            value.row_ref.source_row_number,
            value.row_ref.row_sha256,
        )
        if tuple(sorted(self.reg1_observations, key=reg1_sort_key)) != self.reg1_observations:
            raise ValueError("REG1 observations must be deterministically sorted")
        if tuple(sorted(self.complete_band_observations, key=band_sort_key)) != self.complete_band_observations:
            raise ValueError("complete-band observations must be deterministically sorted")
        if tuple(sorted(self.target_band_changes, key=band_change_sort_key)) != self.target_band_changes:
            raise ValueError("band changes must be deterministically sorted")
        if tuple(sorted(self.relevant_series_changes, key=series_sort_key)) != self.relevant_series_changes:
            raise ValueError("series changes must be deterministically sorted")
        if len({value.row_ref for value in self.reg1_observations}) != len(self.reg1_observations):
            raise ValueError("REG1 observations cannot contain duplicate rows")
        if len({value.row_ref for value in self.complete_band_observations}) != len(
            self.complete_band_observations
        ):
            raise ValueError("complete-band observations cannot contain duplicate rows")
        if len({value.row_ref for value in self.target_band_changes}) != len(
            self.target_band_changes
        ):
            raise ValueError("band-change observations cannot contain duplicate rows")
        if len({value.row_ref for value in self.relevant_series_changes}) != len(
            self.relevant_series_changes
        ):
            raise ValueError("series-change observations cannot contain duplicate rows")
        if self.effective_reg1 is not None and type(self.effective_reg1) is not Reg1Observation:
            raise TypeError("effective_reg1 must be an exact REG1 observation")
        if self.effective_complete_band is not None and type(
            self.effective_complete_band
        ) is not BandObservation:
            raise TypeError("effective_complete_band must be an exact band observation")
        if self.target_sme_band is not None and type(self.target_sme_band) is not BandObservation:
            raise TypeError("target_sme_band must be an exact band observation")
        for value, name in (
            (self.udiff_trade_row, "udiff_trade_row"),
            (self.full_delivery_row, "full_delivery_row"),
        ):
            if value is not None and type(value) is not EvidenceRowRef:
                raise TypeError(f"{name} must be an exact row reference")

        listing_key = (self.symbol, self.series)
        owned_row_references = [
            value.row_ref for value in self.reg1_observations
        ]
        owned_row_references.extend(
            value.row_ref for value in self.complete_band_observations
        )
        if self.target_sme_band is not None:
            owned_row_references.append(self.target_sme_band.row_ref)
        if self.udiff_trade_row is not None:
            owned_row_references.append(self.udiff_trade_row)
        if self.full_delivery_row is not None:
            owned_row_references.append(self.full_delivery_row)
        owned_row_references.extend(
            value.row_ref for value in self.target_band_changes
        )
        owned_row_references.extend(
            value.row_ref for value in self.relevant_series_changes
        )
        if any(listing_key not in reference.listing_keys for reference in owned_row_references):
            raise ValueError("evidence row listing key does not belong to its entry")

        if any(
            value.row_ref.family is not DailyReportFamily.SURVEILLANCE_REG1
            for value in self.reg1_observations
        ):
            raise ValueError("REG1 entry field contains another report family")
        if any(
            value.row_ref.family is not DailyReportFamily.COMPLETE_PRICE_BANDS
            for value in self.complete_band_observations
        ):
            raise ValueError("complete-band entry field contains another report family")
        if (
            self.target_sme_band is not None
            and self.target_sme_band.row_ref.family
            is not DailyReportFamily.SME_PRICE_BANDS
        ):
            raise ValueError("target SME field contains another report family")
        if (
            self.udiff_trade_row is not None
            and self.udiff_trade_row.family is not DailyReportFamily.UDIFF_BHAVCOPY
        ):
            raise ValueError("UDiFF entry field contains another report family")
        if (
            self.full_delivery_row is not None
            and self.full_delivery_row.family
            is not DailyReportFamily.FULL_BHAVCOPY_DELIVERY
        ):
            raise ValueError("full-delivery entry field contains another report family")
        if any(
            value.row_ref.family is not DailyReportFamily.PRICE_BAND_CHANGES
            for value in self.target_band_changes
        ):
            raise ValueError("band-change entry field contains another report family")
        if any(
            value.row_ref.family is not DailyReportFamily.SERIES_CHANGES
            or value.symbol != self.symbol
            or self.series not in {value.from_series, value.to_series}
            for value in self.relevant_series_changes
        ):
            raise ValueError("series-change entry field is not relevant to its listing")

        if self.effective_reg1 is not None and self.effective_reg1 not in self.reg1_observations:
            raise ValueError("effective REG1 evidence must be among candidate observations")
        if (
            self.effective_complete_band is not None
            and self.effective_complete_band not in self.complete_band_observations
        ):
            raise ValueError("effective complete-band evidence must be among observations")
        if self.scope is ReconciliationScope.UNSUPPORTED_SERIES:
            if self.disposition is not ReconciliationDisposition.EXCLUDED_UNSUPPORTED_SERIES:
                raise ValueError("unsupported series require the explicit excluded disposition")
        elif self.disposition is ReconciliationDisposition.EXCLUDED_UNSUPPORTED_SERIES:
            raise ValueError("supported series cannot use the unsupported disposition")
        if self.disposition is ReconciliationDisposition.UNVERIFIED_MAIN_SCOPE:
            if self.scope is not ReconciliationScope.MAIN_EQ:
                raise ValueError("main-scope disposition requires EQ scope")
            if self.effective_reg1 is None or self.effective_complete_band is None:
                raise ValueError("resolved main scope requires REG1 and complete-band evidence")
        if self.disposition is ReconciliationDisposition.UNVERIFIED_SME_WATCH_SCOPE:
            if self.scope is not ReconciliationScope.SME_SM:
                raise ValueError("SME watch disposition requires SM scope")
            if (
                self.effective_reg1 is None
                or self.effective_complete_band is None
                or self.target_sme_band is None
            ):
                raise ValueError("resolved SME scope requires REG1 and both band reports")


def _binding_sort_key(binding: ReportBinding) -> tuple[str, str, str, str]:
    return (
        binding.family.value,
        binding.claimed_report_date.isoformat() if binding.claimed_report_date else "",
        binding.source_entry_name,
        binding.binding_id,
    )


def _orphan_sort_key(orphan: OrphanReportKey) -> tuple[str, str, str, str, str]:
    return (
        orphan.family.value,
        orphan.claimed_date.isoformat() if orphan.claimed_date else "",
        orphan.symbol,
        orphan.series,
        orphan.row_ref.row_sha256,
    )


@dataclass(frozen=True, slots=True)
class CollectionReconciliationSnapshot:
    exchange: str
    segment: str
    market_session: date
    cutoff: datetime
    calendar_snapshot_id: str | None
    security_master_manifest: ReferenceArtifactManifest
    daily_bundle_manifests: tuple[DailyBundleArtifactManifest, ...]
    report_bindings: tuple[ReportBinding, ...]
    retained_source_row_ids: tuple[str, ...]
    entries: tuple[ReconciledListingEvidence, ...]
    orphan_report_keys: tuple[OrphanReportKey, ...]
    global_reason_codes: tuple[str, ...]
    readiness: ReferenceReadiness
    actionable: bool
    schema_version: str = RECONCILIATION_SCHEMA_VERSION
    policy_version: str = RECONCILIATION_POLICY_VERSION
    snapshot_id: str = field(init=False)

    def __post_init__(self) -> None:
        if self.exchange != "NSE" or self.segment != "CM":
            raise ValueError("reconciliation scope must be NSE cash market")
        _require_date(self.market_session, "reconciliation market_session")
        object.__setattr__(
            self,
            "cutoff",
            _utc(self.cutoff, "reconciliation cutoff"),
        )
        if self.cutoff < final_report_not_before(self.market_session):
            raise ValueError(
                "reconciliation cutoff predates the target final-report boundary"
            )
        if self.calendar_snapshot_id is not None:
            _require_sha256(self.calendar_snapshot_id, "reconciliation calendar_snapshot_id")
        if type(self.security_master_manifest) is not ReferenceArtifactManifest:
            raise TypeError("reconciliation requires an exact security-master manifest")
        master = self.security_master_manifest
        if (
            master.schema_version != REFERENCE_ARTIFACT_SCHEMA_VERSION
            or master.dataset != NSE_CM_SECURITY_DATASET
            or master.parser_version != NSE_CM_SECURITY_PARSER_VERSION
            or master.source_schema_version != NSE_CM_SECURITY_SOURCE_SCHEMA_VERSION
            or master.scope_policy_version != NSE_CM_SECURITY_SCOPE_POLICY_VERSION
            or master.normalized_codec_version != REFERENCE_NORMALIZED_CODEC_VERSION
            or master.raw_filename != "source.csv.gz"
            or master.normalized_filename != "normalized.json"
        ):
            raise ValueError("security-master lineage uses an unsupported contract")
        if content_id(_reference_artifact_identity(master), length=64) != master.artifact_id:
            raise ValueError("security-master lineage artifact ID is invalid")
        if content_id(_reference_manifest_identity(master), length=64) != master.manifest_id:
            raise ValueError("security-master lineage manifest ID is invalid")
        if master.claimed_report_date != self.market_session:
            raise ValueError("security-master date must equal the reconciliation session")
        if master.validated_at < master.first_seen_at:
            raise ValueError("security-master validation precedes first observation")
        if master.validated_at > self.cutoff:
            raise ValueError("security-master validation follows the reconciliation cutoff")
        if type(self.daily_bundle_manifests) is not tuple or not self.daily_bundle_manifests:
            raise ValueError("reconciliation requires paired daily-bundle lineage")
        if any(
            type(value) is not DailyBundleArtifactManifest
            for value in self.daily_bundle_manifests
        ):
            raise TypeError("daily-bundle lineage requires exact manifests")
        if tuple(
            sorted(self.daily_bundle_manifests, key=_daily_manifest_sort_key)
        ) != self.daily_bundle_manifests:
            raise ValueError("daily-bundle manifests must be deterministically sorted")
        if len(
            {value.artifact_id for value in self.daily_bundle_manifests}
        ) != len(self.daily_bundle_manifests):
            raise ValueError("daily-bundle artifact lineage must be unique")
        if len(
            {value.manifest_id for value in self.daily_bundle_manifests}
        ) != len(self.daily_bundle_manifests):
            raise ValueError("daily-bundle manifest lineage must be unique")
        for manifest in self.daily_bundle_manifests:
            if (
                manifest.schema_version != NSE_DAILY_BUNDLE_ARTIFACT_SCHEMA_VERSION
                or manifest.dataset != NSE_DAILY_BUNDLE_DATASET
                or manifest.parser_version != NSE_DAILY_BUNDLE_PARSER_VERSION
                or manifest.normalized_codec_version != NSE_DAILY_BUNDLE_CODEC_VERSION
                or manifest.raw_filename != "bundle.zip"
                or manifest.normalized_filename != "normalized.json"
            ):
                raise ValueError("daily-bundle lineage uses an unsupported contract")
            if manifest.validated_at > self.cutoff:
                raise ValueError("daily-bundle lineage follows the reconciliation cutoff")
            if content_id(_daily_artifact_identity(manifest), length=64) != manifest.artifact_id:
                raise ValueError("daily-bundle lineage artifact ID is invalid")
            if content_id(_daily_manifest_identity(manifest), length=64) != manifest.manifest_id:
                raise ValueError("daily-bundle lineage manifest ID is invalid")

        manifest_by_artifact_id = {
            manifest.artifact_id: manifest
            for manifest in self.daily_bundle_manifests
        }
        if type(self.report_bindings) is not tuple or any(
            type(value) is not ReportBinding for value in self.report_bindings
        ):
            raise TypeError("report bindings must be an immutable exact tuple")
        if tuple(sorted(self.report_bindings, key=_binding_sort_key)) != self.report_bindings:
            raise ValueError("report bindings must be deterministically sorted")
        if len({binding.binding_id for binding in self.report_bindings}) != len(
            self.report_bindings
        ):
            raise ValueError("report binding IDs must be unique")
        if len(
            {
                (binding.artifact_id, binding.source_entry_name)
                for binding in self.report_bindings
            }
        ) != len(self.report_bindings):
            raise ValueError("report binding source entries must be unique per artifact")
        for binding in self.report_bindings:
            manifest = manifest_by_artifact_id.get(binding.artifact_id)
            if manifest is None:
                raise ValueError("report binding is absent from daily-bundle lineage")
            if (
                binding.manifest_id != manifest.manifest_id
                or binding.bundle_raw_sha256 != manifest.raw_sha256
                or binding.bundle_normalized_sha256 != manifest.normalized_sha256
                or binding.first_seen_at != manifest.first_seen_at
                or binding.validated_at != manifest.validated_at
            ):
                raise ValueError("report binding disagrees with its paired bundle manifest")
        if any(binding.validated_at > self.cutoff for binding in self.report_bindings):
            raise ValueError("report binding validation follows the reconciliation cutoff")
        if any(
            binding.date_role is ReportDateRole.TRADE_DATE
            and (
                binding.claimed_report_date is None
                or self.cutoff
                < final_report_not_before(binding.claimed_report_date)
            )
            for binding in self.report_bindings
        ):
            raise ValueError("final report binding is future evidence at the cutoff")

        if type(self.retained_source_row_ids) is not tuple or not self.retained_source_row_ids:
            raise ValueError("retained source-row scope cannot be empty")
        if tuple(sorted(set(self.retained_source_row_ids))) != self.retained_source_row_ids:
            raise ValueError("retained source-row IDs must be unique and sorted")
        for value in self.retained_source_row_ids:
            _require_sha256(value, "reconciliation retained source-row ID")
        if type(self.entries) is not tuple or any(
            type(value) is not ReconciledListingEvidence for value in self.entries
        ):
            raise TypeError("reconciliation entries must be an immutable exact tuple")
        if tuple(entry.source_record_id for entry in self.entries) != self.retained_source_row_ids:
            raise ValueError("every retained master row requires exactly one sorted entry")
        if len(self.entries) != master.retained_unverified_equity_count:
            raise ValueError(
                "reconciliation entries do not cover every retained master row"
            )
        retained_listing_keys = {
            (entry.symbol, entry.series) for entry in self.entries
        }
        if len(retained_listing_keys) != len(self.entries):
            raise ValueError("reconciliation listing keys must be unique")
        if len({entry.financial_instrument_id for entry in self.entries}) != len(
            self.entries
        ):
            raise ValueError("reconciliation financial instrument IDs must be unique")

        binding_by_id = {binding.binding_id: binding for binding in self.report_bindings}

        def require_bound_row(reference: EvidenceRowRef) -> None:
            binding = binding_by_id.get(reference.binding_id)
            if binding is None:
                raise ValueError("evidence row is absent from report-binding lineage")
            if reference.family is not binding.family:
                raise ValueError("evidence row family disagrees with its report binding")
            if reference.source_row_number > binding.row_count + 1:
                raise ValueError("evidence row number exceeds its report binding")

        for entry in self.entries:
            row_references = [
                observation.row_ref for observation in entry.reg1_observations
            ]
            row_references.extend(
                observation.row_ref
                for observation in entry.complete_band_observations
            )
            if entry.target_sme_band is not None:
                row_references.append(entry.target_sme_band.row_ref)
            if entry.udiff_trade_row is not None:
                row_references.append(entry.udiff_trade_row)
            if entry.full_delivery_row is not None:
                row_references.append(entry.full_delivery_row)
            row_references.extend(
                observation.row_ref
                for observation in entry.target_band_changes
            )
            row_references.extend(
                observation.row_ref
                for observation in entry.relevant_series_changes
            )
            for reference in row_references:
                require_bound_row(reference)

            for observation in entry.reg1_observations:
                binding = binding_by_id[observation.row_ref.binding_id]
                if (
                    observation.publication_date_claim
                    != binding.claimed_report_date
                    or observation.effective_session != binding.effective_session
                ):
                    raise ValueError("REG1 observation disagrees with its report binding")
            for observation in entry.complete_band_observations:
                binding = binding_by_id[observation.row_ref.binding_id]
                if (
                    observation.claimed_date != binding.claimed_report_date
                    or observation.effective_session != binding.effective_session
                ):
                    raise ValueError("complete-band observation disagrees with its binding")
            if entry.target_sme_band is not None:
                binding = binding_by_id[entry.target_sme_band.row_ref.binding_id]
                if (
                    entry.target_sme_band.claimed_date
                    != binding.claimed_report_date
                    or entry.target_sme_band.effective_session
                    != binding.effective_session
                ):
                    raise ValueError("SME-band observation disagrees with its binding")
            for observation in entry.target_band_changes:
                binding = binding_by_id[observation.row_ref.binding_id]
                if observation.claimed_effective_date != binding.claimed_report_date:
                    raise ValueError("band-change observation disagrees with its binding")
            for observation in entry.relevant_series_changes:
                binding = binding_by_id[observation.row_ref.binding_id]
                if observation.effective_date not in binding.confirmed_row_dates:
                    raise ValueError("series-change observation disagrees with its binding")

            if entry.udiff_trade_row is not None:
                binding = binding_by_id[entry.udiff_trade_row.binding_id]
                if (
                    binding.claimed_report_date != self.market_session
                    or binding.effective_session != self.market_session
                ):
                    raise ValueError("UDiFF entry evidence is not for the target session")
            if entry.full_delivery_row is not None:
                binding = binding_by_id[entry.full_delivery_row.binding_id]
                if (
                    binding.claimed_report_date != self.market_session
                    or binding.effective_session != self.market_session
                    or entry.udiff_trade_row is None
                ):
                    raise ValueError("full-delivery evidence is inconsistent with the target session")
            if (
                entry.effective_reg1 is not None
                and entry.effective_reg1.effective_session != self.market_session
            ):
                raise ValueError("effective REG1 state is not for the target session")
            if (
                entry.effective_complete_band is not None
                and entry.effective_complete_band.effective_session
                != self.market_session
            ):
                raise ValueError("effective complete band is not for the target session")
            if (
                entry.target_sme_band is not None
                and entry.target_sme_band.effective_session != self.market_session
            ):
                raise ValueError("target SME band is not for the target session")
            if any(
                value.claimed_effective_date != self.market_session
                for value in entry.target_band_changes
            ):
                raise ValueError("band-change evidence is not for the target session")
            if any(
                value.effective_date != self.market_session
                for value in entry.relevant_series_changes
            ):
                raise ValueError("series-change evidence is not for the target session")

        if type(self.orphan_report_keys) is not tuple or any(
            type(value) is not OrphanReportKey for value in self.orphan_report_keys
        ):
            raise TypeError("orphan report keys must be an immutable exact tuple")
        if tuple(sorted(self.orphan_report_keys, key=_orphan_sort_key)) != self.orphan_report_keys:
            raise ValueError("orphan report keys must be deterministically sorted")
        if len({_orphan_sort_key(value) for value in self.orphan_report_keys}) != len(
            self.orphan_report_keys
        ):
            raise ValueError("orphan report keys must be unique")
        for orphan in self.orphan_report_keys:
            require_bound_row(orphan.row_ref)
            if orphan.claimed_date != binding_by_id[orphan.row_ref.binding_id].claimed_report_date:
                raise ValueError("orphan claimed date disagrees with its report binding")
            if (orphan.symbol, orphan.series) not in orphan.row_ref.listing_keys:
                raise ValueError("orphan listing key disagrees with its evidence row")
            if (orphan.symbol, orphan.series) in retained_listing_keys:
                raise ValueError("orphan listing key overlaps retained membership")
        _require_reasons(self.global_reason_codes, "reconciliation global_reason_codes")
        if not self.global_reason_codes:
            raise ValueError("collection-only reconciliation requires global blockers")
        if self.readiness is not ReferenceReadiness.COLLECTION_ONLY or self.actionable is not False:
            raise ValueError("reconciliation must remain collection-only and non-actionable")
        if self.schema_version != RECONCILIATION_SCHEMA_VERSION:
            raise ValueError("unsupported reconciliation schema version")
        if self.policy_version != RECONCILIATION_POLICY_VERSION:
            raise ValueError("unsupported reconciliation policy version")
        object.__setattr__(self, "snapshot_id", self._calculated_snapshot_id())

    def _calculated_snapshot_id(self) -> str:
        return content_id(
            {
                "schema_version": self.schema_version,
                "policy_version": self.policy_version,
                "exchange": self.exchange,
                "segment": self.segment,
                "market_session": self.market_session,
                "cutoff": self.cutoff,
                "calendar_snapshot_id": self.calendar_snapshot_id,
                "security_master_manifest": self.security_master_manifest,
                "daily_bundle_manifests": self.daily_bundle_manifests,
                "report_bindings": self.report_bindings,
                "retained_source_row_ids": self.retained_source_row_ids,
                "entries": self.entries,
                "orphan_report_keys": self.orphan_report_keys,
                "global_reason_codes": self.global_reason_codes,
                "readiness": self.readiness,
                "actionable": self.actionable,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if any(type(binding) is not ReportBinding for binding in self.report_bindings):
            raise ReconciliationIntegrityError("report binding graph contains an invalid type")
        for binding in self.report_bindings:
            binding.verify_content_identity()
        if self.snapshot_id != self._calculated_snapshot_id():
            raise ReconciliationIntegrityError("reconciliation identity verification failed")

    @property
    def daily_bundle_artifact_ids(self) -> tuple[str, ...]:
        return tuple(value.artifact_id for value in self.daily_bundle_manifests)

    @property
    def daily_bundle_manifest_ids(self) -> tuple[str, ...]:
        return tuple(value.manifest_id for value in self.daily_bundle_manifests)

    @property
    def security_master_artifact_id(self) -> str:
        return self.security_master_manifest.artifact_id

    @property
    def security_master_manifest_id(self) -> str:
        return self.security_master_manifest.manifest_id

    @property
    def security_master_claimed_report_date(self) -> date:
        return self.security_master_manifest.claimed_report_date

    @property
    def security_master_raw_sha256(self) -> str:
        return self.security_master_manifest.raw_sha256

    @property
    def security_master_normalized_sha256(self) -> str:
        return self.security_master_manifest.normalized_sha256

    @property
    def security_master_first_seen_at(self) -> datetime:
        return self.security_master_manifest.first_seen_at

    @property
    def security_master_validated_at(self) -> datetime:
        return self.security_master_manifest.validated_at

    @property
    def retained_row_count(self) -> int:
        return len(self.entries)

    @property
    def main_scope_count(self) -> int:
        return sum(entry.scope is ReconciliationScope.MAIN_EQ for entry in self.entries)

    @property
    def sme_scope_count(self) -> int:
        return sum(entry.scope is ReconciliationScope.SME_SM for entry in self.entries)

    @property
    def unsupported_series_count(self) -> int:
        return sum(
            entry.scope is ReconciliationScope.UNSUPPORTED_SERIES for entry in self.entries
        )

    @property
    def unresolved_count(self) -> int:
        return sum(
            entry.disposition is ReconciliationDisposition.UNRESOLVED
            for entry in self.entries
        )

    @property
    def traded_row_count(self) -> int:
        return sum(entry.udiff_trade_row is not None for entry in self.entries)
