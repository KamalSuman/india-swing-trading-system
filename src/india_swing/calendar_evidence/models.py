from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import PurePath

from india_swing.daily_reports.models import DailyReportFamily
from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference_data.models import AcquisitionMode

from .policy import final_report_not_before


CALENDAR_EVIDENCE_SCHEMA_VERSION = "observed-market-date-artifact/v1"
CALENDAR_EVIDENCE_POLICY_VERSION = "nse-cm-positive-trade-dates-only/v1"
POSITIVE_TRADE_DATES_ONLY = "POSITIVE_TRADE_DATES_ONLY"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_REPORT_FAMILY_ORDER = (
    DailyReportFamily.UDIFF_BHAVCOPY,
    DailyReportFamily.FULL_BHAVCOPY_DELIVERY,
)


class CalendarEvidenceError(ValueError):
    pass


class CalendarEvidenceIntegrityError(CalendarEvidenceError):
    pass


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
class DailyReportEvidenceRef:
    """Lineage for one report that positively observes a CM trade date."""

    bundle_artifact_id: str
    bundle_manifest_id: str
    family: DailyReportFamily
    source_entry_name: str
    content_name: str
    source_entry_sha256: str
    content_sha256: str
    header_sha256: str
    ordered_row_digest: str
    row_count: int
    trade_date: date
    knowledge_time: datetime

    def __post_init__(self) -> None:
        for value, name in (
            (self.bundle_artifact_id, "bundle_artifact_id"),
            (self.bundle_manifest_id, "bundle_manifest_id"),
            (self.source_entry_sha256, "source_entry_sha256"),
            (self.content_sha256, "content_sha256"),
            (self.header_sha256, "header_sha256"),
            (self.ordered_row_digest, "ordered_row_digest"),
        ):
            _require_sha256(value, name)
        if self.family not in _REPORT_FAMILY_ORDER:
            raise ValueError("calendar evidence accepts only UDiFF and full bhavcopy reports")
        _require_safe_basename(self.source_entry_name, "source_entry_name")
        _require_safe_basename(self.content_name, "content_name")
        if type(self.row_count) is not int or self.row_count <= 0:
            raise ValueError("positive trade-date evidence requires report rows")
        if type(self.trade_date) is not date:
            raise TypeError("trade_date must be a date")
        object.__setattr__(
            self,
            "knowledge_time",
            _utc(self.knowledge_time, "knowledge_time"),
        )
        if self.knowledge_time < final_report_not_before(self.trade_date):
            raise CalendarEvidenceIntegrityError(
                "trade-date evidence predates the conservative final-report boundary"
            )


@dataclass(frozen=True, slots=True)
class ObservedMarketDate:
    """A positive date observation, not a complete trading-session assertion."""

    market_date: date
    report_refs: tuple[DailyReportEvidenceRef, ...]
    evidence_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.market_date) is not date:
            raise TypeError("market_date must be a date")
        if type(self.report_refs) is not tuple or any(
            type(reference) is not DailyReportEvidenceRef
            for reference in self.report_refs
        ):
            raise TypeError("report_refs must be an immutable exact evidence-ref tuple")
        if tuple(reference.family for reference in self.report_refs) != _REPORT_FAMILY_ORDER:
            raise CalendarEvidenceIntegrityError(
                "each observed date requires one ordered UDiFF/full report pair"
            )
        if any(reference.trade_date != self.market_date for reference in self.report_refs):
            raise CalendarEvidenceIntegrityError(
                "report evidence date does not match its observed market date"
            )
        if len({reference.bundle_artifact_id for reference in self.report_refs}) != 1:
            raise CalendarEvidenceIntegrityError(
                "one observed date cannot mix daily-bundle artifacts"
            )
        if len({reference.bundle_manifest_id for reference in self.report_refs}) != 1:
            raise CalendarEvidenceIntegrityError(
                "one observed date cannot mix daily-bundle manifests"
            )
        if len({reference.knowledge_time for reference in self.report_refs}) != 1:
            raise CalendarEvidenceIntegrityError(
                "paired reports must share one conservative knowledge time"
            )
        object.__setattr__(self, "evidence_id", self._calculated_evidence_id())

    def _calculated_evidence_id(self) -> str:
        return content_id(
            {
                "schema": "observed-market-date/v1",
                "market_date": self.market_date,
                "report_refs": self.report_refs,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if type(self.report_refs) is not tuple or any(
            type(reference) is not DailyReportEvidenceRef
            for reference in self.report_refs
        ):
            raise CalendarEvidenceIntegrityError(
                "observed-date reference graph contains an unaudited type"
            )
        if self.evidence_id != self._calculated_evidence_id():
            raise CalendarEvidenceIntegrityError(
                "observed-date content identity verification failed"
            )


@dataclass(frozen=True, slots=True)
class ObservedMarketDateArtifact:
    """Collection-only positive observations derived from one archived bundle.

    Absence from ``observations`` says nothing about whether a date was closed.
    This type intentionally has no session kind, session windows, data-ready
    time, or next-session operation.
    """

    exchange: str
    segment: str
    cutoff: datetime
    source_bundle_artifact_id: str
    source_bundle_manifest_id: str
    source_bundle_raw_sha256: str
    source_bundle_normalized_sha256: str
    source_acquisition_mode: AcquisitionMode
    source_readiness: ReferenceReadiness
    source_first_seen_at: datetime
    source_validated_at: datetime
    observations: tuple[ObservedMarketDate, ...]
    inference_scope: str = POSITIVE_TRADE_DATES_ONLY
    readiness: ReferenceReadiness = ReferenceReadiness.COLLECTION_ONLY
    actionable: bool = False
    policy_version: str = CALENDAR_EVIDENCE_POLICY_VERSION
    schema_version: str = CALENDAR_EVIDENCE_SCHEMA_VERSION
    artifact_id: str = field(init=False)

    def __post_init__(self) -> None:
        if self.exchange != "NSE" or self.segment != "CM":
            raise ValueError("observed market-date evidence is pinned to NSE CM")
        object.__setattr__(self, "cutoff", _utc(self.cutoff, "cutoff"))
        for value, name in (
            (self.source_bundle_artifact_id, "source_bundle_artifact_id"),
            (self.source_bundle_manifest_id, "source_bundle_manifest_id"),
            (self.source_bundle_raw_sha256, "source_bundle_raw_sha256"),
            (
                self.source_bundle_normalized_sha256,
                "source_bundle_normalized_sha256",
            ),
        ):
            _require_sha256(value, name)
        if self.source_acquisition_mode is not AcquisitionMode.UNVERIFIED_MANUAL_FILE:
            raise ValueError("only the current unverified manual source mode is supported")
        if self.source_readiness is not ReferenceReadiness.COLLECTION_ONLY:
            raise ValueError("source daily bundle must remain collection-only")
        object.__setattr__(
            self,
            "source_first_seen_at",
            _utc(self.source_first_seen_at, "source_first_seen_at"),
        )
        object.__setattr__(
            self,
            "source_validated_at",
            _utc(self.source_validated_at, "source_validated_at"),
        )
        if self.source_validated_at < self.source_first_seen_at:
            raise ValueError("source validation cannot precede first observation")
        if self.source_validated_at > self.cutoff:
            raise CalendarEvidenceIntegrityError(
                "daily bundle was not validated by the requested cutoff"
            )
        if type(self.observations) is not tuple or not self.observations or any(
            type(observation) is not ObservedMarketDate
            for observation in self.observations
        ):
            raise TypeError("observations must be a non-empty immutable exact tuple")
        if tuple(sorted(self.observations, key=lambda item: item.market_date)) != self.observations:
            raise CalendarEvidenceIntegrityError("observed market dates must be sorted")
        if len({item.market_date for item in self.observations}) != len(self.observations):
            raise CalendarEvidenceIntegrityError("observed market dates must be unique")
        for observation in self.observations:
            observation.verify_content_identity()
            for reference in observation.report_refs:
                if (
                    reference.bundle_artifact_id != self.source_bundle_artifact_id
                    or reference.bundle_manifest_id != self.source_bundle_manifest_id
                ):
                    raise CalendarEvidenceIntegrityError(
                        "observed-date lineage does not match its source bundle"
                    )
                if reference.knowledge_time != self.source_validated_at:
                    raise CalendarEvidenceIntegrityError(
                        "report knowledge time must equal source validation time"
                    )
        if self.inference_scope != POSITIVE_TRADE_DATES_ONLY:
            raise ValueError("calendar evidence cannot claim negative-date inference")
        if self.readiness is not ReferenceReadiness.COLLECTION_ONLY or self.actionable is not False:
            raise ValueError("observed market-date evidence must remain collection-only")
        if self.policy_version != CALENDAR_EVIDENCE_POLICY_VERSION:
            raise ValueError("unsupported calendar-evidence policy version")
        if self.schema_version != CALENDAR_EVIDENCE_SCHEMA_VERSION:
            raise ValueError("unsupported calendar-evidence schema version")
        object.__setattr__(self, "artifact_id", self._calculated_artifact_id())

    def _calculated_artifact_id(self) -> str:
        return content_id(
            {
                "schema_version": self.schema_version,
                "policy_version": self.policy_version,
                "exchange": self.exchange,
                "segment": self.segment,
                "cutoff": self.cutoff,
                "source_bundle_artifact_id": self.source_bundle_artifact_id,
                "source_bundle_manifest_id": self.source_bundle_manifest_id,
                "source_bundle_raw_sha256": self.source_bundle_raw_sha256,
                "source_bundle_normalized_sha256": (
                    self.source_bundle_normalized_sha256
                ),
                "source_acquisition_mode": self.source_acquisition_mode,
                "source_readiness": self.source_readiness,
                "source_first_seen_at": self.source_first_seen_at,
                "source_validated_at": self.source_validated_at,
                "observations": self.observations,
                "inference_scope": self.inference_scope,
                "readiness": self.readiness,
                "actionable": self.actionable,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if type(self.observations) is not tuple or any(
            type(observation) is not ObservedMarketDate
            for observation in self.observations
        ):
            raise CalendarEvidenceIntegrityError(
                "calendar-evidence graph contains an unaudited type"
            )
        for observation in self.observations:
            observation.verify_content_identity()
        if self.artifact_id != self._calculated_artifact_id():
            raise CalendarEvidenceIntegrityError(
                "calendar-evidence content identity verification failed"
            )

    @property
    def observed_dates(self) -> tuple[date, ...]:
        return tuple(observation.market_date for observation in self.observations)

    @property
    def knowledge_time(self) -> datetime:
        return self.source_validated_at

    @property
    def age_at_cutoff(self) -> timedelta:
        return self.cutoff - self.source_validated_at
