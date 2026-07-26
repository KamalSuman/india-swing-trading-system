from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.identity import content_id

from .backfill import (
    HistoricalBackfillCompletion,
    HistoricalBackfillIssue,
    HistoricalBackfillPlan,
    HistoricalBackfillProgress,
)
from .backfill_gaps import (
    HistoricalBackfillGapClassification,
    HistoricalBackfillSessionGapEvidence,
)
from .codec import MARKET_PAYLOAD_CODEC_VERSION, encode_market_payload
from .collection import historical_dataset_name
from .gap_adjudication import HistoricalGapAdjudicationReport, HistoricalGapNseStatus
from .models import (
    HistoricalDailyCandleBatch,
    MARKET_DATA_PROVIDER_PATTERN,
    SHA256_IDENTIFIER,
)
from .reconciliation import (
    HistoricalCandleDifference,
    HistoricalCandleReconciliationReport,
    HistoricalCandleReconciliationRow,
    HistoricalReconciliationStatus,
)
from .snapshot_store import PAYLOAD_FILENAME, SNAPSHOT_SCHEMA_VERSION, StoredMarketSnapshot


HISTORICAL_DATASET_ADMISSION_SCHEMA_VERSION = (
    "historical-dataset-admission-report/v1"
)
HISTORICAL_DATASET_ADMISSION_POLICY_VERSION = (
    "historical-dataset-admission-policy/v1"
)
HISTORICAL_DATASET_ADMISSION_CODEC_VERSION = (
    "historical-dataset-admission-report-json/v1"
)
HISTORICAL_DATASET_ADMISSION_DATASET = "historical-dataset-admission-reports"
REPORT_FILENAME = "report.json"
MAXIMUM_DATASET_ADMISSION_REPORT_BYTES = 64 * 1024 * 1024


class HistoricalDatasetAdmissionError(ValueError):
    pass


class HistoricalDatasetAdmissionIntegrityError(HistoricalDatasetAdmissionError):
    pass


class HistoricalDatasetAdmissionDisposition(str, Enum):
    """A disposition never fills a gap; only ADMITTED can ever be used downstream."""

    ADMITTED = "ADMITTED"
    MISSING_COMPLETION = "MISSING_COMPLETION"
    UNRESOLVED_GAP_WITHOUT_ADJUDICATION = "UNRESOLVED_GAP_WITHOUT_ADJUDICATION"
    EXACT_NSE_BAR_PENDING_SOURCE_SELECTION_REVIEW = (
        "EXACT_NSE_BAR_PENDING_SOURCE_SELECTION_REVIEW"
    )
    RELATED_NSE_IDENTITY_CONFLICT = "RELATED_NSE_IDENTITY_CONFLICT"
    NSE_TRADED_BAR_ABSENT = "NSE_TRADED_BAR_ABSENT"
    RECONCILIATION_MISSING_OR_FAILED = "RECONCILIATION_MISSING_OR_FAILED"

    @property
    def is_admitted(self) -> bool:
        return self is HistoricalDatasetAdmissionDisposition.ADMITTED


_D = HistoricalDatasetAdmissionDisposition

_REQUIRES_SNAPSHOT_LINEAGE = {_D.ADMITTED, _D.RECONCILIATION_MISSING_OR_FAILED}
_FORBIDS_SNAPSHOT_LINEAGE = {
    _D.MISSING_COMPLETION,
    _D.UNRESOLVED_GAP_WITHOUT_ADJUDICATION,
    _D.EXACT_NSE_BAR_PENDING_SOURCE_SELECTION_REVIEW,
    _D.RELATED_NSE_IDENTITY_CONFLICT,
    _D.NSE_TRADED_BAR_ABSENT,
}
_REQUIRES_RECONCILIATION = {_D.ADMITTED}
_FORBIDS_RECONCILIATION = {
    _D.MISSING_COMPLETION,
    _D.UNRESOLVED_GAP_WITHOUT_ADJUDICATION,
    _D.EXACT_NSE_BAR_PENDING_SOURCE_SELECTION_REVIEW,
    _D.RELATED_NSE_IDENTITY_CONFLICT,
    _D.NSE_TRADED_BAR_ABSENT,
}
_REQUIRES_GAP = {
    _D.UNRESOLVED_GAP_WITHOUT_ADJUDICATION,
    _D.EXACT_NSE_BAR_PENDING_SOURCE_SELECTION_REVIEW,
    _D.RELATED_NSE_IDENTITY_CONFLICT,
    _D.NSE_TRADED_BAR_ABSENT,
}
_FORBIDS_GAP = {_D.ADMITTED, _D.MISSING_COMPLETION, _D.RECONCILIATION_MISSING_OR_FAILED}
_REQUIRES_ADJUDICATION_ENTRY = {
    _D.EXACT_NSE_BAR_PENDING_SOURCE_SELECTION_REVIEW,
    _D.RELATED_NSE_IDENTITY_CONFLICT,
    _D.NSE_TRADED_BAR_ABSENT,
}
_FORBIDS_ADJUDICATION_ENTRY = {
    _D.ADMITTED,
    _D.MISSING_COMPLETION,
    _D.RECONCILIATION_MISSING_OR_FAILED,
    _D.UNRESOLVED_GAP_WITHOUT_ADJUDICATION,
}

_STATUS_TO_DISPOSITION = {
    HistoricalGapNseStatus.EXACT_TRADED_BAR_PRESENT: (
        _D.EXACT_NSE_BAR_PENDING_SOURCE_SELECTION_REVIEW
    ),
    HistoricalGapNseStatus.RELATED_NSE_IDENTITY_CONFLICT: (
        _D.RELATED_NSE_IDENTITY_CONFLICT
    ),
    HistoricalGapNseStatus.NSE_TRADED_BAR_ABSENT: _D.NSE_TRADED_BAR_ABSENT,
}


def _sha256(value: object, field_name: str) -> None:
    if type(value) is not str or SHA256_IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a lowercase SHA-256")


def _provider(value: object, field_name: str = "provider") -> None:
    if type(value) is not str or MARKET_DATA_PROVIDER_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be canonical uppercase provider text")


def _utc(value: object, field_name: str) -> datetime:
    if type(value) is not datetime:
        raise TypeError(f"{field_name} must be an exact datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class HistoricalDatasetAdmissionEntry:
    """One plan request's exact admission disposition and supporting lineage.

    An explained gap is still a gap: only ADMITTED may ever be consumed as a
    completed, reconciled provider observation. Every other disposition
    remains non-actionable, non-training-eligible evidence.
    """

    request_id: str
    binding_id: str
    sessions: tuple[date, ...]
    disposition: HistoricalDatasetAdmissionDisposition
    snapshot_id: str | None
    historical_batch_id: str | None
    reconciliation_report_id: str | None
    gap_evidence_id: str | None
    gap_adjudication_entry_id: str | None
    collection_only: bool = True
    actionable: bool = False
    training_eligible: bool = False
    entry_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "entry_id", self._calculated_id())

    def _validate(self) -> None:
        _sha256(self.request_id, "entry request_id")
        _sha256(self.binding_id, "entry binding_id")
        if (
            type(self.sessions) is not tuple
            or not self.sessions
            or any(type(value) is not date for value in self.sessions)
            or self.sessions != tuple(sorted(set(self.sessions)))
        ):
            raise ValueError("entry sessions must be sorted unique exact dates")
        if type(self.disposition) is not HistoricalDatasetAdmissionDisposition:
            raise TypeError("entry disposition must be exact")
        for value, name in (
            (self.snapshot_id, "snapshot_id"),
            (self.historical_batch_id, "historical_batch_id"),
            (self.reconciliation_report_id, "reconciliation_report_id"),
            (self.gap_evidence_id, "gap_evidence_id"),
            (self.gap_adjudication_entry_id, "gap_adjudication_entry_id"),
        ):
            if value is not None:
                _sha256(value, f"entry {name}")
        disposition = self.disposition
        if disposition in _REQUIRES_SNAPSHOT_LINEAGE and (
            self.snapshot_id is None or self.historical_batch_id is None
        ):
            raise ValueError("disposition requires snapshot/batch lineage")
        if disposition in _FORBIDS_SNAPSHOT_LINEAGE and (
            self.snapshot_id is not None or self.historical_batch_id is not None
        ):
            raise ValueError("disposition forbids snapshot/batch lineage")
        if (
            disposition in _REQUIRES_RECONCILIATION
            and self.reconciliation_report_id is None
        ):
            raise ValueError("disposition requires a passing reconciliation report")
        if (
            disposition in _FORBIDS_RECONCILIATION
            and self.reconciliation_report_id is not None
        ):
            raise ValueError("disposition forbids reconciliation lineage")
        if disposition in _REQUIRES_GAP and self.gap_evidence_id is None:
            raise ValueError("disposition requires gap evidence")
        if disposition in _FORBIDS_GAP and self.gap_evidence_id is not None:
            raise ValueError("disposition forbids gap evidence")
        if (
            disposition in _REQUIRES_ADJUDICATION_ENTRY
            and self.gap_adjudication_entry_id is None
        ):
            raise ValueError("disposition requires an adjudication entry")
        if (
            disposition in _FORBIDS_ADJUDICATION_ENTRY
            and self.gap_adjudication_entry_id is not None
        ):
            raise ValueError("disposition forbids adjudication linkage")
        if self.collection_only is not True:
            raise ValueError("dataset admission entries must remain collection-only")
        if self.actionable is not False:
            raise ValueError("dataset admission entries cannot authorize trading")
        if self.training_eligible is not False:
            raise ValueError("dataset admission entries cannot be training-eligible")

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "entry_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.entry_id != self._calculated_id():
            raise HistoricalDatasetAdmissionIntegrityError(
                "dataset admission entry identity failed"
            )

    @property
    def is_admitted(self) -> bool:
        return self.disposition.is_admitted


@dataclass(frozen=True, slots=True)
class HistoricalDatasetAdmissionReport:
    """One sealed, plan-scoped verdict on which provider batches are admitted.

    Exact NSE bar presence is evidence for later source-selection review, not
    permission to fill a provider response, train a model, or trade.
    """

    plan_id: str
    progress_id: str
    provider: str
    connector_version: str
    assessed_at: datetime
    gap_adjudication_report_id: str | None
    plan_issue_ids: tuple[str, ...]
    blocking_plan_issue_ids: tuple[str, ...]
    entries: tuple[HistoricalDatasetAdmissionEntry, ...]
    safe_requests_complete: bool
    coverage_complete: bool
    collection_only: bool = True
    actionable: bool = False
    training_eligible: bool = False
    schema_version: str = HISTORICAL_DATASET_ADMISSION_SCHEMA_VERSION
    policy_version: str = HISTORICAL_DATASET_ADMISSION_POLICY_VERSION
    codec_version: str = HISTORICAL_DATASET_ADMISSION_CODEC_VERSION
    report_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(
            self, "assessed_at", _utc(self.assessed_at, "report assessed_at")
        )
        object.__setattr__(self, "report_id", self._calculated_id())

    def _validate(self) -> None:
        _sha256(self.plan_id, "report plan_id")
        _sha256(self.progress_id, "report progress_id")
        _provider(self.provider, "report provider")
        if (
            type(self.connector_version) is not str
            or not self.connector_version
            or len(self.connector_version) > 128
        ):
            raise ValueError("report connector_version must be bounded text")
        _utc(self.assessed_at, "report assessed_at")
        if self.gap_adjudication_report_id is not None:
            _sha256(
                self.gap_adjudication_report_id, "report gap_adjudication_report_id"
            )
        if type(self.plan_issue_ids) is not tuple or any(
            type(value) is not str for value in self.plan_issue_ids
        ):
            raise TypeError("report plan_issue_ids must be an exact tuple")
        for value in self.plan_issue_ids:
            _sha256(value, "report plan_issue_ids entry")
        if (
            self.plan_issue_ids != tuple(sorted(self.plan_issue_ids))
            or len(set(self.plan_issue_ids)) != len(self.plan_issue_ids)
        ):
            raise ValueError("report plan_issue_ids must be sorted and unique")
        if type(self.blocking_plan_issue_ids) is not tuple or any(
            type(value) is not str for value in self.blocking_plan_issue_ids
        ):
            raise TypeError("report blocking_plan_issue_ids must be an exact tuple")
        for value in self.blocking_plan_issue_ids:
            _sha256(value, "report blocking_plan_issue_ids entry")
        if (
            self.blocking_plan_issue_ids != tuple(sorted(self.blocking_plan_issue_ids))
            or len(set(self.blocking_plan_issue_ids))
            != len(self.blocking_plan_issue_ids)
        ):
            raise ValueError(
                "report blocking_plan_issue_ids must be sorted and unique"
            )
        if not set(self.blocking_plan_issue_ids) <= set(self.plan_issue_ids):
            raise ValueError(
                "report blocking_plan_issue_ids must be a subset of plan_issue_ids"
            )
        if type(self.entries) is not tuple or not self.entries or any(
            type(value) is not HistoricalDatasetAdmissionEntry
            for value in self.entries
        ):
            raise TypeError("report entries must be a non-empty exact immutable tuple")
        if self.entries != tuple(
            sorted(self.entries, key=lambda value: value.request_id)
        ):
            raise ValueError("report entries must be request_id-ordered")
        if len({value.request_id for value in self.entries}) != len(self.entries):
            raise ValueError("report entries must be unique by request_id")
        any_adjudication_linkage = False
        for entry in self.entries:
            entry.verify_content_identity()
            if entry.gap_adjudication_entry_id is not None:
                any_adjudication_linkage = True
                if self.gap_adjudication_report_id is None:
                    raise HistoricalDatasetAdmissionIntegrityError(
                        "entry references adjudication without a bound report"
                    )
        if self.gap_adjudication_report_id is not None and not any_adjudication_linkage:
            raise HistoricalDatasetAdmissionIntegrityError(
                "report references an adjudication report used by no entry"
            )
        if type(self.safe_requests_complete) is not bool:
            raise TypeError("safe_requests_complete must be bool")
        expected_safe_requests_complete = all(
            value.is_admitted for value in self.entries
        )
        if self.safe_requests_complete != expected_safe_requests_complete:
            raise HistoricalDatasetAdmissionIntegrityError(
                "safe_requests_complete disagrees with entry dispositions"
            )
        if type(self.coverage_complete) is not bool:
            raise TypeError("coverage_complete must be bool")
        expected_coverage_complete = (
            self.safe_requests_complete and not self.blocking_plan_issue_ids
        )
        if self.coverage_complete != expected_coverage_complete:
            raise HistoricalDatasetAdmissionIntegrityError(
                "coverage_complete disagrees with safe_requests_complete and "
                "blocking_plan_issue_ids"
            )
        if self.collection_only is not True:
            raise ValueError("dataset admission reports must remain collection-only")
        if self.actionable is not False:
            raise ValueError("dataset admission reports cannot authorize trading")
        if self.training_eligible is not False:
            raise ValueError("dataset admission reports cannot be training-eligible")
        if (
            self.schema_version != HISTORICAL_DATASET_ADMISSION_SCHEMA_VERSION
            or self.policy_version != HISTORICAL_DATASET_ADMISSION_POLICY_VERSION
            or self.codec_version != HISTORICAL_DATASET_ADMISSION_CODEC_VERSION
        ):
            raise ValueError("unsupported historical dataset admission contract")

    @property
    def admitted_request_ids(self) -> tuple[str, ...]:
        return tuple(value.request_id for value in self.entries if value.is_admitted)

    @property
    def admitted_snapshot_ids(self) -> tuple[str, ...]:
        return tuple(value.snapshot_id for value in self.entries if value.is_admitted)

    @property
    def admitted_batch_ids(self) -> tuple[str, ...]:
        return tuple(
            value.historical_batch_id for value in self.entries if value.is_admitted
        )

    @property
    def admitted_request_count(self) -> int:
        return len(self.admitted_request_ids)

    @property
    def blocked_request_count(self) -> int:
        return len(self.entries) - self.admitted_request_count

    @property
    def total_request_count(self) -> int:
        return len(self.entries)

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "report_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.report_id != self._calculated_id():
            raise HistoricalDatasetAdmissionIntegrityError(
                "dataset admission report identity failed"
            )


def _reconstruct_issue(issue: HistoricalBackfillIssue) -> HistoricalBackfillIssue:
    """Replay a plan issue through its own constructor; never trust a self-hash.

    A caller can mutate a field and recompute issue_id so the object's own
    hash is internally consistent; only replaying the real constructor
    re-enforces the business invariants __post_init__ would have applied.
    """

    try:
        rebuilt = HistoricalBackfillIssue(
            code=issue.code,
            affected_dates=issue.affected_dates,
            observation_ids=issue.observation_ids,
        )
    except (TypeError, ValueError):
        raise HistoricalDatasetAdmissionIntegrityError(
            "plan issue failed constructor replay"
        ) from None
    if rebuilt != issue or rebuilt.issue_id != issue.issue_id:
        raise HistoricalDatasetAdmissionIntegrityError(
            "plan issue identity failed constructor replay"
        )
    return rebuilt


def _reconstruct_plan(plan: HistoricalBackfillPlan) -> HistoricalBackfillPlan:
    reconstructed_issues = tuple(_reconstruct_issue(value) for value in plan.issues)
    try:
        rebuilt = HistoricalBackfillPlan(
            provider=plan.provider,
            resolver_version=plan.resolver_version,
            identity_registry_id=plan.identity_registry_id,
            calendar_snapshot_id=plan.calendar_snapshot_id,
            coverage_start=plan.coverage_start,
            coverage_end=plan.coverage_end,
            requested_at=plan.requested_at,
            requests=plan.requests,
            issues=reconstructed_issues,
            identity_snapshot_id=plan.identity_snapshot_id,
            collection_only=plan.collection_only,
            schema_version=plan.schema_version,
        )
    except (TypeError, ValueError):
        raise HistoricalDatasetAdmissionIntegrityError(
            "plan failed constructor replay"
        ) from None
    if rebuilt != plan or rebuilt.plan_id != plan.plan_id:
        raise HistoricalDatasetAdmissionIntegrityError(
            "plan identity failed constructor replay"
        )
    return rebuilt


def _reconstruct_completion(
    completion: HistoricalBackfillCompletion,
) -> HistoricalBackfillCompletion:
    try:
        rebuilt = HistoricalBackfillCompletion(
            request_id=completion.request_id,
            snapshot_id=completion.snapshot_id,
            completed_at=completion.completed_at,
            recovered_existing=completion.recovered_existing,
        )
    except (TypeError, ValueError):
        raise HistoricalDatasetAdmissionIntegrityError(
            "progress completion failed constructor replay"
        ) from None
    if rebuilt != completion or rebuilt.completion_id != completion.completion_id:
        raise HistoricalDatasetAdmissionIntegrityError(
            "progress completion identity failed constructor replay"
        )
    return rebuilt


def _reconstruct_progress(
    progress: HistoricalBackfillProgress,
) -> HistoricalBackfillProgress:
    reconstructed_completions = tuple(
        _reconstruct_completion(value) for value in progress.completions
    )
    try:
        rebuilt = HistoricalBackfillProgress(
            plan_id=progress.plan_id,
            provider=progress.provider,
            connector_version=progress.connector_version,
            completions=reconstructed_completions,
            updated_at=progress.updated_at,
            schema_version=progress.schema_version,
        )
    except (TypeError, ValueError):
        raise HistoricalDatasetAdmissionIntegrityError(
            "progress failed constructor replay"
        ) from None
    if rebuilt != progress or rebuilt.progress_id != progress.progress_id:
        raise HistoricalDatasetAdmissionIntegrityError(
            "progress identity failed constructor replay"
        )
    return rebuilt


def _reconstruct_difference(
    value: HistoricalCandleDifference,
) -> HistoricalCandleDifference:
    try:
        rebuilt = HistoricalCandleDifference(
            field_name=value.field_name,
            provider_value=value.provider_value,
            nse_value=value.nse_value,
        )
    except (TypeError, ValueError):
        raise HistoricalDatasetAdmissionIntegrityError(
            "reconciliation difference failed constructor replay"
        ) from None
    if rebuilt != value:
        raise HistoricalDatasetAdmissionIntegrityError(
            "reconciliation difference identity failed constructor replay"
        )
    return rebuilt


def _reconstruct_row(
    row: HistoricalCandleReconciliationRow,
) -> HistoricalCandleReconciliationRow:
    reconstructed_differences = tuple(
        _reconstruct_difference(value) for value in row.differences
    )
    try:
        rebuilt = HistoricalCandleReconciliationRow(
            session=row.session,
            nse_artifact_id=row.nse_artifact_id,
            nse_bar_id=row.nse_bar_id,
            status=row.status,
            differences=reconstructed_differences,
        )
    except (TypeError, ValueError):
        raise HistoricalDatasetAdmissionIntegrityError(
            "reconciliation row failed constructor replay"
        ) from None
    if rebuilt != row or rebuilt.row_id != row.row_id:
        raise HistoricalDatasetAdmissionIntegrityError(
            "reconciliation row identity failed constructor replay"
        )
    return rebuilt


def _reconstruct_reconciliation(
    report: HistoricalCandleReconciliationReport,
) -> HistoricalCandleReconciliationReport:
    reconstructed_rows = tuple(_reconstruct_row(value) for value in report.rows)
    try:
        rebuilt = HistoricalCandleReconciliationReport(
            historical_batch_id=report.historical_batch_id,
            historical_request_id=report.historical_request_id,
            provider=report.provider,
            provider_version=report.provider_version,
            listing_key=report.listing_key,
            security_series=report.security_series,
            isin=report.isin,
            reconciled_at=report.reconciled_at,
            rows=reconstructed_rows,
            passed=report.passed,
            actionable=report.actionable,
            schema_version=report.schema_version,
            policy_version=report.policy_version,
        )
    except (TypeError, ValueError):
        raise HistoricalDatasetAdmissionIntegrityError(
            "reconciliation report failed constructor replay"
        ) from None
    if rebuilt != report or rebuilt.report_id != report.report_id:
        raise HistoricalDatasetAdmissionIntegrityError(
            "reconciliation report identity failed constructor replay"
        )
    return rebuilt


def _expected_snapshot_id(manifest) -> str:
    return content_id(
        {
            "schema_version": manifest.schema_version,
            "codec_version": manifest.codec_version,
            "dataset": manifest.dataset,
            "selection_key": manifest.selection_key,
            "provider": manifest.provider,
            "provider_version": manifest.provider_version,
            "observed_at": manifest.observed_at,
            "record_count": manifest.record_count,
            "payload_filename": manifest.payload_filename,
            "payload_sha256": manifest.payload_sha256,
        },
        length=64,
    )


def _verify_snapshot(
    *,
    snapshot: StoredMarketSnapshot,
    request_id: str,
    plan: HistoricalBackfillPlan,
    progress: HistoricalBackfillProgress,
    completion_snapshot_id: str,
) -> HistoricalDailyCandleBatch:
    batch = snapshot.normalized_payload
    if type(batch) is not HistoricalDailyCandleBatch:
        raise HistoricalDatasetAdmissionError(
            "snapshot payload must be an exact HistoricalDailyCandleBatch"
        )
    try:
        batch.verify_content_identity()
    except (TypeError, ValueError):
        raise HistoricalDatasetAdmissionIntegrityError(
            "completed snapshot batch identity failed"
        ) from None
    manifest = snapshot.manifest
    if (
        manifest.schema_version != SNAPSHOT_SCHEMA_VERSION
        or manifest.codec_version != MARKET_PAYLOAD_CODEC_VERSION
        or manifest.payload_filename != PAYLOAD_FILENAME
        or manifest.snapshot_id != completion_snapshot_id
        or manifest.dataset != historical_dataset_name(plan.provider)
        or manifest.selection_key != request_id
        or manifest.provider != plan.provider
        or manifest.provider_version != progress.connector_version
        or manifest.provider_version != batch.provider_version
        or manifest.observed_at != batch.observed_at
        or manifest.record_count != batch.record_count
    ):
        raise HistoricalDatasetAdmissionIntegrityError(
            "completed snapshot manifest disagrees with the plan/progress/batch"
        )
    if snapshot.payload_bytes != encode_market_payload(batch):
        raise HistoricalDatasetAdmissionIntegrityError(
            "completed snapshot payload bytes disagree with its normalized batch"
        )
    if manifest.payload_sha256 != hashlib.sha256(snapshot.payload_bytes).hexdigest():
        raise HistoricalDatasetAdmissionIntegrityError(
            "completed snapshot payload hash is invalid"
        )
    if manifest.snapshot_id != _expected_snapshot_id(manifest):
        raise HistoricalDatasetAdmissionIntegrityError(
            "completed snapshot identifier does not match its manifest"
        )
    return batch


def build_historical_dataset_admission_report(
    *,
    plan: HistoricalBackfillPlan,
    progress: HistoricalBackfillProgress,
    snapshots: tuple[StoredMarketSnapshot, ...],
    reconciliations: tuple[HistoricalCandleReconciliationReport, ...],
    gaps: tuple[HistoricalBackfillSessionGapEvidence, ...],
    gap_adjudication: HistoricalGapAdjudicationReport | None,
    assessed_at: datetime,
) -> HistoricalDatasetAdmissionReport:
    """Independently verify and admit only complete, reconciled provider batches.

    Never fills, resolves, or trains on a gap; never authorizes an evaluation
    dataset, forecast, signal, trade, or capital action.
    """

    if type(plan) is not HistoricalBackfillPlan:
        raise HistoricalDatasetAdmissionError(
            "plan must be an exact HistoricalBackfillPlan"
        )
    plan.verify_content_identity()
    plan = _reconstruct_plan(plan)
    if not plan.requests:
        raise HistoricalDatasetAdmissionError(
            "a plan with zero safe requests cannot produce an admission report"
        )
    if type(progress) is not HistoricalBackfillProgress:
        raise HistoricalDatasetAdmissionError(
            "progress must be an exact HistoricalBackfillProgress"
        )
    progress.verify_content_identity()
    progress = _reconstruct_progress(progress)
    if progress.plan_id != plan.plan_id or progress.provider != plan.provider:
        raise HistoricalDatasetAdmissionError(
            "progress does not belong to the exact plan"
        )

    requests_by_id = {value.request_id: value for value in plan.requests}
    if any(value.request_id not in requests_by_id for value in progress.completions):
        raise HistoricalDatasetAdmissionError(
            "progress contains a completion outside the plan"
        )
    completions_by_id = {value.request_id: value for value in progress.completions}

    assessed_at = _utc(assessed_at, "assessed_at")
    if assessed_at < plan.requested_at or assessed_at < progress.updated_at:
        raise HistoricalDatasetAdmissionError(
            "assessed_at cannot precede the plan or progress"
        )

    if type(snapshots) is not tuple or any(
        type(value) is not StoredMarketSnapshot for value in snapshots
    ):
        raise HistoricalDatasetAdmissionError(
            "snapshots must be an exact immutable tuple"
        )
    snapshot_ids_seen: set[str] = set()
    batch_by_request_id: dict[str, HistoricalDailyCandleBatch] = {}
    snapshot_id_by_request_id: dict[str, str] = {}
    for snapshot in snapshots:
        manifest_snapshot_id = snapshot.manifest.snapshot_id
        if manifest_snapshot_id in snapshot_ids_seen:
            raise HistoricalDatasetAdmissionError("duplicate snapshot supplied")
        snapshot_ids_seen.add(manifest_snapshot_id)
        request_id = snapshot.manifest.selection_key
        completion = completions_by_id.get(request_id)
        if completion is None or completion.snapshot_id != manifest_snapshot_id:
            raise HistoricalDatasetAdmissionError(
                "supplied snapshot does not correspond to a completed request"
            )
        if request_id in batch_by_request_id:
            raise HistoricalDatasetAdmissionError(
                "more than one snapshot supplied for one completed request"
            )
        batch = _verify_snapshot(
            snapshot=snapshot,
            request_id=request_id,
            plan=plan,
            progress=progress,
            completion_snapshot_id=completion.snapshot_id,
        )
        if assessed_at < batch.observed_at:
            raise HistoricalDatasetAdmissionError(
                "assessed_at cannot precede a completed batch observation"
            )
        batch_by_request_id[request_id] = batch
        snapshot_id_by_request_id[request_id] = manifest_snapshot_id
    if set(batch_by_request_id) != set(completions_by_id):
        raise HistoricalDatasetAdmissionError(
            "supplied snapshots do not exactly cover every completed request"
        )

    if type(reconciliations) is not tuple or any(
        type(value) is not HistoricalCandleReconciliationReport
        for value in reconciliations
    ):
        raise HistoricalDatasetAdmissionError(
            "reconciliations must be an exact immutable tuple"
        )
    batch_id_to_request_id = {
        batch.batch_id: request_id
        for request_id, batch in batch_by_request_id.items()
    }
    reconciliation_by_request_id: dict[str, HistoricalCandleReconciliationReport] = {}
    seen_report_ids: set[str] = set()
    for report in reconciliations:
        report.verify_content_identity()
        report = _reconstruct_reconciliation(report)
        if report.report_id in seen_report_ids:
            raise HistoricalDatasetAdmissionError(
                "duplicate reconciliation report supplied"
            )
        seen_report_ids.add(report.report_id)
        request_id = batch_id_to_request_id.get(report.historical_batch_id)
        if request_id is None:
            raise HistoricalDatasetAdmissionError(
                "reconciliation report does not correspond to a completed request"
            )
        if request_id in reconciliation_by_request_id:
            raise HistoricalDatasetAdmissionError(
                "more than one reconciliation report supplied for one request"
            )
        request = requests_by_id[request_id]
        batch = batch_by_request_id[request_id]
        if (
            report.historical_request_id != batch.request.request_id
            or report.historical_batch_id != batch.batch_id
            or report.provider != plan.provider
            or report.provider_version != progress.connector_version
            or report.listing_key != request.binding.listing_key
            or report.security_series != request.binding.security_series
            or report.isin != request.binding.isin
            or tuple(row.session for row in report.rows) != batch.request.sessions
            or report.reconciled_at < batch.observed_at
        ):
            raise HistoricalDatasetAdmissionIntegrityError(
                "reconciliation report lineage disagrees with the completed request"
            )
        if assessed_at < report.reconciled_at:
            raise HistoricalDatasetAdmissionError(
                "assessed_at cannot precede a reconciliation report"
            )
        reconciliation_by_request_id[request_id] = report

    if type(gaps) is not tuple or any(
        type(value) is not HistoricalBackfillSessionGapEvidence for value in gaps
    ):
        raise HistoricalDatasetAdmissionError("gaps must be an exact immutable tuple")
    gap_by_request_id: dict[str, HistoricalBackfillSessionGapEvidence] = {}
    for gap in gaps:
        gap.verify_content_identity()
        if (
            gap.plan_id != plan.plan_id
            or gap.provider != plan.provider
            or gap.provider_version != progress.connector_version
        ):
            raise HistoricalDatasetAdmissionError(
                "gap does not belong to the exact plan"
            )
        request = requests_by_id.get(gap.request_id)
        if request is None:
            raise HistoricalDatasetAdmissionError(
                "gap references a request outside the plan"
            )
        if (
            gap.provider_instrument_id != request.binding.provider_instrument_id
            or gap.listing_key != request.binding.listing_key
            or gap.security_series != request.binding.security_series
            or gap.isin != request.binding.isin
            or gap.session not in request.sessions
            or gap.response_observed_at < request.requested_at
        ):
            raise HistoricalDatasetAdmissionIntegrityError(
                "gap evidence lineage disagrees with its plan request"
            )
        if assessed_at < gap.response_observed_at:
            raise HistoricalDatasetAdmissionError(
                "assessed_at cannot precede a gap's provider response"
            )
        if gap.request_id in gap_by_request_id:
            raise HistoricalDatasetAdmissionError(
                "more than one gap supplied for one request"
            )
        if gap.request_id in batch_by_request_id:
            raise HistoricalDatasetAdmissionError(
                "a request cannot be both completed and gapped"
            )
        gap_by_request_id[gap.request_id] = gap

    adjudication_entry_by_gap_id: dict[str, object] = {}
    gap_adjudication_report_id: str | None = None
    if gap_adjudication is not None:
        if type(gap_adjudication) is not HistoricalGapAdjudicationReport:
            raise HistoricalDatasetAdmissionError(
                "gap_adjudication must be an exact HistoricalGapAdjudicationReport"
            )
        gap_adjudication.verify_content_identity()
        if gap_adjudication.plan_id != plan.plan_id:
            raise HistoricalDatasetAdmissionError(
                "gap adjudication report does not belong to the exact plan"
            )
        if assessed_at < gap_adjudication.adjudicated_at:
            raise HistoricalDatasetAdmissionError(
                "assessed_at cannot precede the gap adjudication report"
            )
        adjudication_gap_ids = [
            value.gap_evidence_id for value in gap_adjudication.entries
        ]
        if len(set(adjudication_gap_ids)) != len(adjudication_gap_ids):
            raise HistoricalDatasetAdmissionIntegrityError(
                "gap adjudication report has duplicate gap ownership"
            )
        if set(adjudication_gap_ids) != set(gap_by_request_id[key].evidence_id for key in gap_by_request_id):
            raise HistoricalDatasetAdmissionError(
                "gap adjudication report does not exactly cover the supplied gaps"
            )
        gaps_by_evidence_id = {value.evidence_id: value for value in gaps}
        for entry in gap_adjudication.entries:
            gap = gaps_by_evidence_id.get(entry.gap_evidence_id)
            if (
                gap is None
                or entry.plan_id != plan.plan_id
                or entry.request_id != gap.request_id
                or entry.provider != gap.provider
                or entry.provider_version != gap.provider_version
                or entry.provider_instrument_id != gap.provider_instrument_id
                or entry.listing_key != gap.listing_key
                or entry.security_series != gap.security_series
                or entry.isin != gap.isin
                or entry.session != gap.session
                or entry.original_classification != gap.classification
                or entry.normalized_response_sha256 != gap.normalized_response_sha256
            ):
                raise HistoricalDatasetAdmissionIntegrityError(
                    "gap adjudication entry disagrees with its gap evidence"
                )
            adjudication_entry_by_gap_id[entry.gap_evidence_id] = entry
        gap_adjudication_report_id = gap_adjudication.report_id

    entries: list[HistoricalDatasetAdmissionEntry] = []
    for request in plan.requests:
        request_id = request.request_id
        completion = completions_by_id.get(request_id)
        gap = gap_by_request_id.get(request_id)
        if completion is not None:
            batch = batch_by_request_id[request_id]
            reconciliation = reconciliation_by_request_id.get(request_id)
            admitted = reconciliation is not None and (
                reconciliation.passed
                and all(
                    row.status is HistoricalReconciliationStatus.MATCH
                    for row in reconciliation.rows
                )
            )
            disposition = (
                _D.ADMITTED if admitted else _D.RECONCILIATION_MISSING_OR_FAILED
            )
            entries.append(
                HistoricalDatasetAdmissionEntry(
                    request_id=request_id,
                    binding_id=request.binding.binding_id,
                    sessions=request.sessions,
                    disposition=disposition,
                    snapshot_id=snapshot_id_by_request_id[request_id],
                    historical_batch_id=batch.batch_id,
                    reconciliation_report_id=(
                        reconciliation.report_id
                        if reconciliation is not None
                        else None
                    ),
                    gap_evidence_id=None,
                    gap_adjudication_entry_id=None,
                )
            )
        elif gap is not None:
            adjudication_entry = adjudication_entry_by_gap_id.get(gap.evidence_id)
            if adjudication_entry is None:
                disposition = _D.UNRESOLVED_GAP_WITHOUT_ADJUDICATION
                gap_adjudication_entry_id = None
            else:
                disposition = _STATUS_TO_DISPOSITION.get(adjudication_entry.status)
                if disposition is None:
                    raise HistoricalDatasetAdmissionIntegrityError(
                        "unsupported gap adjudication status"
                    )
                gap_adjudication_entry_id = adjudication_entry.entry_id
            entries.append(
                HistoricalDatasetAdmissionEntry(
                    request_id=request_id,
                    binding_id=request.binding.binding_id,
                    sessions=request.sessions,
                    disposition=disposition,
                    snapshot_id=None,
                    historical_batch_id=None,
                    reconciliation_report_id=None,
                    gap_evidence_id=gap.evidence_id,
                    gap_adjudication_entry_id=gap_adjudication_entry_id,
                )
            )
        else:
            entries.append(
                HistoricalDatasetAdmissionEntry(
                    request_id=request_id,
                    binding_id=request.binding.binding_id,
                    sessions=request.sessions,
                    disposition=_D.MISSING_COMPLETION,
                    snapshot_id=None,
                    historical_batch_id=None,
                    reconciliation_report_id=None,
                    gap_evidence_id=None,
                    gap_adjudication_entry_id=None,
                )
            )

    entries_sorted = tuple(sorted(entries, key=lambda value: value.request_id))
    safe_requests_complete = all(value.is_admitted for value in entries_sorted)
    blocking_plan_issue_ids = tuple(
        sorted(issue.issue_id for issue in plan.issues if issue.blocks_collection)
    )
    coverage_complete = safe_requests_complete and not blocking_plan_issue_ids

    return HistoricalDatasetAdmissionReport(
        plan_id=plan.plan_id,
        progress_id=progress.progress_id,
        provider=plan.provider,
        connector_version=progress.connector_version,
        assessed_at=assessed_at,
        gap_adjudication_report_id=gap_adjudication_report_id,
        plan_issue_ids=tuple(issue.issue_id for issue in plan.issues),
        blocking_plan_issue_ids=blocking_plan_issue_ids,
        entries=entries_sorted,
        safe_requests_complete=safe_requests_complete,
        coverage_complete=coverage_complete,
    )


def _entry_value(entry: HistoricalDatasetAdmissionEntry) -> dict[str, object]:
    return {
        "entry_id": entry.entry_id,
        "request_id": entry.request_id,
        "binding_id": entry.binding_id,
        "sessions": [value.isoformat() for value in entry.sessions],
        "disposition": entry.disposition.value,
        "snapshot_id": entry.snapshot_id,
        "historical_batch_id": entry.historical_batch_id,
        "reconciliation_report_id": entry.reconciliation_report_id,
        "gap_evidence_id": entry.gap_evidence_id,
        "gap_adjudication_entry_id": entry.gap_adjudication_entry_id,
        "collection_only": entry.collection_only,
        "actionable": entry.actionable,
        "training_eligible": entry.training_eligible,
    }


_EXPECTED_ENTRY_KEYS = {
    "entry_id",
    "request_id",
    "binding_id",
    "sessions",
    "disposition",
    "snapshot_id",
    "historical_batch_id",
    "reconciliation_report_id",
    "gap_evidence_id",
    "gap_adjudication_entry_id",
    "collection_only",
    "actionable",
    "training_eligible",
}

_EXPECTED_REPORT_KEYS = {
    "schema_version",
    "policy_version",
    "codec_version",
    "report_id",
    "plan_id",
    "progress_id",
    "provider",
    "connector_version",
    "assessed_at",
    "gap_adjudication_report_id",
    "plan_issue_ids",
    "blocking_plan_issue_ids",
    "entries",
    "safe_requests_complete",
    "coverage_complete",
    "collection_only",
    "actionable",
    "training_eligible",
}


def encode_historical_dataset_admission_report(
    report: HistoricalDatasetAdmissionReport,
) -> bytes:
    if type(report) is not HistoricalDatasetAdmissionReport:
        raise TypeError("report must be an exact HistoricalDatasetAdmissionReport")
    report.verify_content_identity()
    value = {
        "schema_version": report.schema_version,
        "policy_version": report.policy_version,
        "codec_version": report.codec_version,
        "report_id": report.report_id,
        "plan_id": report.plan_id,
        "progress_id": report.progress_id,
        "provider": report.provider,
        "connector_version": report.connector_version,
        "assessed_at": report.assessed_at.isoformat(),
        "gap_adjudication_report_id": report.gap_adjudication_report_id,
        "plan_issue_ids": list(report.plan_issue_ids),
        "blocking_plan_issue_ids": list(report.blocking_plan_issue_ids),
        "entries": [_entry_value(item) for item in report.entries],
        "safe_requests_complete": report.safe_requests_complete,
        "coverage_complete": report.coverage_complete,
        "collection_only": report.collection_only,
        "actionable": report.actionable,
        "training_eligible": report.training_eligible,
    }
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise HistoricalDatasetAdmissionError(
                "dataset admission report contains duplicate JSON keys"
            )
        value[key] = item
    return value


def decode_historical_dataset_admission_report(
    payload: bytes,
) -> HistoricalDatasetAdmissionReport:
    try:
        if type(payload) is not bytes:
            raise TypeError
        if not payload:
            raise ValueError
        if len(payload) > MAXIMUM_DATASET_ADMISSION_REPORT_BYTES:
            raise ValueError
        root = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        if type(root) is not dict or set(root) != _EXPECTED_REPORT_KEYS:
            raise ValueError
        raw_entries = root["entries"]
        if type(raw_entries) is not list:
            raise ValueError
        entries: list[HistoricalDatasetAdmissionEntry] = []
        for value in raw_entries:
            if type(value) is not dict or set(value) != _EXPECTED_ENTRY_KEYS:
                raise ValueError
            raw_sessions = value["sessions"]
            if type(raw_sessions) is not list:
                raise ValueError
            entry = HistoricalDatasetAdmissionEntry(
                request_id=value["request_id"],
                binding_id=value["binding_id"],
                sessions=tuple(date.fromisoformat(item) for item in raw_sessions),
                disposition=HistoricalDatasetAdmissionDisposition(
                    value["disposition"]
                ),
                snapshot_id=value["snapshot_id"],
                historical_batch_id=value["historical_batch_id"],
                reconciliation_report_id=value["reconciliation_report_id"],
                gap_evidence_id=value["gap_evidence_id"],
                gap_adjudication_entry_id=value["gap_adjudication_entry_id"],
                collection_only=value["collection_only"],
                actionable=value["actionable"],
                training_eligible=value["training_eligible"],
            )
            if value["entry_id"] != entry.entry_id:
                raise ValueError
            entries.append(entry)
        report = HistoricalDatasetAdmissionReport(
            plan_id=root["plan_id"],
            progress_id=root["progress_id"],
            provider=root["provider"],
            connector_version=root["connector_version"],
            assessed_at=datetime.fromisoformat(root["assessed_at"]),
            gap_adjudication_report_id=root["gap_adjudication_report_id"],
            plan_issue_ids=tuple(root["plan_issue_ids"]),
            blocking_plan_issue_ids=tuple(root["blocking_plan_issue_ids"]),
            entries=tuple(entries),
            safe_requests_complete=root["safe_requests_complete"],
            coverage_complete=root["coverage_complete"],
            collection_only=root["collection_only"],
            actionable=root["actionable"],
            training_eligible=root["training_eligible"],
            schema_version=root["schema_version"],
            policy_version=root["policy_version"],
            codec_version=root["codec_version"],
        )
        if root["report_id"] != report.report_id:
            raise ValueError
        if payload != encode_historical_dataset_admission_report(report):
            raise ValueError
        return report
    except HistoricalDatasetAdmissionError:
        raise
    except (
        AttributeError,
        KeyError,
        TypeError,
        ValueError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ):
        raise HistoricalDatasetAdmissionIntegrityError(
            "stored dataset admission report is invalid"
        ) from None


def _is_link_like(path: Path) -> bool:
    is_junction = getattr(path, "is_junction", None)
    return path.is_symlink() or bool(is_junction and is_junction())


def _write_fsynced(path: Path, payload: bytes) -> None:
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


class LocalHistoricalDatasetAdmissionReportStore:
    """Create-once local store; exposes only exact-ID get, never a listing."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @property
    def dataset_root(self) -> Path:
        return self.root / HISTORICAL_DATASET_ADMISSION_DATASET

    def put(
        self,
        report: HistoricalDatasetAdmissionReport,
    ) -> HistoricalDatasetAdmissionReport:
        if type(report) is not HistoricalDatasetAdmissionReport:
            raise TypeError("report must be an exact HistoricalDatasetAdmissionReport")
        report.verify_content_identity()
        payload = encode_historical_dataset_admission_report(report)
        if len(payload) > MAXIMUM_DATASET_ADMISSION_REPORT_BYTES:
            raise HistoricalDatasetAdmissionError(
                "dataset admission report exceeds its size limit"
            )
        self.dataset_root.mkdir(parents=True, exist_ok=True)
        target = self.dataset_root / report.report_id
        lock = self.dataset_root / ".dataset-admission-reports.lock"
        try:
            with advisory_file_lock(lock):
                if target.exists():
                    existing = self._read_path(target)
                    if existing != report:
                        raise HistoricalDatasetAdmissionIntegrityError(
                            "report ID already stores different content"
                        )
                    return existing
                temporary = Path(
                    tempfile.mkdtemp(
                        prefix=".dataset-admission-",
                        dir=self.dataset_root,
                    )
                )
                try:
                    _write_fsynced(temporary / REPORT_FILENAME, payload)
                    os.replace(temporary, target)
                except Exception:
                    shutil.rmtree(temporary, ignore_errors=True)
                    raise
        except (FileLockUnavailable, FileSafetyError):
            raise HistoricalDatasetAdmissionIntegrityError(
                "dataset admission report store is unavailable"
            ) from None
        return self._read_path(target)

    def get(self, report_id: str) -> HistoricalDatasetAdmissionReport:
        _sha256(report_id, "dataset admission report_id")
        target = self.dataset_root / report_id
        if not target.exists():
            raise HistoricalDatasetAdmissionError(
                "dataset admission report was not found"
            )
        return self._read_path(target)

    def _read_path(self, target: Path) -> HistoricalDatasetAdmissionReport:
        try:
            if not target.is_dir() or _is_link_like(target):
                raise HistoricalDatasetAdmissionIntegrityError(
                    "dataset admission report path is invalid"
                )
            children = tuple(target.iterdir())
            if (
                {value.name for value in children} != {REPORT_FILENAME}
                or any(
                    _is_link_like(value) or not value.is_file()
                    for value in children
                )
            ):
                raise HistoricalDatasetAdmissionIntegrityError(
                    "dataset admission report directory is invalid"
                )
            payload = read_stable_regular_file(
                target / REPORT_FILENAME,
                maximum_bytes=MAXIMUM_DATASET_ADMISSION_REPORT_BYTES,
            )
            report = decode_historical_dataset_admission_report(payload)
            if (
                target.name != report.report_id
                or payload != encode_historical_dataset_admission_report(report)
            ):
                raise HistoricalDatasetAdmissionIntegrityError(
                    "dataset admission report storage identity failed"
                )
            return report
        except HistoricalDatasetAdmissionIntegrityError:
            raise
        except (FileSafetyError, OSError):
            raise HistoricalDatasetAdmissionIntegrityError(
                "dataset admission report could not be read safely"
            ) from None
