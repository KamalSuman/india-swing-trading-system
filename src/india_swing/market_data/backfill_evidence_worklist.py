from __future__ import annotations

import csv
import io
import json
import os
import re
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
from india_swing.identity_registry import (
    CrossVintageIdentityRegistry,
    IdentityAdjudicationQueue,
    IdentityAdjudicationRequirement,
)

from .backfill import HistoricalBackfillIssueCode
from .backfill_blockers import (
    HistoricalBackfillBlockerAction,
    HistoricalBackfillBlockerReport,
)


HISTORICAL_BACKFILL_EVIDENCE_WORK_PACKAGE_SCHEMA_VERSION = (
    "historical-backfill-evidence-work-package/v1"
)
HISTORICAL_BACKFILL_EVIDENCE_WORK_PACKAGE_POLICY_VERSION = (
    "exact-blocker-evidence-procurement-no-satisfaction/v1"
)
HISTORICAL_BACKFILL_EVIDENCE_WORK_PACKAGE_DATASET = (
    "historical-backfill-evidence-work-packages"
)
PACKAGE_FILENAME = "package.json"
WORKLIST_FILENAME = "worklist.csv"
MAXIMUM_WORK_PACKAGE_BYTES = 64 * 1024 * 1024
MAXIMUM_WORKLIST_BYTES = 64 * 1024 * 1024
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class HistoricalBackfillEvidenceWorklistError(ValueError):
    pass


class HistoricalBackfillEvidenceWorklistIntegrityError(
    HistoricalBackfillEvidenceWorklistError
):
    pass


class HistoricalBackfillEvidenceDocumentNeed(str, Enum):
    NSE_DATED_SECURITY_MASTER = "NSE_DATED_SECURITY_MASTER"
    NSE_ADJACENT_DATED_SECURITY_MASTER = (
        "NSE_ADJACENT_DATED_SECURITY_MASTER"
    )
    NSE_REPORT_DATE_PROVENANCE = "NSE_REPORT_DATE_PROVENANCE"
    NSE_LISTING_CIRCULAR_PDF = "NSE_LISTING_CIRCULAR_PDF"
    NSE_CORPORATE_ACTION_CSV = "NSE_CORPORATE_ACTION_CSV"


def _sha(value: str, name: str) -> None:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise ValueError(f"{name} must be lowercase SHA-256")


def _utc(value: datetime, name: str) -> datetime:
    if type(value) is not datetime:
        raise TypeError(f"{name} must be an exact datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _ordered_unique_tuple(
    values: tuple[object, ...],
    *,
    key,
    name: str,
    required: bool = True,
) -> None:
    if (
        type(values) is not tuple
        or (required and not values)
        or values != tuple(sorted(set(values), key=key))
    ):
        raise ValueError(f"{name} must be sorted and unique")


@dataclass(frozen=True, slots=True)
class HistoricalBackfillEvidenceObservation:
    observation_id: str
    claimed_report_date: date
    financial_instrument_id: int
    ticker_symbol: str
    security_series: str
    instrument_name: str
    raw_source_identifier: str
    validated_isin: str | None
    delete_flag: str
    directly_blocked: bool
    record_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha(self.observation_id, "worklist observation_id")
        if type(self.claimed_report_date) is not date:
            raise TypeError("worklist claimed_report_date must be an exact date")
        if (
            type(self.financial_instrument_id) is not int
            or self.financial_instrument_id <= 0
        ):
            raise ValueError(
                "worklist financial_instrument_id must be positive"
            )
        for value, name in (
            (self.ticker_symbol, "ticker_symbol"),
            (self.security_series, "security_series"),
            (self.instrument_name, "instrument_name"),
            (self.raw_source_identifier, "raw_source_identifier"),
        ):
            if type(value) is not str or not value or len(value) > 512:
                raise ValueError(f"worklist {name} must be bounded text")
        if self.validated_isin is not None and (
            type(self.validated_isin) is not str
            or len(self.validated_isin) > 32
        ):
            raise ValueError("worklist validated_isin must be null or text")
        if self.delete_flag not in {"N", "Y"}:
            raise ValueError("worklist delete_flag must be N or Y")
        if type(self.directly_blocked) is not bool:
            raise TypeError("worklist directly_blocked must be bool")
        object.__setattr__(self, "record_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                value.name: getattr(self, value.name)
                for value in fields(self)
                if value.name != "record_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.record_id != self._calculated_id():
            raise HistoricalBackfillEvidenceWorklistIntegrityError(
                "evidence observation identity failed"
            )


@dataclass(frozen=True, slots=True)
class HistoricalBackfillEvidenceCaseRequest:
    candidate_id: str
    adjudication_case_id: str
    issue_ids: tuple[str, ...]
    issue_codes: tuple[HistoricalBackfillIssueCode, ...]
    affected_dates: tuple[date, ...]
    requirements: tuple[IdentityAdjudicationRequirement, ...]
    actions: tuple[HistoricalBackfillBlockerAction, ...]
    document_needs: tuple[HistoricalBackfillEvidenceDocumentNeed, ...]
    observations: tuple[HistoricalBackfillEvidenceObservation, ...]
    evidence_collected: bool = False
    review_completed: bool = False
    request_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha(self.candidate_id, "worklist candidate_id")
        _sha(self.adjudication_case_id, "worklist adjudication_case_id")
        _ordered_unique_tuple(
            self.issue_ids,
            key=lambda value: value,
            name="worklist issue_ids",
        )
        for value in self.issue_ids:
            _sha(value, "worklist issue_id")
        _ordered_unique_tuple(
            self.issue_codes,
            key=lambda value: value.value,
            name="worklist issue_codes",
        )
        _ordered_unique_tuple(
            self.affected_dates,
            key=lambda value: value,
            name="worklist affected_dates",
        )
        if any(type(value) is not date for value in self.affected_dates):
            raise TypeError("worklist affected dates must be exact dates")
        for values, expected, name in (
            (
                self.requirements,
                IdentityAdjudicationRequirement,
                "worklist requirements",
            ),
            (
                self.actions,
                HistoricalBackfillBlockerAction,
                "worklist actions",
            ),
            (
                self.document_needs,
                HistoricalBackfillEvidenceDocumentNeed,
                "worklist document_needs",
            ),
        ):
            _ordered_unique_tuple(
                values,
                key=lambda value: value.value,
                name=name,
            )
            if any(type(value) is not expected for value in values):
                raise TypeError(f"{name} must contain exact enum values")
        if (
            type(self.observations) is not tuple
            or not self.observations
            or any(
                type(value) is not HistoricalBackfillEvidenceObservation
                for value in self.observations
            )
            or self.observations
            != tuple(
                sorted(
                    self.observations,
                    key=lambda value: (
                        value.claimed_report_date,
                        value.ticker_symbol,
                        value.security_series,
                        value.observation_id,
                    ),
                )
            )
            or len({value.observation_id for value in self.observations})
            != len(self.observations)
        ):
            raise ValueError(
                "worklist observations must be ordered, unique, and non-empty"
            )
        for value in self.observations:
            value.verify_content_identity()
        if self.evidence_collected is not False or self.review_completed is not False:
            raise ValueError(
                "an evidence request cannot claim collection or review completion"
            )
        object.__setattr__(self, "request_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                value.name: getattr(self, value.name)
                for value in fields(self)
                if value.name != "request_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        for value in self.observations:
            value.verify_content_identity()
        if self.request_id != self._calculated_id():
            raise HistoricalBackfillEvidenceWorklistIntegrityError(
                "evidence case request identity failed"
            )


@dataclass(frozen=True, slots=True)
class HistoricalBackfillOperationalEvidenceRequest:
    issue_id: str
    issue_code: HistoricalBackfillIssueCode
    affected_dates: tuple[date, ...]
    observation_ids: tuple[str, ...]
    actions: tuple[HistoricalBackfillBlockerAction, ...]
    document_needs: tuple[HistoricalBackfillEvidenceDocumentNeed, ...]
    evidence_collected: bool = False
    request_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha(self.issue_id, "operational issue_id")
        if type(self.issue_code) is not HistoricalBackfillIssueCode:
            raise TypeError("operational issue_code must be exact")
        _ordered_unique_tuple(
            self.affected_dates,
            key=lambda value: value,
            name="operational affected_dates",
        )
        if any(type(value) is not date for value in self.affected_dates):
            raise TypeError("operational affected dates must be exact dates")
        _ordered_unique_tuple(
            self.observation_ids,
            key=lambda value: value,
            name="operational observation_ids",
            required=False,
        )
        for value in self.observation_ids:
            _sha(value, "operational observation_id")
        for values, expected, name in (
            (
                self.actions,
                HistoricalBackfillBlockerAction,
                "operational actions",
            ),
            (
                self.document_needs,
                HistoricalBackfillEvidenceDocumentNeed,
                "operational document_needs",
            ),
        ):
            _ordered_unique_tuple(
                values,
                key=lambda value: value.value,
                name=name,
            )
            if any(type(value) is not expected for value in values):
                raise TypeError(f"{name} must contain exact enum values")
        if self.evidence_collected is not False:
            raise ValueError(
                "an operational evidence request cannot claim completion"
            )
        object.__setattr__(self, "request_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                value.name: getattr(self, value.name)
                for value in fields(self)
                if value.name != "request_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.request_id != self._calculated_id():
            raise HistoricalBackfillEvidenceWorklistIntegrityError(
                "operational evidence request identity failed"
            )


@dataclass(frozen=True, slots=True)
class HistoricalBackfillEvidenceWorkPackage:
    blocker_report_id: str
    plan_id: str
    identity_registry_id: str
    adjudication_queue_id: str
    blocker_generated_at: datetime
    generated_at: datetime
    case_requests: tuple[HistoricalBackfillEvidenceCaseRequest, ...]
    operational_requests: tuple[
        HistoricalBackfillOperationalEvidenceRequest, ...
    ]
    actionable: bool = False
    evidence_satisfied: bool = False
    schema_version: str = (
        HISTORICAL_BACKFILL_EVIDENCE_WORK_PACKAGE_SCHEMA_VERSION
    )
    policy_version: str = (
        HISTORICAL_BACKFILL_EVIDENCE_WORK_PACKAGE_POLICY_VERSION
    )
    package_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.blocker_report_id, "package blocker_report_id"),
            (self.plan_id, "package plan_id"),
            (self.identity_registry_id, "package identity_registry_id"),
            (self.adjudication_queue_id, "package adjudication_queue_id"),
        ):
            _sha(value, name)
        object.__setattr__(
            self,
            "blocker_generated_at",
            _utc(self.blocker_generated_at, "package blocker_generated_at"),
        )
        object.__setattr__(
            self,
            "generated_at",
            _utc(self.generated_at, "package generated_at"),
        )
        if self.generated_at < self.blocker_generated_at:
            raise ValueError("evidence work package cannot predate its blocker report")
        if (
            type(self.case_requests) is not tuple
            or any(
                type(value) is not HistoricalBackfillEvidenceCaseRequest
                for value in self.case_requests
            )
            or self.case_requests
            != tuple(
                sorted(
                    self.case_requests,
                    key=lambda value: value.candidate_id,
                )
            )
            or len({value.candidate_id for value in self.case_requests})
            != len(self.case_requests)
        ):
            raise ValueError(
                "case requests must be candidate-ordered and unique"
            )
        if (
            type(self.operational_requests) is not tuple
            or any(
                type(value)
                is not HistoricalBackfillOperationalEvidenceRequest
                for value in self.operational_requests
            )
            or self.operational_requests
            != tuple(
                sorted(
                    self.operational_requests,
                    key=lambda value: value.issue_id,
                )
            )
            or len(
                {value.issue_id for value in self.operational_requests}
            )
            != len(self.operational_requests)
        ):
            raise ValueError(
                "operational requests must be issue-ordered and unique"
            )
        for value in (*self.case_requests, *self.operational_requests):
            value.verify_content_identity()
        if self.actionable is not False or self.evidence_satisfied is not False:
            raise ValueError(
                "evidence work package cannot authorize or satisfy evidence"
            )
        if (
            self.schema_version
            != HISTORICAL_BACKFILL_EVIDENCE_WORK_PACKAGE_SCHEMA_VERSION
            or self.policy_version
            != HISTORICAL_BACKFILL_EVIDENCE_WORK_PACKAGE_POLICY_VERSION
        ):
            raise ValueError("unsupported evidence work-package contract")
        object.__setattr__(self, "package_id", self._calculated_id())

    @property
    def candidate_count(self) -> int:
        return len(self.case_requests)

    @property
    def observation_count(self) -> int:
        return sum(len(value.observations) for value in self.case_requests)

    @property
    def requirement_pair_count(self) -> int:
        return sum(len(value.requirements) for value in self.case_requests)

    def _calculated_id(self) -> str:
        return content_id(
            {
                value.name: getattr(self, value.name)
                for value in fields(self)
                if value.name != "package_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        for value in (*self.case_requests, *self.operational_requests):
            value.verify_content_identity()
        if self.package_id != self._calculated_id():
            raise HistoricalBackfillEvidenceWorklistIntegrityError(
                "evidence work-package identity failed"
            )


_NEEDS_BY_REQUIREMENT = {
    IdentityAdjudicationRequirement.AUTHORIZED_SOURCE_PROVENANCE: {
        HistoricalBackfillEvidenceDocumentNeed.NSE_DATED_SECURITY_MASTER,
        HistoricalBackfillEvidenceDocumentNeed.NSE_REPORT_DATE_PROVENANCE,
    },
    IdentityAdjudicationRequirement.REPORT_DATE_VERIFICATION: {
        HistoricalBackfillEvidenceDocumentNeed.NSE_REPORT_DATE_PROVENANCE,
    },
    IdentityAdjudicationRequirement.ADJACENT_VINTAGE_OBSERVATION: {
        HistoricalBackfillEvidenceDocumentNeed.NSE_ADJACENT_DATED_SECURITY_MASTER,
    },
    IdentityAdjudicationRequirement.VALIDATED_IDENTIFIER: {
        HistoricalBackfillEvidenceDocumentNeed.NSE_ADJACENT_DATED_SECURITY_MASTER,
        HistoricalBackfillEvidenceDocumentNeed.NSE_LISTING_CIRCULAR_PDF,
    },
    IdentityAdjudicationRequirement.OFFICIAL_CONTINUITY_CONFIRMATION: {
        HistoricalBackfillEvidenceDocumentNeed.NSE_LISTING_CIRCULAR_PDF,
        HistoricalBackfillEvidenceDocumentNeed.NSE_CORPORATE_ACTION_CSV,
    },
    IdentityAdjudicationRequirement.OFFICIAL_LISTING_LIFECYCLE: {
        HistoricalBackfillEvidenceDocumentNeed.NSE_LISTING_CIRCULAR_PDF,
    },
    IdentityAdjudicationRequirement.OFFICIAL_LISTING_STATUS: {
        HistoricalBackfillEvidenceDocumentNeed.NSE_LISTING_CIRCULAR_PDF,
    },
    IdentityAdjudicationRequirement.OFFICIAL_CONFLICT_RESOLUTION: {
        HistoricalBackfillEvidenceDocumentNeed.NSE_LISTING_CIRCULAR_PDF,
        HistoricalBackfillEvidenceDocumentNeed.NSE_CORPORATE_ACTION_CSV,
    },
}


def _needs_for_requirements(
    requirements: tuple[IdentityAdjudicationRequirement, ...],
) -> tuple[HistoricalBackfillEvidenceDocumentNeed, ...]:
    values = {
        need
        for requirement in requirements
        for need in _NEEDS_BY_REQUIREMENT[requirement]
    }
    return tuple(sorted(values, key=lambda value: value.value))


def _needs_for_issue(
    code: HistoricalBackfillIssueCode,
) -> tuple[HistoricalBackfillEvidenceDocumentNeed, ...]:
    if code is HistoricalBackfillIssueCode.MISSING_SECURITY_MASTER_VINTAGE:
        values = {
            HistoricalBackfillEvidenceDocumentNeed.NSE_DATED_SECURITY_MASTER,
            HistoricalBackfillEvidenceDocumentNeed.NSE_REPORT_DATE_PROVENANCE,
        }
    elif code is HistoricalBackfillIssueCode.NON_SESSION_SECURITY_MASTER:
        values = {
            HistoricalBackfillEvidenceDocumentNeed.NSE_REPORT_DATE_PROVENANCE
        }
    else:
        values = {
            HistoricalBackfillEvidenceDocumentNeed.NSE_LISTING_CIRCULAR_PDF
        }
    return tuple(sorted(values, key=lambda value: value.value))


def build_historical_backfill_evidence_work_package(
    *,
    blocker_report: HistoricalBackfillBlockerReport,
    registry: CrossVintageIdentityRegistry,
    adjudication_queue: IdentityAdjudicationQueue,
    generated_at: datetime,
) -> HistoricalBackfillEvidenceWorkPackage:
    if type(blocker_report) is not HistoricalBackfillBlockerReport:
        raise TypeError("blocker_report must be exact")
    if type(registry) is not CrossVintageIdentityRegistry:
        raise TypeError("registry must be exact")
    if type(adjudication_queue) is not IdentityAdjudicationQueue:
        raise TypeError("adjudication_queue must be exact")
    blocker_report.verify_content_identity()
    registry.verify_content_identity()
    adjudication_queue.verify_content_identity()
    if (
        blocker_report.identity_registry_id != registry.registry_id
        or blocker_report.adjudication_queue_id
        != adjudication_queue.queue_id
        or adjudication_queue.source_registry_id != registry.registry_id
    ):
        raise HistoricalBackfillEvidenceWorklistIntegrityError(
            "evidence work-package lineage disagrees"
        )

    observations = {
        value.observation_id: value for value in registry.observations
    }
    candidates = {
        value.candidate_id: value for value in registry.candidates
    }
    cases = {
        value.candidate_id: value for value in adjudication_queue.cases
    }
    entries_by_candidate: dict[str, list[object]] = {}
    operational = []
    for entry in blocker_report.entries:
        if not entry.candidate_ids:
            operational.append(
                HistoricalBackfillOperationalEvidenceRequest(
                    issue_id=entry.issue_id,
                    issue_code=entry.issue_code,
                    affected_dates=entry.affected_dates,
                    observation_ids=entry.observation_ids,
                    actions=entry.actions,
                    document_needs=_needs_for_issue(entry.issue_code),
                )
            )
            continue
        for candidate_id in entry.candidate_ids:
            entries_by_candidate.setdefault(candidate_id, []).append(entry)

    case_requests = []
    for candidate_id, entries in sorted(entries_by_candidate.items()):
        candidate = candidates.get(candidate_id)
        case = cases.get(candidate_id)
        if candidate is None or case is None:
            raise HistoricalBackfillEvidenceWorklistIntegrityError(
                "blocker candidate is absent from the selected identity lineage"
            )
        directly_blocked = {
            observation_id
            for entry in entries
            for observation_id in entry.observation_ids
        }
        if not directly_blocked.issubset(candidate.observation_ids):
            raise HistoricalBackfillEvidenceWorklistIntegrityError(
                "blocker observations do not belong to their candidate"
            )
        case_observations = []
        for observation_id in candidate.observation_ids:
            observation = observations.get(observation_id)
            if observation is None:
                raise HistoricalBackfillEvidenceWorklistIntegrityError(
                    "candidate observation is absent from the registry"
                )
            case_observations.append(
                HistoricalBackfillEvidenceObservation(
                    observation_id=observation.observation_id,
                    claimed_report_date=observation.claimed_report_date,
                    financial_instrument_id=(
                        observation.financial_instrument_id
                    ),
                    ticker_symbol=observation.ticker_symbol,
                    security_series=observation.security_series,
                    instrument_name=observation.instrument_name,
                    raw_source_identifier=(
                        observation.raw_source_identifier
                    ),
                    validated_isin=observation.validated_isin,
                    delete_flag=observation.delete_flag,
                    directly_blocked=(
                        observation.observation_id in directly_blocked
                    ),
                )
            )
        case_requests.append(
            HistoricalBackfillEvidenceCaseRequest(
                candidate_id=candidate_id,
                adjudication_case_id=case.case_id,
                issue_ids=tuple(
                    sorted({value.issue_id for value in entries})
                ),
                issue_codes=tuple(
                    sorted(
                        {value.issue_code for value in entries},
                        key=lambda value: value.value,
                    )
                ),
                affected_dates=tuple(
                    sorted(
                        {
                            day
                            for value in entries
                            for day in value.affected_dates
                        }
                    )
                ),
                requirements=case.requirements,
                actions=tuple(
                    sorted(
                        {
                            action
                            for value in entries
                            for action in value.actions
                        },
                        key=lambda value: value.value,
                    )
                ),
                document_needs=_needs_for_requirements(case.requirements),
                observations=tuple(
                    sorted(
                        case_observations,
                        key=lambda value: (
                            value.claimed_report_date,
                            value.ticker_symbol,
                            value.security_series,
                            value.observation_id,
                        ),
                    )
                ),
            )
        )

    package = HistoricalBackfillEvidenceWorkPackage(
        blocker_report_id=blocker_report.report_id,
        plan_id=blocker_report.plan_id,
        identity_registry_id=registry.registry_id,
        adjudication_queue_id=adjudication_queue.queue_id,
        blocker_generated_at=blocker_report.generated_at,
        generated_at=generated_at,
        case_requests=tuple(
            sorted(case_requests, key=lambda value: value.candidate_id)
        ),
        operational_requests=tuple(
            sorted(operational, key=lambda value: value.issue_id)
        ),
    )
    covered_issue_ids = {
        issue_id
        for value in package.case_requests
        for issue_id in value.issue_ids
    } | {
        value.issue_id for value in package.operational_requests
    }
    if covered_issue_ids != {
        value.issue_id for value in blocker_report.entries
    }:
        raise HistoricalBackfillEvidenceWorklistIntegrityError(
            "evidence work package does not cover every blocker"
        )
    return package


def _observation_value(
    value: HistoricalBackfillEvidenceObservation,
) -> dict[str, object]:
    return {
        "record_id": value.record_id,
        "observation_id": value.observation_id,
        "claimed_report_date": value.claimed_report_date.isoformat(),
        "financial_instrument_id": value.financial_instrument_id,
        "ticker_symbol": value.ticker_symbol,
        "security_series": value.security_series,
        "instrument_name": value.instrument_name,
        "raw_source_identifier": value.raw_source_identifier,
        "validated_isin": value.validated_isin,
        "delete_flag": value.delete_flag,
        "directly_blocked": value.directly_blocked,
    }


def encode_historical_backfill_evidence_work_package(
    package: HistoricalBackfillEvidenceWorkPackage,
) -> bytes:
    if type(package) is not HistoricalBackfillEvidenceWorkPackage:
        raise TypeError("package must be exact")
    package.verify_content_identity()
    value = {
        "package_id": package.package_id,
        "schema_version": package.schema_version,
        "policy_version": package.policy_version,
        "blocker_report_id": package.blocker_report_id,
        "plan_id": package.plan_id,
        "identity_registry_id": package.identity_registry_id,
        "adjudication_queue_id": package.adjudication_queue_id,
        "blocker_generated_at": package.blocker_generated_at.isoformat(),
        "generated_at": package.generated_at.isoformat(),
        "case_requests": [
            {
                "request_id": value.request_id,
                "candidate_id": value.candidate_id,
                "adjudication_case_id": value.adjudication_case_id,
                "issue_ids": list(value.issue_ids),
                "issue_codes": [item.value for item in value.issue_codes],
                "affected_dates": [
                    item.isoformat() for item in value.affected_dates
                ],
                "requirements": [
                    item.value for item in value.requirements
                ],
                "actions": [item.value for item in value.actions],
                "document_needs": [
                    item.value for item in value.document_needs
                ],
                "observations": [
                    _observation_value(item) for item in value.observations
                ],
                "evidence_collected": value.evidence_collected,
                "review_completed": value.review_completed,
            }
            for value in package.case_requests
        ],
        "operational_requests": [
            {
                "request_id": value.request_id,
                "issue_id": value.issue_id,
                "issue_code": value.issue_code.value,
                "affected_dates": [
                    item.isoformat() for item in value.affected_dates
                ],
                "observation_ids": list(value.observation_ids),
                "actions": [item.value for item in value.actions],
                "document_needs": [
                    item.value for item in value.document_needs
                ],
                "evidence_collected": value.evidence_collected,
            }
            for value in package.operational_requests
        ],
        "actionable": package.actionable,
        "evidence_satisfied": package.evidence_satisfied,
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


WORKLIST_COLUMNS = (
    "package_id",
    "request_type",
    "request_id",
    "candidate_id",
    "adjudication_case_id",
    "issue_ids",
    "issue_codes",
    "affected_dates",
    "observation_id",
    "directly_blocked",
    "claimed_report_date",
    "financial_instrument_id",
    "ticker_symbol",
    "security_series",
    "instrument_name",
    "raw_source_identifier",
    "validated_isin",
    "delete_flag",
    "requirement",
    "recommended_document_needs",
    "required_actions",
    "evidence_collected",
    "review_completed",
)


def encode_historical_backfill_evidence_worklist_csv(
    package: HistoricalBackfillEvidenceWorkPackage,
) -> bytes:
    if type(package) is not HistoricalBackfillEvidenceWorkPackage:
        raise TypeError("package must be exact")
    package.verify_content_identity()
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(
        stream,
        fieldnames=WORKLIST_COLUMNS,
        lineterminator="\n",
        extrasaction="raise",
    )
    writer.writeheader()
    for request in package.case_requests:
        for observation in request.observations:
            for requirement in request.requirements:
                writer.writerow(
                    {
                        "package_id": package.package_id,
                        "request_type": "IDENTITY_CASE",
                        "request_id": request.request_id,
                        "candidate_id": request.candidate_id,
                        "adjudication_case_id": (
                            request.adjudication_case_id
                        ),
                        "issue_ids": "|".join(request.issue_ids),
                        "issue_codes": "|".join(
                            value.value for value in request.issue_codes
                        ),
                        "affected_dates": "|".join(
                            value.isoformat()
                            for value in request.affected_dates
                        ),
                        "observation_id": observation.observation_id,
                        "directly_blocked": str(
                            observation.directly_blocked
                        ).lower(),
                        "claimed_report_date": (
                            observation.claimed_report_date.isoformat()
                        ),
                        "financial_instrument_id": (
                            observation.financial_instrument_id
                        ),
                        "ticker_symbol": observation.ticker_symbol,
                        "security_series": observation.security_series,
                        "instrument_name": observation.instrument_name,
                        "raw_source_identifier": (
                            observation.raw_source_identifier
                        ),
                        "validated_isin": observation.validated_isin or "",
                        "delete_flag": observation.delete_flag,
                        "requirement": requirement.value,
                        "recommended_document_needs": "|".join(
                            value.value
                            for value in _needs_for_requirements(
                                (requirement,)
                            )
                        ),
                        "required_actions": "|".join(
                            value.value for value in request.actions
                        ),
                        "evidence_collected": "false",
                        "review_completed": "false",
                    }
                )
    for request in package.operational_requests:
        writer.writerow(
            {
                "package_id": package.package_id,
                "request_type": "OPERATIONAL",
                "request_id": request.request_id,
                "candidate_id": "",
                "adjudication_case_id": "",
                "issue_ids": request.issue_id,
                "issue_codes": request.issue_code.value,
                "affected_dates": "|".join(
                    value.isoformat() for value in request.affected_dates
                ),
                "observation_id": "|".join(request.observation_ids),
                "directly_blocked": "",
                "claimed_report_date": "",
                "financial_instrument_id": "",
                "ticker_symbol": "",
                "security_series": "",
                "instrument_name": "",
                "raw_source_identifier": "",
                "validated_isin": "",
                "delete_flag": "",
                "requirement": "",
                "recommended_document_needs": "|".join(
                    value.value for value in request.document_needs
                ),
                "required_actions": "|".join(
                    value.value for value in request.actions
                ),
                "evidence_collected": "false",
                "review_completed": "false",
            }
        )
    return stream.getvalue().encode("utf-8")


def _reject_duplicate_keys(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON key")
        value[key] = item
    return value


def decode_historical_backfill_evidence_work_package(
    payload: bytes,
) -> HistoricalBackfillEvidenceWorkPackage:
    try:
        root = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        root_keys = {
            "package_id",
            "schema_version",
            "policy_version",
            "blocker_report_id",
            "plan_id",
            "identity_registry_id",
            "adjudication_queue_id",
            "blocker_generated_at",
            "generated_at",
            "case_requests",
            "operational_requests",
            "actionable",
            "evidence_satisfied",
        }
        if type(root) is not dict or set(root) != root_keys:
            raise ValueError
        case_keys = {
            "request_id",
            "candidate_id",
            "adjudication_case_id",
            "issue_ids",
            "issue_codes",
            "affected_dates",
            "requirements",
            "actions",
            "document_needs",
            "observations",
            "evidence_collected",
            "review_completed",
        }
        observation_keys = {
            "record_id",
            "observation_id",
            "claimed_report_date",
            "financial_instrument_id",
            "ticker_symbol",
            "security_series",
            "instrument_name",
            "raw_source_identifier",
            "validated_isin",
            "delete_flag",
            "directly_blocked",
        }
        raw_cases = root["case_requests"]
        if type(raw_cases) is not list:
            raise ValueError
        cases = []
        claimed_case_ids = []
        claimed_observation_ids = []
        calculated_observation_ids = []
        for raw_case in raw_cases:
            if type(raw_case) is not dict or set(raw_case) != case_keys:
                raise ValueError
            raw_observations = raw_case["observations"]
            if type(raw_observations) is not list:
                raise ValueError
            observations = []
            for raw in raw_observations:
                if type(raw) is not dict or set(raw) != observation_keys:
                    raise ValueError
                claimed_observation_ids.append(raw["record_id"])
                observation = HistoricalBackfillEvidenceObservation(
                    observation_id=raw["observation_id"],
                    claimed_report_date=date.fromisoformat(
                        raw["claimed_report_date"]
                    ),
                    financial_instrument_id=raw["financial_instrument_id"],
                    ticker_symbol=raw["ticker_symbol"],
                    security_series=raw["security_series"],
                    instrument_name=raw["instrument_name"],
                    raw_source_identifier=raw["raw_source_identifier"],
                    validated_isin=raw["validated_isin"],
                    delete_flag=raw["delete_flag"],
                    directly_blocked=raw["directly_blocked"],
                )
                calculated_observation_ids.append(observation.record_id)
                observations.append(observation)
            claimed_case_ids.append(raw_case["request_id"])
            cases.append(
                HistoricalBackfillEvidenceCaseRequest(
                    candidate_id=raw_case["candidate_id"],
                    adjudication_case_id=raw_case[
                        "adjudication_case_id"
                    ],
                    issue_ids=tuple(raw_case["issue_ids"]),
                    issue_codes=tuple(
                        HistoricalBackfillIssueCode(value)
                        for value in raw_case["issue_codes"]
                    ),
                    affected_dates=tuple(
                        date.fromisoformat(value)
                        for value in raw_case["affected_dates"]
                    ),
                    requirements=tuple(
                        IdentityAdjudicationRequirement(value)
                        for value in raw_case["requirements"]
                    ),
                    actions=tuple(
                        HistoricalBackfillBlockerAction(value)
                        for value in raw_case["actions"]
                    ),
                    document_needs=tuple(
                        HistoricalBackfillEvidenceDocumentNeed(value)
                        for value in raw_case["document_needs"]
                    ),
                    observations=tuple(observations),
                    evidence_collected=raw_case["evidence_collected"],
                    review_completed=raw_case["review_completed"],
                )
            )
        operational_keys = {
            "request_id",
            "issue_id",
            "issue_code",
            "affected_dates",
            "observation_ids",
            "actions",
            "document_needs",
            "evidence_collected",
        }
        raw_operational = root["operational_requests"]
        if type(raw_operational) is not list:
            raise ValueError
        operational = []
        claimed_operational_ids = []
        for raw in raw_operational:
            if type(raw) is not dict or set(raw) != operational_keys:
                raise ValueError
            claimed_operational_ids.append(raw["request_id"])
            operational.append(
                HistoricalBackfillOperationalEvidenceRequest(
                    issue_id=raw["issue_id"],
                    issue_code=HistoricalBackfillIssueCode(
                        raw["issue_code"]
                    ),
                    affected_dates=tuple(
                        date.fromisoformat(value)
                        for value in raw["affected_dates"]
                    ),
                    observation_ids=tuple(raw["observation_ids"]),
                    actions=tuple(
                        HistoricalBackfillBlockerAction(value)
                        for value in raw["actions"]
                    ),
                    document_needs=tuple(
                        HistoricalBackfillEvidenceDocumentNeed(value)
                        for value in raw["document_needs"]
                    ),
                    evidence_collected=raw["evidence_collected"],
                )
            )
        package = HistoricalBackfillEvidenceWorkPackage(
            blocker_report_id=root["blocker_report_id"],
            plan_id=root["plan_id"],
            identity_registry_id=root["identity_registry_id"],
            adjudication_queue_id=root["adjudication_queue_id"],
            blocker_generated_at=datetime.fromisoformat(
                root["blocker_generated_at"]
            ),
            generated_at=datetime.fromisoformat(root["generated_at"]),
            case_requests=tuple(cases),
            operational_requests=tuple(operational),
            actionable=root["actionable"],
            evidence_satisfied=root["evidence_satisfied"],
            schema_version=root["schema_version"],
            policy_version=root["policy_version"],
        )
        if (
            claimed_observation_ids != calculated_observation_ids
            or claimed_case_ids
            != [value.request_id for value in package.case_requests]
            or claimed_operational_ids
            != [
                value.request_id for value in package.operational_requests
            ]
            or root["package_id"] != package.package_id
        ):
            raise ValueError
        return package
    except HistoricalBackfillEvidenceWorklistIntegrityError:
        raise
    except (
        KeyError,
        TypeError,
        UnicodeDecodeError,
        ValueError,
        json.JSONDecodeError,
    ):
        raise HistoricalBackfillEvidenceWorklistIntegrityError(
            "stored evidence work package is invalid"
        ) from None


def _is_link_like(path: Path) -> bool:
    is_junction = getattr(path, "is_junction", None)
    return path.is_symlink() or bool(is_junction and is_junction())


def _write_fsynced(path: Path, payload: bytes) -> None:
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


class LocalHistoricalBackfillEvidenceWorkPackageStore:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @property
    def dataset_root(self) -> Path:
        return self.root / HISTORICAL_BACKFILL_EVIDENCE_WORK_PACKAGE_DATASET

    def path_for(self, package_id: str) -> Path:
        _sha(package_id, "evidence work package_id")
        return self.dataset_root / package_id

    def put(
        self,
        package: HistoricalBackfillEvidenceWorkPackage,
    ) -> HistoricalBackfillEvidenceWorkPackage:
        if type(package) is not HistoricalBackfillEvidenceWorkPackage:
            raise TypeError("package must be exact")
        package.verify_content_identity()
        package_bytes = encode_historical_backfill_evidence_work_package(
            package
        )
        csv_bytes = encode_historical_backfill_evidence_worklist_csv(package)
        if (
            len(package_bytes) > MAXIMUM_WORK_PACKAGE_BYTES
            or len(csv_bytes) > MAXIMUM_WORKLIST_BYTES
        ):
            raise HistoricalBackfillEvidenceWorklistError(
                "evidence work package exceeds its size limit"
            )
        self.dataset_root.mkdir(parents=True, exist_ok=True)
        target = self.path_for(package.package_id)
        lock = self.dataset_root / ".evidence-work-packages.lock"
        try:
            with advisory_file_lock(lock):
                if target.exists():
                    existing = self._read_path(target)
                    if existing != package:
                        raise HistoricalBackfillEvidenceWorklistIntegrityError(
                            "package ID already stores different content"
                        )
                    return existing
                temporary = Path(
                    tempfile.mkdtemp(
                        prefix=".evidence-work-package-",
                        dir=self.dataset_root,
                    )
                )
                try:
                    _write_fsynced(
                        temporary / PACKAGE_FILENAME,
                        package_bytes,
                    )
                    _write_fsynced(
                        temporary / WORKLIST_FILENAME,
                        csv_bytes,
                    )
                    os.replace(temporary, target)
                except Exception:
                    shutil.rmtree(temporary, ignore_errors=True)
                    raise
        except (FileLockUnavailable, FileSafetyError):
            raise HistoricalBackfillEvidenceWorklistIntegrityError(
                "evidence work-package store is unavailable"
            ) from None
        return self._read_path(target)

    def get(
        self,
        package_id: str,
    ) -> HistoricalBackfillEvidenceWorkPackage:
        target = self.path_for(package_id)
        if not target.exists():
            raise HistoricalBackfillEvidenceWorklistError(
                "evidence work package was not found"
            )
        return self._read_path(target)

    def worklist_path(self, package_id: str) -> Path:
        package = self.get(package_id)
        return self.path_for(package.package_id) / WORKLIST_FILENAME

    def _read_path(
        self,
        target: Path,
    ) -> HistoricalBackfillEvidenceWorkPackage:
        try:
            if not target.is_dir() or _is_link_like(target):
                raise HistoricalBackfillEvidenceWorklistIntegrityError(
                    "evidence work-package path is invalid"
                )
            children = tuple(target.iterdir())
            if (
                {value.name for value in children}
                != {PACKAGE_FILENAME, WORKLIST_FILENAME}
                or any(
                    _is_link_like(value) or not value.is_file()
                    for value in children
                )
            ):
                raise HistoricalBackfillEvidenceWorklistIntegrityError(
                    "evidence work-package directory is invalid"
                )
            package_bytes = read_stable_regular_file(
                target / PACKAGE_FILENAME,
                maximum_bytes=MAXIMUM_WORK_PACKAGE_BYTES,
            )
            csv_bytes = read_stable_regular_file(
                target / WORKLIST_FILENAME,
                maximum_bytes=MAXIMUM_WORKLIST_BYTES,
            )
            package = decode_historical_backfill_evidence_work_package(
                package_bytes
            )
            if (
                target.name != package.package_id
                or package_bytes
                != encode_historical_backfill_evidence_work_package(package)
                or csv_bytes
                != encode_historical_backfill_evidence_worklist_csv(package)
            ):
                raise HistoricalBackfillEvidenceWorklistIntegrityError(
                    "evidence work-package storage identity failed"
                )
            return package
        except HistoricalBackfillEvidenceWorklistIntegrityError:
            raise
        except (FileSafetyError, OSError):
            raise HistoricalBackfillEvidenceWorklistIntegrityError(
                "evidence work package could not be read safely"
            ) from None
