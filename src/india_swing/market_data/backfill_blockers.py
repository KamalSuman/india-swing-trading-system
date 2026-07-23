from __future__ import annotations

import re
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
from india_swing.identity_registry import (
    CrossVintageIdentityRegistry,
    IdentityAdjudicationQueue,
    IdentityAdjudicationRequirement,
)

from .backfill import (
    HistoricalBackfillIssue,
    HistoricalBackfillIssueCode,
    HistoricalBackfillPlan,
)


HISTORICAL_BACKFILL_BLOCKER_SCHEMA_VERSION = (
    "historical-backfill-blocker-report/v1"
)
HISTORICAL_BACKFILL_BLOCKER_POLICY_VERSION = (
    "route-to-sealed-identity-adjudication/v1"
)
HISTORICAL_BACKFILL_BLOCKER_DATASET = "historical-backfill-blocker-reports"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
REPORT_FILENAME = "report.json"
MAXIMUM_BLOCKER_REPORT_BYTES = 32 * 1024 * 1024


class HistoricalBackfillBlockerError(ValueError):
    pass


class HistoricalBackfillBlockerIntegrityError(
    HistoricalBackfillBlockerError
):
    pass


class HistoricalBackfillBlockerAction(str, Enum):
    SUPPLY_DATED_SECURITY_MASTER = "SUPPLY_DATED_SECURITY_MASTER"
    VERIFY_REPORT_DATE_AND_CALENDAR = "VERIFY_REPORT_DATE_AND_CALENDAR"
    REVIEW_EXISTING_ADJUDICATION_CASE = "REVIEW_EXISTING_ADJUDICATION_CASE"
    IMPORT_OFFICIAL_NSE_IDENTITY_EVIDENCE = (
        "IMPORT_OFFICIAL_NSE_IDENTITY_EVIDENCE"
    )
    RESOLVE_CONCURRENT_LISTING_LANES = "RESOLVE_CONCURRENT_LISTING_LANES"
    VERIFY_PROVIDER_ROUTING = "VERIFY_PROVIDER_ROUTING"


def _sha(value: str, field_name: str) -> None:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be lowercase SHA-256")


def _utc(value: datetime, field_name: str) -> datetime:
    if type(value) is not datetime:
        raise TypeError(f"{field_name} must be an exact datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _sha_tuple(values: tuple[str, ...], field_name: str) -> None:
    if (
        type(values) is not tuple
        or values != tuple(sorted(set(values)))
    ):
        raise ValueError(f"{field_name} must be a sorted unique tuple")
    for value in values:
        _sha(value, field_name)


@dataclass(frozen=True, slots=True)
class HistoricalBackfillBlockerEntry:
    issue_id: str
    issue_code: HistoricalBackfillIssueCode
    affected_dates: tuple[date, ...]
    observation_ids: tuple[str, ...]
    candidate_ids: tuple[str, ...]
    adjudication_case_ids: tuple[str, ...]
    requirements: tuple[IdentityAdjudicationRequirement, ...]
    actions: tuple[HistoricalBackfillBlockerAction, ...]
    entry_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha(self.issue_id, "blocker issue_id")
        if type(self.issue_code) is not HistoricalBackfillIssueCode:
            raise TypeError("blocker issue_code must be exact")
        if self.issue_code in {
            HistoricalBackfillIssueCode.DELETED_SECURITY,
            HistoricalBackfillIssueCode.PROVIDER_CATALOG_ABSENT,
            HistoricalBackfillIssueCode.UNSUPPORTED_LISTING_LANE,
        }:
            raise ValueError("non-blocking plan issue cannot enter blocker report")
        if (
            type(self.affected_dates) is not tuple
            or not self.affected_dates
            or any(type(value) is not date for value in self.affected_dates)
            or self.affected_dates
            != tuple(sorted(set(self.affected_dates)))
        ):
            raise ValueError("blocker dates must be sorted unique exact dates")
        for values, name in (
            (self.observation_ids, "blocker observation_ids"),
            (self.candidate_ids, "blocker candidate_ids"),
            (self.adjudication_case_ids, "blocker adjudication_case_ids"),
        ):
            _sha_tuple(values, name)
        if bool(self.candidate_ids) != bool(self.adjudication_case_ids):
            raise ValueError(
                "blocker candidates and adjudication cases must co-occur"
            )
        if (
            type(self.requirements) is not tuple
            or not self.requirements
            or any(
                type(value) is not IdentityAdjudicationRequirement
                for value in self.requirements
            )
            or self.requirements
            != tuple(sorted(set(self.requirements), key=lambda value: value.value))
        ):
            raise ValueError("blocker requirements must be sorted and non-empty")
        if (
            type(self.actions) is not tuple
            or not self.actions
            or any(
                type(value) is not HistoricalBackfillBlockerAction
                for value in self.actions
            )
            or self.actions
            != tuple(sorted(set(self.actions), key=lambda value: value.value))
        ):
            raise ValueError("blocker actions must be sorted and non-empty")
        object.__setattr__(self, "entry_id", self._calculated_id())

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
        if self.entry_id != self._calculated_id():
            raise HistoricalBackfillBlockerIntegrityError(
                "backfill blocker entry identity failed"
            )


@dataclass(frozen=True, slots=True)
class HistoricalBackfillBlockerReport:
    plan_id: str
    identity_registry_id: str
    adjudication_queue_id: str
    requested_at: datetime
    generated_at: datetime
    entries: tuple[HistoricalBackfillBlockerEntry, ...]
    actionable: bool = False
    evidence_satisfied: bool = False
    schema_version: str = HISTORICAL_BACKFILL_BLOCKER_SCHEMA_VERSION
    policy_version: str = HISTORICAL_BACKFILL_BLOCKER_POLICY_VERSION
    report_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.plan_id, "blocker plan_id"),
            (self.identity_registry_id, "blocker identity_registry_id"),
            (self.adjudication_queue_id, "blocker adjudication_queue_id"),
        ):
            _sha(value, name)
        object.__setattr__(
            self,
            "requested_at",
            _utc(self.requested_at, "blocker requested_at"),
        )
        object.__setattr__(
            self,
            "generated_at",
            _utc(self.generated_at, "blocker generated_at"),
        )
        if self.generated_at < self.requested_at:
            raise ValueError("blocker report cannot predate its plan")
        if type(self.entries) is not tuple or any(
            type(value) is not HistoricalBackfillBlockerEntry
            for value in self.entries
        ):
            raise TypeError("blocker entries must be an exact immutable tuple")
        if self.entries != tuple(
            sorted(self.entries, key=lambda value: value.issue_id)
        ) or len({value.issue_id for value in self.entries}) != len(self.entries):
            raise ValueError("blocker entries must be issue-ordered and unique")
        for value in self.entries:
            value.verify_content_identity()
        if self.actionable is not False or self.evidence_satisfied is not False:
            raise ValueError(
                "blocker report cannot authorize or claim evidence satisfaction"
            )
        if (
            self.schema_version != HISTORICAL_BACKFILL_BLOCKER_SCHEMA_VERSION
            or self.policy_version != HISTORICAL_BACKFILL_BLOCKER_POLICY_VERSION
        ):
            raise ValueError("unsupported backfill blocker report contract")
        object.__setattr__(self, "report_id", self._calculated_id())

    @property
    def record_count(self) -> int:
        return len(self.entries)

    @property
    def candidate_count(self) -> int:
        return len(
            {
                candidate_id
                for entry in self.entries
                for candidate_id in entry.candidate_ids
            }
        )

    @property
    def adjudication_case_count(self) -> int:
        return len(
            {
                case_id
                for entry in self.entries
                for case_id in entry.adjudication_case_ids
            }
        )

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
        for value in self.entries:
            value.verify_content_identity()
        if self.report_id != self._calculated_id():
            raise HistoricalBackfillBlockerIntegrityError(
                "backfill blocker report identity failed"
            )


def _fallback_requirements(
    code: HistoricalBackfillIssueCode,
) -> set[IdentityAdjudicationRequirement]:
    values = {
        IdentityAdjudicationRequirement.AUTHORIZED_SOURCE_PROVENANCE,
        IdentityAdjudicationRequirement.REPORT_DATE_VERIFICATION,
    }
    if code is HistoricalBackfillIssueCode.MISSING_SECURITY_MASTER_VINTAGE:
        values.add(IdentityAdjudicationRequirement.ADJACENT_VINTAGE_OBSERVATION)
    elif code is HistoricalBackfillIssueCode.UNVALIDATED_IDENTIFIER:
        values.add(IdentityAdjudicationRequirement.VALIDATED_IDENTIFIER)
    elif code is HistoricalBackfillIssueCode.CONFLICTING_IDENTITY:
        values.add(
            IdentityAdjudicationRequirement.OFFICIAL_CONFLICT_RESOLUTION
        )
    elif code is HistoricalBackfillIssueCode.AMBIGUOUS_PROVIDER_KEY:
        values.add(
            IdentityAdjudicationRequirement.OFFICIAL_LISTING_LIFECYCLE
        )
    return values


def _actions_for(
    issue: HistoricalBackfillIssue,
) -> set[HistoricalBackfillBlockerAction]:
    code = issue.code
    if code is HistoricalBackfillIssueCode.MISSING_SECURITY_MASTER_VINTAGE:
        return {HistoricalBackfillBlockerAction.SUPPLY_DATED_SECURITY_MASTER}
    if code is HistoricalBackfillIssueCode.NON_SESSION_SECURITY_MASTER:
        return {
            HistoricalBackfillBlockerAction.VERIFY_REPORT_DATE_AND_CALENDAR
        }
    if code in {
        HistoricalBackfillIssueCode.CONFLICTING_IDENTITY,
        HistoricalBackfillIssueCode.UNVALIDATED_IDENTIFIER,
    }:
        return {
            HistoricalBackfillBlockerAction.IMPORT_OFFICIAL_NSE_IDENTITY_EVIDENCE,
            HistoricalBackfillBlockerAction.REVIEW_EXISTING_ADJUDICATION_CASE,
        }
    if code is HistoricalBackfillIssueCode.AMBIGUOUS_PROVIDER_KEY:
        return {
            HistoricalBackfillBlockerAction.IMPORT_OFFICIAL_NSE_IDENTITY_EVIDENCE,
            HistoricalBackfillBlockerAction.RESOLVE_CONCURRENT_LISTING_LANES,
            HistoricalBackfillBlockerAction.REVIEW_EXISTING_ADJUDICATION_CASE,
        }
    return {HistoricalBackfillBlockerAction.VERIFY_PROVIDER_ROUTING}


def build_historical_backfill_blocker_report(
    *,
    plan: HistoricalBackfillPlan,
    registry: CrossVintageIdentityRegistry,
    adjudication_queue: IdentityAdjudicationQueue,
    generated_at: datetime,
) -> HistoricalBackfillBlockerReport:
    if type(plan) is not HistoricalBackfillPlan:
        raise TypeError("plan must be an exact HistoricalBackfillPlan")
    if type(registry) is not CrossVintageIdentityRegistry:
        raise TypeError("registry must be exact")
    if type(adjudication_queue) is not IdentityAdjudicationQueue:
        raise TypeError("adjudication_queue must be exact")
    plan.verify_content_identity()
    registry.verify_content_identity()
    adjudication_queue.verify_content_identity()
    if (
        plan.identity_registry_id != registry.registry_id
        or adjudication_queue.source_registry_id != registry.registry_id
        or adjudication_queue.source_cutoff > plan.requested_at
    ):
        raise HistoricalBackfillBlockerError(
            "plan, registry, and adjudication queue lineage disagree"
        )

    candidate_by_observation: dict[str, list[str]] = {}
    for candidate in registry.candidates:
        for observation_id in candidate.observation_ids:
            candidate_by_observation.setdefault(observation_id, []).append(
                candidate.candidate_id
            )
    case_by_candidate = {
        value.candidate_id: value for value in adjudication_queue.cases
    }
    entries: list[HistoricalBackfillBlockerEntry] = []
    for issue in plan.issues:
        if not issue.blocks_collection:
            continue
        candidate_ids = tuple(
            sorted(
                {
                    candidate_id
                    for observation_id in issue.observation_ids
                    for candidate_id in candidate_by_observation.get(
                        observation_id,
                        (),
                    )
                }
            )
        )
        if issue.observation_ids and not candidate_ids:
            raise HistoricalBackfillBlockerIntegrityError(
                "blocker observations are absent from the identity registry"
            )
        try:
            cases = tuple(case_by_candidate[value] for value in candidate_ids)
        except KeyError:
            raise HistoricalBackfillBlockerIntegrityError(
                "blocker candidate is absent from the adjudication queue"
            ) from None
        requirements = _fallback_requirements(issue.code)
        for case in cases:
            requirements.update(case.requirements)
        entries.append(
            HistoricalBackfillBlockerEntry(
                issue_id=issue.issue_id,
                issue_code=issue.code,
                affected_dates=issue.affected_dates,
                observation_ids=issue.observation_ids,
                candidate_ids=candidate_ids,
                adjudication_case_ids=tuple(
                    sorted(value.case_id for value in cases)
                ),
                requirements=tuple(
                    sorted(requirements, key=lambda value: value.value)
                ),
                actions=tuple(
                    sorted(
                        _actions_for(issue),
                        key=lambda value: value.value,
                    )
                ),
            )
        )
    report = HistoricalBackfillBlockerReport(
        plan_id=plan.plan_id,
        identity_registry_id=registry.registry_id,
        adjudication_queue_id=adjudication_queue.queue_id,
        requested_at=plan.requested_at,
        generated_at=generated_at,
        entries=tuple(sorted(entries, key=lambda value: value.issue_id)),
    )
    expected_issue_ids = {
        value.issue_id for value in plan.issues if value.blocks_collection
    }
    if {value.issue_id for value in report.entries} != expected_issue_ids:
        raise HistoricalBackfillBlockerIntegrityError(
            "blocker report does not exactly cover plan blockers"
        )
    return report


def _entry_value(value: HistoricalBackfillBlockerEntry) -> dict[str, object]:
    return {
        "entry_id": value.entry_id,
        "issue_id": value.issue_id,
        "issue_code": value.issue_code.value,
        "affected_dates": [item.isoformat() for item in value.affected_dates],
        "observation_ids": list(value.observation_ids),
        "candidate_ids": list(value.candidate_ids),
        "adjudication_case_ids": list(value.adjudication_case_ids),
        "requirements": [item.value for item in value.requirements],
        "actions": [item.value for item in value.actions],
    }


def encode_historical_backfill_blocker_report(
    report: HistoricalBackfillBlockerReport,
) -> bytes:
    if type(report) is not HistoricalBackfillBlockerReport:
        raise TypeError("report must be exact")
    report.verify_content_identity()
    value = {
        "report_id": report.report_id,
        "schema_version": report.schema_version,
        "policy_version": report.policy_version,
        "plan_id": report.plan_id,
        "identity_registry_id": report.identity_registry_id,
        "adjudication_queue_id": report.adjudication_queue_id,
        "requested_at": report.requested_at.isoformat(),
        "generated_at": report.generated_at.isoformat(),
        "entries": [_entry_value(item) for item in report.entries],
        "actionable": report.actionable,
        "evidence_satisfied": report.evidence_satisfied,
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


def _reject_duplicate_keys(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON key")
        value[key] = item
    return value


def decode_historical_backfill_blocker_report(
    payload: bytes,
) -> HistoricalBackfillBlockerReport:
    try:
        root = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        expected_root = {
            "report_id",
            "schema_version",
            "policy_version",
            "plan_id",
            "identity_registry_id",
            "adjudication_queue_id",
            "requested_at",
            "generated_at",
            "entries",
            "actionable",
            "evidence_satisfied",
        }
        if type(root) is not dict or set(root) != expected_root:
            raise ValueError
        values = root["entries"]
        if type(values) is not list:
            raise ValueError
        expected_entry = {
            "entry_id",
            "issue_id",
            "issue_code",
            "affected_dates",
            "observation_ids",
            "candidate_ids",
            "adjudication_case_ids",
            "requirements",
            "actions",
        }
        entries: list[HistoricalBackfillBlockerEntry] = []
        claimed_entry_ids: list[str] = []
        for value in values:
            if type(value) is not dict or set(value) != expected_entry:
                raise ValueError
            claimed_entry_ids.append(value["entry_id"])
            entries.append(
                HistoricalBackfillBlockerEntry(
                    issue_id=value["issue_id"],
                    issue_code=HistoricalBackfillIssueCode(
                        value["issue_code"]
                    ),
                    affected_dates=tuple(
                        date.fromisoformat(item)
                        for item in value["affected_dates"]
                    ),
                    observation_ids=tuple(value["observation_ids"]),
                    candidate_ids=tuple(value["candidate_ids"]),
                    adjudication_case_ids=tuple(
                        value["adjudication_case_ids"]
                    ),
                    requirements=tuple(
                        IdentityAdjudicationRequirement(item)
                        for item in value["requirements"]
                    ),
                    actions=tuple(
                        HistoricalBackfillBlockerAction(item)
                        for item in value["actions"]
                    ),
                )
            )
        report = HistoricalBackfillBlockerReport(
            plan_id=root["plan_id"],
            identity_registry_id=root["identity_registry_id"],
            adjudication_queue_id=root["adjudication_queue_id"],
            requested_at=datetime.fromisoformat(root["requested_at"]),
            generated_at=datetime.fromisoformat(root["generated_at"]),
            entries=tuple(entries),
            actionable=root["actionable"],
            evidence_satisfied=root["evidence_satisfied"],
            schema_version=root["schema_version"],
            policy_version=root["policy_version"],
        )
        if (
            claimed_entry_ids
            != [value.entry_id for value in report.entries]
            or root["report_id"] != report.report_id
        ):
            raise ValueError
        return report
    except HistoricalBackfillBlockerIntegrityError:
        raise
    except (
        KeyError,
        TypeError,
        UnicodeDecodeError,
        ValueError,
        json.JSONDecodeError,
    ):
        raise HistoricalBackfillBlockerIntegrityError(
            "stored backfill blocker report is invalid"
        ) from None


def _is_link_like(path: Path) -> bool:
    is_junction = getattr(path, "is_junction", None)
    return path.is_symlink() or bool(is_junction and is_junction())


def _write_fsynced(path: Path, payload: bytes) -> None:
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


class LocalHistoricalBackfillBlockerReportStore:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @property
    def dataset_root(self) -> Path:
        return self.root / HISTORICAL_BACKFILL_BLOCKER_DATASET

    def put(
        self,
        report: HistoricalBackfillBlockerReport,
    ) -> HistoricalBackfillBlockerReport:
        if type(report) is not HistoricalBackfillBlockerReport:
            raise TypeError("report must be exact")
        report.verify_content_identity()
        payload = encode_historical_backfill_blocker_report(report)
        if len(payload) > MAXIMUM_BLOCKER_REPORT_BYTES:
            raise HistoricalBackfillBlockerError(
                "backfill blocker report exceeds its size limit"
            )
        self.dataset_root.mkdir(parents=True, exist_ok=True)
        target = self.dataset_root / report.report_id
        lock = self.dataset_root / ".blocker-reports.lock"
        try:
            with advisory_file_lock(lock):
                if target.exists():
                    existing = self._read_path(target)
                    if existing != report:
                        raise HistoricalBackfillBlockerIntegrityError(
                            "report ID already stores different content"
                        )
                    return existing
                temporary = Path(
                    tempfile.mkdtemp(
                        prefix=".backfill-blockers-",
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
            raise HistoricalBackfillBlockerIntegrityError(
                "backfill blocker report store is unavailable"
            ) from None
        return self._read_path(target)

    def get(self, report_id: str) -> HistoricalBackfillBlockerReport:
        _sha(report_id, "blocker report_id")
        target = self.dataset_root / report_id
        if not target.exists():
            raise HistoricalBackfillBlockerError(
                "backfill blocker report was not found"
            )
        return self._read_path(target)

    def _read_path(self, target: Path) -> HistoricalBackfillBlockerReport:
        try:
            if not target.is_dir() or _is_link_like(target):
                raise HistoricalBackfillBlockerIntegrityError(
                    "backfill blocker report path is invalid"
                )
            children = tuple(target.iterdir())
            if (
                {value.name for value in children} != {REPORT_FILENAME}
                or any(
                    _is_link_like(value) or not value.is_file()
                    for value in children
                )
            ):
                raise HistoricalBackfillBlockerIntegrityError(
                    "backfill blocker report directory is invalid"
                )
            payload = read_stable_regular_file(
                target / REPORT_FILENAME,
                maximum_bytes=MAXIMUM_BLOCKER_REPORT_BYTES,
            )
            report = decode_historical_backfill_blocker_report(payload)
            if (
                target.name != report.report_id
                or payload != encode_historical_backfill_blocker_report(report)
            ):
                raise HistoricalBackfillBlockerIntegrityError(
                    "backfill blocker report storage identity failed"
                )
            return report
        except HistoricalBackfillBlockerIntegrityError:
            raise
        except (FileSafetyError, OSError):
            raise HistoricalBackfillBlockerIntegrityError(
                "backfill blocker report could not be read safely"
            ) from None
