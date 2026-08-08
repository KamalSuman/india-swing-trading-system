"""Research Dataset v1: a lineage/control manifest over verified NSE archive ranges.

This module binds multiple exact, already-verified
``VerifiedNseHistoricalArchiveRange`` objects (produced only by the existing
public trust boundary ``load_verified_nse_historical_archive_range``) into
one chronological, leakage-aware research corpus definition.

It is a compact manifest, not a materialized price panel: it references
exact range/session snapshot IDs and aggregate counts. It never reads the
filesystem, network, environment, or clock; never lists, discovers, or
selects a "latest" artifact; and never marks raw archive evidence as
training-, feature-, label-, alert-, or execution-eligible.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, timedelta
from enum import Enum
from typing import Mapping

from india_swing.identity import content_id
from india_swing.market_data.nse_archive import (
    EVIDENCE_PROFILE_COMPLETE,
    EVIDENCE_PROFILE_PRICE_UDIFF,
    EVIDENCE_PROFILE_PRICE_UDIFF_SECURITY,
    EVIDENCE_PROFILE_UNRECONCILED,
)
from india_swing.market_data.nse_archive_range import (
    NseHistoricalArchiveSnapshotReader,
    VerifiedNseHistoricalArchiveRange,
    load_verified_nse_historical_archive_range,
)
from india_swing.market_data.snapshot_store import StoredMarketSnapshot


RESEARCH_DATASET_SCHEMA_VERSION = "nse-archive-research-dataset/v1"
SPLIT_POLICY_SCHEMA_VERSION = "nse-archive-research-split-policy/v1"
EXCLUSION_SCHEMA_VERSION = "nse-archive-research-exclusion/v1"
RANGE_BINDING_SCHEMA_VERSION = "nse-archive-research-range-binding/v1"
PARTITION_SCHEMA_VERSION = "nse-archive-research-partition/v1"

MINIMUM_FORWARD_LABEL_HORIZON_SESSIONS = 20

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_KNOWN_EVIDENCE_PROFILES = (
    EVIDENCE_PROFILE_PRICE_UDIFF,
    EVIDENCE_PROFILE_PRICE_UDIFF_SECURITY,
    EVIDENCE_PROFILE_COMPLETE,
    EVIDENCE_PROFILE_UNRECONCILED,
)


class NseArchiveResearchDatasetError(ValueError):
    """A research dataset input or artifact failed a static safety rule."""


class NseArchiveResearchDatasetIntegrityError(NseArchiveResearchDatasetError):
    """Persisted or reconstructed research dataset evidence failed re-verification."""


def _fail(message: str) -> None:
    raise NseArchiveResearchDatasetError(message)


def _is_sha256(value: object) -> bool:
    return type(value) is str and _SHA256_PATTERN.fullmatch(value) is not None


def _sorted_counts(counts: Mapping[str, int]) -> tuple[tuple[str, int], ...]:
    return tuple(sorted(counts.items()))


class ResearchArchiveExclusionReason(Enum):
    SOURCE_ACCOUNTING_FAILED = "SOURCE_ACCOUNTING_FAILED"
    SOURCE_CROSS_SOURCE_JOIN_FAILED = "SOURCE_CROSS_SOURCE_JOIN_FAILED"


@dataclass(frozen=True, slots=True)
class ResearchArchiveExclusion:
    """One explicit, content-addressed unresolved-source-session exclusion."""

    session: date
    reason: ResearchArchiveExclusionReason
    exclusion_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "exclusion_id", self._calculated_id())

    def _validate(self) -> None:
        if type(self.session) is not date:
            _fail("research archive exclusion session must be an exact date")
        if type(self.reason) is not ResearchArchiveExclusionReason:
            _fail("research archive exclusion reason must be an exact enum member")

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": EXCLUSION_SCHEMA_VERSION,
                "session": self.session,
                "reason": self.reason,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.exclusion_id != self._calculated_id():
            raise NseArchiveResearchDatasetIntegrityError(
                "research archive exclusion identity failed"
            )


class ResearchSplitRole(Enum):
    TRAIN = "TRAIN"
    VALIDATION = "VALIDATION"
    UNTOUCHED_TEST = "UNTOUCHED_TEST"


@dataclass(frozen=True, slots=True)
class ResearchArchiveSplitPolicy:
    """An explicit, non-overlapping, calendar-adjacent chronological split policy."""

    train_end: date
    validation_start: date
    validation_end: date
    test_start: date
    maximum_forward_label_horizon_sessions: int
    policy_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "policy_id", self._calculated_id())

    def _validate(self) -> None:
        for value, name in (
            (self.train_end, "train_end"),
            (self.validation_start, "validation_start"),
            (self.validation_end, "validation_end"),
            (self.test_start, "test_start"),
        ):
            if type(value) is not date:
                _fail(f"research split policy {name} must be an exact date")
        if type(self.maximum_forward_label_horizon_sessions) is not int:
            _fail("research split policy horizon must be an exact integer")
        if (
            self.maximum_forward_label_horizon_sessions
            < MINIMUM_FORWARD_LABEL_HORIZON_SESSIONS
        ):
            _fail(
                "research split policy horizon must be at least "
                f"{MINIMUM_FORWARD_LABEL_HORIZON_SESSIONS} trading sessions"
            )
        if self.validation_start != self.train_end + timedelta(days=1):
            _fail(
                "research split policy train/validation boundary must be calendar-adjacent"
            )
        if self.validation_end < self.validation_start:
            _fail("research split policy validation window is reversed or empty")
        if self.test_start != self.validation_end + timedelta(days=1):
            _fail(
                "research split policy validation/test boundary must be calendar-adjacent"
            )

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": SPLIT_POLICY_SCHEMA_VERSION,
                "train_end": self.train_end,
                "validation_start": self.validation_start,
                "validation_end": self.validation_end,
                "test_start": self.test_start,
                "maximum_forward_label_horizon_sessions": (
                    self.maximum_forward_label_horizon_sessions
                ),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.policy_id != self._calculated_id():
            raise NseArchiveResearchDatasetIntegrityError(
                "research split policy identity failed"
            )


@dataclass(frozen=True, slots=True)
class NseArchiveResearchRangeBinding:
    """One immutable per-range binding, independently re-checked before its ID."""

    index_snapshot_id: str
    range_start: date
    range_end: date
    session_snapshot_ids: tuple[str, ...]
    accepted_sessions: tuple[date, ...]
    record_count: int
    identity_issue_count: int
    identity_quarantined_session_count: int
    incomplete_evidence_session_count: int
    evidence_profile_counts: tuple[tuple[str, int], ...]
    binding_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "binding_id", self._calculated_id())

    def _validate(self) -> None:
        if not _is_sha256(self.index_snapshot_id):
            _fail("research range binding index snapshot id is invalid")
        if (
            type(self.range_start) is not date
            or type(self.range_end) is not date
            or self.range_start > self.range_end
        ):
            _fail("research range binding calendar envelope is invalid")
        if (
            type(self.session_snapshot_ids) is not tuple
            or type(self.accepted_sessions) is not tuple
            or not self.session_snapshot_ids
            or len(self.session_snapshot_ids) != len(self.accepted_sessions)
        ):
            _fail("research range binding session lineage is invalid")
        if any(not _is_sha256(value) for value in self.session_snapshot_ids):
            _fail("research range binding session snapshot id is invalid")
        if len(set(self.session_snapshot_ids)) != len(self.session_snapshot_ids):
            _fail("research range binding session snapshot ids must be unique")
        if (
            any(type(value) is not date for value in self.accepted_sessions)
            or self.accepted_sessions != tuple(sorted(set(self.accepted_sessions)))
        ):
            _fail("research range binding accepted sessions must be sorted and unique")
        if any(
            not self.range_start <= value <= self.range_end
            for value in self.accepted_sessions
        ):
            _fail(
                "research range binding accepted session lies outside its calendar envelope"
            )
        for value, name in (
            (self.record_count, "record_count"),
            (self.identity_issue_count, "identity_issue_count"),
            (
                self.identity_quarantined_session_count,
                "identity_quarantined_session_count",
            ),
            (
                self.incomplete_evidence_session_count,
                "incomplete_evidence_session_count",
            ),
        ):
            if type(value) is not int or value < 0:
                _fail(f"research range binding {name} must be a non-negative exact integer")
        if self.identity_quarantined_session_count > len(self.accepted_sessions):
            _fail("research range binding quarantine accounting is invalid")
        if self.incomplete_evidence_session_count > len(self.accepted_sessions):
            _fail("research range binding evidence accounting is invalid")
        if (
            type(self.evidence_profile_counts) is not tuple
            or self.evidence_profile_counts != tuple(sorted(self.evidence_profile_counts))
            or {profile for profile, _ in self.evidence_profile_counts}
            != set(_KNOWN_EVIDENCE_PROFILES)
            or any(
                type(count) is not int or count < 0
                for _, count in self.evidence_profile_counts
            )
        ):
            _fail("research range binding evidence profile counts are invalid")
        if sum(count for _, count in self.evidence_profile_counts) != len(
            self.accepted_sessions
        ):
            _fail("research range binding evidence profile counts are inconsistent")
        incomplete_from_profiles = sum(
            count
            for profile, count in self.evidence_profile_counts
            if profile != EVIDENCE_PROFILE_COMPLETE
        )
        if incomplete_from_profiles != self.incomplete_evidence_session_count:
            _fail("research range binding evidence profile accounting is inconsistent")

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": RANGE_BINDING_SCHEMA_VERSION,
                "index_snapshot_id": self.index_snapshot_id,
                "range_start": self.range_start,
                "range_end": self.range_end,
                "session_snapshot_ids": self.session_snapshot_ids,
                "accepted_sessions": self.accepted_sessions,
                "record_count": self.record_count,
                "identity_issue_count": self.identity_issue_count,
                "identity_quarantined_session_count": (
                    self.identity_quarantined_session_count
                ),
                "incomplete_evidence_session_count": (
                    self.incomplete_evidence_session_count
                ),
                "evidence_profile_counts": self.evidence_profile_counts,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.binding_id != self._calculated_id():
            raise NseArchiveResearchDatasetIntegrityError(
                "research range binding identity failed"
            )

    @classmethod
    def from_verified_range(
        cls, range_obj: VerifiedNseHistoricalArchiveRange
    ) -> "NseArchiveResearchRangeBinding":
        """Build one binding from an exact verified range, re-checking it independently."""

        if type(range_obj) is not VerifiedNseHistoricalArchiveRange:
            _fail("verified archive range type is invalid")
        if (
            type(range_obj.session_snapshot_ids) is not tuple
            or type(range_obj.sessions) is not tuple
            or len(range_obj.session_snapshot_ids) != len(range_obj.sessions)
            or not range_obj.sessions
        ):
            _fail("verified archive range session lineage is invalid")

        accepted_sessions: list[date] = []
        record_count_total = 0
        identity_issue_total = 0
        quarantined_total = 0
        for snapshot_id, stored in zip(
            range_obj.session_snapshot_ids, range_obj.sessions
        ):
            if type(stored) is not StoredMarketSnapshot:
                _fail("verified archive range session snapshot type is invalid")
            if stored.manifest.snapshot_id != snapshot_id:
                _fail("verified archive range session snapshot identity is inconsistent")
            payload = stored.normalized_payload
            if not isinstance(payload, Mapping):
                _fail("verified archive range session payload is invalid")
            session = payload.get("session")
            if type(session) is not date:
                _fail("verified archive range session date is invalid")
            if (
                payload.get("collection_only") is not True
                or payload.get("actionable") is not False
                or payload.get("training_eligible") is not False
            ):
                _fail("verified archive range session safety posture is invalid")
            issue_count = payload.get("identity_issue_count")
            if type(issue_count) is not int or issue_count < 0:
                _fail("verified archive range session identity accounting is invalid")
            accepted_sessions.append(session)
            record_count_total += stored.manifest.record_count
            identity_issue_total += issue_count
            quarantined_total += issue_count > 0

        accepted_sessions_tuple = tuple(accepted_sessions)
        if accepted_sessions_tuple != tuple(sorted(set(accepted_sessions_tuple))):
            _fail("verified archive range sessions must be sorted and unique")
        if record_count_total != range_obj.record_count:
            _fail("verified archive range record count is inconsistent")
        if identity_issue_total != range_obj.identity_issue_count:
            _fail("verified archive range identity issue accounting is inconsistent")
        if quarantined_total != range_obj.identity_quarantined_session_count:
            _fail("verified archive range quarantine accounting is inconsistent")

        evidence_counts = range_obj.evidence_profile_counts
        if (
            not isinstance(evidence_counts, Mapping)
            or set(evidence_counts) != set(_KNOWN_EVIDENCE_PROFILES)
            or any(
                type(value) is not int or value < 0
                for value in evidence_counts.values()
            )
        ):
            _fail("verified archive range evidence profile counts are invalid")
        if sum(evidence_counts.values()) != len(accepted_sessions_tuple):
            _fail("verified archive range evidence profile counts are inconsistent")
        incomplete_from_profiles = sum(
            count
            for profile, count in evidence_counts.items()
            if profile != EVIDENCE_PROFILE_COMPLETE
        )
        if incomplete_from_profiles != range_obj.incomplete_evidence_session_count:
            _fail("verified archive range evidence profile accounting is inconsistent")

        return cls(
            index_snapshot_id=range_obj.index_snapshot_id,
            range_start=range_obj.range_start,
            range_end=range_obj.range_end,
            session_snapshot_ids=range_obj.session_snapshot_ids,
            accepted_sessions=accepted_sessions_tuple,
            record_count=range_obj.record_count,
            identity_issue_count=range_obj.identity_issue_count,
            identity_quarantined_session_count=(
                range_obj.identity_quarantined_session_count
            ),
            incomplete_evidence_session_count=(
                range_obj.incomplete_evidence_session_count
            ),
            evidence_profile_counts=_sorted_counts(evidence_counts),
        )


@dataclass(frozen=True, slots=True)
class NseArchiveResearchDatasetSplitPartition:
    """One chronological split role's sessions, with a reserved forward-label tail.

    The horizon is retained and bound into this partition's own identity so
    that its candidate/tail split can be independently re-derived and
    verified from ``sessions`` and ``maximum_forward_label_horizon_sessions``
    alone: a zero, short, or otherwise arbitrary tail cannot be constructed,
    let alone survive reconstruction, regardless of what a caller supplies.
    """

    role: ResearchSplitRole
    sessions: tuple[date, ...]
    candidate_label_origin_sessions: tuple[date, ...]
    unavailable_label_tail_sessions: tuple[date, ...]
    maximum_forward_label_horizon_sessions: int
    partition_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "partition_id", self._calculated_id())

    def _validate(self) -> None:
        if type(self.role) is not ResearchSplitRole:
            _fail("research split partition role must be an exact enum member")
        if (
            type(self.sessions) is not tuple
            or not self.sessions
            or any(type(value) is not date for value in self.sessions)
            or self.sessions != tuple(sorted(set(self.sessions)))
        ):
            _fail("research split partition sessions must be sorted and unique")
        if (
            type(self.candidate_label_origin_sessions) is not tuple
            or type(self.unavailable_label_tail_sessions) is not tuple
        ):
            _fail("research split partition label accounting is invalid")
        if type(self.maximum_forward_label_horizon_sessions) is not int:
            _fail("research split partition horizon must be an exact integer")
        if (
            self.maximum_forward_label_horizon_sessions
            < MINIMUM_FORWARD_LABEL_HORIZON_SESSIONS
        ):
            _fail(
                "research split partition horizon must be at least "
                f"{MINIMUM_FORWARD_LABEL_HORIZON_SESSIONS} trading sessions"
            )
        horizon = self.maximum_forward_label_horizon_sessions
        if len(self.sessions) <= horizon:
            _fail("research split partition has no candidate label origin sessions")
        if (
            self.candidate_label_origin_sessions != self.sessions[:-horizon]
            or self.unavailable_label_tail_sessions != self.sessions[-horizon:]
        ):
            _fail(
                "research split partition label accounting does not match "
                "its declared horizon"
            )

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": PARTITION_SCHEMA_VERSION,
                "role": self.role,
                "sessions": self.sessions,
                "candidate_label_origin_sessions": self.candidate_label_origin_sessions,
                "unavailable_label_tail_sessions": self.unavailable_label_tail_sessions,
                "maximum_forward_label_horizon_sessions": (
                    self.maximum_forward_label_horizon_sessions
                ),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.partition_id != self._calculated_id():
            raise NseArchiveResearchDatasetIntegrityError(
                "research split partition identity failed"
            )


_EXPECTED_PARTITION_ROLES = (
    ResearchSplitRole.TRAIN,
    ResearchSplitRole.VALIDATION,
    ResearchSplitRole.UNTOUCHED_TEST,
)
_ALWAYS_FALSE_SAFETY_FLAGS = (
    "feature_eligible",
    "label_eligible",
    "alert_eligible",
    "execution_eligible",
    "identity_resolution_complete",
    "corporate_action_adjustment_complete",
)


@dataclass(frozen=True, slots=True)
class NseArchiveResearchDataset:
    """The immutable Research Dataset v1 lineage/control manifest."""

    index_snapshot_ids: tuple[str, ...]
    range_bindings: tuple[NseArchiveResearchRangeBinding, ...]
    accepted_sessions: tuple[date, ...]
    session_snapshot_ids: tuple[str, ...]
    exclusions: tuple[ResearchArchiveExclusion, ...]
    partitions: tuple[NseArchiveResearchDatasetSplitPartition, ...]
    record_count: int
    identity_issue_count: int
    identity_quarantined_session_count: int
    incomplete_evidence_session_count: int
    evidence_profile_counts: tuple[tuple[str, int], ...]
    split_policy: ResearchArchiveSplitPolicy
    split_policy_id: str = field(init=False)
    collection_only: bool = field(init=False)
    actionable: bool = field(init=False)
    training_eligible: bool = field(init=False)
    feature_eligible: bool = field(init=False)
    label_eligible: bool = field(init=False)
    alert_eligible: bool = field(init=False)
    execution_eligible: bool = field(init=False)
    identity_resolution_complete: bool = field(init=False)
    corporate_action_adjustment_complete: bool = field(init=False)
    coverage_complete: bool = field(init=False)
    dataset_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.split_policy) is not ResearchArchiveSplitPolicy:
            _fail("research dataset split policy is invalid")
        self.split_policy.verify_content_identity()
        object.__setattr__(self, "split_policy_id", self.split_policy.policy_id)
        object.__setattr__(self, "collection_only", True)
        object.__setattr__(self, "actionable", False)
        object.__setattr__(self, "training_eligible", False)
        object.__setattr__(self, "feature_eligible", False)
        object.__setattr__(self, "label_eligible", False)
        object.__setattr__(self, "alert_eligible", False)
        object.__setattr__(self, "execution_eligible", False)
        object.__setattr__(self, "identity_resolution_complete", False)
        object.__setattr__(self, "corporate_action_adjustment_complete", False)
        object.__setattr__(self, "coverage_complete", len(self.exclusions) == 0)
        self._validate()
        object.__setattr__(self, "dataset_id", self._calculated_id())

    def _validate(self) -> None:
        if type(self.split_policy) is not ResearchArchiveSplitPolicy:
            _fail("research dataset split policy is invalid")
        self.split_policy.verify_content_identity()
        if self.split_policy_id != self.split_policy.policy_id:
            _fail("research dataset split policy id is inconsistent")
        if (
            type(self.index_snapshot_ids) is not tuple
            or not self.index_snapshot_ids
            or any(not _is_sha256(value) for value in self.index_snapshot_ids)
            or len(set(self.index_snapshot_ids)) != len(self.index_snapshot_ids)
        ):
            _fail("research dataset index snapshot ids are invalid")
        if (
            type(self.range_bindings) is not tuple
            or len(self.range_bindings) != len(self.index_snapshot_ids)
            or any(
                type(value) is not NseArchiveResearchRangeBinding
                for value in self.range_bindings
            )
        ):
            _fail("research dataset range bindings are invalid")
        for binding, index_snapshot_id in zip(
            self.range_bindings, self.index_snapshot_ids
        ):
            binding.verify_content_identity()
            if binding.index_snapshot_id != index_snapshot_id:
                _fail("research dataset range binding index lineage is inconsistent")

        previous: NseArchiveResearchRangeBinding | None = None
        combined_sessions: list[date] = []
        combined_session_snapshot_ids: list[str] = []
        for binding in self.range_bindings:
            if previous is not None:
                if binding.range_start <= previous.range_end:
                    _fail("research dataset range bindings overlap or reorder")
                if binding.range_start != previous.range_end + timedelta(days=1):
                    _fail("research dataset range bindings are not calendar-adjacent")
            combined_sessions.extend(binding.accepted_sessions)
            combined_session_snapshot_ids.extend(binding.session_snapshot_ids)
            previous = binding
        if tuple(combined_sessions) != self.accepted_sessions:
            _fail("research dataset accepted sessions are inconsistent")
        if tuple(combined_session_snapshot_ids) != self.session_snapshot_ids:
            _fail("research dataset session snapshot ids are inconsistent")
        if len(set(self.accepted_sessions)) != len(self.accepted_sessions):
            _fail("research dataset accepted sessions must be unique")
        if len(set(self.session_snapshot_ids)) != len(self.session_snapshot_ids):
            _fail("research dataset session snapshot ids must be unique")

        if type(self.exclusions) is not tuple or any(
            type(value) is not ResearchArchiveExclusion for value in self.exclusions
        ):
            _fail("research dataset exclusions are invalid")
        exclusion_sessions = tuple(value.session for value in self.exclusions)
        if exclusion_sessions != tuple(sorted(set(exclusion_sessions))):
            _fail("research dataset exclusions must be sorted and unique")
        accepted_session_set = set(self.accepted_sessions)
        envelope_start = self.range_bindings[0].range_start
        envelope_end = self.range_bindings[-1].range_end
        for exclusion in self.exclusions:
            exclusion.verify_content_identity()
            if not envelope_start <= exclusion.session <= envelope_end:
                _fail(
                    "research dataset exclusion lies outside the combined calendar envelope"
                )
            if exclusion.session in accepted_session_set:
                _fail("research dataset exclusion collides with an accepted session")

        if (
            type(self.partitions) is not tuple
            or len(self.partitions) != 3
            or any(
                type(value) is not NseArchiveResearchDatasetSplitPartition
                for value in self.partitions
            )
        ):
            _fail("research dataset partitions are invalid")
        if tuple(value.role for value in self.partitions) != _EXPECTED_PARTITION_ROLES:
            _fail(
                "research dataset partitions must cover train, validation, "
                "and untouched test in that order"
            )
        # Each partition's sessions must equal the tuple independently derived
        # from accepted_sessions and the retained, freshly re-verified split
        # policy's own boundaries -- not merely be disjoint and exhaustive.
        # Because the policy's own validation already requires
        # validation_start == train_end + 1 day and test_start ==
        # validation_end + 1 day, these three derived tuples are always
        # disjoint and exhaustive over accepted_sessions by construction.
        policy = self.split_policy
        expected_role_sessions = {
            ResearchSplitRole.TRAIN: tuple(
                value for value in self.accepted_sessions if value <= policy.train_end
            ),
            ResearchSplitRole.VALIDATION: tuple(
                value
                for value in self.accepted_sessions
                if policy.validation_start <= value <= policy.validation_end
            ),
            ResearchSplitRole.UNTOUCHED_TEST: tuple(
                value
                for value in self.accepted_sessions
                if value >= policy.test_start
            ),
        }
        for partition in self.partitions:
            partition.verify_content_identity()
            if (
                partition.maximum_forward_label_horizon_sessions
                != policy.maximum_forward_label_horizon_sessions
            ):
                _fail(
                    "research dataset partition horizon does not match its split policy"
                )
            if partition.sessions != expected_role_sessions[partition.role]:
                _fail(
                    "research dataset partition sessions do not match the "
                    "derived split policy role boundary"
                )

        if self.record_count != sum(
            value.record_count for value in self.range_bindings
        ):
            _fail("research dataset record count is inconsistent")
        if self.identity_issue_count != sum(
            value.identity_issue_count for value in self.range_bindings
        ):
            _fail("research dataset identity issue count is inconsistent")
        if self.identity_quarantined_session_count != sum(
            value.identity_quarantined_session_count for value in self.range_bindings
        ):
            _fail("research dataset quarantine accounting is inconsistent")
        if self.incomplete_evidence_session_count != sum(
            value.incomplete_evidence_session_count for value in self.range_bindings
        ):
            _fail("research dataset evidence accounting is inconsistent")
        expected_profile_counts = {profile: 0 for profile in _KNOWN_EVIDENCE_PROFILES}
        for binding in self.range_bindings:
            for profile, count in binding.evidence_profile_counts:
                expected_profile_counts[profile] += count
        if self.evidence_profile_counts != _sorted_counts(expected_profile_counts):
            _fail("research dataset evidence profile counts are inconsistent")

        if (
            self.collection_only is not True
            or self.actionable is not False
            or self.training_eligible is not False
        ):
            _fail("research dataset safety posture is invalid")
        for flag_name in _ALWAYS_FALSE_SAFETY_FLAGS:
            if getattr(self, flag_name) is not False:
                _fail("research dataset safety posture is invalid")
        if self.coverage_complete is not (len(self.exclusions) == 0):
            _fail("research dataset coverage completeness is inconsistent")

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": RESEARCH_DATASET_SCHEMA_VERSION,
                "index_snapshot_ids": self.index_snapshot_ids,
                "range_binding_ids": tuple(
                    value.binding_id for value in self.range_bindings
                ),
                "accepted_sessions": self.accepted_sessions,
                "session_snapshot_ids": self.session_snapshot_ids,
                "exclusion_ids": tuple(
                    value.exclusion_id for value in self.exclusions
                ),
                "partition_ids": tuple(
                    value.partition_id for value in self.partitions
                ),
                "record_count": self.record_count,
                "identity_issue_count": self.identity_issue_count,
                "identity_quarantined_session_count": (
                    self.identity_quarantined_session_count
                ),
                "incomplete_evidence_session_count": (
                    self.incomplete_evidence_session_count
                ),
                "evidence_profile_counts": self.evidence_profile_counts,
                "split_policy_id": self.split_policy_id,
                "collection_only": self.collection_only,
                "actionable": self.actionable,
                "training_eligible": self.training_eligible,
                "feature_eligible": self.feature_eligible,
                "label_eligible": self.label_eligible,
                "alert_eligible": self.alert_eligible,
                "execution_eligible": self.execution_eligible,
                "identity_resolution_complete": self.identity_resolution_complete,
                "corporate_action_adjustment_complete": (
                    self.corporate_action_adjustment_complete
                ),
                "coverage_complete": self.coverage_complete,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.dataset_id != self._calculated_id():
            raise NseArchiveResearchDatasetIntegrityError(
                "research dataset identity failed"
            )


def build_nse_archive_research_dataset(
    reader: NseHistoricalArchiveSnapshotReader,
    *,
    index_snapshot_ids: tuple[str, ...],
    split_policy: ResearchArchiveSplitPolicy,
    exclusions: tuple[ResearchArchiveExclusion, ...] = (),
) -> NseArchiveResearchDataset:
    """Build the Research Dataset v1 manifest from exact, pre-identified range indexes.

    Calls ``load_verified_nse_historical_archive_range`` exactly once per exact
    requested ID, in the supplied order. Never lists, discovers, or selects a
    latest range, and never continues past one invalid or inconsistent range.
    """

    if (
        type(index_snapshot_ids) is not tuple
        or not index_snapshot_ids
        or any(not _is_sha256(value) for value in index_snapshot_ids)
        or len(set(index_snapshot_ids)) != len(index_snapshot_ids)
    ):
        _fail("research dataset index snapshot ids are invalid")
    if type(split_policy) is not ResearchArchiveSplitPolicy:
        _fail("research dataset split policy is invalid")
    split_policy.verify_content_identity()
    if type(exclusions) is not tuple or any(
        type(value) is not ResearchArchiveExclusion for value in exclusions
    ):
        _fail("research dataset exclusions are invalid")
    for exclusion in exclusions:
        exclusion.verify_content_identity()
    exclusion_sessions = tuple(value.session for value in exclusions)
    if exclusion_sessions != tuple(sorted(set(exclusion_sessions))):
        _fail("research dataset exclusions must be sorted and unique")

    # A fresh exception raised from inside an except clause still attaches
    # the caught exception as __context__ even with `from None` (which only
    # clears __cause__ and suppresses display). Collecting a flag and
    # raising only after this loop has fully exited leaves no currently
    # handled exception, so the sanitized error carries no nested cause,
    # context, path, hash, or payload content.
    range_load_failed = False
    range_bindings: list[NseArchiveResearchRangeBinding] = []
    for requested_id in index_snapshot_ids:
        verified: VerifiedNseHistoricalArchiveRange | None = None
        try:
            verified = load_verified_nse_historical_archive_range(
                reader, index_snapshot_id=requested_id
            )
        except Exception:
            range_load_failed = True
        if range_load_failed or verified is None:
            range_load_failed = True
            break
        if verified.index_snapshot_id != requested_id:
            range_load_failed = True
            break
        range_bindings.append(
            NseArchiveResearchRangeBinding.from_verified_range(verified)
        )
    if range_load_failed:
        raise NseArchiveResearchDatasetError(
            "research dataset archive range could not be loaded"
        )
    range_bindings_tuple = tuple(range_bindings)

    accepted_sessions: list[date] = []
    session_snapshot_ids: list[str] = []
    previous: NseArchiveResearchRangeBinding | None = None
    for binding in range_bindings_tuple:
        if previous is not None:
            if binding.range_start <= previous.range_end:
                _fail("research dataset range bindings overlap or reorder")
            if binding.range_start != previous.range_end + timedelta(days=1):
                _fail("research dataset range bindings are not calendar-adjacent")
        accepted_sessions.extend(binding.accepted_sessions)
        session_snapshot_ids.extend(binding.session_snapshot_ids)
        previous = binding
    accepted_sessions_tuple = tuple(accepted_sessions)
    session_snapshot_ids_tuple = tuple(session_snapshot_ids)
    if len(set(accepted_sessions_tuple)) != len(accepted_sessions_tuple):
        _fail("research dataset accepted sessions must be unique")
    if len(set(session_snapshot_ids_tuple)) != len(session_snapshot_ids_tuple):
        _fail("research dataset session snapshot ids must be unique")

    accepted_session_set = set(accepted_sessions_tuple)
    envelope_start = range_bindings_tuple[0].range_start
    envelope_end = range_bindings_tuple[-1].range_end
    for exclusion in exclusions:
        if not envelope_start <= exclusion.session <= envelope_end:
            _fail(
                "research dataset exclusion lies outside the combined calendar envelope"
            )
        if exclusion.session in accepted_session_set:
            _fail("research dataset exclusion collides with an accepted session")

    horizon = split_policy.maximum_forward_label_horizon_sessions
    partitions: list[NseArchiveResearchDatasetSplitPartition] = []
    role_predicates = (
        (ResearchSplitRole.TRAIN, lambda value: value <= split_policy.train_end),
        (
            ResearchSplitRole.VALIDATION,
            lambda value: (
                split_policy.validation_start <= value <= split_policy.validation_end
            ),
        ),
        (
            ResearchSplitRole.UNTOUCHED_TEST,
            lambda value: value >= split_policy.test_start,
        ),
    )
    for role, predicate in role_predicates:
        role_sessions = tuple(
            value for value in accepted_sessions_tuple if predicate(value)
        )
        if not role_sessions:
            _fail("research dataset split role has no accepted sessions")
        if len(role_sessions) <= horizon:
            _fail("research dataset split role has no candidate label origin sessions")
        partitions.append(
            NseArchiveResearchDatasetSplitPartition(
                role=role,
                sessions=role_sessions,
                candidate_label_origin_sessions=role_sessions[:-horizon],
                unavailable_label_tail_sessions=role_sessions[-horizon:],
                maximum_forward_label_horizon_sessions=horizon,
            )
        )

    aggregate_profile_counts = {profile: 0 for profile in _KNOWN_EVIDENCE_PROFILES}
    for binding in range_bindings_tuple:
        for profile, count in binding.evidence_profile_counts:
            aggregate_profile_counts[profile] += count

    return NseArchiveResearchDataset(
        index_snapshot_ids=index_snapshot_ids,
        range_bindings=range_bindings_tuple,
        accepted_sessions=accepted_sessions_tuple,
        session_snapshot_ids=session_snapshot_ids_tuple,
        exclusions=exclusions,
        partitions=tuple(partitions),
        record_count=sum(value.record_count for value in range_bindings_tuple),
        identity_issue_count=sum(
            value.identity_issue_count for value in range_bindings_tuple
        ),
        identity_quarantined_session_count=sum(
            value.identity_quarantined_session_count
            for value in range_bindings_tuple
        ),
        incomplete_evidence_session_count=sum(
            value.incomplete_evidence_session_count
            for value in range_bindings_tuple
        ),
        evidence_profile_counts=_sorted_counts(aggregate_profile_counts),
        split_policy=split_policy,
    )
