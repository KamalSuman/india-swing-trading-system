from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum

from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness

from .models import (
    CrossVintageIdentityRegistry,
    IdentityCandidateBasis,
    IdentityCandidateStatus,
    IdentityRegistryIntegrityError,
)


IDENTITY_ADJUDICATION_SCHEMA_VERSION = "identity-adjudication-queue/v1"
IDENTITY_ADJUDICATION_POLICY_VERSION = "official-evidence-required/v1"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class IdentityAdjudicationError(ValueError):
    pass


class IdentityAdjudicationRequirement(str, Enum):
    AUTHORIZED_SOURCE_PROVENANCE = "AUTHORIZED_SOURCE_PROVENANCE"
    REPORT_DATE_VERIFICATION = "REPORT_DATE_VERIFICATION"
    ADJACENT_VINTAGE_OBSERVATION = "ADJACENT_VINTAGE_OBSERVATION"
    VALIDATED_IDENTIFIER = "VALIDATED_IDENTIFIER"
    OFFICIAL_CONTINUITY_CONFIRMATION = "OFFICIAL_CONTINUITY_CONFIRMATION"
    OFFICIAL_LISTING_LIFECYCLE = "OFFICIAL_LISTING_LIFECYCLE"
    OFFICIAL_LISTING_STATUS = "OFFICIAL_LISTING_STATUS"
    OFFICIAL_CONFLICT_RESOLUTION = "OFFICIAL_CONFLICT_RESOLUTION"


class IdentityAdjudicationState(str, Enum):
    EVIDENCE_REQUIRED = "EVIDENCE_REQUIRED"


def _sha(value: str, name: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise IdentityAdjudicationError(f"{name} must be a full lowercase SHA-256")


def _utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise IdentityAdjudicationError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _sha_tuple(
    values: tuple[str, ...],
    name: str,
    *,
    required: bool = False,
) -> None:
    if (
        type(values) is not tuple
        or (required and not values)
        or values != tuple(sorted(set(values)))
    ):
        raise IdentityAdjudicationError(f"{name} must be sorted and unique")
    for value in values:
        _sha(value, name)


@dataclass(frozen=True, slots=True)
class IdentityAdjudicationCase:
    candidate_id: str
    basis: IdentityCandidateBasis
    candidate_status: IdentityCandidateStatus
    observation_claims: tuple[tuple[str, date], ...]
    transition_ids: tuple[str, ...]
    conflict_ids: tuple[str, ...]
    requirements: tuple[IdentityAdjudicationRequirement, ...]
    state: IdentityAdjudicationState = IdentityAdjudicationState.EVIDENCE_REQUIRED
    case_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha(self.candidate_id, "adjudication candidate_id")
        if type(self.basis) is not IdentityCandidateBasis:
            raise TypeError("adjudication basis must be exact")
        if type(self.candidate_status) is not IdentityCandidateStatus:
            raise TypeError("adjudication candidate_status must be exact")
        if (
            type(self.observation_claims) is not tuple
            or not self.observation_claims
            or self.observation_claims
            != tuple(sorted(self.observation_claims, key=lambda value: value[0]))
        ):
            raise IdentityAdjudicationError(
                "observation claims must be a non-empty observation-ID-ordered tuple"
            )
        observation_ids = tuple(value[0] for value in self.observation_claims)
        if len(set(observation_ids)) != len(observation_ids):
            raise IdentityAdjudicationError(
                "observation claims cannot repeat an observation"
            )
        for observation_id, claimed_date in self.observation_claims:
            _sha(observation_id, "adjudication observation_id")
            if type(claimed_date) is not date:
                raise TypeError("adjudication claimed date must be a date")
        _sha_tuple(self.transition_ids, "adjudication transition_ids")
        _sha_tuple(self.conflict_ids, "adjudication conflict_ids")
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
            raise IdentityAdjudicationError(
                "adjudication requirements must be sorted, unique, and non-empty"
            )
        mandatory = {
            IdentityAdjudicationRequirement.AUTHORIZED_SOURCE_PROVENANCE,
            IdentityAdjudicationRequirement.REPORT_DATE_VERIFICATION,
        }
        if not mandatory.issubset(self.requirements):
            raise IdentityAdjudicationError(
                "every adjudication case requires source and report-date verification"
            )
        if type(self.state) is not IdentityAdjudicationState:
            raise TypeError("adjudication state must be exact")
        if self.state is not IdentityAdjudicationState.EVIDENCE_REQUIRED:
            raise IdentityAdjudicationError(
                "candidate queues cannot claim an adjudication outcome"
            )
        object.__setattr__(self, "case_id", self._calculated_id())

    @property
    def observation_ids(self) -> tuple[str, ...]:
        return tuple(value[0] for value in self.observation_claims)

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema_version": IDENTITY_ADJUDICATION_SCHEMA_VERSION,
                "policy_version": IDENTITY_ADJUDICATION_POLICY_VERSION,
                "candidate_id": self.candidate_id,
                "basis": self.basis,
                "candidate_status": self.candidate_status,
                "observation_claims": self.observation_claims,
                "transition_ids": self.transition_ids,
                "conflict_ids": self.conflict_ids,
                "requirements": self.requirements,
                "state": self.state,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.case_id != self._calculated_id():
            raise IdentityAdjudicationError(
                "identity-adjudication case content identity failed"
            )


@dataclass(frozen=True, slots=True)
class IdentityAdjudicationQueue:
    source_registry_id: str
    source_cutoff: datetime
    source_knowledge_time: datetime
    source_artifact_ids: tuple[str, ...]
    source_manifest_ids: tuple[str, ...]
    cases: tuple[IdentityAdjudicationCase, ...]
    readiness: ReferenceReadiness = ReferenceReadiness.COLLECTION_ONLY
    actionable: bool = False
    stable_identity_assigned: bool = False
    schema_version: str = IDENTITY_ADJUDICATION_SCHEMA_VERSION
    policy_version: str = IDENTITY_ADJUDICATION_POLICY_VERSION
    queue_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha(self.source_registry_id, "adjudication source_registry_id")
        object.__setattr__(
            self,
            "source_cutoff",
            _utc(self.source_cutoff, "adjudication source_cutoff"),
        )
        object.__setattr__(
            self,
            "source_knowledge_time",
            _utc(self.source_knowledge_time, "adjudication source_knowledge_time"),
        )
        if self.source_knowledge_time > self.source_cutoff:
            raise IdentityAdjudicationError(
                "adjudication source knowledge cannot follow its cutoff"
            )
        for values, name in (
            (self.source_artifact_ids, "source_artifact_ids"),
            (self.source_manifest_ids, "source_manifest_ids"),
        ):
            if type(values) is not tuple or not values or len(set(values)) != len(values):
                raise IdentityAdjudicationError(
                    f"adjudication {name} must be non-empty and unique"
                )
            for value in values:
                _sha(value, f"adjudication {name}")
        if len(self.source_artifact_ids) != len(self.source_manifest_ids):
            raise IdentityAdjudicationError(
                "source artifact and manifest lineage must have equal length"
            )
        if (
            type(self.cases) is not tuple
            or not self.cases
            or any(type(value) is not IdentityAdjudicationCase for value in self.cases)
            or self.cases
            != tuple(sorted(self.cases, key=lambda value: value.candidate_id))
        ):
            raise IdentityAdjudicationError(
                "adjudication cases must be a non-empty candidate-ordered exact tuple"
            )
        if len({value.candidate_id for value in self.cases}) != len(self.cases):
            raise IdentityAdjudicationError(
                "adjudication queue contains duplicate candidates"
            )
        for value in self.cases:
            value.verify_content_identity()
        if (
            self.readiness is not ReferenceReadiness.COLLECTION_ONLY
            or self.actionable is not False
            or self.stable_identity_assigned is not False
        ):
            raise IdentityAdjudicationError(
                "adjudication queue must remain collection-only and non-actionable"
            )
        if (
            self.schema_version != IDENTITY_ADJUDICATION_SCHEMA_VERSION
            or self.policy_version != IDENTITY_ADJUDICATION_POLICY_VERSION
        ):
            raise IdentityAdjudicationError(
                "unsupported identity-adjudication queue contract"
            )
        object.__setattr__(self, "queue_id", self._calculated_id())

    @property
    def requirement_counts(self) -> tuple[tuple[str, int], ...]:
        counts = {
            requirement.value: sum(
                requirement in case.requirements for case in self.cases
            )
            for requirement in IdentityAdjudicationRequirement
        }
        return tuple((name, count) for name, count in sorted(counts.items()) if count)

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema_version": self.schema_version,
                "policy_version": self.policy_version,
                "source_registry_id": self.source_registry_id,
                "source_cutoff": self.source_cutoff,
                "source_knowledge_time": self.source_knowledge_time,
                "source_artifact_ids": self.source_artifact_ids,
                "source_manifest_ids": self.source_manifest_ids,
                "cases": self.cases,
                "readiness": self.readiness,
                "actionable": self.actionable,
                "stable_identity_assigned": self.stable_identity_assigned,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        for value in self.cases:
            if type(value) is not IdentityAdjudicationCase:
                raise IdentityAdjudicationError(
                    "identity-adjudication queue contains an invalid case type"
                )
            value.verify_content_identity()
        if self.queue_id != self._calculated_id():
            raise IdentityAdjudicationError(
                "identity-adjudication queue content identity failed"
            )


def _requirements_for(
    *,
    candidate_status: IdentityCandidateStatus,
    observed_date_count: int,
    transition_changed: bool,
    has_delete_flag: bool,
) -> tuple[IdentityAdjudicationRequirement, ...]:
    values = {
        IdentityAdjudicationRequirement.AUTHORIZED_SOURCE_PROVENANCE,
        IdentityAdjudicationRequirement.REPORT_DATE_VERIFICATION,
    }
    if type(observed_date_count) is not int or observed_date_count <= 0:
        raise IdentityRegistryIntegrityError(
            "identity candidate must contain a positive observed-date count"
        )
    if observed_date_count < 2:
        values.add(
            IdentityAdjudicationRequirement.ADJACENT_VINTAGE_OBSERVATION
        )
    if candidate_status is IdentityCandidateStatus.SINGLE_VINTAGE:
        values.add(
            IdentityAdjudicationRequirement.OFFICIAL_LISTING_STATUS
        )
    elif candidate_status is IdentityCandidateStatus.UNRESOLVED_IDENTIFIER:
        values.add(
            IdentityAdjudicationRequirement.VALIDATED_IDENTIFIER
        )
    elif candidate_status is IdentityCandidateStatus.CANDIDATE_CONTINUITY:
        values.add(
            IdentityAdjudicationRequirement.OFFICIAL_CONTINUITY_CONFIRMATION
        )
    elif candidate_status is IdentityCandidateStatus.CONFLICT:
        values.add(IdentityAdjudicationRequirement.OFFICIAL_CONFLICT_RESOLUTION)
    else:
        raise IdentityRegistryIntegrityError("unsupported identity candidate status")
    if transition_changed:
        values.add(IdentityAdjudicationRequirement.OFFICIAL_LISTING_LIFECYCLE)
    if has_delete_flag:
        values.add(IdentityAdjudicationRequirement.OFFICIAL_LISTING_STATUS)
    return tuple(sorted(values, key=lambda value: value.value))


def build_identity_adjudication_queue(
    registry: CrossVintageIdentityRegistry,
) -> IdentityAdjudicationQueue:
    """Enumerate every unresolved identity case without assigning stable IDs."""

    if type(registry) is not CrossVintageIdentityRegistry:
        raise TypeError("registry must be an exact CrossVintageIdentityRegistry")
    registry.verify_content_identity()
    observations = {value.observation_id: value for value in registry.observations}
    cases: list[IdentityAdjudicationCase] = []
    for candidate in registry.candidates:
        candidate_observations = tuple(
            observations[value] for value in candidate.observation_ids
        )
        transitions = tuple(
            value
            for value in registry.transitions
            if value.candidate_id == candidate.candidate_id
        )
        candidate_observation_ids = set(candidate.observation_ids)
        conflicts = tuple(
            value
            for value in registry.conflicts
            if candidate_observation_ids.intersection(value.observation_ids)
        )
        transition_changed = any(
            value.symbol_changed
            or value.series_changed
            or value.financial_instrument_id_changed
            or value.instrument_name_changed
            for value in transitions
        )
        cases.append(
            IdentityAdjudicationCase(
                candidate_id=candidate.candidate_id,
                basis=candidate.basis,
                candidate_status=candidate.status,
                observation_claims=tuple(
                    sorted(
                        (
                            (value.observation_id, value.claimed_report_date)
                            for value in candidate_observations
                        ),
                        key=lambda value: value[0],
                    )
                ),
                transition_ids=tuple(
                    sorted(value.transition_id for value in transitions)
                ),
                conflict_ids=tuple(sorted(value.conflict_id for value in conflicts)),
                requirements=_requirements_for(
                    candidate_status=candidate.status,
                    observed_date_count=len(
                        {value.claimed_report_date for value in candidate_observations}
                    ),
                    transition_changed=transition_changed,
                    has_delete_flag=any(
                        value.delete_flag == "Y" for value in candidate_observations
                    ),
                ),
            )
        )
    queue = IdentityAdjudicationQueue(
        source_registry_id=registry.registry_id,
        source_cutoff=registry.cutoff,
        source_knowledge_time=registry.knowledge_time,
        source_artifact_ids=registry.source_artifact_ids,
        source_manifest_ids=tuple(
            value.manifest_id for value in registry.source_manifests
        ),
        cases=tuple(sorted(cases, key=lambda value: value.candidate_id)),
    )
    if {value.candidate_id for value in queue.cases} != {
        value.candidate_id for value in registry.candidates
    }:
        raise IdentityAdjudicationError(
            "adjudication queue does not cover every registry candidate"
        )
    return queue
