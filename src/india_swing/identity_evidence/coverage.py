from __future__ import annotations

from dataclasses import dataclass, field
import re

from india_swing.identity import content_id
from india_swing.identity_registry import (
    IdentityAdjudicationQueue,
    IdentityAdjudicationRequirement,
)
from india_swing.reference.models import ReferenceReadiness

from .models import IdentityEvidenceIntegrityError, StoredIdentityEvidenceArtifact


IDENTITY_EVIDENCE_COVERAGE_SCHEMA_VERSION = "identity-evidence-coverage/v1"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def _sha(value: str, name: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ValueError(f"{name} must be a full lowercase SHA-256")


@dataclass(frozen=True, slots=True)
class IdentityEvidenceCoverageEntry:
    candidate_id: str
    requirement: IdentityAdjudicationRequirement
    artifact_ids: tuple[str, ...]
    claim_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        _sha(self.candidate_id, "coverage candidate_id")
        if type(self.requirement) is not IdentityAdjudicationRequirement:
            raise TypeError("coverage requirement must be exact")
        for values, name in (
            (self.artifact_ids, "coverage artifact_ids"),
            (self.claim_ids, "coverage claim_ids"),
        ):
            if type(values) is not tuple or values != tuple(sorted(set(values))):
                raise ValueError(f"{name} must be sorted and unique")
            for value in values:
                _sha(value, name)
        if bool(self.artifact_ids) != bool(self.claim_ids):
            raise ValueError("coverage artifacts and claims must both be present or absent")

    @property
    def evidence_collected(self) -> bool:
        return bool(self.claim_ids)


@dataclass(frozen=True, slots=True)
class IdentityEvidenceCoverageReport:
    queue_id: str
    source_registry_id: str
    evidence_artifact_ids: tuple[str, ...]
    entries: tuple[IdentityEvidenceCoverageEntry, ...]
    readiness: ReferenceReadiness = ReferenceReadiness.COLLECTION_ONLY
    actionable: bool = False
    stable_identity_assigned: bool = False
    requirements_satisfied: bool = False
    schema_version: str = IDENTITY_EVIDENCE_COVERAGE_SCHEMA_VERSION
    report_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha(self.queue_id, "coverage queue_id")
        _sha(self.source_registry_id, "coverage source_registry_id")
        if self.evidence_artifact_ids != tuple(sorted(set(self.evidence_artifact_ids))):
            raise ValueError("coverage artifact IDs must be sorted and unique")
        for value in self.evidence_artifact_ids:
            _sha(value, "coverage evidence_artifact_ids")
        expected = tuple(sorted(self.entries, key=lambda value: (value.candidate_id, value.requirement.value)))
        if type(self.entries) is not tuple or self.entries != expected:
            raise ValueError("coverage entries must be candidate/requirement ordered")
        if (
            self.readiness is not ReferenceReadiness.COLLECTION_ONLY
            or self.actionable is not False
            or self.stable_identity_assigned is not False
            or self.requirements_satisfied is not False
            or self.schema_version != IDENTITY_EVIDENCE_COVERAGE_SCHEMA_VERSION
        ):
            raise ValueError("coverage cannot claim adjudication or actionability")
        object.__setattr__(self, "report_id", content_id({
            "schema_version": self.schema_version, "queue_id": self.queue_id,
            "source_registry_id": self.source_registry_id,
            "evidence_artifact_ids": self.evidence_artifact_ids, "entries": self.entries,
            "readiness": self.readiness, "actionable": self.actionable,
            "stable_identity_assigned": self.stable_identity_assigned,
            "requirements_satisfied": self.requirements_satisfied,
        }, length=64))

    def verify_content_identity(self) -> None:
        expected = content_id({
            "schema_version": self.schema_version, "queue_id": self.queue_id,
            "source_registry_id": self.source_registry_id,
            "evidence_artifact_ids": self.evidence_artifact_ids, "entries": self.entries,
            "readiness": self.readiness, "actionable": self.actionable,
            "stable_identity_assigned": self.stable_identity_assigned,
            "requirements_satisfied": self.requirements_satisfied,
        }, length=64)
        if expected != self.report_id:
            raise IdentityEvidenceIntegrityError("identity evidence coverage identity failed")

    @property
    def required_pair_count(self) -> int:
        return len(self.entries)

    @property
    def evidence_collected_pair_count(self) -> int:
        return sum(value.evidence_collected for value in self.entries)

    @property
    def missing_pair_count(self) -> int:
        return self.required_pair_count - self.evidence_collected_pair_count

    @property
    def requirement_counts(self) -> dict[str, dict[str, int]]:
        result: dict[str, dict[str, int]] = {}
        for entry in self.entries:
            counts = result.setdefault(entry.requirement.value, {"required": 0, "evidence_collected": 0, "missing": 0})
            counts["required"] += 1
            if entry.evidence_collected:
                counts["evidence_collected"] += 1
            else:
                counts["missing"] += 1
        return result


def build_identity_evidence_coverage(
    queue: IdentityAdjudicationQueue,
    artifacts: tuple[StoredIdentityEvidenceArtifact, ...],
) -> IdentityEvidenceCoverageReport:
    if type(queue) is not IdentityAdjudicationQueue:
        raise TypeError("coverage queue must be exact")
    queue.verify_content_identity()
    if type(artifacts) is not tuple or any(type(value) is not StoredIdentityEvidenceArtifact for value in artifacts):
        raise TypeError("coverage artifacts must be an exact tuple")
    artifact_ids = tuple(sorted(value.manifest.artifact_id for value in artifacts))
    if len(set(artifact_ids)) != len(artifact_ids):
        raise IdentityEvidenceIntegrityError("coverage cannot repeat an evidence artifact")
    required = {
        (case.candidate_id, requirement)
        for case in queue.cases
        for requirement in case.requirements
    }
    collected: dict[tuple[str, IdentityAdjudicationRequirement], list[tuple[str, str]]] = {}
    for artifact in artifacts:
        artifact.parsed.verify_content_identity()
        for claim in artifact.parsed.claims:
            key = (claim.candidate_id, claim.requirement)
            if key not in required:
                raise IdentityEvidenceIntegrityError(
                    "evidence claim does not map to an exact requirement in the selected queue"
                )
            collected.setdefault(key, []).append((artifact.manifest.artifact_id, claim.claim_id))
    entries = []
    for candidate_id, requirement in sorted(required, key=lambda value: (value[0], value[1].value)):
        pairs = collected.get((candidate_id, requirement), [])
        entries.append(IdentityEvidenceCoverageEntry(
            candidate_id=candidate_id, requirement=requirement,
            artifact_ids=tuple(sorted({value[0] for value in pairs})),
            claim_ids=tuple(sorted({value[1] for value in pairs})),
        ))
    return IdentityEvidenceCoverageReport(
        queue_id=queue.queue_id, source_registry_id=queue.source_registry_id,
        evidence_artifact_ids=artifact_ids, entries=tuple(entries),
    )
