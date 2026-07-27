from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone

from india_swing.identity import content_id
from india_swing.identity_evidence import (
    StoredIdentityEvidenceArtifact,
)
from india_swing.identity_registry.promoted_intake import (
    VerifiedPromotedIdentityIntake,
)
from india_swing.reference.models import ReferenceReadiness

from .materialize import _materialize_adjudicated_identity_snapshot_core
from .models import AdjudicatedIdentitySnapshot, StoredIdentityReviewBundle


class PromotedIdentityAdjudicationError(ValueError):
    pass


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")

PROMOTED_IDENTITY_ADJUDICATION_SCHEMA_VERSION = "promoted-identity-adjudication/v1"
PROMOTED_IDENTITY_ADJUDICATION_POLICY_VERSION = (
    "promoted-identity-adjudication/pre-satisfied-source-requirements-v1"
)

_ERR_TYPE = "promoted identity adjudication type is invalid"
_ERR_SCHEMA_VERSION = "promoted identity adjudication schema version is unsupported"
_ERR_INTAKE = "promoted identity adjudication intake is invalid"
_ERR_INTAKE_VERIFY = (
    "promoted identity adjudication could not verify the source intake"
)
_ERR_EVIDENCE_SHAPE = "promoted identity adjudication evidence is invalid"
_ERR_REVIEW_SHAPE = "promoted identity adjudication reviews are invalid"
_ERR_CUTOFF = "promoted identity adjudication cutoff is invalid"
_ERR_CUTOFF_BEFORE_KNOWLEDGE = (
    "promoted identity adjudication cutoff precedes intake knowledge time"
)
_ERR_SNAPSHOT = "promoted identity adjudication could not build its snapshot"
_ERR_DERIVED_FIELD = "promoted identity adjudication derived field is invalid"
_ERR_COMPARISON_FAILED = (
    "promoted identity adjudication retained content could not be verified"
)
_ERR_ADJUDICATION_ID = (
    "promoted identity adjudication identifier disagrees with independently "
    "recomputed content"
)


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise PromotedIdentityAdjudicationError(_ERR_CUTOFF)
    if value.tzinfo is None or value.utcoffset() is None:
        raise PromotedIdentityAdjudicationError(_ERR_CUTOFF)
    return value.astimezone(timezone.utc)


def _adjudication_identity(
    *,
    intake_id: str,
    source_graph_id: str,
    queue_id: str,
    cutoff: datetime,
    knowledge_time: datetime,
    evidence_artifact_ids: tuple[str, ...],
    review_bundle_ids: tuple[str, ...],
    snapshot_id: str,
    readiness: ReferenceReadiness,
    actionable: bool,
    stable_identity_assigned: bool,
) -> dict[str, object]:
    """The complete canonical mapping adjudication_id is derived from.

    Binds schema/policy versions, intake_id, source_graph_id, queue_id,
    cutoff, knowledge_time, evidence artifact IDs, review bundle IDs,
    snapshot_id, readiness, actionable, and stable_identity_assigned -- no
    filesystem path, mtime, repr, or runtime identity.
    """

    return {
        "schema_version": PROMOTED_IDENTITY_ADJUDICATION_SCHEMA_VERSION,
        "policy_version": PROMOTED_IDENTITY_ADJUDICATION_POLICY_VERSION,
        "intake_id": intake_id,
        "source_graph_id": source_graph_id,
        "queue_id": queue_id,
        "cutoff": cutoff,
        "knowledge_time": knowledge_time,
        "evidence_artifact_ids": evidence_artifact_ids,
        "review_bundle_ids": review_bundle_ids,
        "snapshot_id": snapshot_id,
        "readiness": readiness,
        "actionable": actionable,
        "stable_identity_assigned": stable_identity_assigned,
    }


@dataclass(frozen=True)
class _AdjudicationFacts:
    """Plain normalized adjudication facts returned by the single strict
    decoder. Never a VerifiedPromotedIdentityAdjudication."""

    evidence_artifacts: tuple[StoredIdentityEvidenceArtifact, ...]
    review_bundles: tuple[StoredIdentityReviewBundle, ...]
    cutoff: datetime
    snapshot: AdjudicatedIdentitySnapshot
    readiness: ReferenceReadiness
    actionable: bool
    stable_identity_assigned: bool
    adjudication_id: str


def _build_adjudication_facts(
    intake: VerifiedPromotedIdentityIntake,
    evidence_artifacts: tuple[StoredIdentityEvidenceArtifact, ...],
    review_bundles: tuple[StoredIdentityReviewBundle, ...],
    cutoff: datetime,
) -> _AdjudicationFacts:
    """The single strict adjudication derivation routine.

    Requires an exact VerifiedPromotedIdentityIntake and exact evidence/
    review tuples, independently replays the intake's own content identity
    before trusting any of its fields, requires cutoff to be at or after
    intake.knowledge_time, and delegates the actual evidence/review
    adjudication to the exact same shared core the legacy
    materialize_adjudicated_identity_snapshot already uses -- passing each
    candidate's upstream pre-satisfied requirement set (AUTHORIZED_SOURCE_
    PROVENANCE and REPORT_DATE_VERIFICATION, exactly the intake's own
    per-case IdentityRequirementSatisfaction.satisfied_requirements) so a
    review decision can never target, double-count, or override those two
    already-trusted facts. Never constructs VerifiedPromotedIdentityAdjudication.

    The retained intake/evidence/review graph is treated as untrusted at
    every replay: every step below deliberately catches ordinary
    ``Exception`` (never ``BaseException``, so ``KeyboardInterrupt``/
    ``SystemExit`` still propagate) so a malformed nested field fails closed
    with one static sanitized error instead of leaking a raw nested exception.
    """

    if type(intake) is not VerifiedPromotedIdentityIntake:
        raise PromotedIdentityAdjudicationError(_ERR_INTAKE)
    if type(evidence_artifacts) is not tuple or any(
        type(value) is not StoredIdentityEvidenceArtifact
        for value in evidence_artifacts
    ):
        raise PromotedIdentityAdjudicationError(_ERR_EVIDENCE_SHAPE)
    if type(review_bundles) is not tuple or any(
        type(value) is not StoredIdentityReviewBundle for value in review_bundles
    ):
        raise PromotedIdentityAdjudicationError(_ERR_REVIEW_SHAPE)

    cutoff = _utc(cutoff)

    try:
        intake.verify_content_identity()
    except Exception:
        raise PromotedIdentityAdjudicationError(_ERR_INTAKE_VERIFY) from None

    if cutoff < intake.knowledge_time:
        raise PromotedIdentityAdjudicationError(_ERR_CUTOFF_BEFORE_KNOWLEDGE)

    try:
        pre_satisfied = {
            status.candidate_id: frozenset(status.satisfied_requirements)
            for status in intake.requirement_statuses
        }
        observations = {value.observation_id: value for value in intake.observations}
        candidates = {value.candidate_id: value for value in intake.candidates}

        snapshot = _materialize_adjudicated_identity_snapshot_core(
            source_registry_id=intake.source_graph_id,
            source_queue_id=intake.queue.queue_id,
            source_knowledge_time=intake.knowledge_time,
            observations=observations,
            candidates=candidates,
            queue=intake.queue,
            evidence_artifacts=evidence_artifacts,
            review_bundles=review_bundles,
            cutoff=cutoff,
            pre_satisfied_requirements=pre_satisfied,
        )
    except PromotedIdentityAdjudicationError:
        raise
    except Exception:
        raise PromotedIdentityAdjudicationError(_ERR_SNAPSHOT) from None

    canonical_evidence = tuple(
        sorted(evidence_artifacts, key=lambda value: value.manifest.artifact_id)
    )
    canonical_review = tuple(
        sorted(review_bundles, key=lambda value: value.manifest.bundle_id)
    )
    evidence_ids = tuple(value.manifest.artifact_id for value in canonical_evidence)
    review_ids = tuple(value.manifest.bundle_id for value in canonical_review)
    readiness = ReferenceReadiness.COLLECTION_ONLY
    actionable = False
    stable_identity_assigned = snapshot.stable_identity_assigned

    adjudication_id = content_id(
        _adjudication_identity(
            intake_id=intake.intake_id,
            source_graph_id=intake.source_graph_id,
            queue_id=intake.queue.queue_id,
            cutoff=cutoff,
            knowledge_time=snapshot.knowledge_time,
            evidence_artifact_ids=evidence_ids,
            review_bundle_ids=review_ids,
            snapshot_id=snapshot.snapshot_id,
            readiness=readiness,
            actionable=actionable,
            stable_identity_assigned=stable_identity_assigned,
        ),
        length=64,
    )

    return _AdjudicationFacts(
        evidence_artifacts=canonical_evidence,
        review_bundles=canonical_review,
        cutoff=cutoff,
        snapshot=snapshot,
        readiness=readiness,
        actionable=actionable,
        stable_identity_assigned=stable_identity_assigned,
        adjudication_id=adjudication_id,
    )


@dataclass(frozen=True, slots=True)
class VerifiedPromotedIdentityAdjudication:
    """Immutable, content-addressed bridge from one
    VerifiedPromotedIdentityIntake into the existing identity-evidence and
    review-decision machinery.

    Trusted acquisition provenance (AUTHORIZED_SOURCE_PROVENANCE,
    REPORT_DATE_VERIFICATION) is discharged automatically per candidate,
    exactly as the retained intake's own requirement_statuses already
    proved; a review decision can never target, double-count, or override
    those two facts. Every remaining official continuity/lifecycle/
    listing-status/conflict requirement still requires an explicit,
    non-conflicting, accepted evidence-review chain -- identical to the
    legacy materialize_adjudicated_identity_snapshot policy, since both
    share the exact same core. ``readiness`` is always COLLECTION_ONLY and
    ``actionable`` is always False; ``stable_identity_assigned`` mirrors
    ``snapshot.stable_identity_assigned`` exactly. A structurally complete
    snapshot is still not point-in-time trading authority: official
    evidence acquisition/publication provenance and effective listing
    intervals require a later promotion boundary. __post_init__ calls
    verify_content_identity(), which requires the exact concrete type
    (rejecting subclasses/impostors even when every retained field is
    otherwise valid) and independently replays the complete derivation, so
    direct construction with a mismatched value or post-construction
    mutation anywhere in the retained graph fails closed with one static
    sanitized PromotedIdentityAdjudicationError.
    """

    schema_version: str
    intake: VerifiedPromotedIdentityIntake
    evidence_artifacts: tuple[StoredIdentityEvidenceArtifact, ...]
    review_bundles: tuple[StoredIdentityReviewBundle, ...]
    cutoff: datetime
    snapshot: AdjudicatedIdentitySnapshot
    readiness: ReferenceReadiness
    actionable: bool
    stable_identity_assigned: bool
    adjudication_id: str

    def __post_init__(self) -> None:
        self.verify_content_identity()

    def verify_content_identity(self) -> None:
        if type(self) is not VerifiedPromotedIdentityAdjudication:
            raise PromotedIdentityAdjudicationError(_ERR_TYPE)
        if (
            type(self.schema_version) is not str
            or self.schema_version != PROMOTED_IDENTITY_ADJUDICATION_SCHEMA_VERSION
        ):
            raise PromotedIdentityAdjudicationError(_ERR_SCHEMA_VERSION)
        if type(self.intake) is not VerifiedPromotedIdentityIntake:
            raise PromotedIdentityAdjudicationError(_ERR_INTAKE)
        if type(self.evidence_artifacts) is not tuple or any(
            type(value) is not StoredIdentityEvidenceArtifact
            for value in self.evidence_artifacts
        ):
            raise PromotedIdentityAdjudicationError(_ERR_EVIDENCE_SHAPE)
        if type(self.review_bundles) is not tuple or any(
            type(value) is not StoredIdentityReviewBundle
            for value in self.review_bundles
        ):
            raise PromotedIdentityAdjudicationError(_ERR_REVIEW_SHAPE)
        if type(self.cutoff) is not datetime:
            raise PromotedIdentityAdjudicationError(_ERR_CUTOFF)
        if type(self.snapshot) is not AdjudicatedIdentitySnapshot:
            raise PromotedIdentityAdjudicationError(_ERR_DERIVED_FIELD)
        if (
            type(self.readiness) is not ReferenceReadiness
            or self.readiness is not ReferenceReadiness.COLLECTION_ONLY
        ):
            raise PromotedIdentityAdjudicationError(_ERR_DERIVED_FIELD)
        if type(self.actionable) is not bool or self.actionable is not False:
            raise PromotedIdentityAdjudicationError(_ERR_DERIVED_FIELD)
        if type(self.stable_identity_assigned) is not bool:
            raise PromotedIdentityAdjudicationError(_ERR_DERIVED_FIELD)
        if (
            type(self.adjudication_id) is not str
            or _SHA256.fullmatch(self.adjudication_id) is None
        ):
            raise PromotedIdentityAdjudicationError(_ERR_ADJUDICATION_ID)

        try:
            facts = _build_adjudication_facts(
                self.intake, self.evidence_artifacts, self.review_bundles, self.cutoff
            )
        except PromotedIdentityAdjudicationError:
            raise
        except Exception:
            raise PromotedIdentityAdjudicationError(_ERR_DERIVED_FIELD) from None

        # Every comparison below may recursively invoke a retained nested
        # value's own __eq__, so a single fail-closed boundary wraps all of
        # them: an already-raised PromotedIdentityAdjudicationError from an
        # intentional mismatch below propagates unchanged, while any other
        # ordinary Exception raised by equality itself (never BaseException/
        # KeyboardInterrupt/SystemExit) is translated to one static
        # sanitized message.
        try:
            if self.evidence_artifacts != facts.evidence_artifacts:
                raise PromotedIdentityAdjudicationError(_ERR_DERIVED_FIELD)
            if self.review_bundles != facts.review_bundles:
                raise PromotedIdentityAdjudicationError(_ERR_DERIVED_FIELD)
            if self.cutoff != facts.cutoff:
                raise PromotedIdentityAdjudicationError(_ERR_DERIVED_FIELD)
            if self.snapshot != facts.snapshot:
                raise PromotedIdentityAdjudicationError(_ERR_DERIVED_FIELD)
            if self.stable_identity_assigned != facts.stable_identity_assigned:
                raise PromotedIdentityAdjudicationError(_ERR_DERIVED_FIELD)
            if self.adjudication_id != facts.adjudication_id:
                raise PromotedIdentityAdjudicationError(_ERR_ADJUDICATION_ID)
        except PromotedIdentityAdjudicationError:
            raise
        except Exception:
            raise PromotedIdentityAdjudicationError(_ERR_COMPARISON_FAILED) from None


class PromotedIdentityAdjudicationService:
    """Bridges one VerifiedPromotedIdentityIntake into the existing
    identity-evidence and review-decision machinery.

    Never fabricates automatic accepted review decisions, synthetic
    evidence claims, or placeholder official documents. Never assigns a
    stable instrument/listing ID from matching an ISIN, symbol, series, or
    financial-instrument ID alone; every non-pre-satisfied requirement
    still requires an explicit, non-conflicting, accepted evidence-review
    chain identical to the legacy materialize_adjudicated_identity_snapshot
    policy.
    """

    def materialize(
        self,
        *,
        intake: VerifiedPromotedIdentityIntake,
        evidence_artifacts: tuple[StoredIdentityEvidenceArtifact, ...],
        review_bundles: tuple[StoredIdentityReviewBundle, ...],
        cutoff: datetime,
    ) -> VerifiedPromotedIdentityAdjudication:
        facts = _build_adjudication_facts(
            intake, evidence_artifacts, review_bundles, cutoff
        )
        return VerifiedPromotedIdentityAdjudication(
            schema_version=PROMOTED_IDENTITY_ADJUDICATION_SCHEMA_VERSION,
            intake=intake,
            evidence_artifacts=facts.evidence_artifacts,
            review_bundles=facts.review_bundles,
            cutoff=facts.cutoff,
            snapshot=facts.snapshot,
            readiness=facts.readiness,
            actionable=facts.actionable,
            stable_identity_assigned=facts.stable_identity_assigned,
            adjudication_id=facts.adjudication_id,
        )
