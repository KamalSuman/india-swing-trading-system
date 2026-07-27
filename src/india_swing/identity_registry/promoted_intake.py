from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timezone

from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference_data.acquisition_promotion import (
    VerifiedReferenceArtifactPromotion,
)
from india_swing.reference_data.models import SourceRowDisposition

from .adjudication import (
    IdentityAdjudicationQueue,
    IdentityAdjudicationRequirement,
    _build_adjudication_cases,
)
from .materialize import _build_identity_graph, _identity_observation_from_record
from .models import (
    IdentityCandidateTransition,
    IdentityConflict,
    IdentityContinuityCandidate,
    IdentityObservation,
)


class PromotedIdentityIntakeError(ValueError):
    pass


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")

PROMOTED_IDENTITY_INTAKE_SCHEMA_VERSION = "promoted-identity-intake/v1"
PROMOTED_IDENTITY_INTAKE_POLICY_VERSION = "promoted-identity-intake/positive-only-v1"

_ERR_TYPE = "promoted identity intake type is invalid"
_ERR_SCHEMA_VERSION = "promoted identity intake schema version is unsupported"
_ERR_PROMOTIONS_SHAPE = "promoted identity intake promotions are invalid"
_ERR_EXPECTED_DATES_SHAPE = (
    "promoted identity intake expected report dates are invalid"
)
_ERR_CUTOFF = "promoted identity intake cutoff is invalid"
_ERR_PROMOTION_VERIFY = (
    "promoted identity intake could not verify a source promotion"
)
_ERR_DUPLICATE_PROMOTION = "promoted identity intake contains a duplicate promotion"
_ERR_DUPLICATE_JOIN = "promoted identity intake contains a duplicate join lineage"
_ERR_DUPLICATE_ARTIFACT = (
    "promoted identity intake contains a duplicate source artifact"
)
_ERR_DUPLICATE_MANIFEST = (
    "promoted identity intake contains a duplicate source manifest"
)
_ERR_DUPLICATE_REPORT_DATE = (
    "promoted identity intake contains a duplicate verified report date"
)
_ERR_EXPECTED_COVERAGE = (
    "promoted identity intake promotions disagree with expected report dates"
)
_ERR_KNOWLEDGE_TIME_AFTER_CUTOFF = (
    "promoted identity intake knowledge time is after cutoff"
)
_ERR_NO_OBSERVATIONS = (
    "promoted identity intake contains no retained equity observations"
)
_ERR_GRAPH = "promoted identity intake could not build its identity graph"
_ERR_DERIVED_FIELD = "promoted identity intake derived field is invalid"
_ERR_COMPARISON_FAILED = (
    "promoted identity intake retained content could not be verified"
)
_ERR_SOURCE_GRAPH_ID = "promoted identity intake source graph identifier is invalid"
_ERR_INTAKE_ID = (
    "promoted identity intake identifier disagrees with independently "
    "recomputed content"
)
_ERR_REQUIREMENT_SATISFACTION = (
    "promoted identity intake requirement satisfaction is invalid"
)

_SATISFIED_REQUIREMENTS = (
    IdentityAdjudicationRequirement.AUTHORIZED_SOURCE_PROVENANCE,
    IdentityAdjudicationRequirement.REPORT_DATE_VERIFICATION,
)
_SATISFIED_REQUIREMENTS_SET = frozenset(_SATISFIED_REQUIREMENTS)


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise PromotedIdentityIntakeError(_ERR_CUTOFF)
    if value.tzinfo is None or value.utcoffset() is None:
        raise PromotedIdentityIntakeError(_ERR_CUTOFF)
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class IdentityRequirementSatisfaction:
    """Which adjudication-queue-case requirements this promoted intake can
    already satisfy from trusted acquisition/promotion provenance alone.

    ``satisfied_requirements`` is always exactly
    ``{AUTHORIZED_SOURCE_PROVENANCE, REPORT_DATE_VERIFICATION}`` -- a
    promoted acquisition proves only those two facts, never stable identity,
    universe membership, or official lifecycle/conflict evidence.
    ``unresolved_requirements`` is the exact remaining requirement set for
    that same queue case. The two sets are always disjoint and their union
    always equals the case's complete requirement set.
    """

    candidate_id: str
    satisfied_requirements: tuple[IdentityAdjudicationRequirement, ...]
    unresolved_requirements: tuple[IdentityAdjudicationRequirement, ...]

    def __post_init__(self) -> None:
        if (
            type(self.candidate_id) is not str
            or _SHA256.fullmatch(self.candidate_id) is None
        ):
            raise PromotedIdentityIntakeError(_ERR_REQUIREMENT_SATISFACTION)
        for values in (self.satisfied_requirements, self.unresolved_requirements):
            if (
                type(values) is not tuple
                or any(
                    type(value) is not IdentityAdjudicationRequirement
                    for value in values
                )
                or values != tuple(sorted(set(values), key=lambda value: value.value))
            ):
                raise PromotedIdentityIntakeError(_ERR_REQUIREMENT_SATISFACTION)
        if set(self.satisfied_requirements) != _SATISFIED_REQUIREMENTS_SET:
            raise PromotedIdentityIntakeError(_ERR_REQUIREMENT_SATISFACTION)
        if set(self.satisfied_requirements) & set(self.unresolved_requirements):
            raise PromotedIdentityIntakeError(_ERR_REQUIREMENT_SATISFACTION)


def _source_graph_identity(
    *,
    cutoff: datetime,
    knowledge_time: datetime,
    expected_report_dates: tuple[date, ...],
    canonical_promotions: tuple[VerifiedReferenceArtifactPromotion, ...],
    observations: tuple[IdentityObservation, ...],
    candidates: tuple[IdentityContinuityCandidate, ...],
    transitions: tuple[IdentityCandidateTransition, ...],
    conflicts: tuple[IdentityConflict, ...],
) -> dict[str, object]:
    """The complete canonical mapping source_graph_id is derived from.

    Binds schema/policy versions, cutoff, authoritative knowledge_time,
    expected_report_dates, ordered promotion/join/artifact/manifest IDs, and
    the complete ordered observation/candidate/transition/conflict graph --
    no filesystem path, mtime, repr, or runtime identity.
    """

    return {
        "schema_version": PROMOTED_IDENTITY_INTAKE_SCHEMA_VERSION,
        "policy_version": PROMOTED_IDENTITY_INTAKE_POLICY_VERSION,
        "cutoff": cutoff,
        "knowledge_time": knowledge_time,
        "expected_report_dates": expected_report_dates,
        "promotion_ids": tuple(value.promotion_id for value in canonical_promotions),
        "join_ids": tuple(value.join.join_id for value in canonical_promotions),
        "artifact_ids": tuple(
            value.artifact.manifest.artifact_id for value in canonical_promotions
        ),
        "manifest_ids": tuple(
            value.artifact.manifest.manifest_id for value in canonical_promotions
        ),
        "observation_ids": tuple(value.observation_id for value in observations),
        "candidate_ids": tuple(value.candidate_id for value in candidates),
        "transition_ids": tuple(value.transition_id for value in transitions),
        "conflict_ids": tuple(value.conflict_id for value in conflicts),
    }


def _intake_identity(
    *,
    source_graph_id: str,
    cutoff: datetime,
    knowledge_time: datetime,
    expected_report_dates: tuple[date, ...],
    queue_id: str,
    requirement_statuses: tuple[IdentityRequirementSatisfaction, ...],
    source_readiness: ReferenceReadiness,
    readiness: ReferenceReadiness,
    actionable: bool,
    stable_identity_assigned: bool,
) -> dict[str, object]:
    return {
        "schema_version": PROMOTED_IDENTITY_INTAKE_SCHEMA_VERSION,
        "policy_version": PROMOTED_IDENTITY_INTAKE_POLICY_VERSION,
        "source_graph_id": source_graph_id,
        "cutoff": cutoff,
        "knowledge_time": knowledge_time,
        "expected_report_dates": expected_report_dates,
        "queue_id": queue_id,
        "requirement_statuses": requirement_statuses,
        "source_readiness": source_readiness,
        "readiness": readiness,
        "actionable": actionable,
        "stable_identity_assigned": stable_identity_assigned,
    }


@dataclass(frozen=True)
class _IntakeFacts:
    """Plain normalized intake facts returned by the single strict decoder.

    Never a VerifiedPromotedIdentityIntake: this is the one decoder both
    PromotedIdentityIntakeService.materialize and
    VerifiedPromotedIdentityIntake's own defensive check call.
    """

    canonical_promotions: tuple[VerifiedReferenceArtifactPromotion, ...]
    cutoff: datetime
    knowledge_time: datetime
    observations: tuple[IdentityObservation, ...]
    candidates: tuple[IdentityContinuityCandidate, ...]
    transitions: tuple[IdentityCandidateTransition, ...]
    conflicts: tuple[IdentityConflict, ...]
    queue: IdentityAdjudicationQueue
    requirement_statuses: tuple[IdentityRequirementSatisfaction, ...]
    source_graph_id: str
    intake_id: str


def _build_intake_facts(
    promotions: tuple[VerifiedReferenceArtifactPromotion, ...],
    expected_report_dates: tuple[date, ...],
    cutoff: datetime,
) -> _IntakeFacts:
    """The single strict intake derivation routine.

    Requires exact non-empty promotion/expected-date tuples, independently
    replays every promotion's own content identity before trusting any of
    its fields, rejects duplicate/ambiguous lineage, requires every
    promotion knowledge_time to be at or before cutoff, builds one
    IdentityObservation per retained equity row (binding claimed_report_date
    and knowledge_time to the promotion's own trusted values, never the
    manifest's claimed/validated fields), reuses the exact shared graph and
    adjudication-case builders the legacy materializer/queue already use,
    and returns the deterministic source_graph_id/intake_id plus every
    derived fact. Never constructs VerifiedPromotedIdentityIntake.

    The retained promotion graph is treated as untrusted at every replay:
    graph/case/queue construction is wrapped in a broad ``Exception`` catch
    (never ``BaseException``) so a malformed nested field fails closed with
    one static sanitized error instead of leaking a raw nested exception.
    """

    if (
        type(promotions) is not tuple
        or not promotions
        or any(
            type(value) is not VerifiedReferenceArtifactPromotion
            for value in promotions
        )
    ):
        raise PromotedIdentityIntakeError(_ERR_PROMOTIONS_SHAPE)
    if (
        type(expected_report_dates) is not tuple
        or len(expected_report_dates) < 2
        or any(type(value) is not date for value in expected_report_dates)
        or expected_report_dates != tuple(sorted(set(expected_report_dates)))
    ):
        raise PromotedIdentityIntakeError(_ERR_EXPECTED_DATES_SHAPE)

    cutoff = _utc(cutoff)

    for promotion in promotions:
        try:
            promotion.verify_content_identity()
        except Exception:
            raise PromotedIdentityIntakeError(_ERR_PROMOTION_VERIFY) from None

    promotion_ids = tuple(value.promotion_id for value in promotions)
    if len(set(promotion_ids)) != len(promotion_ids):
        raise PromotedIdentityIntakeError(_ERR_DUPLICATE_PROMOTION)
    join_ids = tuple(value.join.join_id for value in promotions)
    if len(set(join_ids)) != len(join_ids):
        raise PromotedIdentityIntakeError(_ERR_DUPLICATE_JOIN)
    artifact_ids = tuple(value.artifact.manifest.artifact_id for value in promotions)
    if len(set(artifact_ids)) != len(artifact_ids):
        raise PromotedIdentityIntakeError(_ERR_DUPLICATE_ARTIFACT)
    manifest_ids = tuple(value.artifact.manifest.manifest_id for value in promotions)
    if len(set(manifest_ids)) != len(manifest_ids):
        raise PromotedIdentityIntakeError(_ERR_DUPLICATE_MANIFEST)
    report_dates = tuple(value.verified_report_date for value in promotions)
    if len(set(report_dates)) != len(report_dates):
        raise PromotedIdentityIntakeError(_ERR_DUPLICATE_REPORT_DATE)
    if set(report_dates) != set(expected_report_dates):
        raise PromotedIdentityIntakeError(_ERR_EXPECTED_COVERAGE)
    if any(value.knowledge_time > cutoff for value in promotions):
        raise PromotedIdentityIntakeError(_ERR_KNOWLEDGE_TIME_AFTER_CUTOFF)

    canonical_promotions = tuple(
        sorted(promotions, key=lambda value: value.verified_report_date)
    )
    knowledge_time = max(value.knowledge_time for value in canonical_promotions)

    try:
        observations = tuple(
            sorted(
                (
                    _identity_observation_from_record(
                        source_artifact_id=promotion.artifact.manifest.artifact_id,
                        source_manifest_id=promotion.artifact.manifest.manifest_id,
                        claimed_report_date=promotion.verified_report_date,
                        knowledge_time=promotion.knowledge_time,
                        record=record,
                    )
                    for promotion in canonical_promotions
                    for record in promotion.artifact.parsed.records
                    if record.disposition
                    is SourceRowDisposition.RETAINED_UNVERIFIED_EQUITY
                ),
                key=lambda value: value.observation_id,
            )
        )
    except PromotedIdentityIntakeError:
        raise
    except Exception:
        raise PromotedIdentityIntakeError(_ERR_GRAPH) from None
    if not observations:
        raise PromotedIdentityIntakeError(_ERR_NO_OBSERVATIONS)

    try:
        candidates, transitions, conflicts = _build_identity_graph(observations)
        cases = _build_adjudication_cases(
            observations, candidates, transitions, conflicts
        )
    except PromotedIdentityIntakeError:
        raise
    except Exception:
        raise PromotedIdentityIntakeError(_ERR_GRAPH) from None

    source_graph_id = content_id(
        _source_graph_identity(
            cutoff=cutoff,
            knowledge_time=knowledge_time,
            expected_report_dates=expected_report_dates,
            canonical_promotions=canonical_promotions,
            observations=observations,
            candidates=candidates,
            transitions=transitions,
            conflicts=conflicts,
        ),
        length=64,
    )

    try:
        queue = IdentityAdjudicationQueue(
            source_registry_id=source_graph_id,
            source_cutoff=cutoff,
            source_knowledge_time=knowledge_time,
            source_artifact_ids=tuple(
                value.artifact.manifest.artifact_id for value in canonical_promotions
            ),
            source_manifest_ids=tuple(
                value.artifact.manifest.manifest_id for value in canonical_promotions
            ),
            cases=cases,
        )
    except PromotedIdentityIntakeError:
        raise
    except Exception:
        raise PromotedIdentityIntakeError(_ERR_GRAPH) from None

    requirement_statuses = tuple(
        IdentityRequirementSatisfaction(
            candidate_id=case.candidate_id,
            satisfied_requirements=_SATISFIED_REQUIREMENTS,
            unresolved_requirements=tuple(
                sorted(
                    (
                        value
                        for value in case.requirements
                        if value not in _SATISFIED_REQUIREMENTS_SET
                    ),
                    key=lambda value: value.value,
                )
            ),
        )
        for case in queue.cases
    )

    source_readiness = ReferenceReadiness.POINT_IN_TIME_VERIFIED
    readiness = ReferenceReadiness.COLLECTION_ONLY
    actionable = False
    stable_identity_assigned = False

    intake_id = content_id(
        _intake_identity(
            source_graph_id=source_graph_id,
            cutoff=cutoff,
            knowledge_time=knowledge_time,
            expected_report_dates=expected_report_dates,
            queue_id=queue.queue_id,
            requirement_statuses=requirement_statuses,
            source_readiness=source_readiness,
            readiness=readiness,
            actionable=actionable,
            stable_identity_assigned=stable_identity_assigned,
        ),
        length=64,
    )

    return _IntakeFacts(
        canonical_promotions=canonical_promotions,
        cutoff=cutoff,
        knowledge_time=knowledge_time,
        observations=observations,
        candidates=candidates,
        transitions=transitions,
        conflicts=conflicts,
        queue=queue,
        requirement_statuses=requirement_statuses,
        source_graph_id=source_graph_id,
        intake_id=intake_id,
    )


@dataclass(frozen=True, slots=True)
class VerifiedPromotedIdentityIntake:
    """Immutable, content-addressed research boundary artifact bridging
    multiple independently verified security-master promotions into the
    existing cross-vintage identity candidate graph and adjudication queue.

    ``source_readiness`` is always POINT_IN_TIME_VERIFIED: it describes only
    the retained source promotions. The intake's own ``readiness`` is always
    COLLECTION_ONLY, ``actionable`` is always False, and
    ``stable_identity_assigned`` is always False -- trusted source provenance
    and a verified report date satisfy exactly two adjudication-queue
    requirements per candidate (AUTHORIZED_SOURCE_PROVENANCE,
    REPORT_DATE_VERIFICATION); every other requirement (adjacent-vintage
    observation, validated identifier, official continuity/lifecycle/
    listing-status/conflict-resolution evidence) remains explicitly
    unresolved. No StableInstrumentId, StableListingId, universe, calendar,
    price, liquidity, corporate-action, model, signal, ranking,
    recommendation, notification, broker, order, position-size, or capital
    field exists on this type. __post_init__ calls verify_content_identity(),
    which requires the exact concrete type (rejecting subclasses/impostors
    even when every retained field is otherwise valid) and independently
    replays the complete derivation from the retained
    promotions/expected_report_dates/cutoff, so direct construction with a
    mismatched value or post-construction object.__setattr__ mutation
    anywhere in the retained graph fails closed with one static sanitized
    PromotedIdentityIntakeError.
    """

    schema_version: str
    source_graph_id: str
    cutoff: datetime
    knowledge_time: datetime
    expected_report_dates: tuple[date, ...]
    promotions: tuple[VerifiedReferenceArtifactPromotion, ...]
    observations: tuple[IdentityObservation, ...]
    candidates: tuple[IdentityContinuityCandidate, ...]
    transitions: tuple[IdentityCandidateTransition, ...]
    conflicts: tuple[IdentityConflict, ...]
    queue: IdentityAdjudicationQueue
    requirement_statuses: tuple[IdentityRequirementSatisfaction, ...]
    source_readiness: ReferenceReadiness
    readiness: ReferenceReadiness
    actionable: bool
    stable_identity_assigned: bool
    intake_id: str

    def __post_init__(self) -> None:
        self.verify_content_identity()

    def verify_content_identity(self) -> None:
        if type(self) is not VerifiedPromotedIdentityIntake:
            raise PromotedIdentityIntakeError(_ERR_TYPE)
        if (
            type(self.schema_version) is not str
            or self.schema_version != PROMOTED_IDENTITY_INTAKE_SCHEMA_VERSION
        ):
            raise PromotedIdentityIntakeError(_ERR_SCHEMA_VERSION)
        if (
            type(self.source_graph_id) is not str
            or _SHA256.fullmatch(self.source_graph_id) is None
        ):
            raise PromotedIdentityIntakeError(_ERR_SOURCE_GRAPH_ID)
        if type(self.cutoff) is not datetime:
            raise PromotedIdentityIntakeError(_ERR_CUTOFF)
        if type(self.knowledge_time) is not datetime:
            raise PromotedIdentityIntakeError(_ERR_DERIVED_FIELD)
        if type(self.expected_report_dates) is not tuple or any(
            type(value) is not date for value in self.expected_report_dates
        ):
            raise PromotedIdentityIntakeError(_ERR_EXPECTED_DATES_SHAPE)
        if type(self.promotions) is not tuple or any(
            type(value) is not VerifiedReferenceArtifactPromotion
            for value in self.promotions
        ):
            raise PromotedIdentityIntakeError(_ERR_PROMOTIONS_SHAPE)
        for values, expected_type in (
            (self.observations, IdentityObservation),
            (self.candidates, IdentityContinuityCandidate),
            (self.transitions, IdentityCandidateTransition),
            (self.conflicts, IdentityConflict),
            (self.requirement_statuses, IdentityRequirementSatisfaction),
        ):
            if type(values) is not tuple or any(
                type(value) is not expected_type for value in values
            ):
                raise PromotedIdentityIntakeError(_ERR_DERIVED_FIELD)
        if type(self.queue) is not IdentityAdjudicationQueue:
            raise PromotedIdentityIntakeError(_ERR_DERIVED_FIELD)
        if (
            type(self.source_readiness) is not ReferenceReadiness
            or self.source_readiness is not ReferenceReadiness.POINT_IN_TIME_VERIFIED
        ):
            raise PromotedIdentityIntakeError(_ERR_DERIVED_FIELD)
        if (
            type(self.readiness) is not ReferenceReadiness
            or self.readiness is not ReferenceReadiness.COLLECTION_ONLY
        ):
            raise PromotedIdentityIntakeError(_ERR_DERIVED_FIELD)
        if type(self.actionable) is not bool or self.actionable is not False:
            raise PromotedIdentityIntakeError(_ERR_DERIVED_FIELD)
        if (
            type(self.stable_identity_assigned) is not bool
            or self.stable_identity_assigned is not False
        ):
            raise PromotedIdentityIntakeError(_ERR_DERIVED_FIELD)
        if (
            type(self.intake_id) is not str
            or _SHA256.fullmatch(self.intake_id) is None
        ):
            raise PromotedIdentityIntakeError(_ERR_INTAKE_ID)

        try:
            facts = _build_intake_facts(
                self.promotions, self.expected_report_dates, self.cutoff
            )
        except PromotedIdentityIntakeError:
            raise
        except Exception:
            raise PromotedIdentityIntakeError(_ERR_DERIVED_FIELD) from None

        # Every comparison below may recursively invoke a retained nested
        # value's own __eq__ (dataclass/tuple equality descends into every
        # field), so a single fail-closed boundary wraps all of them: an
        # already-raised PromotedIdentityIntakeError from an intentional
        # mismatch below propagates unchanged, while any other ordinary
        # Exception raised by equality itself (never BaseException/
        # KeyboardInterrupt/SystemExit) is translated to one static
        # sanitized message. Adding a further comparison to this block
        # inherits the same sanitization automatically.
        try:
            if self.promotions != facts.canonical_promotions:
                raise PromotedIdentityIntakeError(_ERR_DERIVED_FIELD)
            if self.cutoff != facts.cutoff:
                raise PromotedIdentityIntakeError(_ERR_DERIVED_FIELD)
            if self.knowledge_time != facts.knowledge_time:
                raise PromotedIdentityIntakeError(_ERR_DERIVED_FIELD)
            if self.observations != facts.observations:
                raise PromotedIdentityIntakeError(_ERR_DERIVED_FIELD)
            if self.candidates != facts.candidates:
                raise PromotedIdentityIntakeError(_ERR_DERIVED_FIELD)
            if self.transitions != facts.transitions:
                raise PromotedIdentityIntakeError(_ERR_DERIVED_FIELD)
            if self.conflicts != facts.conflicts:
                raise PromotedIdentityIntakeError(_ERR_DERIVED_FIELD)
            if self.queue != facts.queue:
                raise PromotedIdentityIntakeError(_ERR_DERIVED_FIELD)
            if self.requirement_statuses != facts.requirement_statuses:
                raise PromotedIdentityIntakeError(_ERR_DERIVED_FIELD)
            if self.source_graph_id != facts.source_graph_id:
                raise PromotedIdentityIntakeError(_ERR_SOURCE_GRAPH_ID)
            if self.intake_id != facts.intake_id:
                raise PromotedIdentityIntakeError(_ERR_INTAKE_ID)
        except PromotedIdentityIntakeError:
            raise
        except Exception:
            raise PromotedIdentityIntakeError(_ERR_COMPARISON_FAILED) from None


class PromotedIdentityIntakeService:
    """Bridges multiple independently verified security-master promotions
    into the existing cross-vintage identity candidate graph and
    adjudication queue.

    Never fabricates, imports, or infers evidence, review decisions, stable
    instrument/listing IDs, listing intervals, delisting dates, universes,
    calendars, surveillance, liquidity, prices, corporate actions, models,
    signals, alerts, notifications, orders, or broker actions. Reuses the
    exact same conflict/candidate/transition graph builder and adjudication
    case builder the legacy collection-only materializer and adjudication
    queue already use, so promoted and legacy inputs never diverge in
    policy.
    """

    def materialize(
        self,
        *,
        promotions: tuple[VerifiedReferenceArtifactPromotion, ...],
        expected_report_dates: tuple[date, ...],
        cutoff: datetime,
    ) -> VerifiedPromotedIdentityIntake:
        facts = _build_intake_facts(promotions, expected_report_dates, cutoff)
        return VerifiedPromotedIdentityIntake(
            schema_version=PROMOTED_IDENTITY_INTAKE_SCHEMA_VERSION,
            source_graph_id=facts.source_graph_id,
            cutoff=facts.cutoff,
            knowledge_time=facts.knowledge_time,
            expected_report_dates=expected_report_dates,
            promotions=facts.canonical_promotions,
            observations=facts.observations,
            candidates=facts.candidates,
            transitions=facts.transitions,
            conflicts=facts.conflicts,
            queue=facts.queue,
            requirement_statuses=facts.requirement_statuses,
            source_readiness=ReferenceReadiness.POINT_IN_TIME_VERIFIED,
            readiness=ReferenceReadiness.COLLECTION_ONLY,
            actionable=False,
            stable_identity_assigned=False,
            intake_id=facts.intake_id,
        )
