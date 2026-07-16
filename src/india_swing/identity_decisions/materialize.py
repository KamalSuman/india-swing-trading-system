from __future__ import annotations

from datetime import datetime, timezone

from india_swing.identity import content_id
from india_swing.identity_evidence import (
    StoredIdentityEvidenceArtifact,
    verify_stored_identity_evidence_provenance,
)
from india_swing.identity_registry import (
    CrossVintageIdentityRegistry,
    IdentityAdjudicationQueue,
    IdentityCandidateBasis,
    IdentityCandidateStatus,
    build_identity_adjudication_queue,
)

from .models import (
    STABLE_INSTRUMENT_ID_SCHEME,
    STABLE_LISTING_ID_SCHEME,
    AdjudicatedIdentitySnapshot,
    CandidateIdentityResolution,
    EffectiveStableListingObservation,
    IdentityDecisionConflict,
    IdentityDecisionIntegrityError,
    IdentityResolutionBlocker,
    IdentityReviewOutcome,
    StoredIdentityReviewBundle,
)
from .artifact_store import verify_stored_identity_review_provenance


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("identity decision cutoff must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("identity decision cutoff must be timezone-aware")
    return value.astimezone(timezone.utc)


def _stable_instrument_id(validated_isin: str) -> str:
    return content_id(
        {
            "scheme": STABLE_INSTRUMENT_ID_SCHEME,
            "exchange": "NSE",
            "segment": "CM",
            "validated_isin": validated_isin,
        },
        length=64,
    )


def _stable_listing_id(stable_instrument_id: str, series: str) -> str:
    return content_id(
        {
            "scheme": STABLE_LISTING_ID_SCHEME,
            "stable_instrument_id": stable_instrument_id,
            "exchange": "NSE",
            "segment": "CM",
            "series": series,
        },
        length=64,
    )


def materialize_adjudicated_identity_snapshot(
    *,
    registry: CrossVintageIdentityRegistry,
    queue: IdentityAdjudicationQueue,
    evidence_artifacts: tuple[StoredIdentityEvidenceArtifact, ...],
    review_bundles: tuple[StoredIdentityReviewBundle, ...],
    cutoff: datetime,
) -> AdjudicatedIdentitySnapshot:
    """Assign partial stable IDs only from an explicit, non-conflicting review set."""

    cutoff = _utc(cutoff)
    if type(registry) is not CrossVintageIdentityRegistry:
        raise TypeError("identity decision registry must be exact")
    if type(queue) is not IdentityAdjudicationQueue:
        raise TypeError("identity decision queue must be exact")
    registry.verify_content_identity()
    queue.verify_content_identity()
    if queue != build_identity_adjudication_queue(registry):
        raise IdentityDecisionIntegrityError("identity decision queue does not replay from registry")
    if type(evidence_artifacts) is not tuple or any(
        type(value) is not StoredIdentityEvidenceArtifact for value in evidence_artifacts
    ):
        raise TypeError("identity decision evidence must be an exact tuple")
    if type(review_bundles) is not tuple or any(
        type(value) is not StoredIdentityReviewBundle for value in review_bundles
    ):
        raise TypeError("identity decision reviews must be an exact tuple")

    evidence_ids = tuple(sorted(value.manifest.artifact_id for value in evidence_artifacts))
    review_ids = tuple(sorted(value.manifest.bundle_id for value in review_bundles))
    if len(set(evidence_ids)) != len(evidence_ids) or len(set(review_ids)) != len(review_ids):
        raise IdentityDecisionConflict("identity decision inputs cannot repeat artifacts")
    for artifact in evidence_artifacts:
        verify_stored_identity_evidence_provenance(artifact)
        if artifact.manifest.validated_at > cutoff:
            raise IdentityDecisionIntegrityError("identity evidence is known after cutoff")
    for bundle in review_bundles:
        verify_stored_identity_review_provenance(bundle)
        if bundle.manifest.validated_at > cutoff:
            raise IdentityDecisionIntegrityError("identity review is known after cutoff")
        if bundle.parsed.queue_id != queue.queue_id or bundle.parsed.source_registry_id != registry.registry_id:
            raise IdentityDecisionIntegrityError("identity review targets another queue or registry")

    claims = {
        (artifact.manifest.artifact_id, claim.claim_id): (artifact, claim)
        for artifact in evidence_artifacts
        for claim in artifact.parsed.claims
    }
    required_pairs = {
        (case.candidate_id, requirement)
        for case in queue.cases
        for requirement in case.requirements
    }
    decisions = {}
    for bundle in review_bundles:
        for decision in bundle.parsed.decisions:
            pair = (decision.candidate_id, decision.requirement)
            if pair not in required_pairs:
                raise IdentityDecisionIntegrityError("review decision does not target a required queue pair")
            if pair in decisions:
                raise IdentityDecisionConflict("explicit review set contains duplicate decisions for one pair")
            evidence = claims.get((decision.evidence_artifact_id, decision.evidence_claim_id))
            if evidence is None:
                raise IdentityDecisionIntegrityError("review decision references unselected evidence")
            artifact, claim = evidence
            if claim.candidate_id != decision.candidate_id or claim.requirement is not decision.requirement:
                raise IdentityDecisionIntegrityError("review decision and evidence claim subjects differ")
            if artifact.manifest.validated_at > bundle.parsed.reviewed_at:
                raise IdentityDecisionIntegrityError("review decision predates its evidence")
            decisions[pair] = decision

    candidates = {value.candidate_id: value for value in registry.candidates}
    observations = {value.observation_id: value for value in registry.observations}
    resolutions = []
    listing_observations = []
    for case in queue.cases:
        candidate = candidates[case.candidate_id]
        pair_decisions = {
            requirement: decisions.get((case.candidate_id, requirement))
            for requirement in case.requirements
        }
        missing = tuple(
            requirement for requirement in case.requirements
            if pair_decisions[requirement] is None
        )
        accepted = tuple(sorted(
            decision.decision_id
            for decision in pair_decisions.values()
            if decision is not None and decision.outcome is IdentityReviewOutcome.ACCEPTED
        ))
        rejected = tuple(sorted(
            decision.decision_id
            for decision in pair_decisions.values()
            if decision is not None and decision.outcome is IdentityReviewOutcome.REJECTED
        ))
        blockers = set()
        if missing:
            blockers.add(IdentityResolutionBlocker.MISSING_REVIEW_DECISION)
        if rejected:
            blockers.add(IdentityResolutionBlocker.REJECTED_REVIEW_DECISION)
        supported_shape = (
            candidate.basis is IdentityCandidateBasis.VALIDATED_ISIN
            and candidate.status in {
                IdentityCandidateStatus.SINGLE_VINTAGE,
                IdentityCandidateStatus.CANDIDATE_CONTINUITY,
            }
            and candidate.validated_isin is not None
        )
        if not supported_shape:
            blockers.add(IdentityResolutionBlocker.UNSUPPORTED_CANDIDATE_SHAPE)
        stable_instrument_id = None
        if not blockers:
            assert candidate.validated_isin is not None
            stable_instrument_id = _stable_instrument_id(candidate.validated_isin)
            for observation_id in candidate.observation_ids:
                observation = observations[observation_id]
                listing_observations.append(EffectiveStableListingObservation(
                    candidate_id=candidate.candidate_id,
                    source_observation_id=observation.observation_id,
                    stable_instrument_id=stable_instrument_id,
                    stable_listing_id=_stable_listing_id(
                        stable_instrument_id,
                        observation.security_series,
                    ),
                    effective_on=observation.claimed_report_date,
                    symbol=observation.ticker_symbol,
                    series=observation.security_series,
                    isin=candidate.validated_isin,
                ))
        resolutions.append(CandidateIdentityResolution(
            candidate_id=candidate.candidate_id,
            required_requirements=case.requirements,
            accepted_decision_ids=accepted,
            rejected_decision_ids=rejected,
            missing_requirements=missing,
            blocker_codes=tuple(sorted(blockers, key=lambda value: value.value)),
            stable_instrument_id=stable_instrument_id,
        ))

    knowledge_times = [registry.knowledge_time]
    knowledge_times.extend(value.manifest.validated_at for value in evidence_artifacts)
    knowledge_times.extend(value.manifest.validated_at for value in review_bundles)
    snapshot = AdjudicatedIdentitySnapshot(
        source_registry_id=registry.registry_id,
        source_queue_id=queue.queue_id,
        cutoff=cutoff,
        knowledge_time=max(knowledge_times),
        evidence_artifact_ids=evidence_ids,
        review_bundle_ids=review_ids,
        resolutions=tuple(sorted(resolutions, key=lambda value: value.candidate_id)),
        listing_observations=tuple(sorted(
            listing_observations,
            key=lambda value: (value.effective_on, value.stable_listing_id, value.source_observation_id),
        )),
    )
    snapshot.verify_content_identity()
    return snapshot
