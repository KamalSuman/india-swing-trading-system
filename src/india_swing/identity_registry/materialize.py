from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Callable, Hashable

from india_swing.reference.models import ReferenceReadiness
from india_swing.reference_data.artifact_store import verify_stored_reference_provenance
from india_swing.reference_data.models import (
    SourceRowDisposition,
    StoredReferenceArtifact,
)

from .models import (
    CrossVintageIdentityRegistry,
    IdentityCandidateBasis,
    IdentityCandidateStatus,
    IdentityCandidateTransition,
    IdentityConflict,
    IdentityConflictType,
    IdentityContinuityCandidate,
    IdentityObservation,
    IdentityRegistryIntegrityError,
)


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("cutoff must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("cutoff must be timezone-aware")
    return value.astimezone(timezone.utc)


def _observation(source: StoredReferenceArtifact, record: object) -> IdentityObservation:
    # Exact source-record typing is already enforced by ParsedNseCmSecurityMaster.
    return IdentityObservation(
        source_artifact_id=source.manifest.artifact_id,
        source_manifest_id=source.manifest.manifest_id,
        source_record_id=record.source_record_id,
        normalized_row_sha256=record.normalized_row_sha256,
        claimed_report_date=source.manifest.claimed_report_date,
        knowledge_time=source.manifest.validated_at,
        financial_instrument_id=record.financial_instrument_id,
        ticker_symbol=record.ticker_symbol,
        security_series=record.security_series,
        instrument_name=record.instrument_name,
        raw_source_identifier=record.raw_source_identifier,
        validated_isin=record.validated_isin,
        delete_flag=record.delete_flag,
    )


def _conflict(
    conflict_type: IdentityConflictType,
    observations: list[IdentityObservation],
) -> IdentityConflict:
    return IdentityConflict(
        conflict_type=conflict_type,
        observation_ids=tuple(sorted({value.observation_id for value in observations})),
    )


def _build_conflicts(
    observations: tuple[IdentityObservation, ...],
) -> tuple[IdentityConflict, ...]:
    conflicts: dict[str, IdentityConflict] = {}

    by_isin_date_series: dict[
        tuple[str, date, str], list[IdentityObservation]
    ] = defaultdict(list)
    for observation in observations:
        if observation.validated_isin is not None:
            by_isin_date_series[
                (
                    observation.validated_isin,
                    observation.claimed_report_date,
                    observation.security_series,
                )
            ].append(observation)
    for values in by_isin_date_series.values():
        if len(values) > 1:
            conflict = _conflict(
                IdentityConflictType.DUPLICATE_SERIES_WITHIN_ISIN_VINTAGE,
                values,
            )
            conflicts[conflict.conflict_id] = conflict

    by_financial_id: dict[int, list[IdentityObservation]] = defaultdict(list)
    by_listing_key: dict[str, list[IdentityObservation]] = defaultdict(list)
    for observation in observations:
        by_financial_id[observation.financial_instrument_id].append(observation)
        by_listing_key[observation.listing_key].append(observation)

    for conflict_type, groups in (
        (
            IdentityConflictType.FINANCIAL_ID_REUSED_ACROSS_IDENTIFIERS,
            by_financial_id.values(),
        ),
        (
            IdentityConflictType.LISTING_KEY_REUSED_ACROSS_IDENTIFIERS,
            by_listing_key.values(),
        ),
    ):
        for values in groups:
            if len({value.identifier_key for value in values}) > 1:
                conflict = _conflict(conflict_type, values)
                conflicts[conflict.conflict_id] = conflict

    return tuple(conflicts[value] for value in sorted(conflicts))


def _build_candidates(
    observations: tuple[IdentityObservation, ...],
    conflicts: tuple[IdentityConflict, ...],
) -> tuple[IdentityContinuityCandidate, ...]:
    conflicted = {
        observation_id
        for conflict in conflicts
        for observation_id in conflict.observation_ids
    }
    by_isin: dict[str, list[IdentityObservation]] = defaultdict(list)
    unvalidated: list[IdentityObservation] = []
    for observation in observations:
        if observation.validated_isin is None:
            unvalidated.append(observation)
        else:
            by_isin[observation.validated_isin].append(observation)

    candidates: list[IdentityContinuityCandidate] = []
    for isin, values in by_isin.items():
        observation_ids = tuple(sorted(value.observation_id for value in values))
        observed_dates = {value.claimed_report_date for value in values}
        status = (
            IdentityCandidateStatus.CONFLICT
            if any(value in conflicted for value in observation_ids)
            else IdentityCandidateStatus.SINGLE_VINTAGE
            if len(observed_dates) == 1
            else IdentityCandidateStatus.CANDIDATE_CONTINUITY
        )
        candidates.append(
            IdentityContinuityCandidate(
                basis=IdentityCandidateBasis.VALIDATED_ISIN,
                validated_isin=isin,
                raw_source_identifier=None,
                observation_ids=observation_ids,
                status=status,
            )
        )
    for observation in unvalidated:
        candidates.append(
            IdentityContinuityCandidate(
                basis=IdentityCandidateBasis.UNVALIDATED_SOURCE_IDENTIFIER,
                validated_isin=None,
                raw_source_identifier=observation.raw_source_identifier,
                observation_ids=(observation.observation_id,),
                status=(
                    IdentityCandidateStatus.CONFLICT
                    if observation.observation_id in conflicted
                    else IdentityCandidateStatus.UNRESOLVED_IDENTIFIER
                ),
            )
        )
    return tuple(sorted(candidates, key=lambda value: value.candidate_id))


def _pair_unique_by(
    previous: list[IdentityObservation],
    current: list[IdentityObservation],
    key: Callable[[IdentityObservation], Hashable],
) -> tuple[
    list[tuple[IdentityObservation, IdentityObservation]],
    list[IdentityObservation],
    list[IdentityObservation],
]:
    previous_by_key: dict[Hashable, list[IdentityObservation]] = defaultdict(list)
    current_by_key: dict[Hashable, list[IdentityObservation]] = defaultdict(list)
    for value in previous:
        previous_by_key[key(value)].append(value)
    for value in current:
        current_by_key[key(value)].append(value)
    pairs: list[tuple[IdentityObservation, IdentityObservation]] = []
    used_previous: set[str] = set()
    used_current: set[str] = set()
    for value in sorted(set(previous_by_key) & set(current_by_key), key=str):
        left = previous_by_key[value]
        right = current_by_key[value]
        if len(left) == 1 and len(right) == 1:
            pairs.append((left[0], right[0]))
            used_previous.add(left[0].observation_id)
            used_current.add(right[0].observation_id)
    return (
        pairs,
        [value for value in previous if value.observation_id not in used_previous],
        [value for value in current if value.observation_id not in used_current],
    )


def _adjacent_listing_pairs(
    previous: list[IdentityObservation],
    current: list[IdentityObservation],
) -> tuple[tuple[IdentityObservation, IdentityObservation], ...]:
    pairs: list[tuple[IdentityObservation, IdentityObservation]] = []
    remaining_previous = previous
    remaining_current = current
    for key in (
        lambda value: value.listing_key,
        lambda value: value.financial_instrument_id,
        lambda value: value.security_series,
    ):
        matched, remaining_previous, remaining_current = _pair_unique_by(
            remaining_previous,
            remaining_current,
            key,
        )
        pairs.extend(matched)
    if len(remaining_previous) == 1 and len(remaining_current) == 1:
        pairs.append((remaining_previous[0], remaining_current[0]))
    return tuple(pairs)


def _build_transitions(
    observations: tuple[IdentityObservation, ...],
    candidates: tuple[IdentityContinuityCandidate, ...],
) -> tuple[IdentityCandidateTransition, ...]:
    observations_by_id = {value.observation_id: value for value in observations}
    transitions: list[IdentityCandidateTransition] = []
    for candidate in candidates:
        if (
            candidate.basis is not IdentityCandidateBasis.VALIDATED_ISIN
            or candidate.status is not IdentityCandidateStatus.CANDIDATE_CONTINUITY
        ):
            continue
        by_date: dict[date, list[IdentityObservation]] = defaultdict(list)
        for observation_id in candidate.observation_ids:
            observation = observations_by_id[observation_id]
            by_date[observation.claimed_report_date].append(observation)
        ordered_dates = sorted(by_date)
        for previous_date, current_date in zip(ordered_dates, ordered_dates[1:]):
            for previous, current in _adjacent_listing_pairs(
                by_date[previous_date],
                by_date[current_date],
            ):
                transitions.append(
                    IdentityCandidateTransition(
                        candidate_id=candidate.candidate_id,
                        previous_observation_id=previous.observation_id,
                        current_observation_id=current.observation_id,
                        previous_claimed_report_date=previous.claimed_report_date,
                        current_claimed_report_date=current.claimed_report_date,
                        symbol_changed=previous.ticker_symbol != current.ticker_symbol,
                        series_changed=previous.security_series != current.security_series,
                        financial_instrument_id_changed=(
                            previous.financial_instrument_id
                            != current.financial_instrument_id
                        ),
                        instrument_name_changed=(
                            previous.instrument_name != current.instrument_name
                        ),
                    )
                )
    return tuple(sorted(transitions, key=lambda value: value.transition_id))


def materialize_cross_vintage_identity_registry(
    *,
    sources: tuple[StoredReferenceArtifact, ...],
    cutoff: datetime,
) -> CrossVintageIdentityRegistry:
    """Build positive-only identity candidates without assigning stable IDs."""

    cutoff = _utc(cutoff)
    if (
        type(sources) is not tuple
        or not sources
        or any(type(value) is not StoredReferenceArtifact for value in sources)
    ):
        raise TypeError("sources must be a non-empty exact artifact tuple")
    for source in sources:
        verify_stored_reference_provenance(source)
        if source.manifest.readiness is not ReferenceReadiness.COLLECTION_ONLY:
            raise IdentityRegistryIntegrityError(
                "identity source must remain collection-only"
            )
        if source.manifest.validated_at > cutoff:
            raise IdentityRegistryIntegrityError(
                "identity source is known after the requested cutoff"
            )
    if len({value.manifest.artifact_id for value in sources}) != len(sources):
        raise IdentityRegistryIntegrityError("duplicate identity source artifact")
    if len({value.manifest.claimed_report_date for value in sources}) != len(sources):
        raise IdentityRegistryIntegrityError(
            "identity sources contain an ambiguous claimed report date"
        )

    ordered_sources = tuple(
        sorted(
            sources,
            key=lambda value: (
                value.manifest.claimed_report_date,
                value.manifest.artifact_id,
            ),
        )
    )
    observations = tuple(
        sorted(
            (
                _observation(source, record)
                for source in ordered_sources
                for record in source.parsed.records
                if record.disposition
                is SourceRowDisposition.RETAINED_UNVERIFIED_EQUITY
            ),
            key=lambda value: value.observation_id,
        )
    )
    if not observations:
        raise IdentityRegistryIntegrityError(
            "identity sources contain no retained equity observations"
        )
    conflicts = _build_conflicts(observations)
    candidates = _build_candidates(observations, conflicts)
    transitions = _build_transitions(observations, candidates)
    registry = CrossVintageIdentityRegistry(
        cutoff=cutoff,
        knowledge_time=max(value.manifest.validated_at for value in ordered_sources),
        source_manifests=tuple(value.manifest for value in ordered_sources),
        observations=observations,
        candidates=candidates,
        transitions=transitions,
        conflicts=conflicts,
    )
    registry.verify_content_identity()
    return registry
