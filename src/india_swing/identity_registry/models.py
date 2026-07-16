from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum

from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference_data.models import ReferenceArtifactManifest


IDENTITY_REGISTRY_SCHEMA_VERSION = "nse-cm-identity-registry/v1"
IDENTITY_REGISTRY_POLICY_VERSION = "nse-cm-identity-candidates/positive-only-v1"
IDENTITY_REGISTRY_CODEC_VERSION = "nse-cm-identity-registry-json/v1"
IDENTITY_REGISTRY_DATASET = "nse-cm-cross-vintage-identity-candidates"
POSITIVE_OBSERVATIONS_ONLY = "POSITIVE_OBSERVATIONS_ONLY"
UNVERIFIED_REPORT_DATE_CLAIMS = "UNVERIFIED_REPORT_DATE_CLAIMS"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_ISIN = re.compile(r"[A-Z]{2}[A-Z0-9]{9}[0-9]\Z")


class IdentityRegistryError(RuntimeError):
    pass


class IdentityRegistryIntegrityError(IdentityRegistryError):
    pass


class IdentityCandidateBasis(str, Enum):
    VALIDATED_ISIN = "VALIDATED_ISIN"
    UNVALIDATED_SOURCE_IDENTIFIER = "UNVALIDATED_SOURCE_IDENTIFIER"


class IdentityCandidateStatus(str, Enum):
    SINGLE_VINTAGE = "SINGLE_VINTAGE"
    CANDIDATE_CONTINUITY = "CANDIDATE_CONTINUITY"
    UNRESOLVED_IDENTIFIER = "UNRESOLVED_IDENTIFIER"
    CONFLICT = "CONFLICT"


class IdentityConflictType(str, Enum):
    DUPLICATE_SERIES_WITHIN_ISIN_VINTAGE = (
        "DUPLICATE_SERIES_WITHIN_ISIN_VINTAGE"
    )
    FINANCIAL_ID_REUSED_ACROSS_IDENTIFIERS = (
        "FINANCIAL_ID_REUSED_ACROSS_IDENTIFIERS"
    )
    LISTING_KEY_REUSED_ACROSS_IDENTIFIERS = (
        "LISTING_KEY_REUSED_ACROSS_IDENTIFIERS"
    )


def _require_sha256(value: str, field_name: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a full lowercase SHA-256")


def _required_text(value: str, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or any(ord(character) < 32 for character in value)
    ):
        raise ValueError(f"{field_name} must be canonical non-empty text")


def _required_source_text(value: str, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or any(ord(character) < 32 for character in value)
    ):
        raise ValueError(f"{field_name} must be non-empty source text")


def _utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class IdentityObservation:
    source_artifact_id: str
    source_manifest_id: str
    source_record_id: str
    normalized_row_sha256: str
    claimed_report_date: date
    knowledge_time: datetime
    financial_instrument_id: int
    ticker_symbol: str
    security_series: str
    instrument_name: str
    raw_source_identifier: str
    validated_isin: str | None
    delete_flag: str
    observation_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.source_artifact_id, "source_artifact_id"),
            (self.source_manifest_id, "source_manifest_id"),
            (self.source_record_id, "source_record_id"),
            (self.normalized_row_sha256, "normalized_row_sha256"),
        ):
            _require_sha256(value, name)
        if type(self.claimed_report_date) is not date:
            raise TypeError("claimed_report_date must be a date")
        object.__setattr__(
            self,
            "knowledge_time",
            _utc(self.knowledge_time, "knowledge_time"),
        )
        if type(self.financial_instrument_id) is not int or self.financial_instrument_id <= 0:
            raise ValueError("financial_instrument_id must be positive")
        for name in (
            "ticker_symbol",
            "security_series",
            "raw_source_identifier",
        ):
            _required_text(getattr(self, name), name)
        # The official file contains fixed-width/padded names. Preserve that
        # observed source value exactly; it is lineage, not a join key.
        _required_source_text(self.instrument_name, "instrument_name")
        if self.ticker_symbol != self.ticker_symbol.upper().strip():
            raise ValueError("ticker_symbol must be normalized uppercase text")
        if self.security_series != self.security_series.upper().strip():
            raise ValueError("security_series must be normalized uppercase text")
        if self.validated_isin is not None and (
            _ISIN.fullmatch(self.validated_isin) is None
            or self.validated_isin != self.raw_source_identifier
        ):
            raise ValueError("validated_isin must be the exact validated source identifier")
        if self.delete_flag not in {"N", "Y"}:
            raise ValueError("delete_flag must be N or Y")
        object.__setattr__(self, "observation_id", self._calculated_id())

    @property
    def listing_key(self) -> str:
        return f"NSE:CM:{self.ticker_symbol}:{self.security_series}"

    @property
    def identifier_key(self) -> str:
        return self.validated_isin or self.raw_source_identifier

    def _calculated_id(self) -> str:
        return content_id(
            {
                "policy_version": IDENTITY_REGISTRY_POLICY_VERSION,
                "source_artifact_id": self.source_artifact_id,
                "source_manifest_id": self.source_manifest_id,
                "source_record_id": self.source_record_id,
                "normalized_row_sha256": self.normalized_row_sha256,
                "claimed_report_date": self.claimed_report_date,
                "knowledge_time": self.knowledge_time,
                "financial_instrument_id": self.financial_instrument_id,
                "ticker_symbol": self.ticker_symbol,
                "security_series": self.security_series,
                "instrument_name": self.instrument_name,
                "raw_source_identifier": self.raw_source_identifier,
                "validated_isin": self.validated_isin,
                "delete_flag": self.delete_flag,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.observation_id != self._calculated_id():
            raise IdentityRegistryIntegrityError(
                "identity observation content identity failed"
            )


@dataclass(frozen=True, slots=True)
class IdentityConflict:
    conflict_type: IdentityConflictType
    observation_ids: tuple[str, ...]
    conflict_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.conflict_type) is not IdentityConflictType:
            raise TypeError("conflict_type must be an exact IdentityConflictType")
        if (
            type(self.observation_ids) is not tuple
            or len(self.observation_ids) < 2
            or tuple(sorted(set(self.observation_ids))) != self.observation_ids
        ):
            raise ValueError("conflict observation IDs must be sorted, unique, and plural")
        for value in self.observation_ids:
            _require_sha256(value, "conflict observation ID")
        object.__setattr__(self, "conflict_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "policy_version": IDENTITY_REGISTRY_POLICY_VERSION,
                "conflict_type": self.conflict_type,
                "observation_ids": self.observation_ids,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.conflict_id != self._calculated_id():
            raise IdentityRegistryIntegrityError("identity conflict content identity failed")


@dataclass(frozen=True, slots=True)
class IdentityContinuityCandidate:
    basis: IdentityCandidateBasis
    validated_isin: str | None
    raw_source_identifier: str | None
    observation_ids: tuple[str, ...]
    status: IdentityCandidateStatus
    candidate_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.basis) is not IdentityCandidateBasis:
            raise TypeError("basis must be an exact IdentityCandidateBasis")
        if type(self.status) is not IdentityCandidateStatus:
            raise TypeError("status must be an exact IdentityCandidateStatus")
        if (
            type(self.observation_ids) is not tuple
            or not self.observation_ids
            or tuple(sorted(set(self.observation_ids))) != self.observation_ids
        ):
            raise ValueError("candidate observation IDs must be sorted and unique")
        for value in self.observation_ids:
            _require_sha256(value, "candidate observation ID")
        if self.basis is IdentityCandidateBasis.VALIDATED_ISIN:
            if (
                not isinstance(self.validated_isin, str)
                or _ISIN.fullmatch(self.validated_isin) is None
                or self.raw_source_identifier is not None
                or self.status is IdentityCandidateStatus.UNRESOLVED_IDENTIFIER
            ):
                raise ValueError("validated-ISIN candidate shape is invalid")
        else:
            if (
                self.validated_isin is not None
                or not isinstance(self.raw_source_identifier, str)
                or not self.raw_source_identifier
                or len(self.observation_ids) != 1
                or self.status not in {
                    IdentityCandidateStatus.UNRESOLVED_IDENTIFIER,
                    IdentityCandidateStatus.CONFLICT,
                }
            ):
                raise ValueError("unvalidated-identifier candidate shape is invalid")
        object.__setattr__(self, "candidate_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "policy_version": IDENTITY_REGISTRY_POLICY_VERSION,
                "basis": self.basis,
                "validated_isin": self.validated_isin,
                "raw_source_identifier": self.raw_source_identifier,
                "observation_ids": self.observation_ids,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.candidate_id != self._calculated_id():
            raise IdentityRegistryIntegrityError(
                "identity candidate content identity failed"
            )


@dataclass(frozen=True, slots=True)
class IdentityCandidateTransition:
    candidate_id: str
    previous_observation_id: str
    current_observation_id: str
    previous_claimed_report_date: date
    current_claimed_report_date: date
    symbol_changed: bool
    series_changed: bool
    financial_instrument_id_changed: bool
    instrument_name_changed: bool
    transition_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.candidate_id, "candidate_id"),
            (self.previous_observation_id, "previous_observation_id"),
            (self.current_observation_id, "current_observation_id"),
        ):
            _require_sha256(value, name)
        if (
            type(self.previous_claimed_report_date) is not date
            or type(self.current_claimed_report_date) is not date
            or self.current_claimed_report_date <= self.previous_claimed_report_date
        ):
            raise ValueError("candidate transition dates must be strictly increasing")
        for name in (
            "symbol_changed",
            "series_changed",
            "financial_instrument_id_changed",
            "instrument_name_changed",
        ):
            if type(getattr(self, name)) is not bool:
                raise TypeError(f"{name} must be bool")
        object.__setattr__(self, "transition_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "policy_version": IDENTITY_REGISTRY_POLICY_VERSION,
                "candidate_id": self.candidate_id,
                "previous_observation_id": self.previous_observation_id,
                "current_observation_id": self.current_observation_id,
                "previous_claimed_report_date": self.previous_claimed_report_date,
                "current_claimed_report_date": self.current_claimed_report_date,
                "symbol_changed": self.symbol_changed,
                "series_changed": self.series_changed,
                "financial_instrument_id_changed": self.financial_instrument_id_changed,
                "instrument_name_changed": self.instrument_name_changed,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.transition_id != self._calculated_id():
            raise IdentityRegistryIntegrityError(
                "identity transition content identity failed"
            )


@dataclass(frozen=True, slots=True)
class CrossVintageIdentityRegistry:
    cutoff: datetime
    knowledge_time: datetime
    source_manifests: tuple[ReferenceArtifactManifest, ...]
    observations: tuple[IdentityObservation, ...]
    candidates: tuple[IdentityContinuityCandidate, ...]
    transitions: tuple[IdentityCandidateTransition, ...]
    conflicts: tuple[IdentityConflict, ...]
    readiness: ReferenceReadiness = ReferenceReadiness.COLLECTION_ONLY
    actionable: bool = False
    coverage_scope: str = POSITIVE_OBSERVATIONS_ONLY
    report_date_status: str = UNVERIFIED_REPORT_DATE_CLAIMS
    schema_version: str = IDENTITY_REGISTRY_SCHEMA_VERSION
    policy_version: str = IDENTITY_REGISTRY_POLICY_VERSION
    codec_version: str = IDENTITY_REGISTRY_CODEC_VERSION
    registry_id: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "cutoff", _utc(self.cutoff, "cutoff"))
        object.__setattr__(
            self,
            "knowledge_time",
            _utc(self.knowledge_time, "knowledge_time"),
        )
        if self.knowledge_time > self.cutoff:
            raise ValueError("registry knowledge_time cannot follow cutoff")
        if (
            self.schema_version != IDENTITY_REGISTRY_SCHEMA_VERSION
            or self.policy_version != IDENTITY_REGISTRY_POLICY_VERSION
            or self.codec_version != IDENTITY_REGISTRY_CODEC_VERSION
            or self.coverage_scope != POSITIVE_OBSERVATIONS_ONLY
            or self.report_date_status != UNVERIFIED_REPORT_DATE_CLAIMS
        ):
            raise ValueError("unsupported identity-registry contract")
        if self.readiness is not ReferenceReadiness.COLLECTION_ONLY or self.actionable is not False:
            raise ValueError("identity registry must remain collection-only")
        if (
            type(self.source_manifests) is not tuple
            or not self.source_manifests
            or any(type(value) is not ReferenceArtifactManifest for value in self.source_manifests)
        ):
            raise TypeError("source_manifests must be non-empty exact manifests")
        source_order = tuple(
            sorted(
                self.source_manifests,
                key=lambda value: (value.claimed_report_date, value.artifact_id),
            )
        )
        if self.source_manifests != source_order:
            raise ValueError("source manifests must use claimed-date order")
        if len({value.artifact_id for value in self.source_manifests}) != len(
            self.source_manifests
        ):
            raise ValueError("source artifact IDs must be unique")
        if len({value.claimed_report_date for value in self.source_manifests}) != len(
            self.source_manifests
        ):
            raise ValueError("one source artifact is required per claimed report date")
        if max(value.validated_at for value in self.source_manifests) != self.knowledge_time:
            raise ValueError("registry knowledge_time must equal latest source validation")
        if any(value.validated_at > self.cutoff for value in self.source_manifests):
            raise ValueError("registry source is known after cutoff")

        typed_groups = (
            (self.observations, IdentityObservation, "observation_id"),
            (self.candidates, IdentityContinuityCandidate, "candidate_id"),
            (self.transitions, IdentityCandidateTransition, "transition_id"),
            (self.conflicts, IdentityConflict, "conflict_id"),
        )
        for values, expected_type, id_name in typed_groups:
            if type(values) is not tuple or any(type(value) is not expected_type for value in values):
                raise TypeError(f"registry {id_name} values must use exact immutable types")
            identifiers = tuple(getattr(value, id_name) for value in values)
            if identifiers != tuple(sorted(set(identifiers))):
                raise ValueError(f"registry {id_name} values must be sorted and unique")
            for value in values:
                value.verify_content_identity()
        if not self.observations:
            raise ValueError("registry must contain retained equity observations")

        manifests_by_id = {value.artifact_id: value for value in self.source_manifests}
        counts_by_source = {value: 0 for value in manifests_by_id}
        observations_by_id = {value.observation_id: value for value in self.observations}
        for observation in self.observations:
            manifest = manifests_by_id.get(observation.source_artifact_id)
            if (
                manifest is None
                or observation.source_manifest_id != manifest.manifest_id
                or observation.claimed_report_date != manifest.claimed_report_date
                or observation.knowledge_time != manifest.validated_at
            ):
                raise ValueError("observation lineage disagrees with its source manifest")
            counts_by_source[observation.source_artifact_id] += 1
        if any(
            counts_by_source[manifest.artifact_id]
            != manifest.retained_unverified_equity_count
            for manifest in self.source_manifests
        ):
            raise ValueError("every retained master row must own one identity observation")

        candidate_coverage = tuple(
            sorted(
                observation_id
                for candidate in self.candidates
                for observation_id in candidate.observation_ids
            )
        )
        if candidate_coverage != tuple(sorted(observations_by_id)):
            raise ValueError("every observation must belong to exactly one candidate")
        candidates_by_id = {value.candidate_id: value for value in self.candidates}
        candidate_id_by_observation = {
            observation_id: candidate.candidate_id
            for candidate in self.candidates
            for observation_id in candidate.observation_ids
        }
        for transition in self.transitions:
            candidate = candidates_by_id.get(transition.candidate_id)
            previous = observations_by_id.get(transition.previous_observation_id)
            current = observations_by_id.get(transition.current_observation_id)
            if (
                candidate is None
                or previous is None
                or current is None
                or candidate_id_by_observation[previous.observation_id] != candidate.candidate_id
                or candidate_id_by_observation[current.observation_id] != candidate.candidate_id
                or previous.claimed_report_date != transition.previous_claimed_report_date
                or current.claimed_report_date != transition.current_claimed_report_date
                or transition.symbol_changed
                != (previous.ticker_symbol != current.ticker_symbol)
                or transition.series_changed
                != (previous.security_series != current.security_series)
                or transition.financial_instrument_id_changed
                != (
                    previous.financial_instrument_id
                    != current.financial_instrument_id
                )
                or transition.instrument_name_changed
                != (previous.instrument_name != current.instrument_name)
            ):
                raise ValueError("transition lineage disagrees with its candidate observations")
        conflict_observation_ids: set[str] = set()
        for conflict in self.conflicts:
            if any(value not in observations_by_id for value in conflict.observation_ids):
                raise ValueError("conflict references an unknown observation")
            conflict_observation_ids.update(conflict.observation_ids)
        for candidate in self.candidates:
            touches_conflict = any(
                value in conflict_observation_ids for value in candidate.observation_ids
            )
            if touches_conflict != (candidate.status is IdentityCandidateStatus.CONFLICT):
                raise ValueError("candidate conflict status disagrees with conflict lineage")
            candidate_observations = tuple(
                observations_by_id[value] for value in candidate.observation_ids
            )
            if candidate.basis is IdentityCandidateBasis.VALIDATED_ISIN:
                if any(
                    value.validated_isin != candidate.validated_isin
                    for value in candidate_observations
                ):
                    raise ValueError("ISIN candidate contains unrelated observations")
                observed_dates = {
                    value.claimed_report_date for value in candidate_observations
                }
                expected_status = (
                    IdentityCandidateStatus.CONFLICT
                    if touches_conflict
                    else IdentityCandidateStatus.SINGLE_VINTAGE
                    if len(observed_dates) == 1
                    else IdentityCandidateStatus.CANDIDATE_CONTINUITY
                )
                if candidate.status is not expected_status:
                    raise ValueError("ISIN candidate status disagrees with vintage coverage")
            elif (
                len(candidate_observations) != 1
                or candidate_observations[0].validated_isin is not None
                or candidate_observations[0].raw_source_identifier
                != candidate.raw_source_identifier
            ):
                raise ValueError("unvalidated candidate contains unrelated observations")
        object.__setattr__(self, "registry_id", self._calculated_id())

    @property
    def source_artifact_ids(self) -> tuple[str, ...]:
        return tuple(value.artifact_id for value in self.source_manifests)

    def observations_on_claimed_date(self, value: date) -> tuple[IdentityObservation, ...]:
        if type(value) is not date:
            raise TypeError("claimed date must be a date")
        return tuple(item for item in self.observations if item.claimed_report_date == value)

    def candidate_for_observation(self, observation_id: str) -> IdentityContinuityCandidate:
        _require_sha256(observation_id, "observation_id")
        matches = tuple(
            candidate
            for candidate in self.candidates
            if observation_id in candidate.observation_ids
        )
        if len(matches) != 1:
            raise IdentityRegistryIntegrityError(
                "observation does not resolve to exactly one identity candidate"
            )
        return matches[0]

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema_version": self.schema_version,
                "policy_version": self.policy_version,
                "codec_version": self.codec_version,
                "cutoff": self.cutoff,
                "knowledge_time": self.knowledge_time,
                "source_manifests": self.source_manifests,
                "observations": self.observations,
                "candidates": self.candidates,
                "transitions": self.transitions,
                "conflicts": self.conflicts,
                "readiness": self.readiness,
                "actionable": self.actionable,
                "coverage_scope": self.coverage_scope,
                "report_date_status": self.report_date_status,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        for values in (
            self.observations,
            self.candidates,
            self.transitions,
            self.conflicts,
        ):
            for value in values:
                value.verify_content_identity()
        if self.registry_id != self._calculated_id():
            raise IdentityRegistryIntegrityError("registry content identity failed")
