from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path, PurePath

from india_swing.identity import content_id
from india_swing.identity_registry import IdentityAdjudicationRequirement
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference_data.models import AcquisitionMode


IDENTITY_REVIEW_DECLARATION_SCHEMA_VERSION = "identity-review-declaration/v1"
IDENTITY_REVIEW_DECISION_SCHEMA_VERSION = "identity-review-decision/v1"
IDENTITY_REVIEW_BUNDLE_SCHEMA_VERSION = "identity-review-bundle/v1"
IDENTITY_REVIEW_CODEC_VERSION = "identity-review-normalized-json/v1"
IDENTITY_REVIEW_PARSER_VERSION = "identity-review-parser/v1"
IDENTITY_REVIEW_POLICY_VERSION = "explicit-evidence-human-review/v1"
IDENTITY_REVIEW_DATASET = "identity-review-bundles"

ADJUDICATED_IDENTITY_SCHEMA_VERSION = "adjudicated-identity-snapshot/v1"
ADJUDICATED_IDENTITY_POLICY_VERSION = "all-requirements-accepted-simple-candidates/v1"
STABLE_INSTRUMENT_ID_SCHEME = "nse-cm-stable-instrument-by-validated-isin/v1"
STABLE_LISTING_ID_SCHEME = "nse-cm-stable-listing-by-instrument-series/v1"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_REVIEWER = re.compile(r"[a-z0-9][a-z0-9:._-]{2,127}\Z")
_ISIN = re.compile(r"IN[A-Z0-9]{9}[0-9]\Z")


class IdentityDecisionError(RuntimeError):
    pass


class IdentityDecisionIntegrityError(IdentityDecisionError):
    pass


class IdentityDecisionConflict(IdentityDecisionError):
    pass


class IdentityDecisionNotFound(IdentityDecisionError):
    pass


class IdentityReviewOutcome(str, Enum):
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"


class IdentityResolutionBlocker(str, Enum):
    MISSING_REVIEW_DECISION = "MISSING_REVIEW_DECISION"
    REJECTED_REVIEW_DECISION = "REJECTED_REVIEW_DECISION"
    UNSUPPORTED_CANDIDATE_SHAPE = "UNSUPPORTED_CANDIDATE_SHAPE"


def _sha(value: str, name: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ValueError(f"{name} must be a full lowercase SHA-256")


def _utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _text(value: str, name: str, maximum: int = 2_000) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > maximum
        or any(ord(character) < 32 for character in value)
    ):
        raise ValueError(f"{name} must be canonical non-empty text")


def _safe_basename(value: str, name: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or PurePath(value).name != value
        or "/" in value
        or "\\" in value
    ):
        raise ValueError(f"{name} must be a safe basename")


@dataclass(frozen=True, slots=True)
class IdentityReviewDecision:
    queue_id: str
    source_registry_id: str
    candidate_id: str
    requirement: IdentityAdjudicationRequirement
    outcome: IdentityReviewOutcome
    evidence_artifact_id: str
    evidence_claim_id: str
    rationale: str
    schema_version: str = IDENTITY_REVIEW_DECISION_SCHEMA_VERSION
    decision_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.queue_id, "decision queue_id"),
            (self.source_registry_id, "decision source_registry_id"),
            (self.candidate_id, "decision candidate_id"),
            (self.evidence_artifact_id, "decision evidence_artifact_id"),
            (self.evidence_claim_id, "decision evidence_claim_id"),
        ):
            _sha(value, name)
        if type(self.requirement) is not IdentityAdjudicationRequirement:
            raise TypeError("decision requirement must be exact")
        if type(self.outcome) is not IdentityReviewOutcome:
            raise TypeError("decision outcome must be exact")
        _text(self.rationale, "decision rationale")
        if self.schema_version != IDENTITY_REVIEW_DECISION_SCHEMA_VERSION:
            raise ValueError("unsupported identity-review decision schema")
        object.__setattr__(self, "decision_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema_version": self.schema_version,
                "queue_id": self.queue_id,
                "source_registry_id": self.source_registry_id,
                "candidate_id": self.candidate_id,
                "requirement": self.requirement,
                "outcome": self.outcome,
                "evidence_artifact_id": self.evidence_artifact_id,
                "evidence_claim_id": self.evidence_claim_id,
                "rationale": self.rationale,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.decision_id != self._calculated_id():
            raise IdentityDecisionIntegrityError("review decision identity failed")


@dataclass(frozen=True, slots=True)
class ParsedIdentityReviewBundle:
    queue_id: str
    source_registry_id: str
    reviewer_id: str
    reviewed_at: datetime
    decisions: tuple[IdentityReviewDecision, ...]
    schema_version: str = IDENTITY_REVIEW_DECLARATION_SCHEMA_VERSION
    bundle_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha(self.queue_id, "review queue_id")
        _sha(self.source_registry_id, "review source_registry_id")
        if not isinstance(self.reviewer_id, str) or _REVIEWER.fullmatch(self.reviewer_id) is None:
            raise ValueError("reviewer_id must be canonical lower-case text")
        object.__setattr__(self, "reviewed_at", _utc(self.reviewed_at, "reviewed_at"))
        if (
            type(self.decisions) is not tuple
            or not self.decisions
            or any(type(value) is not IdentityReviewDecision for value in self.decisions)
            or self.decisions != tuple(sorted(self.decisions, key=lambda value: value.decision_id))
            or len({(value.candidate_id, value.requirement) for value in self.decisions})
            != len(self.decisions)
        ):
            raise ValueError("review decisions must be non-empty, exact, ordered, and pair-unique")
        for decision in self.decisions:
            decision.verify_content_identity()
            if decision.queue_id != self.queue_id or decision.source_registry_id != self.source_registry_id:
                raise ValueError("review decision lineage disagrees with its bundle")
        if self.schema_version != IDENTITY_REVIEW_DECLARATION_SCHEMA_VERSION:
            raise ValueError("unsupported identity-review declaration schema")
        object.__setattr__(self, "bundle_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema_version": self.schema_version,
                "policy_version": IDENTITY_REVIEW_POLICY_VERSION,
                "queue_id": self.queue_id,
                "source_registry_id": self.source_registry_id,
                "reviewer_id": self.reviewer_id,
                "reviewed_at": self.reviewed_at,
                "decisions": self.decisions,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        for value in self.decisions:
            if type(value) is not IdentityReviewDecision:
                raise IdentityDecisionIntegrityError("review bundle contains an invalid decision")
            value.verify_content_identity()
        if self.bundle_id != self._calculated_id():
            raise IdentityDecisionIntegrityError("review bundle identity failed")


@dataclass(frozen=True, slots=True)
class IdentityReviewBundleManifest:
    bundle_id: str
    manifest_id: str
    first_seen_at: datetime
    validated_at: datetime
    original_declaration_filename: str
    declaration_byte_count: int
    declaration_sha256: str
    normalized_byte_count: int
    normalized_sha256: str
    queue_id: str
    source_registry_id: str
    reviewer_id: str
    reviewed_at: datetime
    decision_ids: tuple[str, ...]
    acquisition_mode: AcquisitionMode = AcquisitionMode.UNVERIFIED_MANUAL_FILE
    readiness: ReferenceReadiness = ReferenceReadiness.COLLECTION_ONLY
    actionable: bool = False
    parser_version: str = IDENTITY_REVIEW_PARSER_VERSION
    codec_version: str = IDENTITY_REVIEW_CODEC_VERSION
    policy_version: str = IDENTITY_REVIEW_POLICY_VERSION
    schema_version: str = IDENTITY_REVIEW_BUNDLE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        for value, name in (
            (self.bundle_id, "review bundle_id"),
            (self.manifest_id, "review manifest_id"),
            (self.declaration_sha256, "review declaration_sha256"),
            (self.normalized_sha256, "review normalized_sha256"),
            (self.queue_id, "review queue_id"),
            (self.source_registry_id, "review source_registry_id"),
        ):
            _sha(value, name)
        object.__setattr__(self, "first_seen_at", _utc(self.first_seen_at, "first_seen_at"))
        object.__setattr__(self, "validated_at", _utc(self.validated_at, "validated_at"))
        object.__setattr__(self, "reviewed_at", _utc(self.reviewed_at, "reviewed_at"))
        if self.validated_at < self.first_seen_at or self.reviewed_at > self.first_seen_at:
            raise ValueError("review timing is inconsistent with local observation")
        _safe_basename(self.original_declaration_filename, "original_declaration_filename")
        if not isinstance(self.reviewer_id, str) or _REVIEWER.fullmatch(self.reviewer_id) is None:
            raise ValueError("manifest reviewer_id is invalid")
        for value in (self.declaration_byte_count, self.normalized_byte_count):
            if type(value) is not int or value <= 0:
                raise ValueError("review byte counts must be positive")
        if self.decision_ids != tuple(sorted(set(self.decision_ids))) or not self.decision_ids:
            raise ValueError("manifest decision_ids must be non-empty, sorted, and unique")
        for value in self.decision_ids:
            _sha(value, "review decision_id")
        if (
            self.acquisition_mode is not AcquisitionMode.UNVERIFIED_MANUAL_FILE
            or self.readiness is not ReferenceReadiness.COLLECTION_ONLY
            or self.actionable is not False
            or self.parser_version != IDENTITY_REVIEW_PARSER_VERSION
            or self.codec_version != IDENTITY_REVIEW_CODEC_VERSION
            or self.policy_version != IDENTITY_REVIEW_POLICY_VERSION
            or self.schema_version != IDENTITY_REVIEW_BUNDLE_SCHEMA_VERSION
        ):
            raise ValueError("review bundle must remain manual, collection-only, and non-actionable")


@dataclass(frozen=True, slots=True)
class StoredIdentityReviewBundle:
    path: Path
    manifest: IdentityReviewBundleManifest
    parsed: ParsedIdentityReviewBundle
    declaration_bytes: bytes
    normalized_bytes: bytes

    def __post_init__(self) -> None:
        if not isinstance(self.path, Path):
            raise TypeError("stored review path must be a Path")
        if type(self.manifest) is not IdentityReviewBundleManifest:
            raise TypeError("stored review manifest must be exact")
        if type(self.parsed) is not ParsedIdentityReviewBundle:
            raise TypeError("stored review bundle must be exact")
        if type(self.declaration_bytes) is not bytes or type(self.normalized_bytes) is not bytes:
            raise TypeError("stored review payloads must be exact bytes")


@dataclass(frozen=True, slots=True)
class CandidateIdentityResolution:
    candidate_id: str
    required_requirements: tuple[IdentityAdjudicationRequirement, ...]
    accepted_decision_ids: tuple[str, ...]
    rejected_decision_ids: tuple[str, ...]
    missing_requirements: tuple[IdentityAdjudicationRequirement, ...]
    blocker_codes: tuple[IdentityResolutionBlocker, ...]
    stable_instrument_id: str | None

    def __post_init__(self) -> None:
        _sha(self.candidate_id, "resolution candidate_id")
        for values, name in (
            (self.required_requirements, "required_requirements"),
            (self.missing_requirements, "missing_requirements"),
        ):
            if type(values) is not tuple or values != tuple(sorted(set(values), key=lambda value: value.value)):
                raise ValueError(f"{name} must be sorted, unique, and exact")
        if not set(self.missing_requirements).issubset(self.required_requirements):
            raise ValueError("missing requirements must be required")
        for values, name in (
            (self.accepted_decision_ids, "accepted_decision_ids"),
            (self.rejected_decision_ids, "rejected_decision_ids"),
        ):
            if values != tuple(sorted(set(values))):
                raise ValueError(f"{name} must be sorted and unique")
            for value in values:
                _sha(value, name)
        if self.blocker_codes != tuple(sorted(set(self.blocker_codes), key=lambda value: value.value)):
            raise ValueError("resolution blocker codes must be sorted and unique")
        if (self.stable_instrument_id is None) != bool(self.blocker_codes):
            raise ValueError("stable identity is assigned exactly when no blockers remain")
        if self.stable_instrument_id is not None:
            _sha(self.stable_instrument_id, "stable_instrument_id")


@dataclass(frozen=True, slots=True)
class EffectiveStableListingObservation:
    candidate_id: str
    source_observation_id: str
    stable_instrument_id: str
    stable_listing_id: str
    effective_on: date
    symbol: str
    series: str
    isin: str
    record_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.candidate_id, "listing candidate_id"),
            (self.source_observation_id, "listing source_observation_id"),
            (self.stable_instrument_id, "listing stable_instrument_id"),
            (self.stable_listing_id, "listing stable_listing_id"),
        ):
            _sha(value, name)
        if type(self.effective_on) is not date:
            raise TypeError("listing effective_on must be a date")
        _text(self.symbol, "listing symbol", 64)
        _text(self.series, "listing series", 16)
        if self.symbol != self.symbol.upper() or self.series != self.series.upper():
            raise ValueError("listing symbol and series must be uppercase")
        if not isinstance(self.isin, str) or _ISIN.fullmatch(self.isin) is None:
            raise ValueError("listing isin must be a validated Indian ISIN")
        object.__setattr__(self, "record_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "effective-stable-listing-observation/v1",
                "candidate_id": self.candidate_id,
                "source_observation_id": self.source_observation_id,
                "stable_instrument_id": self.stable_instrument_id,
                "stable_listing_id": self.stable_listing_id,
                "effective_on": self.effective_on,
                "symbol": self.symbol,
                "series": self.series,
                "isin": self.isin,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.record_id != self._calculated_id():
            raise IdentityDecisionIntegrityError("stable listing observation identity failed")


@dataclass(frozen=True, slots=True)
class AdjudicatedIdentitySnapshot:
    source_registry_id: str
    source_queue_id: str
    cutoff: datetime
    knowledge_time: datetime
    evidence_artifact_ids: tuple[str, ...]
    review_bundle_ids: tuple[str, ...]
    resolutions: tuple[CandidateIdentityResolution, ...]
    listing_observations: tuple[EffectiveStableListingObservation, ...]
    readiness: ReferenceReadiness = ReferenceReadiness.COLLECTION_ONLY
    actionable: bool = False
    stable_identity_assigned: bool = field(init=False)
    schema_version: str = ADJUDICATED_IDENTITY_SCHEMA_VERSION
    policy_version: str = ADJUDICATED_IDENTITY_POLICY_VERSION
    snapshot_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha(self.source_registry_id, "snapshot source_registry_id")
        _sha(self.source_queue_id, "snapshot source_queue_id")
        object.__setattr__(self, "cutoff", _utc(self.cutoff, "snapshot cutoff"))
        object.__setattr__(self, "knowledge_time", _utc(self.knowledge_time, "snapshot knowledge_time"))
        if self.knowledge_time > self.cutoff:
            raise ValueError("snapshot knowledge cannot follow cutoff")
        for values, name in (
            (self.evidence_artifact_ids, "snapshot evidence_artifact_ids"),
            (self.review_bundle_ids, "snapshot review_bundle_ids"),
        ):
            if values != tuple(sorted(set(values))):
                raise ValueError(f"{name} must be sorted and unique")
            for value in values:
                _sha(value, name)
        if self.resolutions != tuple(sorted(self.resolutions, key=lambda value: value.candidate_id)):
            raise ValueError("snapshot resolutions must be candidate ordered")
        if self.listing_observations != tuple(sorted(
            self.listing_observations,
            key=lambda value: (value.effective_on, value.stable_listing_id, value.source_observation_id),
        )):
            raise ValueError("stable listing observations must be effective-date ordered")
        for value in self.listing_observations:
            value.verify_content_identity()
        assigned_ids = {
            value.stable_instrument_id
            for value in self.resolutions
            if value.stable_instrument_id is not None
        }
        if {value.stable_instrument_id for value in self.listing_observations} != assigned_ids:
            raise ValueError("listing observations must exactly cover assigned instruments")
        if self.readiness is not ReferenceReadiness.COLLECTION_ONLY or self.actionable is not False:
            raise ValueError("adjudicated identity snapshot must remain collection-only")
        if (
            self.schema_version != ADJUDICATED_IDENTITY_SCHEMA_VERSION
            or self.policy_version != ADJUDICATED_IDENTITY_POLICY_VERSION
        ):
            raise ValueError("unsupported adjudicated identity contract")
        object.__setattr__(self, "stable_identity_assigned", bool(self.listing_observations))
        object.__setattr__(self, "snapshot_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema_version": self.schema_version,
                "policy_version": self.policy_version,
                "source_registry_id": self.source_registry_id,
                "source_queue_id": self.source_queue_id,
                "cutoff": self.cutoff,
                "knowledge_time": self.knowledge_time,
                "evidence_artifact_ids": self.evidence_artifact_ids,
                "review_bundle_ids": self.review_bundle_ids,
                "resolutions": self.resolutions,
                "listing_observations": self.listing_observations,
                "readiness": self.readiness,
                "actionable": self.actionable,
                "stable_identity_assigned": self.stable_identity_assigned,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        for value in self.listing_observations:
            value.verify_content_identity()
        if self.snapshot_id != self._calculated_id():
            raise IdentityDecisionIntegrityError("adjudicated identity snapshot identity failed")
