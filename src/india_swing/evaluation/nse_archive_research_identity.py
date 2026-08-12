"""NSE archive research-only, point-in-time identity-admission layer.

Streams a deterministic, research-only ISIN-based join key ("research
identity") over one already verified
:class:`~india_swing.evaluation.nse_archive_research_replay.NseArchiveResearchReplaySession`
at a time, obtained exclusively through the existing public replay trust
boundary ``iter_verified_nse_archive_research_sessions``. This module never
reads the filesystem, network, environment, or clock; never constructs a
store; never lists, discovers, or selects a "latest" artifact; and never
marks a decision or session as feature-, label-, alert-, training-, paper-,
or execution-eligible.

A research identity is a deterministic hash of an exchange and an ISIN. It
is a research join key, never a production ``financial_instrument_id`` and
never an authorization. Two admission bases feed it:

- ``VALIDATED_SAME_SESSION_ISIN``: the modern archive's own same-session
  matched, validated identity.
- ``LEGACY_SOURCE_ATTESTED_ISIN``: one exact ordered legacy Bhavcopy ISIN
  claim, retained as evidence, never described as validated.

A record with neither is ``BLOCKED_UNRESOLVED``. A record carrying both is
an impossible, ambiguous shape and rejects its entire session. Two distinct
listing lanes admissibly claiming the same ISIN in one session are both
``BLOCKED_SAME_SESSION_ISIN_COLLISION`` -- no winner is ever selected.

Streaming and bounded: only the one replay session currently being admitted
is ever held in memory, plus two bounded dictionaries -- the latest admitted
observation per listing key and per research identity -- used solely to
detect past-only ``LISTING_KEY_REBOUND``/``IDENTITY_SYMBOL_CHANGED``
transitions. Prior-state maps are updated only after a session's decisions
and transitions are fully constructed and independently re-verified; a
blocked row never updates them. A rebound never blocks the new identity and
never rewrites an earlier decision -- the identity key itself, not a
lookup-time check, is what prevents price continuity across a rebound.

``research_identity_admission_complete`` never implies production identity
resolution, corporate-action adjustment, or any training/feature/label/
alert/paper/execution authority.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Iterator

from india_swing.identity import content_id

from .nse_archive_research_dataset import NseArchiveResearchDataset, ResearchSplitRole
from .nse_archive_research_replay import (
    NseArchiveResearchReplaySession,
    NseHistoricalArchiveSnapshotReader,
    _SYMBOL,
    iter_verified_nse_archive_research_sessions,
)


RESEARCH_IDENTITY_ADMISSION_POLICY_VERSION = (
    "nse-archive-research-identity-admission/positive-only-v1"
)
RESEARCH_IDENTITY_SCHEMA_VERSION = "nse-archive-research-identity/v1"
RESEARCH_IDENTITY_DECISION_SCHEMA_VERSION = "nse-archive-research-identity-decision/v1"
RESEARCH_IDENTITY_TRANSITION_SCHEMA_VERSION = (
    "nse-archive-research-identity-transition/v1"
)
RESEARCH_IDENTITY_ADMISSION_SESSION_SCHEMA_VERSION = (
    "nse-archive-research-identity-admission-session/v1"
)
RESEARCH_IDENTITY_PAIRED_SESSION_SCHEMA_VERSION = (
    "nse-archive-research-paired-session/v1"
)

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
# Canonical ISIN shape only -- deliberately narrower than the replay layer's
# generic _RAW_IDENTIFIER source-identifier pattern. A research identity is a
# cross-year join key; admitting a merely source-shaped but non-ISIN value
# here (e.g. a short internal code) risks splicing unrelated securities
# together under a false shared key.
_ISIN_PATTERN = re.compile(r"[A-Z]{2}[A-Z0-9]{9}[0-9]\Z")


def _is_canonical_isin(value: object) -> bool:
    return type(value) is str and _ISIN_PATTERN.fullmatch(value) is not None


class NseArchiveResearchIdentityError(ValueError):
    """A research-identity input, capability, or reconstructed artifact failed a static safety rule."""


def _fail(message: str) -> None:
    raise NseArchiveResearchIdentityError(message)


def _is_sha256(value: object) -> bool:
    return type(value) is str and _SHA256_PATTERN.fullmatch(value) is not None


class NseArchiveResearchIdentityBasis(Enum):
    VALIDATED_SAME_SESSION_ISIN = "VALIDATED_SAME_SESSION_ISIN"
    LEGACY_SOURCE_ATTESTED_ISIN = "LEGACY_SOURCE_ATTESTED_ISIN"
    UNAVAILABLE = "UNAVAILABLE"


class NseArchiveResearchIdentityAdmissionStatus(Enum):
    ADMITTED_VALIDATED = "ADMITTED_VALIDATED"
    ADMITTED_SOURCE_ATTESTED = "ADMITTED_SOURCE_ATTESTED"
    BLOCKED_UNRESOLVED = "BLOCKED_UNRESOLVED"
    BLOCKED_SAME_SESSION_ISIN_COLLISION = "BLOCKED_SAME_SESSION_ISIN_COLLISION"


class NseArchiveResearchIdentityTransitionKind(Enum):
    LISTING_KEY_REBOUND = "LISTING_KEY_REBOUND"
    IDENTITY_SYMBOL_CHANGED = "IDENTITY_SYMBOL_CHANGED"


_ADMITTED_STATUSES = frozenset(
    {
        NseArchiveResearchIdentityAdmissionStatus.ADMITTED_VALIDATED,
        NseArchiveResearchIdentityAdmissionStatus.ADMITTED_SOURCE_ATTESTED,
    }
)


def research_identity_id_for_isin(isin: str) -> str:
    """Deterministic research join key for one exchange+ISIN pair.

    The same exact ISIN produces the same research identity regardless of
    whether it arrived via a validated same-session match or a retained
    legacy source-attested claim. This is a research join key, never a
    production ``financial_instrument_id`` or an authorization.
    """

    if not _is_canonical_isin(isin):
        _fail("research identity ISIN is invalid")
    return content_id(
        {
            "schema": RESEARCH_IDENTITY_SCHEMA_VERSION,
            "policy_version": RESEARCH_IDENTITY_ADMISSION_POLICY_VERSION,
            "exchange": "NSE",
            "isin": isin,
        },
        length=64,
    )


@dataclass(frozen=True, slots=True)
class NseArchiveResearchIdentityDecision:
    """One immutable, independently re-verifiable per-record admission decision."""

    dataset_id: str
    replay_session_id: str
    session_snapshot_id: str
    market_session: date
    partition_id: str
    partition_role: ResearchSplitRole
    record_id: str
    listing_key: str
    symbol: str
    series: str
    source_claim_id: str | None
    source_isin: str | None
    basis: NseArchiveResearchIdentityBasis
    admission_status: NseArchiveResearchIdentityAdmissionStatus
    research_identity_id: str | None
    decision_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate_shape()
        object.__setattr__(self, "decision_id", self._calculated_id())

    def _validate_shape(self) -> None:
        for value, name in (
            (self.dataset_id, "dataset id"),
            (self.replay_session_id, "replay session id"),
            (self.session_snapshot_id, "session snapshot id"),
            (self.partition_id, "partition id"),
            (self.record_id, "record id"),
        ):
            if not _is_sha256(value):
                _fail(f"research identity decision {name} is invalid")
        if type(self.market_session) is not date:
            _fail("research identity decision market session is invalid")
        if type(self.partition_role) is not ResearchSplitRole:
            _fail("research identity decision partition role is invalid")
        if type(self.symbol) is not str or _SYMBOL.fullmatch(self.symbol) is None:
            _fail("research identity decision symbol is invalid")
        if self.series != "EQ":
            _fail("research identity decision series is invalid")
        if type(self.listing_key) is not str or self.listing_key != f"NSE:{self.symbol}":
            _fail("research identity decision listing key is invalid")
        if self.source_claim_id is not None and not _is_sha256(self.source_claim_id):
            _fail("research identity decision source claim id is invalid")
        if self.source_isin is not None and not _is_canonical_isin(self.source_isin):
            _fail("research identity decision source isin is invalid")
        if type(self.basis) is not NseArchiveResearchIdentityBasis:
            _fail("research identity decision basis is invalid")
        if type(self.admission_status) is not NseArchiveResearchIdentityAdmissionStatus:
            _fail("research identity decision admission status is invalid")
        if self.research_identity_id is not None and not _is_sha256(
            self.research_identity_id
        ):
            _fail("research identity decision research identity id is invalid")

        basis = self.basis
        status = self.admission_status
        if basis is NseArchiveResearchIdentityBasis.VALIDATED_SAME_SESSION_ISIN:
            if status not in (
                NseArchiveResearchIdentityAdmissionStatus.ADMITTED_VALIDATED,
                NseArchiveResearchIdentityAdmissionStatus.BLOCKED_SAME_SESSION_ISIN_COLLISION,
            ):
                _fail("research identity decision basis/status combination is invalid")
            if self.source_isin is None or self.source_claim_id is not None:
                _fail("research identity decision evidence fields are invalid for its basis")
        elif basis is NseArchiveResearchIdentityBasis.LEGACY_SOURCE_ATTESTED_ISIN:
            if status not in (
                NseArchiveResearchIdentityAdmissionStatus.ADMITTED_SOURCE_ATTESTED,
                NseArchiveResearchIdentityAdmissionStatus.BLOCKED_SAME_SESSION_ISIN_COLLISION,
            ):
                _fail("research identity decision basis/status combination is invalid")
            if self.source_isin is None or self.source_claim_id is None:
                _fail("research identity decision evidence fields are invalid for its basis")
        else:
            if status is not NseArchiveResearchIdentityAdmissionStatus.BLOCKED_UNRESOLVED:
                _fail("research identity decision basis/status combination is invalid")
            if self.source_isin is not None or self.source_claim_id is not None:
                _fail("research identity decision evidence fields are invalid for its basis")

        if status in _ADMITTED_STATUSES:
            if self.research_identity_id is None:
                _fail("research identity decision admitted rows must carry a research identity")
            if self.research_identity_id != research_identity_id_for_isin(self.source_isin):
                _fail("research identity decision research identity id failed")
        elif self.research_identity_id is not None:
            _fail("research identity decision blocked rows must not carry a research identity")

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": RESEARCH_IDENTITY_DECISION_SCHEMA_VERSION,
                "policy_version": RESEARCH_IDENTITY_ADMISSION_POLICY_VERSION,
                "dataset_id": self.dataset_id,
                "replay_session_id": self.replay_session_id,
                "session_snapshot_id": self.session_snapshot_id,
                "market_session": self.market_session,
                "partition_id": self.partition_id,
                "partition_role": self.partition_role,
                "record_id": self.record_id,
                "listing_key": self.listing_key,
                "symbol": self.symbol,
                "series": self.series,
                "source_claim_id": self.source_claim_id,
                "source_isin": self.source_isin,
                "basis": self.basis,
                "admission_status": self.admission_status,
                "research_identity_id": self.research_identity_id,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate_shape()
        if self.decision_id != self._calculated_id():
            _fail("research identity decision identity failed")


@dataclass(frozen=True, slots=True)
class NseArchiveResearchIdentityTransition:
    """One immutable, past-only transition between two admitted decisions."""

    kind: NseArchiveResearchIdentityTransitionKind
    previous_market_session: date
    current_market_session: date
    previous_record_id: str
    current_record_id: str
    previous_research_identity_id: str
    current_research_identity_id: str
    previous_listing_key: str
    current_listing_key: str
    previous_symbol: str
    current_symbol: str
    transition_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate_shape()
        object.__setattr__(self, "transition_id", self._calculated_id())

    def _validate_shape(self) -> None:
        if type(self.kind) is not NseArchiveResearchIdentityTransitionKind:
            _fail("research identity transition kind is invalid")
        if (
            type(self.previous_market_session) is not date
            or type(self.current_market_session) is not date
            or self.current_market_session <= self.previous_market_session
        ):
            _fail("research identity transition sessions must be strictly increasing")
        for value, name in (
            (self.previous_record_id, "previous record id"),
            (self.current_record_id, "current record id"),
            (self.previous_research_identity_id, "previous research identity id"),
            (self.current_research_identity_id, "current research identity id"),
        ):
            if not _is_sha256(value):
                _fail(f"research identity transition {name} is invalid")
        for value, name in (
            (self.previous_symbol, "previous symbol"),
            (self.current_symbol, "current symbol"),
        ):
            if type(value) is not str or _SYMBOL.fullmatch(value) is None:
                _fail(f"research identity transition {name} is invalid")
        if self.previous_listing_key != f"NSE:{self.previous_symbol}":
            _fail("research identity transition previous listing key is invalid")
        if self.current_listing_key != f"NSE:{self.current_symbol}":
            _fail("research identity transition current listing key is invalid")

        if self.kind is NseArchiveResearchIdentityTransitionKind.LISTING_KEY_REBOUND:
            if self.previous_listing_key != self.current_listing_key:
                _fail("research identity listing-key-rebound listing key must be unchanged")
            if self.previous_research_identity_id == self.current_research_identity_id:
                _fail(
                    "research identity listing-key-rebound must change research identity"
                )
        else:
            if self.previous_research_identity_id != self.current_research_identity_id:
                _fail(
                    "research identity symbol-change transition must keep research identity"
                )
            if self.previous_symbol == self.current_symbol:
                _fail("research identity symbol-change transition must change symbol")

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": RESEARCH_IDENTITY_TRANSITION_SCHEMA_VERSION,
                "policy_version": RESEARCH_IDENTITY_ADMISSION_POLICY_VERSION,
                "kind": self.kind,
                "previous_market_session": self.previous_market_session,
                "current_market_session": self.current_market_session,
                "previous_record_id": self.previous_record_id,
                "current_record_id": self.current_record_id,
                "previous_research_identity_id": self.previous_research_identity_id,
                "current_research_identity_id": self.current_research_identity_id,
                "previous_listing_key": self.previous_listing_key,
                "current_listing_key": self.current_listing_key,
                "previous_symbol": self.previous_symbol,
                "current_symbol": self.current_symbol,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate_shape()
        if self.transition_id != self._calculated_id():
            _fail("research identity transition identity failed")


@dataclass(frozen=True, slots=True)
class NseArchiveResearchIdentityAdmissionSession:
    """One immutable research-identity admission grade over one replayed session."""

    dataset_id: str
    replay_session_id: str
    session_snapshot_id: str
    market_session: date
    partition_id: str
    partition_role: ResearchSplitRole
    decisions: tuple[NseArchiveResearchIdentityDecision, ...]
    transitions: tuple[NseArchiveResearchIdentityTransition, ...]
    admitted_validated_count: int
    admitted_source_attested_count: int
    blocked_unresolved_count: int
    blocked_collision_count: int
    research_identity_admission_complete: bool = field(init=False)
    production_identity_resolution_complete: bool = field(init=False)
    corporate_action_adjustment_complete: bool = field(init=False)
    collection_only: bool = field(init=False)
    actionable: bool = field(init=False)
    training_eligible: bool = field(init=False)
    feature_eligible: bool = field(init=False)
    label_eligible: bool = field(init=False)
    alert_eligible: bool = field(init=False)
    execution_eligible: bool = field(init=False)
    admission_session_id: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "research_identity_admission_complete",
            self.blocked_unresolved_count == 0 and self.blocked_collision_count == 0,
        )
        object.__setattr__(self, "production_identity_resolution_complete", False)
        object.__setattr__(self, "corporate_action_adjustment_complete", False)
        object.__setattr__(self, "collection_only", True)
        object.__setattr__(self, "actionable", False)
        object.__setattr__(self, "training_eligible", False)
        object.__setattr__(self, "feature_eligible", False)
        object.__setattr__(self, "label_eligible", False)
        object.__setattr__(self, "alert_eligible", False)
        object.__setattr__(self, "execution_eligible", False)
        self._validate()
        object.__setattr__(self, "admission_session_id", self._calculated_id())

    def _validate(self) -> None:
        for value, name in (
            (self.dataset_id, "dataset id"),
            (self.replay_session_id, "replay session id"),
            (self.session_snapshot_id, "session snapshot id"),
            (self.partition_id, "partition id"),
        ):
            if not _is_sha256(value):
                _fail(f"research identity admission session {name} is invalid")
        if type(self.market_session) is not date:
            _fail("research identity admission session market session is invalid")
        if type(self.partition_role) is not ResearchSplitRole:
            _fail("research identity admission session partition role is invalid")
        if type(self.decisions) is not tuple or any(
            type(value) is not NseArchiveResearchIdentityDecision for value in self.decisions
        ):
            _fail("research identity admission session decisions are invalid")
        record_ids: list[str] = []
        for decision in self.decisions:
            decision.verify_content_identity()
            if (
                decision.dataset_id != self.dataset_id
                or decision.replay_session_id != self.replay_session_id
                or decision.session_snapshot_id != self.session_snapshot_id
                or decision.market_session != self.market_session
                or decision.partition_id != self.partition_id
                or decision.partition_role != self.partition_role
            ):
                _fail("research identity admission session decision lineage is invalid")
            record_ids.append(decision.record_id)
        if len(set(record_ids)) != len(record_ids):
            _fail("research identity admission session decisions are duplicated")

        if type(self.transitions) is not tuple or any(
            type(value) is not NseArchiveResearchIdentityTransition
            for value in self.transitions
        ):
            _fail("research identity admission session transitions are invalid")
        decisions_by_record_id = {
            decision.record_id: decision for decision in self.decisions
        }
        transition_ids: list[str] = []
        for transition in self.transitions:
            transition.verify_content_identity()
            if transition.current_market_session != self.market_session:
                _fail(
                    "research identity admission session transition current "
                    "session does not match its session"
                )
            current_decision = decisions_by_record_id.get(transition.current_record_id)
            if (
                current_decision is None
                or current_decision.admission_status not in _ADMITTED_STATUSES
                or current_decision.research_identity_id
                != transition.current_research_identity_id
                or current_decision.listing_key != transition.current_listing_key
                or current_decision.symbol != transition.current_symbol
            ):
                _fail(
                    "research identity admission session transition current "
                    "binding is invalid"
                )
            transition_ids.append(transition.transition_id)
        if len(set(transition_ids)) != len(transition_ids):
            _fail("research identity admission session transitions are duplicated")
        if tuple(transition_ids) != tuple(sorted(transition_ids)):
            _fail("research identity admission session transitions are not canonically ordered")

        counts = (
            self.admitted_validated_count,
            self.admitted_source_attested_count,
            self.blocked_unresolved_count,
            self.blocked_collision_count,
        )
        if any(type(value) is not int or value < 0 for value in counts):
            _fail("research identity admission session counts are invalid")
        expected_counts = (
            sum(
                1
                for value in self.decisions
                if value.admission_status
                is NseArchiveResearchIdentityAdmissionStatus.ADMITTED_VALIDATED
            ),
            sum(
                1
                for value in self.decisions
                if value.admission_status
                is NseArchiveResearchIdentityAdmissionStatus.ADMITTED_SOURCE_ATTESTED
            ),
            sum(
                1
                for value in self.decisions
                if value.admission_status
                is NseArchiveResearchIdentityAdmissionStatus.BLOCKED_UNRESOLVED
            ),
            sum(
                1
                for value in self.decisions
                if value.admission_status
                is NseArchiveResearchIdentityAdmissionStatus.BLOCKED_SAME_SESSION_ISIN_COLLISION
            ),
        )
        if counts != expected_counts:
            _fail("research identity admission session counts disagree with its decisions")
        if self.research_identity_admission_complete is not (
            self.blocked_unresolved_count == 0 and self.blocked_collision_count == 0
        ):
            _fail("research identity admission session completeness flag is invalid")
        if (
            self.production_identity_resolution_complete is not False
            or self.corporate_action_adjustment_complete is not False
            or self.collection_only is not True
            or self.actionable is not False
            or self.training_eligible is not False
            or self.feature_eligible is not False
            or self.label_eligible is not False
            or self.alert_eligible is not False
            or self.execution_eligible is not False
        ):
            _fail("research identity admission session safety posture is invalid")

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": RESEARCH_IDENTITY_ADMISSION_SESSION_SCHEMA_VERSION,
                "policy_version": RESEARCH_IDENTITY_ADMISSION_POLICY_VERSION,
                "dataset_id": self.dataset_id,
                "replay_session_id": self.replay_session_id,
                "session_snapshot_id": self.session_snapshot_id,
                "market_session": self.market_session,
                "partition_id": self.partition_id,
                "partition_role": self.partition_role,
                "decision_ids": tuple(value.decision_id for value in self.decisions),
                "transition_ids": tuple(value.transition_id for value in self.transitions),
                "admitted_validated_count": self.admitted_validated_count,
                "admitted_source_attested_count": self.admitted_source_attested_count,
                "blocked_unresolved_count": self.blocked_unresolved_count,
                "blocked_collision_count": self.blocked_collision_count,
                "research_identity_admission_complete": self.research_identity_admission_complete,
                "production_identity_resolution_complete": (
                    self.production_identity_resolution_complete
                ),
                "corporate_action_adjustment_complete": (
                    self.corporate_action_adjustment_complete
                ),
                "collection_only": self.collection_only,
                "actionable": self.actionable,
                "training_eligible": self.training_eligible,
                "feature_eligible": self.feature_eligible,
                "label_eligible": self.label_eligible,
                "alert_eligible": self.alert_eligible,
                "execution_eligible": self.execution_eligible,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.admission_session_id != self._calculated_id():
            _fail("research identity admission session identity failed")


@dataclass(frozen=True, slots=True)
class NseArchiveResearchPairedSession:
    """One immutable pairing of a verified replay session with its exact admission grade.

    Both nested objects are independently re-verified; the paired identity is
    derived only from the two already-verified nested content identities and
    the existing policy/schema constants -- never from raw bytes or object
    ``repr``.
    """

    replay_session: NseArchiveResearchReplaySession
    admission_session: NseArchiveResearchIdentityAdmissionSession
    paired_session_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "paired_session_id", self._calculated_id())

    def _validate(self) -> None:
        if type(self.replay_session) is not NseArchiveResearchReplaySession:
            _fail("research identity paired session replay session type is invalid")
        if type(self.admission_session) is not NseArchiveResearchIdentityAdmissionSession:
            _fail("research identity paired session admission session type is invalid")
        self.replay_session.verify_content_identity()
        self.admission_session.verify_content_identity()
        if (
            self.replay_session.dataset_id != self.admission_session.dataset_id
            or self.replay_session.replay_session_id
            != self.admission_session.replay_session_id
            or self.replay_session.session_snapshot_id
            != self.admission_session.session_snapshot_id
            or self.replay_session.market_session != self.admission_session.market_session
            or self.replay_session.partition_id != self.admission_session.partition_id
            or self.replay_session.partition_role != self.admission_session.partition_role
        ):
            _fail("research identity paired session lineage is invalid")

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": RESEARCH_IDENTITY_PAIRED_SESSION_SCHEMA_VERSION,
                "policy_version": RESEARCH_IDENTITY_ADMISSION_POLICY_VERSION,
                "replay_session_id": self.replay_session.replay_session_id,
                "admission_session_id": self.admission_session.admission_session_id,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.paired_session_id != self._calculated_id():
            _fail("research identity paired session identity failed")

    # Read-only, fixed fail-closed posture. These are not dataclass fields --
    # no per-instance state exists for them, and they never enter
    # paired_session_id, since the posture is a constant of this type, not a
    # verified fact about either nested object.
    @property
    def collection_only(self) -> bool:
        return True

    @property
    def actionable(self) -> bool:
        return False

    @property
    def training_eligible(self) -> bool:
        return False

    @property
    def feature_eligible(self) -> bool:
        return False

    @property
    def label_eligible(self) -> bool:
        return False

    @property
    def alert_eligible(self) -> bool:
        return False

    @property
    def execution_eligible(self) -> bool:
        return False

    @property
    def production_identity_resolution_complete(self) -> bool:
        return False

    @property
    def corporate_action_adjustment_complete(self) -> bool:
        return False


def _verify_admission_dataset_safety_posture(dataset: NseArchiveResearchDataset) -> None:
    if (
        dataset.collection_only is not True
        or dataset.actionable is not False
        or dataset.training_eligible is not False
        or dataset.feature_eligible is not False
        or dataset.label_eligible is not False
        or dataset.alert_eligible is not False
        or dataset.execution_eligible is not False
        or dataset.identity_resolution_complete is not False
        or dataset.corporate_action_adjustment_complete is not False
    ):
        _fail("research identity admission dataset safety posture is invalid")


# One prior-admitted observation: (research_identity_id or listing_key peer
# value, symbol, record_id, market_session).
_PriorObservation = tuple[str, str, str, date]


def _admission_candidate(
    record, claims_by_listing_key
) -> tuple[NseArchiveResearchIdentityBasis, str | None, str | None]:
    claim = claims_by_listing_key.get(record.listing_key)
    validated = (
        record.identity_status == "MATCHED_SAME_SESSION"
        and record.identity_matched is True
        and type(record.validated_isin) is str
        and record.validated_isin != ""
    )
    if claim is not None and validated:
        _fail(
            "research identity admission record carries both validated and "
            "legacy source-attested evidence"
        )
    if validated:
        if not _is_canonical_isin(record.validated_isin):
            _fail(
                "research identity admission validated evidence is not a "
                "canonical ISIN"
            )
        return (
            NseArchiveResearchIdentityBasis.VALIDATED_SAME_SESSION_ISIN,
            record.validated_isin,
            None,
        )
    if claim is not None:
        if not _is_canonical_isin(claim.claimed_isin):
            _fail(
                "research identity admission source-attested evidence is not "
                "a canonical ISIN"
            )
        return (
            NseArchiveResearchIdentityBasis.LEGACY_SOURCE_ATTESTED_ISIN,
            claim.claimed_isin,
            claim.claim_id,
        )
    return (NseArchiveResearchIdentityBasis.UNAVAILABLE, None, None)


def _build_admission_session_decisions_and_transitions(
    session: NseArchiveResearchReplaySession,
    latest_by_listing_key: dict[str, _PriorObservation],
    latest_by_identity: dict[str, _PriorObservation],
) -> tuple[
    tuple[NseArchiveResearchIdentityDecision, ...],
    tuple[NseArchiveResearchIdentityTransition, ...],
]:
    claims_by_listing_key = (
        {
            record.listing_key: claim
            for claim, record in zip(
                session.source_identity_claims, session.records, strict=True
            )
        }
        if session.source_identity_claims
        else {}
    )

    record_ids = tuple(record.record_id for record in session.records)
    if len(set(record_ids)) != len(record_ids):
        _fail("research identity admission session contains duplicate replay record ids")
    lanes = tuple((record.listing_key, record.series) for record in session.records)
    if len(set(lanes)) != len(lanes):
        _fail("research identity admission session contains duplicate replay lanes")

    candidates = tuple(
        (record, *_admission_candidate(record, claims_by_listing_key))
        for record in session.records
    )

    isin_listing_keys: dict[str, set[str]] = {}
    for record, basis, isin, _claim_id in candidates:
        if basis is not NseArchiveResearchIdentityBasis.UNAVAILABLE:
            isin_listing_keys.setdefault(isin, set()).add(record.listing_key)
    colliding_isins = {isin for isin, keys in isin_listing_keys.items() if len(keys) > 1}

    decisions: list[NseArchiveResearchIdentityDecision] = []
    for record, basis, isin, claim_id in candidates:
        if basis is NseArchiveResearchIdentityBasis.UNAVAILABLE:
            status = NseArchiveResearchIdentityAdmissionStatus.BLOCKED_UNRESOLVED
            research_identity_id = None
        elif isin in colliding_isins:
            status = NseArchiveResearchIdentityAdmissionStatus.BLOCKED_SAME_SESSION_ISIN_COLLISION
            research_identity_id = None
        else:
            status = (
                NseArchiveResearchIdentityAdmissionStatus.ADMITTED_VALIDATED
                if basis is NseArchiveResearchIdentityBasis.VALIDATED_SAME_SESSION_ISIN
                else NseArchiveResearchIdentityAdmissionStatus.ADMITTED_SOURCE_ATTESTED
            )
            research_identity_id = research_identity_id_for_isin(isin)
        decisions.append(
            NseArchiveResearchIdentityDecision(
                dataset_id=session.dataset_id,
                replay_session_id=session.replay_session_id,
                session_snapshot_id=session.session_snapshot_id,
                market_session=session.market_session,
                partition_id=session.partition_id,
                partition_role=session.partition_role,
                record_id=record.record_id,
                listing_key=record.listing_key,
                symbol=record.symbol,
                series=record.series,
                source_claim_id=claim_id,
                source_isin=isin,
                basis=basis,
                admission_status=status,
                research_identity_id=research_identity_id,
            )
        )

    transitions: list[NseArchiveResearchIdentityTransition] = []
    pending_listing: dict[str, NseArchiveResearchIdentityDecision] = {}
    pending_identity: dict[str, NseArchiveResearchIdentityDecision] = {}
    for decision in decisions:
        if decision.admission_status not in _ADMITTED_STATUSES:
            continue
        if decision.listing_key in pending_listing:
            _fail(
                "research identity admission session would update the same "
                "listing key twice"
            )
        if decision.research_identity_id in pending_identity:
            _fail(
                "research identity admission session would update the same "
                "research identity twice"
            )
        pending_listing[decision.listing_key] = decision
        pending_identity[decision.research_identity_id] = decision

        prior_listing = latest_by_listing_key.get(decision.listing_key)
        if prior_listing is not None:
            prior_identity_id, prior_symbol, prior_record_id, prior_session = prior_listing
            if prior_identity_id != decision.research_identity_id:
                transitions.append(
                    NseArchiveResearchIdentityTransition(
                        kind=NseArchiveResearchIdentityTransitionKind.LISTING_KEY_REBOUND,
                        previous_market_session=prior_session,
                        current_market_session=session.market_session,
                        previous_record_id=prior_record_id,
                        current_record_id=decision.record_id,
                        previous_research_identity_id=prior_identity_id,
                        current_research_identity_id=decision.research_identity_id,
                        previous_listing_key=decision.listing_key,
                        current_listing_key=decision.listing_key,
                        previous_symbol=prior_symbol,
                        current_symbol=decision.symbol,
                    )
                )

        prior_identity = latest_by_identity.get(decision.research_identity_id)
        if prior_identity is not None:
            prior_listing_key, prior_symbol, prior_record_id, prior_session = prior_identity
            if prior_symbol != decision.symbol:
                transitions.append(
                    NseArchiveResearchIdentityTransition(
                        kind=NseArchiveResearchIdentityTransitionKind.IDENTITY_SYMBOL_CHANGED,
                        previous_market_session=prior_session,
                        current_market_session=session.market_session,
                        previous_record_id=prior_record_id,
                        current_record_id=decision.record_id,
                        previous_research_identity_id=decision.research_identity_id,
                        current_research_identity_id=decision.research_identity_id,
                        previous_listing_key=prior_listing_key,
                        current_listing_key=decision.listing_key,
                        previous_symbol=prior_symbol,
                        current_symbol=decision.symbol,
                    )
                )

    transitions.sort(key=lambda value: value.transition_id)
    return tuple(decisions), tuple(transitions)


def _admission_counts(
    decisions: tuple[NseArchiveResearchIdentityDecision, ...]
) -> tuple[int, int, int, int]:
    return (
        sum(
            1
            for value in decisions
            if value.admission_status
            is NseArchiveResearchIdentityAdmissionStatus.ADMITTED_VALIDATED
        ),
        sum(
            1
            for value in decisions
            if value.admission_status
            is NseArchiveResearchIdentityAdmissionStatus.ADMITTED_SOURCE_ATTESTED
        ),
        sum(
            1
            for value in decisions
            if value.admission_status
            is NseArchiveResearchIdentityAdmissionStatus.BLOCKED_UNRESOLVED
        ),
        sum(
            1
            for value in decisions
            if value.admission_status
            is NseArchiveResearchIdentityAdmissionStatus.BLOCKED_SAME_SESSION_ISIN_COLLISION
        ),
    )


def _build_admission_session(
    session: NseArchiveResearchReplaySession,
    latest_by_listing_key: dict[str, _PriorObservation],
    latest_by_identity: dict[str, _PriorObservation],
) -> NseArchiveResearchIdentityAdmissionSession:
    if type(session) is not NseArchiveResearchReplaySession:
        _fail("research identity admission replay session type is invalid")
    session.verify_content_identity()
    decisions, transitions = _build_admission_session_decisions_and_transitions(
        session, latest_by_listing_key, latest_by_identity
    )
    (
        admitted_validated_count,
        admitted_source_attested_count,
        blocked_unresolved_count,
        blocked_collision_count,
    ) = _admission_counts(decisions)
    return NseArchiveResearchIdentityAdmissionSession(
        dataset_id=session.dataset_id,
        replay_session_id=session.replay_session_id,
        session_snapshot_id=session.session_snapshot_id,
        market_session=session.market_session,
        partition_id=session.partition_id,
        partition_role=session.partition_role,
        decisions=decisions,
        transitions=transitions,
        admitted_validated_count=admitted_validated_count,
        admitted_source_attested_count=admitted_source_attested_count,
        blocked_unresolved_count=blocked_unresolved_count,
        blocked_collision_count=blocked_collision_count,
    )


def _iter_paired_sessions(
    dataset: NseArchiveResearchDataset,
    reader: NseHistoricalArchiveSnapshotReader,
    *,
    yield_from_session: date | None = None,
) -> Iterator[NseArchiveResearchPairedSession]:
    latest_by_listing_key: dict[str, _PriorObservation] = {}
    latest_by_identity: dict[str, _PriorObservation] = {}
    replay_iterator = iter(iter_verified_nse_archive_research_sessions(dataset, reader))

    while True:
        advance_failed = False
        session: NseArchiveResearchReplaySession | None = None
        try:
            session = next(replay_iterator)
        except StopIteration:
            return
        except Exception:
            advance_failed = True
        if advance_failed:
            _fail("research identity admission replay session could not be obtained")

        build_failed = False
        admission_session: NseArchiveResearchIdentityAdmissionSession | None = None
        try:
            admission_session = _build_admission_session(
                session, latest_by_listing_key, latest_by_identity
            )
        except NseArchiveResearchIdentityError:
            raise
        except Exception:
            build_failed = True
        if build_failed or admission_session is None:
            _fail("research identity admission session could not be reconstructed")

        for decision in admission_session.decisions:
            if decision.admission_status in _ADMITTED_STATUSES:
                latest_by_listing_key[decision.listing_key] = (
                    decision.research_identity_id,
                    decision.symbol,
                    decision.record_id,
                    decision.market_session,
                )
                latest_by_identity[decision.research_identity_id] = (
                    decision.listing_key,
                    decision.symbol,
                    decision.record_id,
                    decision.market_session,
                )

        # Earlier sessions remain mandatory identity warm-up: their admitted
        # state and transitions have already been fully reconstructed and
        # verified above.  They do not need the additional paired-session and
        # price-stream object graph when a caller has pinned a later history
        # window boundary.
        if (
            yield_from_session is not None
            and session.market_session < yield_from_session
        ):
            continue

        pair_failed = False
        paired: NseArchiveResearchPairedSession | None = None
        try:
            paired = NseArchiveResearchPairedSession(
                replay_session=session, admission_session=admission_session
            )
        except NseArchiveResearchIdentityError:
            raise
        except Exception:
            pair_failed = True
        if pair_failed or paired is None:
            _fail("research identity paired session could not be reconstructed")

        yield paired


def iter_nse_archive_research_paired_sessions(
    dataset: NseArchiveResearchDataset,
    reader: NseHistoricalArchiveSnapshotReader,
) -> Iterator[NseArchiveResearchPairedSession]:
    """Pair one already sealed research dataset's replayed sessions with their admission grade.

    Calls only the public ``iter_verified_nse_archive_research_sessions``,
    in stored order, one session at a time, exactly once per invocation of
    this iterator. Only the session currently being paired, plus two bounded
    latest-observation dictionaries keyed by listing key and by research
    identity, are ever held in memory. This is the single-pass source both
    :func:`iter_nse_archive_research_identity_admission_sessions` and the
    price-stream module project from, so the multi-year corpus is never
    reopened or reparsed more than once per traversal. Stopping iteration
    early never advances past the next unread session and never constitutes
    a completed or publishable research artifact.
    """

    if type(dataset) is not NseArchiveResearchDataset:
        _fail("research identity admission dataset is invalid")
    if reader is None:
        _fail("research identity admission reader is invalid")

    dataset_identity_failed = False
    try:
        dataset.verify_content_identity()
    except Exception:
        dataset_identity_failed = True
    if dataset_identity_failed:
        _fail("research identity admission dataset identity failed")
    _verify_admission_dataset_safety_posture(dataset)

    return _iter_paired_sessions(dataset, reader)


def iter_nse_archive_research_paired_sessions_from(
    dataset: NseArchiveResearchDataset,
    reader: NseHistoricalArchiveSnapshotReader,
    *,
    start_session: date,
) -> Iterator[NseArchiveResearchPairedSession]:
    """Warm identity state from dataset start and yield pairs only from a boundary.

    Every earlier replay session is still consumed and admitted in stored
    order, so listing rebounds and identity-symbol changes remain point-in-
    time correct.  Only the redundant paired-session graph for pre-window
    warm-up sessions is omitted.
    """

    if type(start_session) is not date:
        _fail("research identity paired-session boundary is invalid")
    if type(dataset) is not NseArchiveResearchDataset or reader is None:
        _fail("research identity admission dataset or reader is invalid")
    if start_session not in dataset.accepted_sessions:
        _fail("research identity paired-session boundary is invalid")

    dataset_identity_failed = False
    try:
        dataset.verify_content_identity()
    except Exception:
        dataset_identity_failed = True
    if dataset_identity_failed:
        _fail("research identity admission dataset identity failed")
    _verify_admission_dataset_safety_posture(dataset)

    return _iter_paired_sessions(
        dataset,
        reader,
        yield_from_session=start_session,
    )


def _project_admission_sessions(
    paired_iterator: Iterator[NseArchiveResearchPairedSession],
) -> Iterator[NseArchiveResearchIdentityAdmissionSession]:
    for paired in paired_iterator:
        yield paired.admission_session


def iter_nse_archive_research_identity_admission_sessions(
    dataset: NseArchiveResearchDataset,
    reader: NseHistoricalArchiveSnapshotReader,
) -> Iterator[NseArchiveResearchIdentityAdmissionSession]:
    """Admit one already sealed research dataset's replayed sessions into research identities.

    Projects admission sessions from :func:`iter_nse_archive_research_paired_sessions`,
    so there is exactly one upstream replay traversal per invocation of this
    iterator -- calling this function never triggers a second, independent
    pass over the archive. Only the session currently being admitted, plus
    two bounded latest-observation dictionaries keyed by listing key and by
    research identity, are ever held in memory. Stopping iteration early
    never advances past the next unread session and never constitutes a
    completed or publishable research artifact.
    """

    paired_iterator = iter_nse_archive_research_paired_sessions(dataset, reader)
    return _project_admission_sessions(paired_iterator)
