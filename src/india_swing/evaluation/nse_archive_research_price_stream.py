"""NSE archive research-only, identity-bound raw price stream.

Streams one already paired
:class:`~india_swing.evaluation.nse_archive_research_identity.NseArchiveResearchPairedSession`
at a time -- obtained exclusively through the existing public
``iter_nse_archive_research_paired_sessions`` seam -- into an immutable
per-session stream of price observations, each binding exactly one verified
replay record to its exact identity-admission decision without copying or
recalculating any raw market field.

This is a bridge into a future cutoff-aware corporate-action adjustment and
feature stage. It is not itself valid backtest or model input: prices
remain ``RAW_UNADJUSTED`` and untouched, no return/feature/label/signal is
computed, and every observation retains ``collection_only=True`` with every
actionable/training/feature/label/alert/execution flag false. Production
identity resolution and corporate-action adjustment remain false.

Every replay record in a session is retained as exactly one observation --
``BLOCKED_UNRESOLVED`` and ``BLOCKED_SAME_SESSION_ISIN_COLLISION`` rows stay
present with ``research_identity_id=None``; none are ever silently dropped,
reordered, duplicated, or substituted. Admission transitions are carried
through byte-for-byte, exactly as the identity-admission layer emitted
them -- this module never recomputes or reinterprets them.

This module never reads the filesystem, network, environment, or clock;
never constructs a store; never lists, discovers, or selects a "latest"
artifact; and never reopens or reparses the archive independently of the
one paired-session traversal it projects from.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterator

from india_swing.identity import content_id

from .nse_archive_research_dataset import NseArchiveResearchDataset
from .nse_archive_research_identity import (
    NseArchiveResearchIdentityDecision,
    NseArchiveResearchIdentityTransition,
    NseArchiveResearchPairedSession,
    iter_nse_archive_research_paired_sessions,
)
from .nse_archive_research_replay import (
    NseArchiveResearchReplayRecord,
    NseHistoricalArchiveSnapshotReader,
)


PRICE_STREAM_POLICY_VERSION = "nse-archive-research-price-stream/positive-only-v1"
PRICE_STREAM_OBSERVATION_SCHEMA_VERSION = "nse-archive-research-price-observation/v1"
PRICE_STREAM_SESSION_SCHEMA_VERSION = "nse-archive-research-price-stream-session/v1"


class NseArchiveResearchPriceStreamError(ValueError):
    """A price-stream input, capability, or reconstructed artifact failed a static safety rule."""


def _fail(message: str) -> None:
    raise NseArchiveResearchPriceStreamError(message)


@dataclass(frozen=True, slots=True)
class NseArchiveResearchPriceObservation:
    """One immutable binding of exactly one verified replay record to its identity decision.

    Raw OHLCV/delivery fields live only on ``replay_record`` and are never
    copied or recalculated here; this type only asserts the two nested
    values are independently valid and refer to the exact same record.
    """

    replay_record: NseArchiveResearchReplayRecord
    identity_decision: NseArchiveResearchIdentityDecision
    observation_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "observation_id", self._calculated_id())

    def _validate(self) -> None:
        if type(self.replay_record) is not NseArchiveResearchReplayRecord:
            _fail("research price observation replay record type is invalid")
        if type(self.identity_decision) is not NseArchiveResearchIdentityDecision:
            _fail("research price observation identity decision type is invalid")
        self.replay_record.verify_content_identity()
        self.identity_decision.verify_content_identity()
        if (
            self.replay_record.record_id != self.identity_decision.record_id
            or self.replay_record.session != self.identity_decision.market_session
            or self.replay_record.listing_key != self.identity_decision.listing_key
            or self.replay_record.symbol != self.identity_decision.symbol
            or self.replay_record.series != self.identity_decision.series
        ):
            _fail("research price observation lineage is invalid")

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": PRICE_STREAM_OBSERVATION_SCHEMA_VERSION,
                "policy_version": PRICE_STREAM_POLICY_VERSION,
                "record_id": self.replay_record.record_id,
                "decision_id": self.identity_decision.decision_id,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.observation_id != self._calculated_id():
            _fail("research price observation identity failed")

    @property
    def market_session(self):
        return self.identity_decision.market_session

    @property
    def listing_key(self) -> str:
        return self.identity_decision.listing_key

    @property
    def symbol(self) -> str:
        return self.identity_decision.symbol

    @property
    def series(self) -> str:
        return self.identity_decision.series

    @property
    def research_identity_id(self) -> str | None:
        return self.identity_decision.research_identity_id

    @property
    def basis(self):
        return self.identity_decision.basis

    @property
    def admission_status(self):
        return self.identity_decision.admission_status

    # Read-only, fixed fail-closed posture. These are not dataclass fields --
    # no per-instance state exists for them, and they never enter
    # observation_id, since the posture is a constant of this type, not a
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


@dataclass(frozen=True, slots=True)
class NseArchiveResearchPriceStreamSession:
    """One immutable session pairing raw prices with their exact identity admission.

    Independently proves a one-to-one, order-preserving bijection between
    the paired session's replay records, its admission decisions, and this
    session's own observations: no missing, duplicate, reordered, orphaned,
    or substituted record can pass construction. Transitions are retained
    exactly as the admission layer emitted them.
    """

    paired_session: NseArchiveResearchPairedSession
    observations: tuple[NseArchiveResearchPriceObservation, ...]
    transitions: tuple[NseArchiveResearchIdentityTransition, ...]
    collection_only: bool = field(init=False)
    actionable: bool = field(init=False)
    training_eligible: bool = field(init=False)
    feature_eligible: bool = field(init=False)
    label_eligible: bool = field(init=False)
    alert_eligible: bool = field(init=False)
    execution_eligible: bool = field(init=False)
    production_identity_resolution_complete: bool = field(init=False)
    corporate_action_adjustment_complete: bool = field(init=False)
    price_stream_session_id: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "collection_only", True)
        object.__setattr__(self, "actionable", False)
        object.__setattr__(self, "training_eligible", False)
        object.__setattr__(self, "feature_eligible", False)
        object.__setattr__(self, "label_eligible", False)
        object.__setattr__(self, "alert_eligible", False)
        object.__setattr__(self, "execution_eligible", False)
        object.__setattr__(self, "production_identity_resolution_complete", False)
        object.__setattr__(self, "corporate_action_adjustment_complete", False)
        self._validate()
        object.__setattr__(self, "price_stream_session_id", self._calculated_id())

    def _validate(self) -> None:
        if type(self.paired_session) is not NseArchiveResearchPairedSession:
            _fail("research price stream session paired session type is invalid")
        self.paired_session.verify_content_identity()
        replay_session = self.paired_session.replay_session
        admission_session = self.paired_session.admission_session

        if type(self.observations) is not tuple or any(
            type(value) is not NseArchiveResearchPriceObservation for value in self.observations
        ):
            _fail("research price stream session observations are invalid")
        if len(self.observations) != len(replay_session.records) or len(
            self.observations
        ) != len(admission_session.decisions):
            _fail("research price stream session observation bijection is invalid")

        observation_ids: list[str] = []
        for observation, record, decision in zip(
            self.observations, replay_session.records, admission_session.decisions, strict=True
        ):
            observation.verify_content_identity()
            if (
                observation.replay_record.record_id != record.record_id
                or observation.identity_decision.decision_id != decision.decision_id
            ):
                _fail("research price stream session observation bijection is invalid")
            observation_ids.append(observation.observation_id)
        if len(set(observation_ids)) != len(observation_ids):
            _fail("research price stream session observations are duplicated")

        if type(self.transitions) is not tuple or any(
            type(value) is not NseArchiveResearchIdentityTransition for value in self.transitions
        ):
            _fail("research price stream session transitions are invalid")
        for transition in self.transitions:
            transition.verify_content_identity()
        if tuple(value.transition_id for value in self.transitions) != tuple(
            value.transition_id for value in admission_session.transitions
        ):
            _fail("research price stream session transitions disagree with its admission")

        if (
            self.collection_only is not True
            or self.actionable is not False
            or self.training_eligible is not False
            or self.feature_eligible is not False
            or self.label_eligible is not False
            or self.alert_eligible is not False
            or self.execution_eligible is not False
            or self.production_identity_resolution_complete is not False
            or self.corporate_action_adjustment_complete is not False
        ):
            _fail("research price stream session safety posture is invalid")

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": PRICE_STREAM_SESSION_SCHEMA_VERSION,
                "policy_version": PRICE_STREAM_POLICY_VERSION,
                "paired_session_id": self.paired_session.paired_session_id,
                "observation_ids": tuple(value.observation_id for value in self.observations),
                "transition_ids": tuple(value.transition_id for value in self.transitions),
                "collection_only": self.collection_only,
                "actionable": self.actionable,
                "training_eligible": self.training_eligible,
                "feature_eligible": self.feature_eligible,
                "label_eligible": self.label_eligible,
                "alert_eligible": self.alert_eligible,
                "execution_eligible": self.execution_eligible,
                "production_identity_resolution_complete": (
                    self.production_identity_resolution_complete
                ),
                "corporate_action_adjustment_complete": (
                    self.corporate_action_adjustment_complete
                ),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.price_stream_session_id != self._calculated_id():
            _fail("research price stream session identity failed")


def _build_price_stream_session(
    paired: NseArchiveResearchPairedSession,
) -> NseArchiveResearchPriceStreamSession:
    if type(paired) is not NseArchiveResearchPairedSession:
        _fail("research price stream paired session type is invalid")
    observations = tuple(
        NseArchiveResearchPriceObservation(replay_record=record, identity_decision=decision)
        for record, decision in zip(
            paired.replay_session.records,
            paired.admission_session.decisions,
            strict=True,
        )
    )
    return NseArchiveResearchPriceStreamSession(
        paired_session=paired,
        observations=observations,
        transitions=paired.admission_session.transitions,
    )


def _iter_price_stream_sessions(
    paired_iterator: Iterator[NseArchiveResearchPairedSession],
) -> Iterator[NseArchiveResearchPriceStreamSession]:
    iterator = iter(paired_iterator)
    while True:
        advance_failed = False
        paired: NseArchiveResearchPairedSession | None = None
        try:
            paired = next(iterator)
        except StopIteration:
            return
        except Exception:
            advance_failed = True
        if advance_failed:
            _fail("research price stream paired session could not be obtained")

        build_failed = False
        session_obj: NseArchiveResearchPriceStreamSession | None = None
        try:
            session_obj = _build_price_stream_session(paired)
        except NseArchiveResearchPriceStreamError:
            raise
        except Exception:
            build_failed = True
        if build_failed or session_obj is None:
            _fail("research price stream session could not be reconstructed")

        yield session_obj


def iter_nse_archive_research_price_stream_sessions(
    dataset: NseArchiveResearchDataset,
    reader: NseHistoricalArchiveSnapshotReader,
) -> Iterator[NseArchiveResearchPriceStreamSession]:
    """Stream one paired replay/admission session's raw price observations at a time.

    Calls only the public ``iter_nse_archive_research_paired_sessions``, in
    stored order, one session at a time -- never reopens or reparses the
    archive independently, and keeps no corpus-sized collection. Foreign or
    tampered nested failures (an invalid dataset/reader, or a corrupted
    paired session) cross this function's public boundary only as one
    static sanitized ``NseArchiveResearchPriceStreamError`` with no cause or
    context leakage.
    """

    paired_call_failed = False
    paired_iterator: Iterator[NseArchiveResearchPairedSession] | None = None
    try:
        paired_iterator = iter_nse_archive_research_paired_sessions(dataset, reader)
    except Exception:
        paired_call_failed = True
    if paired_call_failed or paired_iterator is None:
        _fail("research price stream dataset or reader is invalid")

    return _iter_price_stream_sessions(paired_iterator)
