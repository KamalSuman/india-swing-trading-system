"""Forward-paper raw current-cross-section history-window bridge.

Converts one explicitly pinned, cutoff-safe sequence of
:class:`~india_swing.evaluation.nse_archive_research_price_stream.NseArchiveResearchPriceStreamSession`
values -- obtained exclusively through the existing public
``iter_nse_archive_research_price_stream_sessions`` seam -- into one
immutable :class:`ForwardPaperRawHistoryWindow`: every subject present on
the final "signal session" becomes either a complete 60-session raw-history
candidate or an explicit, fixed-reason veto.

This is a bridge into a later cutoff-aware corporate-action/tick-size
adjustment and deterministic-feature stage. It is not itself valid model,
backtest, signal, alert, paper-trade, or execution input: every output type
retains ``collection_only=True`` with every training/feature/label/
ranking/alert/paper-trade/notification/execution flag false, and
``production_identity_resolution_complete``/``corporate_action_adjustment_complete``
remain false. Prices, delivery, surveillance, identity, and transition
fields are never copied or recalculated -- only the already-verified nested
:class:`NseArchiveResearchPriceObservation` objects are retained by
reference.

Current-universe membership is defined solely by the final signal session;
a subject missing, duplicated, or unresolved in any one of the pinned 60
sessions never silently shrinks the cross-section -- it becomes an explicit
veto with a fixed enum reason and exact lineage IDs instead of disappearing.

This module never reads the filesystem, network, environment, or clock;
never constructs a store; never lists, discovers, or selects a "latest"
artifact; and never reopens or reparses the price-stream layer independently
of the one caller-supplied iterator it consumes exactly once.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from enum import Enum
from typing import Iterator, Union

from india_swing.identity import content_id

from india_swing.evaluation.nse_archive_research_price_stream import (
    NseArchiveResearchPriceObservation,
    NseArchiveResearchPriceStreamSession,
)


FORWARD_PAPER_HISTORY_POLICY_VERSION = (
    "forward-paper-raw-history-window/positive-only-v1"
)
FORWARD_PAPER_HISTORY_WINDOW_SPEC_SCHEMA_VERSION = (
    "forward-paper-history-window-spec/v1"
)
FORWARD_PAPER_HISTORY_CANDIDATE_SCHEMA_VERSION = "forward-paper-history-candidate/v1"
FORWARD_PAPER_HISTORY_VETO_SCHEMA_VERSION = "forward-paper-history-veto/v2"
FORWARD_PAPER_RAW_HISTORY_WINDOW_SCHEMA_VERSION = "forward-paper-raw-history-window/v2"

# The pinned window is exactly 60 sessions: the signal session plus its 59
# preceding required sessions.
FORWARD_PAPER_HISTORY_WINDOW_SESSION_COUNT = 60

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")


def _is_sha256(value: object) -> bool:
    return type(value) is str and _SHA256_PATTERN.fullmatch(value) is not None


class ForwardPaperHistoryError(ValueError):
    """A forward-paper history input, capability, or reconstructed artifact failed a static safety rule."""


def _fail(message: str) -> None:
    raise ForwardPaperHistoryError(message)


class ForwardPaperHistoryVetoReason(Enum):
    SIGNAL_IDENTITY_UNRESOLVED = "SIGNAL_IDENTITY_UNRESOLVED"
    REQUIRED_SESSION_MISSING = "REQUIRED_SESSION_MISSING"
    REQUIRED_SESSION_DUPLICATED = "REQUIRED_SESSION_DUPLICATED"


@dataclass(frozen=True, slots=True)
class ForwardPaperHistoryWindowSpec:
    """One immutable pin of a dataset, signal session, decision cutoff, and required-session set."""

    dataset_id: str
    signal_session: date
    decision_cutoff: datetime
    expected_market_sessions: tuple[date, ...]
    spec_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "spec_id", self._calculated_id())

    def _validate(self) -> None:
        if not _is_sha256(self.dataset_id):
            _fail("forward paper history window spec dataset id is invalid")
        if type(self.signal_session) is not date:
            _fail("forward paper history window spec signal session is invalid")
        if (
            type(self.decision_cutoff) is not datetime
            or self.decision_cutoff.tzinfo is None
            or self.decision_cutoff.utcoffset() is None
            or self.decision_cutoff.utcoffset() != timedelta(0)
        ):
            _fail("forward paper history window spec decision cutoff is invalid")
        if type(self.expected_market_sessions) is not tuple or any(
            type(value) is not date for value in self.expected_market_sessions
        ):
            _fail("forward paper history window spec expected sessions are invalid")
        if len(self.expected_market_sessions) != FORWARD_PAPER_HISTORY_WINDOW_SESSION_COUNT:
            _fail("forward paper history window spec expected session count is invalid")
        if len(set(self.expected_market_sessions)) != len(self.expected_market_sessions):
            _fail("forward paper history window spec expected sessions are duplicated")
        if tuple(self.expected_market_sessions) != tuple(
            sorted(self.expected_market_sessions)
        ):
            _fail(
                "forward paper history window spec expected sessions must be strictly "
                "increasing"
            )
        if self.expected_market_sessions[-1] != self.signal_session:
            _fail(
                "forward paper history window spec expected sessions must end on the "
                "signal session"
            )

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": FORWARD_PAPER_HISTORY_WINDOW_SPEC_SCHEMA_VERSION,
                "policy_version": FORWARD_PAPER_HISTORY_POLICY_VERSION,
                "dataset_id": self.dataset_id,
                "signal_session": self.signal_session,
                "decision_cutoff": self.decision_cutoff,
                "expected_market_sessions": self.expected_market_sessions,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.spec_id != self._calculated_id():
            _fail("forward paper history window spec identity failed")

    # Read-only, fixed fail-closed posture. Not dataclass fields -- no
    # per-instance state exists for them, and they never enter spec_id.
    @property
    def collection_only(self) -> bool:
        return True

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
    def ranking_eligible(self) -> bool:
        return False

    @property
    def alert_eligible(self) -> bool:
        return False

    @property
    def paper_trade_eligible(self) -> bool:
        return False

    @property
    def notification_eligible(self) -> bool:
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
class ForwardPaperHistoryCandidate:
    """One immutable, complete 60-session raw-history candidate for a signal-session subject.

    ``history_observations`` retains exactly one
    :class:`NseArchiveResearchPriceObservation` per pinned expected session,
    in expected-session order, all sharing the same ``research_identity_id``
    -- the last entry is the signal session's own observation.
    """

    spec_id: str
    research_identity_id: str
    history_observations: tuple[NseArchiveResearchPriceObservation, ...]
    candidate_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "candidate_id", self._calculated_id())

    def _validate(self) -> None:
        if not _is_sha256(self.spec_id):
            _fail("forward paper history candidate spec id is invalid")
        if not _is_sha256(self.research_identity_id):
            _fail("forward paper history candidate research identity id is invalid")
        if type(self.history_observations) is not tuple or len(
            self.history_observations
        ) != FORWARD_PAPER_HISTORY_WINDOW_SESSION_COUNT:
            _fail("forward paper history candidate history observations are invalid")

        observation_ids: list[str] = []
        sessions: list[date] = []
        for observation in self.history_observations:
            if type(observation) is not NseArchiveResearchPriceObservation:
                _fail("forward paper history candidate observation type is invalid")
            observation.verify_content_identity()
            if observation.research_identity_id != self.research_identity_id:
                _fail("forward paper history candidate observation identity is invalid")
            observation_ids.append(observation.observation_id)
            sessions.append(observation.market_session)
        if len(set(observation_ids)) != len(observation_ids):
            _fail("forward paper history candidate observations are duplicated")
        if tuple(sessions) != tuple(sorted(sessions)) or len(set(sessions)) != len(sessions):
            _fail(
                "forward paper history candidate observation sessions must be "
                "strictly increasing"
            )

    @property
    def signal_observation(self) -> NseArchiveResearchPriceObservation:
        return self.history_observations[-1]

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": FORWARD_PAPER_HISTORY_CANDIDATE_SCHEMA_VERSION,
                "policy_version": FORWARD_PAPER_HISTORY_POLICY_VERSION,
                "spec_id": self.spec_id,
                "research_identity_id": self.research_identity_id,
                "observation_ids": tuple(
                    value.observation_id for value in self.history_observations
                ),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.candidate_id != self._calculated_id():
            _fail("forward paper history candidate identity failed")

    # Read-only, fixed fail-closed posture. Not dataclass fields -- no
    # per-instance state exists for them, and they never enter candidate_id.
    @property
    def collection_only(self) -> bool:
        return True

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
    def ranking_eligible(self) -> bool:
        return False

    @property
    def alert_eligible(self) -> bool:
        return False

    @property
    def paper_trade_eligible(self) -> bool:
        return False

    @property
    def notification_eligible(self) -> bool:
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
class ForwardPaperHistoryVeto:
    """One immutable, explicit rejection of a signal-session subject from raw-history candidacy.

    ``evidence_session_ids``/``evidence_observation_ids`` are the exact,
    fixed-shape, canonical audit trail for ``reason`` -- never free text.
    For ``SIGNAL_IDENTITY_UNRESOLVED`` they name only the signal session's
    own ``price_stream_session_id`` (observation evidence is empty, since
    ``signal_observation`` already carries it). For
    ``REQUIRED_SESSION_MISSING`` they name every affected required
    session's ``price_stream_session_id``, in expected-session order, with
    empty observation evidence. For ``REQUIRED_SESSION_DUPLICATED`` they
    name every affected required session's ``price_stream_session_id`` in
    expected-session order, plus every duplicate observation's
    ``observation_id`` in expected-session/stored-observation order. This
    type validates shape and internal consistency only; cross-checking
    these IDs against the actual retained sessions/observations is the
    aggregate :class:`ForwardPaperRawHistoryWindow`'s own responsibility.
    """

    spec_id: str
    research_identity_id: str | None
    signal_observation: NseArchiveResearchPriceObservation
    reason: ForwardPaperHistoryVetoReason
    evidence_session_ids: tuple[str, ...]
    evidence_observation_ids: tuple[str, ...]
    veto_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "veto_id", self._calculated_id())

    def _validate(self) -> None:
        if not _is_sha256(self.spec_id):
            _fail("forward paper history veto spec id is invalid")
        if type(self.signal_observation) is not NseArchiveResearchPriceObservation:
            _fail("forward paper history veto signal observation type is invalid")
        self.signal_observation.verify_content_identity()
        if type(self.reason) is not ForwardPaperHistoryVetoReason:
            _fail("forward paper history veto reason is invalid")
        if type(self.evidence_session_ids) is not tuple or any(
            not _is_sha256(value) for value in self.evidence_session_ids
        ):
            _fail("forward paper history veto evidence session ids are invalid")
        if len(set(self.evidence_session_ids)) != len(self.evidence_session_ids):
            _fail("forward paper history veto evidence session ids are duplicated")
        if type(self.evidence_observation_ids) is not tuple or any(
            not _is_sha256(value) for value in self.evidence_observation_ids
        ):
            _fail("forward paper history veto evidence observation ids are invalid")

        if self.reason is ForwardPaperHistoryVetoReason.SIGNAL_IDENTITY_UNRESOLVED:
            if self.research_identity_id is not None:
                _fail("forward paper history veto research identity id is invalid")
            if self.signal_observation.research_identity_id is not None:
                _fail("forward paper history veto signal observation identity is invalid")
            if len(self.evidence_session_ids) != 1 or self.evidence_observation_ids != ():
                _fail("forward paper history veto unresolved evidence shape is invalid")
        else:
            if not _is_sha256(self.research_identity_id):
                _fail("forward paper history veto research identity id is invalid")
            if self.signal_observation.research_identity_id != self.research_identity_id:
                _fail("forward paper history veto signal observation identity is invalid")
            if len(self.evidence_session_ids) == 0:
                _fail("forward paper history veto evidence session ids are invalid")
            if self.reason is ForwardPaperHistoryVetoReason.REQUIRED_SESSION_MISSING:
                if self.evidence_observation_ids != ():
                    _fail("forward paper history veto missing evidence shape is invalid")
            else:
                if len(self.evidence_observation_ids) < len(self.evidence_session_ids):
                    _fail("forward paper history veto duplicated evidence shape is invalid")
                if len(set(self.evidence_observation_ids)) != len(
                    self.evidence_observation_ids
                ):
                    _fail("forward paper history veto duplicated evidence observations are invalid")

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": FORWARD_PAPER_HISTORY_VETO_SCHEMA_VERSION,
                "policy_version": FORWARD_PAPER_HISTORY_POLICY_VERSION,
                "spec_id": self.spec_id,
                "research_identity_id": self.research_identity_id,
                "signal_observation_id": self.signal_observation.observation_id,
                "reason": self.reason,
                "evidence_session_ids": self.evidence_session_ids,
                "evidence_observation_ids": self.evidence_observation_ids,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.veto_id != self._calculated_id():
            _fail("forward paper history veto identity failed")

    # Read-only, fixed fail-closed posture. Not dataclass fields -- no
    # per-instance state exists for them, and they never enter veto_id.
    @property
    def collection_only(self) -> bool:
        return True

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
    def ranking_eligible(self) -> bool:
        return False

    @property
    def alert_eligible(self) -> bool:
        return False

    @property
    def paper_trade_eligible(self) -> bool:
        return False

    @property
    def notification_eligible(self) -> bool:
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


ForwardPaperHistoryOutcome = Union[ForwardPaperHistoryCandidate, ForwardPaperHistoryVeto]


@dataclass(frozen=True, slots=True)
class ForwardPaperRawHistoryWindow:
    """One immutable, complete current-cross-section raw-history manifest for one spec.

    ``sessions`` retains, by reference, the exact ordered tuple of all 60
    consumed :class:`NseArchiveResearchPriceStreamSession` values -- one per
    ``spec.expected_market_sessions`` entry, in that same order. The signal
    session is always ``sessions[-1]``, exposed via the ``signal_session``
    property. ``outcomes`` covers every observation of the signal session in
    its stored order, one-to-one: each signal-session row becomes exactly
    one :class:`ForwardPaperHistoryCandidate` or
    :class:`ForwardPaperHistoryVeto`, never both, never neither. Every
    candidate observation and every veto evidence ID is independently
    cross-checked against the exact retained session it claims to come
    from -- never merely another self-consistent object with a matching
    date/identity.
    """

    spec: ForwardPaperHistoryWindowSpec
    sessions: tuple[NseArchiveResearchPriceStreamSession, ...]
    outcomes: tuple[ForwardPaperHistoryOutcome, ...]
    expected_session_count: int
    consumed_session_count: int
    signal_subject_count: int
    complete_candidate_count: int
    veto_count: int
    collection_only: bool = field(init=False)
    training_eligible: bool = field(init=False)
    feature_eligible: bool = field(init=False)
    label_eligible: bool = field(init=False)
    ranking_eligible: bool = field(init=False)
    alert_eligible: bool = field(init=False)
    paper_trade_eligible: bool = field(init=False)
    notification_eligible: bool = field(init=False)
    execution_eligible: bool = field(init=False)
    production_identity_resolution_complete: bool = field(init=False)
    corporate_action_adjustment_complete: bool = field(init=False)
    window_id: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "collection_only", True)
        object.__setattr__(self, "training_eligible", False)
        object.__setattr__(self, "feature_eligible", False)
        object.__setattr__(self, "label_eligible", False)
        object.__setattr__(self, "ranking_eligible", False)
        object.__setattr__(self, "alert_eligible", False)
        object.__setattr__(self, "paper_trade_eligible", False)
        object.__setattr__(self, "notification_eligible", False)
        object.__setattr__(self, "execution_eligible", False)
        object.__setattr__(self, "production_identity_resolution_complete", False)
        object.__setattr__(self, "corporate_action_adjustment_complete", False)
        self._validate()
        object.__setattr__(self, "window_id", self._calculated_id())

    def _validate(self) -> None:
        if type(self.spec) is not ForwardPaperHistoryWindowSpec:
            _fail("forward paper raw history window spec type is invalid")
        self.spec.verify_content_identity()

        expected_sessions = self.spec.expected_market_sessions
        if type(self.sessions) is not tuple or len(self.sessions) != len(expected_sessions):
            _fail("forward paper raw history window sessions are invalid")

        session_ids: list[str] = []
        session_indexes: list[_SessionIdentityIndex] = []
        for expected_date, session in zip(expected_sessions, self.sessions, strict=True):
            if type(session) is not NseArchiveResearchPriceStreamSession:
                _fail("forward paper raw history window session type is invalid")
            verify_failed = False
            try:
                session.verify_content_identity()
            except Exception:
                verify_failed = True
            if verify_failed:
                _fail("forward paper raw history window session failed verification")
            replay_session = session.paired_session.replay_session
            if replay_session.dataset_id != self.spec.dataset_id:
                _fail("forward paper raw history window session dataset is invalid")
            if replay_session.market_session != expected_date:
                _fail("forward paper raw history window session date is invalid")
            if replay_session.observed_at > self.spec.decision_cutoff:
                _fail(
                    "forward paper raw history window session was observed after its "
                    "decision cutoff"
                )
            session_ids.append(session.price_stream_session_id)
            session_indexes.append(_build_session_identity_index(session))
        if len(set(session_ids)) != len(session_ids):
            _fail("forward paper raw history window sessions are duplicated")

        signal_session = self.sessions[-1]
        if signal_session.paired_session.replay_session.market_session != (
            self.spec.signal_session
        ):
            _fail("forward paper raw history window signal session date is invalid")

        signal_observations = signal_session.observations
        if type(self.outcomes) is not tuple or len(self.outcomes) != len(
            signal_observations
        ):
            _fail("forward paper raw history window outcomes are invalid")

        candidate_count = 0
        veto_count = 0
        for outcome, signal_observation in zip(
            self.outcomes, signal_observations, strict=True
        ):
            if type(outcome) is ForwardPaperHistoryCandidate:
                outcome.verify_content_identity()
                if outcome.spec_id != self.spec.spec_id:
                    _fail("forward paper raw history window candidate spec lineage is invalid")
                if outcome.signal_observation.observation_id != signal_observation.observation_id:
                    _fail(
                        "forward paper raw history window candidate is out of order "
                        "with its signal session"
                    )
                if tuple(
                    value.market_session for value in outcome.history_observations
                ) != expected_sessions:
                    _fail(
                        "forward paper raw history window candidate history sessions "
                        "disagree with its spec"
                    )
                for session_index, observation in zip(
                    session_indexes, outcome.history_observations, strict=True
                ):
                    retained_matches = session_index.get(outcome.research_identity_id, ())
                    if (
                        len(retained_matches) != 1
                        or retained_matches[0].observation_id != observation.observation_id
                    ):
                        _fail(
                            "forward paper raw history window candidate observation is "
                            "not the retained observation"
                        )
                candidate_count += 1
            elif type(outcome) is ForwardPaperHistoryVeto:
                outcome.verify_content_identity()
                if outcome.spec_id != self.spec.spec_id:
                    _fail("forward paper raw history window veto spec lineage is invalid")
                if outcome.signal_observation.observation_id != signal_observation.observation_id:
                    _fail(
                        "forward paper raw history window veto is out of order with "
                        "its signal session"
                    )
                self._verify_veto_evidence(outcome, session_ids, session_indexes)
                veto_count += 1
            else:
                _fail("forward paper raw history window outcome type is invalid")

        counts = (
            self.expected_session_count,
            self.consumed_session_count,
            self.signal_subject_count,
            self.complete_candidate_count,
            self.veto_count,
        )
        if any(type(value) is not int or isinstance(value, bool) or value < 0 for value in counts):
            _fail("forward paper raw history window counts are invalid")
        expected_counts = (
            len(expected_sessions),
            len(self.sessions),
            len(signal_observations),
            candidate_count,
            veto_count,
        )
        if counts != expected_counts:
            _fail("forward paper raw history window counts disagree with its outcomes")

        if (
            self.collection_only is not True
            or self.training_eligible is not False
            or self.feature_eligible is not False
            or self.label_eligible is not False
            or self.ranking_eligible is not False
            or self.alert_eligible is not False
            or self.paper_trade_eligible is not False
            or self.notification_eligible is not False
            or self.execution_eligible is not False
            or self.production_identity_resolution_complete is not False
            or self.corporate_action_adjustment_complete is not False
        ):
            _fail("forward paper raw history window safety posture is invalid")

    def _verify_veto_evidence(
        self,
        veto: ForwardPaperHistoryVeto,
        session_ids: list[str],
        session_indexes: list[_SessionIdentityIndex],
    ) -> None:
        """Independently re-derive the complete expected evidence and require exact equality.

        Consults every one of the 60 retained sessions' identity indexes --
        never only the sessions the veto itself names -- so a veto cannot
        pass by supplying a merely valid subset of a multi-session anomaly.
        An omission, extra, reorder, duplicate, or unrelated ID all fail the
        same exact tuple-equality check.
        """

        if veto.reason is ForwardPaperHistoryVetoReason.SIGNAL_IDENTITY_UNRESOLVED:
            if veto.evidence_session_ids != (session_ids[-1],):
                _fail(
                    "forward paper raw history window unresolved veto evidence "
                    "disagrees with its retained signal session"
                )
            return

        expected_missing_session_ids: list[str] = []
        expected_duplicated_session_ids: list[str] = []
        expected_duplicated_observation_ids: list[str] = []
        for index, session_index in enumerate(session_indexes):
            retained_matches = session_index.get(veto.research_identity_id, ())
            if len(retained_matches) == 0:
                expected_missing_session_ids.append(session_ids[index])
            elif len(retained_matches) > 1:
                expected_duplicated_session_ids.append(session_ids[index])
                expected_duplicated_observation_ids.extend(
                    value.observation_id for value in retained_matches
                )

        if veto.reason is ForwardPaperHistoryVetoReason.REQUIRED_SESSION_MISSING:
            if (
                tuple(expected_missing_session_ids) != veto.evidence_session_ids
                or veto.evidence_observation_ids != ()
            ):
                _fail(
                    "forward paper raw history window missing veto evidence disagrees "
                    "with its retained sessions"
                )
        else:
            if (
                tuple(expected_duplicated_session_ids) != veto.evidence_session_ids
                or tuple(expected_duplicated_observation_ids)
                != veto.evidence_observation_ids
            ):
                _fail(
                    "forward paper raw history window duplicated veto evidence "
                    "disagrees with its retained sessions and observations"
                )

    @property
    def signal_session(self) -> NseArchiveResearchPriceStreamSession:
        return self.sessions[-1]

    def _calculated_id(self) -> str:
        outcome_ids = tuple(
            value.candidate_id if type(value) is ForwardPaperHistoryCandidate else value.veto_id
            for value in self.outcomes
        )
        session_ids = tuple(value.price_stream_session_id for value in self.sessions)
        return content_id(
            {
                "schema": FORWARD_PAPER_RAW_HISTORY_WINDOW_SCHEMA_VERSION,
                "policy_version": FORWARD_PAPER_HISTORY_POLICY_VERSION,
                "spec_id": self.spec.spec_id,
                "session_ids": session_ids,
                "outcome_ids": outcome_ids,
                "expected_session_count": self.expected_session_count,
                "consumed_session_count": self.consumed_session_count,
                "signal_subject_count": self.signal_subject_count,
                "complete_candidate_count": self.complete_candidate_count,
                "veto_count": self.veto_count,
                "collection_only": self.collection_only,
                "training_eligible": self.training_eligible,
                "feature_eligible": self.feature_eligible,
                "label_eligible": self.label_eligible,
                "ranking_eligible": self.ranking_eligible,
                "alert_eligible": self.alert_eligible,
                "paper_trade_eligible": self.paper_trade_eligible,
                "notification_eligible": self.notification_eligible,
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
        if self.window_id != self._calculated_id():
            _fail("forward paper raw history window identity failed")


_SessionIdentityIndex = dict[str, tuple[NseArchiveResearchPriceObservation, ...]]


def _build_session_identity_index(
    session: NseArchiveResearchPriceStreamSession,
) -> _SessionIdentityIndex:
    """Map each resolved ``research_identity_id`` in ``session`` to its exact observations.

    Preserves original stored order and every duplicate -- never
    overwrites -- so a lookup's cardinality (0, 1, or many) distinguishes
    missing, resolved, and duplicated identities. Unresolved observations
    (``research_identity_id is None``) are never entered under a
    fabricated key.
    """

    index: dict[str, list[NseArchiveResearchPriceObservation]] = {}
    for observation in session.observations:
        identity_id = observation.research_identity_id
        if identity_id is None:
            continue
        index.setdefault(identity_id, []).append(observation)
    return {key: tuple(value) for key, value in index.items()}


def _build_outcome(
    spec: ForwardPaperHistoryWindowSpec,
    signal_observation: NseArchiveResearchPriceObservation,
    signal_session_id: str,
    history_by_session: dict[date, NseArchiveResearchPriceStreamSession],
    history_index_by_session: dict[date, _SessionIdentityIndex],
) -> ForwardPaperHistoryOutcome:
    identity_id = signal_observation.research_identity_id
    if identity_id is None:
        return ForwardPaperHistoryVeto(
            spec_id=spec.spec_id,
            research_identity_id=None,
            signal_observation=signal_observation,
            reason=ForwardPaperHistoryVetoReason.SIGNAL_IDENTITY_UNRESOLVED,
            evidence_session_ids=(signal_session_id,),
            evidence_observation_ids=(),
        )

    history_observations: list[NseArchiveResearchPriceObservation] = []
    missing_session_ids: list[str] = []
    duplicated_session_ids: list[str] = []
    duplicated_observation_ids: list[str] = []
    for expected_date in spec.expected_market_sessions:
        session_for_date = history_by_session[expected_date]
        matches = history_index_by_session[expected_date].get(identity_id, ())
        if len(matches) == 0:
            missing_session_ids.append(session_for_date.price_stream_session_id)
        elif len(matches) > 1:
            duplicated_session_ids.append(session_for_date.price_stream_session_id)
            duplicated_observation_ids.extend(value.observation_id for value in matches)
        else:
            history_observations.append(matches[0])

    # Deterministic priority when a required identity is both duplicated in
    # one session and missing from another: duplication is reported first,
    # since an ambiguous same-identity collision is a stronger integrity
    # failure than a plain absence.
    if duplicated_session_ids:
        return ForwardPaperHistoryVeto(
            spec_id=spec.spec_id,
            research_identity_id=identity_id,
            signal_observation=signal_observation,
            reason=ForwardPaperHistoryVetoReason.REQUIRED_SESSION_DUPLICATED,
            evidence_session_ids=tuple(duplicated_session_ids),
            evidence_observation_ids=tuple(duplicated_observation_ids),
        )
    if missing_session_ids:
        return ForwardPaperHistoryVeto(
            spec_id=spec.spec_id,
            research_identity_id=identity_id,
            signal_observation=signal_observation,
            reason=ForwardPaperHistoryVetoReason.REQUIRED_SESSION_MISSING,
            evidence_session_ids=tuple(missing_session_ids),
            evidence_observation_ids=(),
        )
    return ForwardPaperHistoryCandidate(
        spec_id=spec.spec_id,
        research_identity_id=identity_id,
        history_observations=tuple(history_observations),
    )


def build_forward_paper_raw_history_window(
    spec: ForwardPaperHistoryWindowSpec,
    price_stream_sessions: Iterator[NseArchiveResearchPriceStreamSession],
) -> ForwardPaperRawHistoryWindow:
    """Build one complete raw-history window from one caller-supplied price-stream iterator.

    Consumes ``price_stream_sessions`` exactly once, in stored order. Any
    session strictly before ``spec.expected_market_sessions[0]`` is skipped
    and discarded; once the window has started, every subsequently consumed
    session must agree exactly, one-for-one, with the next pinned expected
    date -- a missing, duplicate, reordered, or substituted session fails
    closed immediately. Consumption stops the instant the exact signal
    session has been consumed; no later session is ever pulled.
    """

    if type(spec) is not ForwardPaperHistoryWindowSpec:
        _fail("forward paper raw history window spec type is invalid")
    spec.verify_content_identity()
    if price_stream_sessions is None:
        _fail("forward paper raw history window price stream iterator is invalid")

    construct_failed = False
    iterator: Iterator[NseArchiveResearchPriceStreamSession] | None = None
    try:
        iterator = iter(price_stream_sessions)
    except Exception:
        construct_failed = True
    if construct_failed or iterator is None:
        _fail(
            "forward paper raw history window price stream iterator could not be "
            "constructed"
        )

    expected_sessions = spec.expected_market_sessions
    consumed_sessions: list[NseArchiveResearchPriceStreamSession] = []
    history_by_session: dict[date, NseArchiveResearchPriceStreamSession] = {}
    history_index_by_session: dict[date, _SessionIdentityIndex] = {}
    started = False

    for expected_date in expected_sessions:
        while True:
            stream_exhausted = False
            advance_failed = False
            session: NseArchiveResearchPriceStreamSession | None = None
            try:
                session = next(iterator)
            except StopIteration:
                stream_exhausted = True
            except Exception:
                advance_failed = True
            if stream_exhausted:
                _fail(
                    "forward paper raw history window price stream ended before its "
                    "expected sessions were consumed"
                )
            if advance_failed:
                _fail(
                    "forward paper raw history window price stream session could not "
                    "be obtained"
                )

            verify_failed = False
            try:
                session.verify_content_identity()
            except Exception:
                verify_failed = True
            if verify_failed or type(session) is not NseArchiveResearchPriceStreamSession:
                _fail(
                    "forward paper raw history window price stream session failed "
                    "verification"
                )

            replay_session = session.paired_session.replay_session
            if replay_session.dataset_id != spec.dataset_id:
                _fail("forward paper raw history window stream session dataset is invalid")

            session_date = replay_session.market_session
            if not started:
                if session_date < expected_date:
                    continue
                started = True
            if session_date != expected_date:
                _fail(
                    "forward paper raw history window stream session does not match "
                    "its expected session"
                )
            if replay_session.observed_at > spec.decision_cutoff:
                _fail(
                    "forward paper raw history window stream session was observed "
                    "after its decision cutoff"
                )
            break

        consumed_sessions.append(session)
        history_by_session[expected_date] = session
        history_index_by_session[expected_date] = _build_session_identity_index(session)
        if expected_date == spec.signal_session:
            break

    if len(consumed_sessions) != len(expected_sessions) or spec.signal_session not in (
        history_by_session
    ):
        _fail(
            "forward paper raw history window did not consume every expected session"
        )

    signal_session_obj = consumed_sessions[-1]
    signal_observations = signal_session_obj.observations

    outcomes = tuple(
        _build_outcome(
            spec,
            signal_observation,
            signal_session_obj.price_stream_session_id,
            history_by_session,
            history_index_by_session,
        )
        for signal_observation in signal_observations
    )
    complete_candidate_count = sum(
        1 for value in outcomes if type(value) is ForwardPaperHistoryCandidate
    )
    veto_count = sum(1 for value in outcomes if type(value) is ForwardPaperHistoryVeto)

    return ForwardPaperRawHistoryWindow(
        spec=spec,
        sessions=tuple(consumed_sessions),
        outcomes=outcomes,
        expected_session_count=len(expected_sessions),
        consumed_session_count=len(consumed_sessions),
        signal_subject_count=len(signal_observations),
        complete_candidate_count=complete_candidate_count,
        veto_count=veto_count,
    )
