"""Signal-session tick join for adjusted forward-paper histories.

Historical OHLCV bars do not need an invented tick-size history.  The join
requires one point-in-time verified tick fact for the signal session only,
which is the fact used for order-price rounding and current spread features.
Missing or ambiguous signal-session evidence becomes an explicit veto; no
previous/next/latest tick is substituted and no current tick is backfilled
onto historical sessions.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Union

from india_swing.evaluation.dataset_assembly import EffectiveTickSize
from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness

from .adjustments import (
    ForwardPaperAdjustedCandidate,
    ForwardPaperAdjustedHistoryWindow,
    ForwardPaperAdjustedObservation,
    ForwardPaperAdjustmentVeto,
)
from .history import ForwardPaperHistoryVeto


FORWARD_PAPER_FEATURE_INPUT_POLICY_VERSION = (
    "forward-paper-feature-input/signal-session-tick-only-60-bars-v2"
)
FORWARD_PAPER_FEATURE_INPUT_BAR_SCHEMA_VERSION = "forward-paper-feature-input-bar/v1"
FORWARD_PAPER_FEATURE_INPUT_CANDIDATE_SCHEMA_VERSION = (
    "forward-paper-feature-input-candidate/v1"
)
FORWARD_PAPER_FEATURE_INPUT_VETO_SCHEMA_VERSION = "forward-paper-feature-input-veto/v1"
FORWARD_PAPER_FEATURE_INPUT_WINDOW_SCHEMA_VERSION = "forward-paper-feature-input-window/v1"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class ForwardPaperFeatureInputError(ValueError):
    """A feature-input graph failed an exact lineage or safety rule."""


def _fail(message: str) -> None:
    raise ForwardPaperFeatureInputError(message)


def _sha(value: object) -> bool:
    return type(value) is str and _SHA256.fullmatch(value) is not None


@dataclass(frozen=True, slots=True)
class ForwardPaperFeatureInputBar:
    adjusted_observation: ForwardPaperAdjustedObservation
    tick_specification: EffectiveTickSize | None
    stable_instrument_id: str
    stable_listing_id: str
    market_session: date
    knowledge_time: datetime
    input_bar_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "input_bar_id", self._calculated_id())

    def _validate(self) -> None:
        if type(self.adjusted_observation) is not ForwardPaperAdjustedObservation:
            _fail("forward paper feature input adjusted observation is invalid")
        if self.tick_specification is not None and type(
            self.tick_specification
        ) is not EffectiveTickSize:
            _fail("forward paper feature input tick specification is invalid")
        verification_failed = False
        try:
            self.adjusted_observation.verify_content_identity()
            if self.tick_specification is not None:
                self.tick_specification.verify_content_identity()
        except Exception:
            verification_failed = True
        if verification_failed:
            _fail("forward paper feature input evidence failed verification")
        if not _sha(self.stable_instrument_id) or not _sha(self.stable_listing_id):
            _fail("forward paper feature input stable identity is invalid")
        if type(self.market_session) is not date:
            _fail("forward paper feature input session is invalid")
        source = self.adjusted_observation.source_observation
        spec = self.tick_specification
        if source.market_session != self.market_session:
            _fail("forward paper feature input tick lineage is invalid")
        if spec is not None and (
            spec.instrument_id != self.stable_instrument_id
            or spec.listing_id != self.stable_listing_id
            or spec.effective_from_session != self.market_session
            or not spec.is_effective_on(self.market_session)
            or spec.readiness is not ReferenceReadiness.POINT_IN_TIME_VERIFIED
        ):
            _fail("forward paper feature input tick lineage is invalid")
        if spec is not None and self.knowledge_time != spec.knowledge_time:
            _fail("forward paper feature input knowledge time is invalid")
        try:
            aware = self.knowledge_time.utcoffset() is not None
        except Exception:
            aware = False
        if not aware:
            _fail("forward paper feature input knowledge time is invalid")

    @property
    def adjusted_open(self):
        return self.adjusted_observation.adjusted_open

    @property
    def adjusted_high(self):
        return self.adjusted_observation.adjusted_high

    @property
    def adjusted_low(self):
        return self.adjusted_observation.adjusted_low

    @property
    def adjusted_close(self):
        return self.adjusted_observation.adjusted_close

    @property
    def adjusted_volume(self):
        return self.adjusted_observation.adjusted_volume

    @property
    def tick_size(self):
        if self.tick_specification is None:
            _fail("historical forward paper bar has no signal-session tick")
        return self.tick_specification.tick_size

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": FORWARD_PAPER_FEATURE_INPUT_BAR_SCHEMA_VERSION,
                "policy_version": FORWARD_PAPER_FEATURE_INPUT_POLICY_VERSION,
                "adjusted_observation_id": self.adjusted_observation.observation_id,
                "tick_specification_id": (
                    None
                    if self.tick_specification is None
                    else self.tick_specification.specification_id
                ),
                "stable_instrument_id": self.stable_instrument_id,
                "stable_listing_id": self.stable_listing_id,
                "market_session": self.market_session,
                "knowledge_time": self.knowledge_time,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.input_bar_id != self._calculated_id():
            _fail("forward paper feature input bar failed verification")


@dataclass(frozen=True, slots=True)
class ForwardPaperFeatureInputCandidate:
    source_candidate: ForwardPaperAdjustedCandidate
    bars: tuple[ForwardPaperFeatureInputBar, ...]
    candidate_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "candidate_id", self._calculated_id())

    def _validate(self) -> None:
        if type(self.source_candidate) is not ForwardPaperAdjustedCandidate:
            _fail("forward paper feature input candidate source is invalid")
        verification_failed = False
        try:
            self.source_candidate.verify_content_identity()
        except Exception:
            verification_failed = True
        if verification_failed:
            _fail("forward paper feature input candidate source failed verification")
        if type(self.bars) is not tuple or len(self.bars) != len(
            self.source_candidate.observations
        ):
            _fail("forward paper feature input candidate bars are invalid")
        binding = self.source_candidate.identity_binding
        for bar, adjusted in zip(
            self.bars, self.source_candidate.observations, strict=True
        ):
            if type(bar) is not ForwardPaperFeatureInputBar:
                _fail("forward paper feature input candidate bars are invalid")
            bar.verify_content_identity()
            if (
                bar.adjusted_observation.observation_id != adjusted.observation_id
                or bar.stable_instrument_id != binding.stable_instrument_id
                or bar.stable_listing_id != binding.stable_listing_id
            ):
                _fail("forward paper feature input candidate lineage is invalid")
        if any(value.tick_specification is not None for value in self.bars[:-1]) or (
            self.bars[-1].tick_specification is None
        ):
            _fail("forward paper feature input signal tick placement is invalid")

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": FORWARD_PAPER_FEATURE_INPUT_CANDIDATE_SCHEMA_VERSION,
                "policy_version": FORWARD_PAPER_FEATURE_INPUT_POLICY_VERSION,
                "source_candidate_id": self.source_candidate.candidate_id,
                "input_bar_ids": tuple(value.input_bar_id for value in self.bars),
            },
            length=64,
        )

    @property
    def history_id(self) -> str:
        """Compatibility identity for the shared deterministic feature kernel."""

        return self.candidate_id

    @property
    def stable_instrument_id(self) -> str:
        return self.source_candidate.identity_binding.stable_instrument_id

    @property
    def stable_listing_id(self) -> str:
        return self.source_candidate.identity_binding.stable_listing_id

    @property
    def signal_session(self) -> date:
        return self.bars[-1].market_session

    def verify_content_identity(self) -> None:
        self._validate()
        if self.candidate_id != self._calculated_id():
            _fail("forward paper feature input candidate failed verification")


class ForwardPaperFeatureInputVetoReason(Enum):
    SOURCE_HISTORY_VETO = "SOURCE_HISTORY_VETO"
    SOURCE_ADJUSTMENT_VETO = "SOURCE_ADJUSTMENT_VETO"
    EXACT_SESSION_TICK_MISSING = "EXACT_SESSION_TICK_MISSING"
    EXACT_SESSION_TICK_AMBIGUOUS = "EXACT_SESSION_TICK_AMBIGUOUS"
    EXACT_SESSION_TICK_UNVERIFIED = "EXACT_SESSION_TICK_UNVERIFIED"
    EXACT_SESSION_TICK_FUTURE_KNOWN = "EXACT_SESSION_TICK_FUTURE_KNOWN"


@dataclass(frozen=True, slots=True)
class ForwardPaperFeatureInputVeto:
    source_outcome_id: str
    reason: ForwardPaperFeatureInputVetoReason
    affected_sessions: tuple[date, ...]
    evidence_tick_specification_ids: tuple[str, ...]
    veto_id: str = field(init=False)

    def __post_init__(self) -> None:
        if not _sha(self.source_outcome_id):
            _fail("forward paper feature input veto source is invalid")
        if type(self.reason) is not ForwardPaperFeatureInputVetoReason:
            _fail("forward paper feature input veto reason is invalid")
        if (
            type(self.affected_sessions) is not tuple
            or self.affected_sessions != tuple(sorted(set(self.affected_sessions)))
            or any(type(value) is not date for value in self.affected_sessions)
        ):
            _fail("forward paper feature input veto sessions are invalid")
        if (
            type(self.evidence_tick_specification_ids) is not tuple
            or self.evidence_tick_specification_ids
            != tuple(sorted(set(self.evidence_tick_specification_ids)))
            or any(not _sha(value) for value in self.evidence_tick_specification_ids)
        ):
            _fail("forward paper feature input veto evidence is invalid")
        source_veto = self.reason in {
            ForwardPaperFeatureInputVetoReason.SOURCE_HISTORY_VETO,
            ForwardPaperFeatureInputVetoReason.SOURCE_ADJUSTMENT_VETO,
        }
        if source_veto is not (not self.affected_sessions):
            _fail("forward paper feature input veto evidence shape is invalid")
        object.__setattr__(self, "veto_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": FORWARD_PAPER_FEATURE_INPUT_VETO_SCHEMA_VERSION,
                "policy_version": FORWARD_PAPER_FEATURE_INPUT_POLICY_VERSION,
                "source_outcome_id": self.source_outcome_id,
                "reason": self.reason,
                "affected_sessions": self.affected_sessions,
                "evidence_tick_specification_ids": (
                    self.evidence_tick_specification_ids
                ),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.veto_id != self._calculated_id():
            _fail("forward paper feature input veto failed verification")


ForwardPaperFeatureInputOutcome = Union[
    ForwardPaperFeatureInputCandidate,
    ForwardPaperFeatureInputVeto,
]


@dataclass(frozen=True, slots=True)
class ForwardPaperFeatureInputWindow:
    source_window: ForwardPaperAdjustedHistoryWindow
    tick_specifications: tuple[EffectiveTickSize, ...]
    outcomes: tuple[ForwardPaperFeatureInputOutcome, ...]
    assembled_candidate_count: int
    veto_count: int
    resolved_histories_input_complete: bool
    window_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "window_id", self._calculated_id())

    def _validate(self) -> None:
        if type(self.source_window) is not ForwardPaperAdjustedHistoryWindow:
            _fail("forward paper feature input window source is invalid")
        self.source_window.verify_content_identity()
        if (
            type(self.tick_specifications) is not tuple
            or any(type(value) is not EffectiveTickSize for value in self.tick_specifications)
            or self.tick_specifications
            != tuple(
                sorted(
                    self.tick_specifications,
                    key=lambda value: (
                        value.instrument_id,
                        value.listing_id,
                        value.effective_from_session,
                        value.specification_id,
                    ),
                )
            )
        ):
            _fail("forward paper feature input window tick specifications are invalid")
        for value in self.tick_specifications:
            value.verify_content_identity()
        if len({value.specification_id for value in self.tick_specifications}) != len(
            self.tick_specifications
        ):
            _fail("forward paper feature input window tick specifications are duplicated")
        ticks_by_id = {
            value.specification_id: value for value in self.tick_specifications
        }
        if type(self.outcomes) is not tuple or len(self.outcomes) != len(
            self.source_window.outcomes
        ):
            _fail("forward paper feature input window outcomes are invalid")
        assembled = vetoes = 0
        for source, outcome in zip(self.source_window.outcomes, self.outcomes, strict=True):
            source_id = _source_outcome_id(source)
            if type(outcome) is ForwardPaperFeatureInputCandidate:
                outcome.verify_content_identity()
                if type(source) is not ForwardPaperAdjustedCandidate:
                    _fail("forward paper feature input window promoted a source veto")
                if outcome.source_candidate.candidate_id != source_id:
                    _fail("forward paper feature input window lineage is invalid")
                if any(
                    bar.tick_specification is not None
                    and ticks_by_id.get(bar.tick_specification.specification_id)
                    is not bar.tick_specification
                    for bar in outcome.bars
                ):
                    _fail("forward paper feature input window tick lineage is invalid")
                assembled += 1
            elif type(outcome) is ForwardPaperFeatureInputVeto:
                outcome.verify_content_identity()
                if outcome.source_outcome_id != source_id:
                    _fail("forward paper feature input window veto lineage is invalid")
                if any(
                    value not in ticks_by_id
                    for value in outcome.evidence_tick_specification_ids
                ):
                    _fail("forward paper feature input window veto evidence is invalid")
                vetoes += 1
            else:
                _fail("forward paper feature input window outcome is invalid")
        if (
            self.assembled_candidate_count != assembled
            or self.veto_count != vetoes
            or self.resolved_histories_input_complete is not (vetoes == 0)
        ):
            _fail("forward paper feature input window derived state is invalid")

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": FORWARD_PAPER_FEATURE_INPUT_WINDOW_SCHEMA_VERSION,
                "policy_version": FORWARD_PAPER_FEATURE_INPUT_POLICY_VERSION,
                "source_window_id": self.source_window.window_id,
                "tick_specification_ids": tuple(
                    value.specification_id for value in self.tick_specifications
                ),
                "outcome_ids": tuple(
                    value.candidate_id
                    if type(value) is ForwardPaperFeatureInputCandidate
                    else value.veto_id
                    for value in self.outcomes
                ),
                "assembled_candidate_count": self.assembled_candidate_count,
                "veto_count": self.veto_count,
                "resolved_histories_input_complete": (
                    self.resolved_histories_input_complete
                ),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.window_id != self._calculated_id():
            _fail("forward paper feature input window failed verification")

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


def _source_outcome_id(source: object) -> str:
    if type(source) is ForwardPaperAdjustedCandidate:
        return source.candidate_id
    if type(source) is ForwardPaperAdjustmentVeto:
        return source.veto_id
    if type(source) is ForwardPaperHistoryVeto:
        return source.veto_id
    _fail("forward paper feature input source outcome is invalid")


def _source_veto(source: object) -> ForwardPaperFeatureInputVeto:
    reason = (
        ForwardPaperFeatureInputVetoReason.SOURCE_ADJUSTMENT_VETO
        if type(source) is ForwardPaperAdjustmentVeto
        else ForwardPaperFeatureInputVetoReason.SOURCE_HISTORY_VETO
    )
    return ForwardPaperFeatureInputVeto(
        source_outcome_id=_source_outcome_id(source),
        reason=reason,
        affected_sessions=(),
        evidence_tick_specification_ids=(),
    )


def build_forward_paper_feature_input_window(
    *,
    source_window: ForwardPaperAdjustedHistoryWindow,
    tick_specifications: tuple[EffectiveTickSize, ...],
) -> ForwardPaperFeatureInputWindow:
    """Join only the signal bar to one exact-session tick fact, never latest."""

    if type(source_window) is not ForwardPaperAdjustedHistoryWindow:
        _fail("forward paper feature input source window is invalid")
    if type(tick_specifications) is not tuple or any(
        type(value) is not EffectiveTickSize for value in tick_specifications
    ):
        _fail("forward paper feature input tick specifications are invalid")
    verification_failed = False
    try:
        source_window.verify_content_identity()
        for value in tick_specifications:
            value.verify_content_identity()
    except Exception:
        verification_failed = True
    if verification_failed:
        _fail("forward paper feature input evidence failed verification")
    canonical_ticks = tuple(
        sorted(
            tick_specifications,
            key=lambda value: (
                value.instrument_id,
                value.listing_id,
                value.effective_from_session,
                value.specification_id,
            ),
        )
    )
    if len({value.specification_id for value in canonical_ticks}) != len(canonical_ticks):
        _fail("forward paper feature input tick specifications are duplicated")

    relevant_keys = {
        (
            outcome.identity_binding.stable_instrument_id,
            outcome.identity_binding.stable_listing_id,
            outcome.observations[-1].source_observation.market_session,
        )
        for outcome in source_window.outcomes
        if type(outcome) is ForwardPaperAdjustedCandidate
    }
    if any(
        (value.instrument_id, value.listing_id, value.effective_from_session)
        not in relevant_keys
        for value in canonical_ticks
    ):
        _fail("forward paper feature input tick specification is foreign")
    by_key: dict[tuple[str, str, date], list[EffectiveTickSize]] = {}
    for value in canonical_ticks:
        key = (value.instrument_id, value.listing_id, value.effective_from_session)
        by_key.setdefault(key, []).append(value)

    outcomes: list[ForwardPaperFeatureInputOutcome] = []
    assembled = vetoes = 0
    cutoff = source_window.source_window.spec.decision_cutoff
    for source in source_window.outcomes:
        if type(source) is not ForwardPaperAdjustedCandidate:
            outcomes.append(_source_veto(source))
            vetoes += 1
            continue
        binding = source.identity_binding
        signal_session = source.observations[-1].source_observation.market_session
        matches = tuple(
            by_key.get(
                (
                    binding.stable_instrument_id,
                    binding.stable_listing_id,
                    signal_session,
                ),
                (),
            )
        )
        missing = (signal_session,) if not matches else ()
        ambiguous = (signal_session,) if len(matches) > 1 else ()
        unverified = (
            (signal_session,)
            if len(matches) == 1
            and matches[0].readiness is not ReferenceReadiness.POINT_IN_TIME_VERIFIED
            else ()
        )
        future = (
            (signal_session,)
            if len(matches) == 1 and matches[0].knowledge_time > cutoff
            else ()
        )
        if ambiguous:
            reason = ForwardPaperFeatureInputVetoReason.EXACT_SESSION_TICK_AMBIGUOUS
            affected = ambiguous
        elif missing:
            reason = ForwardPaperFeatureInputVetoReason.EXACT_SESSION_TICK_MISSING
            affected = missing
        elif unverified:
            reason = ForwardPaperFeatureInputVetoReason.EXACT_SESSION_TICK_UNVERIFIED
            affected = unverified
        elif future:
            reason = ForwardPaperFeatureInputVetoReason.EXACT_SESSION_TICK_FUTURE_KNOWN
            affected = future
        else:
            signal_tick = matches[0]
            bars = tuple(
                ForwardPaperFeatureInputBar(
                    adjusted_observation=bar,
                    tick_specification=(
                        signal_tick if index == len(source.observations) - 1 else None
                    ),
                    stable_instrument_id=binding.stable_instrument_id,
                    stable_listing_id=binding.stable_listing_id,
                    market_session=bar.source_observation.market_session,
                    knowledge_time=signal_tick.knowledge_time,
                )
                for index, bar in enumerate(source.observations)
            )
            outcomes.append(
                ForwardPaperFeatureInputCandidate(source_candidate=source, bars=bars)
            )
            assembled += 1
            continue
        evidence = tuple(
            sorted(
                {
                    value.specification_id
                    for value in matches
                    if value.effective_from_session in affected
                }
            )
        )
        outcomes.append(
            ForwardPaperFeatureInputVeto(
                source_outcome_id=source.candidate_id,
                reason=reason,
                affected_sessions=affected,
                evidence_tick_specification_ids=evidence,
            )
        )
        vetoes += 1

    return ForwardPaperFeatureInputWindow(
        source_window=source_window,
        tick_specifications=canonical_ticks,
        outcomes=tuple(outcomes),
        assembled_candidate_count=assembled,
        veto_count=vetoes,
        resolved_histories_input_complete=vetoes == 0,
    )
