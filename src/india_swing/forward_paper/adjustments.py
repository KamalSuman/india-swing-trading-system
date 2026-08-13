"""Point-in-time corporate-action adjustment for forward-paper histories.

This module is a narrow adapter between the verified NSE archive research
stream and the existing corporate-action policy.  It never infers that a
research identity is a production stable identity: callers must provide an
explicit, knowledge-timed binding.  The result remains collection-only and
cannot authorize ranking, alerts, paper trades, notifications, or execution.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Context, Decimal, localcontext
from enum import Enum
from typing import Union

from india_swing.identity import content_id
from india_swing.corporate_actions.adjustments import (
    PriceAdjustmentError,
    corporate_action_factors_for_session,
    select_automatic_adjustment_events,
)
from india_swing.corporate_actions.models import CorporateActionSnapshot
from india_swing.reference.models import ReferenceReadiness

from .history import (
    ForwardPaperHistoryCandidate,
    ForwardPaperHistoryVeto,
    ForwardPaperRawHistoryWindow,
)


FORWARD_PAPER_ADJUSTMENT_POLICY_VERSION = (
    "forward-paper-corporate-action-adjustment/split-bonus-point-in-time-v2"
)
FORWARD_PAPER_IDENTITY_BINDING_SCHEMA_VERSION = (
    "forward-paper-corporate-action-identity-binding/v1"
)
FORWARD_PAPER_ADJUSTED_OBSERVATION_SCHEMA_VERSION = (
    "forward-paper-adjusted-observation/v1"
)
FORWARD_PAPER_ADJUSTED_CANDIDATE_SCHEMA_VERSION = (
    "forward-paper-adjusted-candidate/v1"
)
FORWARD_PAPER_ADJUSTMENT_VETO_SCHEMA_VERSION = "forward-paper-adjustment-veto/v1"
FORWARD_PAPER_ADJUSTED_WINDOW_SCHEMA_VERSION = "forward-paper-adjusted-window/v1"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_DECIMAL_CONTEXT = Context(prec=50)


class ForwardPaperAdjustmentError(ValueError):
    """An adjustment input or derived graph failed a static safety rule."""


def _fail(message: str) -> None:
    raise ForwardPaperAdjustmentError(message)


def _sha(value: object) -> bool:
    return type(value) is str and _SHA256.fullmatch(value) is not None


def _utc(value: object) -> datetime:
    if (
        type(value) is not datetime
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        _fail("forward paper adjustment time is invalid")
    result = value.astimezone(timezone.utc)
    if result.utcoffset() != timedelta(0):
        _fail("forward paper adjustment time is invalid")
    return result


def _positive(value: object) -> bool:
    return (
        type(value) is Decimal
        and value.is_finite()
        and value > Decimal("0")
    )


def _nonnegative(value: object) -> bool:
    return (
        type(value) is Decimal
        and value.is_finite()
        and value >= Decimal("0")
    )


@dataclass(frozen=True, slots=True)
class ForwardPaperCorporateActionIdentityBinding:
    """Explicit point-in-time bridge from research identity to stable IDs."""

    research_identity_id: str
    stable_instrument_id: str
    stable_listing_id: str
    knowledge_time: datetime
    source_artifact_id: str
    binding_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "knowledge_time", _utc(self.knowledge_time))
        object.__setattr__(self, "binding_id", self._calculated_id())

    def _validate(self) -> None:
        if any(
            not _sha(value)
            for value in (
                self.research_identity_id,
                self.stable_instrument_id,
                self.stable_listing_id,
                self.source_artifact_id,
            )
        ):
            _fail("forward paper adjustment identity binding is invalid")
        _utc(self.knowledge_time)

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": FORWARD_PAPER_IDENTITY_BINDING_SCHEMA_VERSION,
                "policy_version": FORWARD_PAPER_ADJUSTMENT_POLICY_VERSION,
                "research_identity_id": self.research_identity_id,
                "stable_instrument_id": self.stable_instrument_id,
                "stable_listing_id": self.stable_listing_id,
                "knowledge_time": self.knowledge_time,
                "source_artifact_id": self.source_artifact_id,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.binding_id != self._calculated_id():
            _fail("forward paper adjustment identity binding failed verification")


@dataclass(frozen=True, slots=True)
class ForwardPaperAdjustedObservation:
    """One source observation adjusted only by explicitly applied events."""

    source_observation: object
    identity_binding_id: str
    corporate_action_snapshot_id: str
    adjusted_previous_close: Decimal
    adjusted_open: Decimal
    adjusted_high: Decimal
    adjusted_low: Decimal
    adjusted_last: Decimal
    adjusted_close: Decimal
    adjusted_average_price: Decimal
    adjusted_volume: Decimal
    price_factor: Decimal
    volume_factor: Decimal
    applied_event_ids: tuple[str, ...]
    observation_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "observation_id", self._calculated_id())

    def _validate(self) -> None:
        from india_swing.evaluation.nse_archive_research_price_stream import (
            NseArchiveResearchPriceObservation,
        )

        if type(self.source_observation) is not NseArchiveResearchPriceObservation:
            _fail("forward paper adjusted observation source is invalid")
        verification_failed = False
        try:
            self.source_observation.verify_content_identity()
        except Exception:
            verification_failed = True
        if verification_failed:
            _fail("forward paper adjusted observation source failed verification")
        if not _sha(self.identity_binding_id) or not _sha(
            self.corporate_action_snapshot_id
        ):
            _fail("forward paper adjusted observation lineage is invalid")
        positive_decimals = (
            self.adjusted_previous_close,
            self.adjusted_open,
            self.adjusted_high,
            self.adjusted_low,
            self.adjusted_last,
            self.adjusted_close,
            self.adjusted_average_price,
            self.price_factor,
            self.volume_factor,
        )
        if any(not _positive(value) for value in positive_decimals) or not _nonnegative(
            self.adjusted_volume
        ):
            _fail("forward paper adjusted observation values are invalid")
        if (
            type(self.applied_event_ids) is not tuple
            or self.applied_event_ids != tuple(sorted(set(self.applied_event_ids)))
            or any(not _sha(value) for value in self.applied_event_ids)
        ):
            _fail("forward paper adjusted observation events are invalid")
        raw = self.source_observation.replay_record
        with localcontext(_DECIMAL_CONTEXT):
            expected = (
                raw.previous_close * self.price_factor,
                raw.open * self.price_factor,
                raw.high * self.price_factor,
                raw.low * self.price_factor,
                raw.last * self.price_factor,
                raw.close * self.price_factor,
                raw.average_price * self.price_factor,
                Decimal(raw.volume) * self.volume_factor,
            )
        actual = (
            self.adjusted_previous_close,
            self.adjusted_open,
            self.adjusted_high,
            self.adjusted_low,
            self.adjusted_last,
            self.adjusted_close,
            self.adjusted_average_price,
            self.adjusted_volume,
        )
        if actual != expected:
            _fail("forward paper adjusted observation derivation is invalid")

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": FORWARD_PAPER_ADJUSTED_OBSERVATION_SCHEMA_VERSION,
                "policy_version": FORWARD_PAPER_ADJUSTMENT_POLICY_VERSION,
                "source_observation_id": self.source_observation.observation_id,
                "identity_binding_id": self.identity_binding_id,
                "corporate_action_snapshot_id": self.corporate_action_snapshot_id,
                "adjusted_previous_close": self.adjusted_previous_close,
                "adjusted_open": self.adjusted_open,
                "adjusted_high": self.adjusted_high,
                "adjusted_low": self.adjusted_low,
                "adjusted_last": self.adjusted_last,
                "adjusted_close": self.adjusted_close,
                "adjusted_average_price": self.adjusted_average_price,
                "adjusted_volume": self.adjusted_volume,
                "price_factor": self.price_factor,
                "volume_factor": self.volume_factor,
                "applied_event_ids": self.applied_event_ids,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.observation_id != self._calculated_id():
            _fail("forward paper adjusted observation failed verification")

    @classmethod
    def _from_freshly_verified_derivation(
        cls,
        **values: object,
    ) -> "ForwardPaperAdjustedObservation":
        value = object.__new__(cls)
        for name, item in values.items():
            object.__setattr__(value, name, item)
        object.__setattr__(value, "observation_id", value._calculated_id())
        return value


@dataclass(frozen=True, slots=True)
class ForwardPaperAdjustedCandidate:
    source_candidate: ForwardPaperHistoryCandidate
    identity_binding: ForwardPaperCorporateActionIdentityBinding
    observations: tuple[ForwardPaperAdjustedObservation, ...]
    applied_event_ids: tuple[str, ...]
    candidate_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "candidate_id", self._calculated_id())

    def _validate(self) -> None:
        if type(self.source_candidate) is not ForwardPaperHistoryCandidate:
            _fail("forward paper adjusted candidate source is invalid")
        if type(self.identity_binding) is not ForwardPaperCorporateActionIdentityBinding:
            _fail("forward paper adjusted candidate binding is invalid")
        verification_failed = False
        try:
            self.source_candidate.verify_content_identity()
            self.identity_binding.verify_content_identity()
        except Exception:
            verification_failed = True
        if verification_failed:
            _fail("forward paper adjusted candidate lineage failed verification")
        if (
            self.source_candidate.research_identity_id
            != self.identity_binding.research_identity_id
        ):
            _fail("forward paper adjusted candidate identity is invalid")
        if (
            type(self.observations) is not tuple
            or len(self.observations) != len(self.source_candidate.history_observations)
        ):
            _fail("forward paper adjusted candidate observations are invalid")
        for adjusted, source in zip(
            self.observations,
            self.source_candidate.history_observations,
            strict=True,
        ):
            if type(adjusted) is not ForwardPaperAdjustedObservation:
                _fail("forward paper adjusted candidate observations are invalid")
            adjusted.verify_content_identity()
            if (
                adjusted.source_observation.observation_id != source.observation_id
                or adjusted.identity_binding_id != self.identity_binding.binding_id
            ):
                _fail("forward paper adjusted candidate observation lineage is invalid")
        expected_events = tuple(
            sorted(
                {
                    event_id
                    for observation in self.observations
                    for event_id in observation.applied_event_ids
                }
            )
        )
        if self.applied_event_ids != expected_events:
            _fail("forward paper adjusted candidate events are invalid")

    @property
    def signal_observation(self) -> ForwardPaperAdjustedObservation:
        return self.observations[-1]

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": FORWARD_PAPER_ADJUSTED_CANDIDATE_SCHEMA_VERSION,
                "policy_version": FORWARD_PAPER_ADJUSTMENT_POLICY_VERSION,
                "source_candidate_id": self.source_candidate.candidate_id,
                "identity_binding_id": self.identity_binding.binding_id,
                "observation_ids": tuple(value.observation_id for value in self.observations),
                "applied_event_ids": self.applied_event_ids,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.candidate_id != self._calculated_id():
            _fail("forward paper adjusted candidate failed verification")

    @classmethod
    def _from_freshly_verified_derivation(
        cls,
        *,
        source_candidate: ForwardPaperHistoryCandidate,
        identity_binding: ForwardPaperCorporateActionIdentityBinding,
        observations: tuple[ForwardPaperAdjustedObservation, ...],
        applied_event_ids: tuple[str, ...],
    ) -> "ForwardPaperAdjustedCandidate":
        value = object.__new__(cls)
        object.__setattr__(value, "source_candidate", source_candidate)
        object.__setattr__(value, "identity_binding", identity_binding)
        object.__setattr__(value, "observations", observations)
        object.__setattr__(value, "applied_event_ids", applied_event_ids)
        object.__setattr__(value, "candidate_id", value._calculated_id())
        return value


class ForwardPaperAdjustmentVetoReason(Enum):
    IDENTITY_BINDING_MISSING = "IDENTITY_BINDING_MISSING"
    CORPORATE_ACTION_POLICY_BLOCKED = "CORPORATE_ACTION_POLICY_BLOCKED"


@dataclass(frozen=True, slots=True)
class ForwardPaperAdjustmentVeto:
    source_candidate: ForwardPaperHistoryCandidate
    reason: ForwardPaperAdjustmentVetoReason
    veto_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.source_candidate) is not ForwardPaperHistoryCandidate:
            _fail("forward paper adjustment veto source is invalid")
        verification_failed = False
        try:
            self.source_candidate.verify_content_identity()
        except Exception:
            verification_failed = True
        if verification_failed:
            _fail("forward paper adjustment veto source failed verification")
        if type(self.reason) is not ForwardPaperAdjustmentVetoReason:
            _fail("forward paper adjustment veto reason is invalid")
        object.__setattr__(self, "veto_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": FORWARD_PAPER_ADJUSTMENT_VETO_SCHEMA_VERSION,
                "policy_version": FORWARD_PAPER_ADJUSTMENT_POLICY_VERSION,
                "source_candidate_id": self.source_candidate.candidate_id,
                "reason": self.reason,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if type(self.source_candidate) is not ForwardPaperHistoryCandidate:
            _fail("forward paper adjustment veto source is invalid")
        self.source_candidate.verify_content_identity()
        if (
            type(self.reason) is not ForwardPaperAdjustmentVetoReason
            or self.veto_id != self._calculated_id()
        ):
            _fail("forward paper adjustment veto failed verification")

    @classmethod
    def _from_freshly_verified_derivation(
        cls,
        *,
        source_candidate: ForwardPaperHistoryCandidate,
        reason: ForwardPaperAdjustmentVetoReason,
    ) -> "ForwardPaperAdjustmentVeto":
        value = object.__new__(cls)
        object.__setattr__(value, "source_candidate", source_candidate)
        object.__setattr__(value, "reason", reason)
        object.__setattr__(value, "veto_id", value._calculated_id())
        return value


ForwardPaperAdjustedOutcome = Union[
    ForwardPaperAdjustedCandidate,
    ForwardPaperAdjustmentVeto,
    ForwardPaperHistoryVeto,
]


@dataclass(frozen=True, slots=True)
class ForwardPaperAdjustedHistoryWindow:
    source_window: ForwardPaperRawHistoryWindow
    corporate_actions: CorporateActionSnapshot
    identity_bindings: tuple[ForwardPaperCorporateActionIdentityBinding, ...]
    outcomes: tuple[ForwardPaperAdjustedOutcome, ...]
    adjusted_candidate_count: int
    adjustment_veto_count: int
    source_veto_count: int
    resolved_histories_adjustment_complete: bool
    window_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "window_id", self._calculated_id())

    def _validate(self) -> None:
        if type(self.source_window) is not ForwardPaperRawHistoryWindow:
            _fail("forward paper adjusted window source is invalid")
        if type(self.corporate_actions) is not CorporateActionSnapshot:
            _fail("forward paper adjusted window corporate actions are invalid")
        verification_failed = False
        try:
            self.source_window.verify_content_identity()
            self.corporate_actions.verify_content_identity()
        except Exception:
            verification_failed = True
        if verification_failed:
            _fail("forward paper adjusted window source failed verification")
        if (
            type(self.identity_bindings) is not tuple
            or any(
                type(value) is not ForwardPaperCorporateActionIdentityBinding
                for value in self.identity_bindings
            )
        ):
            _fail("forward paper adjusted window bindings are invalid")
        for binding in self.identity_bindings:
            binding.verify_content_identity()
        if self.identity_bindings != tuple(
            sorted(
                self.identity_bindings,
                key=lambda value: (value.research_identity_id, value.binding_id),
            )
        ):
            _fail("forward paper adjusted window bindings are not canonical")
        if len({value.research_identity_id for value in self.identity_bindings}) != len(
            self.identity_bindings
        ):
            _fail("forward paper adjusted window bindings are duplicated")
        if type(self.outcomes) is not tuple or len(self.outcomes) != len(
            self.source_window.outcomes
        ):
            _fail("forward paper adjusted window outcomes are invalid")
        adjusted_count = adjustment_veto_count = source_veto_count = 0
        for source, outcome in zip(
            self.source_window.outcomes, self.outcomes, strict=True
        ):
            if type(source) is ForwardPaperHistoryCandidate:
                if type(outcome) is ForwardPaperAdjustedCandidate:
                    outcome.verify_content_identity()
                    source_id = outcome.source_candidate.candidate_id
                    adjusted_count += 1
                elif type(outcome) is ForwardPaperAdjustmentVeto:
                    outcome.verify_content_identity()
                    source_id = outcome.source_candidate.candidate_id
                    adjustment_veto_count += 1
                else:
                    _fail("forward paper adjusted window outcome type is invalid")
                if source_id != source.candidate_id:
                    _fail("forward paper adjusted window outcome lineage is invalid")
            elif type(source) is ForwardPaperHistoryVeto:
                if type(outcome) is not ForwardPaperHistoryVeto:
                    _fail("forward paper adjusted window source veto was replaced")
                outcome.verify_content_identity()
                if outcome.veto_id != source.veto_id:
                    _fail("forward paper adjusted window source veto lineage is invalid")
                source_veto_count += 1
            else:
                _fail("forward paper adjusted window source outcome is invalid")
        if (
            self.adjusted_candidate_count != adjusted_count
            or self.adjustment_veto_count != adjustment_veto_count
            or self.source_veto_count != source_veto_count
            or self.resolved_histories_adjustment_complete
            is not (adjustment_veto_count == 0)
        ):
            _fail("forward paper adjusted window derived state is invalid")

    def _calculated_id(self) -> str:
        outcome_ids = tuple(
            value.candidate_id
            if type(value) is ForwardPaperAdjustedCandidate
            else value.veto_id
            for value in self.outcomes
        )
        return content_id(
            {
                "schema": FORWARD_PAPER_ADJUSTED_WINDOW_SCHEMA_VERSION,
                "policy_version": FORWARD_PAPER_ADJUSTMENT_POLICY_VERSION,
                "source_window_id": self.source_window.window_id,
                "corporate_action_snapshot_id": self.corporate_actions.snapshot_id,
                "identity_binding_ids": tuple(
                    value.binding_id for value in self.identity_bindings
                ),
                "outcome_ids": outcome_ids,
                "adjusted_candidate_count": self.adjusted_candidate_count,
                "adjustment_veto_count": self.adjustment_veto_count,
                "source_veto_count": self.source_veto_count,
                "resolved_histories_adjustment_complete": (
                    self.resolved_histories_adjustment_complete
                ),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.window_id != self._calculated_id():
            _fail("forward paper adjusted window failed verification")

    @classmethod
    def _from_freshly_verified_derivation(
        cls,
        *,
        source_window: ForwardPaperRawHistoryWindow,
        corporate_actions: CorporateActionSnapshot,
        identity_bindings: tuple[ForwardPaperCorporateActionIdentityBinding, ...],
        outcomes: tuple[ForwardPaperAdjustedOutcome, ...],
        adjusted_candidate_count: int,
        adjustment_veto_count: int,
        source_veto_count: int,
        resolved_histories_adjustment_complete: bool,
    ) -> "ForwardPaperAdjustedHistoryWindow":
        value = object.__new__(cls)
        for name, item in (
            ("source_window", source_window),
            ("corporate_actions", corporate_actions),
            ("identity_bindings", identity_bindings),
            ("outcomes", outcomes),
            ("adjusted_candidate_count", adjusted_candidate_count),
            ("adjustment_veto_count", adjustment_veto_count),
            ("source_veto_count", source_veto_count),
            (
                "resolved_histories_adjustment_complete",
                resolved_histories_adjustment_complete,
            ),
        ):
            object.__setattr__(value, name, item)
        object.__setattr__(value, "window_id", value._calculated_id())
        return value

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


def _adjust_candidate(
    candidate: ForwardPaperHistoryCandidate,
    binding: ForwardPaperCorporateActionIdentityBinding,
    corporate_actions: CorporateActionSnapshot,
) -> ForwardPaperAdjustedCandidate | ForwardPaperAdjustmentVeto:
    rejected = False
    failed = False
    adjusted: list[ForwardPaperAdjustedObservation] = []
    try:
        events = select_automatic_adjustment_events(
            corporate_actions=corporate_actions,
            stable_instrument_id=binding.stable_instrument_id,
            stable_listing_id=binding.stable_listing_id,
            history_start=candidate.history_observations[0].market_session,
            signal_session=candidate.signal_observation.market_session,
        )
        with localcontext(_DECIMAL_CONTEXT):
            for source in candidate.history_observations:
                price_factor, volume_factor, event_ids = (
                    corporate_action_factors_for_session(
                        events=events,
                        market_session=source.market_session,
                    )
                )
                raw = source.replay_record
                adjusted.append(
                    ForwardPaperAdjustedObservation._from_freshly_verified_derivation(
                        source_observation=source,
                        identity_binding_id=binding.binding_id,
                        corporate_action_snapshot_id=corporate_actions.snapshot_id,
                        adjusted_previous_close=raw.previous_close * price_factor,
                        adjusted_open=raw.open * price_factor,
                        adjusted_high=raw.high * price_factor,
                        adjusted_low=raw.low * price_factor,
                        adjusted_last=raw.last * price_factor,
                        adjusted_close=raw.close * price_factor,
                        adjusted_average_price=raw.average_price * price_factor,
                        adjusted_volume=Decimal(raw.volume) * volume_factor,
                        price_factor=price_factor,
                        volume_factor=volume_factor,
                        applied_event_ids=event_ids,
                    )
                )
    except ForwardPaperAdjustmentError:
        raise
    except PriceAdjustmentError:
        rejected = True
    except Exception:
        failed = True
    if rejected:
        return ForwardPaperAdjustmentVeto._from_freshly_verified_derivation(
            source_candidate=candidate,
            reason=(
                ForwardPaperAdjustmentVetoReason.CORPORATE_ACTION_POLICY_BLOCKED
            ),
        )
    if failed:
        _fail("forward paper candidate corporate action adjustment failed")
    observations = tuple(adjusted)
    return ForwardPaperAdjustedCandidate._from_freshly_verified_derivation(
        source_candidate=candidate,
        identity_binding=binding,
        observations=observations,
        applied_event_ids=tuple(
            sorted(
                {
                    event_id
                    for observation in observations
                    for event_id in observation.applied_event_ids
                }
            )
        ),
    )


def _build_forward_paper_adjusted_history_window(
    *,
    source_window: ForwardPaperRawHistoryWindow,
    corporate_actions: CorporateActionSnapshot,
    identity_bindings: tuple[ForwardPaperCorporateActionIdentityBinding, ...],
    verify_inputs: bool,
) -> ForwardPaperAdjustedHistoryWindow:
    """Build an immutable adjustment view at the raw window's pinned cutoff."""

    if type(source_window) is not ForwardPaperRawHistoryWindow:
        _fail("forward paper adjustment source window is invalid")
    if type(corporate_actions) is not CorporateActionSnapshot:
        _fail("forward paper adjustment corporate actions are invalid")
    if (
        type(identity_bindings) is not tuple
        or any(
            type(value) is not ForwardPaperCorporateActionIdentityBinding
            for value in identity_bindings
        )
    ):
        _fail("forward paper adjustment identity bindings are invalid")
    if verify_inputs:
        verification_failed = False
        try:
            source_window.verify_content_identity()
            corporate_actions.verify_content_identity()
            for binding in identity_bindings:
                binding.verify_content_identity()
        except Exception:
            verification_failed = True
        if verification_failed:
            _fail("forward paper adjustment input failed verification")
    if (
        not corporate_actions.complete
        or not corporate_actions.actionable
        or corporate_actions.readiness is ReferenceReadiness.COLLECTION_ONLY
    ):
        _fail("forward paper corporate action evidence is not actionable")
    cutoff = source_window.spec.decision_cutoff
    if corporate_actions.cutoff > cutoff:
        _fail("forward paper corporate action evidence is future-known")
    if (
        corporate_actions.coverage_start
        > source_window.spec.expected_market_sessions[0]
        or corporate_actions.coverage_end < source_window.spec.signal_session
    ):
        _fail("forward paper corporate action coverage is incomplete")
    if any(binding.knowledge_time > cutoff for binding in identity_bindings):
        _fail("forward paper adjustment identity binding is future-known")
    canonical_bindings = tuple(
        sorted(
            identity_bindings,
            key=lambda value: (value.research_identity_id, value.binding_id),
        )
    )
    by_identity: dict[str, ForwardPaperCorporateActionIdentityBinding] = {}
    for binding in canonical_bindings:
        if binding.research_identity_id in by_identity:
            _fail("forward paper adjustment identity bindings are duplicated")
        by_identity[binding.research_identity_id] = binding
    candidate_ids = {
        value.research_identity_id
        for value in source_window.outcomes
        if type(value) is ForwardPaperHistoryCandidate
    }
    if any(value not in candidate_ids for value in by_identity):
        _fail("forward paper adjustment identity binding is not in the source window")

    outcomes: list[ForwardPaperAdjustedOutcome] = []
    adjusted_count = adjustment_veto_count = source_veto_count = 0
    for source in source_window.outcomes:
        if type(source) is ForwardPaperHistoryVeto:
            outcomes.append(source)
            source_veto_count += 1
            continue
        binding = by_identity.get(source.research_identity_id)
        if binding is None:
            outcomes.append(
                ForwardPaperAdjustmentVeto._from_freshly_verified_derivation(
                    source_candidate=source,
                    reason=ForwardPaperAdjustmentVetoReason.IDENTITY_BINDING_MISSING,
                )
            )
            adjustment_veto_count += 1
            continue
        outcome = _adjust_candidate(source, binding, corporate_actions)
        outcomes.append(outcome)
        if type(outcome) is ForwardPaperAdjustedCandidate:
            adjusted_count += 1
        else:
            adjustment_veto_count += 1

    return ForwardPaperAdjustedHistoryWindow._from_freshly_verified_derivation(
        source_window=source_window,
        corporate_actions=corporate_actions,
        identity_bindings=canonical_bindings,
        outcomes=tuple(outcomes),
        adjusted_candidate_count=adjusted_count,
        adjustment_veto_count=adjustment_veto_count,
        source_veto_count=source_veto_count,
        resolved_histories_adjustment_complete=adjustment_veto_count == 0,
    )


def build_forward_paper_adjusted_history_window(
    *,
    source_window: ForwardPaperRawHistoryWindow,
    corporate_actions: CorporateActionSnapshot,
    identity_bindings: tuple[ForwardPaperCorporateActionIdentityBinding, ...],
) -> ForwardPaperAdjustedHistoryWindow:
    """Build an immutable adjustment view after independently verifying inputs."""

    return _build_forward_paper_adjusted_history_window(
        source_window=source_window,
        corporate_actions=corporate_actions,
        identity_bindings=identity_bindings,
        verify_inputs=True,
    )


def _build_forward_paper_adjusted_history_window_from_verified_inputs(
    *,
    source_window: ForwardPaperRawHistoryWindow,
    corporate_actions: CorporateActionSnapshot,
    identity_bindings: tuple[ForwardPaperCorporateActionIdentityBinding, ...],
) -> ForwardPaperAdjustedHistoryWindow:
    return _build_forward_paper_adjusted_history_window(
        source_window=source_window,
        corporate_actions=corporate_actions,
        identity_bindings=identity_bindings,
        verify_inputs=False,
    )
