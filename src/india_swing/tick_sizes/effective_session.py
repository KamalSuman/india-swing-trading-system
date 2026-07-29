from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from enum import Enum

from india_swing.evaluation.dataset_assembly import EffectiveTickSize
from india_swing.historical_prices.promoted_history import (
    PromotedStableListingHistoryObservation,
    VerifiedPromotedStableListingHistoryPanel,
)
from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness
from india_swing.tick_sizes.promoted_session import (
    VerifiedPromotedSessionTickSnapshot,
)


class PromotedEffectiveSessionTickError(ValueError):
    pass


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_REASON = re.compile(r"[A-Z][A-Z0-9_]{2,127}\Z")
_ONE_DAY = timedelta(days=1)

PROMOTED_EFFECTIVE_SESSION_TICK_SCHEMA_VERSION = "promoted-effective-session-tick/v1"
PROMOTED_EFFECTIVE_SESSION_TICK_POLICY_VERSION = (
    "promoted-effective-session-tick/one-calendar-date-interval-v1"
)

_EXACT_SESSION_REASON = "EXACT_SESSION_TICK_INTERVAL_ONLY"
_NO_CROSS_SESSION_REASON = "NO_CROSS_SESSION_TICK_INFERENCE"
_POINT_IN_TIME_VERIFIED_REASON = "EFFECTIVE_TICK_SIZE_POINT_IN_TIME_VERIFIED"
_MISSING_OBSERVATION_REASON = "MISSING_TICK_OBSERVATION_NO_STATE_INFERENCE"

_VERIFIED_REASONS: tuple[str, ...] = tuple(
    sorted(
        (
            _EXACT_SESSION_REASON,
            _NO_CROSS_SESSION_REASON,
            _POINT_IN_TIME_VERIFIED_REASON,
        )
    )
)
_BLOCKED_REASONS: tuple[str, ...] = tuple(
    sorted((_MISSING_OBSERVATION_REASON, _NO_CROSS_SESSION_REASON))
)

_ERR_TYPE = "promoted effective session tick type is invalid"
_ERR_SCHEMA_VERSION = "promoted effective session tick schema version is unsupported"
_ERR_SOURCE_PANEL = "promoted effective session tick source panel is invalid"
_ERR_SOURCE_PANEL_VERIFY = (
    "promoted effective session tick could not verify the source panel"
)
_ERR_SOURCE_PANEL_STATE = (
    "promoted effective session tick source panel is not collection-only"
)
_ERR_CUTOFF = "promoted effective session tick cutoff is invalid"
_ERR_FUTURE = "promoted effective session tick contains future-known evidence"
_ERR_GRAPH = "promoted effective session tick could not build its results"
_ERR_DERIVED_FIELD = "promoted effective session tick derived field is invalid"
_ERR_COMPARISON_FAILED = (
    "promoted effective session tick retained content could not be verified"
)
_ERR_PANEL_ID = (
    "promoted effective session tick identifier disagrees with independently "
    "recomputed content"
)


def _utc(value: datetime) -> datetime:
    if type(value) is not datetime:
        raise PromotedEffectiveSessionTickError(_ERR_CUTOFF)
    try:
        offset = value.utcoffset()
    except Exception:
        raise PromotedEffectiveSessionTickError(_ERR_CUTOFF) from None
    if value.tzinfo is None or offset is None:
        raise PromotedEffectiveSessionTickError(_ERR_CUTOFF)
    return value.astimezone(timezone.utc)


class PromotedEffectiveSessionTickStatus(str, Enum):
    VERIFIED_EXACT_SESSION_ONLY = "VERIFIED_EXACT_SESSION_ONLY"
    MISSING_OBSERVATION_BLOCKED = "MISSING_OBSERVATION_BLOCKED"


@dataclass(frozen=True, slots=True)
class PromotedEffectiveSessionTickResult:
    """One immutable point-in-time tick-size promotion decision for exactly
    one (stable_instrument_id, stable_listing_id, market_session) cell of
    the source stable-listing history panel.

    A cell whose retained history observation still carries its exact
    PromotedSessionTickEntry becomes VERIFIED_EXACT_SESSION_ONLY with
    exactly one EffectiveTickSize covering only that one calendar date
    (``effective_from_session`` through ``effective_from_session`` plus one
    day, exclusive) -- never merged, extended, or made open-ended, and
    entirely independent of whether a price bar was observed for that
    session. A cell with no retained tick entry -- because its whole
    session snapshot or universe row is missing -- becomes
    MISSING_OBSERVATION_BLOCKED with no tick specification at all; no
    adjacent-session value is ever forward-filled, backfilled, or inferred.
    """

    stable_instrument_id: str
    stable_listing_id: str
    market_session: date
    status: PromotedEffectiveSessionTickStatus
    source_observation: PromotedStableListingHistoryObservation
    tick_specification: EffectiveTickSize | None
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        for value in (self.stable_instrument_id, self.stable_listing_id):
            if type(value) is not str or _SHA256.fullmatch(value) is None:
                raise PromotedEffectiveSessionTickError(_ERR_GRAPH)
        if type(self.market_session) is not date:
            raise PromotedEffectiveSessionTickError(_ERR_GRAPH)
        if type(self.status) is not PromotedEffectiveSessionTickStatus:
            raise PromotedEffectiveSessionTickError(_ERR_GRAPH)
        if type(self.source_observation) is not PromotedStableListingHistoryObservation:
            raise PromotedEffectiveSessionTickError(_ERR_GRAPH)
        if (
            self.tick_specification is not None
            and type(self.tick_specification) is not EffectiveTickSize
        ):
            raise PromotedEffectiveSessionTickError(_ERR_GRAPH)
        if self.tick_specification is not None:
            try:
                self.tick_specification.verify_content_identity()
            except Exception:
                raise PromotedEffectiveSessionTickError(_ERR_GRAPH) from None
        if (
            type(self.reason_codes) is not tuple
            or not self.reason_codes
            or self.reason_codes != tuple(sorted(set(self.reason_codes)))
            or any(_REASON.fullmatch(value) is None for value in self.reason_codes)
        ):
            raise PromotedEffectiveSessionTickError(_ERR_GRAPH)

        if self.source_observation.market_session != self.market_session:
            raise PromotedEffectiveSessionTickError(_ERR_GRAPH)

        if self.status is PromotedEffectiveSessionTickStatus.VERIFIED_EXACT_SESSION_ONLY:
            if self.reason_codes != _VERIFIED_REASONS:
                raise PromotedEffectiveSessionTickError(_ERR_GRAPH)
            tick_entry = self.source_observation.tick_entry
            if tick_entry is None or tick_entry.observation is None:
                raise PromotedEffectiveSessionTickError(_ERR_GRAPH)
            universe_entry = tick_entry.frame_entry.universe_entry
            if (
                universe_entry.stable_instrument_id != self.stable_instrument_id
                or universe_entry.stable_listing_id != self.stable_listing_id
            ):
                raise PromotedEffectiveSessionTickError(_ERR_GRAPH)
            if tick_entry.effective_interval_verified is not False:
                raise PromotedEffectiveSessionTickError(_ERR_GRAPH)
            spec = self.tick_specification
            if (
                spec is None
                or spec.instrument_id != self.stable_instrument_id
                or spec.listing_id != self.stable_listing_id
                or spec.effective_from_session != self.market_session
                or spec.effective_to_exclusive != self.market_session + _ONE_DAY
                or spec.tick_size != tick_entry.observation.tick_size_rupees
                or spec.readiness is not ReferenceReadiness.POINT_IN_TIME_VERIFIED
            ):
                raise PromotedEffectiveSessionTickError(_ERR_GRAPH)
        else:
            if (
                self.reason_codes != _BLOCKED_REASONS
                or self.tick_specification is not None
                or self.source_observation.tick_entry is not None
            ):
                raise PromotedEffectiveSessionTickError(_ERR_GRAPH)

    def _identity(self) -> dict[str, object]:
        return {
            "stable_instrument_id": self.stable_instrument_id,
            "stable_listing_id": self.stable_listing_id,
            "market_session": self.market_session,
            "status": self.status,
            "source_observation": self.source_observation._identity(),
            "tick_specification_id": (
                None
                if self.tick_specification is None
                else self.tick_specification.specification_id
            ),
            "reason_codes": self.reason_codes,
        }


def _panel_identity(
    *,
    source_panel_id: str,
    cutoff: datetime,
    knowledge_time: datetime,
    results: tuple[PromotedEffectiveSessionTickResult, ...],
    status_counts: tuple[tuple[str, int], ...],
    reason_counts: tuple[tuple[str, int], ...],
    resolved_histories_tick_coverage_complete: bool,
    readiness: ReferenceReadiness,
    actionable: bool,
    training_eligible: bool,
    feature_eligible: bool,
    alert_eligible: bool,
    execution_eligible: bool,
) -> dict[str, object]:
    return {
        "schema_version": PROMOTED_EFFECTIVE_SESSION_TICK_SCHEMA_VERSION,
        "policy_version": PROMOTED_EFFECTIVE_SESSION_TICK_POLICY_VERSION,
        "source_panel_id": source_panel_id,
        "cutoff": cutoff,
        "knowledge_time": knowledge_time,
        "results": tuple(value._identity() for value in results),
        "status_counts": status_counts,
        "reason_counts": reason_counts,
        "resolved_histories_tick_coverage_complete": (
            resolved_histories_tick_coverage_complete
        ),
        "readiness": readiness,
        "actionable": actionable,
        "training_eligible": training_eligible,
        "feature_eligible": feature_eligible,
        "alert_eligible": alert_eligible,
        "execution_eligible": execution_eligible,
    }


@dataclass(frozen=True)
class _EffectiveSessionFacts:
    """Plain normalized facts returned by the single strict decoder. Never a
    VerifiedPromotedEffectiveSessionTickPanel."""

    cutoff: datetime
    knowledge_time: datetime
    results: tuple[PromotedEffectiveSessionTickResult, ...]
    status_counts: tuple[tuple[str, int], ...]
    reason_counts: tuple[tuple[str, int], ...]
    resolved_histories_tick_coverage_complete: bool
    readiness: ReferenceReadiness
    actionable: bool
    training_eligible: bool
    feature_eligible: bool
    alert_eligible: bool
    execution_eligible: bool
    panel_id: str


def _result_for(
    stable_instrument_id: str,
    stable_listing_id: str,
    observation: PromotedStableListingHistoryObservation,
    snapshots_by_session: dict[date, VerifiedPromotedSessionTickSnapshot],
) -> PromotedEffectiveSessionTickResult:
    tick_entry = observation.tick_entry
    if tick_entry is None:
        return PromotedEffectiveSessionTickResult(
            stable_instrument_id=stable_instrument_id,
            stable_listing_id=stable_listing_id,
            market_session=observation.market_session,
            status=PromotedEffectiveSessionTickStatus.MISSING_OBSERVATION_BLOCKED,
            source_observation=observation,
            tick_specification=None,
            reason_codes=_BLOCKED_REASONS,
        )
    if tick_entry.observation is None:
        raise PromotedEffectiveSessionTickError(_ERR_GRAPH)
    snapshot = snapshots_by_session.get(observation.market_session)
    if (
        type(snapshot) is not VerifiedPromotedSessionTickSnapshot
        or snapshot.market_session != observation.market_session
    ):
        raise PromotedEffectiveSessionTickError(_ERR_GRAPH)
    source = tick_entry.frame_entry.universe_entry
    tick_observation = tick_entry.observation
    matching_entries = tuple(
        value
        for value in snapshot.entries
        if value.source_record_id == tick_entry.source_record_id
    )
    if (
        len(matching_entries) != 1
        or matching_entries[0]._identity() != tick_entry._identity()
        or tick_observation.market_session_claim != observation.market_session
        or tick_entry.source_record_id != source.source_record_id
        or tick_observation.source_record_id != source.source_record_id
        or tick_observation.financial_instrument_id
        != source.financial_instrument_id
        or tick_observation.symbol != source.symbol
        or tick_observation.series != source.series
        or tick_observation.validated_isin != source.validated_isin
    ):
        raise PromotedEffectiveSessionTickError(_ERR_GRAPH)
    spec = EffectiveTickSize(
        instrument_id=stable_instrument_id,
        listing_id=stable_listing_id,
        effective_from_session=observation.market_session,
        effective_to_exclusive=observation.market_session + _ONE_DAY,
        tick_size=tick_entry.observation.tick_size_rupees,
        knowledge_time=snapshot.knowledge_time,
        source_snapshot_id=snapshot.snapshot_id,
        readiness=ReferenceReadiness.POINT_IN_TIME_VERIFIED,
    )
    return PromotedEffectiveSessionTickResult(
        stable_instrument_id=stable_instrument_id,
        stable_listing_id=stable_listing_id,
        market_session=observation.market_session,
        status=PromotedEffectiveSessionTickStatus.VERIFIED_EXACT_SESSION_ONLY,
        source_observation=observation,
        tick_specification=spec,
        reason_codes=_VERIFIED_REASONS,
    )


def _build_effective_session_facts(
    source_panel: VerifiedPromotedStableListingHistoryPanel,
    cutoff: datetime,
) -> _EffectiveSessionFacts:
    """The single strict effective-session-tick derivation routine.

    Requires an exact VerifiedPromotedStableListingHistoryPanel,
    independently replays its own content identity, requires it to remain
    COLLECTION_ONLY and fully non-eligible, requires cutoff at or after the
    source panel's own knowledge_time, and produces exactly one
    PromotedEffectiveSessionTickResult per (stable_instrument_id,
    stable_listing_id, market_session) cell already present in
    source_panel.histories x source_panel.sessions. Never constructs
    VerifiedPromotedEffectiveSessionTickPanel.

    The retained source-panel graph is treated as untrusted at every replay:
    every step below deliberately catches ordinary ``Exception`` (never
    ``BaseException``, so ``KeyboardInterrupt``/``SystemExit`` still
    propagate) so a malformed nested field fails closed with one static
    sanitized error instead of leaking a raw nested exception.
    """

    if type(source_panel) is not VerifiedPromotedStableListingHistoryPanel:
        raise PromotedEffectiveSessionTickError(_ERR_SOURCE_PANEL)

    cutoff = _utc(cutoff)

    try:
        source_panel.verify_content_identity()
    except Exception:
        raise PromotedEffectiveSessionTickError(_ERR_SOURCE_PANEL_VERIFY) from None

    if (
        source_panel.readiness is not ReferenceReadiness.COLLECTION_ONLY
        or source_panel.actionable is not False
        or source_panel.training_eligible is not False
        or source_panel.feature_eligible is not False
        or source_panel.alert_eligible is not False
        or source_panel.execution_eligible is not False
    ):
        raise PromotedEffectiveSessionTickError(_ERR_SOURCE_PANEL_STATE)

    if cutoff < source_panel.knowledge_time:
        raise PromotedEffectiveSessionTickError(_ERR_FUTURE)

    snapshots_by_session = {
        value.market_session: value for value in source_panel.tick_snapshots
    }
    if len(snapshots_by_session) != len(source_panel.tick_snapshots):
        raise PromotedEffectiveSessionTickError(_ERR_GRAPH)

    try:
        results: list[PromotedEffectiveSessionTickResult] = []
        for history in source_panel.histories:
            if tuple(
                value.market_session for value in history.observations
            ) != source_panel.sessions:
                raise PromotedEffectiveSessionTickError(_ERR_GRAPH)
            for observation in history.observations:
                results.append(
                    _result_for(
                        history.stable_instrument_id,
                        history.stable_listing_id,
                        observation,
                        snapshots_by_session,
                    )
                )
    except PromotedEffectiveSessionTickError:
        raise
    except Exception:
        raise PromotedEffectiveSessionTickError(_ERR_GRAPH) from None

    results_tuple = tuple(
        sorted(
            results,
            key=lambda value: (
                value.stable_instrument_id,
                value.stable_listing_id,
                value.market_session,
            ),
        )
    )
    expected_count = sum(len(value.observations) for value in source_panel.histories)
    if len(results_tuple) != expected_count:
        raise PromotedEffectiveSessionTickError(_ERR_GRAPH)

    status_totals: dict[str, int] = {}
    reason_totals: dict[str, int] = {}
    for result in results_tuple:
        status_totals[result.status.value] = status_totals.get(result.status.value, 0) + 1
        for reason in result.reason_codes:
            reason_totals[reason] = reason_totals.get(reason, 0) + 1
    status_counts = tuple(sorted(status_totals.items()))
    reason_counts = tuple(sorted(reason_totals.items()))

    resolved_histories_tick_coverage_complete = bool(source_panel.histories) and all(
        value.status is PromotedEffectiveSessionTickStatus.VERIFIED_EXACT_SESSION_ONLY
        for value in results_tuple
    )

    knowledge_time = source_panel.knowledge_time
    readiness = ReferenceReadiness.COLLECTION_ONLY
    actionable = False
    training_eligible = False
    feature_eligible = False
    alert_eligible = False
    execution_eligible = False

    panel_id = content_id(
        _panel_identity(
            source_panel_id=source_panel.panel_id,
            cutoff=cutoff,
            knowledge_time=knowledge_time,
            results=results_tuple,
            status_counts=status_counts,
            reason_counts=reason_counts,
            resolved_histories_tick_coverage_complete=(
                resolved_histories_tick_coverage_complete
            ),
            readiness=readiness,
            actionable=actionable,
            training_eligible=training_eligible,
            feature_eligible=feature_eligible,
            alert_eligible=alert_eligible,
            execution_eligible=execution_eligible,
        ),
        length=64,
    )

    return _EffectiveSessionFacts(
        cutoff=cutoff,
        knowledge_time=knowledge_time,
        results=results_tuple,
        status_counts=status_counts,
        reason_counts=reason_counts,
        resolved_histories_tick_coverage_complete=(
            resolved_histories_tick_coverage_complete
        ),
        readiness=readiness,
        actionable=actionable,
        training_eligible=training_eligible,
        feature_eligible=feature_eligible,
        alert_eligible=alert_eligible,
        execution_eligible=execution_eligible,
        panel_id=panel_id,
    )


@dataclass(frozen=True, slots=True)
class VerifiedPromotedEffectiveSessionTickPanel:
    """Immutable, content-addressed conversion of exact promoted same-session
    BidIntrvl observations into point-in-time verified EffectiveTickSize
    specifications, each bounded to the one calendar date it was actually
    observed on.

    ``resolved_histories_tick_coverage_complete`` is true only when the
    source panel has at least one resolved history and every produced
    result is VERIFIED_EXACT_SESSION_ONLY; it says nothing about whole-
    universe identity or tick coverage, since unresolved and source-excluded
    source rows are never converted into stable-listing tick specifications
    at all (they remain retained only on the source panel itself).
    ``readiness`` is always COLLECTION_ONLY and every eligibility flag is
    always False: this boundary grants no feature, signal, alert, or
    execution authority. __post_init__ calls verify_content_identity(),
    which requires the exact concrete type (rejecting subclasses/impostors
    even when every retained field is otherwise valid) and independently
    replays the complete derivation, so direct construction with a
    mismatched value or post-construction mutation anywhere in the retained
    graph fails closed with one static sanitized
    PromotedEffectiveSessionTickError.
    """

    schema_version: str
    policy_version: str
    source_panel: VerifiedPromotedStableListingHistoryPanel
    cutoff: datetime
    knowledge_time: datetime
    results: tuple[PromotedEffectiveSessionTickResult, ...]
    status_counts: tuple[tuple[str, int], ...]
    reason_counts: tuple[tuple[str, int], ...]
    resolved_histories_tick_coverage_complete: bool
    readiness: ReferenceReadiness
    actionable: bool
    training_eligible: bool
    feature_eligible: bool
    alert_eligible: bool
    execution_eligible: bool
    panel_id: str

    def __post_init__(self) -> None:
        self.verify_content_identity()

    def verify_content_identity(self) -> None:
        if type(self) is not VerifiedPromotedEffectiveSessionTickPanel:
            raise PromotedEffectiveSessionTickError(_ERR_TYPE)
        if (
            type(self.schema_version) is not str
            or self.schema_version != PROMOTED_EFFECTIVE_SESSION_TICK_SCHEMA_VERSION
        ):
            raise PromotedEffectiveSessionTickError(_ERR_SCHEMA_VERSION)
        if (
            type(self.policy_version) is not str
            or self.policy_version != PROMOTED_EFFECTIVE_SESSION_TICK_POLICY_VERSION
        ):
            raise PromotedEffectiveSessionTickError(_ERR_SCHEMA_VERSION)
        if type(self.source_panel) is not VerifiedPromotedStableListingHistoryPanel:
            raise PromotedEffectiveSessionTickError(_ERR_SOURCE_PANEL)
        if type(self.cutoff) is not datetime:
            raise PromotedEffectiveSessionTickError(_ERR_CUTOFF)
        if type(self.knowledge_time) is not datetime:
            raise PromotedEffectiveSessionTickError(_ERR_DERIVED_FIELD)
        if type(self.results) is not tuple or any(
            type(value) is not PromotedEffectiveSessionTickResult for value in self.results
        ):
            raise PromotedEffectiveSessionTickError(_ERR_DERIVED_FIELD)
        for values in (self.status_counts, self.reason_counts):
            if type(values) is not tuple or any(
                type(pair) is not tuple
                or len(pair) != 2
                or type(pair[0]) is not str
                or type(pair[1]) is not int
                for pair in values
            ):
                raise PromotedEffectiveSessionTickError(_ERR_DERIVED_FIELD)
        if (
            type(self.resolved_histories_tick_coverage_complete) is not bool
        ):
            raise PromotedEffectiveSessionTickError(_ERR_DERIVED_FIELD)
        if (
            type(self.readiness) is not ReferenceReadiness
            or self.readiness is not ReferenceReadiness.COLLECTION_ONLY
        ):
            raise PromotedEffectiveSessionTickError(_ERR_DERIVED_FIELD)
        for flag_name in (
            "actionable",
            "training_eligible",
            "feature_eligible",
            "alert_eligible",
            "execution_eligible",
        ):
            value = getattr(self, flag_name)
            if type(value) is not bool or value is not False:
                raise PromotedEffectiveSessionTickError(_ERR_DERIVED_FIELD)
        if type(self.panel_id) is not str or _SHA256.fullmatch(self.panel_id) is None:
            raise PromotedEffectiveSessionTickError(_ERR_PANEL_ID)

        try:
            facts = _build_effective_session_facts(self.source_panel, self.cutoff)
        except PromotedEffectiveSessionTickError:
            raise
        except Exception:
            raise PromotedEffectiveSessionTickError(_ERR_DERIVED_FIELD) from None

        # Every comparison below may recursively invoke a retained nested
        # value's own __eq__, so a single fail-closed boundary wraps all of
        # them: an already-raised PromotedEffectiveSessionTickError from an
        # intentional mismatch below propagates unchanged, while any other
        # ordinary Exception raised by equality itself (never BaseException/
        # KeyboardInterrupt/SystemExit) is translated to one static
        # sanitized message.
        try:
            if self.cutoff != facts.cutoff:
                raise PromotedEffectiveSessionTickError(_ERR_DERIVED_FIELD)
            if self.knowledge_time != facts.knowledge_time:
                raise PromotedEffectiveSessionTickError(_ERR_DERIVED_FIELD)
            if self.results != facts.results:
                raise PromotedEffectiveSessionTickError(_ERR_DERIVED_FIELD)
            if self.status_counts != facts.status_counts:
                raise PromotedEffectiveSessionTickError(_ERR_DERIVED_FIELD)
            if self.reason_counts != facts.reason_counts:
                raise PromotedEffectiveSessionTickError(_ERR_DERIVED_FIELD)
            if (
                self.resolved_histories_tick_coverage_complete
                != facts.resolved_histories_tick_coverage_complete
            ):
                raise PromotedEffectiveSessionTickError(_ERR_DERIVED_FIELD)
            if self.panel_id != facts.panel_id:
                raise PromotedEffectiveSessionTickError(_ERR_PANEL_ID)
        except PromotedEffectiveSessionTickError:
            raise
        except Exception:
            raise PromotedEffectiveSessionTickError(_ERR_COMPARISON_FAILED) from None


class PromotedEffectiveSessionTickService:
    """Converts exact promoted same-session BidIntrvl observations for
    resolved stable listings into point-in-time verified EffectiveTickSize
    specifications bounded to that one observed calendar date.

    Never merges, extends, or makes open-ended a tick-size interval. Never
    forward-fills, backfills, interpolates, copies an adjacent value, or
    selects a nearest/latest observation for a missing cell. Never upgrades
    the source PromotedSessionTickEntry or VerifiedPromotedSessionTickSnapshot
    readiness or its own effective_interval_verified flag.
    """

    def materialize(
        self,
        *,
        source_panel: VerifiedPromotedStableListingHistoryPanel,
        cutoff: datetime,
    ) -> VerifiedPromotedEffectiveSessionTickPanel:
        facts = _build_effective_session_facts(source_panel, cutoff)
        return VerifiedPromotedEffectiveSessionTickPanel(
            schema_version=PROMOTED_EFFECTIVE_SESSION_TICK_SCHEMA_VERSION,
            policy_version=PROMOTED_EFFECTIVE_SESSION_TICK_POLICY_VERSION,
            source_panel=source_panel,
            cutoff=facts.cutoff,
            knowledge_time=facts.knowledge_time,
            results=facts.results,
            status_counts=facts.status_counts,
            reason_counts=facts.reason_counts,
            resolved_histories_tick_coverage_complete=(
                facts.resolved_histories_tick_coverage_complete
            ),
            readiness=facts.readiness,
            actionable=facts.actionable,
            training_eligible=facts.training_eligible,
            feature_eligible=facts.feature_eligible,
            alert_eligible=facts.alert_eligible,
            execution_eligible=facts.execution_eligible,
            panel_id=facts.panel_id,
        )
