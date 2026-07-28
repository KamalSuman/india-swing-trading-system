"""Cross-session raw history assembly from promoted session evidence.

This module creates a diagnostic, collection-only panel.  It preserves full
calendar-session coverage, unresolved identities, source exclusions, orphan
bars, and all missing observations.  It never interpolates prices, adjusts for
corporate actions, or creates signal/training/execution authority.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum

from india_swing.calendar_data import CollectionCalendarMaterialization
from india_swing.identity import content_id
from india_swing.market_data.promoted_session_frame import (
    PromotedSessionBarStatus,
    PromotedSessionMarketDataOrphan,
)
from india_swing.reference.models import ReferenceReadiness
from india_swing.tick_sizes.promoted_session import (
    PromotedSessionTickEntry,
    VerifiedPromotedSessionTickSnapshot,
)


class PromotedStableListingHistoryError(ValueError):
    pass


PROMOTED_STABLE_LISTING_HISTORY_SCHEMA_VERSION = (
    "promoted-stable-listing-history-panel/v1"
)
PROMOTED_STABLE_LISTING_HISTORY_POLICY_VERSION = (
    "promoted-stable-listing-history/raw-unadjusted-no-gap-inference-v1"
)
PROMOTED_STABLE_LISTING_HISTORY_PRICE_BASIS = "RAW_UNADJUSTED"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_REASON = re.compile(r"[A-Z][A-Z0-9_]{2,127}\Z")

_ERR_TYPE = "promoted stable-listing history type is invalid"
_ERR_INPUT = "promoted stable-listing history input is invalid"
_ERR_VERIFY = "promoted stable-listing history input could not be verified"
_ERR_CALENDAR = "promoted stable-listing history calendar is invalid"
_ERR_SESSION = "promoted stable-listing history session coverage is invalid"
_ERR_CUTOFF = "promoted stable-listing history cutoff is invalid"
_ERR_FUTURE = "promoted stable-listing history contains future-known evidence"
_ERR_GRAPH = "promoted stable-listing history graph is invalid"
_ERR_DERIVED = "promoted stable-listing history derived content is invalid"
_ERR_ID = "promoted stable-listing history identifier is invalid"
_ERR_COMPARISON = (
    "promoted stable-listing history retained content could not be verified"
)

_COMMON_REASONS = {
    "CORPORATE_ACTION_ADJUSTMENT_REQUIRED",
    "FEATURE_CALCULATION_NOT_AUTHORIZED",
    "RAW_UNADJUSTED_HISTORY_COLLECTION_ONLY",
}


def _utc(value: datetime) -> datetime:
    if type(value) is not datetime:
        raise PromotedStableListingHistoryError(_ERR_CUTOFF)
    try:
        offset = value.utcoffset()
    except Exception:
        raise PromotedStableListingHistoryError(_ERR_CUTOFF) from None
    if value.tzinfo is None or offset is None:
        raise PromotedStableListingHistoryError(_ERR_CUTOFF)
    return value.astimezone(timezone.utc)


def _reason_tuple(values: set[str]) -> tuple[str, ...]:
    result = tuple(sorted(values))
    if not result or any(_REASON.fullmatch(value) is None for value in result):
        raise PromotedStableListingHistoryError(_ERR_GRAPH)
    return result


class PromotedHistorySessionStatus(str, Enum):
    SNAPSHOT_PRESENT_COLLECTION_ONLY = "SNAPSHOT_PRESENT_COLLECTION_ONLY"
    SNAPSHOT_MISSING_NO_STATE_INFERENCE = "SNAPSHOT_MISSING_NO_STATE_INFERENCE"


class PromotedStableListingObservationStatus(str, Enum):
    RAW_BAR_OBSERVED = "RAW_BAR_OBSERVED"
    BAR_NOT_OBSERVED_NO_STATE_INFERENCE = "BAR_NOT_OBSERVED_NO_STATE_INFERENCE"
    BAR_IDENTITY_CONFLICT = "BAR_IDENTITY_CONFLICT"
    UNIVERSE_ROW_NOT_PRESENT_NO_STATE_INFERENCE = (
        "UNIVERSE_ROW_NOT_PRESENT_NO_STATE_INFERENCE"
    )
    SESSION_SNAPSHOT_MISSING_NO_STATE_INFERENCE = (
        "SESSION_SNAPSHOT_MISSING_NO_STATE_INFERENCE"
    )


class PromotedUnassignedHistoryCategory(str, Enum):
    IDENTITY_UNRESOLVED = "IDENTITY_UNRESOLVED"
    SOURCE_EXCLUDED = "SOURCE_EXCLUDED"


_OBSERVATION_REASON = {
    PromotedStableListingObservationStatus.RAW_BAR_OBSERVED: {
        "RAW_BAR_OBSERVED_NOT_ADJUSTED",
    },
    PromotedStableListingObservationStatus.BAR_NOT_OBSERVED_NO_STATE_INFERENCE: {
        "PRICE_BAR_NOT_OBSERVED_NO_STATE_INFERENCE",
    },
    PromotedStableListingObservationStatus.BAR_IDENTITY_CONFLICT: {
        "BAR_IDENTITY_CONFLICT",
    },
    PromotedStableListingObservationStatus.UNIVERSE_ROW_NOT_PRESENT_NO_STATE_INFERENCE: {
        "UNIVERSE_ROW_NOT_PRESENT_NO_STATE_INFERENCE",
    },
    PromotedStableListingObservationStatus.SESSION_SNAPSHOT_MISSING_NO_STATE_INFERENCE: {
        "SESSION_SNAPSHOT_MISSING_NO_STATE_INFERENCE",
    },
}


@dataclass(frozen=True, slots=True)
class PromotedHistorySessionBinding:
    market_session: date
    status: PromotedHistorySessionStatus
    tick_snapshot: VerifiedPromotedSessionTickSnapshot | None
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        if type(self.market_session) is not date:
            raise PromotedStableListingHistoryError(_ERR_GRAPH)
        if type(self.status) is not PromotedHistorySessionStatus:
            raise PromotedStableListingHistoryError(_ERR_GRAPH)
        if (
            self.tick_snapshot is not None
            and type(self.tick_snapshot) is not VerifiedPromotedSessionTickSnapshot
        ):
            raise PromotedStableListingHistoryError(_ERR_GRAPH)
        if self.status is PromotedHistorySessionStatus.SNAPSHOT_PRESENT_COLLECTION_ONLY:
            valid = (
                self.tick_snapshot is not None
                and self.tick_snapshot.market_session == self.market_session
            )
            expected = (
                "COLLECTION_ONLY_SESSION_SNAPSHOT_PRESENT",
                "RAW_UNADJUSTED_HISTORY_COLLECTION_ONLY",
            )
        else:
            valid = self.tick_snapshot is None
            expected = (
                "RAW_UNADJUSTED_HISTORY_COLLECTION_ONLY",
                "SESSION_SNAPSHOT_MISSING_NO_STATE_INFERENCE",
            )
        if not valid or self.reason_codes != expected:
            raise PromotedStableListingHistoryError(_ERR_GRAPH)

    def _identity(self) -> dict[str, object]:
        return {
            "market_session": self.market_session,
            "status": self.status,
            "tick_snapshot_id": (
                None if self.tick_snapshot is None else self.tick_snapshot.snapshot_id
            ),
            "reason_codes": self.reason_codes,
        }


def _observation_reasons(
    status: PromotedStableListingObservationStatus,
    tick_entry: PromotedSessionTickEntry | None,
) -> tuple[str, ...]:
    values = {*_COMMON_REASONS, *_OBSERVATION_REASON[status]}
    if tick_entry is not None:
        values.update(tick_entry.reason_codes)
    return _reason_tuple(values)


@dataclass(frozen=True, slots=True)
class PromotedStableListingHistoryObservation:
    market_session: date
    status: PromotedStableListingObservationStatus
    tick_entry: PromotedSessionTickEntry | None
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        if type(self.market_session) is not date:
            raise PromotedStableListingHistoryError(_ERR_GRAPH)
        if type(self.status) is not PromotedStableListingObservationStatus:
            raise PromotedStableListingHistoryError(_ERR_GRAPH)
        if self.tick_entry is not None and type(self.tick_entry) is not PromotedSessionTickEntry:
            raise PromotedStableListingHistoryError(_ERR_GRAPH)
        if self.reason_codes != _observation_reasons(self.status, self.tick_entry):
            raise PromotedStableListingHistoryError(_ERR_GRAPH)

        if self.tick_entry is None:
            if self.status not in (
                PromotedStableListingObservationStatus.UNIVERSE_ROW_NOT_PRESENT_NO_STATE_INFERENCE,
                PromotedStableListingObservationStatus.SESSION_SNAPSHOT_MISSING_NO_STATE_INFERENCE,
            ):
                raise PromotedStableListingHistoryError(_ERR_GRAPH)
            return

        universe_entry = self.tick_entry.frame_entry.universe_entry
        if (
            universe_entry.stable_instrument_id is None
            or universe_entry.stable_listing_id is None
            or self.tick_entry.observation is None
            or self.tick_entry.observation.market_session_claim != self.market_session
        ):
            raise PromotedStableListingHistoryError(_ERR_GRAPH)
        frame_status = self.tick_entry.frame_entry.status
        expected = {
            PromotedSessionBarStatus.RESOLVED_LISTING_BAR_OBSERVED: (
                PromotedStableListingObservationStatus.RAW_BAR_OBSERVED
            ),
            PromotedSessionBarStatus.RESOLVED_LISTING_BAR_NOT_OBSERVED: (
                PromotedStableListingObservationStatus.BAR_NOT_OBSERVED_NO_STATE_INFERENCE
            ),
            PromotedSessionBarStatus.LANE_BAR_IDENTITY_CONFLICT: (
                PromotedStableListingObservationStatus.BAR_IDENTITY_CONFLICT
            ),
        }.get(frame_status)
        if self.status is not expected:
            raise PromotedStableListingHistoryError(_ERR_GRAPH)

    @property
    def raw_bar_id(self) -> str | None:
        if self.tick_entry is None or self.tick_entry.frame_entry.bar is None:
            return None
        return self.tick_entry.frame_entry.bar.bar_id

    @property
    def bid_interval_paise(self) -> int | None:
        if self.tick_entry is None or self.tick_entry.observation is None:
            return None
        return self.tick_entry.observation.bid_interval_paise

    def _identity(self) -> dict[str, object]:
        return {
            "market_session": self.market_session,
            "status": self.status,
            "tick_entry": (
                None if self.tick_entry is None else self.tick_entry._identity()
            ),
            "raw_bar_id": self.raw_bar_id,
            "bid_interval_paise": self.bid_interval_paise,
            "reason_codes": self.reason_codes,
        }


@dataclass(frozen=True, slots=True)
class PromotedStableListingHistory:
    stable_instrument_id: str
    stable_listing_id: str
    observations: tuple[PromotedStableListingHistoryObservation, ...]
    raw_bar_count: int
    gap_count: int
    identity_conflict_count: int
    price_basis: str
    corporate_action_adjusted: bool
    feature_eligible: bool
    reason_codes: tuple[str, ...]
    history_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value in (self.stable_instrument_id, self.stable_listing_id):
            if type(value) is not str or _SHA256.fullmatch(value) is None:
                raise PromotedStableListingHistoryError(_ERR_GRAPH)
        if (
            type(self.observations) is not tuple
            or not self.observations
            or any(
                type(value) is not PromotedStableListingHistoryObservation
                for value in self.observations
            )
        ):
            raise PromotedStableListingHistoryError(_ERR_GRAPH)
        sessions = tuple(value.market_session for value in self.observations)
        if sessions != tuple(sorted(set(sessions))):
            raise PromotedStableListingHistoryError(_ERR_GRAPH)
        for value in self.observations:
            if value.tick_entry is not None:
                source = value.tick_entry.frame_entry.universe_entry
                if (
                    source.stable_instrument_id != self.stable_instrument_id
                    or source.stable_listing_id != self.stable_listing_id
                ):
                    raise PromotedStableListingHistoryError(_ERR_GRAPH)
        expected_raw = sum(
            value.status is PromotedStableListingObservationStatus.RAW_BAR_OBSERVED
            for value in self.observations
        )
        expected_conflicts = sum(
            value.status is PromotedStableListingObservationStatus.BAR_IDENTITY_CONFLICT
            for value in self.observations
        )
        expected_gaps = len(self.observations) - expected_raw - expected_conflicts
        if (
            type(self.raw_bar_count) is not int
            or self.raw_bar_count != expected_raw
            or type(self.gap_count) is not int
            or self.gap_count != expected_gaps
            or type(self.identity_conflict_count) is not int
            or self.identity_conflict_count != expected_conflicts
        ):
            raise PromotedStableListingHistoryError(_ERR_GRAPH)
        if (
            self.price_basis != PROMOTED_STABLE_LISTING_HISTORY_PRICE_BASIS
            or type(self.corporate_action_adjusted) is not bool
            or self.corporate_action_adjusted is not False
            or type(self.feature_eligible) is not bool
            or self.feature_eligible is not False
            or self.reason_codes != tuple(sorted(_COMMON_REASONS))
        ):
            raise PromotedStableListingHistoryError(_ERR_GRAPH)
        object.__setattr__(
            self,
            "history_id",
            content_id(
                {
                    "schema": "promoted-stable-listing-history/v1",
                    "stable_instrument_id": self.stable_instrument_id,
                    "stable_listing_id": self.stable_listing_id,
                    "observations": tuple(
                        value._identity() for value in self.observations
                    ),
                    "raw_bar_count": self.raw_bar_count,
                    "gap_count": self.gap_count,
                    "identity_conflict_count": self.identity_conflict_count,
                    "price_basis": self.price_basis,
                    "corporate_action_adjusted": self.corporate_action_adjusted,
                    "feature_eligible": self.feature_eligible,
                    "reason_codes": self.reason_codes,
                },
                length=64,
            ),
        )

    def verify_content_identity(self) -> None:
        expected = PromotedStableListingHistory(
            stable_instrument_id=self.stable_instrument_id,
            stable_listing_id=self.stable_listing_id,
            observations=self.observations,
            raw_bar_count=self.raw_bar_count,
            gap_count=self.gap_count,
            identity_conflict_count=self.identity_conflict_count,
            price_basis=self.price_basis,
            corporate_action_adjusted=self.corporate_action_adjusted,
            feature_eligible=self.feature_eligible,
            reason_codes=self.reason_codes,
        )
        if self.history_id != expected.history_id:
            raise PromotedStableListingHistoryError(_ERR_ID)


@dataclass(frozen=True, slots=True)
class PromotedUnassignedHistoryEntry:
    market_session: date
    tick_snapshot_id: str
    category: PromotedUnassignedHistoryCategory
    tick_entry: PromotedSessionTickEntry
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        if type(self.market_session) is not date:
            raise PromotedStableListingHistoryError(_ERR_GRAPH)
        if type(self.tick_snapshot_id) is not str or _SHA256.fullmatch(self.tick_snapshot_id) is None:
            raise PromotedStableListingHistoryError(_ERR_GRAPH)
        if type(self.category) is not PromotedUnassignedHistoryCategory:
            raise PromotedStableListingHistoryError(_ERR_GRAPH)
        if type(self.tick_entry) is not PromotedSessionTickEntry:
            raise PromotedStableListingHistoryError(_ERR_GRAPH)
        source = self.tick_entry.frame_entry.universe_entry
        if self.category is PromotedUnassignedHistoryCategory.IDENTITY_UNRESOLVED:
            valid = source.stable_instrument_id is None and self.tick_entry.observation is not None
            extra = "STABLE_IDENTITY_UNRESOLVED_HISTORY_ENTRY"
        else:
            valid = self.tick_entry.observation is None
            extra = "SOURCE_EXCLUDED_HISTORY_ENTRY"
        expected = _reason_tuple({*self.tick_entry.reason_codes, extra})
        if not valid or self.reason_codes != expected:
            raise PromotedStableListingHistoryError(_ERR_GRAPH)

    def _identity(self) -> dict[str, object]:
        return {
            "market_session": self.market_session,
            "tick_snapshot_id": self.tick_snapshot_id,
            "category": self.category,
            "tick_entry": self.tick_entry._identity(),
            "reason_codes": self.reason_codes,
        }


@dataclass(frozen=True, slots=True)
class PromotedHistoryOrphanBar:
    market_session: date
    tick_snapshot_id: str
    orphan: PromotedSessionMarketDataOrphan
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        if type(self.market_session) is not date:
            raise PromotedStableListingHistoryError(_ERR_GRAPH)
        if type(self.tick_snapshot_id) is not str or _SHA256.fullmatch(self.tick_snapshot_id) is None:
            raise PromotedStableListingHistoryError(_ERR_GRAPH)
        if type(self.orphan) is not PromotedSessionMarketDataOrphan:
            raise PromotedStableListingHistoryError(_ERR_GRAPH)
        expected = _reason_tuple(
            {
                *self.orphan.reason_codes,
                "ORPHAN_BAR_RETAINED_IN_RAW_HISTORY",
            }
        )
        if self.reason_codes != expected or self.orphan.bar.session != self.market_session:
            raise PromotedStableListingHistoryError(_ERR_GRAPH)

    def _identity(self) -> dict[str, object]:
        return {
            "market_session": self.market_session,
            "tick_snapshot_id": self.tick_snapshot_id,
            "orphan": self.orphan._identity(),
            "reason_codes": self.reason_codes,
        }


@dataclass(frozen=True)
class _PanelFacts:
    cutoff: datetime
    knowledge_time: datetime
    sessions: tuple[date, ...]
    session_bindings: tuple[PromotedHistorySessionBinding, ...]
    histories: tuple[PromotedStableListingHistory, ...]
    unassigned_entries: tuple[PromotedUnassignedHistoryEntry, ...]
    orphan_bars: tuple[PromotedHistoryOrphanBar, ...]
    session_status_counts: tuple[tuple[str, int], ...]
    observation_status_counts: tuple[tuple[str, int], ...]
    unassigned_category_counts: tuple[tuple[str, int], ...]
    readiness: ReferenceReadiness
    actionable: bool
    training_eligible: bool
    feature_eligible: bool
    alert_eligible: bool
    execution_eligible: bool
    panel_id: str


def _count(values: list[str]) -> tuple[tuple[str, int], ...]:
    totals: dict[str, int] = {}
    for value in values:
        totals[value] = totals.get(value, 0) + 1
    return tuple(sorted(totals.items()))


def _session_grid(
    calendar: CollectionCalendarMaterialization,
    start: date,
    end: date,
) -> tuple[date, ...]:
    try:
        calendar.calendar_snapshot.require_session(start)
        calendar.calendar_snapshot.require_session(end)
        return tuple(
            value.day
            for value in calendar.calendar_snapshot.days
            if start <= value.day <= end and value.is_session
        )
    except Exception:
        raise PromotedStableListingHistoryError(_ERR_SESSION) from None


def _history_observation(
    market_session: date,
    tick_entry: PromotedSessionTickEntry | None,
    *,
    snapshot_missing: bool,
) -> PromotedStableListingHistoryObservation:
    if snapshot_missing:
        status = (
            PromotedStableListingObservationStatus.SESSION_SNAPSHOT_MISSING_NO_STATE_INFERENCE
        )
    elif tick_entry is None:
        status = (
            PromotedStableListingObservationStatus.UNIVERSE_ROW_NOT_PRESENT_NO_STATE_INFERENCE
        )
    else:
        status = {
            PromotedSessionBarStatus.RESOLVED_LISTING_BAR_OBSERVED: (
                PromotedStableListingObservationStatus.RAW_BAR_OBSERVED
            ),
            PromotedSessionBarStatus.RESOLVED_LISTING_BAR_NOT_OBSERVED: (
                PromotedStableListingObservationStatus.BAR_NOT_OBSERVED_NO_STATE_INFERENCE
            ),
            PromotedSessionBarStatus.LANE_BAR_IDENTITY_CONFLICT: (
                PromotedStableListingObservationStatus.BAR_IDENTITY_CONFLICT
            ),
        }.get(tick_entry.frame_entry.status)
        if status is None:
            raise PromotedStableListingHistoryError(_ERR_GRAPH)
    return PromotedStableListingHistoryObservation(
        market_session=market_session,
        status=status,
        tick_entry=tick_entry,
        reason_codes=_observation_reasons(status, tick_entry),
    )


def _panel_identity(
    *,
    calendar_id: str,
    calendar_snapshot_id: str,
    tick_snapshot_ids: tuple[str, ...],
    cutoff: datetime,
    knowledge_time: datetime,
    sessions: tuple[date, ...],
    session_bindings: tuple[PromotedHistorySessionBinding, ...],
    histories: tuple[PromotedStableListingHistory, ...],
    unassigned_entries: tuple[PromotedUnassignedHistoryEntry, ...],
    orphan_bars: tuple[PromotedHistoryOrphanBar, ...],
    session_status_counts: tuple[tuple[str, int], ...],
    observation_status_counts: tuple[tuple[str, int], ...],
    unassigned_category_counts: tuple[tuple[str, int], ...],
    readiness: ReferenceReadiness,
    actionable: bool,
    training_eligible: bool,
    feature_eligible: bool,
    alert_eligible: bool,
    execution_eligible: bool,
) -> dict[str, object]:
    return {
        "schema_version": PROMOTED_STABLE_LISTING_HISTORY_SCHEMA_VERSION,
        "policy_version": PROMOTED_STABLE_LISTING_HISTORY_POLICY_VERSION,
        "calendar_id": calendar_id,
        "calendar_snapshot_id": calendar_snapshot_id,
        "tick_snapshot_ids": tick_snapshot_ids,
        "cutoff": cutoff,
        "knowledge_time": knowledge_time,
        "sessions": sessions,
        "session_bindings": tuple(value._identity() for value in session_bindings),
        "history_ids": tuple(value.history_id for value in histories),
        "unassigned_entries": tuple(value._identity() for value in unassigned_entries),
        "orphan_bars": tuple(value._identity() for value in orphan_bars),
        "session_status_counts": session_status_counts,
        "observation_status_counts": observation_status_counts,
        "unassigned_category_counts": unassigned_category_counts,
        "readiness": readiness,
        "actionable": actionable,
        "training_eligible": training_eligible,
        "feature_eligible": feature_eligible,
        "alert_eligible": alert_eligible,
        "execution_eligible": execution_eligible,
    }


def _build_panel_facts(
    tick_snapshots: tuple[VerifiedPromotedSessionTickSnapshot, ...],
    calendar: CollectionCalendarMaterialization,
    cutoff: datetime,
) -> _PanelFacts:
    if (
        type(tick_snapshots) is not tuple
        or len(tick_snapshots) < 2
        or any(
            type(value) is not VerifiedPromotedSessionTickSnapshot
            for value in tick_snapshots
        )
    ):
        raise PromotedStableListingHistoryError(_ERR_INPUT)
    if type(calendar) is not CollectionCalendarMaterialization:
        raise PromotedStableListingHistoryError(_ERR_CALENDAR)
    cutoff = _utc(cutoff)
    try:
        calendar.verify_content_identity()
        for value in tick_snapshots:
            value.verify_content_identity()
    except Exception:
        raise PromotedStableListingHistoryError(_ERR_VERIFY) from None
    if (
        calendar.readiness is not ReferenceReadiness.COLLECTION_ONLY
        or calendar.actionable is not False
        or (calendar.exchange, calendar.segment) != ("NSE", "CM")
    ):
        raise PromotedStableListingHistoryError(_ERR_CALENDAR)

    supplied_sessions = tuple(value.market_session for value in tick_snapshots)
    if supplied_sessions != tuple(sorted(set(supplied_sessions))):
        raise PromotedStableListingHistoryError(_ERR_SESSION)
    if len({value.snapshot_id for value in tick_snapshots}) != len(tick_snapshots):
        raise PromotedStableListingHistoryError(_ERR_INPUT)
    for value in tick_snapshots:
        if (
            value.readiness is not ReferenceReadiness.COLLECTION_ONLY
            or value.actionable is not False
            or value.training_eligible is not False
            or value.alert_eligible is not False
            or value.execution_eligible is not False
            or value.frame.universe.calendar.materialization_id
            != calendar.materialization_id
        ):
            raise PromotedStableListingHistoryError(_ERR_INPUT)
    if cutoff < calendar.cutoff or any(value.knowledge_time > cutoff for value in tick_snapshots):
        raise PromotedStableListingHistoryError(_ERR_FUTURE)

    sessions = _session_grid(calendar, supplied_sessions[0], supplied_sessions[-1])
    if not sessions or any(value not in sessions for value in supplied_sessions):
        raise PromotedStableListingHistoryError(_ERR_SESSION)
    by_session = {value.market_session: value for value in tick_snapshots}
    bindings = tuple(
        PromotedHistorySessionBinding(
            market_session=session,
            status=(
                PromotedHistorySessionStatus.SNAPSHOT_PRESENT_COLLECTION_ONLY
                if session in by_session
                else PromotedHistorySessionStatus.SNAPSHOT_MISSING_NO_STATE_INFERENCE
            ),
            tick_snapshot=by_session.get(session),
            reason_codes=(
                (
                    "COLLECTION_ONLY_SESSION_SNAPSHOT_PRESENT",
                    "RAW_UNADJUSTED_HISTORY_COLLECTION_ONLY",
                )
                if session in by_session
                else (
                    "RAW_UNADJUSTED_HISTORY_COLLECTION_ONLY",
                    "SESSION_SNAPSHOT_MISSING_NO_STATE_INFERENCE",
                )
            ),
        )
        for session in sessions
    )

    resolved_by_session: dict[
        date, dict[tuple[str, str], PromotedSessionTickEntry]
    ] = {}
    stable_keys: set[tuple[str, str]] = set()
    unassigned: list[PromotedUnassignedHistoryEntry] = []
    orphans: list[PromotedHistoryOrphanBar] = []
    for snapshot in tick_snapshots:
        current: dict[tuple[str, str], PromotedSessionTickEntry] = {}
        for entry in snapshot.entries:
            source = entry.frame_entry.universe_entry
            if source.stable_instrument_id is not None:
                if source.stable_listing_id is None or entry.observation is None:
                    raise PromotedStableListingHistoryError(_ERR_GRAPH)
                key = (source.stable_instrument_id, source.stable_listing_id)
                if key in current:
                    raise PromotedStableListingHistoryError(_ERR_GRAPH)
                current[key] = entry
                stable_keys.add(key)
            else:
                category = (
                    PromotedUnassignedHistoryCategory.IDENTITY_UNRESOLVED
                    if entry.observation is not None
                    else PromotedUnassignedHistoryCategory.SOURCE_EXCLUDED
                )
                extra = (
                    "STABLE_IDENTITY_UNRESOLVED_HISTORY_ENTRY"
                    if category is PromotedUnassignedHistoryCategory.IDENTITY_UNRESOLVED
                    else "SOURCE_EXCLUDED_HISTORY_ENTRY"
                )
                unassigned.append(
                    PromotedUnassignedHistoryEntry(
                        market_session=snapshot.market_session,
                        tick_snapshot_id=snapshot.snapshot_id,
                        category=category,
                        tick_entry=entry,
                        reason_codes=_reason_tuple({*entry.reason_codes, extra}),
                    )
                )
        resolved_by_session[snapshot.market_session] = current
        for orphan in snapshot.frame.orphan_bars:
            orphans.append(
                PromotedHistoryOrphanBar(
                    market_session=snapshot.market_session,
                    tick_snapshot_id=snapshot.snapshot_id,
                    orphan=orphan,
                    reason_codes=_reason_tuple(
                        {
                            *orphan.reason_codes,
                            "ORPHAN_BAR_RETAINED_IN_RAW_HISTORY",
                        }
                    ),
                )
            )

    histories: list[PromotedStableListingHistory] = []
    for stable_instrument_id, stable_listing_id in sorted(stable_keys):
        observations = tuple(
            _history_observation(
                session,
                resolved_by_session.get(session, {}).get(
                    (stable_instrument_id, stable_listing_id)
                ),
                snapshot_missing=session not in by_session,
            )
            for session in sessions
        )
        raw_count = sum(
            value.status is PromotedStableListingObservationStatus.RAW_BAR_OBSERVED
            for value in observations
        )
        conflict_count = sum(
            value.status is PromotedStableListingObservationStatus.BAR_IDENTITY_CONFLICT
            for value in observations
        )
        histories.append(
            PromotedStableListingHistory(
                stable_instrument_id=stable_instrument_id,
                stable_listing_id=stable_listing_id,
                observations=observations,
                raw_bar_count=raw_count,
                gap_count=len(observations) - raw_count - conflict_count,
                identity_conflict_count=conflict_count,
                price_basis=PROMOTED_STABLE_LISTING_HISTORY_PRICE_BASIS,
                corporate_action_adjusted=False,
                feature_eligible=False,
                reason_codes=tuple(sorted(_COMMON_REASONS)),
            )
        )
    histories_tuple = tuple(histories)
    unassigned_tuple = tuple(
        sorted(
            unassigned,
            key=lambda value: (
                value.market_session,
                value.tick_entry.source_record_id,
            ),
        )
    )
    orphan_tuple = tuple(
        sorted(
            orphans,
            key=lambda value: (
                value.market_session,
                value.orphan.bar.listing_lane,
            ),
        )
    )
    session_counts = _count([value.status.value for value in bindings])
    observation_counts = _count(
        [
            observation.status.value
            for history in histories_tuple
            for observation in history.observations
        ]
    )
    unassigned_counts = _count([value.category.value for value in unassigned_tuple])
    knowledge_time = max(
        calendar.cutoff,
        *(value.knowledge_time for value in tick_snapshots),
    )
    readiness = ReferenceReadiness.COLLECTION_ONLY
    actionable = training_eligible = feature_eligible = alert_eligible = execution_eligible = False
    panel_id = content_id(
        _panel_identity(
            calendar_id=calendar.materialization_id,
            calendar_snapshot_id=calendar.calendar_snapshot.snapshot_id,
            tick_snapshot_ids=tuple(value.snapshot_id for value in tick_snapshots),
            cutoff=cutoff,
            knowledge_time=knowledge_time,
            sessions=sessions,
            session_bindings=bindings,
            histories=histories_tuple,
            unassigned_entries=unassigned_tuple,
            orphan_bars=orphan_tuple,
            session_status_counts=session_counts,
            observation_status_counts=observation_counts,
            unassigned_category_counts=unassigned_counts,
            readiness=readiness,
            actionable=actionable,
            training_eligible=training_eligible,
            feature_eligible=feature_eligible,
            alert_eligible=alert_eligible,
            execution_eligible=execution_eligible,
        ),
        length=64,
    )
    return _PanelFacts(
        cutoff=cutoff,
        knowledge_time=knowledge_time,
        sessions=sessions,
        session_bindings=bindings,
        histories=histories_tuple,
        unassigned_entries=unassigned_tuple,
        orphan_bars=orphan_tuple,
        session_status_counts=session_counts,
        observation_status_counts=observation_counts,
        unassigned_category_counts=unassigned_counts,
        readiness=readiness,
        actionable=actionable,
        training_eligible=training_eligible,
        feature_eligible=feature_eligible,
        alert_eligible=alert_eligible,
        execution_eligible=execution_eligible,
        panel_id=panel_id,
    )


@dataclass(frozen=True, slots=True)
class VerifiedPromotedStableListingHistoryPanel:
    schema_version: str
    policy_version: str
    tick_snapshots: tuple[VerifiedPromotedSessionTickSnapshot, ...]
    calendar: CollectionCalendarMaterialization
    cutoff: datetime
    knowledge_time: datetime
    sessions: tuple[date, ...]
    session_bindings: tuple[PromotedHistorySessionBinding, ...]
    histories: tuple[PromotedStableListingHistory, ...]
    unassigned_entries: tuple[PromotedUnassignedHistoryEntry, ...]
    orphan_bars: tuple[PromotedHistoryOrphanBar, ...]
    session_status_counts: tuple[tuple[str, int], ...]
    observation_status_counts: tuple[tuple[str, int], ...]
    unassigned_category_counts: tuple[tuple[str, int], ...]
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
        if type(self) is not VerifiedPromotedStableListingHistoryPanel:
            raise PromotedStableListingHistoryError(_ERR_TYPE)
        if (
            self.schema_version != PROMOTED_STABLE_LISTING_HISTORY_SCHEMA_VERSION
            or self.policy_version != PROMOTED_STABLE_LISTING_HISTORY_POLICY_VERSION
        ):
            raise PromotedStableListingHistoryError(_ERR_DERIVED)
        if (
            type(self.tick_snapshots) is not tuple
            or type(self.calendar) is not CollectionCalendarMaterialization
            or type(self.cutoff) is not datetime
            or type(self.knowledge_time) is not datetime
            or type(self.sessions) is not tuple
            or any(type(value) is not date for value in self.sessions)
            or type(self.session_bindings) is not tuple
            or any(
                type(value) is not PromotedHistorySessionBinding
                for value in self.session_bindings
            )
            or type(self.histories) is not tuple
            or any(
                type(value) is not PromotedStableListingHistory
                for value in self.histories
            )
            or type(self.unassigned_entries) is not tuple
            or any(
                type(value) is not PromotedUnassignedHistoryEntry
                for value in self.unassigned_entries
            )
            or type(self.orphan_bars) is not tuple
            or any(
                type(value) is not PromotedHistoryOrphanBar
                for value in self.orphan_bars
            )
            or any(
                type(value) is not tuple
                for value in (
                    self.session_status_counts,
                    self.observation_status_counts,
                    self.unassigned_category_counts,
                )
            )
            or any(
                type(item) is not tuple
                or len(item) != 2
                or type(item[0]) is not str
                or type(item[1]) is not int
                for values in (
                    self.session_status_counts,
                    self.observation_status_counts,
                    self.unassigned_category_counts,
                )
                for item in values
            )
            or type(self.readiness) is not ReferenceReadiness
            or any(
                type(value) is not bool
                for value in (
                    self.actionable,
                    self.training_eligible,
                    self.feature_eligible,
                    self.alert_eligible,
                    self.execution_eligible,
                )
            )
        ):
            raise PromotedStableListingHistoryError(_ERR_DERIVED)
        if type(self.panel_id) is not str or _SHA256.fullmatch(self.panel_id) is None:
            raise PromotedStableListingHistoryError(_ERR_ID)
        try:
            facts = _build_panel_facts(
                self.tick_snapshots,
                self.calendar,
                self.cutoff,
            )
        except PromotedStableListingHistoryError:
            raise
        except Exception:
            raise PromotedStableListingHistoryError(_ERR_DERIVED) from None
        try:
            comparisons = (
                (self.cutoff, facts.cutoff),
                (self.knowledge_time, facts.knowledge_time),
                (self.sessions, facts.sessions),
                (self.session_bindings, facts.session_bindings),
                (self.histories, facts.histories),
                (self.unassigned_entries, facts.unassigned_entries),
                (self.orphan_bars, facts.orphan_bars),
                (self.session_status_counts, facts.session_status_counts),
                (self.observation_status_counts, facts.observation_status_counts),
                (self.unassigned_category_counts, facts.unassigned_category_counts),
                (self.readiness, facts.readiness),
                (self.actionable, facts.actionable),
                (self.training_eligible, facts.training_eligible),
                (self.feature_eligible, facts.feature_eligible),
                (self.alert_eligible, facts.alert_eligible),
                (self.execution_eligible, facts.execution_eligible),
                (self.panel_id, facts.panel_id),
            )
            if any(left != right for left, right in comparisons):
                raise PromotedStableListingHistoryError(_ERR_COMPARISON)
        except PromotedStableListingHistoryError:
            raise
        except Exception:
            raise PromotedStableListingHistoryError(_ERR_COMPARISON) from None


class PromotedStableListingHistoryService:
    def materialize(
        self,
        *,
        tick_snapshots: tuple[VerifiedPromotedSessionTickSnapshot, ...],
        calendar: CollectionCalendarMaterialization,
        cutoff: datetime,
    ) -> VerifiedPromotedStableListingHistoryPanel:
        facts = _build_panel_facts(tick_snapshots, calendar, cutoff)
        return VerifiedPromotedStableListingHistoryPanel(
            schema_version=PROMOTED_STABLE_LISTING_HISTORY_SCHEMA_VERSION,
            policy_version=PROMOTED_STABLE_LISTING_HISTORY_POLICY_VERSION,
            tick_snapshots=tick_snapshots,
            calendar=calendar,
            cutoff=facts.cutoff,
            knowledge_time=facts.knowledge_time,
            sessions=facts.sessions,
            session_bindings=facts.session_bindings,
            histories=facts.histories,
            unassigned_entries=facts.unassigned_entries,
            orphan_bars=facts.orphan_bars,
            session_status_counts=facts.session_status_counts,
            observation_status_counts=facts.observation_status_counts,
            unassigned_category_counts=facts.unassigned_category_counts,
            readiness=facts.readiness,
            actionable=facts.actionable,
            training_eligible=facts.training_eligible,
            feature_eligible=facts.feature_eligible,
            alert_eligible=facts.alert_eligible,
            execution_eligible=facts.execution_eligible,
            panel_id=facts.panel_id,
        )
