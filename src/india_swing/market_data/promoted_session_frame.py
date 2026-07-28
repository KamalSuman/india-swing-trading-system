"""Fail-closed session join between promoted identities and historical bars.

The resulting frame is diagnostic collection evidence.  It deliberately
preserves missing rows, unresolved identities, source exclusions, conflicts,
and corpus orphans instead of turning source presence into listing or trading
authority.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum

from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference_data.models import SourceRowDisposition
from india_swing.universe.promoted_identity import (
    PromotedIdentitySessionDisposition,
    PromotedIdentitySessionEntry,
    VerifiedPromotedIdentitySessionUniverse,
)

from .historical_corpus import (
    HistoricalEvaluationCorpusBar,
    HistoricalEvaluationCorpusIndex,
    HistoricalEvaluationCorpusSessionPartition,
)


class PromotedSessionMarketDataError(ValueError):
    pass


PROMOTED_SESSION_MARKET_DATA_SCHEMA_VERSION = (
    "promoted-session-market-data-frame/v1"
)
PROMOTED_SESSION_MARKET_DATA_POLICY_VERSION = (
    "promoted-session-market-data/no-absence-inference-v1"
)

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_REASON = re.compile(r"[A-Z][A-Z0-9_]{2,127}\Z")

_ERR_TYPE = "promoted session market-data type is invalid"
_ERR_UNIVERSE = "promoted session market-data universe is invalid"
_ERR_UNIVERSE_VERIFY = (
    "promoted session market-data could not verify the source universe"
)
_ERR_CORPUS = "promoted session market-data corpus is invalid"
_ERR_CORPUS_VERIFY = (
    "promoted session market-data could not verify the source corpus"
)
_ERR_PARTITION = "promoted session market-data partition is invalid"
_ERR_PARTITION_VERIFY = (
    "promoted session market-data could not verify the source partition"
)
_ERR_LINEAGE = "promoted session market-data lineage is inconsistent"
_ERR_CUTOFF = "promoted session market-data cutoff is invalid"
_ERR_FUTURE = "promoted session market-data contains future-known evidence"
_ERR_GRAPH = "promoted session market-data join graph is invalid"
_ERR_DERIVED = "promoted session market-data derived content is invalid"
_ERR_ID = "promoted session market-data identifier is invalid"
_ERR_COMPARISON = (
    "promoted session market-data retained content could not be verified"
)


def _utc(value: datetime) -> datetime:
    if type(value) is not datetime:
        raise PromotedSessionMarketDataError(_ERR_CUTOFF)
    try:
        offset = value.utcoffset()
    except Exception:
        raise PromotedSessionMarketDataError(_ERR_CUTOFF) from None
    if value.tzinfo is None or offset is None:
        raise PromotedSessionMarketDataError(_ERR_CUTOFF)
    return value.astimezone(timezone.utc)


class PromotedSessionBarStatus(str, Enum):
    RESOLVED_LISTING_BAR_OBSERVED = "RESOLVED_LISTING_BAR_OBSERVED"
    RESOLVED_LISTING_BAR_NOT_OBSERVED = "RESOLVED_LISTING_BAR_NOT_OBSERVED"
    CANDIDATE_BAR_OBSERVED_IDENTITY_UNRESOLVED = (
        "CANDIDATE_BAR_OBSERVED_IDENTITY_UNRESOLVED"
    )
    IDENTITY_UNRESOLVED_BAR_NOT_OBSERVED = (
        "IDENTITY_UNRESOLVED_BAR_NOT_OBSERVED"
    )
    LANE_BAR_IDENTITY_CONFLICT = "LANE_BAR_IDENTITY_CONFLICT"
    EXCLUDED_SOURCE_BAR_OBSERVED = "EXCLUDED_SOURCE_BAR_OBSERVED"
    EXCLUDED_SOURCE_BAR_NOT_OBSERVED = "EXCLUDED_SOURCE_BAR_NOT_OBSERVED"


_EQUITY_INPUT_BLOCKERS = {
    "CORPORATE_ACTION_ADJUSTMENT_UNAVAILABLE",
    "EFFECTIVE_TICK_SIZE_UNVERIFIED",
    "LIQUIDITY_HISTORY_UNVERIFIED",
}

_STATUS_REASONS = {
    PromotedSessionBarStatus.RESOLVED_LISTING_BAR_OBSERVED: {
        "STABLE_IDENTITY_BAR_MATCH_OBSERVED",
        *_EQUITY_INPUT_BLOCKERS,
    },
    PromotedSessionBarStatus.RESOLVED_LISTING_BAR_NOT_OBSERVED: {
        "PRICE_BAR_NOT_OBSERVED_NO_STATE_INFERENCE",
        *_EQUITY_INPUT_BLOCKERS,
    },
    PromotedSessionBarStatus.CANDIDATE_BAR_OBSERVED_IDENTITY_UNRESOLVED: {
        "CANDIDATE_BAR_OBSERVED_NOT_STABLE_BOUND",
        *_EQUITY_INPUT_BLOCKERS,
    },
    PromotedSessionBarStatus.IDENTITY_UNRESOLVED_BAR_NOT_OBSERVED: {
        "PRICE_BAR_NOT_OBSERVED_NO_STATE_INFERENCE",
        *_EQUITY_INPUT_BLOCKERS,
    },
    PromotedSessionBarStatus.LANE_BAR_IDENTITY_CONFLICT: {
        "BAR_IDENTITY_CONFLICT",
        *_EQUITY_INPUT_BLOCKERS,
    },
    PromotedSessionBarStatus.EXCLUDED_SOURCE_BAR_OBSERVED: {
        "SOURCE_EXCLUDED_BAR_OBSERVED_NOT_ELIGIBILITY_EVIDENCE",
    },
    PromotedSessionBarStatus.EXCLUDED_SOURCE_BAR_NOT_OBSERVED: {
        "PRICE_BAR_NOT_OBSERVED_NO_STATE_INFERENCE",
    },
}

_ORPHAN_REASONS = (
    "BAR_NOT_PRESENT_IN_SESSION_UNIVERSE",
    "COLLECTION_ONLY_SESSION_MARKET_DATA",
    "NO_STABLE_IDENTITY_BINDING",
)


def _entry_reasons(
    universe_entry: PromotedIdentitySessionEntry,
    status: PromotedSessionBarStatus,
) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                *universe_entry.reason_codes,
                "COLLECTION_ONLY_SESSION_MARKET_DATA",
                *_STATUS_REASONS[status],
            }
        )
    )


@dataclass(frozen=True, slots=True)
class PromotedSessionMarketDataEntry:
    """One retained universe row and its same-lane bar observation, if any."""

    universe_entry: PromotedIdentitySessionEntry
    status: PromotedSessionBarStatus
    bar: HistoricalEvaluationCorpusBar | None
    stable_identity_bound: bool
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        if type(self.universe_entry) is not PromotedIdentitySessionEntry:
            raise PromotedSessionMarketDataError(_ERR_GRAPH)
        if type(self.status) is not PromotedSessionBarStatus:
            raise PromotedSessionMarketDataError(_ERR_GRAPH)
        if self.bar is not None and type(self.bar) is not HistoricalEvaluationCorpusBar:
            raise PromotedSessionMarketDataError(_ERR_GRAPH)
        if self.bar is not None:
            try:
                self.bar.verify_content_identity()
            except Exception:
                raise PromotedSessionMarketDataError(_ERR_GRAPH) from None
        if type(self.stable_identity_bound) is not bool:
            raise PromotedSessionMarketDataError(_ERR_GRAPH)
        if (
            type(self.reason_codes) is not tuple
            or self.reason_codes != tuple(sorted(set(self.reason_codes)))
            or any(
                type(value) is not str or _REASON.fullmatch(value) is None
                for value in self.reason_codes
            )
            or self.reason_codes != _entry_reasons(self.universe_entry, self.status)
        ):
            raise PromotedSessionMarketDataError(_ERR_GRAPH)

        resolved = (
            self.universe_entry.disposition
            is PromotedIdentitySessionDisposition.IDENTITY_RESOLVED_COLLECTION_ONLY
        )
        unresolved = (
            self.universe_entry.disposition
            is PromotedIdentitySessionDisposition.IDENTITY_UNRESOLVED
        )
        excluded = not resolved and not unresolved
        if excluded and self.universe_entry.source_disposition is (
            SourceRowDisposition.RETAINED_UNVERIFIED_EQUITY
        ):
            raise PromotedSessionMarketDataError(_ERR_GRAPH)

        bar_present = self.bar is not None
        if bar_present:
            lane_matches = (
                self.bar.listing_key == f"NSE:{self.universe_entry.symbol}"
                and self.bar.series == self.universe_entry.series
            )
            if not lane_matches:
                raise PromotedSessionMarketDataError(_ERR_GRAPH)
            isin_matches = (
                self.universe_entry.validated_isin is not None
                and self.bar.isin == self.universe_entry.validated_isin
            )
        else:
            isin_matches = False

        if self.status is PromotedSessionBarStatus.RESOLVED_LISTING_BAR_OBSERVED:
            valid = resolved and bar_present and isin_matches and self.stable_identity_bound
        elif (
            self.status
            is PromotedSessionBarStatus.RESOLVED_LISTING_BAR_NOT_OBSERVED
        ):
            valid = resolved and not bar_present and not self.stable_identity_bound
        elif (
            self.status
            is PromotedSessionBarStatus.CANDIDATE_BAR_OBSERVED_IDENTITY_UNRESOLVED
        ):
            valid = (
                unresolved
                and bar_present
                and isin_matches
                and not self.stable_identity_bound
            )
        elif (
            self.status
            is PromotedSessionBarStatus.IDENTITY_UNRESOLVED_BAR_NOT_OBSERVED
        ):
            valid = unresolved and not bar_present and not self.stable_identity_bound
        elif self.status is PromotedSessionBarStatus.LANE_BAR_IDENTITY_CONFLICT:
            valid = (
                (resolved or unresolved)
                and bar_present
                and not isin_matches
                and not self.stable_identity_bound
            )
        elif self.status is PromotedSessionBarStatus.EXCLUDED_SOURCE_BAR_OBSERVED:
            valid = excluded and bar_present and not self.stable_identity_bound
        else:
            valid = excluded and not bar_present and not self.stable_identity_bound
        if not valid:
            raise PromotedSessionMarketDataError(_ERR_GRAPH)

    @property
    def source_record_id(self) -> str:
        return self.universe_entry.source_record_id

    def _identity(self) -> dict[str, object]:
        return {
            "universe_entry": self.universe_entry._identity(),
            "status": self.status,
            "bar_id": None if self.bar is None else self.bar.bar_id,
            "stable_identity_bound": self.stable_identity_bound,
            "reason_codes": self.reason_codes,
        }


@dataclass(frozen=True, slots=True)
class PromotedSessionMarketDataOrphan:
    """A corpus bar whose symbol/series lane is absent from the session master."""

    bar: HistoricalEvaluationCorpusBar
    reason_codes: tuple[str, ...] = _ORPHAN_REASONS

    def __post_init__(self) -> None:
        if type(self.bar) is not HistoricalEvaluationCorpusBar:
            raise PromotedSessionMarketDataError(_ERR_GRAPH)
        try:
            self.bar.verify_content_identity()
        except Exception:
            raise PromotedSessionMarketDataError(_ERR_GRAPH) from None
        if self.reason_codes != _ORPHAN_REASONS:
            raise PromotedSessionMarketDataError(_ERR_GRAPH)

    def _identity(self) -> dict[str, object]:
        return {
            "bar_id": self.bar.bar_id,
            "reason_codes": self.reason_codes,
        }


@dataclass(frozen=True)
class _SessionFrameFacts:
    cutoff: datetime
    knowledge_time: datetime
    entries: tuple[PromotedSessionMarketDataEntry, ...]
    orphan_bars: tuple[PromotedSessionMarketDataOrphan, ...]
    status_counts: tuple[tuple[str, int], ...]
    reason_counts: tuple[tuple[str, int], ...]
    readiness: ReferenceReadiness
    actionable: bool
    training_eligible: bool
    alert_eligible: bool
    execution_eligible: bool
    frame_id: str


def _frame_identity(
    *,
    universe_id: str,
    adjudication_id: str,
    identity_snapshot_id: str,
    calendar_materialization_id: str,
    calendar_snapshot_id: str,
    corpus_id: str,
    admission_report_id: str,
    reconciliation_index_id: str,
    partition_id: str,
    source_snapshot_ids: tuple[str, ...],
    source_report_ids: tuple[str, ...],
    market_session: date,
    cutoff: datetime,
    knowledge_time: datetime,
    entries: tuple[PromotedSessionMarketDataEntry, ...],
    orphan_bars: tuple[PromotedSessionMarketDataOrphan, ...],
    status_counts: tuple[tuple[str, int], ...],
    reason_counts: tuple[tuple[str, int], ...],
    readiness: ReferenceReadiness,
    actionable: bool,
    training_eligible: bool,
    alert_eligible: bool,
    execution_eligible: bool,
) -> dict[str, object]:
    return {
        "schema_version": PROMOTED_SESSION_MARKET_DATA_SCHEMA_VERSION,
        "policy_version": PROMOTED_SESSION_MARKET_DATA_POLICY_VERSION,
        "universe_id": universe_id,
        "adjudication_id": adjudication_id,
        "identity_snapshot_id": identity_snapshot_id,
        "calendar_materialization_id": calendar_materialization_id,
        "calendar_snapshot_id": calendar_snapshot_id,
        "corpus_id": corpus_id,
        "admission_report_id": admission_report_id,
        "reconciliation_index_id": reconciliation_index_id,
        "partition_id": partition_id,
        "source_snapshot_ids": source_snapshot_ids,
        "source_report_ids": source_report_ids,
        "market_session": market_session,
        "cutoff": cutoff,
        "knowledge_time": knowledge_time,
        "entries": tuple(value._identity() for value in entries),
        "orphan_bars": tuple(value._identity() for value in orphan_bars),
        "status_counts": status_counts,
        "reason_counts": reason_counts,
        "readiness": readiness,
        "actionable": actionable,
        "training_eligible": training_eligible,
        "alert_eligible": alert_eligible,
        "execution_eligible": execution_eligible,
    }


def _status_for(
    universe_entry: PromotedIdentitySessionEntry,
    bar: HistoricalEvaluationCorpusBar | None,
) -> tuple[PromotedSessionBarStatus, bool]:
    resolved = (
        universe_entry.disposition
        is PromotedIdentitySessionDisposition.IDENTITY_RESOLVED_COLLECTION_ONLY
    )
    unresolved = (
        universe_entry.disposition
        is PromotedIdentitySessionDisposition.IDENTITY_UNRESOLVED
    )
    if bar is None:
        if resolved:
            return (
                PromotedSessionBarStatus.RESOLVED_LISTING_BAR_NOT_OBSERVED,
                False,
            )
        if unresolved:
            return (
                PromotedSessionBarStatus.IDENTITY_UNRESOLVED_BAR_NOT_OBSERVED,
                False,
            )
        return PromotedSessionBarStatus.EXCLUDED_SOURCE_BAR_NOT_OBSERVED, False

    isin_matches = (
        universe_entry.validated_isin is not None
        and bar.isin == universe_entry.validated_isin
    )
    if resolved and isin_matches:
        return PromotedSessionBarStatus.RESOLVED_LISTING_BAR_OBSERVED, True
    if unresolved and isin_matches:
        return (
            PromotedSessionBarStatus.CANDIDATE_BAR_OBSERVED_IDENTITY_UNRESOLVED,
            False,
        )
    if resolved or unresolved:
        return PromotedSessionBarStatus.LANE_BAR_IDENTITY_CONFLICT, False
    return PromotedSessionBarStatus.EXCLUDED_SOURCE_BAR_OBSERVED, False


def _build_session_frame_facts(
    universe: VerifiedPromotedIdentitySessionUniverse,
    corpus_index: HistoricalEvaluationCorpusIndex,
    partition: HistoricalEvaluationCorpusSessionPartition,
    cutoff: datetime,
) -> _SessionFrameFacts:
    if type(universe) is not VerifiedPromotedIdentitySessionUniverse:
        raise PromotedSessionMarketDataError(_ERR_UNIVERSE)
    if type(corpus_index) is not HistoricalEvaluationCorpusIndex:
        raise PromotedSessionMarketDataError(_ERR_CORPUS)
    if type(partition) is not HistoricalEvaluationCorpusSessionPartition:
        raise PromotedSessionMarketDataError(_ERR_PARTITION)
    cutoff = _utc(cutoff)

    try:
        universe.verify_content_identity()
    except Exception:
        raise PromotedSessionMarketDataError(_ERR_UNIVERSE_VERIFY) from None
    try:
        corpus_index.verify_content_identity()
    except Exception:
        raise PromotedSessionMarketDataError(_ERR_CORPUS_VERIFY) from None
    try:
        partition.verify_content_identity()
    except Exception:
        raise PromotedSessionMarketDataError(_ERR_PARTITION_VERIFY) from None

    if (
        universe.readiness is not ReferenceReadiness.COLLECTION_ONLY
        or universe.actionable is not False
        or universe.execution_eligible is not False
        or corpus_index.collection_only is not True
        or corpus_index.actionable is not False
        or corpus_index.training_eligible is not False
        or partition.collection_only is not True
        or partition.actionable is not False
        or partition.training_eligible is not False
    ):
        raise PromotedSessionMarketDataError(_ERR_LINEAGE)

    matches = tuple(
        partition_id
        for session, partition_id in zip(
            corpus_index.partition_sessions,
            corpus_index.partition_ids,
            strict=True,
        )
        if session == universe.market_session
    )
    if (
        partition.market_session != universe.market_session
        or len(matches) != 1
        or matches[0] != partition.partition_id
    ):
        raise PromotedSessionMarketDataError(_ERR_LINEAGE)
    if cutoff < universe.knowledge_time or cutoff < corpus_index.built_at:
        raise PromotedSessionMarketDataError(_ERR_FUTURE)
    if any(value.observed_at > corpus_index.built_at for value in partition.bars):
        raise PromotedSessionMarketDataError(_ERR_FUTURE)

    universe_lanes: dict[tuple[str, str], PromotedIdentitySessionEntry] = {}
    for value in universe.entries:
        lane = (f"NSE:{value.symbol}", value.series)
        if lane in universe_lanes:
            raise PromotedSessionMarketDataError(_ERR_GRAPH)
        universe_lanes[lane] = value
    bars_by_lane: dict[tuple[str, str], HistoricalEvaluationCorpusBar] = {}
    for value in partition.bars:
        if value.listing_lane in bars_by_lane:
            raise PromotedSessionMarketDataError(_ERR_GRAPH)
        bars_by_lane[value.listing_lane] = value

    entries: list[PromotedSessionMarketDataEntry] = []
    consumed_lanes: set[tuple[str, str]] = set()
    for universe_entry in universe.entries:
        lane = (f"NSE:{universe_entry.symbol}", universe_entry.series)
        bar = bars_by_lane.get(lane)
        if bar is not None:
            consumed_lanes.add(lane)
        status, stable_identity_bound = _status_for(universe_entry, bar)
        entries.append(
            PromotedSessionMarketDataEntry(
                universe_entry=universe_entry,
                status=status,
                bar=bar,
                stable_identity_bound=stable_identity_bound,
                reason_codes=_entry_reasons(universe_entry, status),
            )
        )
    entries_tuple = tuple(sorted(entries, key=lambda value: value.source_record_id))
    if len(entries_tuple) != len(universe.entries) or {
        value.source_record_id for value in entries_tuple
    } != {value.source_record_id for value in universe.entries}:
        raise PromotedSessionMarketDataError(_ERR_GRAPH)

    orphan_bars = tuple(
        PromotedSessionMarketDataOrphan(bar=value)
        for value in sorted(
            (
                bar
                for lane, bar in bars_by_lane.items()
                if lane not in consumed_lanes
            ),
            key=lambda value: value.listing_lane,
        )
    )

    status_totals: dict[str, int] = {}
    reason_totals: dict[str, int] = {}
    for entry in entries_tuple:
        status_totals[entry.status.value] = status_totals.get(entry.status.value, 0) + 1
        for reason in entry.reason_codes:
            reason_totals[reason] = reason_totals.get(reason, 0) + 1
    for orphan in orphan_bars:
        status_totals["ORPHAN_BAR_UNMATCHED_UNIVERSE"] = (
            status_totals.get("ORPHAN_BAR_UNMATCHED_UNIVERSE", 0) + 1
        )
        for reason in orphan.reason_codes:
            reason_totals[reason] = reason_totals.get(reason, 0) + 1
    status_counts = tuple(sorted(status_totals.items()))
    reason_counts = tuple(sorted(reason_totals.items()))

    knowledge_time = max(universe.knowledge_time, corpus_index.built_at)
    readiness = ReferenceReadiness.COLLECTION_ONLY
    actionable = False
    training_eligible = False
    alert_eligible = False
    execution_eligible = False
    frame_id = content_id(
        _frame_identity(
            universe_id=universe.universe_id,
            adjudication_id=universe.adjudication.adjudication_id,
            identity_snapshot_id=universe.adjudication.snapshot.snapshot_id,
            calendar_materialization_id=universe.calendar.materialization_id,
            calendar_snapshot_id=universe.calendar.calendar_snapshot.snapshot_id,
            corpus_id=corpus_index.corpus_id,
            admission_report_id=corpus_index.admission_report_id,
            reconciliation_index_id=corpus_index.reconciliation_index_id,
            partition_id=partition.partition_id,
            source_snapshot_ids=partition.source_snapshot_ids,
            source_report_ids=partition.source_report_ids,
            market_session=universe.market_session,
            cutoff=cutoff,
            knowledge_time=knowledge_time,
            entries=entries_tuple,
            orphan_bars=orphan_bars,
            status_counts=status_counts,
            reason_counts=reason_counts,
            readiness=readiness,
            actionable=actionable,
            training_eligible=training_eligible,
            alert_eligible=alert_eligible,
            execution_eligible=execution_eligible,
        ),
        length=64,
    )
    return _SessionFrameFacts(
        cutoff=cutoff,
        knowledge_time=knowledge_time,
        entries=entries_tuple,
        orphan_bars=orphan_bars,
        status_counts=status_counts,
        reason_counts=reason_counts,
        readiness=readiness,
        actionable=actionable,
        training_eligible=training_eligible,
        alert_eligible=alert_eligible,
        execution_eligible=execution_eligible,
        frame_id=frame_id,
    )


@dataclass(frozen=True, slots=True)
class VerifiedPromotedSessionMarketDataFrame:
    schema_version: str
    policy_version: str
    universe: VerifiedPromotedIdentitySessionUniverse
    corpus_index: HistoricalEvaluationCorpusIndex
    partition: HistoricalEvaluationCorpusSessionPartition
    market_session: date
    cutoff: datetime
    knowledge_time: datetime
    entries: tuple[PromotedSessionMarketDataEntry, ...]
    orphan_bars: tuple[PromotedSessionMarketDataOrphan, ...]
    status_counts: tuple[tuple[str, int], ...]
    reason_counts: tuple[tuple[str, int], ...]
    readiness: ReferenceReadiness
    actionable: bool
    training_eligible: bool
    alert_eligible: bool
    execution_eligible: bool
    frame_id: str

    def __post_init__(self) -> None:
        self.verify_content_identity()

    def verify_content_identity(self) -> None:
        if type(self) is not VerifiedPromotedSessionMarketDataFrame:
            raise PromotedSessionMarketDataError(_ERR_TYPE)
        if (
            self.schema_version != PROMOTED_SESSION_MARKET_DATA_SCHEMA_VERSION
            or self.policy_version != PROMOTED_SESSION_MARKET_DATA_POLICY_VERSION
        ):
            raise PromotedSessionMarketDataError(_ERR_DERIVED)
        if type(self.universe) is not VerifiedPromotedIdentitySessionUniverse:
            raise PromotedSessionMarketDataError(_ERR_UNIVERSE)
        if type(self.corpus_index) is not HistoricalEvaluationCorpusIndex:
            raise PromotedSessionMarketDataError(_ERR_CORPUS)
        if type(self.partition) is not HistoricalEvaluationCorpusSessionPartition:
            raise PromotedSessionMarketDataError(_ERR_PARTITION)
        if type(self.market_session) is not date:
            raise PromotedSessionMarketDataError(_ERR_DERIVED)
        if type(self.entries) is not tuple or any(
            type(value) is not PromotedSessionMarketDataEntry for value in self.entries
        ):
            raise PromotedSessionMarketDataError(_ERR_DERIVED)
        if type(self.orphan_bars) is not tuple or any(
            type(value) is not PromotedSessionMarketDataOrphan
            for value in self.orphan_bars
        ):
            raise PromotedSessionMarketDataError(_ERR_DERIVED)
        for values in (self.status_counts, self.reason_counts):
            if type(values) is not tuple or any(
                type(value) is not tuple or len(value) != 2 for value in values
            ):
                raise PromotedSessionMarketDataError(_ERR_DERIVED)
        if (
            self.readiness is not ReferenceReadiness.COLLECTION_ONLY
            or self.actionable is not False
            or self.training_eligible is not False
            or self.alert_eligible is not False
            or self.execution_eligible is not False
        ):
            raise PromotedSessionMarketDataError(_ERR_DERIVED)
        if type(self.frame_id) is not str or _SHA256.fullmatch(self.frame_id) is None:
            raise PromotedSessionMarketDataError(_ERR_ID)

        try:
            facts = _build_session_frame_facts(
                self.universe,
                self.corpus_index,
                self.partition,
                self.cutoff,
            )
        except PromotedSessionMarketDataError:
            raise
        except Exception:
            raise PromotedSessionMarketDataError(_ERR_DERIVED) from None
        try:
            if self.market_session != self.universe.market_session:
                raise PromotedSessionMarketDataError(_ERR_DERIVED)
            if self.cutoff != facts.cutoff:
                raise PromotedSessionMarketDataError(_ERR_DERIVED)
            if self.knowledge_time != facts.knowledge_time:
                raise PromotedSessionMarketDataError(_ERR_DERIVED)
            if self.entries != facts.entries:
                raise PromotedSessionMarketDataError(_ERR_DERIVED)
            if self.orphan_bars != facts.orphan_bars:
                raise PromotedSessionMarketDataError(_ERR_DERIVED)
            if self.status_counts != facts.status_counts:
                raise PromotedSessionMarketDataError(_ERR_DERIVED)
            if self.reason_counts != facts.reason_counts:
                raise PromotedSessionMarketDataError(_ERR_DERIVED)
            if self.frame_id != facts.frame_id:
                raise PromotedSessionMarketDataError(_ERR_ID)
        except PromotedSessionMarketDataError:
            raise
        except Exception:
            raise PromotedSessionMarketDataError(_ERR_COMPARISON) from None


class PromotedSessionMarketDataFrameService:
    """Build one exact collection-only session frame; never select latest data."""

    def materialize(
        self,
        *,
        universe: VerifiedPromotedIdentitySessionUniverse,
        corpus_index: HistoricalEvaluationCorpusIndex,
        partition: HistoricalEvaluationCorpusSessionPartition,
        cutoff: datetime,
    ) -> VerifiedPromotedSessionMarketDataFrame:
        facts = _build_session_frame_facts(
            universe,
            corpus_index,
            partition,
            cutoff,
        )
        return VerifiedPromotedSessionMarketDataFrame(
            schema_version=PROMOTED_SESSION_MARKET_DATA_SCHEMA_VERSION,
            policy_version=PROMOTED_SESSION_MARKET_DATA_POLICY_VERSION,
            universe=universe,
            corpus_index=corpus_index,
            partition=partition,
            market_session=universe.market_session,
            cutoff=facts.cutoff,
            knowledge_time=facts.knowledge_time,
            entries=facts.entries,
            orphan_bars=facts.orphan_bars,
            status_counts=facts.status_counts,
            reason_counts=facts.reason_counts,
            readiness=facts.readiness,
            actionable=facts.actionable,
            training_eligible=facts.training_eligible,
            alert_eligible=facts.alert_eligible,
            execution_eligible=facts.execution_eligible,
            frame_id=facts.frame_id,
        )
