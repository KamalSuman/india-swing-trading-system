from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

from india_swing.historical_prices.artifact_store import (
    LocalHistoricalPriceArtifactStore,
)
from india_swing.identity import content_id
from india_swing.liquidity import (
    LocalLiquiditySnapshotStore,
    materialize_collection_liquidity,
)
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference_data.artifact_store import LocalReferenceArtifactStore
from india_swing.tick_sizes import (
    LocalTickSizeSnapshotStore,
    materialize_collection_tick_sizes,
)
from india_swing.universe import (
    LocalCollectionUniverseSnapshotStore,
    materialize_collection_universe,
)

from .models import DailyPipelineRun


DAILY_DERIVED_EVIDENCE_SCHEMA_VERSION = "nse-cm-daily-derived-evidence/v1"
DAILY_DERIVED_EVIDENCE_POLICY_VERSION = "nse-cm-derived-evidence/v1"
DAILY_DERIVED_EVIDENCE_CODEC_VERSION = "nse-cm-daily-derived-evidence-json/v1"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_REASON = re.compile(r"[A-Z][A-Z0-9_]*\Z")


class DailyDerivedEvidenceError(RuntimeError):
    pass


class DailyDerivedEvidenceIntegrityError(DailyDerivedEvidenceError):
    pass


class DailyDerivedEvidenceConflict(DailyDerivedEvidenceError):
    pass


class DailyDerivedEvidenceNotFound(DailyDerivedEvidenceError):
    pass


def _sha(value: str, name: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise DailyDerivedEvidenceIntegrityError(
            f"{name} must be a full lowercase SHA-256"
        )


def _utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise DailyDerivedEvidenceIntegrityError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class DailyDerivedEvidence:
    run_id: str
    market_session: date
    cutoff: datetime
    calendar_snapshot_id: str
    current_security_master_artifact_id: str
    historical_price_artifact_ids: tuple[str, ...]
    tick_size_snapshot_id: str
    liquidity_snapshot_id: str
    universe_snapshot_id: str
    minimum_history_sessions: int
    reason_codes: tuple[str, ...]
    readiness: ReferenceReadiness = ReferenceReadiness.COLLECTION_ONLY
    actionable: bool = False
    policy_version: str = DAILY_DERIVED_EVIDENCE_POLICY_VERSION
    schema_version: str = DAILY_DERIVED_EVIDENCE_SCHEMA_VERSION
    evidence_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha(self.run_id, "derived evidence run_id")
        if type(self.market_session) is not date:
            raise TypeError("derived evidence market_session must be a date")
        object.__setattr__(self, "cutoff", _utc(self.cutoff, "derived evidence cutoff"))
        for value, name in (
            (self.calendar_snapshot_id, "calendar_snapshot_id"),
            (self.current_security_master_artifact_id, "security_master_artifact_id"),
            (self.tick_size_snapshot_id, "tick_size_snapshot_id"),
            (self.liquidity_snapshot_id, "liquidity_snapshot_id"),
            (self.universe_snapshot_id, "universe_snapshot_id"),
        ):
            _sha(value, name)
        if (
            type(self.historical_price_artifact_ids) is not tuple
            or not self.historical_price_artifact_ids
            or len(set(self.historical_price_artifact_ids))
            != len(self.historical_price_artifact_ids)
        ):
            raise DailyDerivedEvidenceIntegrityError(
                "historical price artifact IDs must be a non-empty unique tuple"
            )
        for value in self.historical_price_artifact_ids:
            _sha(value, "historical_price_artifact_ids")
        if (
            type(self.minimum_history_sessions) is not int
            or self.minimum_history_sessions <= 0
        ):
            raise DailyDerivedEvidenceIntegrityError(
                "minimum history sessions must be positive"
            )
        if (
            type(self.reason_codes) is not tuple
            or not self.reason_codes
            or self.reason_codes != tuple(sorted(set(self.reason_codes)))
            or any(_REASON.fullmatch(value) is None for value in self.reason_codes)
        ):
            raise DailyDerivedEvidenceIntegrityError(
                "derived evidence requires sorted reason codes"
            )
        if self.readiness is not ReferenceReadiness.COLLECTION_ONLY or self.actionable:
            raise DailyDerivedEvidenceIntegrityError(
                "daily derived evidence must remain collection-only"
            )
        if (
            self.policy_version != DAILY_DERIVED_EVIDENCE_POLICY_VERSION
            or self.schema_version != DAILY_DERIVED_EVIDENCE_SCHEMA_VERSION
        ):
            raise DailyDerivedEvidenceIntegrityError(
                "unsupported daily derived evidence contract"
            )
        object.__setattr__(self, "evidence_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema_version": self.schema_version,
                "policy_version": self.policy_version,
                "run_id": self.run_id,
                "market_session": self.market_session,
                "cutoff": self.cutoff,
                "calendar_snapshot_id": self.calendar_snapshot_id,
                "current_security_master_artifact_id": (
                    self.current_security_master_artifact_id
                ),
                "historical_price_artifact_ids": self.historical_price_artifact_ids,
                "tick_size_snapshot_id": self.tick_size_snapshot_id,
                "liquidity_snapshot_id": self.liquidity_snapshot_id,
                "universe_snapshot_id": self.universe_snapshot_id,
                "minimum_history_sessions": self.minimum_history_sessions,
                "reason_codes": self.reason_codes,
                "readiness": self.readiness,
                "actionable": self.actionable,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.evidence_id != self._calculated_id():
            raise DailyDerivedEvidenceIntegrityError(
                "daily derived evidence identity failed"
            )


def _verify_run_chain(
    runs: tuple[DailyPipelineRun, ...],
) -> tuple[DailyPipelineRun, ...]:
    if (
        type(runs) is not tuple
        or not runs
        or any(type(value) is not DailyPipelineRun for value in runs)
    ):
        raise TypeError("daily derived evidence requires exact daily runs")
    for value in runs:
        value.verify_content_identity()
    for previous, current in zip(runs, runs[1:]):
        if current.previous_run_id != previous.run_id:
            raise DailyDerivedEvidenceIntegrityError(
                "daily evidence run chain has a missing predecessor link"
            )
        if previous.market_session >= current.market_session:
            raise DailyDerivedEvidenceIntegrityError(
                "daily evidence run chain is not session ordered"
            )
        if previous.cutoff >= current.cutoff:
            raise DailyDerivedEvidenceIntegrityError(
                "daily evidence run chain is not cutoff ordered"
            )
    return runs


def materialize_daily_derived_evidence(
    *,
    runs: tuple[DailyPipelineRun, ...],
    reference_store: LocalReferenceArtifactStore,
    historical_store: LocalHistoricalPriceArtifactStore,
    tick_store: LocalTickSizeSnapshotStore,
    liquidity_store: LocalLiquiditySnapshotStore,
    universe_store: LocalCollectionUniverseSnapshotStore,
    minimum_history_sessions: int = 120,
) -> DailyDerivedEvidence:
    runs = _verify_run_chain(runs)
    if type(reference_store) is not LocalReferenceArtifactStore:
        raise TypeError("derived evidence reference store must be exact")
    if type(historical_store) is not LocalHistoricalPriceArtifactStore:
        raise TypeError("derived evidence historical store must be exact")
    if type(tick_store) is not LocalTickSizeSnapshotStore:
        raise TypeError("derived evidence tick store must be exact")
    if type(liquidity_store) is not LocalLiquiditySnapshotStore:
        raise TypeError("derived evidence liquidity store must be exact")
    if type(universe_store) is not LocalCollectionUniverseSnapshotStore:
        raise TypeError("derived evidence universe store must be exact")
    if type(minimum_history_sessions) is not int or minimum_history_sessions <= 0:
        raise DailyDerivedEvidenceIntegrityError(
            "minimum history sessions must be positive"
        )
    current = runs[-1]
    source = reference_store.get(current.current_security_master_artifact_id)
    history = tuple(
        historical_store.get(value.historical_price_artifact_id).artifact
        for value in runs
    )
    tick = tick_store.put(materialize_collection_tick_sizes(source, cutoff=current.cutoff))
    liquidity = liquidity_store.put(
        materialize_collection_liquidity(
            history,
            decision_cutoff=current.cutoff,
            minimum_history_sessions=minimum_history_sessions,
        )
    )
    universe = universe_store.put(
        materialize_collection_universe(
            source,
            cutoff=current.cutoff,
            calendar_snapshot_id=current.calendar_snapshot_id,
        )
    )
    return DailyDerivedEvidence(
        run_id=current.run_id,
        market_session=current.market_session,
        cutoff=current.cutoff,
        calendar_snapshot_id=current.calendar_snapshot_id,
        current_security_master_artifact_id=current.current_security_master_artifact_id,
        historical_price_artifact_ids=tuple(
            value.historical_price_artifact_id for value in runs
        ),
        tick_size_snapshot_id=tick.snapshot_id,
        liquidity_snapshot_id=liquidity.snapshot_id,
        universe_snapshot_id=universe.snapshot_id,
        minimum_history_sessions=minimum_history_sessions,
        reason_codes=("DERIVED_FROM_COLLECTION_ONLY_RUN",),
    )


def daily_run_chain(
    run: DailyPipelineRun,
    *,
    run_store: object,
) -> tuple[DailyPipelineRun, ...]:
    """Resolve the complete explicit predecessor chain from oldest to ``run``."""

    if type(run) is not DailyPipelineRun:
        raise TypeError("daily evidence run must be exact")
    run.verify_content_identity()
    get = getattr(run_store, "get", None)
    if not callable(get):
        raise TypeError("daily evidence run store must provide get")
    reversed_runs = [run]
    seen = {run.run_id}
    current = run
    while current.previous_run_id is not None:
        previous = get(current.previous_run_id)
        if type(previous) is not DailyPipelineRun:
            raise DailyDerivedEvidenceIntegrityError(
                "daily evidence predecessor has an invalid type"
            )
        if previous.run_id in seen:
            raise DailyDerivedEvidenceIntegrityError(
                "daily evidence predecessor chain is cyclic"
            )
        seen.add(previous.run_id)
        reversed_runs.append(previous)
        current = previous
    return _verify_run_chain(tuple(reversed(reversed_runs)))


def validate_daily_derived_evidence(
    value: DailyDerivedEvidence,
    *,
    run: DailyPipelineRun,
    run_store: object,
) -> tuple[DailyPipelineRun, ...]:
    """Bind derived snapshot IDs to one exact, complete daily-run chain."""

    if type(value) is not DailyDerivedEvidence:
        raise TypeError("daily derived evidence must be exact")
    if type(run) is not DailyPipelineRun:
        raise TypeError("daily evidence run must be exact")
    value.verify_content_identity()
    run.verify_content_identity()
    runs = daily_run_chain(run, run_store=run_store)
    if (
        value.run_id != run.run_id
        or value.market_session != run.market_session
        or value.cutoff != run.cutoff
        or value.calendar_snapshot_id != run.calendar_snapshot_id
        or value.current_security_master_artifact_id
        != run.current_security_master_artifact_id
        or value.historical_price_artifact_ids
        != tuple(item.historical_price_artifact_id for item in runs)
    ):
        raise DailyDerivedEvidenceIntegrityError(
            "derived evidence differs from its sealed daily-run chain"
        )
    return runs
