"""Cutoff-safe corporate-action bridge for promoted stable-listing histories.

The bridge reuses the established split/bonus adjustment engine.  It never
fills raw-history gaps, never turns collected tick observations into effective
tick intervals, and never grants feature, signal, alert, or execution authority.
Histories that cannot be adjusted safely remain explicit blocked results.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum

from india_swing.corporate_actions.adjustments import (
    ADJUSTED_PRICE_BASIS,
    ADJUSTMENT_POLICY_VERSION,
    PriceAdjustmentError,
    StableRawBarBinding,
    corporate_action_factors_for_session,
    select_automatic_adjustment_events,
)
from india_swing.corporate_actions.models import (
    CorporateActionSnapshot,
    CorporateActionType,
)
from india_swing.historical_prices.promoted_history import (
    PromotedStableListingHistory,
    PromotedStableListingObservationStatus,
    VerifiedPromotedStableListingHistoryPanel,
)
from india_swing.identity import content_id
from india_swing.market_data.historical_corpus import HistoricalEvaluationCorpusBar
from india_swing.reference.models import ReferenceReadiness


class PromotedCorporateActionBridgeError(ValueError):
    pass


PROMOTED_CORPORATE_ACTION_BRIDGE_SCHEMA_VERSION = (
    "promoted-corporate-action-adjustment-panel/v1"
)
PROMOTED_CORPORATE_ACTION_BRIDGE_POLICY_VERSION = (
    "promoted-corporate-action-adjustment/fail-closed-per-listing-v1"
)

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_REASON = re.compile(r"[A-Z][A-Z0-9_]{2,127}\Z")

_ERR_TYPE = "promoted corporate-action bridge type is invalid"
_ERR_INPUT = "promoted corporate-action bridge input is invalid"
_ERR_VERIFY = "promoted corporate-action bridge input could not be verified"
_ERR_CUTOFF = "promoted corporate-action bridge cutoff is invalid"
_ERR_FUTURE = "promoted corporate-action bridge contains future-known evidence"
_ERR_GRAPH = "promoted corporate-action bridge graph is invalid"
_ERR_DERIVED = "promoted corporate-action bridge derived content is invalid"
_ERR_ID = "promoted corporate-action bridge identifier is invalid"
_ERR_ENGINE = "promoted corporate-action adjustment engine failed closed"

_COMMON_REASONS = {
    "EFFECTIVE_TICK_INTERVAL_REQUIRED",
    "FEATURE_CALCULATION_NOT_AUTHORIZED",
    "PROMOTED_CORPORATE_ACTION_ADJUSTMENT_COLLECTION_ONLY",
}


def _utc(value: datetime) -> datetime:
    if type(value) is not datetime:
        raise PromotedCorporateActionBridgeError(_ERR_CUTOFF)
    try:
        offset = value.utcoffset()
    except Exception:
        raise PromotedCorporateActionBridgeError(_ERR_CUTOFF) from None
    if value.tzinfo is None or offset is None:
        raise PromotedCorporateActionBridgeError(_ERR_CUTOFF)
    return value.astimezone(timezone.utc)


def _reasons(*values: str) -> tuple[str, ...]:
    result = tuple(sorted({*_COMMON_REASONS, *values}))
    if any(type(value) is not str or _REASON.fullmatch(value) is None for value in result):
        raise PromotedCorporateActionBridgeError(_ERR_GRAPH)
    return result


def _counts(values: list[str]) -> tuple[tuple[str, int], ...]:
    result: dict[str, int] = {}
    for value in values:
        result[value] = result.get(value, 0) + 1
    return tuple(sorted(result.items()))


class PromotedCorporateActionAdjustmentStatus(str, Enum):
    ADJUSTED_HISTORY_BUILT_COLLECTION_ONLY = (
        "ADJUSTED_HISTORY_BUILT_COLLECTION_ONLY"
    )
    RAW_HISTORY_GAP_BLOCKED = "RAW_HISTORY_GAP_BLOCKED"
    RAW_HISTORY_IDENTITY_CONFLICT_BLOCKED = (
        "RAW_HISTORY_IDENTITY_CONFLICT_BLOCKED"
    )
    CORPORATE_ACTION_EVIDENCE_NOT_ACTIONABLE = (
        "CORPORATE_ACTION_EVIDENCE_NOT_ACTIONABLE"
    )
    CORPORATE_ACTION_COVERAGE_INCOMPLETE = (
        "CORPORATE_ACTION_COVERAGE_INCOMPLETE"
    )
    CORPORATE_ACTION_MANUAL_REVIEW_REQUIRED = (
        "CORPORATE_ACTION_MANUAL_REVIEW_REQUIRED"
    )


_STATUS_REASON = {
    PromotedCorporateActionAdjustmentStatus.ADJUSTED_HISTORY_BUILT_COLLECTION_ONLY: (
        "CORPORATE_ACTION_ADJUSTMENT_APPLIED_AS_OF_CUTOFF"
    ),
    PromotedCorporateActionAdjustmentStatus.RAW_HISTORY_GAP_BLOCKED: (
        "RAW_HISTORY_GAP_BLOCKS_ADJUSTMENT"
    ),
    PromotedCorporateActionAdjustmentStatus.RAW_HISTORY_IDENTITY_CONFLICT_BLOCKED: (
        "RAW_HISTORY_IDENTITY_CONFLICT_BLOCKS_ADJUSTMENT"
    ),
    PromotedCorporateActionAdjustmentStatus.CORPORATE_ACTION_EVIDENCE_NOT_ACTIONABLE: (
        "CORPORATE_ACTION_EVIDENCE_NOT_ACTIONABLE"
    ),
    PromotedCorporateActionAdjustmentStatus.CORPORATE_ACTION_COVERAGE_INCOMPLETE: (
        "CORPORATE_ACTION_COVERAGE_INCOMPLETE"
    ),
    PromotedCorporateActionAdjustmentStatus.CORPORATE_ACTION_MANUAL_REVIEW_REQUIRED: (
        "CORPORATE_ACTION_MANUAL_REVIEW_REQUIRED"
    ),
}


@dataclass(frozen=True, slots=True)
class PromotedCorporateActionAdjustedCorpusBar:
    source_bar: HistoricalEvaluationCorpusBar
    stable_instrument_id: str
    stable_listing_id: str
    identity_binding_id: str
    corporate_action_snapshot_id: str
    price_factor: Decimal
    volume_factor: Decimal
    adjusted_open: Decimal
    adjusted_high: Decimal
    adjusted_low: Decimal
    adjusted_close: Decimal
    adjusted_volume: Decimal
    applied_event_ids: tuple[str, ...]
    knowledge_time: datetime
    adjustment_policy_version: str
    adjusted_bar_id: str

    def __post_init__(self) -> None:
        if type(self.source_bar) is not HistoricalEvaluationCorpusBar:
            raise PromotedCorporateActionBridgeError(_ERR_GRAPH)
        try:
            self.source_bar.verify_content_identity()
        except Exception:
            raise PromotedCorporateActionBridgeError(_ERR_GRAPH) from None
        for value in (
            self.stable_instrument_id,
            self.stable_listing_id,
            self.identity_binding_id,
            self.corporate_action_snapshot_id,
            self.adjusted_bar_id,
        ):
            if type(value) is not str or _SHA256.fullmatch(value) is None:
                raise PromotedCorporateActionBridgeError(_ERR_GRAPH)
        for value in (
            self.price_factor,
            self.volume_factor,
            self.adjusted_open,
            self.adjusted_high,
            self.adjusted_low,
            self.adjusted_close,
            self.adjusted_volume,
        ):
            if type(value) is not Decimal or not value.is_finite() or value < 0:
                raise PromotedCorporateActionBridgeError(_ERR_GRAPH)
        if (
            self.price_factor <= 0
            or self.volume_factor <= 0
            or self.adjusted_open <= 0
            or self.adjusted_high <= 0
            or self.adjusted_low <= 0
            or self.adjusted_close <= 0
            or self.adjusted_high
            < max(self.adjusted_open, self.adjusted_low, self.adjusted_close)
            or self.adjusted_low
            > min(self.adjusted_open, self.adjusted_high, self.adjusted_close)
            or self.volume_factor != Decimal("1") / self.price_factor
            or self.adjusted_open != self.source_bar.open * self.price_factor
            or self.adjusted_high != self.source_bar.high * self.price_factor
            or self.adjusted_low != self.source_bar.low * self.price_factor
            or self.adjusted_close != self.source_bar.close * self.price_factor
            or self.adjusted_volume
            != Decimal(self.source_bar.volume) * self.volume_factor
        ):
            raise PromotedCorporateActionBridgeError(_ERR_GRAPH)
        if (
            type(self.applied_event_ids) is not tuple
            or self.applied_event_ids != tuple(sorted(set(self.applied_event_ids)))
            or any(
                type(value) is not str or _SHA256.fullmatch(value) is None
                for value in self.applied_event_ids
            )
            or self.adjustment_policy_version != ADJUSTMENT_POLICY_VERSION
        ):
            raise PromotedCorporateActionBridgeError(_ERR_GRAPH)
        knowledge_time = _utc(self.knowledge_time)
        if knowledge_time != self.knowledge_time:
            object.__setattr__(self, "knowledge_time", knowledge_time)
        if self.knowledge_time < self.source_bar.observed_at:
            raise PromotedCorporateActionBridgeError(_ERR_GRAPH)
        if self.adjusted_bar_id != self._calculated_id():
            raise PromotedCorporateActionBridgeError(_ERR_ID)

    def _identity(self) -> dict[str, object]:
        return {
            "source_bar_id": self.source_bar.bar_id,
            "stable_instrument_id": self.stable_instrument_id,
            "stable_listing_id": self.stable_listing_id,
            "identity_binding_id": self.identity_binding_id,
            "corporate_action_snapshot_id": self.corporate_action_snapshot_id,
            "price_factor": self.price_factor,
            "volume_factor": self.volume_factor,
            "adjusted_open": self.adjusted_open,
            "adjusted_high": self.adjusted_high,
            "adjusted_low": self.adjusted_low,
            "adjusted_close": self.adjusted_close,
            "adjusted_volume": self.adjusted_volume,
            "applied_event_ids": self.applied_event_ids,
            "knowledge_time": self.knowledge_time,
            "adjustment_policy_version": self.adjustment_policy_version,
        }

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "promoted-corporate-action-adjusted-corpus-bar/v1",
                **self._identity(),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self.source_bar.verify_content_identity()
        if self.adjusted_bar_id != self._calculated_id():
            raise PromotedCorporateActionBridgeError(_ERR_ID)


def _adjusted_corpus_bar(
    *,
    source_bar: HistoricalEvaluationCorpusBar,
    stable_instrument_id: str,
    stable_listing_id: str,
    identity_binding: StableRawBarBinding,
    corporate_action_snapshot_id: str,
    price_factor: Decimal,
    volume_factor: Decimal,
    applied_event_ids: tuple[str, ...],
    knowledge_time: datetime,
) -> PromotedCorporateActionAdjustedCorpusBar:
    values = {
        "source_bar_id": source_bar.bar_id,
        "stable_instrument_id": stable_instrument_id,
        "stable_listing_id": stable_listing_id,
        "identity_binding_id": identity_binding.binding_id,
        "corporate_action_snapshot_id": corporate_action_snapshot_id,
        "price_factor": price_factor,
        "volume_factor": volume_factor,
        "adjusted_open": source_bar.open * price_factor,
        "adjusted_high": source_bar.high * price_factor,
        "adjusted_low": source_bar.low * price_factor,
        "adjusted_close": source_bar.close * price_factor,
        "adjusted_volume": Decimal(source_bar.volume) * volume_factor,
        "applied_event_ids": applied_event_ids,
        "knowledge_time": knowledge_time,
        "adjustment_policy_version": ADJUSTMENT_POLICY_VERSION,
    }
    return PromotedCorporateActionAdjustedCorpusBar(
        source_bar=source_bar,
        stable_instrument_id=stable_instrument_id,
        stable_listing_id=stable_listing_id,
        identity_binding_id=identity_binding.binding_id,
        corporate_action_snapshot_id=corporate_action_snapshot_id,
        price_factor=price_factor,
        volume_factor=volume_factor,
        adjusted_open=values["adjusted_open"],
        adjusted_high=values["adjusted_high"],
        adjusted_low=values["adjusted_low"],
        adjusted_close=values["adjusted_close"],
        adjusted_volume=values["adjusted_volume"],
        applied_event_ids=applied_event_ids,
        knowledge_time=knowledge_time,
        adjustment_policy_version=ADJUSTMENT_POLICY_VERSION,
        adjusted_bar_id=content_id(
            {
                "schema": "promoted-corporate-action-adjusted-corpus-bar/v1",
                **values,
            },
            length=64,
        ),
    )


@dataclass(frozen=True, slots=True)
class PromotedCorporateActionAdjustedHistory:
    source_history_id: str
    stable_instrument_id: str
    stable_listing_id: str
    signal_session: date
    cutoff: datetime
    corporate_action_snapshot_id: str
    bars: tuple[PromotedCorporateActionAdjustedCorpusBar, ...]
    price_basis: str
    adjustment_policy_version: str
    history_id: str

    def __post_init__(self) -> None:
        for value in (
            self.source_history_id,
            self.stable_instrument_id,
            self.stable_listing_id,
            self.corporate_action_snapshot_id,
            self.history_id,
        ):
            if type(value) is not str or _SHA256.fullmatch(value) is None:
                raise PromotedCorporateActionBridgeError(_ERR_GRAPH)
        cutoff = _utc(self.cutoff)
        if cutoff != self.cutoff:
            object.__setattr__(self, "cutoff", cutoff)
        if (
            type(self.signal_session) is not date
            or type(self.bars) is not tuple
            or not self.bars
            or any(
                type(value) is not PromotedCorporateActionAdjustedCorpusBar
                for value in self.bars
            )
            or tuple(value.source_bar.session for value in self.bars)
            != tuple(sorted({value.source_bar.session for value in self.bars}))
            or self.bars[-1].source_bar.session != self.signal_session
            or self.price_basis != ADJUSTED_PRICE_BASIS
            or self.adjustment_policy_version != ADJUSTMENT_POLICY_VERSION
        ):
            raise PromotedCorporateActionBridgeError(_ERR_GRAPH)
        for value in self.bars:
            try:
                value.verify_content_identity()
            except Exception:
                raise PromotedCorporateActionBridgeError(_ERR_GRAPH) from None
            if (
                value.stable_instrument_id != self.stable_instrument_id
                or value.stable_listing_id != self.stable_listing_id
                or value.corporate_action_snapshot_id
                != self.corporate_action_snapshot_id
                or value.knowledge_time > self.cutoff
            ):
                raise PromotedCorporateActionBridgeError(_ERR_GRAPH)
        if self.history_id != self._calculated_id():
            raise PromotedCorporateActionBridgeError(_ERR_ID)

    def _identity(self) -> dict[str, object]:
        return {
            "source_history_id": self.source_history_id,
            "stable_instrument_id": self.stable_instrument_id,
            "stable_listing_id": self.stable_listing_id,
            "signal_session": self.signal_session,
            "cutoff": self.cutoff,
            "corporate_action_snapshot_id": self.corporate_action_snapshot_id,
            "adjusted_bar_ids": tuple(value.adjusted_bar_id for value in self.bars),
            "price_basis": self.price_basis,
            "adjustment_policy_version": self.adjustment_policy_version,
        }

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "promoted-corporate-action-adjusted-history/v1",
                **self._identity(),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        for value in self.bars:
            value.verify_content_identity()
        if self.history_id != self._calculated_id():
            raise PromotedCorporateActionBridgeError(_ERR_ID)


def _adjusted_history(
    *,
    source_history: PromotedStableListingHistory,
    signal_session: date,
    cutoff: datetime,
    corporate_action_snapshot_id: str,
    bars: tuple[PromotedCorporateActionAdjustedCorpusBar, ...],
) -> PromotedCorporateActionAdjustedHistory:
    values = {
        "source_history_id": source_history.history_id,
        "stable_instrument_id": source_history.stable_instrument_id,
        "stable_listing_id": source_history.stable_listing_id,
        "signal_session": signal_session,
        "cutoff": cutoff,
        "corporate_action_snapshot_id": corporate_action_snapshot_id,
        "adjusted_bar_ids": tuple(value.adjusted_bar_id for value in bars),
        "price_basis": ADJUSTED_PRICE_BASIS,
        "adjustment_policy_version": ADJUSTMENT_POLICY_VERSION,
    }
    return PromotedCorporateActionAdjustedHistory(
        source_history_id=source_history.history_id,
        stable_instrument_id=source_history.stable_instrument_id,
        stable_listing_id=source_history.stable_listing_id,
        signal_session=signal_session,
        cutoff=cutoff,
        corporate_action_snapshot_id=corporate_action_snapshot_id,
        bars=bars,
        price_basis=ADJUSTED_PRICE_BASIS,
        adjustment_policy_version=ADJUSTMENT_POLICY_VERSION,
        history_id=content_id(
            {
                "schema": "promoted-corporate-action-adjusted-history/v1",
                **values,
            },
            length=64,
        ),
    )


@dataclass(frozen=True, slots=True)
class PromotedCorporateActionAdjustmentResult:
    source_history: PromotedStableListingHistory
    status: PromotedCorporateActionAdjustmentStatus
    identity_bindings: tuple[StableRawBarBinding, ...]
    adjusted_history: PromotedCorporateActionAdjustedHistory | None
    reason_codes: tuple[str, ...]
    result_id: str

    def __post_init__(self) -> None:
        if type(self.source_history) is not PromotedStableListingHistory:
            raise PromotedCorporateActionBridgeError(_ERR_GRAPH)
        try:
            self.source_history.verify_content_identity()
        except Exception:
            raise PromotedCorporateActionBridgeError(_ERR_GRAPH) from None
        if type(self.status) is not PromotedCorporateActionAdjustmentStatus:
            raise PromotedCorporateActionBridgeError(_ERR_GRAPH)
        if (
            type(self.identity_bindings) is not tuple
            or any(type(value) is not StableRawBarBinding for value in self.identity_bindings)
        ):
            raise PromotedCorporateActionBridgeError(_ERR_GRAPH)
        if (
            self.adjusted_history is not None
            and type(self.adjusted_history) is not PromotedCorporateActionAdjustedHistory
        ):
            raise PromotedCorporateActionBridgeError(_ERR_GRAPH)
        try:
            for value in self.identity_bindings:
                value.verify_content_identity()
            if self.adjusted_history is not None:
                self.adjusted_history.verify_content_identity()
        except Exception:
            raise PromotedCorporateActionBridgeError(_ERR_GRAPH) from None
        expected_reasons = _reasons(_STATUS_REASON[self.status])
        if self.reason_codes != expected_reasons:
            raise PromotedCorporateActionBridgeError(_ERR_GRAPH)
        success = (
            self.status
            is PromotedCorporateActionAdjustmentStatus.ADJUSTED_HISTORY_BUILT_COLLECTION_ONLY
        )
        if success:
            if (
                not self.identity_bindings
                or self.adjusted_history is None
                or self.adjusted_history.stable_instrument_id
                != self.source_history.stable_instrument_id
                or self.adjusted_history.stable_listing_id
                != self.source_history.stable_listing_id
                or len(self.identity_bindings) != self.source_history.raw_bar_count
            ):
                raise PromotedCorporateActionBridgeError(_ERR_GRAPH)
            assert self.adjusted_history is not None
            source_bars = tuple(
                value.tick_entry.frame_entry.bar
                for value in self.source_history.observations
                if value.tick_entry is not None
                and value.tick_entry.frame_entry.bar is not None
            )
            if (
                len(source_bars) != len(self.identity_bindings)
                or len(source_bars) != len(self.adjusted_history.bars)
                or any(
                    binding.raw_bar_id != source_bar.bar_id
                    or adjusted.source_bar.bar_id != source_bar.bar_id
                    or adjusted.identity_binding_id != binding.binding_id
                    for source_bar, binding, adjusted in zip(
                        source_bars,
                        self.identity_bindings,
                        self.adjusted_history.bars,
                    )
                )
            ):
                raise PromotedCorporateActionBridgeError(_ERR_GRAPH)
        elif self.identity_bindings or self.adjusted_history is not None:
            raise PromotedCorporateActionBridgeError(_ERR_GRAPH)
        if type(self.result_id) is not str or _SHA256.fullmatch(self.result_id) is None:
            raise PromotedCorporateActionBridgeError(_ERR_ID)
        if self.result_id != self._calculated_id():
            raise PromotedCorporateActionBridgeError(_ERR_ID)

    def _identity(self) -> dict[str, object]:
        return {
            "source_history_id": self.source_history.history_id,
            "status": self.status,
            "identity_binding_ids": tuple(
                value.binding_id for value in self.identity_bindings
            ),
            "adjusted_history_id": (
                None
                if self.adjusted_history is None
                else self.adjusted_history.history_id
            ),
            "reason_codes": self.reason_codes,
        }

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "promoted-corporate-action-adjustment-result/v1",
                **self._identity(),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self.source_history.verify_content_identity()
        for value in self.identity_bindings:
            value.verify_content_identity()
        if self.adjusted_history is not None:
            self.adjusted_history.verify_content_identity()
        if self.result_id != self._calculated_id():
            raise PromotedCorporateActionBridgeError(_ERR_ID)


def _result(
    source_history: PromotedStableListingHistory,
    status: PromotedCorporateActionAdjustmentStatus,
    *,
    identity_bindings: tuple[StableRawBarBinding, ...] = (),
    adjusted_history: PromotedCorporateActionAdjustedHistory | None = None,
) -> PromotedCorporateActionAdjustmentResult:
    reason_codes = _reasons(_STATUS_REASON[status])
    identity = {
        "source_history_id": source_history.history_id,
        "status": status,
        "identity_binding_ids": tuple(
            value.binding_id for value in identity_bindings
        ),
        "adjusted_history_id": (
            None if adjusted_history is None else adjusted_history.history_id
        ),
        "reason_codes": reason_codes,
    }
    return PromotedCorporateActionAdjustmentResult(
        source_history=source_history,
        status=status,
        identity_bindings=identity_bindings,
        adjusted_history=adjusted_history,
        reason_codes=reason_codes,
        result_id=content_id(
            {
                "schema": "promoted-corporate-action-adjustment-result/v1",
                **identity,
            },
            length=64,
        ),
    )


@dataclass(frozen=True)
class _BridgeFacts:
    cutoff: datetime
    signal_session: date
    knowledge_time: datetime
    results: tuple[PromotedCorporateActionAdjustmentResult, ...]
    status_counts: tuple[tuple[str, int], ...]
    resolved_histories_adjustment_complete: bool
    readiness: ReferenceReadiness
    actionable: bool
    training_eligible: bool
    feature_eligible: bool
    alert_eligible: bool
    execution_eligible: bool
    bridge_id: str


def _global_blocker(
    source_panel: VerifiedPromotedStableListingHistoryPanel,
    corporate_actions: CorporateActionSnapshot,
) -> PromotedCorporateActionAdjustmentStatus | None:
    if (
        corporate_actions.complete is not True
        or corporate_actions.actionable is not True
        or corporate_actions.readiness is ReferenceReadiness.COLLECTION_ONLY
    ):
        return (
            PromotedCorporateActionAdjustmentStatus.CORPORATE_ACTION_EVIDENCE_NOT_ACTIONABLE
        )
    if (
        corporate_actions.coverage_start > source_panel.sessions[0]
        or corporate_actions.coverage_end < source_panel.sessions[-1]
    ):
        return (
            PromotedCorporateActionAdjustmentStatus.CORPORATE_ACTION_COVERAGE_INCOMPLETE
        )
    return None


def _history_has_manual_action(
    history: PromotedStableListingHistory,
    corporate_actions: CorporateActionSnapshot,
    signal_session: date,
) -> bool:
    first_session = history.observations[0].market_session
    relevant = tuple(
        value
        for value in corporate_actions.active_events
        if value.stable_instrument_id == history.stable_instrument_id
        and first_session < value.effective_session <= signal_session
    )
    return any(
        value.stable_listing_id not in (None, history.stable_listing_id)
        or value.action_type not in {CorporateActionType.SPLIT, CorporateActionType.BONUS}
        or value.automatic_raw_price_factor is None
        for value in relevant
    )


def _successful_result(
    *,
    source_panel: VerifiedPromotedStableListingHistoryPanel,
    history: PromotedStableListingHistory,
    corporate_actions: CorporateActionSnapshot,
    cutoff: datetime,
    signal_session: date,
) -> PromotedCorporateActionAdjustmentResult:
    snapshot_by_session = {
        value.market_session: value for value in source_panel.tick_snapshots
    }
    source_bars = tuple(
        value.tick_entry.frame_entry.bar
        for value in history.observations
        if value.tick_entry is not None and value.tick_entry.frame_entry.bar is not None
    )
    if (
        len(source_bars) != len(history.observations)
        or any(type(value) is not HistoricalEvaluationCorpusBar for value in source_bars)
    ):
        raise PromotedCorporateActionBridgeError(_ERR_GRAPH)
    bindings = tuple(
        StableRawBarBinding(
            market_session=bar.session,
            raw_bar_id=bar.bar_id,
            stable_instrument_id=history.stable_instrument_id,
            stable_listing_id=history.stable_listing_id,
            identity_snapshot_id=(
                snapshot_by_session[bar.session].frame.universe.universe_id
            ),
            knowledge_time=(
                snapshot_by_session[bar.session].frame.universe.knowledge_time
            ),
        )
        for bar in source_bars
    )
    try:
        events = select_automatic_adjustment_events(
            corporate_actions=corporate_actions,
            stable_instrument_id=history.stable_instrument_id,
            stable_listing_id=history.stable_listing_id,
            history_start=source_bars[0].session,
            signal_session=signal_session,
        )
        adjusted_bars = tuple(
            _adjusted_corpus_bar(
                source_bar=bar,
                stable_instrument_id=history.stable_instrument_id,
                stable_listing_id=history.stable_listing_id,
                identity_binding=binding,
                corporate_action_snapshot_id=corporate_actions.snapshot_id,
                price_factor=factors[0],
                volume_factor=factors[1],
                applied_event_ids=factors[2],
                knowledge_time=max(
                    bar.observed_at,
                    binding.knowledge_time,
                    corporate_actions.cutoff,
                ),
            )
            for bar, binding in zip(source_bars, bindings)
            for factors in (
                corporate_action_factors_for_session(
                    events=events,
                    market_session=bar.session,
                ),
            )
        )
        adjusted = _adjusted_history(
            source_history=history,
            signal_session=signal_session,
            cutoff=cutoff,
            corporate_action_snapshot_id=corporate_actions.snapshot_id,
            bars=adjusted_bars,
        )
    except PriceAdjustmentError:
        raise PromotedCorporateActionBridgeError(_ERR_ENGINE) from None
    return _result(
        history,
        PromotedCorporateActionAdjustmentStatus.ADJUSTED_HISTORY_BUILT_COLLECTION_ONLY,
        identity_bindings=bindings,
        adjusted_history=adjusted,
    )


def _bridge_identity(
    *,
    source_panel_id: str,
    corporate_action_snapshot_id: str,
    cutoff: datetime,
    signal_session: date,
    knowledge_time: datetime,
    results: tuple[PromotedCorporateActionAdjustmentResult, ...],
    status_counts: tuple[tuple[str, int], ...],
    resolved_histories_adjustment_complete: bool,
    readiness: ReferenceReadiness,
    actionable: bool,
    training_eligible: bool,
    feature_eligible: bool,
    alert_eligible: bool,
    execution_eligible: bool,
) -> dict[str, object]:
    return {
        "schema_version": PROMOTED_CORPORATE_ACTION_BRIDGE_SCHEMA_VERSION,
        "policy_version": PROMOTED_CORPORATE_ACTION_BRIDGE_POLICY_VERSION,
        "source_panel_id": source_panel_id,
        "corporate_action_snapshot_id": corporate_action_snapshot_id,
        "cutoff": cutoff,
        "signal_session": signal_session,
        "knowledge_time": knowledge_time,
        "result_ids": tuple(value.result_id for value in results),
        "status_counts": status_counts,
        "resolved_histories_adjustment_complete": (
            resolved_histories_adjustment_complete
        ),
        "readiness": readiness,
        "actionable": actionable,
        "training_eligible": training_eligible,
        "feature_eligible": feature_eligible,
        "alert_eligible": alert_eligible,
        "execution_eligible": execution_eligible,
    }


def _build_facts(
    source_panel: VerifiedPromotedStableListingHistoryPanel,
    corporate_actions: CorporateActionSnapshot,
    cutoff: datetime,
) -> _BridgeFacts:
    if type(source_panel) is not VerifiedPromotedStableListingHistoryPanel:
        raise PromotedCorporateActionBridgeError(_ERR_INPUT)
    if type(corporate_actions) is not CorporateActionSnapshot:
        raise PromotedCorporateActionBridgeError(_ERR_INPUT)
    cutoff = _utc(cutoff)
    try:
        source_panel.verify_content_identity()
        corporate_actions.verify_content_identity()
    except Exception:
        raise PromotedCorporateActionBridgeError(_ERR_VERIFY) from None
    if cutoff < source_panel.knowledge_time or corporate_actions.cutoff > cutoff:
        raise PromotedCorporateActionBridgeError(_ERR_FUTURE)
    if not source_panel.sessions:
        raise PromotedCorporateActionBridgeError(_ERR_INPUT)
    signal_session = source_panel.sessions[-1]
    global_blocker = _global_blocker(source_panel, corporate_actions)
    results: list[PromotedCorporateActionAdjustmentResult] = []
    for history in source_panel.histories:
        if history.identity_conflict_count:
            result = _result(
                history,
                PromotedCorporateActionAdjustmentStatus.RAW_HISTORY_IDENTITY_CONFLICT_BLOCKED,
            )
        elif history.gap_count:
            result = _result(
                history,
                PromotedCorporateActionAdjustmentStatus.RAW_HISTORY_GAP_BLOCKED,
            )
        elif global_blocker is not None:
            result = _result(history, global_blocker)
        elif _history_has_manual_action(history, corporate_actions, signal_session):
            result = _result(
                history,
                PromotedCorporateActionAdjustmentStatus.CORPORATE_ACTION_MANUAL_REVIEW_REQUIRED,
            )
        else:
            if any(
                value.status
                is not PromotedStableListingObservationStatus.RAW_BAR_OBSERVED
                for value in history.observations
            ):
                raise PromotedCorporateActionBridgeError(_ERR_GRAPH)
            result = _successful_result(
                source_panel=source_panel,
                history=history,
                corporate_actions=corporate_actions,
                cutoff=cutoff,
                signal_session=signal_session,
            )
        results.append(result)
    results_tuple = tuple(results)
    status_counts = _counts([value.status.value for value in results_tuple])
    resolved_histories_adjustment_complete = bool(results_tuple) and all(
        value.status
        is PromotedCorporateActionAdjustmentStatus.ADJUSTED_HISTORY_BUILT_COLLECTION_ONLY
        for value in results_tuple
    )
    knowledge_time = max(source_panel.knowledge_time, corporate_actions.cutoff)
    readiness = ReferenceReadiness.COLLECTION_ONLY
    actionable = training_eligible = feature_eligible = alert_eligible = execution_eligible = False
    bridge_id = content_id(
        _bridge_identity(
            source_panel_id=source_panel.panel_id,
            corporate_action_snapshot_id=corporate_actions.snapshot_id,
            cutoff=cutoff,
            signal_session=signal_session,
            knowledge_time=knowledge_time,
            results=results_tuple,
            status_counts=status_counts,
            resolved_histories_adjustment_complete=(
                resolved_histories_adjustment_complete
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
    return _BridgeFacts(
        cutoff=cutoff,
        signal_session=signal_session,
        knowledge_time=knowledge_time,
        results=results_tuple,
        status_counts=status_counts,
        resolved_histories_adjustment_complete=(
            resolved_histories_adjustment_complete
        ),
        readiness=readiness,
        actionable=actionable,
        training_eligible=training_eligible,
        feature_eligible=feature_eligible,
        alert_eligible=alert_eligible,
        execution_eligible=execution_eligible,
        bridge_id=bridge_id,
    )


@dataclass(frozen=True, slots=True)
class VerifiedPromotedCorporateActionAdjustmentPanel:
    schema_version: str
    policy_version: str
    source_panel: VerifiedPromotedStableListingHistoryPanel
    corporate_actions: CorporateActionSnapshot
    cutoff: datetime
    signal_session: date
    knowledge_time: datetime
    results: tuple[PromotedCorporateActionAdjustmentResult, ...]
    status_counts: tuple[tuple[str, int], ...]
    resolved_histories_adjustment_complete: bool
    readiness: ReferenceReadiness
    actionable: bool
    training_eligible: bool
    feature_eligible: bool
    alert_eligible: bool
    execution_eligible: bool
    bridge_id: str

    def __post_init__(self) -> None:
        self.verify_content_identity()

    def verify_content_identity(self) -> None:
        if type(self) is not VerifiedPromotedCorporateActionAdjustmentPanel:
            raise PromotedCorporateActionBridgeError(_ERR_TYPE)
        if (
            type(self.schema_version) is not str
            or self.schema_version != PROMOTED_CORPORATE_ACTION_BRIDGE_SCHEMA_VERSION
            or type(self.policy_version) is not str
            or self.policy_version != PROMOTED_CORPORATE_ACTION_BRIDGE_POLICY_VERSION
            or type(self.source_panel) is not VerifiedPromotedStableListingHistoryPanel
            or type(self.corporate_actions) is not CorporateActionSnapshot
            or type(self.cutoff) is not datetime
            or type(self.signal_session) is not date
            or type(self.knowledge_time) is not datetime
            or type(self.results) is not tuple
            or any(
                type(value) is not PromotedCorporateActionAdjustmentResult
                for value in self.results
            )
            or type(self.status_counts) is not tuple
            or any(
                type(value) is not tuple
                or len(value) != 2
                or type(value[0]) is not str
                or type(value[1]) is not int
                for value in self.status_counts
            )
            or type(self.resolved_histories_adjustment_complete) is not bool
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
            raise PromotedCorporateActionBridgeError(_ERR_DERIVED)
        if type(self.bridge_id) is not str or _SHA256.fullmatch(self.bridge_id) is None:
            raise PromotedCorporateActionBridgeError(_ERR_ID)
        facts = _build_facts(self.source_panel, self.corporate_actions, self.cutoff)
        try:
            comparisons = (
                (self.cutoff, facts.cutoff),
                (self.signal_session, facts.signal_session),
                (self.knowledge_time, facts.knowledge_time),
                (self.results, facts.results),
                (self.status_counts, facts.status_counts),
                (
                    self.resolved_histories_adjustment_complete,
                    facts.resolved_histories_adjustment_complete,
                ),
                (self.readiness, facts.readiness),
                (self.actionable, facts.actionable),
                (self.training_eligible, facts.training_eligible),
                (self.feature_eligible, facts.feature_eligible),
                (self.alert_eligible, facts.alert_eligible),
                (self.execution_eligible, facts.execution_eligible),
                (self.bridge_id, facts.bridge_id),
            )
            if any(left != right for left, right in comparisons):
                raise PromotedCorporateActionBridgeError(_ERR_DERIVED)
        except PromotedCorporateActionBridgeError:
            raise
        except Exception:
            raise PromotedCorporateActionBridgeError(_ERR_DERIVED) from None


class PromotedCorporateActionAdjustmentService:
    def materialize(
        self,
        *,
        source_panel: VerifiedPromotedStableListingHistoryPanel,
        corporate_actions: CorporateActionSnapshot,
        cutoff: datetime,
    ) -> VerifiedPromotedCorporateActionAdjustmentPanel:
        facts = _build_facts(source_panel, corporate_actions, cutoff)
        return VerifiedPromotedCorporateActionAdjustmentPanel(
            schema_version=PROMOTED_CORPORATE_ACTION_BRIDGE_SCHEMA_VERSION,
            policy_version=PROMOTED_CORPORATE_ACTION_BRIDGE_POLICY_VERSION,
            source_panel=source_panel,
            corporate_actions=corporate_actions,
            cutoff=facts.cutoff,
            signal_session=facts.signal_session,
            knowledge_time=facts.knowledge_time,
            results=facts.results,
            status_counts=facts.status_counts,
            resolved_histories_adjustment_complete=(
                facts.resolved_histories_adjustment_complete
            ),
            readiness=facts.readiness,
            actionable=facts.actionable,
            training_eligible=facts.training_eligible,
            feature_eligible=facts.feature_eligible,
            alert_eligible=facts.alert_eligible,
            execution_eligible=facts.execution_eligible,
            bridge_id=facts.bridge_id,
        )
