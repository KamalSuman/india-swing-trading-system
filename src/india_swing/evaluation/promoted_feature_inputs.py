"""Strict point-in-time assembly of promoted feature-calculation inputs.

This module joins only two already-verified collection boundaries:

* corporate-action-adjusted stable-listing price histories; and
* exact-session, point-in-time-verified tick-size specifications.

The join key is always ``(stable_instrument_id, stable_listing_id,
market_session)``.  Symbols, tickers, names, and ISINs are deliberately not
accepted.  The output remains collection-only: it computes no indicators,
labels, ranks, scores, signals, alerts, orders, or portfolio decisions.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum

from india_swing.corporate_actions.promoted_adjustments import (
    PromotedCorporateActionAdjustedCorpusBar,
    PromotedCorporateActionAdjustedHistory,
    PromotedCorporateActionAdjustmentResult,
    PromotedCorporateActionAdjustmentStatus,
    VerifiedPromotedCorporateActionAdjustmentPanel,
)
from india_swing.evaluation.dataset_assembly import EffectiveTickSize
from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness
from india_swing.tick_sizes.effective_session import (
    PromotedEffectiveSessionTickResult,
    PromotedEffectiveSessionTickStatus,
    VerifiedPromotedEffectiveSessionTickPanel,
)


class PromotedFeatureInputError(ValueError):
    """Raised when the promoted feature-input boundary fails closed."""


PROMOTED_FEATURE_INPUT_SCHEMA_VERSION = "promoted-feature-input-panel/v1"
PROMOTED_FEATURE_INPUT_POLICY_VERSION = (
    "promoted-feature-input/stable-listing-exact-session-join-v1"
)

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_REASON = re.compile(r"[A-Z][A-Z0-9_]{2,127}\Z")

_ERR_TYPE = "promoted feature-input type is invalid"
_ERR_INPUT = "promoted feature-input source is invalid"
_ERR_VERIFY = "promoted feature-input source could not be verified"
_ERR_LINEAGE = "promoted feature-input sources do not share exact lineage"
_ERR_CUTOFF = "promoted feature-input cutoff is invalid"
_ERR_FUTURE = "promoted feature-input contains future-known evidence"
_ERR_GRAPH = "promoted feature-input graph is invalid"
_ERR_DERIVED = "promoted feature-input derived content is invalid"
_ERR_ID = "promoted feature-input identifier is invalid"

_COMMON_REASONS = {
    "COLLECTION_ONLY_NO_DECISION_AUTHORITY",
    "FEATURE_DECISION_USE_NOT_AUTHORIZED",
    "NO_CROSS_SESSION_TICK_INFERENCE",
}
_ASSEMBLED_REASONS = {
    "ADJUSTED_HISTORY_AND_EXACT_SESSION_TICKS_ASSEMBLED",
}
_ADJUSTMENT_BLOCKED_REASONS = {
    "CORPORATE_ACTION_ADJUSTMENT_NOT_COMPLETE",
}
_TICK_BLOCKED_REASONS = {
    "EXACT_SESSION_TICK_COVERAGE_INCOMPLETE",
}


def _utc(value: datetime) -> datetime:
    if type(value) is not datetime:
        raise PromotedFeatureInputError(_ERR_CUTOFF)
    try:
        offset = value.utcoffset()
    except Exception:
        raise PromotedFeatureInputError(_ERR_CUTOFF) from None
    if value.tzinfo is None or offset is None:
        raise PromotedFeatureInputError(_ERR_CUTOFF)
    return value.astimezone(timezone.utc)


def _sha(value: object) -> bool:
    return type(value) is str and _SHA256.fullmatch(value) is not None


def _reasons(values: set[str]) -> tuple[str, ...]:
    result = tuple(sorted(_COMMON_REASONS | values))
    if any(type(value) is not str or _REASON.fullmatch(value) is None for value in result):
        raise PromotedFeatureInputError(_ERR_GRAPH)
    return result


def _counts(values: tuple[str, ...]) -> tuple[tuple[str, int], ...]:
    totals: dict[str, int] = {}
    for value in values:
        totals[value] = totals.get(value, 0) + 1
    return tuple(sorted(totals.items()))


class PromotedFeatureInputStatus(str, Enum):
    INPUT_GRAPH_ASSEMBLED_COLLECTION_ONLY = (
        "INPUT_GRAPH_ASSEMBLED_COLLECTION_ONLY"
    )
    CORPORATE_ACTION_ADJUSTMENT_BLOCKED = (
        "CORPORATE_ACTION_ADJUSTMENT_BLOCKED"
    )
    EXACT_SESSION_TICK_COVERAGE_BLOCKED = (
        "EXACT_SESSION_TICK_COVERAGE_BLOCKED"
    )


_STATUS_REASONS = {
    PromotedFeatureInputStatus.INPUT_GRAPH_ASSEMBLED_COLLECTION_ONLY: (
        _ASSEMBLED_REASONS
    ),
    PromotedFeatureInputStatus.CORPORATE_ACTION_ADJUSTMENT_BLOCKED: (
        _ADJUSTMENT_BLOCKED_REASONS
    ),
    PromotedFeatureInputStatus.EXACT_SESSION_TICK_COVERAGE_BLOCKED: (
        _TICK_BLOCKED_REASONS
    ),
}


@dataclass(frozen=True, slots=True)
class PromotedFeatureInputBar:
    """One adjusted OHLCV bar joined to its exact-session tick specification."""

    adjusted_bar: PromotedCorporateActionAdjustedCorpusBar
    tick_result: PromotedEffectiveSessionTickResult
    stable_instrument_id: str
    stable_listing_id: str
    market_session: date
    adjusted_open: Decimal
    adjusted_high: Decimal
    adjusted_low: Decimal
    adjusted_close: Decimal
    adjusted_volume: Decimal
    tick_size: Decimal
    knowledge_time: datetime
    input_bar_id: str

    def __post_init__(self) -> None:
        if (
            type(self.adjusted_bar) is not PromotedCorporateActionAdjustedCorpusBar
            or type(self.tick_result) is not PromotedEffectiveSessionTickResult
            or not _sha(self.stable_instrument_id)
            or not _sha(self.stable_listing_id)
            or type(self.market_session) is not date
        ):
            raise PromotedFeatureInputError(_ERR_GRAPH)
        try:
            self.adjusted_bar.verify_content_identity()
            specification = self.tick_result.tick_specification
            if specification is not None:
                specification.verify_content_identity()
        except Exception:
            raise PromotedFeatureInputError(_ERR_GRAPH) from None
        if (
            self.tick_result.status
            is not PromotedEffectiveSessionTickStatus.VERIFIED_EXACT_SESSION_ONLY
            or type(self.tick_result.tick_specification) is not EffectiveTickSize
        ):
            raise PromotedFeatureInputError(_ERR_GRAPH)
        specification = self.tick_result.tick_specification
        assert specification is not None
        source_session = self.adjusted_bar.source_bar.session
        if (
            self.stable_instrument_id != self.adjusted_bar.stable_instrument_id
            or self.stable_instrument_id != self.tick_result.stable_instrument_id
            or self.stable_instrument_id != specification.instrument_id
            or self.stable_listing_id != self.adjusted_bar.stable_listing_id
            or self.stable_listing_id != self.tick_result.stable_listing_id
            or self.stable_listing_id != specification.listing_id
            or self.market_session != source_session
            or self.market_session != self.tick_result.market_session
            or specification.effective_from_session != self.market_session
            or not specification.is_effective_on(self.market_session)
            or specification.readiness
            is not ReferenceReadiness.POINT_IN_TIME_VERIFIED
        ):
            raise PromotedFeatureInputError(_ERR_GRAPH)
        expected_values = (
            self.adjusted_bar.adjusted_open,
            self.adjusted_bar.adjusted_high,
            self.adjusted_bar.adjusted_low,
            self.adjusted_bar.adjusted_close,
            self.adjusted_bar.adjusted_volume,
            specification.tick_size,
        )
        actual_values = (
            self.adjusted_open,
            self.adjusted_high,
            self.adjusted_low,
            self.adjusted_close,
            self.adjusted_volume,
            self.tick_size,
        )
        if any(type(value) is not Decimal for value in actual_values):
            raise PromotedFeatureInputError(_ERR_GRAPH)
        if actual_values != expected_values:
            raise PromotedFeatureInputError(_ERR_GRAPH)
        knowledge_time = _utc(self.knowledge_time)
        expected_knowledge_time = max(
            self.adjusted_bar.knowledge_time,
            specification.knowledge_time,
        )
        if knowledge_time != expected_knowledge_time:
            raise PromotedFeatureInputError(_ERR_GRAPH)
        if knowledge_time != self.knowledge_time:
            object.__setattr__(self, "knowledge_time", knowledge_time)
        if not _sha(self.input_bar_id) or self.input_bar_id != self._calculated_id():
            raise PromotedFeatureInputError(_ERR_ID)

    def _identity(self) -> dict[str, object]:
        specification = self.tick_result.tick_specification
        assert specification is not None
        return {
            "adjusted_bar_id": self.adjusted_bar.adjusted_bar_id,
            "tick_specification_id": specification.specification_id,
            "stable_instrument_id": self.stable_instrument_id,
            "stable_listing_id": self.stable_listing_id,
            "market_session": self.market_session,
            "adjusted_open": self.adjusted_open,
            "adjusted_high": self.adjusted_high,
            "adjusted_low": self.adjusted_low,
            "adjusted_close": self.adjusted_close,
            "adjusted_volume": self.adjusted_volume,
            "tick_size": self.tick_size,
            "knowledge_time": self.knowledge_time,
        }

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "promoted-feature-input-bar/v1",
                **self._identity(),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        expected = PromotedFeatureInputBar(
            adjusted_bar=self.adjusted_bar,
            tick_result=self.tick_result,
            stable_instrument_id=self.stable_instrument_id,
            stable_listing_id=self.stable_listing_id,
            market_session=self.market_session,
            adjusted_open=self.adjusted_open,
            adjusted_high=self.adjusted_high,
            adjusted_low=self.adjusted_low,
            adjusted_close=self.adjusted_close,
            adjusted_volume=self.adjusted_volume,
            tick_size=self.tick_size,
            knowledge_time=self.knowledge_time,
            input_bar_id=self.input_bar_id,
        )
        if self.input_bar_id != expected.input_bar_id:
            raise PromotedFeatureInputError(_ERR_ID)


def _input_bar(
    adjusted_bar: PromotedCorporateActionAdjustedCorpusBar,
    tick_result: PromotedEffectiveSessionTickResult,
) -> PromotedFeatureInputBar:
    specification = tick_result.tick_specification
    if type(specification) is not EffectiveTickSize:
        raise PromotedFeatureInputError(_ERR_GRAPH)
    values = {
        "adjusted_bar": adjusted_bar,
        "tick_result": tick_result,
        "stable_instrument_id": adjusted_bar.stable_instrument_id,
        "stable_listing_id": adjusted_bar.stable_listing_id,
        "market_session": adjusted_bar.source_bar.session,
        "adjusted_open": adjusted_bar.adjusted_open,
        "adjusted_high": adjusted_bar.adjusted_high,
        "adjusted_low": adjusted_bar.adjusted_low,
        "adjusted_close": adjusted_bar.adjusted_close,
        "adjusted_volume": adjusted_bar.adjusted_volume,
        "tick_size": specification.tick_size,
        "knowledge_time": max(
            adjusted_bar.knowledge_time,
            specification.knowledge_time,
        ),
    }
    input_bar_id = content_id(
        {
            "schema": "promoted-feature-input-bar/v1",
            "adjusted_bar_id": adjusted_bar.adjusted_bar_id,
            "tick_specification_id": specification.specification_id,
            **{
                key: value
                for key, value in values.items()
                if key not in {"adjusted_bar", "tick_result"}
            },
        },
        length=64,
    )
    return PromotedFeatureInputBar(**values, input_bar_id=input_bar_id)


@dataclass(frozen=True, slots=True)
class PromotedFeatureInputHistory:
    source_adjusted_history: PromotedCorporateActionAdjustedHistory
    stable_instrument_id: str
    stable_listing_id: str
    signal_session: date
    cutoff: datetime
    knowledge_time: datetime
    bars: tuple[PromotedFeatureInputBar, ...]
    history_id: str

    def __post_init__(self) -> None:
        if (
            type(self.source_adjusted_history)
            is not PromotedCorporateActionAdjustedHistory
            or not _sha(self.stable_instrument_id)
            or not _sha(self.stable_listing_id)
            or type(self.signal_session) is not date
            or type(self.bars) is not tuple
            or not self.bars
            or any(type(value) is not PromotedFeatureInputBar for value in self.bars)
        ):
            raise PromotedFeatureInputError(_ERR_GRAPH)
        try:
            self.source_adjusted_history.verify_content_identity()
            for value in self.bars:
                value.verify_content_identity()
        except Exception:
            raise PromotedFeatureInputError(_ERR_GRAPH) from None
        cutoff = _utc(self.cutoff)
        knowledge_time = _utc(self.knowledge_time)
        sessions = tuple(value.market_session for value in self.bars)
        if (
            self.stable_instrument_id
            != self.source_adjusted_history.stable_instrument_id
            or self.stable_listing_id
            != self.source_adjusted_history.stable_listing_id
            or self.signal_session != self.source_adjusted_history.signal_session
            or sessions != tuple(sorted(set(sessions)))
            or sessions
            != tuple(
                value.source_bar.session
                for value in self.source_adjusted_history.bars
            )
            or any(
                value.stable_instrument_id != self.stable_instrument_id
                or value.stable_listing_id != self.stable_listing_id
                for value in self.bars
            )
            or knowledge_time != max(value.knowledge_time for value in self.bars)
            or knowledge_time > cutoff
        ):
            raise PromotedFeatureInputError(_ERR_GRAPH)
        if cutoff != self.cutoff:
            object.__setattr__(self, "cutoff", cutoff)
        if knowledge_time != self.knowledge_time:
            object.__setattr__(self, "knowledge_time", knowledge_time)
        if not _sha(self.history_id) or self.history_id != self._calculated_id():
            raise PromotedFeatureInputError(_ERR_ID)

    def _identity(self) -> dict[str, object]:
        return {
            "source_adjusted_history_id": self.source_adjusted_history.history_id,
            "stable_instrument_id": self.stable_instrument_id,
            "stable_listing_id": self.stable_listing_id,
            "signal_session": self.signal_session,
            "cutoff": self.cutoff,
            "knowledge_time": self.knowledge_time,
            "input_bar_ids": tuple(value.input_bar_id for value in self.bars),
        }

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "promoted-feature-input-history/v1",
                **self._identity(),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        expected = PromotedFeatureInputHistory(
            source_adjusted_history=self.source_adjusted_history,
            stable_instrument_id=self.stable_instrument_id,
            stable_listing_id=self.stable_listing_id,
            signal_session=self.signal_session,
            cutoff=self.cutoff,
            knowledge_time=self.knowledge_time,
            bars=self.bars,
            history_id=self.history_id,
        )
        if self.history_id != expected.history_id:
            raise PromotedFeatureInputError(_ERR_ID)


def _input_history(
    *,
    adjusted_history: PromotedCorporateActionAdjustedHistory,
    cutoff: datetime,
    bars: tuple[PromotedFeatureInputBar, ...],
) -> PromotedFeatureInputHistory:
    knowledge_time = max(value.knowledge_time for value in bars)
    values = {
        "source_adjusted_history": adjusted_history,
        "stable_instrument_id": adjusted_history.stable_instrument_id,
        "stable_listing_id": adjusted_history.stable_listing_id,
        "signal_session": adjusted_history.signal_session,
        "cutoff": cutoff,
        "knowledge_time": knowledge_time,
        "bars": bars,
    }
    history_id = content_id(
        {
            "schema": "promoted-feature-input-history/v1",
            "source_adjusted_history_id": adjusted_history.history_id,
            "stable_instrument_id": adjusted_history.stable_instrument_id,
            "stable_listing_id": adjusted_history.stable_listing_id,
            "signal_session": adjusted_history.signal_session,
            "cutoff": cutoff,
            "knowledge_time": knowledge_time,
            "input_bar_ids": tuple(value.input_bar_id for value in bars),
        },
        length=64,
    )
    return PromotedFeatureInputHistory(**values, history_id=history_id)


@dataclass(frozen=True, slots=True)
class PromotedFeatureInputResult:
    source_adjustment_result: PromotedCorporateActionAdjustmentResult
    source_tick_results: tuple[PromotedEffectiveSessionTickResult, ...]
    status: PromotedFeatureInputStatus
    input_history: PromotedFeatureInputHistory | None
    reason_codes: tuple[str, ...]
    result_id: str

    def __post_init__(self) -> None:
        if (
            type(self.source_adjustment_result)
            is not PromotedCorporateActionAdjustmentResult
            or type(self.source_tick_results) is not tuple
            or any(
                type(value) is not PromotedEffectiveSessionTickResult
                for value in self.source_tick_results
            )
            or type(self.status) is not PromotedFeatureInputStatus
            or (
                self.input_history is not None
                and type(self.input_history) is not PromotedFeatureInputHistory
            )
            or self.reason_codes != _reasons(_STATUS_REASONS[self.status])
        ):
            raise PromotedFeatureInputError(_ERR_GRAPH)
        try:
            self.source_adjustment_result.verify_content_identity()
            if self.input_history is not None:
                self.input_history.verify_content_identity()
        except Exception:
            raise PromotedFeatureInputError(_ERR_GRAPH) from None
        source = self.source_adjustment_result.source_history
        expected_sessions = tuple(value.market_session for value in source.observations)
        if (
            tuple(value.market_session for value in self.source_tick_results)
            != expected_sessions
            or any(
                value.stable_instrument_id != source.stable_instrument_id
                or value.stable_listing_id != source.stable_listing_id
                for value in self.source_tick_results
            )
        ):
            raise PromotedFeatureInputError(_ERR_GRAPH)
        assembled = (
            self.status
            is PromotedFeatureInputStatus.INPUT_GRAPH_ASSEMBLED_COLLECTION_ONLY
        )
        if assembled:
            if (
                self.source_adjustment_result.status
                is not (
                    PromotedCorporateActionAdjustmentStatus
                    .ADJUSTED_HISTORY_BUILT_COLLECTION_ONLY
                )
                or self.source_adjustment_result.adjusted_history is None
                or self.input_history is None
                or any(
                    value.status
                    is not PromotedEffectiveSessionTickStatus.VERIFIED_EXACT_SESSION_ONLY
                    for value in self.source_tick_results
                )
            ):
                raise PromotedFeatureInputError(_ERR_GRAPH)
            assert self.input_history is not None
            if (
                self.input_history.stable_instrument_id
                != source.stable_instrument_id
                or self.input_history.stable_listing_id
                != source.stable_listing_id
                or tuple(
                    value.tick_result._identity()
                    for value in self.input_history.bars
                )
                != tuple(value._identity() for value in self.source_tick_results)
            ):
                raise PromotedFeatureInputError(_ERR_GRAPH)
        else:
            if self.input_history is not None:
                raise PromotedFeatureInputError(_ERR_GRAPH)
            adjustment_succeeded = (
                self.source_adjustment_result.status
                is PromotedCorporateActionAdjustmentStatus.ADJUSTED_HISTORY_BUILT_COLLECTION_ONLY
            )
            tick_coverage_complete = all(
                value.status
                is PromotedEffectiveSessionTickStatus.VERIFIED_EXACT_SESSION_ONLY
                and value.tick_specification is not None
                for value in self.source_tick_results
            )
            if (
                self.status
                is PromotedFeatureInputStatus.CORPORATE_ACTION_ADJUSTMENT_BLOCKED
                and adjustment_succeeded
            ) or (
                self.status
                is PromotedFeatureInputStatus.EXACT_SESSION_TICK_COVERAGE_BLOCKED
                and (not adjustment_succeeded or tick_coverage_complete)
            ):
                raise PromotedFeatureInputError(_ERR_GRAPH)
        if not _sha(self.result_id) or self.result_id != self._calculated_id():
            raise PromotedFeatureInputError(_ERR_ID)

    def _identity(self) -> dict[str, object]:
        return {
            "source_adjustment_result_id": self.source_adjustment_result.result_id,
            "source_tick_cells": tuple(
                value._identity() for value in self.source_tick_results
            ),
            "status": self.status,
            "input_history_id": (
                None if self.input_history is None else self.input_history.history_id
            ),
            "reason_codes": self.reason_codes,
        }

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "promoted-feature-input-result/v1",
                **self._identity(),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        expected = PromotedFeatureInputResult(
            source_adjustment_result=self.source_adjustment_result,
            source_tick_results=self.source_tick_results,
            status=self.status,
            input_history=self.input_history,
            reason_codes=self.reason_codes,
            result_id=self.result_id,
        )
        if self.result_id != expected.result_id:
            raise PromotedFeatureInputError(_ERR_ID)


def _result(
    *,
    source_adjustment_result: PromotedCorporateActionAdjustmentResult,
    source_tick_results: tuple[PromotedEffectiveSessionTickResult, ...],
    status: PromotedFeatureInputStatus,
    input_history: PromotedFeatureInputHistory | None,
) -> PromotedFeatureInputResult:
    reason_codes = _reasons(_STATUS_REASONS[status])
    identity = {
        "source_adjustment_result_id": source_adjustment_result.result_id,
        "source_tick_cells": tuple(
            value._identity() for value in source_tick_results
        ),
        "status": status,
        "input_history_id": (
            None if input_history is None else input_history.history_id
        ),
        "reason_codes": reason_codes,
    }
    return PromotedFeatureInputResult(
        source_adjustment_result=source_adjustment_result,
        source_tick_results=source_tick_results,
        status=status,
        input_history=input_history,
        reason_codes=reason_codes,
        result_id=content_id(
            {"schema": "promoted-feature-input-result/v1", **identity},
            length=64,
        ),
    )


@dataclass(frozen=True, slots=True)
class _FeatureInputFacts:
    cutoff: datetime
    knowledge_time: datetime
    results: tuple[PromotedFeatureInputResult, ...]
    status_counts: tuple[tuple[str, int], ...]
    resolved_histories_input_complete: bool
    unassigned_entry_count: int
    readiness: ReferenceReadiness
    actionable: bool
    training_eligible: bool
    feature_eligible: bool
    cross_sectional_ranking_eligible: bool
    alert_eligible: bool
    execution_eligible: bool
    panel_id: str


def _panel_identity(
    *,
    adjustment_panel_id: str,
    tick_panel_id: str,
    source_panel_id: str,
    facts: _FeatureInputFacts,
) -> dict[str, object]:
    return {
        "schema_version": PROMOTED_FEATURE_INPUT_SCHEMA_VERSION,
        "policy_version": PROMOTED_FEATURE_INPUT_POLICY_VERSION,
        "adjustment_panel_id": adjustment_panel_id,
        "tick_panel_id": tick_panel_id,
        "source_panel_id": source_panel_id,
        "cutoff": facts.cutoff,
        "knowledge_time": facts.knowledge_time,
        "result_ids": tuple(value.result_id for value in facts.results),
        "status_counts": facts.status_counts,
        "resolved_histories_input_complete": (
            facts.resolved_histories_input_complete
        ),
        "unassigned_entry_count": facts.unassigned_entry_count,
        "readiness": facts.readiness,
        "actionable": facts.actionable,
        "training_eligible": facts.training_eligible,
        "feature_eligible": facts.feature_eligible,
        "cross_sectional_ranking_eligible": (
            facts.cross_sectional_ranking_eligible
        ),
        "alert_eligible": facts.alert_eligible,
        "execution_eligible": facts.execution_eligible,
    }


def _build_facts(
    adjustment_panel: VerifiedPromotedCorporateActionAdjustmentPanel,
    tick_panel: VerifiedPromotedEffectiveSessionTickPanel,
    cutoff: datetime,
) -> _FeatureInputFacts:
    if (
        type(adjustment_panel)
        is not VerifiedPromotedCorporateActionAdjustmentPanel
        or type(tick_panel) is not VerifiedPromotedEffectiveSessionTickPanel
    ):
        raise PromotedFeatureInputError(_ERR_INPUT)
    cutoff = _utc(cutoff)
    try:
        adjustment_panel.verify_content_identity()
        tick_panel.verify_content_identity()
    except Exception:
        raise PromotedFeatureInputError(_ERR_VERIFY) from None

    source_panel = adjustment_panel.source_panel
    if (
        source_panel.panel_id != tick_panel.source_panel.panel_id
        or adjustment_panel.signal_session != source_panel.sessions[-1]
    ):
        raise PromotedFeatureInputError(_ERR_LINEAGE)
    if cutoff < max(
        adjustment_panel.cutoff,
        tick_panel.cutoff,
        adjustment_panel.knowledge_time,
        tick_panel.knowledge_time,
    ):
        raise PromotedFeatureInputError(_ERR_FUTURE)
    for panel in (adjustment_panel, tick_panel):
        if (
            panel.readiness is not ReferenceReadiness.COLLECTION_ONLY
            or panel.actionable is not False
            or panel.training_eligible is not False
            or panel.feature_eligible is not False
            or panel.alert_eligible is not False
            or panel.execution_eligible is not False
        ):
            raise PromotedFeatureInputError(_ERR_INPUT)

    tick_by_key: dict[
        tuple[str, str, date],
        PromotedEffectiveSessionTickResult,
    ] = {}
    try:
        for tick_result in tick_panel.results:
            key = (
                tick_result.stable_instrument_id,
                tick_result.stable_listing_id,
                tick_result.market_session,
            )
            if key in tick_by_key:
                raise PromotedFeatureInputError(_ERR_GRAPH)
            tick_by_key[key] = tick_result

        results: list[PromotedFeatureInputResult] = []
        for adjustment_result in adjustment_panel.results:
            source_history = adjustment_result.source_history
            history_tick_results = tuple(
                tick_by_key[
                    (
                        source_history.stable_instrument_id,
                        source_history.stable_listing_id,
                        observation.market_session,
                    )
                ]
                for observation in source_history.observations
            )
            if (
                adjustment_result.status
                is not (
                    PromotedCorporateActionAdjustmentStatus
                    .ADJUSTED_HISTORY_BUILT_COLLECTION_ONLY
                )
            ):
                results.append(
                    _result(
                        source_adjustment_result=adjustment_result,
                        source_tick_results=history_tick_results,
                        status=PromotedFeatureInputStatus.CORPORATE_ACTION_ADJUSTMENT_BLOCKED,
                        input_history=None,
                    )
                )
                continue
            adjusted_history = adjustment_result.adjusted_history
            if adjusted_history is None:
                raise PromotedFeatureInputError(_ERR_GRAPH)
            if any(
                value.status
                is not PromotedEffectiveSessionTickStatus.VERIFIED_EXACT_SESSION_ONLY
                or value.tick_specification is None
                for value in history_tick_results
            ):
                results.append(
                    _result(
                        source_adjustment_result=adjustment_result,
                        source_tick_results=history_tick_results,
                        status=PromotedFeatureInputStatus.EXACT_SESSION_TICK_COVERAGE_BLOCKED,
                        input_history=None,
                    )
                )
                continue
            tick_for_session = {
                value.market_session: value for value in history_tick_results
            }
            bars = tuple(
                _input_bar(adjusted_bar, tick_for_session[adjusted_bar.source_bar.session])
                for adjusted_bar in adjusted_history.bars
            )
            input_history = _input_history(
                adjusted_history=adjusted_history,
                cutoff=cutoff,
                bars=bars,
            )
            results.append(
                _result(
                    source_adjustment_result=adjustment_result,
                    source_tick_results=history_tick_results,
                    status=PromotedFeatureInputStatus.INPUT_GRAPH_ASSEMBLED_COLLECTION_ONLY,
                    input_history=input_history,
                )
            )
    except PromotedFeatureInputError:
        raise
    except Exception:
        raise PromotedFeatureInputError(_ERR_GRAPH) from None

    results_tuple = tuple(
        sorted(
            results,
            key=lambda value: (
                value.source_adjustment_result.source_history.stable_instrument_id,
                value.source_adjustment_result.source_history.stable_listing_id,
            ),
        )
    )
    if len(results_tuple) != len(source_panel.histories):
        raise PromotedFeatureInputError(_ERR_GRAPH)
    status_counts = _counts(tuple(value.status.value for value in results_tuple))
    resolved_complete = bool(results_tuple) and all(
        value.status
        is PromotedFeatureInputStatus.INPUT_GRAPH_ASSEMBLED_COLLECTION_ONLY
        for value in results_tuple
    )
    knowledge_time = max(
        adjustment_panel.knowledge_time,
        tick_panel.knowledge_time,
    )
    provisional = _FeatureInputFacts(
        cutoff=cutoff,
        knowledge_time=knowledge_time,
        results=results_tuple,
        status_counts=status_counts,
        resolved_histories_input_complete=resolved_complete,
        unassigned_entry_count=len(source_panel.unassigned_entries),
        readiness=ReferenceReadiness.COLLECTION_ONLY,
        actionable=False,
        training_eligible=False,
        feature_eligible=False,
        cross_sectional_ranking_eligible=False,
        alert_eligible=False,
        execution_eligible=False,
        panel_id="",
    )
    panel_id = content_id(
        _panel_identity(
            adjustment_panel_id=adjustment_panel.bridge_id,
            tick_panel_id=tick_panel.panel_id,
            source_panel_id=source_panel.panel_id,
            facts=provisional,
        ),
        length=64,
    )
    return _FeatureInputFacts(
        cutoff=provisional.cutoff,
        knowledge_time=provisional.knowledge_time,
        results=provisional.results,
        status_counts=provisional.status_counts,
        resolved_histories_input_complete=(
            provisional.resolved_histories_input_complete
        ),
        unassigned_entry_count=provisional.unassigned_entry_count,
        readiness=provisional.readiness,
        actionable=provisional.actionable,
        training_eligible=provisional.training_eligible,
        feature_eligible=provisional.feature_eligible,
        cross_sectional_ranking_eligible=(
            provisional.cross_sectional_ranking_eligible
        ),
        alert_eligible=provisional.alert_eligible,
        execution_eligible=provisional.execution_eligible,
        panel_id=panel_id,
    )


@dataclass(frozen=True, slots=True)
class VerifiedPromotedFeatureInputPanel:
    """Verified collection-only inputs for a later feature-computation stage."""

    schema_version: str
    policy_version: str
    adjustment_panel: VerifiedPromotedCorporateActionAdjustmentPanel
    tick_panel: VerifiedPromotedEffectiveSessionTickPanel
    cutoff: datetime
    knowledge_time: datetime
    results: tuple[PromotedFeatureInputResult, ...]
    status_counts: tuple[tuple[str, int], ...]
    resolved_histories_input_complete: bool
    unassigned_entry_count: int
    readiness: ReferenceReadiness
    actionable: bool
    training_eligible: bool
    feature_eligible: bool
    cross_sectional_ranking_eligible: bool
    alert_eligible: bool
    execution_eligible: bool
    panel_id: str

    def __post_init__(self) -> None:
        self.verify_content_identity()

    def verify_content_identity(self) -> None:
        if type(self) is not VerifiedPromotedFeatureInputPanel:
            raise PromotedFeatureInputError(_ERR_TYPE)
        if (
            type(self.schema_version) is not str
            or self.schema_version != PROMOTED_FEATURE_INPUT_SCHEMA_VERSION
            or type(self.policy_version) is not str
            or self.policy_version != PROMOTED_FEATURE_INPUT_POLICY_VERSION
            or type(self.adjustment_panel)
            is not VerifiedPromotedCorporateActionAdjustmentPanel
            or type(self.tick_panel) is not VerifiedPromotedEffectiveSessionTickPanel
            or type(self.cutoff) is not datetime
            or type(self.knowledge_time) is not datetime
            or type(self.results) is not tuple
            or any(type(value) is not PromotedFeatureInputResult for value in self.results)
            or type(self.status_counts) is not tuple
            or any(
                type(value) is not tuple
                or len(value) != 2
                or type(value[0]) is not str
                or type(value[1]) is not int
                for value in self.status_counts
            )
            or type(self.resolved_histories_input_complete) is not bool
            or type(self.unassigned_entry_count) is not int
            or self.unassigned_entry_count < 0
            or type(self.readiness) is not ReferenceReadiness
            or any(
                type(value) is not bool
                for value in (
                    self.actionable,
                    self.training_eligible,
                    self.feature_eligible,
                    self.cross_sectional_ranking_eligible,
                    self.alert_eligible,
                    self.execution_eligible,
                )
            )
            or not _sha(self.panel_id)
        ):
            raise PromotedFeatureInputError(_ERR_DERIVED)
        try:
            facts = _build_facts(
                self.adjustment_panel,
                self.tick_panel,
                self.cutoff,
            )
            comparisons = (
                (self.cutoff, facts.cutoff),
                (self.knowledge_time, facts.knowledge_time),
                (self.results, facts.results),
                (self.status_counts, facts.status_counts),
                (
                    self.resolved_histories_input_complete,
                    facts.resolved_histories_input_complete,
                ),
                (self.unassigned_entry_count, facts.unassigned_entry_count),
                (self.readiness, facts.readiness),
                (self.actionable, facts.actionable),
                (self.training_eligible, facts.training_eligible),
                (self.feature_eligible, facts.feature_eligible),
                (
                    self.cross_sectional_ranking_eligible,
                    facts.cross_sectional_ranking_eligible,
                ),
                (self.alert_eligible, facts.alert_eligible),
                (self.execution_eligible, facts.execution_eligible),
                (self.panel_id, facts.panel_id),
            )
            if any(left != right for left, right in comparisons):
                raise PromotedFeatureInputError(_ERR_DERIVED)
        except PromotedFeatureInputError:
            raise
        except Exception:
            raise PromotedFeatureInputError(_ERR_DERIVED) from None


class PromotedFeatureInputService:
    """Assembles verified inputs without granting feature or trading authority."""

    def materialize(
        self,
        *,
        adjustment_panel: VerifiedPromotedCorporateActionAdjustmentPanel,
        tick_panel: VerifiedPromotedEffectiveSessionTickPanel,
        cutoff: datetime,
    ) -> VerifiedPromotedFeatureInputPanel:
        facts = _build_facts(adjustment_panel, tick_panel, cutoff)
        return VerifiedPromotedFeatureInputPanel(
            schema_version=PROMOTED_FEATURE_INPUT_SCHEMA_VERSION,
            policy_version=PROMOTED_FEATURE_INPUT_POLICY_VERSION,
            adjustment_panel=adjustment_panel,
            tick_panel=tick_panel,
            cutoff=facts.cutoff,
            knowledge_time=facts.knowledge_time,
            results=facts.results,
            status_counts=facts.status_counts,
            resolved_histories_input_complete=(
                facts.resolved_histories_input_complete
            ),
            unassigned_entry_count=facts.unassigned_entry_count,
            readiness=facts.readiness,
            actionable=facts.actionable,
            training_eligible=facts.training_eligible,
            feature_eligible=facts.feature_eligible,
            cross_sectional_ranking_eligible=(
                facts.cross_sectional_ranking_eligible
            ),
            alert_eligible=facts.alert_eligible,
            execution_eligible=facts.execution_eligible,
            panel_id=facts.panel_id,
        )
