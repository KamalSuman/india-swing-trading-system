from __future__ import annotations

from dataclasses import dataclass, field, fields

from india_swing.corporate_actions.adjustments import (
    ADJUSTED_PRICE_BASIS,
    CorporateActionAdjustedHistory,
)
from india_swing.domain.models import EvidenceItem
from india_swing.evaluation.dataset_assembly import EffectiveTickSize
from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness

from .deterministic_swing import AsOfSwingBar, InstrumentSwingHistory


class SwingHistoryAdapterError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class SwingHistoryMaterialization:
    adjusted_history_id: str
    history: InstrumentSwingHistory
    evidence: tuple[EvidenceItem, ...]
    materialization_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.adjusted_history_id) is not str or len(self.adjusted_history_id) != 64:
            raise SwingHistoryAdapterError("adjusted_history_id must be a full content ID")
        if type(self.history) is not InstrumentSwingHistory:
            raise SwingHistoryAdapterError("history must be exact")
        self.history.verify_content_identity()
        if (
            type(self.evidence) is not tuple
            or not self.evidence
            or any(type(value) is not EvidenceItem for value in self.evidence)
        ):
            raise SwingHistoryAdapterError("evidence must be a non-empty exact tuple")
        evidence_ids = tuple(value.evidence_id for value in self.evidence)
        if len(set(evidence_ids)) != len(evidence_ids):
            raise SwingHistoryAdapterError("materialized evidence IDs must be unique")
        expected_ids = tuple(value.evidence_id for value in self.history.bars) + (
            self.history.tick_evidence_id,
            self.history.adjustment_evidence_id,
        )
        if evidence_ids != expected_ids:
            raise SwingHistoryAdapterError("evidence does not cover history inputs exactly")
        object.__setattr__(self, "materialization_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "materialization_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self.history.verify_content_identity()
        if self.materialization_id != self._calculated_id():
            raise SwingHistoryAdapterError("materialization content identity failed")


def materialize_swing_history(
    *,
    adjusted: CorporateActionAdjustedHistory,
    tick_size: EffectiveTickSize,
) -> SwingHistoryMaterialization:
    if type(adjusted) is not CorporateActionAdjustedHistory:
        raise SwingHistoryAdapterError("adjusted history must be exact")
    if type(tick_size) is not EffectiveTickSize:
        raise SwingHistoryAdapterError("tick size must be exact")
    adjusted.verify_content_identity()
    tick_size.verify_content_identity()
    if (
        tick_size.instrument_id != adjusted.stable_instrument_id
        or tick_size.listing_id != adjusted.stable_listing_id
    ):
        raise SwingHistoryAdapterError("tick size belongs to another instrument or listing")
    if not tick_size.is_effective_on(adjusted.signal_session):
        raise SwingHistoryAdapterError("tick size is not effective on the signal session")
    if tick_size.knowledge_time > adjusted.cutoff:
        raise SwingHistoryAdapterError("tick size is future-known")
    if tick_size.readiness is ReferenceReadiness.COLLECTION_ONLY:
        raise SwingHistoryAdapterError("tick size is collection-only")

    swing_bars = tuple(
        AsOfSwingBar(
            market_session=value.market_session,
            open=value.open,
            high=value.high,
            low=value.low,
            close=value.close,
            volume=value.volume,
            traded_value=value.traded_value,
            available_at=value.knowledge_time,
            evidence_id=value.adjusted_bar_id,
            content_hash=value.adjusted_bar_id,
        )
        for value in adjusted.bars
    )
    history = InstrumentSwingHistory(
        instrument_id=adjusted.stable_instrument_id,
        listing_id=adjusted.stable_listing_id,
        tick_size=tick_size.tick_size,
        tick_available_at=tick_size.knowledge_time,
        tick_evidence_id=tick_size.specification_id,
        tick_content_hash=tick_size.specification_id,
        adjustment_available_at=adjusted.adjustment_knowledge_time,
        adjustment_evidence_id=adjusted.corporate_action_snapshot_id,
        adjustment_content_hash=adjusted.corporate_action_snapshot_id,
        price_basis=ADJUSTED_PRICE_BASIS,
        bars=swing_bars,
    )
    evidence = tuple(
        EvidenceItem(
            evidence_id=value.adjusted_bar_id,
            source="CORPORATE_ACTION_ADJUSTED_NSE_EOD",
            published_at=value.raw_knowledge_time,
            available_at=value.knowledge_time,
            content_hash=value.adjusted_bar_id,
        )
        for value in adjusted.bars
    ) + (
        EvidenceItem(
            evidence_id=tick_size.specification_id,
            source="EFFECTIVE_TICK_SIZE",
            published_at=tick_size.knowledge_time,
            available_at=tick_size.knowledge_time,
            content_hash=tick_size.specification_id,
        ),
        EvidenceItem(
            evidence_id=adjusted.corporate_action_snapshot_id,
            source="CORPORATE_ACTION_SNAPSHOT",
            published_at=adjusted.adjustment_knowledge_time,
            available_at=adjusted.adjustment_knowledge_time,
            content_hash=adjusted.corporate_action_snapshot_id,
        ),
    )
    return SwingHistoryMaterialization(
        adjusted_history_id=adjusted.history_id,
        history=history,
        evidence=evidence,
    )
