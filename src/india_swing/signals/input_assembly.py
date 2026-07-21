from __future__ import annotations

import re
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timezone

from india_swing.corporate_actions.adjustments import (
    CorporateActionAdjustedHistory,
    StableRawBarBinding,
    build_adjusted_price_history,
)
from india_swing.corporate_actions.models import CorporateActionSnapshot
from india_swing.evaluation.dataset_assembly import EffectiveTickSize
from india_swing.historical_prices.models import NseEodSessionArtifact, RawNseEodBar
from india_swing.identity import content_id
from india_swing.promotion.gate import evaluate_promotion
from india_swing.promotion.models import (
    PromotionCapability,
    PromotionDecision,
)
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference.universe import (
    UniverseDisposition,
    UniverseEntry,
    UniverseSnapshot,
)

from .history_adapter import SwingHistoryMaterialization, materialize_swing_history


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class SwingInputAssemblyError(ValueError):
    pass


def _sha(value: str, name: str) -> None:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise SwingInputAssemblyError(f"{name} must be a lowercase SHA-256")


def _utc(value: datetime, name: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is None:
        raise SwingInputAssemblyError(f"{name} must be timezone-aware")
    try:
        offset = value.utcoffset()
    except Exception:
        raise SwingInputAssemblyError(f"{name} has invalid timezone behavior") from None
    if offset is None:
        raise SwingInputAssemblyError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class SwingInputAssembly:
    promotion_decision_id: str
    promotion: PromotionDecision
    stable_identity_snapshot_id: str
    stable_instrument_id: str
    stable_listing_id: str
    signal_session: date
    cutoff: datetime
    raw_artifact_ids: tuple[str, ...]
    universe_snapshot_ids: tuple[str, ...]
    identity_bindings: tuple[StableRawBarBinding, ...]
    adjusted_history: CorporateActionAdjustedHistory
    signal_materialization: SwingHistoryMaterialization
    readiness: ReferenceReadiness
    actionable: bool
    schema_version: str = "swing-input-assembly/v1"
    assembly_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.promotion_decision_id, "promotion_decision_id"),
            (self.stable_identity_snapshot_id, "stable_identity_snapshot_id"),
            (self.stable_instrument_id, "stable_instrument_id"),
            (self.stable_listing_id, "stable_listing_id"),
        ):
            _sha(value, name)
        if type(self.signal_session) is not date:
            raise SwingInputAssemblyError("signal_session must be a date")
        object.__setattr__(self, "cutoff", _utc(self.cutoff, "assembly cutoff"))
        for values, name in (
            (self.raw_artifact_ids, "raw_artifact_ids"),
            (self.universe_snapshot_ids, "universe_snapshot_ids"),
        ):
            if type(values) is not tuple or not values:
                raise SwingInputAssemblyError(f"{name} must be a non-empty exact tuple")
            for value in values:
                _sha(value, name)
        if len(set(self.raw_artifact_ids)) != len(self.raw_artifact_ids):
            raise SwingInputAssemblyError("raw artifact IDs must be unique")
        if len(set(self.universe_snapshot_ids)) != len(self.universe_snapshot_ids):
            raise SwingInputAssemblyError("universe snapshot IDs must be unique")
        if (
            type(self.identity_bindings) is not tuple
            or not self.identity_bindings
            or any(type(value) is not StableRawBarBinding for value in self.identity_bindings)
        ):
            raise SwingInputAssemblyError("identity bindings must be a non-empty exact tuple")
        for value in self.identity_bindings:
            value.verify_content_identity()
            if (
                value.stable_instrument_id != self.stable_instrument_id
                or value.stable_listing_id != self.stable_listing_id
                or value.knowledge_time > self.cutoff
            ):
                raise SwingInputAssemblyError("identity binding differs from the assembly")
        binding_sessions = tuple(value.market_session for value in self.identity_bindings)
        if binding_sessions != tuple(sorted(set(binding_sessions))):
            raise SwingInputAssemblyError("identity binding sessions must be ordered and unique")
        if not (
            len(self.raw_artifact_ids)
            == len(self.universe_snapshot_ids)
            == len(self.identity_bindings)
        ):
            raise SwingInputAssemblyError(
                "raw, universe, and identity lineage must have equal session coverage"
            )
        if any(
            value.identity_snapshot_id != self.stable_identity_snapshot_id
            for value in self.identity_bindings
        ):
            raise SwingInputAssemblyError(
                "identity bindings differ from stable-identity snapshot lineage"
            )
        if type(self.adjusted_history) is not CorporateActionAdjustedHistory:
            raise SwingInputAssemblyError("adjusted_history must be exact")
        if type(self.signal_materialization) is not SwingHistoryMaterialization:
            raise SwingInputAssemblyError("signal_materialization must be exact")
        self.adjusted_history.verify_content_identity()
        self.signal_materialization.verify_content_identity()
        if (
            self.adjusted_history.stable_instrument_id != self.stable_instrument_id
            or self.adjusted_history.stable_listing_id != self.stable_listing_id
            or self.adjusted_history.signal_session != self.signal_session
            or self.adjusted_history.cutoff != self.cutoff
            or tuple(value.market_session for value in self.adjusted_history.bars)
            != binding_sessions
            or tuple(value.raw_bar_id for value in self.adjusted_history.bars)
            != tuple(value.raw_bar_id for value in self.identity_bindings)
            or self.signal_materialization.adjusted_history_id
            != self.adjusted_history.history_id
        ):
            raise SwingInputAssemblyError("assembled signal history lineage is inconsistent")
        if type(self.readiness) is not ReferenceReadiness:
            raise SwingInputAssemblyError("assembly readiness must be exact")
        if type(self.actionable) is not bool:
            raise SwingInputAssemblyError("assembly actionable must be bool")
        if self.readiness is ReferenceReadiness.COLLECTION_ONLY:
            raise SwingInputAssemblyError("collection-only inputs cannot be assembled")
        if self.actionable != (
            self.readiness is ReferenceReadiness.POINT_IN_TIME_VERIFIED
        ):
            raise SwingInputAssemblyError(
                "only point-in-time-verified inputs can be alert actionable"
            )
        if self.schema_version != "swing-input-assembly/v1":
            raise SwingInputAssemblyError("unsupported swing input assembly schema")
        self._verify_promotion()
        object.__setattr__(self, "assembly_id", self._calculated_id())

    def _verify_promotion(self) -> None:
        if type(self.promotion) is not PromotionDecision:
            raise SwingInputAssemblyError("promotion must be exact")
        self.promotion.verify_content_identity()
        if self.promotion.decision_id != self.promotion_decision_id:
            raise SwingInputAssemblyError(
                "promotion decision ID does not match the assembly lineage"
            )
        binding_sessions = tuple(value.market_session for value in self.identity_bindings)
        replayed = evaluate_promotion(
            market_session=self.signal_session,
            history_start=binding_sessions[0],
            decision_cutoff=self.cutoff,
            evidence=self.promotion.evidence,
        )
        if replayed.decision_id != self.promotion_decision_id:
            raise SwingInputAssemblyError("promotion decision does not replay from its evidence")
        for capability, expected in (
            (PromotionCapability.RAW_PRICES, tuple(sorted(self.raw_artifact_ids))),
            (PromotionCapability.UNIVERSE, tuple(sorted(self.universe_snapshot_ids))),
            (PromotionCapability.STABLE_IDENTITY, (self.stable_identity_snapshot_id,)),
            (
                PromotionCapability.CORPORATE_ACTIONS,
                (self.adjusted_history.corporate_action_snapshot_id,),
            ),
            (
                PromotionCapability.TICK_SIZES,
                (self.signal_materialization.history.tick_evidence_id,),
            ),
        ):
            evidence = self.promotion.evidence_for(capability)
            if evidence.source_snapshot_ids != expected:
                raise SwingInputAssemblyError(
                    f"{capability.value} promotion does not bind the exact engine inputs"
                )
            if (
                evidence.readiness is not self.readiness
                or not evidence.complete
                or not evidence.actionable
            ):
                raise SwingInputAssemblyError(
                    f"{capability.value} promotion readiness differs from engine inputs"
                )
        if self.actionable and not replayed.alert_eligible:
            raise SwingInputAssemblyError("verified engine inputs are not alert eligible")

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "assembly_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        for value in self.identity_bindings:
            if type(value) is not StableRawBarBinding:
                raise SwingInputAssemblyError("assembly contains an invalid identity binding")
            value.verify_content_identity()
        self.adjusted_history.verify_content_identity()
        self.signal_materialization.verify_content_identity()
        self._verify_promotion()
        if self.assembly_id != self._calculated_id():
            raise SwingInputAssemblyError("alert input assembly content identity failed")


def _exact_promoted_sources(
    decision: PromotionDecision,
    capability: PromotionCapability,
    expected: tuple[str, ...],
) -> None:
    evidence = decision.evidence_for(capability)
    if evidence.source_snapshot_ids != tuple(sorted(expected)):
        raise SwingInputAssemblyError(
            f"{capability.value} promotion does not bind the exact engine inputs"
        )


def _entry_for(
    snapshot: UniverseSnapshot,
    stable_instrument_id: str,
    stable_listing_id: str,
) -> UniverseEntry:
    matches = tuple(
        value
        for value in snapshot.entries
        if value.listing.instrument_id == stable_instrument_id
        and value.listing.listing_id == stable_listing_id
    )
    if len(matches) != 1:
        raise SwingInputAssemblyError(
            "each universe snapshot requires one exact stable listing"
        )
    return matches[0]


def _bar_for(artifact: NseEodSessionArtifact, entry: UniverseEntry) -> RawNseEodBar:
    listing = entry.listing
    matches = tuple(
        value
        for value in artifact.bars
        if value.symbol == listing.tradingsymbol
        and value.series == listing.series
        and value.validated_isin == listing.isin
    )
    if len(matches) != 1:
        raise SwingInputAssemblyError(
            "each price session requires one exact stable-listing bar"
        )
    return matches[0]


def assemble_swing_inputs(
    *,
    history: tuple[NseEodSessionArtifact, ...],
    universes: tuple[UniverseSnapshot, ...],
    stable_identity_snapshot_id: str,
    stable_instrument_id: str,
    stable_listing_id: str,
    signal_session: date,
    cutoff: datetime,
    corporate_actions: CorporateActionSnapshot,
    tick_size: EffectiveTickSize,
    promotion: PromotionDecision,
) -> SwingInputAssembly:
    """Bind promoted raw sessions to one stable listing and build signal inputs.

    Collection artifacts are never upgraded by this function. It requires an
    independently created, replay-verifiable promotion decision whose source
    IDs exactly equal the artifacts actually consumed. Only the alert wrapper
    requires the resulting assembly to be alert actionable.
    """

    _sha(stable_identity_snapshot_id, "stable_identity_snapshot_id")
    _sha(stable_instrument_id, "stable_instrument_id")
    _sha(stable_listing_id, "stable_listing_id")
    if type(signal_session) is not date:
        raise SwingInputAssemblyError("signal_session must be a date")
    cutoff = _utc(cutoff, "assembly cutoff")
    if (
        type(history) is not tuple
        or not history
        or any(type(value) is not NseEodSessionArtifact for value in history)
    ):
        raise SwingInputAssemblyError("history must be a non-empty exact tuple")
    if (
        type(universes) is not tuple
        or len(universes) != len(history)
        or any(type(value) is not UniverseSnapshot for value in universes)
    ):
        raise SwingInputAssemblyError("one exact universe is required per price session")
    sessions = tuple(value.market_session for value in history)
    if sessions != tuple(sorted(set(sessions))) or sessions[-1] != signal_session:
        raise SwingInputAssemblyError("price history sessions must be ordered and end on signal_session")
    if tuple(value.market_session for value in universes) != sessions:
        raise SwingInputAssemblyError("universe sessions differ from price history")
    if type(promotion) is not PromotionDecision:
        raise SwingInputAssemblyError("promotion must be exact")
    promotion.verify_content_identity()
    replayed_promotion = evaluate_promotion(
        market_session=signal_session,
        history_start=sessions[0],
        decision_cutoff=cutoff,
        evidence=promotion.evidence,
    )
    if replayed_promotion.decision_id != promotion.decision_id:
        raise SwingInputAssemblyError("promotion decision does not replay from its evidence")
    if type(corporate_actions) is not CorporateActionSnapshot:
        raise SwingInputAssemblyError("corporate_actions must be exact")
    if type(tick_size) is not EffectiveTickSize:
        raise SwingInputAssemblyError("tick_size must be exact")
    corporate_actions.verify_content_identity()
    tick_size.verify_content_identity()

    readiness_values = {value.readiness for value in universes}
    if len(readiness_values) != 1:
        raise SwingInputAssemblyError("universe readiness must be consistent across history")
    readiness = next(iter(readiness_values))
    if readiness is ReferenceReadiness.COLLECTION_ONLY:
        raise SwingInputAssemblyError("collection-only universe cannot enter the signal engine")
    if corporate_actions.readiness is not readiness or tick_size.readiness is not readiness:
        raise SwingInputAssemblyError("engine input readiness levels disagree")
    relevant_capabilities = (
        PromotionCapability.RAW_PRICES,
        PromotionCapability.STABLE_IDENTITY,
        PromotionCapability.UNIVERSE,
        PromotionCapability.CORPORATE_ACTIONS,
        PromotionCapability.TICK_SIZES,
    )
    for capability in relevant_capabilities:
        evidence = promotion.evidence_for(capability)
        if (
            evidence.readiness is not readiness
            or not evidence.complete
            or not evidence.actionable
        ):
            raise SwingInputAssemblyError(
                f"{capability.value} promotion readiness differs from engine inputs"
            )
    actionable = readiness is ReferenceReadiness.POINT_IN_TIME_VERIFIED
    if actionable and not promotion.alert_eligible:
        raise SwingInputAssemblyError("verified engine inputs are not alert eligible")

    raw_ids = tuple(value.artifact_id for value in history)
    universe_ids = tuple(value.snapshot_id for value in universes)
    _exact_promoted_sources(promotion, PromotionCapability.RAW_PRICES, raw_ids)
    _exact_promoted_sources(promotion, PromotionCapability.UNIVERSE, universe_ids)
    _exact_promoted_sources(
        promotion,
        PromotionCapability.STABLE_IDENTITY,
        (stable_identity_snapshot_id,),
    )
    _exact_promoted_sources(
        promotion,
        PromotionCapability.CORPORATE_ACTIONS,
        (corporate_actions.snapshot_id,),
    )
    _exact_promoted_sources(
        promotion,
        PromotionCapability.TICK_SIZES,
        (tick_size.specification_id,),
    )

    raw_bars: list[RawNseEodBar] = []
    bindings: list[StableRawBarBinding] = []
    entries: list[UniverseEntry] = []
    identity_evidence = promotion.evidence_for(PromotionCapability.STABLE_IDENTITY)
    for artifact, universe in zip(history, universes, strict=True):
        artifact.verify_content_identity()
        universe.verify_content_identity()
        if artifact.knowledge_time > cutoff or universe.cutoff > cutoff:
            raise SwingInputAssemblyError("engine input contains future-known evidence")
        entry = _entry_for(universe, stable_instrument_id, stable_listing_id)
        if not entry.listing.is_valid_on(artifact.market_session):
            raise SwingInputAssemblyError("stable listing is not valid on a price session")
        bar = _bar_for(artifact, entry)
        binding = StableRawBarBinding(
            market_session=artifact.market_session,
            raw_bar_id=bar.bar_id,
            stable_instrument_id=stable_instrument_id,
            stable_listing_id=stable_listing_id,
            identity_snapshot_id=stable_identity_snapshot_id,
            knowledge_time=max(universe.cutoff, identity_evidence.cutoff),
        )
        entries.append(entry)
        raw_bars.append(bar)
        bindings.append(binding)
    if entries[-1].disposition is not UniverseDisposition.ACTIONABLE:
        raise SwingInputAssemblyError("signal-session stable listing is not actionable")
    if (
        tick_size.instrument_id != stable_instrument_id
        or tick_size.listing_id != stable_listing_id
    ):
        raise SwingInputAssemblyError("tick size belongs to another stable listing")

    adjusted = build_adjusted_price_history(
        raw_bars=tuple(raw_bars),
        identity_bindings=tuple(bindings),
        stable_instrument_id=stable_instrument_id,
        stable_listing_id=stable_listing_id,
        signal_session=signal_session,
        cutoff=cutoff,
        corporate_actions=corporate_actions,
    )
    materialization = materialize_swing_history(
        adjusted=adjusted,
        tick_size=tick_size,
    )
    return SwingInputAssembly(
        promotion_decision_id=promotion.decision_id,
        promotion=promotion,
        stable_identity_snapshot_id=stable_identity_snapshot_id,
        stable_instrument_id=stable_instrument_id,
        stable_listing_id=stable_listing_id,
        signal_session=signal_session,
        cutoff=cutoff,
        raw_artifact_ids=raw_ids,
        universe_snapshot_ids=universe_ids,
        identity_bindings=tuple(bindings),
        adjusted_history=adjusted,
        signal_materialization=materialization,
        readiness=readiness,
        actionable=actionable,
    )


def assemble_alert_swing_inputs(
    *,
    history: tuple[NseEodSessionArtifact, ...],
    universes: tuple[UniverseSnapshot, ...],
    stable_identity_snapshot_id: str,
    stable_instrument_id: str,
    stable_listing_id: str,
    signal_session: date,
    cutoff: datetime,
    corporate_actions: CorporateActionSnapshot,
    tick_size: EffectiveTickSize,
    promotion: PromotionDecision,
) -> SwingInputAssembly:
    result = assemble_swing_inputs(
        history=history,
        universes=universes,
        stable_identity_snapshot_id=stable_identity_snapshot_id,
        stable_instrument_id=stable_instrument_id,
        stable_listing_id=stable_listing_id,
        signal_session=signal_session,
        cutoff=cutoff,
        corporate_actions=corporate_actions,
        tick_size=tick_size,
        promotion=promotion,
    )
    if not result.actionable:
        raise SwingInputAssemblyError(
            "synthetic research inputs cannot enter the alert engine"
        )
    return result
