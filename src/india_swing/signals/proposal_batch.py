from __future__ import annotations

import re
from dataclasses import dataclass, field, fields

from india_swing.identity import content_id
from india_swing.reference.calendar import CalendarSnapshot
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference.universe import UniverseDisposition, UniverseEntry

from .deterministic_swing import (
    DeterministicSwingSignalConfig,
    SwingNextEntryWindow,
    SwingTechnicalMetrics,
    SwingTradeLevels,
    calculate_next_entry_window,
    calculate_swing_technical_metrics,
    calculate_swing_trade_levels,
)
from .input_assembly import SwingInputAssembly
from .universe_batch import SwingUniverseInputBatch, SwingUniverseVeto


PROPOSAL_SCHEMA_VERSION = "swing-technical-proposal/v1"
PROPOSAL_BATCH_SCHEMA_VERSION = "swing-proposal-batch/v1"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class SwingProposalBatchError(ValueError):
    pass


def _sha(value: str, name: str) -> None:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise SwingProposalBatchError(f"{name} must be a lowercase SHA-256")


def _subject_key(instrument_id: str, listing_id: str) -> tuple[str, str]:
    return (instrument_id, listing_id)


@dataclass(frozen=True, slots=True)
class SwingTechnicalProposal:
    """One deterministic, explainable technical proposal for one actionable subject.

    A proposal is descriptive research output derived entirely from an
    already-verified swing-input assembly. It carries no probability,
    confidence, or execution authority of its own.
    """

    assembly: SwingInputAssembly
    universe_entry: UniverseEntry
    universe_snapshot_id: str
    calendar: CalendarSnapshot
    config: DeterministicSwingSignalConfig
    metrics: SwingTechnicalMetrics
    levels: SwingTradeLevels
    entry_window: SwingNextEntryWindow
    readiness: ReferenceReadiness
    evidence_ids: tuple[str, ...]
    schema_version: str = PROPOSAL_SCHEMA_VERSION
    proposal_id: str = field(init=False)

    def __post_init__(self) -> None:
        if self.schema_version != PROPOSAL_SCHEMA_VERSION:
            raise SwingProposalBatchError("unsupported swing technical proposal schema")
        self._verify()
        object.__setattr__(self, "proposal_id", self._calculated_id())

    def _verify(self) -> None:
        if type(self.assembly) is not SwingInputAssembly:
            raise SwingProposalBatchError("assembly must be exact")
        self.assembly.verify_content_identity()
        if type(self.universe_entry) is not UniverseEntry:
            raise SwingProposalBatchError("universe_entry must be exact")
        _sha(self.universe_snapshot_id, "universe_snapshot_id")
        if type(self.calendar) is not CalendarSnapshot:
            raise SwingProposalBatchError("calendar must be exact")
        self.calendar.verify_content_identity()
        if type(self.config) is not DeterministicSwingSignalConfig:
            raise SwingProposalBatchError("config must be exact")
        self.config.verify_content_identity()
        if type(self.readiness) is not ReferenceReadiness:
            raise SwingProposalBatchError("readiness must be exact")

        entry = self.universe_entry
        if (
            self.assembly.stable_instrument_id != entry.listing.instrument_id
            or self.assembly.stable_listing_id != entry.listing.listing_id
        ):
            raise SwingProposalBatchError(
                "assembly subject does not match the universe entry"
            )
        if entry.disposition is not UniverseDisposition.ACTIONABLE:
            raise SwingProposalBatchError("only actionable entries can receive a proposal")
        if not entry.listing.is_valid_on(self.assembly.signal_session):
            raise SwingProposalBatchError("universe entry is not valid on the signal session")
        if not self.assembly.universe_snapshot_ids or (
            self.assembly.universe_snapshot_ids[-1] != self.universe_snapshot_id
        ):
            raise SwingProposalBatchError(
                "assembly final universe snapshot does not match the proposal batch universe"
            )
        if self.readiness is not self.assembly.readiness:
            raise SwingProposalBatchError("readiness does not match the bound assembly")
        if (
            self.calendar.exchange != entry.listing.exchange
            or self.calendar.segment != entry.listing.segment
        ):
            raise SwingProposalBatchError(
                "calendar market differs from the bound universe entry"
            )
        if self.calendar.cutoff > self.assembly.cutoff:
            raise SwingProposalBatchError("calendar postdates the bound assembly cutoff")
        try:
            self.calendar.require_session(self.assembly.signal_session)
        except Exception:
            raise SwingProposalBatchError(
                "calendar does not contain the assembly signal session"
            ) from None

        history = self.assembly.signal_materialization.history

        if type(self.metrics) is not SwingTechnicalMetrics:
            raise SwingProposalBatchError("metrics must be exact")
        self.metrics.verify_content_identity()
        replayed_metrics = calculate_swing_technical_metrics(history, self.config)
        if replayed_metrics.metrics_id != self.metrics.metrics_id:
            raise SwingProposalBatchError("metrics do not replay from the bound assembly")

        if type(self.levels) is not SwingTradeLevels:
            raise SwingProposalBatchError("levels must be exact")
        self.levels.verify_content_identity()
        replayed_levels = calculate_swing_trade_levels(
            current_close=history.bars[-1].close,
            tick=history.tick_size,
            atr=self.metrics.atr,
            estimated_cost_bps=self.config.base_round_trip_cost_bps,
            config=self.config,
        )
        if replayed_levels.levels_id != self.levels.levels_id:
            raise SwingProposalBatchError("levels do not replay from the bound assembly")

        if type(self.entry_window) is not SwingNextEntryWindow:
            raise SwingProposalBatchError("entry_window must be exact")
        self.entry_window.verify_content_identity()
        replayed_window = calculate_next_entry_window(
            self.calendar, self.assembly.signal_session, self.config
        )
        if replayed_window.window_id != self.entry_window.window_id:
            raise SwingProposalBatchError("entry window does not replay from bound inputs")

        expected_evidence_ids = self.metrics.evidence_ids + (
            history.tick_evidence_id,
            history.adjustment_evidence_id,
        )
        if (
            type(self.evidence_ids) is not tuple
            or self.evidence_ids != expected_evidence_ids
        ):
            raise SwingProposalBatchError("evidence IDs do not replay from bound inputs")

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "proposal_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._verify()
        if self.proposal_id != self._calculated_id():
            raise SwingProposalBatchError("technical proposal content identity failed")

    @property
    def symbol(self) -> str:
        return self.universe_entry.listing.tradingsymbol

    @property
    def input_actionable(self) -> bool:
        return self.assembly.actionable

    @property
    def research_only(self) -> bool:
        return not self.assembly.actionable

    @property
    def execution_eligible(self) -> bool:
        return False


@dataclass(frozen=True, slots=True)
class SwingProposalBatch:
    """Exact, content-addressed coverage: one proposal per actionable subject."""

    universe_batch: SwingUniverseInputBatch
    calendar: CalendarSnapshot
    config: DeterministicSwingSignalConfig
    proposals: tuple[SwingTechnicalProposal, ...]
    vetoes: tuple[SwingUniverseVeto, ...]
    scoped_subject_count: int
    proposal_subject_count: int
    veto_subject_count: int
    schema_version: str = PROPOSAL_BATCH_SCHEMA_VERSION
    batch_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.universe_batch) is not SwingUniverseInputBatch:
            raise SwingProposalBatchError("universe_batch must be exact")
        if type(self.calendar) is not CalendarSnapshot:
            raise SwingProposalBatchError("calendar must be exact")
        if type(self.config) is not DeterministicSwingSignalConfig:
            raise SwingProposalBatchError("config must be exact")
        if type(self.proposals) is not tuple or any(
            type(value) is not SwingTechnicalProposal for value in self.proposals
        ):
            raise SwingProposalBatchError("proposals must be an exact tuple")
        if type(self.vetoes) is not tuple or any(
            type(value) is not SwingUniverseVeto for value in self.vetoes
        ):
            raise SwingProposalBatchError("vetoes must be an exact tuple")
        for value in (
            self.scoped_subject_count,
            self.proposal_subject_count,
            self.veto_subject_count,
        ):
            if type(value) is not int or value < 0:
                raise SwingProposalBatchError("batch subject counts must be non-negative")
        if self.schema_version != PROPOSAL_BATCH_SCHEMA_VERSION:
            raise SwingProposalBatchError("unsupported swing proposal batch schema")
        self._verify_coverage()
        object.__setattr__(self, "batch_id", self._calculated_id())

    def _verify_coverage(self) -> None:
        universe_batch = self.universe_batch
        universe_batch.verify_content_identity()
        self.calendar.verify_content_identity()
        self.config.verify_content_identity()

        universe = universe_batch.current_universe
        if self.calendar.snapshot_id != universe.calendar_snapshot_id:
            raise SwingProposalBatchError(
                "calendar snapshot differs from the current universe lineage"
            )
        if (
            self.calendar.exchange != universe.exchange
            or self.calendar.segment != universe.segment
        ):
            raise SwingProposalBatchError(
                "calendar market differs from the current universe"
            )
        if self.calendar.cutoff > universe_batch.cutoff:
            raise SwingProposalBatchError(
                "calendar postdates the universe batch cutoff"
            )
        if self.calendar.readiness is not universe_batch.readiness:
            raise SwingProposalBatchError(
                "calendar readiness differs from the universe batch"
            )
        try:
            self.calendar.require_session(universe_batch.signal_session)
        except Exception:
            raise SwingProposalBatchError(
                "calendar does not contain the universe signal session"
            ) from None

        for value in self.proposals:
            value.verify_content_identity()
        proposal_keys = tuple(
            _subject_key(value.assembly.stable_instrument_id, value.assembly.stable_listing_id)
            for value in self.proposals
        )
        assembly_keys = tuple(
            _subject_key(value.stable_instrument_id, value.stable_listing_id)
            for value in universe_batch.assemblies
        )
        if len(set(proposal_keys)) != len(proposal_keys):
            raise SwingProposalBatchError("proposals contain a duplicate subject")
        if proposal_keys != tuple(sorted(proposal_keys)):
            raise SwingProposalBatchError("proposals must be canonically ordered by subject")
        if proposal_keys != assembly_keys:
            raise SwingProposalBatchError(
                "proposals do not exactly cover the universe batch assemblies"
            )

        entry_by_key = {
            _subject_key(entry.listing.instrument_id, entry.listing.listing_id): entry
            for entry in universe_batch.current_universe.entries
        }
        assembly_ids = tuple(value.assembly_id for value in universe_batch.assemblies)
        for value, expected_assembly_id in zip(self.proposals, assembly_ids):
            if value.assembly.assembly_id != expected_assembly_id:
                raise SwingProposalBatchError(
                    "proposal is not bound to the universe batch's own assembly"
                )
            key = _subject_key(
                value.assembly.stable_instrument_id, value.assembly.stable_listing_id
            )
            if value.universe_entry != entry_by_key.get(key):
                raise SwingProposalBatchError(
                    "proposal universe entry differs from the current universe"
                )
            if value.universe_snapshot_id != universe_batch.universe_snapshot_id:
                raise SwingProposalBatchError(
                    "proposal universe snapshot differs from the universe batch"
                )
            if value.calendar.snapshot_id != self.calendar.snapshot_id:
                raise SwingProposalBatchError(
                    "proposal calendar differs from the proposal batch calendar"
                )
            if value.config.config_id != self.config.config_id:
                raise SwingProposalBatchError(
                    "proposal config differs from the proposal batch config"
                )

        if self.vetoes != universe_batch.vetoes:
            raise SwingProposalBatchError(
                "vetoes must exactly and unchangedly match the universe batch"
            )
        for value in self.vetoes:
            value.verify_content_identity()

        if (
            self.scoped_subject_count != universe_batch.scoped_subject_count
            or self.proposal_subject_count != len(self.proposals)
            or self.veto_subject_count != len(self.vetoes)
            or self.scoped_subject_count
            != self.proposal_subject_count + self.veto_subject_count
        ):
            raise SwingProposalBatchError("batch subject counts are inconsistent")

    @property
    def readiness(self) -> ReferenceReadiness:
        return self.universe_batch.readiness

    @property
    def research_only(self) -> bool:
        return not self.universe_batch.actionable

    @property
    def execution_eligible(self) -> bool:
        return False

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "batch_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._verify_coverage()
        if self.batch_id != self._calculated_id():
            raise SwingProposalBatchError("proposal batch content identity failed")


def assemble_swing_proposal_batch(
    *,
    universe_batch: SwingUniverseInputBatch,
    calendar: CalendarSnapshot,
    config: DeterministicSwingSignalConfig | None = None,
) -> SwingProposalBatch:
    """Bind one exact SwingUniverseInputBatch to full deterministic proposal coverage.

    Every assembled actionable subject receives exactly one technical proposal
    built at the configured base round-trip cost; every existing universe veto
    is carried through unchanged. This function does not create assemblies or
    vetoes -- it only derives and packages proposals from what already exists.
    """

    if type(universe_batch) is not SwingUniverseInputBatch:
        raise SwingProposalBatchError("universe_batch must be exact")
    universe_batch.verify_content_identity()
    if type(calendar) is not CalendarSnapshot:
        raise SwingProposalBatchError("calendar must be exact")
    calendar.verify_content_identity()
    if config is None:
        config = DeterministicSwingSignalConfig()
    if type(config) is not DeterministicSwingSignalConfig:
        raise SwingProposalBatchError("config must be exact")
    config.verify_content_identity()

    entry_by_key = {
        _subject_key(entry.listing.instrument_id, entry.listing.listing_id): entry
        for entry in universe_batch.current_universe.entries
    }

    proposals: list[SwingTechnicalProposal] = []
    for assembly in universe_batch.assemblies:
        key = _subject_key(assembly.stable_instrument_id, assembly.stable_listing_id)
        entry = entry_by_key[key]
        history = assembly.signal_materialization.history
        metrics = calculate_swing_technical_metrics(history, config)
        levels = calculate_swing_trade_levels(
            current_close=history.bars[-1].close,
            tick=history.tick_size,
            atr=metrics.atr,
            estimated_cost_bps=config.base_round_trip_cost_bps,
            config=config,
        )
        entry_window = calculate_next_entry_window(calendar, assembly.signal_session, config)
        evidence_ids = metrics.evidence_ids + (
            history.tick_evidence_id,
            history.adjustment_evidence_id,
        )
        proposals.append(
            SwingTechnicalProposal(
                assembly=assembly,
                universe_entry=entry,
                universe_snapshot_id=universe_batch.universe_snapshot_id,
                calendar=calendar,
                config=config,
                metrics=metrics,
                levels=levels,
                entry_window=entry_window,
                readiness=assembly.readiness,
                evidence_ids=evidence_ids,
            )
        )

    return SwingProposalBatch(
        universe_batch=universe_batch,
        calendar=calendar,
        config=config,
        proposals=tuple(proposals),
        vetoes=universe_batch.vetoes,
        scoped_subject_count=universe_batch.scoped_subject_count,
        proposal_subject_count=len(proposals),
        veto_subject_count=len(universe_batch.vetoes),
    )
