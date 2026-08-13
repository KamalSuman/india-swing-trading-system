"""Operational assembly of the pinned forward-paper research input graph.

This module is deliberately pure.  It grants no signal, paper-trade, alert,
or execution authority; it only joins already verified, explicitly supplied
artifacts into the descriptive feature graph used by later research stages.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Mapping

from india_swing.corporate_actions.models import CorporateActionSnapshot
from india_swing.evaluation.dataset_assembly import EffectiveTickSize
from india_swing.evaluation.nse_archive_research_identity import (
    research_identity_id_for_isin,
)
from india_swing.forward_paper.adjustments import (
    ForwardPaperAdjustedHistoryWindow,
    ForwardPaperCorporateActionIdentityBinding,
    _build_forward_paper_adjusted_history_window_from_verified_inputs,
    build_forward_paper_adjusted_history_window,
)
from india_swing.forward_paper.feature_inputs import (
    ForwardPaperFeatureInputWindow,
    _build_forward_paper_feature_input_window_from_verified_inputs,
    build_forward_paper_feature_input_window,
)
from india_swing.forward_paper.features import (
    ForwardPaperTechnicalFeatureWindow,
    _build_forward_paper_technical_feature_window_from_verified_inputs,
    build_forward_paper_technical_feature_window,
)
from india_swing.forward_paper.history import (
    ForwardPaperHistoryCandidate,
    ForwardPaperRawHistoryWindow,
)
from india_swing.forward_paper.signal_tick import (
    ForwardPaperSignalTickPanel,
    ForwardPaperTickPanel,
    is_forward_paper_tick_panel,
)
from india_swing.identity import content_id
from india_swing.tick_sizes.effective_session import (
    PromotedEffectiveSessionTickStatus,
)


FORWARD_PAPER_OPERATIONAL_GRAPH_SCHEMA_VERSION = "forward-paper-operational-graph-v2"
FORWARD_PAPER_OPERATIONAL_GRAPH_POLICY_VERSION = (
    "exact-pinned-signal-session-tick-evidence-collection-only-v2"
)


class ForwardPaperOperationalGraphError(ValueError):
    """Raised when a pinned operational evidence graph cannot be trusted."""


def _fail(message: str) -> None:
    raise ForwardPaperOperationalGraphError(message)


def _verify(value: object, message: str) -> None:
    failed = False
    try:
        value.verify_content_identity()  # type: ignore[attr-defined]
    except Exception:
        failed = True
    if failed:
        _fail(message)


OperationalGraphStageObserver = Callable[[str, str, Mapping[str, int]], None]


def _observe(
    observer: OperationalGraphStageObserver | None,
    stage: str,
    status: str,
    **details: int,
) -> None:
    if observer is not None:
        observer(stage, status, details)


def _source_isin(result: object) -> str:
    failed = False
    value = None
    try:
        tick_entry = result.source_observation.tick_entry
        if tick_entry is None:
            failed = True
        else:
            value = tick_entry.frame_entry.universe_entry.validated_isin
            if type(value) is not str:
                failed = True
    except Exception:
        failed = True
    if failed:
        _fail("forward paper operational tick identity evidence is invalid")
    assert isinstance(value, str)
    return value


def _derive_inputs(
    *,
    source_window: ForwardPaperRawHistoryWindow,
    tick_panel: ForwardPaperTickPanel,
) -> tuple[
    tuple[ForwardPaperCorporateActionIdentityBinding, ...],
    tuple[EffectiveTickSize, ...],
    int,
]:
    candidates = tuple(
        value
        for value in source_window.outcomes
        if type(value) is ForwardPaperHistoryCandidate
    )
    candidate_ids = {value.research_identity_id for value in candidates}
    expected_sessions = set(source_window.spec.expected_market_sessions)
    signal_session = source_window.spec.expected_market_sessions[-1]
    mappings: dict[str, set[tuple[str, str]]] = {}
    selected: dict[str, EffectiveTickSize] = {}
    unmatched = 0

    if type(tick_panel) is ForwardPaperSignalTickPanel:
        if tick_panel.signal_session != signal_session:
            _fail("forward paper operational tick session is invalid")
        for entry in tick_panel.entries:
            failed = False
            research_identity_id = None
            try:
                research_identity_id = research_identity_id_for_isin(
                    entry.validated_isin
                )
            except Exception:
                failed = True
            if failed or research_identity_id is None:
                _fail("forward paper operational tick identity evidence is invalid")
            if research_identity_id not in candidate_ids:
                unmatched += 1
                continue
            mapping = (entry.stable_instrument_id, entry.stable_listing_id)
            mappings.setdefault(research_identity_id, set()).add(mapping)
            specification = entry.tick_specification
            if specification.specification_id in selected:
                _fail("forward paper operational tick evidence is duplicated")
            selected[specification.specification_id] = specification
    else:
        for result in tick_panel.results:
            if (
                result.status
                is not PromotedEffectiveSessionTickStatus.VERIFIED_EXACT_SESSION_ONLY
            ):
                unmatched += 1
                continue
            specification = result.tick_specification
            if specification is None:
                _fail("forward paper operational verified tick is incomplete")
            failed = False
            research_identity_id = None
            try:
                research_identity_id = research_identity_id_for_isin(
                    _source_isin(result)
                )
            except ForwardPaperOperationalGraphError:
                raise
            except Exception:
                failed = True
            if failed or research_identity_id is None:
                _fail("forward paper operational tick identity evidence is invalid")
            if research_identity_id not in candidate_ids:
                unmatched += 1
                continue
            mapping = (result.stable_instrument_id, result.stable_listing_id)
            mappings.setdefault(research_identity_id, set()).add(mapping)
            if result.market_session not in expected_sessions:
                unmatched += 1
                continue
            if result.market_session != signal_session:
                unmatched += 1
                continue
            if specification.specification_id in selected:
                _fail("forward paper operational tick evidence is duplicated")
            selected[specification.specification_id] = specification

    bindings: list[ForwardPaperCorporateActionIdentityBinding] = []
    for candidate in candidates:
        identity_mappings = mappings.get(candidate.research_identity_id, set())
        if not identity_mappings:
            continue
        if len(identity_mappings) != 1:
            _fail("forward paper operational identity mapping is ambiguous")
        stable_instrument_id, stable_listing_id = next(iter(identity_mappings))
        bindings.append(
            ForwardPaperCorporateActionIdentityBinding(
                research_identity_id=candidate.research_identity_id,
                stable_instrument_id=stable_instrument_id,
                stable_listing_id=stable_listing_id,
                knowledge_time=tick_panel.knowledge_time,
                source_artifact_id=tick_panel.panel_id,
            )
        )

    return (
        tuple(sorted(bindings, key=lambda value: value.research_identity_id)),
        tuple(
            sorted(
                selected.values(),
                key=lambda value: (
                    value.instrument_id,
                    value.listing_id,
                    value.effective_from_session,
                    value.specification_id,
                ),
            )
        ),
        unmatched,
    )


@dataclass(frozen=True, slots=True)
class ForwardPaperOperationalResearchGraph:
    """One immutable exact-artifact graph for a forward-paper research run."""

    source_window: ForwardPaperRawHistoryWindow
    corporate_actions: CorporateActionSnapshot
    tick_panel: ForwardPaperTickPanel
    identity_bindings: tuple[ForwardPaperCorporateActionIdentityBinding, ...]
    tick_specifications: tuple[EffectiveTickSize, ...]
    adjusted_window: ForwardPaperAdjustedHistoryWindow
    feature_input_window: ForwardPaperFeatureInputWindow
    technical_feature_window: ForwardPaperTechnicalFeatureWindow
    unmatched_tick_result_count: int
    schema_version: str = FORWARD_PAPER_OPERATIONAL_GRAPH_SCHEMA_VERSION
    policy_version: str = FORWARD_PAPER_OPERATIONAL_GRAPH_POLICY_VERSION
    graph_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "graph_id", self._calculated_id())

    def _validate(self) -> None:
        if type(self.source_window) is not ForwardPaperRawHistoryWindow:
            _fail("forward paper operational source window is invalid")
        if type(self.corporate_actions) is not CorporateActionSnapshot:
            _fail("forward paper operational corporate actions are invalid")
        if not is_forward_paper_tick_panel(self.tick_panel):
            _fail("forward paper operational tick panel is invalid")
        for value, message in (
            (self.source_window, "forward paper operational source failed verification"),
            (
                self.corporate_actions,
                "forward paper operational corporate actions failed verification",
            ),
            (self.tick_panel, "forward paper operational tick panel failed verification"),
            (self.adjusted_window, "forward paper operational adjustment failed verification"),
            (
                self.feature_input_window,
                "forward paper operational feature input failed verification",
            ),
            (
                self.technical_feature_window,
                "forward paper operational feature result failed verification",
            ),
        ):
            _verify(value, message)
        if self.schema_version != FORWARD_PAPER_OPERATIONAL_GRAPH_SCHEMA_VERSION:
            _fail("forward paper operational schema is invalid")
        if self.policy_version != FORWARD_PAPER_OPERATIONAL_GRAPH_POLICY_VERSION:
            _fail("forward paper operational policy is invalid")
        if type(self.unmatched_tick_result_count) is not int or self.unmatched_tick_result_count < 0:
            _fail("forward paper operational tick count is invalid")
        if self.tick_panel.cutoff > self.source_window.spec.decision_cutoff:
            _fail("forward paper operational tick evidence is future-known")
        if self.tick_panel.knowledge_time > self.source_window.spec.decision_cutoff:
            _fail("forward paper operational tick evidence is future-known")
        expected_bindings, expected_ticks, unmatched = _derive_inputs(
            source_window=self.source_window,
            tick_panel=self.tick_panel,
        )
        if self.identity_bindings != expected_bindings:
            _fail("forward paper operational identity lineage is invalid")
        if self.tick_specifications != expected_ticks:
            _fail("forward paper operational tick lineage is invalid")
        if self.unmatched_tick_result_count != unmatched:
            _fail("forward paper operational tick count is invalid")
        if self.adjusted_window.source_window is not self.source_window:
            _fail("forward paper operational adjustment lineage is invalid")
        if self.adjusted_window.corporate_actions is not self.corporate_actions:
            _fail("forward paper operational adjustment lineage is invalid")
        if self.adjusted_window.identity_bindings != self.identity_bindings:
            _fail("forward paper operational adjustment lineage is invalid")
        if self.feature_input_window.source_window is not self.adjusted_window:
            _fail("forward paper operational feature input lineage is invalid")
        if self.feature_input_window.tick_specifications != self.tick_specifications:
            _fail("forward paper operational feature input lineage is invalid")
        if self.technical_feature_window.source_window is not self.feature_input_window:
            _fail("forward paper operational feature lineage is invalid")

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": self.schema_version,
                "policy_version": self.policy_version,
                "source_window_id": self.source_window.window_id,
                "corporate_action_snapshot_id": self.corporate_actions.snapshot_id,
                "tick_panel_id": self.tick_panel.panel_id,
                "binding_ids": tuple(value.binding_id for value in self.identity_bindings),
                "tick_specification_ids": tuple(
                    value.specification_id for value in self.tick_specifications
                ),
                "adjusted_window_id": self.adjusted_window.window_id,
                "feature_input_window_id": self.feature_input_window.window_id,
                "technical_feature_window_id": self.technical_feature_window.window_id,
                "unmatched_tick_result_count": self.unmatched_tick_result_count,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.graph_id != self._calculated_id():
            _fail("forward paper operational graph identity failed")

    @classmethod
    def _from_freshly_verified_derivation(
        cls,
        *,
        source_window: ForwardPaperRawHistoryWindow,
        corporate_actions: CorporateActionSnapshot,
        tick_panel: ForwardPaperTickPanel,
        identity_bindings: tuple[ForwardPaperCorporateActionIdentityBinding, ...],
        tick_specifications: tuple[EffectiveTickSize, ...],
        adjusted_window: ForwardPaperAdjustedHistoryWindow,
        feature_input_window: ForwardPaperFeatureInputWindow,
        technical_feature_window: ForwardPaperTechnicalFeatureWindow,
        unmatched_tick_result_count: int,
    ) -> "ForwardPaperOperationalResearchGraph":
        value = object.__new__(cls)
        for name, item in (
            ("source_window", source_window),
            ("corporate_actions", corporate_actions),
            ("tick_panel", tick_panel),
            ("identity_bindings", identity_bindings),
            ("tick_specifications", tick_specifications),
            ("adjusted_window", adjusted_window),
            ("feature_input_window", feature_input_window),
            ("technical_feature_window", technical_feature_window),
            ("unmatched_tick_result_count", unmatched_tick_result_count),
            ("schema_version", FORWARD_PAPER_OPERATIONAL_GRAPH_SCHEMA_VERSION),
            ("policy_version", FORWARD_PAPER_OPERATIONAL_GRAPH_POLICY_VERSION),
        ):
            object.__setattr__(value, name, item)
        object.__setattr__(value, "graph_id", value._calculated_id())
        return value

    @property
    def collection_only(self) -> bool:
        return True

    @property
    def resolved_histories_feature_complete(self) -> bool:
        return self.technical_feature_window.resolved_histories_feature_complete

    @property
    def training_eligible(self) -> bool:
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


def _assemble_forward_paper_operational_research_graph(
    *,
    source_window: ForwardPaperRawHistoryWindow,
    corporate_actions: CorporateActionSnapshot,
    tick_panel: ForwardPaperTickPanel,
    verify_inputs: bool,
    stage_observer: OperationalGraphStageObserver | None,
) -> ForwardPaperOperationalResearchGraph:
    """Join three exact pinned artifacts and compute the descriptive graph."""

    if type(source_window) is not ForwardPaperRawHistoryWindow:
        _fail("forward paper operational source window is invalid")
    if type(corporate_actions) is not CorporateActionSnapshot:
        _fail("forward paper operational corporate actions are invalid")
    if not is_forward_paper_tick_panel(tick_panel):
        _fail("forward paper operational tick panel is invalid")
    if verify_inputs:
        _verify(source_window, "forward paper operational source failed verification")
        _verify(
            corporate_actions,
            "forward paper operational corporate actions failed verification",
        )
        _verify(tick_panel, "forward paper operational tick panel failed verification")
    elif (
        source_window.window_id != source_window._calculated_id()
        or source_window.spec.spec_id != source_window.spec._calculated_id()
    ):
        _fail("forward paper operational source failed verification")
    if (
        tick_panel.cutoff > source_window.spec.decision_cutoff
        or tick_panel.knowledge_time > source_window.spec.decision_cutoff
    ):
        _fail("forward paper operational tick evidence is future-known")

    bindings, ticks, unmatched = _derive_inputs(
        source_window=source_window,
        tick_panel=tick_panel,
    )
    adjusted = inputs = features = None
    _observe(stage_observer, "adjustment_derivation", "started")
    try:
        adjustment_builder = (
            build_forward_paper_adjusted_history_window
            if verify_inputs
            else _build_forward_paper_adjusted_history_window_from_verified_inputs
        )
        adjusted = adjustment_builder(
            source_window=source_window,
            corporate_actions=corporate_actions,
            identity_bindings=bindings,
        )
    except Exception:
        _observe(stage_observer, "adjustment_derivation", "failed")
        _fail("forward paper operational downstream assembly failed")
    if adjusted is None:
        _fail("forward paper operational downstream assembly failed")
    _observe(
        stage_observer,
        "adjustment_derivation",
        "completed",
        adjusted_candidate_count=adjusted.adjusted_candidate_count,
        adjustment_veto_count=adjusted.adjustment_veto_count,
        source_veto_count=adjusted.source_veto_count,
    )

    _observe(stage_observer, "feature_input_derivation", "started")
    try:
        input_builder = (
            build_forward_paper_feature_input_window
            if verify_inputs
            else _build_forward_paper_feature_input_window_from_verified_inputs
        )
        inputs = input_builder(
            source_window=adjusted,
            tick_specifications=ticks,
        )
    except Exception:
        _observe(stage_observer, "feature_input_derivation", "failed")
        _fail("forward paper operational downstream assembly failed")
    if inputs is None:
        _fail("forward paper operational downstream assembly failed")
    _observe(
        stage_observer,
        "feature_input_derivation",
        "completed",
        assembled_candidate_count=inputs.assembled_candidate_count,
        veto_count=inputs.veto_count,
    )

    _observe(stage_observer, "technical_feature_derivation", "started")
    try:
        feature_builder = (
            build_forward_paper_technical_feature_window
            if verify_inputs
            else _build_forward_paper_technical_feature_window_from_verified_inputs
        )
        features = feature_builder(source_window=inputs)
    except Exception:
        _observe(stage_observer, "technical_feature_derivation", "failed")
        _fail("forward paper operational downstream assembly failed")
    if features is None:
        _fail("forward paper operational downstream assembly failed")
    _observe(
        stage_observer,
        "technical_feature_derivation",
        "completed",
        computed_feature_count=features.computed_feature_count,
        blocked_feature_count=features.blocked_feature_count,
    )

    constructor = (
        ForwardPaperOperationalResearchGraph
        if verify_inputs
        else ForwardPaperOperationalResearchGraph._from_freshly_verified_derivation
    )
    return constructor(
        source_window=source_window,
        corporate_actions=corporate_actions,
        tick_panel=tick_panel,
        identity_bindings=bindings,
        tick_specifications=ticks,
        adjusted_window=adjusted,
        feature_input_window=inputs,
        technical_feature_window=features,
        unmatched_tick_result_count=unmatched,
    )


def assemble_forward_paper_operational_research_graph(
    *,
    source_window: ForwardPaperRawHistoryWindow,
    corporate_actions: CorporateActionSnapshot,
    tick_panel: ForwardPaperTickPanel,
) -> ForwardPaperOperationalResearchGraph:
    """Join three exact pinned artifacts after independent input verification."""

    return _assemble_forward_paper_operational_research_graph(
        source_window=source_window,
        corporate_actions=corporate_actions,
        tick_panel=tick_panel,
        verify_inputs=True,
        stage_observer=None,
    )


def _assemble_forward_paper_operational_research_graph_from_verified_inputs(
    *,
    source_window: ForwardPaperRawHistoryWindow,
    corporate_actions: CorporateActionSnapshot,
    tick_panel: ForwardPaperTickPanel,
    stage_observer: OperationalGraphStageObserver | None,
) -> ForwardPaperOperationalResearchGraph:
    return _assemble_forward_paper_operational_research_graph(
        source_window=source_window,
        corporate_actions=corporate_actions,
        tick_panel=tick_panel,
        verify_inputs=False,
        stage_observer=stage_observer,
    )
