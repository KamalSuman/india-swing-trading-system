"""Restart-safe, single-signal-session, paper-only promoted-engine runner.

This module consumes only already-published promoted adjustment and
effective-session-tick evidence: it never discovers, imports, adjudicates, or
publishes the upstream promoted graph, and it never sends a Telegram alert or
places a broker order. Every request pins exact source IDs; the runner fails
closed on a missing, extra, substituted, future-known, or lineage-
inconsistent source. A successfully published run manifest is research/paper
evidence only -- technical scores and research intents remain descriptive,
never a calibrated probability or a trade authorization, and sizing/cost/
stop/target/exposure gates remain exactly whatever
``PromotedResearchIntentService``/``PromotedIntentPolicyConfig`` already
enforce.
"""

from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Protocol

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.calendar_data.materialization_store import (
    LocalCalendarMaterializationStore,
)
from india_swing.corporate_actions.promoted_adjustments import (
    VerifiedPromotedCorporateActionAdjustmentPanel,
)
from india_swing.corporate_actions.snapshot_store import (
    LocalCorporateActionSnapshotStore,
)
from india_swing.evaluation.promoted_feature_inputs import (
    PromotedFeatureInputService,
    VerifiedPromotedFeatureInputPanel,
)
from india_swing.evaluation.promoted_intent_store import (
    LocalPromotedResearchIntentStore,
)
from india_swing.evaluation.promoted_intents import (
    PromotedIntentPolicyConfig,
    PromotedResearchIntentService,
    VerifiedPromotedResearchIntentBatch,
)
from india_swing.features.historical_replay import (
    PromotedHistoricalReplayInput,
    PromotedHistoricalReplayService,
    reconstruct_promoted_historical_replay_run,
)
from india_swing.features.promoted_cross_section import (
    PromotedCrossSectionConfig,
    VerifiedPromotedCrossSectionPanel,
)
from india_swing.features.promoted_technical import (
    PromotedTechnicalFeatureConfig,
)
from india_swing.features.store import (
    LocalPromotedCrossSectionStore,
    LocalPromotedFeatureInputStore,
    LocalPromotedTechnicalFeatureStore,
    PromotedAdjustmentPanelResolver,
    PromotedEffectiveTickPanelResolver,
)
from india_swing.historical_prices.promoted_history import (
    VerifiedPromotedStableListingHistoryPanel,
)
from india_swing.identity import content_id
from india_swing.identity_decisions.artifact_store import (
    LocalIdentityReviewBundleStore,
)
from india_swing.identity_evidence.artifact_store import (
    LocalIdentityEvidenceArtifactStore,
)
from india_swing.market_data.historical_corpus import (
    LocalHistoricalEvaluationCorpusStore,
)
from india_swing.promoted_graph_store import (
    LocalPromotedCorporateActionAdjustmentStore,
    LocalPromotedEffectiveSessionTickStore,
    LocalPromotedIdentityAdjudicationStore,
    LocalPromotedIdentityIntakeStore,
    LocalPromotedIdentitySessionUniverseStore,
    LocalPromotedSessionMarketDataFrameStore,
    LocalPromotedSessionTickSnapshotStore,
    LocalPromotedStableListingHistoryStore,
)
from india_swing.reference_data.artifact_store import LocalReferenceArtifactStore
from india_swing.reference_data.promotion_store import (
    LocalReferenceArtifactPromotionStore,
)
from india_swing.tick_sizes.effective_session import (
    VerifiedPromotedEffectiveSessionTickPanel,
)


class PromotedEngineError(ValueError):
    pass


class PromotedEngineConflict(PromotedEngineError):
    pass


class PromotedEngineNotFound(PromotedEngineError):
    pass


PROMOTED_ENGINE_REQUEST_SCHEMA_VERSION = "promoted-engine-run-request/v1"
PROMOTED_ENGINE_MANIFEST_SCHEMA_VERSION = "promoted-engine-run-manifest/v1"
PROMOTED_ENGINE_MANIFEST_CODEC_VERSION = "promoted-engine-run-manifest-json/v1"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAXIMUM_MANIFEST_BYTES = 1 * 1024 * 1024

_ERR_TYPE = "promoted engine type is invalid"
_ERR_REQUEST = "promoted engine request is invalid"
_ERR_ID = "promoted engine identifier is invalid"
_ERR_SOURCE = "promoted engine source could not be resolved"
_ERR_SIGNAL_SESSION = (
    "promoted engine signal session does not match the resolved source"
)
_ERR_LINEAGE = (
    "promoted engine sources do not share exact stable-history lineage"
)
_ERR_FUTURE = "promoted engine cutoff precedes resolved source knowledge"
_ERR_ACTION_SNAPSHOT = (
    "promoted engine corporate-action snapshot does not match expectation"
)
_ERR_PROMOTION_SET = (
    "promoted engine reference promotion set does not match expectation"
)
_ERR_GRAPH = "promoted engine manifest graph is invalid"
_ERR_REQUEST_MISMATCH = (
    "promoted engine manifest request preimage does not recompute to its"
    " stored request_id"
)
_ERR_VERIFY = "promoted engine manifest could not be verified"
_ERR_CONFLICT = "promoted engine run already stores different content"
_ERR_NOT_FOUND = "promoted engine run was not found"
_ERR_UNSAFE_PATH = "promoted engine run path is unsafe"
_ERR_BYTES = "promoted engine run manifest bytes are invalid"
_ERR_SHAPE = "promoted engine run manifest shape is invalid"


def _sha(value: object) -> bool:
    return type(value) is str and _SHA256.fullmatch(value) is not None


def _require_sha(value: object, message: str) -> str:
    if not _sha(value):
        raise PromotedEngineError(message)
    return value  # type: ignore[return-value]


def _utc(value: object, message: str) -> datetime:
    if type(value) is not datetime:
        raise PromotedEngineError(message)
    try:
        offset = value.utcoffset()
    except Exception:
        raise PromotedEngineError(message) from None
    if value.tzinfo is None or offset is None:
        raise PromotedEngineError(message)
    return value.astimezone(timezone.utc)


def _positive_decimal(value: object, message: str) -> Decimal:
    if type(value) is not str:
        raise PromotedEngineError(message)
    try:
        result = Decimal(value)
    except InvalidOperation:
        raise PromotedEngineError(message) from None
    if not result.is_finite() or result <= 0 or str(result) != value:
        raise PromotedEngineError(message)
    return result


def _canonical_date(value: object, message: str) -> date:
    if type(value) is not str:
        raise PromotedEngineError(message)
    try:
        result = date.fromisoformat(value)
    except ValueError:
        raise PromotedEngineError(message) from None
    if result.isoformat() != value:
        raise PromotedEngineError(message)
    return result


def _canonical_datetime(value: object, message: str) -> datetime:
    if type(value) is not str:
        raise PromotedEngineError(message)
    try:
        result = datetime.fromisoformat(value)
    except ValueError:
        raise PromotedEngineError(message) from None
    offset = result.utcoffset()
    if (
        result.tzinfo is None
        or offset is None
        or offset.total_seconds() != 0
        or result.isoformat() != value
    ):
        raise PromotedEngineError(message)
    return result


def _reachable_promotion_ids(
    history: VerifiedPromotedStableListingHistoryPanel,
) -> frozenset[str]:
    """Every reference-artifact promotion ID reachable through the identity graph.

    Walks each retained session tick snapshot down through its frame,
    universe, and adjudication to the identity intake that names the exact
    reference-artifact promotions used to build the trusted security master
    lineage. This is a pure read of already-resolved, already content-
    verified nested objects; it performs no additional store lookup.
    """

    result: set[str] = set()
    for snapshot in history.tick_snapshots:
        intake = snapshot.frame.universe.adjudication.intake
        for promotion in intake.promotions:
            result.add(promotion.promotion_id)
    return frozenset(result)


def _compute_request_id(
    *,
    adjustment_bridge_id: str,
    effective_tick_panel_id: str,
    expected_reference_promotion_ids: tuple[str, ...],
    expected_corporate_action_snapshot_id: str,
    signal_session: date,
    entry_session: date,
    cutoff: datetime,
    initial_capital: Decimal,
    technical_config_id: str,
    cross_section_config_id: str,
    intent_config_id: str,
) -> str:
    """The single request-identity derivation shared by the request and manifest.

    Both ``PromotedEngineRunRequest`` (at construction) and
    ``PromotedEngineRunManifest`` (on every decode/verify) call this exact
    function over the same named fields, so a durable manifest can prove its
    retained root-pin preimage recomputes to its own stored ``request_id``
    rather than persisting an opaque, unverifiable hash.
    """

    return content_id(
        {
            "schema_version": PROMOTED_ENGINE_REQUEST_SCHEMA_VERSION,
            "adjustment_bridge_id": adjustment_bridge_id,
            "effective_tick_panel_id": effective_tick_panel_id,
            "expected_reference_promotion_ids": expected_reference_promotion_ids,
            "expected_corporate_action_snapshot_id": (
                expected_corporate_action_snapshot_id
            ),
            "signal_session": signal_session,
            "entry_session": entry_session,
            "cutoff": cutoff,
            "initial_capital": initial_capital,
            "technical_config_id": technical_config_id,
            "cross_section_config_id": cross_section_config_id,
            "intent_config_id": intent_config_id,
        },
        length=64,
    )


@dataclass(frozen=True, slots=True)
class PromotedEngineRunRequest:
    """Immutable, content-addressed request for one signal-session engine run.

    Every nested knowledge time and cutoff must be at or before ``cutoff``,
    and ``entry_session`` must be after ``signal_session``: this is the only
    lookahead boundary the runner enforces directly, in addition to whatever
    the downstream services already enforce on their own inputs.
    """

    adjustment_bridge_id: str
    effective_tick_panel_id: str
    expected_reference_promotion_ids: tuple[str, ...]
    expected_corporate_action_snapshot_id: str
    signal_session: date
    entry_session: date
    cutoff: datetime
    initial_capital: Decimal
    technical_config: PromotedTechnicalFeatureConfig
    cross_section_config: PromotedCrossSectionConfig
    intent_config: PromotedIntentPolicyConfig
    request_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            not _sha(self.adjustment_bridge_id)
            or not _sha(self.effective_tick_panel_id)
            or not _sha(self.expected_corporate_action_snapshot_id)
            or type(self.expected_reference_promotion_ids) is not tuple
            or not self.expected_reference_promotion_ids
            or any(
                not _sha(value)
                for value in self.expected_reference_promotion_ids
            )
            or self.expected_reference_promotion_ids
            != tuple(sorted(set(self.expected_reference_promotion_ids)))
            or type(self.signal_session) is not date
            or type(self.entry_session) is not date
            or self.entry_session <= self.signal_session
            or type(self.technical_config) is not PromotedTechnicalFeatureConfig
            or type(self.cross_section_config) is not PromotedCrossSectionConfig
            or type(self.intent_config) is not PromotedIntentPolicyConfig
            or type(self.initial_capital) is not Decimal
            or not self.initial_capital.is_finite()
            or self.initial_capital <= 0
        ):
            raise PromotedEngineError(_ERR_REQUEST)
        cutoff = _utc(self.cutoff, _ERR_REQUEST)
        try:
            self.technical_config.verify_content_identity()
            self.cross_section_config.verify_content_identity()
            self.intent_config.verify_content_identity()
        except Exception:
            raise PromotedEngineError(_ERR_REQUEST) from None
        if cutoff != self.cutoff:
            object.__setattr__(self, "cutoff", cutoff)
        object.__setattr__(self, "request_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return _compute_request_id(
            adjustment_bridge_id=self.adjustment_bridge_id,
            effective_tick_panel_id=self.effective_tick_panel_id,
            expected_reference_promotion_ids=self.expected_reference_promotion_ids,
            expected_corporate_action_snapshot_id=(
                self.expected_corporate_action_snapshot_id
            ),
            signal_session=self.signal_session,
            entry_session=self.entry_session,
            cutoff=self.cutoff,
            initial_capital=self.initial_capital,
            technical_config_id=self.technical_config.config_id,
            cross_section_config_id=self.cross_section_config.config_id,
            intent_config_id=self.intent_config.config_id,
        )

    def verify_content_identity(self) -> None:
        if type(self) is not PromotedEngineRunRequest:
            raise PromotedEngineError(_ERR_TYPE)
        if self.request_id != self._calculated_id():
            raise PromotedEngineError(_ERR_ID)


@dataclass(frozen=True, slots=True)
class PromotedEngineRunManifest:
    """Canonical, content-derived, create-once manifest for one engine run.

    Binds the request ID (itself binding every root pin and configuration
    ID), every derived output ID, the sessions/cutoff/capital, and the
    permanently-true ``paper_only`` flag. It grants no trading, alert, or
    execution authority: publication only proves this exact paper research
    pass was durably recorded.
    """

    schema_version: str
    request_id: str
    adjustment_bridge_id: str
    effective_tick_panel_id: str
    expected_reference_promotion_ids: tuple[str, ...]
    expected_corporate_action_snapshot_id: str
    feature_input_panel_id: str
    technical_config_id: str
    technical_panel_id: str
    cross_section_config_id: str
    cross_section_panel_id: str
    intent_config_id: str
    research_intent_batch_id: str
    replay_run_id: str
    signal_session: date
    entry_session: date
    cutoff: datetime
    initial_capital: Decimal
    candidate_count: int
    intent_count: int
    paper_only: bool
    run_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            self.schema_version != PROMOTED_ENGINE_MANIFEST_SCHEMA_VERSION
            or not _sha(self.request_id)
            or not _sha(self.adjustment_bridge_id)
            or not _sha(self.effective_tick_panel_id)
            or type(self.expected_reference_promotion_ids) is not tuple
            or not self.expected_reference_promotion_ids
            or any(
                not _sha(value)
                for value in self.expected_reference_promotion_ids
            )
            or self.expected_reference_promotion_ids
            != tuple(sorted(set(self.expected_reference_promotion_ids)))
            or not _sha(self.expected_corporate_action_snapshot_id)
            or not _sha(self.feature_input_panel_id)
            or not _sha(self.technical_config_id)
            or not _sha(self.technical_panel_id)
            or not _sha(self.cross_section_config_id)
            or not _sha(self.cross_section_panel_id)
            or not _sha(self.intent_config_id)
            or not _sha(self.research_intent_batch_id)
            or not _sha(self.replay_run_id)
            or type(self.signal_session) is not date
            or type(self.entry_session) is not date
            or self.entry_session <= self.signal_session
            or type(self.candidate_count) is not int
            or self.candidate_count <= 0
            or type(self.intent_count) is not int
            or self.intent_count < 0
            or self.intent_count > self.candidate_count
            or self.paper_only is not True
            or type(self.initial_capital) is not Decimal
            or not self.initial_capital.is_finite()
            or self.initial_capital <= 0
        ):
            raise PromotedEngineError(_ERR_GRAPH)
        cutoff = _utc(self.cutoff, _ERR_GRAPH)
        if cutoff != self.cutoff:
            object.__setattr__(self, "cutoff", cutoff)
        expected_request_id = _compute_request_id(
            adjustment_bridge_id=self.adjustment_bridge_id,
            effective_tick_panel_id=self.effective_tick_panel_id,
            expected_reference_promotion_ids=self.expected_reference_promotion_ids,
            expected_corporate_action_snapshot_id=(
                self.expected_corporate_action_snapshot_id
            ),
            signal_session=self.signal_session,
            entry_session=self.entry_session,
            cutoff=self.cutoff,
            initial_capital=self.initial_capital,
            technical_config_id=self.technical_config_id,
            cross_section_config_id=self.cross_section_config_id,
            intent_config_id=self.intent_config_id,
        )
        if self.request_id != expected_request_id:
            raise PromotedEngineError(_ERR_REQUEST_MISMATCH)
        object.__setattr__(self, "run_id", self._calculated_id())

    def _identity(self) -> dict[str, object]:
        return {
            value.name: getattr(self, value.name)
            for value in fields(self)
            if value.name != "run_id"
        }

    def _calculated_id(self) -> str:
        return content_id(
            {"schema": PROMOTED_ENGINE_MANIFEST_SCHEMA_VERSION, **self._identity()},
            length=64,
        )

    def verify_content_identity(self) -> None:
        if type(self) is not PromotedEngineRunManifest:
            raise PromotedEngineError(_ERR_TYPE)
        expected = PromotedEngineRunManifest(**self._identity())
        if self.run_id != expected.run_id:
            raise PromotedEngineError(_ERR_ID)


class _CalendarResolverAdapter:
    """Adapts LocalCalendarMaterializationStore.get to the promoted-graph resolver shape."""

    def __init__(self, store: LocalCalendarMaterializationStore) -> None:
        self._store = store

    def get(self, materialization_id: str):
        return self._store.get(materialization_id).materialization


@dataclass(slots=True)
class PromotedEngineStores:
    """Local exact-store composition boundary for the promoted-engine runner.

    Exposes only the stores the runner and the run store consume directly.
    Every deeper identity/session/history root (intake, adjudication,
    session universe, session frame, tick snapshot, stable-listing history,
    calendar, historical corpus, reference-artifact promotion, identity
    evidence, identity review) is constructed and wired by
    ``build_promoted_engine_stores`` and reached only through
    ``corporate_action_adjustments``/``effective_session_ticks``'s own
    resolver chains -- there is no list/latest/nearest/find/discovery
    capability anywhere in that chain.
    """

    corporate_action_adjustments: PromotedAdjustmentPanelResolver
    effective_session_ticks: PromotedEffectiveTickPanelResolver
    feature_inputs: LocalPromotedFeatureInputStore
    technical_features: LocalPromotedTechnicalFeatureStore
    cross_sections: LocalPromotedCrossSectionStore
    research_intents: LocalPromotedResearchIntentStore
    engine_runs: "LocalPromotedEngineRunStore"


def build_promoted_engine_stores(
    *,
    reference_root: Path,
    identity_evidence_root: Path,
    calendar_root: Path,
    daily_reports_root: Path,
    historical_corpus_root: Path,
    promoted_root: Path,
    engine_run_root: Path,
) -> PromotedEngineStores:
    """Construct every real durable store from explicit roots.

    This is the only place the runner's stores are assembled. It never
    accepts a caller-supplied fake in place of a real store, and it exposes
    no discovery: every store here resolves only the exact ID it is asked
    for.
    """

    reference_artifacts = LocalReferenceArtifactStore(reference_root)
    reference_promotions = LocalReferenceArtifactPromotionStore(
        promoted_root, reference_artifacts
    )
    identity_evidence = LocalIdentityEvidenceArtifactStore(identity_evidence_root)
    identity_reviews = LocalIdentityReviewBundleStore(identity_evidence_root)
    identity_intakes = LocalPromotedIdentityIntakeStore(
        promoted_root, reference_promotions
    )
    identity_adjudications = LocalPromotedIdentityAdjudicationStore(
        promoted_root, identity_intakes, identity_evidence, identity_reviews
    )
    calendars = LocalCalendarMaterializationStore(calendar_root, daily_reports_root)
    calendar_resolver = _CalendarResolverAdapter(calendars)
    identity_session_universes = LocalPromotedIdentitySessionUniverseStore(
        promoted_root, identity_adjudications, calendar_resolver
    )
    historical_corpus = LocalHistoricalEvaluationCorpusStore(historical_corpus_root)
    session_market_data_frames = LocalPromotedSessionMarketDataFrameStore(
        promoted_root, identity_session_universes, historical_corpus
    )
    session_tick_snapshots = LocalPromotedSessionTickSnapshotStore(
        promoted_root, session_market_data_frames
    )
    stable_listing_histories = LocalPromotedStableListingHistoryStore(
        promoted_root, session_tick_snapshots, calendar_resolver
    )
    corporate_action_snapshots = LocalCorporateActionSnapshotStore(promoted_root)
    corporate_action_adjustments = LocalPromotedCorporateActionAdjustmentStore(
        promoted_root, stable_listing_histories, corporate_action_snapshots
    )
    effective_session_ticks = LocalPromotedEffectiveSessionTickStore(
        promoted_root, stable_listing_histories
    )
    feature_inputs = LocalPromotedFeatureInputStore(
        promoted_root, corporate_action_adjustments, effective_session_ticks
    )
    technical_features = LocalPromotedTechnicalFeatureStore(
        promoted_root, feature_inputs
    )
    cross_sections = LocalPromotedCrossSectionStore(promoted_root, technical_features)
    research_intents = LocalPromotedResearchIntentStore(promoted_root, cross_sections)
    engine_runs = LocalPromotedEngineRunStore(
        engine_run_root,
        cross_sections=cross_sections,
        research_intents=research_intents,
    )
    return PromotedEngineStores(
        corporate_action_adjustments=corporate_action_adjustments,
        effective_session_ticks=effective_session_ticks,
        feature_inputs=feature_inputs,
        technical_features=technical_features,
        cross_sections=cross_sections,
        research_intents=research_intents,
        engine_runs=engine_runs,
    )


class PromotedEngineRunner:
    """Executes exactly one signal-session promoted-engine pass.

    Consumes only already-published promoted adjustment/effective-tick
    evidence; it never discovers, imports, adjudicates, or publishes the
    upstream promoted graph, and it never sends an alert or places an order.
    """

    def run(
        self,
        request: PromotedEngineRunRequest,
        stores: PromotedEngineStores,
    ) -> PromotedEngineRunManifest:
        if type(request) is not PromotedEngineRunRequest:
            raise TypeError("promoted engine request must be exact")
        if type(stores) is not PromotedEngineStores:
            raise TypeError("promoted engine stores must be exact")
        request.verify_content_identity()

        try:
            adjustment_panel = stores.corporate_action_adjustments.get(
                request.adjustment_bridge_id
            )
        except Exception:
            raise PromotedEngineError(_ERR_SOURCE) from None
        try:
            tick_panel = stores.effective_session_ticks.get(
                request.effective_tick_panel_id
            )
        except Exception:
            raise PromotedEngineError(_ERR_SOURCE) from None

        if (
            type(adjustment_panel)
            is not VerifiedPromotedCorporateActionAdjustmentPanel
            or adjustment_panel.bridge_id != request.adjustment_bridge_id
            or type(tick_panel) is not VerifiedPromotedEffectiveSessionTickPanel
            or tick_panel.panel_id != request.effective_tick_panel_id
        ):
            raise PromotedEngineError(_ERR_SOURCE)
        if adjustment_panel.signal_session != request.signal_session:
            raise PromotedEngineError(_ERR_SIGNAL_SESSION)
        if (
            adjustment_panel.source_panel.panel_id
            != tick_panel.source_panel.panel_id
        ):
            raise PromotedEngineError(_ERR_LINEAGE)
        if request.cutoff < max(
            adjustment_panel.cutoff,
            adjustment_panel.knowledge_time,
            tick_panel.cutoff,
            tick_panel.knowledge_time,
        ):
            raise PromotedEngineError(_ERR_FUTURE)
        if (
            adjustment_panel.corporate_actions.snapshot_id
            != request.expected_corporate_action_snapshot_id
        ):
            raise PromotedEngineError(_ERR_ACTION_SNAPSHOT)
        if _reachable_promotion_ids(
            adjustment_panel.source_panel
        ) != frozenset(request.expected_reference_promotion_ids):
            raise PromotedEngineError(_ERR_PROMOTION_SET)

        try:
            feature_input_panel = PromotedFeatureInputService().materialize(
                adjustment_panel=adjustment_panel,
                tick_panel=tick_panel,
                cutoff=request.cutoff,
            )
            feature_input_panel = stores.feature_inputs.put(feature_input_panel)
        except PromotedEngineError:
            raise
        except Exception:
            raise PromotedEngineError(_ERR_SOURCE) from None

        try:
            replay_input = PromotedHistoricalReplayInput(
                market_session=request.signal_session,
                source_panel=feature_input_panel,
                technical_config=request.technical_config,
                cross_section_config=request.cross_section_config,
                cutoff=request.cutoff,
            )
            replay_run = PromotedHistoricalReplayService().run(
                inputs=(replay_input,),
                technical_store=stores.technical_features,
                cross_section_store=stores.cross_sections,
            )
        except Exception:
            raise PromotedEngineError(_ERR_SOURCE) from None

        result = replay_run.results[0]
        try:
            cross_section_panel = stores.cross_sections.get(
                result.cross_section_panel_id
            )
        except Exception:
            raise PromotedEngineError(_ERR_SOURCE) from None
        if (
            type(cross_section_panel) is not VerifiedPromotedCrossSectionPanel
            or cross_section_panel.panel_id != result.cross_section_panel_id
        ):
            raise PromotedEngineError(_ERR_SOURCE)

        try:
            batch = PromotedResearchIntentService().generate(
                source_panel=cross_section_panel,
                config=request.intent_config,
                entry_session=request.entry_session,
                initial_capital=request.initial_capital,
            )
            batch = stores.research_intents.put(batch, request.intent_config)
        except Exception:
            raise PromotedEngineError(_ERR_SOURCE) from None

        manifest = PromotedEngineRunManifest(
            schema_version=PROMOTED_ENGINE_MANIFEST_SCHEMA_VERSION,
            request_id=request.request_id,
            adjustment_bridge_id=request.adjustment_bridge_id,
            effective_tick_panel_id=request.effective_tick_panel_id,
            expected_reference_promotion_ids=(
                request.expected_reference_promotion_ids
            ),
            expected_corporate_action_snapshot_id=(
                request.expected_corporate_action_snapshot_id
            ),
            feature_input_panel_id=feature_input_panel.panel_id,
            technical_config_id=request.technical_config.config_id,
            technical_panel_id=result.technical_panel_id,
            cross_section_config_id=request.cross_section_config.config_id,
            cross_section_panel_id=cross_section_panel.panel_id,
            intent_config_id=request.intent_config.config_id,
            research_intent_batch_id=batch.batch_id,
            replay_run_id=replay_run.run_id,
            signal_session=request.signal_session,
            entry_session=request.entry_session,
            cutoff=request.cutoff,
            initial_capital=request.initial_capital,
            candidate_count=len(batch.decisions),
            intent_count=batch.selected_count,
            paper_only=True,
        )
        return stores.engine_runs.put(manifest)


_MANIFEST_KEYS = frozenset(
    {
        "codec_schema_version",
        "schema_version",
        "request_id",
        "adjustment_bridge_id",
        "effective_tick_panel_id",
        "expected_reference_promotion_ids",
        "expected_corporate_action_snapshot_id",
        "feature_input_panel_id",
        "technical_config_id",
        "technical_panel_id",
        "cross_section_config_id",
        "cross_section_panel_id",
        "intent_config_id",
        "research_intent_batch_id",
        "replay_run_id",
        "signal_session",
        "entry_session",
        "cutoff",
        "initial_capital",
        "candidate_count",
        "intent_count",
        "paper_only",
        "run_id",
    }
)


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise PromotedEngineError(_ERR_SHAPE)
        result[key] = value
    return result


def _reject_number(_token: str) -> object:
    raise PromotedEngineError(_ERR_SHAPE)


def encode_promoted_engine_run_manifest(
    manifest: PromotedEngineRunManifest,
) -> bytes:
    if type(manifest) is not PromotedEngineRunManifest:
        raise TypeError("promoted engine run manifest must be exact")
    manifest.verify_content_identity()
    return (
        json.dumps(
            {
                "codec_schema_version": PROMOTED_ENGINE_MANIFEST_CODEC_VERSION,
                "schema_version": manifest.schema_version,
                "request_id": manifest.request_id,
                "adjustment_bridge_id": manifest.adjustment_bridge_id,
                "effective_tick_panel_id": manifest.effective_tick_panel_id,
                "expected_reference_promotion_ids": list(
                    manifest.expected_reference_promotion_ids
                ),
                "expected_corporate_action_snapshot_id": (
                    manifest.expected_corporate_action_snapshot_id
                ),
                "feature_input_panel_id": manifest.feature_input_panel_id,
                "technical_config_id": manifest.technical_config_id,
                "technical_panel_id": manifest.technical_panel_id,
                "cross_section_config_id": manifest.cross_section_config_id,
                "cross_section_panel_id": manifest.cross_section_panel_id,
                "intent_config_id": manifest.intent_config_id,
                "research_intent_batch_id": manifest.research_intent_batch_id,
                "replay_run_id": manifest.replay_run_id,
                "signal_session": manifest.signal_session.isoformat(),
                "entry_session": manifest.entry_session.isoformat(),
                "cutoff": manifest.cutoff.isoformat(),
                "initial_capital": str(manifest.initial_capital),
                "candidate_count": manifest.candidate_count,
                "intent_count": manifest.intent_count,
                "paper_only": manifest.paper_only,
                "run_id": manifest.run_id,
            },
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def decode_promoted_engine_run_manifest(
    payload: bytes,
) -> PromotedEngineRunManifest:
    if (
        type(payload) is not bytes
        or not payload
        or len(payload) > _MAXIMUM_MANIFEST_BYTES
    ):
        raise PromotedEngineError(_ERR_BYTES)
    try:
        text = payload.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        raise PromotedEngineError(_ERR_BYTES) from None
    try:
        decoded = json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
    except PromotedEngineError:
        raise
    except (json.JSONDecodeError, RecursionError):
        raise PromotedEngineError(_ERR_SHAPE) from None
    if type(decoded) is not dict or set(decoded) != _MANIFEST_KEYS:
        raise PromotedEngineError(_ERR_SHAPE)
    if (
        decoded["codec_schema_version"] != PROMOTED_ENGINE_MANIFEST_CODEC_VERSION
        or decoded["schema_version"] != PROMOTED_ENGINE_MANIFEST_SCHEMA_VERSION
    ):
        raise PromotedEngineError(_ERR_SHAPE)
    id_fields = (
        "request_id",
        "adjustment_bridge_id",
        "effective_tick_panel_id",
        "expected_corporate_action_snapshot_id",
        "feature_input_panel_id",
        "technical_config_id",
        "technical_panel_id",
        "cross_section_config_id",
        "cross_section_panel_id",
        "intent_config_id",
        "research_intent_batch_id",
        "replay_run_id",
        "run_id",
    )
    ids = {name: _require_sha(decoded[name], _ERR_SHAPE) for name in id_fields}
    raw_promotion_ids = decoded["expected_reference_promotion_ids"]
    if type(raw_promotion_ids) is not list or not raw_promotion_ids:
        raise PromotedEngineError(_ERR_SHAPE)
    expected_reference_promotion_ids = tuple(
        _require_sha(value, _ERR_SHAPE) for value in raw_promotion_ids
    )
    if expected_reference_promotion_ids != tuple(
        sorted(set(expected_reference_promotion_ids))
    ):
        raise PromotedEngineError(_ERR_SHAPE)
    signal_session = _canonical_date(decoded["signal_session"], _ERR_SHAPE)
    entry_session = _canonical_date(decoded["entry_session"], _ERR_SHAPE)
    cutoff = _canonical_datetime(decoded["cutoff"], _ERR_SHAPE)
    initial_capital = _positive_decimal(decoded["initial_capital"], _ERR_SHAPE)
    candidate_count = decoded["candidate_count"]
    intent_count = decoded["intent_count"]
    paper_only = decoded["paper_only"]
    if (
        type(candidate_count) is not int
        or type(intent_count) is not int
        or type(paper_only) is not bool
    ):
        raise PromotedEngineError(_ERR_SHAPE)
    try:
        manifest = PromotedEngineRunManifest(
            schema_version=decoded["schema_version"],
            request_id=ids["request_id"],
            adjustment_bridge_id=ids["adjustment_bridge_id"],
            effective_tick_panel_id=ids["effective_tick_panel_id"],
            expected_reference_promotion_ids=expected_reference_promotion_ids,
            expected_corporate_action_snapshot_id=(
                ids["expected_corporate_action_snapshot_id"]
            ),
            feature_input_panel_id=ids["feature_input_panel_id"],
            technical_config_id=ids["technical_config_id"],
            technical_panel_id=ids["technical_panel_id"],
            cross_section_config_id=ids["cross_section_config_id"],
            cross_section_panel_id=ids["cross_section_panel_id"],
            intent_config_id=ids["intent_config_id"],
            research_intent_batch_id=ids["research_intent_batch_id"],
            replay_run_id=ids["replay_run_id"],
            signal_session=signal_session,
            entry_session=entry_session,
            cutoff=cutoff,
            initial_capital=initial_capital,
            candidate_count=candidate_count,
            intent_count=intent_count,
            paper_only=paper_only,
        )
    except PromotedEngineError:
        raise PromotedEngineError(_ERR_SHAPE) from None
    if manifest.run_id != ids["run_id"]:
        raise PromotedEngineError(_ERR_SHAPE)
    if encode_promoted_engine_run_manifest(manifest) != payload:
        raise PromotedEngineError(_ERR_SHAPE)
    return manifest


def _is_link_like(path: Path) -> bool:
    try:
        status = os.lstat(path)
    except OSError:
        return path.is_symlink()
    return path.is_symlink() or bool(
        getattr(status, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


class LocalPromotedEngineRunStore:
    """Durable, create-once root store for PromotedEngineRunManifest.

    Exposes only ``put``, ``get``, and ``path_for`` -- no list/latest/
    nearest/find/discovery operation. ``get`` never trusts the stored
    manifest as authority: it strictly decodes it, independently resolves the
    cross-section panel and research-intent batch through their own real
    durable stores (which themselves cascade all the way back through the
    entire promoted source graph, including every reference-artifact
    promotion), reconstructs the historical-replay run, and requires the
    recomputed run_id and canonical bytes to equal the pinned ones.
    """

    _DIRECTORY = "promoted-engine-runs"
    _LOCK_FILENAME = ".promoted-engine-run.lock"

    def __init__(
        self,
        root: Path,
        *,
        cross_sections,
        research_intents,
    ) -> None:
        self.root = Path(root) / self._DIRECTORY
        self.cross_sections = cross_sections
        self.research_intents = research_intents

    def path_for(self, run_id: str) -> Path:
        return self.root / f"{_require_sha(run_id, _ERR_ID)}.json"

    def put(
        self, manifest: PromotedEngineRunManifest
    ) -> PromotedEngineRunManifest:
        if type(manifest) is not PromotedEngineRunManifest:
            raise TypeError("promoted engine run manifest must be exact")
        manifest.verify_content_identity()
        self._verify_downstream(manifest)
        payload = encode_promoted_engine_run_manifest(manifest)
        try:
            replayed = decode_promoted_engine_run_manifest(payload)
        except PromotedEngineError:
            raise
        except Exception:
            raise PromotedEngineError(_ERR_VERIFY) from None
        if replayed != manifest or replayed.run_id != manifest.run_id:
            raise PromotedEngineError(_ERR_VERIFY)
        self._publish(manifest.run_id, payload)
        return self.get(manifest.run_id)

    def get(self, run_id: str) -> PromotedEngineRunManifest:
        path = self.path_for(run_id)
        payload = self._read(path)
        manifest = decode_promoted_engine_run_manifest(payload)
        if manifest.run_id != run_id:
            raise PromotedEngineError(_ERR_SHAPE)
        self._verify_downstream(manifest)
        return manifest

    def _verify_downstream(self, manifest: PromotedEngineRunManifest) -> None:
        try:
            cross_section_panel = self.cross_sections.get(
                manifest.cross_section_panel_id
            )
        except Exception:
            raise PromotedEngineConflict(_ERR_VERIFY) from None
        if (
            type(cross_section_panel) is not VerifiedPromotedCrossSectionPanel
            or cross_section_panel.panel_id != manifest.cross_section_panel_id
            or cross_section_panel.config.config_id
            != manifest.cross_section_config_id
            or cross_section_panel.source_panel.panel_id
            != manifest.technical_panel_id
            or cross_section_panel.source_panel.config.config_id
            != manifest.technical_config_id
            or cross_section_panel.source_panel.source_panel.panel_id
            != manifest.feature_input_panel_id
        ):
            raise PromotedEngineConflict(_ERR_VERIFY)
        # cross_section_panel.source_panel.source_panel is not merely a
        # cached reference: LocalPromotedCrossSectionStore.get and
        # LocalPromotedTechnicalFeatureStore.get each independently resolve
        # their own source through their own resolver before replaying, so
        # this is already the product of a fresh cross-section ->
        # technical -> feature-input replay chain, not a shortcut around it.
        feature_input_panel = cross_section_panel.source_panel.source_panel
        if (
            type(feature_input_panel) is not VerifiedPromotedFeatureInputPanel
            or feature_input_panel.panel_id != manifest.feature_input_panel_id
            or feature_input_panel.adjustment_panel.bridge_id
            != manifest.adjustment_bridge_id
            or feature_input_panel.tick_panel.panel_id
            != manifest.effective_tick_panel_id
            or feature_input_panel.adjustment_panel.corporate_actions.snapshot_id
            != manifest.expected_corporate_action_snapshot_id
            or _reachable_promotion_ids(
                feature_input_panel.adjustment_panel.source_panel
            )
            != frozenset(manifest.expected_reference_promotion_ids)
        ):
            raise PromotedEngineConflict(_ERR_VERIFY)
        try:
            replay_run = reconstruct_promoted_historical_replay_run(
                (cross_section_panel,)
            )
        except Exception:
            raise PromotedEngineConflict(_ERR_VERIFY) from None
        if (
            replay_run.run_id != manifest.replay_run_id
            or replay_run.inputs[0].market_session != manifest.signal_session
            or replay_run.inputs[0].cutoff != manifest.cutoff
        ):
            raise PromotedEngineConflict(_ERR_VERIFY)
        try:
            batch = self.research_intents.get(manifest.research_intent_batch_id)
        except Exception:
            raise PromotedEngineConflict(_ERR_VERIFY) from None
        if (
            type(batch) is not VerifiedPromotedResearchIntentBatch
            or batch.batch_id != manifest.research_intent_batch_id
            or batch.source_panel_id != manifest.cross_section_panel_id
            or batch.config_id != manifest.intent_config_id
            or batch.entry_session != manifest.entry_session
            or batch.initial_capital != manifest.initial_capital
            or len(batch.decisions) != manifest.candidate_count
            or batch.selected_count != manifest.intent_count
        ):
            raise PromotedEngineConflict(_ERR_VERIFY)

    def _read(self, path: Path) -> bytes:
        if not path.exists():
            raise PromotedEngineNotFound(_ERR_NOT_FOUND)
        if not path.is_file() or _is_link_like(path):
            raise PromotedEngineError(_ERR_UNSAFE_PATH)
        try:
            return read_stable_regular_file(
                path, maximum_bytes=_MAXIMUM_MANIFEST_BYTES
            )
        except FileSafetyError:
            raise PromotedEngineError(_ERR_UNSAFE_PATH) from None

    def _publish(self, run_id: str, payload: bytes) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        if not self.root.is_dir() or _is_link_like(self.root):
            raise PromotedEngineError(_ERR_UNSAFE_PATH)
        target = self.path_for(run_id)
        try:
            with advisory_file_lock(self.root / self._LOCK_FILENAME):
                if target.exists():
                    if _is_link_like(target) or self._read(target) != payload:
                        raise PromotedEngineConflict(_ERR_CONFLICT)
                    return
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".promoted-engine-run-",
                    suffix=".tmp",
                    dir=self.root,
                )
                temporary = Path(temporary_name)
                try:
                    with os.fdopen(descriptor, "wb") as handle:
                        handle.write(payload)
                        handle.flush()
                        os.fsync(handle.fileno())
                    os.link(temporary, target)
                finally:
                    temporary.unlink(missing_ok=True)
        except PromotedEngineConflict:
            raise
        except (FileLockUnavailable, FileSafetyError, OSError):
            raise PromotedEngineConflict(_ERR_CONFLICT) from None
