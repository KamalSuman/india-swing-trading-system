from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from india_swing.calendar_data.materialization_store import (
    LocalCalendarMaterializationStore,
)
from india_swing.corporate_actions.promoted_adjustments import (
    PromotedCorporateActionAdjustmentService,
)
from india_swing.corporate_actions.snapshot_store import (
    LocalCorporateActionSnapshotStore,
)
from india_swing.evaluation.promoted_intent_store import (
    LocalPromotedResearchIntentStore,
)
from india_swing.features.promoted_cross_section import PromotedCrossSectionConfig
from india_swing.features.promoted_technical import PromotedTechnicalFeatureConfig
from india_swing.features.store import (
    LocalPromotedCrossSectionStore,
    LocalPromotedFeatureInputStore,
    LocalPromotedTechnicalFeatureStore,
)
from india_swing.identity_decisions.artifact_store import (
    LocalIdentityReviewBundleStore,
)
from india_swing.identity_evidence.artifact_store import (
    LocalIdentityEvidenceArtifactStore,
)
from india_swing.market_data.historical_corpus import (
    LocalHistoricalEvaluationCorpusStore,
)
from india_swing.promoted_engine import (
    LocalPromotedEngineRunStore,
    PromotedEngineConflict,
    PromotedEngineError,
    PromotedEngineNotFound,
    PromotedEngineRunManifest,
    PromotedEngineRunRequest,
    PromotedEngineRunner,
    PromotedEngineStores,
    _CalendarResolverAdapter,
    _compute_request_id,
    build_promoted_engine_stores,
    decode_promoted_engine_run_manifest,
    encode_promoted_engine_run_manifest,
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
    PromotedEffectiveSessionTickService,
)
from tests.test_promoted_corporate_action_bridge import (
    BRIDGE_CUTOFF,
    _event,
    _snapshot,
)
from tests.test_promoted_intents import _permissive_config
from tests.test_promoted_stable_listing_history import _two_session_fixture
from tests.test_promoted_technical_features import _small_config

UTC = timezone.utc


class ExactResolver:
    def __init__(self, values, identity) -> None:
        self.values = {identity(value): value for value in values}

    def get(self, identity_value):
        return self.values[identity_value]


def _identity_stores(root: Path, calendar, snapshots) -> LocalPromotedStableListingHistoryStore:
    """Wire the identity-graph layers with in-memory resolvers.

    Restart-safety for this specific layer is already exercised by
    tests/test_promoted_graph_store.py's own suite (re-run alongside this
    file as part of the same named test command); this fixture exists only
    to make a real, disk-backed LocalPromotedStableListingHistoryStore
    resolvable for promoted_engine.py's own adjustment/tick stores.
    """

    frames = tuple(value.frame for value in snapshots)
    universes = tuple(value.universe for value in frames)
    adjudication = universes[0].adjudication
    intake = adjudication.intake
    promotions = intake.promotions
    corpora = {
        value.corpus_index.corpus_id: (value.corpus_index, (value.partition,))
        for value in frames
    }

    promotion_resolver = ExactResolver(promotions, lambda value: value.promotion_id)
    intake_store = LocalPromotedIdentityIntakeStore(root, promotion_resolver)
    evidence_resolver = ExactResolver(
        adjudication.evidence_artifacts, lambda value: value.manifest.artifact_id
    )
    review_resolver = ExactResolver(
        adjudication.review_bundles, lambda value: value.manifest.bundle_id
    )
    adjudication_store = LocalPromotedIdentityAdjudicationStore(
        root, intake_store, evidence_resolver, review_resolver
    )
    calendar_resolver = ExactResolver(
        (calendar,), lambda value: value.materialization_id
    )
    universe_store = LocalPromotedIdentitySessionUniverseStore(
        root, adjudication_store, calendar_resolver
    )
    corpus_resolver = ExactResolver(
        tuple(corpora.values()), lambda value: value[0].corpus_id
    )
    frame_store = LocalPromotedSessionMarketDataFrameStore(
        root, universe_store, corpus_resolver
    )
    tick_store = LocalPromotedSessionTickSnapshotStore(root, frame_store)
    history_store = LocalPromotedStableListingHistoryStore(
        root, tick_store, calendar_resolver
    )
    intake_store.put(intake)
    adjudication_store.put(adjudication)
    for universe in universes:
        universe_store.put(universe)
    for frame in frames:
        frame_store.put(frame)
    for snapshot in snapshots:
        tick_store.put(snapshot)
    return history_store


def _reachable_promotion_ids(history) -> tuple[str, ...]:
    ids = {
        promotion.promotion_id
        for snapshot in history.tick_snapshots
        for promotion in snapshot.frame.universe.adjudication.intake.promotions
    }
    return tuple(sorted(ids))


# The underlying fixture graph (calendar/snapshots/history/corporate-action
# snapshot/adjustment/effective ticks) is expensive to materialize and is
# identical, immutable content for every test in this module. It is built
# exactly once here and reused (never mutated) by every test's own
# independent, disk-backed store instances -- each test still exercises a
# fresh, real create-once store cascade; only the shared upstream domain
# objects are cached.
_FIXTURE_ROOT_HANDLE = tempfile.TemporaryDirectory()
_CALENDAR, _SNAPSHOTS, _HISTORY = _two_session_fixture(
    Path(_FIXTURE_ROOT_HANDLE.name)
)
_ACTIONS = _snapshot(_event(_HISTORY))
_ADJUSTMENT = PromotedCorporateActionAdjustmentService().materialize(
    source_panel=_HISTORY,
    corporate_actions=_ACTIONS,
    cutoff=BRIDGE_CUTOFF,
)
_TICKS = PromotedEffectiveSessionTickService().materialize(
    source_panel=_HISTORY,
    cutoff=BRIDGE_CUTOFF,
)
_PROMOTION_IDS = _reachable_promotion_ids(_HISTORY)


def _build_engine_stores(root: Path):
    """Wire one full, real-store-backed promoted-engine graph at ``root``.

    Reuses the shared module-level fixture objects; only the store
    instances and on-disk artifacts under ``root`` are fresh per call.

    Returns (stores, history, adjustment, ticks, actions,
    expected_promotion_ids, calendar, snapshots).
    """

    graph_root = root / "graph"
    history_store = _identity_stores(graph_root, _CALENDAR, _SNAPSHOTS)
    history_store.put(_HISTORY)

    action_resolver = ExactResolver((_ACTIONS,), lambda value: value.snapshot_id)
    adjustment_store = LocalPromotedCorporateActionAdjustmentStore(
        graph_root, history_store, action_resolver
    )
    effective_store = LocalPromotedEffectiveSessionTickStore(
        graph_root, history_store
    )
    adjustment_store.put(_ADJUSTMENT)
    effective_store.put(_TICKS)

    engine_root = root / "engine"
    feature_inputs = LocalPromotedFeatureInputStore(
        engine_root, adjustment_store, effective_store
    )
    technical_features = LocalPromotedTechnicalFeatureStore(
        engine_root, feature_inputs
    )
    cross_sections = LocalPromotedCrossSectionStore(engine_root, technical_features)
    research_intents = LocalPromotedResearchIntentStore(engine_root, cross_sections)
    engine_runs = LocalPromotedEngineRunStore(
        root / "runs",
        cross_sections=cross_sections,
        research_intents=research_intents,
    )
    stores = PromotedEngineStores(
        corporate_action_adjustments=adjustment_store,
        effective_session_ticks=effective_store,
        feature_inputs=feature_inputs,
        technical_features=technical_features,
        cross_sections=cross_sections,
        research_intents=research_intents,
        engine_runs=engine_runs,
    )
    return (
        stores,
        _HISTORY,
        _ADJUSTMENT,
        _TICKS,
        _ACTIONS,
        _PROMOTION_IDS,
        _CALENDAR,
        _SNAPSHOTS,
    )


def _rebuild_engine_stores(
    root: Path, calendar, snapshots, actions
) -> PromotedEngineStores:
    """Reconstruct brand-new store instances rooted at the same paths.

    Simulates a fresh process: every object here is newly constructed, and
    every store must independently re-derive its content from the bytes
    already persisted under ``root`` rather than from any in-memory state
    carried over from the original run. The identity-graph layer
    (intake/adjudication/universe/frame/tick-snapshot/history) still uses
    in-memory resolvers seeded from the same immutable fixture values --
    restart-safety for that exact layer is already proven independently by
    tests/test_promoted_graph_store.py, which runs alongside this file.
    """

    graph_root = root / "graph"
    history_store = _identity_stores(graph_root, calendar, snapshots)
    action_resolver = ExactResolver((actions,), lambda value: value.snapshot_id)
    adjustment_store = LocalPromotedCorporateActionAdjustmentStore(
        graph_root, history_store, action_resolver
    )
    effective_store = LocalPromotedEffectiveSessionTickStore(
        graph_root, history_store
    )
    engine_root = root / "engine"
    feature_inputs = LocalPromotedFeatureInputStore(
        engine_root, adjustment_store, effective_store
    )
    technical_features = LocalPromotedTechnicalFeatureStore(
        engine_root, feature_inputs
    )
    cross_sections = LocalPromotedCrossSectionStore(engine_root, technical_features)
    research_intents = LocalPromotedResearchIntentStore(engine_root, cross_sections)
    engine_runs = LocalPromotedEngineRunStore(
        root / "runs",
        cross_sections=cross_sections,
        research_intents=research_intents,
    )
    return PromotedEngineStores(
        corporate_action_adjustments=adjustment_store,
        effective_session_ticks=effective_store,
        feature_inputs=feature_inputs,
        technical_features=technical_features,
        cross_sections=cross_sections,
        research_intents=research_intents,
        engine_runs=engine_runs,
    )


def _small_request(
    *,
    adjustment,
    ticks,
    expected_promotion_ids: tuple[str, ...],
    actions,
    technical_config: PromotedTechnicalFeatureConfig,
    cross_section_config: PromotedCrossSectionConfig,
    intent_config,
    cutoff: datetime = BRIDGE_CUTOFF,
    entry_offset_days: int = 1,
) -> PromotedEngineRunRequest:
    return PromotedEngineRunRequest(
        adjustment_bridge_id=adjustment.bridge_id,
        effective_tick_panel_id=ticks.panel_id,
        expected_reference_promotion_ids=expected_promotion_ids,
        expected_corporate_action_snapshot_id=actions.snapshot_id,
        signal_session=adjustment.signal_session,
        entry_session=adjustment.signal_session + timedelta(days=entry_offset_days),
        cutoff=cutoff,
        initial_capital=Decimal("1000000"),
        technical_config=technical_config,
        cross_section_config=cross_section_config,
        intent_config=intent_config,
    )


class PromotedEngineRunRequestTests(unittest.TestCase):
    def test_entry_session_must_be_after_signal_session(self) -> None:
        with self.assertRaises(PromotedEngineError):
            PromotedEngineRunRequest(
                adjustment_bridge_id="a" * 64,
                effective_tick_panel_id="b" * 64,
                expected_reference_promotion_ids=("c" * 64,),
                expected_corporate_action_snapshot_id="d" * 64,
                signal_session=date(2026, 7, 16),
                entry_session=date(2026, 7, 16),
                cutoff=datetime(2026, 7, 17, tzinfo=UTC),
                initial_capital=Decimal("1000"),
                technical_config=PromotedTechnicalFeatureConfig(
                    minimum_history_sessions=2,
                    short_return_sessions=1,
                    medium_return_sessions=1,
                    long_return_sessions=1,
                    short_trend_sessions=2,
                    long_trend_sessions=2,
                    atr_sessions=1,
                    volatility_sessions=1,
                    liquidity_sessions=1,
                    breakout_sessions=1,
                    drawdown_sessions=2,
                    contraction_short_sessions=1,
                    contraction_long_sessions=1,
                    tick_history_sessions=2,
                ),
                cross_section_config=PromotedCrossSectionConfig(
                    minimum_computed_instruments=1
                ),
                intent_config=_permissive_config(),
            )

    def test_expected_promotion_ids_must_be_sorted_unique_nonempty(self) -> None:
        with self.assertRaises(PromotedEngineError):
            PromotedEngineRunRequest(
                adjustment_bridge_id="a" * 64,
                effective_tick_panel_id="b" * 64,
                expected_reference_promotion_ids=(),
                expected_corporate_action_snapshot_id="d" * 64,
                signal_session=date(2026, 7, 16),
                entry_session=date(2026, 7, 17),
                cutoff=datetime(2026, 7, 17, tzinfo=UTC),
                initial_capital=Decimal("1000"),
                technical_config=_small_config(),
                cross_section_config=PromotedCrossSectionConfig(
                    minimum_computed_instruments=1
                ),
                intent_config=_permissive_config(),
            )

    def test_request_id_changes_with_cutoff(self) -> None:
        kwargs = dict(
            adjustment_bridge_id="a" * 64,
            effective_tick_panel_id="b" * 64,
            expected_reference_promotion_ids=("c" * 64,),
            expected_corporate_action_snapshot_id="d" * 64,
            signal_session=date(2026, 7, 16),
            entry_session=date(2026, 7, 17),
            initial_capital=Decimal("1000"),
            technical_config=_small_config(),
            cross_section_config=PromotedCrossSectionConfig(
                minimum_computed_instruments=1
            ),
            intent_config=_permissive_config(),
        )
        first = PromotedEngineRunRequest(cutoff=datetime(2026, 7, 17, tzinfo=UTC), **kwargs)
        second = PromotedEngineRunRequest(cutoff=datetime(2026, 7, 18, tzinfo=UTC), **kwargs)
        self.assertNotEqual(first.request_id, second.request_id)


class PromotedEngineRunnerAcceptanceTests(unittest.TestCase):
    def test_end_to_end_run_persists_every_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (
                stores,
                history,
                adjustment,
                ticks,
                actions,
                promotion_ids,
                calendar,
                snapshots,
            ) = _build_engine_stores(root)
            request = _small_request(
                adjustment=adjustment,
                ticks=ticks,
                expected_promotion_ids=promotion_ids,
                actions=actions,
                technical_config=_small_config(),
                cross_section_config=PromotedCrossSectionConfig(
                    minimum_computed_instruments=1
                ),
                intent_config=_permissive_config(),
            )
            manifest = PromotedEngineRunner().run(request, stores)

            self.assertTrue(manifest.paper_only)
            self.assertEqual(manifest.signal_session, adjustment.signal_session)
            self.assertGreater(manifest.candidate_count, 0)
            self.assertGreaterEqual(manifest.intent_count, 0)
            self.assertLessEqual(manifest.intent_count, manifest.candidate_count)

            # Every intermediate output actually resolves through its own store.
            feature_input = stores.feature_inputs.get(manifest.feature_input_panel_id)
            self.assertEqual(
                feature_input.adjustment_panel.bridge_id, adjustment.bridge_id
            )
            technical = stores.technical_features.get(manifest.technical_panel_id)
            self.assertEqual(technical.config.config_id, manifest.technical_config_id)
            cross_section = stores.cross_sections.get(manifest.cross_section_panel_id)
            self.assertEqual(
                cross_section.config.config_id, manifest.cross_section_config_id
            )
            batch = stores.research_intents.get(manifest.research_intent_batch_id)
            self.assertEqual(batch.config_id, manifest.intent_config_id)

            got = stores.engine_runs.get(manifest.run_id)
            self.assertEqual(got, manifest)

    def test_repeated_run_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (
                stores,
                history,
                adjustment,
                ticks,
                actions,
                promotion_ids,
                calendar,
                snapshots,
            ) = _build_engine_stores(root)
            request = _small_request(
                adjustment=adjustment,
                ticks=ticks,
                expected_promotion_ids=promotion_ids,
                actions=actions,
                technical_config=_small_config(),
                cross_section_config=PromotedCrossSectionConfig(
                    minimum_computed_instruments=1
                ),
                intent_config=_permissive_config(),
            )
            first = PromotedEngineRunner().run(request, stores)
            second = PromotedEngineRunner().run(request, stores)
            self.assertEqual(first, second)
            self.assertEqual(first.run_id, second.run_id)

    def test_fresh_process_store_reconstruction_replays_the_same_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (
                stores,
                history,
                adjustment,
                ticks,
                actions,
                promotion_ids,
                calendar,
                snapshots,
            ) = _build_engine_stores(root)
            request = _small_request(
                adjustment=adjustment,
                ticks=ticks,
                expected_promotion_ids=promotion_ids,
                actions=actions,
                technical_config=_small_config(),
                cross_section_config=PromotedCrossSectionConfig(
                    minimum_computed_instruments=1
                ),
                intent_config=_permissive_config(),
            )
            original = PromotedEngineRunner().run(request, stores)

            fresh_stores = _rebuild_engine_stores(root, calendar, snapshots, actions)
            replayed = fresh_stores.engine_runs.get(original.run_id)
            self.assertEqual(replayed, original)

    def test_zero_intent_blocked_result_is_persisted_successfully(self) -> None:
        # Deliberately mismatch the default 61-session technical config
        # against the tiny two-session fixture: this guarantees an
        # INSUFFICIENT_HISTORY_BLOCKED technical result (not an exception),
        # cascading to a fully blocked cross-section and a zero-selected
        # research-intent batch -- proving a blocked paper result is a
        # successful, auditable, persisted outcome.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (
                stores,
                history,
                adjustment,
                ticks,
                actions,
                promotion_ids,
                calendar,
                snapshots,
            ) = _build_engine_stores(root)
            request = _small_request(
                adjustment=adjustment,
                ticks=ticks,
                expected_promotion_ids=promotion_ids,
                actions=actions,
                technical_config=PromotedTechnicalFeatureConfig(),
                cross_section_config=PromotedCrossSectionConfig(),
                intent_config=_permissive_config(),
            )
            manifest = PromotedEngineRunner().run(request, stores)
            self.assertEqual(manifest.intent_count, 0)
            self.assertGreater(manifest.candidate_count, 0)
            got = stores.engine_runs.get(manifest.run_id)
            self.assertEqual(got, manifest)


class PromotedEngineRunnerRejectionTests(unittest.TestCase):
    def test_mismatched_expected_promotion_ids_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (
                stores,
                history,
                adjustment,
                ticks,
                actions,
                promotion_ids,
                calendar,
                snapshots,
            ) = _build_engine_stores(root)
            request = _small_request(
                adjustment=adjustment,
                ticks=ticks,
                expected_promotion_ids=("f" * 64,),
                actions=actions,
                technical_config=_small_config(),
                cross_section_config=PromotedCrossSectionConfig(
                    minimum_computed_instruments=1
                ),
                intent_config=_permissive_config(),
            )
            with self.assertRaises(PromotedEngineError):
                PromotedEngineRunner().run(request, stores)

    def test_mismatched_expected_corporate_action_snapshot_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (
                stores,
                history,
                adjustment,
                ticks,
                actions,
                promotion_ids,
                calendar,
                snapshots,
            ) = _build_engine_stores(root)
            request = PromotedEngineRunRequest(
                adjustment_bridge_id=adjustment.bridge_id,
                effective_tick_panel_id=ticks.panel_id,
                expected_reference_promotion_ids=promotion_ids,
                expected_corporate_action_snapshot_id="f" * 64,
                signal_session=adjustment.signal_session,
                entry_session=adjustment.signal_session + timedelta(days=1),
                cutoff=BRIDGE_CUTOFF,
                initial_capital=Decimal("1000000"),
                technical_config=_small_config(),
                cross_section_config=PromotedCrossSectionConfig(
                    minimum_computed_instruments=1
                ),
                intent_config=_permissive_config(),
            )
            with self.assertRaises(PromotedEngineError):
                PromotedEngineRunner().run(request, stores)

    def test_mismatched_signal_session_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (
                stores,
                history,
                adjustment,
                ticks,
                actions,
                promotion_ids,
                calendar,
                snapshots,
            ) = _build_engine_stores(root)
            request = PromotedEngineRunRequest(
                adjustment_bridge_id=adjustment.bridge_id,
                effective_tick_panel_id=ticks.panel_id,
                expected_reference_promotion_ids=promotion_ids,
                expected_corporate_action_snapshot_id=actions.snapshot_id,
                signal_session=adjustment.signal_session - timedelta(days=1),
                entry_session=adjustment.signal_session + timedelta(days=1),
                cutoff=BRIDGE_CUTOFF,
                initial_capital=Decimal("1000000"),
                technical_config=_small_config(),
                cross_section_config=PromotedCrossSectionConfig(
                    minimum_computed_instruments=1
                ),
                intent_config=_permissive_config(),
            )
            with self.assertRaises(PromotedEngineError):
                PromotedEngineRunner().run(request, stores)

    def test_cutoff_before_known_evidence_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (
                stores,
                history,
                adjustment,
                ticks,
                actions,
                promotion_ids,
                calendar,
                snapshots,
            ) = _build_engine_stores(root)
            request = _small_request(
                adjustment=adjustment,
                ticks=ticks,
                expected_promotion_ids=promotion_ids,
                actions=actions,
                technical_config=_small_config(),
                cross_section_config=PromotedCrossSectionConfig(
                    minimum_computed_instruments=1
                ),
                intent_config=_permissive_config(),
                cutoff=BRIDGE_CUTOFF - timedelta(days=3650),
            )
            with self.assertRaises(PromotedEngineError):
                PromotedEngineRunner().run(request, stores)

    def test_unresolvable_bridge_id_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (
                stores,
                history,
                adjustment,
                ticks,
                actions,
                promotion_ids,
                calendar,
                snapshots,
            ) = _build_engine_stores(root)
            request = _small_request(
                adjustment=adjustment,
                ticks=ticks,
                expected_promotion_ids=promotion_ids,
                actions=actions,
                technical_config=_small_config(),
                cross_section_config=PromotedCrossSectionConfig(
                    minimum_computed_instruments=1
                ),
                intent_config=_permissive_config(),
            )
            tampered = PromotedEngineRunRequest(
                adjustment_bridge_id="0" * 64,
                effective_tick_panel_id=request.effective_tick_panel_id,
                expected_reference_promotion_ids=request.expected_reference_promotion_ids,
                expected_corporate_action_snapshot_id=(
                    request.expected_corporate_action_snapshot_id
                ),
                signal_session=request.signal_session,
                entry_session=request.entry_session,
                cutoff=request.cutoff,
                initial_capital=request.initial_capital,
                technical_config=request.technical_config,
                cross_section_config=request.cross_section_config,
                intent_config=request.intent_config,
            )
            with self.assertRaises(PromotedEngineError):
                PromotedEngineRunner().run(tampered, stores)


class PromotedEngineRunStoreTests(unittest.TestCase):
    def test_tampered_output_id_in_manifest_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (
                stores,
                history,
                adjustment,
                ticks,
                actions,
                promotion_ids,
                calendar,
                snapshots,
            ) = _build_engine_stores(root)
            request = _small_request(
                adjustment=adjustment,
                ticks=ticks,
                expected_promotion_ids=promotion_ids,
                actions=actions,
                technical_config=_small_config(),
                cross_section_config=PromotedCrossSectionConfig(
                    minimum_computed_instruments=1
                ),
                intent_config=_permissive_config(),
            )
            manifest = PromotedEngineRunner().run(request, stores)
            path = stores.engine_runs.path_for(manifest.run_id)
            decoded = json.loads(path.read_text("utf-8"))
            decoded["cross_section_panel_id"] = "f" * 64
            payload = (
                json.dumps(decoded, separators=(",", ":"), sort_keys=True) + "\n"
            ).encode("utf-8")
            path.write_bytes(payload)
            with self.assertRaises(PromotedEngineError):
                stores.engine_runs.get(manifest.run_id)

    def test_tampered_candidate_count_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (
                stores,
                history,
                adjustment,
                ticks,
                actions,
                promotion_ids,
                calendar,
                snapshots,
            ) = _build_engine_stores(root)
            request = _small_request(
                adjustment=adjustment,
                ticks=ticks,
                expected_promotion_ids=promotion_ids,
                actions=actions,
                technical_config=_small_config(),
                cross_section_config=PromotedCrossSectionConfig(
                    minimum_computed_instruments=1
                ),
                intent_config=_permissive_config(),
            )
            manifest = PromotedEngineRunner().run(request, stores)
            path = stores.engine_runs.path_for(manifest.run_id)
            decoded = json.loads(path.read_text("utf-8"))
            decoded["candidate_count"] = decoded["candidate_count"] + 5
            payload = (
                json.dumps(decoded, separators=(",", ":"), sort_keys=True) + "\n"
            ).encode("utf-8")
            path.write_bytes(payload)
            with self.assertRaises(PromotedEngineError):
                stores.engine_runs.get(manifest.run_id)

    def test_tampered_nested_source_fails_closed_through_the_engine_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (
                stores,
                history,
                adjustment,
                ticks,
                actions,
                promotion_ids,
                calendar,
                snapshots,
            ) = _build_engine_stores(root)
            request = _small_request(
                adjustment=adjustment,
                ticks=ticks,
                expected_promotion_ids=promotion_ids,
                actions=actions,
                technical_config=_small_config(),
                cross_section_config=PromotedCrossSectionConfig(
                    minimum_computed_instruments=1
                ),
                intent_config=_permissive_config(),
            )
            manifest = PromotedEngineRunner().run(request, stores)
            feature_path = stores.feature_inputs.path_for(
                manifest.feature_input_panel_id
            )
            decoded = json.loads(feature_path.read_text("utf-8"))
            decoded["panel"]["unassigned_entry_count"] = 999999
            payload = (
                json.dumps(decoded, separators=(",", ":"), sort_keys=True) + "\n"
            ).encode("utf-8")
            feature_path.write_bytes(payload)
            with self.assertRaises(Exception):
                stores.engine_runs.get(manifest.run_id)

    def test_create_once_conflict_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (
                stores,
                history,
                adjustment,
                ticks,
                actions,
                promotion_ids,
                calendar,
                snapshots,
            ) = _build_engine_stores(root)
            request = _small_request(
                adjustment=adjustment,
                ticks=ticks,
                expected_promotion_ids=promotion_ids,
                actions=actions,
                technical_config=_small_config(),
                cross_section_config=PromotedCrossSectionConfig(
                    minimum_computed_instruments=1
                ),
                intent_config=_permissive_config(),
            )
            manifest = PromotedEngineRunner().run(request, stores)

            conflicting_store = LocalPromotedEngineRunStore(
                root / "conflict-runs",
                cross_sections=stores.cross_sections,
                research_intents=stores.research_intents,
            )
            path = conflicting_store.path_for(manifest.run_id)
            path.parent.mkdir(parents=True)
            path.write_bytes(b"{}\n")
            with self.assertRaises(PromotedEngineConflict):
                conflicting_store.put(manifest)

    def test_missing_run_is_not_found(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            stores, *_ = _build_engine_stores(Path(tmp))
            with self.assertRaises(PromotedEngineNotFound):
                stores.engine_runs.get("0" * 64)

    def test_symlinked_target_is_rejected_when_supported(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (
                stores,
                history,
                adjustment,
                ticks,
                actions,
                promotion_ids,
                calendar,
                snapshots,
            ) = _build_engine_stores(root)
            request = _small_request(
                adjustment=adjustment,
                ticks=ticks,
                expected_promotion_ids=promotion_ids,
                actions=actions,
                technical_config=_small_config(),
                cross_section_config=PromotedCrossSectionConfig(
                    minimum_computed_instruments=1
                ),
                intent_config=_permissive_config(),
            )
            manifest = PromotedEngineRunner().run(request, stores)
            path = stores.engine_runs.path_for(manifest.run_id)
            real = path.with_suffix(".real.json")
            path.rename(real)
            try:
                path.symlink_to(real)
            except OSError:
                self.skipTest("symbolic links are unavailable in this environment")
            with self.assertRaises(PromotedEngineError):
                stores.engine_runs.get(manifest.run_id)

    def test_error_text_never_includes_raw_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (
                stores,
                history,
                adjustment,
                ticks,
                actions,
                promotion_ids,
                calendar,
                snapshots,
            ) = _build_engine_stores(root)
            request = _small_request(
                adjustment=adjustment,
                ticks=ticks,
                expected_promotion_ids=promotion_ids,
                actions=actions,
                technical_config=_small_config(),
                cross_section_config=PromotedCrossSectionConfig(
                    minimum_computed_instruments=1
                ),
                intent_config=_permissive_config(),
            )
            manifest = PromotedEngineRunner().run(request, stores)
            path = stores.engine_runs.path_for(manifest.run_id)
            decoded = json.loads(path.read_text("utf-8"))
            decoded["cross_section_panel_id"] = "f" * 64
            payload = (
                json.dumps(decoded, separators=(",", ":"), sort_keys=True) + "\n"
            ).encode("utf-8")
            path.write_bytes(payload)
            with self.assertRaises(PromotedEngineError) as ctx:
                stores.engine_runs.get(manifest.run_id)
            message = str(ctx.exception)
            self.assertNotIn("f" * 64, message)
            self.assertNotIn(str(path), message)

    def test_opaque_request_id_mismatch_cannot_survive_encode_decode(self) -> None:
        # An arbitrary valid-SHA request_id that does not recompute from its
        # own retained preimage must never round-trip, at construction time
        # or via the JSON codec -- the manifest must never persist only an
        # unverifiable opaque hash.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (
                stores,
                history,
                adjustment,
                ticks,
                actions,
                promotion_ids,
                calendar,
                snapshots,
            ) = _build_engine_stores(root)
            request = _small_request(
                adjustment=adjustment,
                ticks=ticks,
                expected_promotion_ids=promotion_ids,
                actions=actions,
                technical_config=_small_config(),
                cross_section_config=PromotedCrossSectionConfig(
                    minimum_computed_instruments=1
                ),
                intent_config=_permissive_config(),
            )
            manifest = PromotedEngineRunner().run(request, stores)

            with self.assertRaises(PromotedEngineError):
                PromotedEngineRunManifest(
                    schema_version=manifest.schema_version,
                    request_id="0" * 64,
                    adjustment_bridge_id=manifest.adjustment_bridge_id,
                    effective_tick_panel_id=manifest.effective_tick_panel_id,
                    expected_reference_promotion_ids=(
                        manifest.expected_reference_promotion_ids
                    ),
                    expected_corporate_action_snapshot_id=(
                        manifest.expected_corporate_action_snapshot_id
                    ),
                    feature_input_panel_id=manifest.feature_input_panel_id,
                    technical_config_id=manifest.technical_config_id,
                    technical_panel_id=manifest.technical_panel_id,
                    cross_section_config_id=manifest.cross_section_config_id,
                    cross_section_panel_id=manifest.cross_section_panel_id,
                    intent_config_id=manifest.intent_config_id,
                    research_intent_batch_id=manifest.research_intent_batch_id,
                    replay_run_id=manifest.replay_run_id,
                    signal_session=manifest.signal_session,
                    entry_session=manifest.entry_session,
                    cutoff=manifest.cutoff,
                    initial_capital=manifest.initial_capital,
                    candidate_count=manifest.candidate_count,
                    intent_count=manifest.intent_count,
                    paper_only=True,
                )

            # Same mismatch, but arriving through the JSON codec instead of
            # direct construction: tamper only request_id in an otherwise
            # genuine, previously-published payload.
            path = stores.engine_runs.path_for(manifest.run_id)
            decoded = json.loads(path.read_text("utf-8"))
            decoded["request_id"] = "0" * 64
            payload = (
                json.dumps(decoded, separators=(",", ":"), sort_keys=True) + "\n"
            ).encode("utf-8")
            with self.assertRaises(PromotedEngineError):
                decode_promoted_engine_run_manifest(payload)

    def test_self_consistent_root_pin_tamper_fails_on_reconstructed_lineage(
        self,
    ) -> None:
        # A manifest that is fully self-consistent on its own terms --
        # recomputed request_id and run_id both agree with its own retained
        # fields -- must still fail if the claimed adjustment_bridge_id does
        # not match what the independently re-resolved feature-input panel
        # actually embeds. Self-consistency alone must never substitute for
        # matching the reconstructed downstream lineage.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (
                stores,
                history,
                adjustment,
                ticks,
                actions,
                promotion_ids,
                calendar,
                snapshots,
            ) = _build_engine_stores(root)
            request = _small_request(
                adjustment=adjustment,
                ticks=ticks,
                expected_promotion_ids=promotion_ids,
                actions=actions,
                technical_config=_small_config(),
                cross_section_config=PromotedCrossSectionConfig(
                    minimum_computed_instruments=1
                ),
                intent_config=_permissive_config(),
            )
            manifest = PromotedEngineRunner().run(request, stores)

            substituted_bridge_id = "f" * 64
            self.assertNotEqual(substituted_bridge_id, manifest.adjustment_bridge_id)
            tampered_request_id = _compute_request_id(
                adjustment_bridge_id=substituted_bridge_id,
                effective_tick_panel_id=manifest.effective_tick_panel_id,
                expected_reference_promotion_ids=(
                    manifest.expected_reference_promotion_ids
                ),
                expected_corporate_action_snapshot_id=(
                    manifest.expected_corporate_action_snapshot_id
                ),
                signal_session=manifest.signal_session,
                entry_session=manifest.entry_session,
                cutoff=manifest.cutoff,
                initial_capital=manifest.initial_capital,
                technical_config_id=manifest.technical_config_id,
                cross_section_config_id=manifest.cross_section_config_id,
                intent_config_id=manifest.intent_config_id,
            )
            tampered_manifest = PromotedEngineRunManifest(
                schema_version=manifest.schema_version,
                request_id=tampered_request_id,
                adjustment_bridge_id=substituted_bridge_id,
                effective_tick_panel_id=manifest.effective_tick_panel_id,
                expected_reference_promotion_ids=(
                    manifest.expected_reference_promotion_ids
                ),
                expected_corporate_action_snapshot_id=(
                    manifest.expected_corporate_action_snapshot_id
                ),
                feature_input_panel_id=manifest.feature_input_panel_id,
                technical_config_id=manifest.technical_config_id,
                technical_panel_id=manifest.technical_panel_id,
                cross_section_config_id=manifest.cross_section_config_id,
                cross_section_panel_id=manifest.cross_section_panel_id,
                intent_config_id=manifest.intent_config_id,
                research_intent_batch_id=manifest.research_intent_batch_id,
                replay_run_id=manifest.replay_run_id,
                signal_session=manifest.signal_session,
                entry_session=manifest.entry_session,
                cutoff=manifest.cutoff,
                initial_capital=manifest.initial_capital,
                candidate_count=manifest.candidate_count,
                intent_count=manifest.intent_count,
                paper_only=True,
            )
            # Fully self-consistent: both recomputed on construction.
            tampered_manifest.verify_content_identity()
            self.assertNotEqual(tampered_manifest.run_id, manifest.run_id)

            tampered_path = stores.engine_runs.path_for(tampered_manifest.run_id)
            tampered_path.parent.mkdir(parents=True, exist_ok=True)
            tampered_payload = encode_promoted_engine_run_manifest(tampered_manifest)
            tampered_path.write_bytes(tampered_payload)

            with self.assertRaises(PromotedEngineConflict):
                stores.engine_runs.get(tampered_manifest.run_id)


class PromotedEngineCapabilityTests(unittest.TestCase):
    def test_no_listing_latest_nearest_find_or_live_capability_exists(self) -> None:
        banned_substrings = (
            "list",
            "latest",
            "nearest",
            "find",
            "network",
            "gcp",
            "broker",
            "telegram",
            "order",
            "alert",
        )
        members = [
            name for name in dir(LocalPromotedEngineRunStore) if not name.startswith("__")
        ]
        for name in members:
            lowered = name.lower()
            self.assertFalse(
                any(bad in lowered for bad in banned_substrings),
                f"LocalPromotedEngineRunStore unexpectedly exposes {name!r}",
            )
        public_members = {name for name in members if not name.startswith("_")}
        self.assertEqual(public_members, {"path_for", "put", "get"})


class BuildPromotedEngineStoresTests(unittest.TestCase):
    """Direct, unmocked coverage of the real seven-root store factory.

    Constructs build_promoted_engine_stores from seven distinct temporary
    roots and asserts every exposed store is the exact intended real class,
    and that every nested resolver/root mapping reaches the intended object
    -- catching constructor/root mapping regressions that the CLI tests
    (which patch build_promoted_engine_stores) and the programmatic tests
    (which compose a partly in-memory resolver graph) cannot see. This test
    does not populate or scan any live data.
    """

    def test_real_factory_wires_every_store_to_the_intended_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            reference_root = root / "reference"
            identity_evidence_root = root / "identity-evidence"
            calendar_root = root / "calendar"
            daily_reports_root = root / "daily-reports"
            historical_corpus_root = root / "historical-corpus"
            promoted_root = root / "promoted"
            engine_run_root = root / "engine-run"

            stores = build_promoted_engine_stores(
                reference_root=reference_root,
                identity_evidence_root=identity_evidence_root,
                calendar_root=calendar_root,
                daily_reports_root=daily_reports_root,
                historical_corpus_root=historical_corpus_root,
                promoted_root=promoted_root,
                engine_run_root=engine_run_root,
            )

            self.assertIs(type(stores), PromotedEngineStores)
            self.assertIs(
                type(stores.corporate_action_adjustments),
                LocalPromotedCorporateActionAdjustmentStore,
            )
            self.assertIs(
                type(stores.effective_session_ticks),
                LocalPromotedEffectiveSessionTickStore,
            )
            self.assertIs(type(stores.feature_inputs), LocalPromotedFeatureInputStore)
            self.assertIs(
                type(stores.technical_features), LocalPromotedTechnicalFeatureStore
            )
            self.assertIs(type(stores.cross_sections), LocalPromotedCrossSectionStore)
            self.assertIs(
                type(stores.research_intents), LocalPromotedResearchIntentStore
            )
            self.assertIs(type(stores.engine_runs), LocalPromotedEngineRunStore)

            # Top-level cross-wiring: the two stores the runner calls
            # directly, and the two the run store verifies against, must be
            # the exact same shared instances throughout.
            self.assertIs(
                stores.feature_inputs.adjustment_resolver,
                stores.corporate_action_adjustments,
            )
            self.assertIs(
                stores.feature_inputs.tick_resolver, stores.effective_session_ticks
            )
            self.assertIs(
                stores.technical_features.source_resolver, stores.feature_inputs
            )
            self.assertIs(
                stores.cross_sections.source_resolver, stores.technical_features
            )
            self.assertIs(
                stores.research_intents.cross_section_resolver, stores.cross_sections
            )
            self.assertIs(stores.engine_runs.cross_sections, stores.cross_sections)
            self.assertIs(
                stores.engine_runs.research_intents, stores.research_intents
            )
            self.assertEqual(
                stores.engine_runs.root, engine_run_root / "promoted-engine-runs"
            )

            # Both graph stores share one history store, rooted at promoted_root.
            history_store = stores.corporate_action_adjustments.histories
            self.assertIs(stores.effective_session_ticks.histories, history_store)
            self.assertIs(type(history_store), LocalPromotedStableListingHistoryStore)
            self.assertEqual(
                history_store._store.root,
                promoted_root / "stable-listing-histories",
            )

            corporate_action_snapshots = (
                stores.corporate_action_adjustments.corporate_actions
            )
            self.assertIs(
                type(corporate_action_snapshots), LocalCorporateActionSnapshotStore
            )
            self.assertEqual(
                corporate_action_snapshots.root,
                promoted_root / "corporate-action-snapshots",
            )

            tick_snapshot_store = history_store.tick_snapshots
            self.assertIs(
                type(tick_snapshot_store), LocalPromotedSessionTickSnapshotStore
            )
            self.assertEqual(
                tick_snapshot_store._store.root,
                promoted_root / "session-tick-snapshots",
            )

            frame_store = tick_snapshot_store.frames
            self.assertIs(type(frame_store), LocalPromotedSessionMarketDataFrameStore)
            self.assertEqual(
                frame_store._store.root,
                promoted_root / "session-market-data-frames",
            )
            self.assertIs(
                type(frame_store.corpora), LocalHistoricalEvaluationCorpusStore
            )
            self.assertEqual(frame_store.corpora.root, historical_corpus_root)

            universe_store = frame_store.universes
            self.assertIs(
                type(universe_store), LocalPromotedIdentitySessionUniverseStore
            )
            self.assertEqual(
                universe_store._store.root,
                promoted_root / "identity-session-universes",
            )
            self.assertIs(type(universe_store.calendars), _CalendarResolverAdapter)
            self.assertIs(
                type(universe_store.calendars._store),
                LocalCalendarMaterializationStore,
            )
            self.assertEqual(universe_store.calendars._store.root, calendar_root)
            self.assertEqual(
                universe_store.calendars._store.daily_reports_root,
                daily_reports_root,
            )
            # The stable-listing history store shares the identical
            # calendar-resolver instance as the session-universe store.
            self.assertIs(history_store.calendars, universe_store.calendars)

            adjudication_store = universe_store.adjudications
            self.assertIs(
                type(adjudication_store), LocalPromotedIdentityAdjudicationStore
            )
            self.assertEqual(
                adjudication_store._store.root,
                promoted_root / "identity-adjudications",
            )
            self.assertIs(
                type(adjudication_store.evidence), LocalIdentityEvidenceArtifactStore
            )
            self.assertEqual(adjudication_store.evidence.root, identity_evidence_root)
            self.assertIs(
                type(adjudication_store.reviews), LocalIdentityReviewBundleStore
            )
            self.assertEqual(adjudication_store.reviews.root, identity_evidence_root)

            intake_store = adjudication_store.intakes
            self.assertIs(type(intake_store), LocalPromotedIdentityIntakeStore)
            self.assertEqual(
                intake_store._store.root, promoted_root / "identity-intakes"
            )
            self.assertIs(
                type(intake_store.promotions), LocalReferenceArtifactPromotionStore
            )
            self.assertEqual(
                intake_store.promotions.root,
                promoted_root / "promotions",
            )
            self.assertIs(
                type(intake_store.promotions.artifacts), LocalReferenceArtifactStore
            )
            self.assertEqual(intake_store.promotions.artifacts.root, reference_root)


if __name__ == "__main__":
    unittest.main()
