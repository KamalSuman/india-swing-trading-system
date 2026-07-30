from __future__ import annotations

import tempfile
import unittest
from dataclasses import fields
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from india_swing.evaluation.promoted_intents import PromotedIntentPolicyConfig
from india_swing.features.promoted_cross_section import PromotedCrossSectionConfig
from india_swing.features.promoted_technical import PromotedTechnicalFeatureConfig
from india_swing.promoted_engine import PromotedEngineRunManifest, PromotedEngineStores
from india_swing.promoted_graph_publisher import (
    PromotedGraphPublicationManifest,
    PromotedGraphPublisher,
    ReferenceReadiness,
    _compute_spec_id,
    build_promoted_graph_stores,
    encode_promoted_graph_publication_manifest,
)
from india_swing.promoted_research_run import (
    LocalPromotedResearchRunStore,
    PromotedResearchConflict,
    PromotedResearchError,
    PromotedResearchNotFound,
    PromotedResearchOrchestrator,
    PromotedResearchRunManifest,
    PromotedResearchRunRequest,
    PromotedResearchStores,
    build_promoted_research_stores,
    decode_promoted_research_run_manifest,
    encode_promoted_research_run_manifest,
)
from tests.test_promoted_graph_publisher import _build_fixture_and_stores

UTC = timezone.utc

# Small overrides matching the tiny (single-instrument, two-session) graph
# fixture below: the production default configs require 61 prior sessions
# and 20 computed instruments, which no reasonably-sized disk fixture can
# satisfy. These remain otherwise-valid, fully-checked config objects.
_TECHNICAL_CONFIG = PromotedTechnicalFeatureConfig(
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
    tick_history_sessions=1,
)
_CROSS_SECTION_CONFIG = PromotedCrossSectionConfig(minimum_computed_instruments=1)
_INTENT_CONFIG = PromotedIntentPolicyConfig()

# The underlying promoted graph is expensive to materialize (identity
# intake/adjudication/session-universe/frame/tick-snapshot/history/
# adjustment/effective-ticks, each independently replayed and re-verified).
# It is built exactly once here and reused, unmodified, by every test in
# this module; only the engine-run/research-run store roots vary per test so
# each test still exercises a fresh, real create-once store cascade for the
# layer actually under test.
#
# This graph's identity-adjudication layer is permanently COLLECTION_ONLY/
# actionable=False by VerifiedPromotedIdentityAdjudication's own explicit
# contract, so it is not, and never will be, "actionable" -- exactly the
# realistic case this bridge must support. No test in this module mutates
# or copies its readiness/actionable fields: they are used exactly as the
# real graph produced them, and the orchestrator no longer gates on them.
_FIXTURE_ROOT_HANDLE = tempfile.TemporaryDirectory()
_FIXTURE_ROOT = Path(_FIXTURE_ROOT_HANDLE.name)
_GRAPH_STORES, _SPEC, _GRAPH_ROOTS = _build_fixture_and_stores(_FIXTURE_ROOT / "graph")
_GRAPH_MANIFEST = PromotedGraphPublisher().publish(_SPEC, _GRAPH_STORES)
_ADJUSTMENT = _GRAPH_STORES.corporate_action_adjustments.get(
    _GRAPH_MANIFEST.adjustment_bridge_id
)


def _fresh_stores(tmp_root: Path) -> PromotedResearchStores:
    """Wire one full, real store composition, reusing the shared graph roots
    but rooted at a fresh engine-run/research-run directory. Builds the
    promoted graph exactly once (already durable on disk) and composes the
    engine's downstream stores from that graph's own stores and replay
    scope -- never a second, independent upstream resolver graph."""

    return build_promoted_research_stores(
        reference_root=_GRAPH_ROOTS["reference_root"],
        identity_evidence_root=_GRAPH_ROOTS["identity_evidence_root"],
        calendar_root=_GRAPH_ROOTS["calendar_root"],
        daily_reports_root=_GRAPH_ROOTS["daily_reports_root"],
        historical_corpus_root=_GRAPH_ROOTS["historical_corpus_root"],
        promoted_root=_GRAPH_ROOTS["promoted_root"],
        graph_publication_root=_GRAPH_ROOTS["publication_root"],
        engine_run_root=tmp_root / "engine-runs",
        research_run_root=tmp_root / "research-runs",
    )


def _base_request(**overrides: object) -> PromotedResearchRunRequest:
    values: dict[str, object] = dict(
        graph_manifest_id=_GRAPH_MANIFEST.manifest_id,
        signal_session=_ADJUSTMENT.signal_session,
        entry_session=_ADJUSTMENT.signal_session + timedelta(days=1),
        cutoff=_GRAPH_MANIFEST.cutoff,
        initial_capital=Decimal("1000000"),
        technical_config=_TECHNICAL_CONFIG,
        cross_section_config=_CROSS_SECTION_CONFIG,
        intent_config=_INTENT_CONFIG,
    )
    values.update(overrides)
    return PromotedResearchRunRequest(**values)  # type: ignore[arg-type]


class _PoisonResolver:
    """A resolver that fails loudly if it is ever called."""

    def get(self, identity: str) -> object:
        raise AssertionError(f"unexpected resolve of {identity!r}")


class _StubGraphPublications:
    def __init__(self, manifest: object) -> None:
        self._manifest = manifest

    def get(self, manifest_id: str) -> object:
        if self._manifest is None:
            raise PromotedResearchNotFound("not found")
        return self._manifest


class _CountingResolver:
    def __init__(self, target: object) -> None:
        self.target = target
        self.calls = 0

    def get(self, identity: str) -> object:
        self.calls += 1
        return self.target.get(identity)


class PromotedResearchRunRequestTests(unittest.TestCase):
    def test_entry_session_must_be_after_signal_session(self) -> None:
        with self.assertRaises(PromotedResearchError):
            PromotedResearchRunRequest(
                graph_manifest_id="a" * 64,
                signal_session=date(2026, 7, 16),
                entry_session=date(2026, 7, 16),
                cutoff=datetime(2026, 7, 17, tzinfo=UTC),
                initial_capital=Decimal("1000"),
                technical_config=_TECHNICAL_CONFIG,
                cross_section_config=PromotedCrossSectionConfig(),
                intent_config=PromotedIntentPolicyConfig(),
            )

    def test_naive_cutoff_is_rejected(self) -> None:
        with self.assertRaises(PromotedResearchError):
            PromotedResearchRunRequest(
                graph_manifest_id="a" * 64,
                signal_session=date(2026, 7, 16),
                entry_session=date(2026, 7, 17),
                cutoff=datetime(2026, 7, 17, 0, 0),
                initial_capital=Decimal("1000"),
                technical_config=PromotedTechnicalFeatureConfig(),
                cross_section_config=PromotedCrossSectionConfig(),
                intent_config=PromotedIntentPolicyConfig(),
            )

    def test_non_positive_capital_is_rejected(self) -> None:
        with self.assertRaises(PromotedResearchError):
            PromotedResearchRunRequest(
                graph_manifest_id="a" * 64,
                signal_session=date(2026, 7, 16),
                entry_session=date(2026, 7, 17),
                cutoff=datetime(2026, 7, 17, tzinfo=UTC),
                initial_capital=Decimal("0"),
                technical_config=PromotedTechnicalFeatureConfig(),
                cross_section_config=PromotedCrossSectionConfig(),
                intent_config=PromotedIntentPolicyConfig(),
            )

    def test_request_never_exposes_a_separate_engine_root_pin(self) -> None:
        names = {value.name for value in fields(PromotedResearchRunRequest)}
        for banned in (
            "adjustment_bridge_id",
            "effective_tick_panel_id",
            "expected_reference_promotion_ids",
            "expected_corporate_action_snapshot_id",
        ):
            self.assertNotIn(banned, names)

    def test_opaque_research_request_id_cannot_survive_direct_construction(
        self,
    ) -> None:
        valid = _base_request()
        with self.assertRaises(PromotedResearchError):
            object.__setattr__(valid, "research_request_id", "f" * 64)
            valid.verify_content_identity()


class PromotedResearchRunManifestTests(unittest.TestCase):
    def _valid_kwargs(self) -> dict[str, object]:
        request = _base_request()
        return dict(
            schema_version="promoted-research-run-manifest/v1",
            research_request_id=request.research_request_id,
            graph_manifest_id=request.graph_manifest_id,
            graph_spec_id="a" * 64,
            adjustment_bridge_id="b" * 64,
            effective_tick_panel_id="c" * 64,
            expected_reference_promotion_ids=("d" * 64,),
            expected_corporate_action_snapshot_id="e" * 64,
            engine_request_id="f" * 64,
            engine_run_id="1" * 64,
            feature_input_panel_id="2" * 64,
            technical_config_id=request.technical_config.config_id,
            technical_panel_id="3" * 64,
            cross_section_config_id=request.cross_section_config.config_id,
            cross_section_panel_id="4" * 64,
            intent_config_id=request.intent_config.config_id,
            research_intent_batch_id="5" * 64,
            replay_run_id="6" * 64,
            signal_session=request.signal_session,
            entry_session=request.entry_session,
            cutoff=request.cutoff,
            initial_capital=request.initial_capital,
            candidate_count=1,
            intent_count=0,
            adjustment_readiness=ReferenceReadiness.COLLECTION_ONLY,
            adjustment_actionable=False,
            effective_tick_readiness=ReferenceReadiness.COLLECTION_ONLY,
            effective_tick_actionable=False,
            paper_only=True,
            notification_eligible=False,
            execution_eligible=False,
        )

    def test_valid_manifest_round_trips_through_codec(self) -> None:
        manifest = PromotedResearchRunManifest(**self._valid_kwargs())
        payload = encode_promoted_research_run_manifest(manifest)
        replayed = decode_promoted_research_run_manifest(payload)
        self.assertEqual(replayed, manifest)
        self.assertEqual(replayed.research_run_id, manifest.research_run_id)

    def test_opaque_research_request_id_mismatch_is_rejected(self) -> None:
        kwargs = self._valid_kwargs()
        kwargs["research_request_id"] = "9" * 64
        with self.assertRaises(PromotedResearchError):
            PromotedResearchRunManifest(**kwargs)

    def test_paper_only_must_be_true(self) -> None:
        kwargs = self._valid_kwargs()
        kwargs["paper_only"] = False
        with self.assertRaises(PromotedResearchError):
            PromotedResearchRunManifest(**kwargs)

    def test_notification_eligible_must_be_false(self) -> None:
        kwargs = self._valid_kwargs()
        kwargs["notification_eligible"] = True
        with self.assertRaises(PromotedResearchError):
            PromotedResearchRunManifest(**kwargs)

    def test_execution_eligible_must_be_false(self) -> None:
        kwargs = self._valid_kwargs()
        kwargs["execution_eligible"] = True
        with self.assertRaises(PromotedResearchError):
            PromotedResearchRunManifest(**kwargs)

    def test_intent_count_cannot_exceed_candidate_count(self) -> None:
        kwargs = self._valid_kwargs()
        kwargs["candidate_count"] = 1
        kwargs["intent_count"] = 2
        with self.assertRaises(PromotedResearchError):
            PromotedResearchRunManifest(**kwargs)

    def test_manifest_accepts_a_non_actionable_collection_only_graph_projection(
        self,
    ) -> None:
        """Readiness/actionable are preserved projections, not a gate: a
        manifest binding a COLLECTION_ONLY/non-actionable graph is fully
        valid on its own terms (matching the real graph fixture's own,
        permanent state)."""

        manifest = PromotedResearchRunManifest(**self._valid_kwargs())
        self.assertEqual(manifest.adjustment_readiness, ReferenceReadiness.COLLECTION_ONLY)
        self.assertFalse(manifest.adjustment_actionable)
        self.assertTrue(manifest.paper_only)
        self.assertFalse(manifest.notification_eligible)
        self.assertFalse(manifest.execution_eligible)

    def test_malformed_canonical_bytes_are_rejected(self) -> None:
        with self.assertRaises(PromotedResearchError):
            decode_promoted_research_run_manifest(b"not json")

    def test_duplicate_key_payload_is_rejected(self) -> None:
        manifest = PromotedResearchRunManifest(**self._valid_kwargs())
        payload = encode_promoted_research_run_manifest(manifest)
        text = payload.decode("utf-8")
        tampered = text.rstrip("\n").rstrip("}") + ',"schema_version":"x"}\n'
        with self.assertRaises(PromotedResearchError):
            decode_promoted_research_run_manifest(tampered.encode("utf-8"))


class PromotedResearchStoresCapabilityTests(unittest.TestCase):
    def test_no_listing_latest_nearest_find_or_live_capability_exists(self) -> None:
        instance = LocalPromotedResearchRunStore(
            Path("unused"),
            graph_publications=_PoisonResolver(),
            engine_runs=_PoisonResolver(),
            replay_scope=None,  # type: ignore[arg-type]
        )
        public = {name for name in dir(instance) if not name.startswith("_")}
        self.assertEqual(
            public,
            {"path_for", "put", "get", "root", "graph_publications", "engine_runs"},
        )


class BuildPromotedResearchStoresTests(unittest.TestCase):
    def test_real_factory_builds_the_graph_once_and_shares_its_replay_scope(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            roots = dict(
                reference_root=root / "reference",
                identity_evidence_root=root / "identity-evidence",
                calendar_root=root / "calendar",
                daily_reports_root=root / "daily-reports",
                historical_corpus_root=root / "historical-corpus",
                promoted_root=root / "promoted",
                graph_publication_root=root / "graph-publications",
                engine_run_root=root / "engine-runs",
                research_run_root=root / "research-runs",
            )
            stores = build_promoted_research_stores(**roots)

            self.assertIsInstance(stores, PromotedResearchStores)
            self.assertIsInstance(stores.engine, PromotedEngineStores)
            self.assertIs(
                stores.graph_publications,
                stores.research_runs.graph_publications,
            )
            self.assertIs(
                stores.engine.engine_runs,
                stores.research_runs.engine_runs,
            )
            # The engine's own downstream stores were composed from the
            # graph's own adjustment/effective-tick stores directly -- no
            # second, independent upstream resolver graph was built.
            self.assertIs(
                stores.engine.corporate_action_adjustments,
                stores.graph_publications.corporate_action_adjustments,
            )
            self.assertIs(
                stores.engine.effective_session_ticks,
                stores.graph_publications.effective_session_ticks,
            )
            # Every store shares exactly one operation-scoped replay cache.
            self.assertIs(stores._replay_scope, stores.engine._replay_scope)
            self.assertIs(stores._replay_scope, stores.research_runs._replay_scope)

            expected_graph_stores = build_promoted_graph_stores(
                reference_root=roots["reference_root"],
                identity_evidence_root=roots["identity_evidence_root"],
                calendar_root=roots["calendar_root"],
                daily_reports_root=roots["daily_reports_root"],
                historical_corpus_root=roots["historical_corpus_root"],
                promoted_root=roots["promoted_root"],
                publication_root=roots["graph_publication_root"],
            )
            self.assertEqual(
                type(stores.graph_publications), type(expected_graph_stores.publications)
            )
            self.assertEqual(
                stores.graph_publications.root, expected_graph_stores.publications.root
            )
            self.assertEqual(
                stores.engine.engine_runs.root, roots["engine_run_root"] / "promoted-engine-runs"
            )
            self.assertEqual(
                stores.research_runs.root, roots["research_run_root"] / "promoted-research-runs"
            )


class PromotedResearchRunStoreAdversarialTests(unittest.TestCase):
    def _real_manifest(self) -> tuple[PromotedResearchRunManifest, PromotedResearchStores, Path]:
        tmp = tempfile.TemporaryDirectory()
        root = Path(tmp.name)
        self.addCleanup(tmp.cleanup)
        stores = _fresh_stores(root)
        request = _base_request()
        manifest = PromotedResearchOrchestrator().run(request, stores)
        return manifest, stores, root

    def test_missing_research_run_is_not_found(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            stores = _fresh_stores(Path(tmp))
            with self.assertRaises(PromotedResearchNotFound):
                stores.research_runs.get("0" * 64)

    def test_self_consistent_tamper_then_create_once_conflict(self) -> None:
        """Shares one real manifest+stores build across two adversarial
        checks -- an in-memory tamper check first, then a destructive
        create-once-conflict check last -- rather than paying the full
        graph+engine cost twice for two independent assertions."""

        manifest, stores, _root = self._real_manifest()

        tampered = PromotedResearchRunManifest(
            **{
                **{
                    value.name: getattr(manifest, value.name)
                    for value in fields(manifest)
                    if value.name != "research_run_id"
                },
                "adjustment_bridge_id": "9" * 64,
            }
        )
        self.assertNotEqual(tampered.research_run_id, manifest.research_run_id)
        tampered.verify_content_identity()
        with self.assertRaises(PromotedResearchConflict):
            stores.research_runs._verify_downstream(tampered)

        path = stores.research_runs.path_for(manifest.research_run_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b'{"not":"the same payload"}\n')
        with self.assertRaises(PromotedResearchConflict):
            stores.research_runs.put(manifest)


class PromotedResearchOrchestratorFakeRejectionTests(unittest.TestCase):
    def test_missing_graph_fails_closed_before_touching_engine(self) -> None:
        request = _base_request(graph_manifest_id="0" * 64)
        stores = PromotedResearchStores(
            graph_publications=_StubGraphPublications(None),
            engine=_PoisonResolver(),  # type: ignore[arg-type]
            research_runs=_PoisonResolver(),  # type: ignore[arg-type]
            _replay_scope=_UnscopedReplay(),
        )
        with self.assertRaises(PromotedResearchError):
            PromotedResearchOrchestrator().run(request, stores)

    def test_wrong_type_resolved_graph_fails_closed(self) -> None:
        request = _base_request(graph_manifest_id="0" * 64)
        stores = PromotedResearchStores(
            graph_publications=_StubGraphPublications({"not": "a manifest"}),
            engine=_PoisonResolver(),  # type: ignore[arg-type]
            research_runs=_PoisonResolver(),  # type: ignore[arg-type]
            _replay_scope=_UnscopedReplay(),
        )
        with self.assertRaises(PromotedResearchError):
            PromotedResearchOrchestrator().run(request, stores)

    def test_type_mismatch_request_is_rejected(self) -> None:
        with self.assertRaises(TypeError):
            PromotedResearchOrchestrator().run(
                object(),  # type: ignore[arg-type]
                PromotedResearchStores(
                    graph_publications=_StubGraphPublications(None),
                    engine=_PoisonResolver(),  # type: ignore[arg-type]
                    research_runs=_PoisonResolver(),  # type: ignore[arg-type]
                    _replay_scope=_UnscopedReplay(),
                ),
            )

    def test_type_mismatch_stores_is_rejected(self) -> None:
        with self.assertRaises(TypeError):
            PromotedResearchOrchestrator().run(_base_request(), object())  # type: ignore[arg-type]


class _UnscopedReplay:
    """A no-op stand-in for ExactReplayScope used only where the fake
    rejection path never reaches any resolver call (missing graph / wrong
    type / type-mismatch tests above all fail before any caching would
    matter)."""

    def open(self):
        from contextlib import nullcontext

        return nullcontext()


class PromotedResearchOrchestratorRealRejectionTests(unittest.TestCase):
    """One shared real store composition backs all three rejection checks
    below: none of them mutate state the others depend on (the root-
    substitution check writes a new, differently-ID'd manifest file), so
    sharing one build avoids paying the real graph-resolution cost three
    separate times for three independent failure modes."""

    def test_wrong_session_future_cutoff_and_root_substitution_fail_closed(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            stores = _fresh_stores(Path(tmp))

            wrong_session_request = _base_request(
                signal_session=_ADJUSTMENT.signal_session - timedelta(days=1),
                entry_session=_ADJUSTMENT.signal_session + timedelta(days=1),
            )
            with self.assertRaises(PromotedResearchError):
                PromotedResearchOrchestrator().run(wrong_session_request, stores)

            future_cutoff_request = _base_request(
                cutoff=datetime(2020, 1, 1, tzinfo=UTC)
            )
            with self.assertRaises(PromotedResearchError):
                PromotedResearchOrchestrator().run(future_cutoff_request, stores)

            substituted_snapshot_id = "f" * 64
            self.assertNotEqual(
                substituted_snapshot_id, _GRAPH_MANIFEST.corporate_action_snapshot_id
            )
            kwargs = {
                value.name: getattr(_GRAPH_MANIFEST, value.name)
                for value in fields(_GRAPH_MANIFEST)
                if value.name != "manifest_id" and value.name != "spec_id"
            }
            kwargs["corporate_action_snapshot_id"] = substituted_snapshot_id
            tampered_spec_id = _compute_spec_id(
                promotion_bindings=kwargs["promotion_bindings"],
                identity_evidence_artifact_ids=kwargs["identity_evidence_artifact_ids"],
                identity_review_bundle_ids=kwargs["identity_review_bundle_ids"],
                calendar_materialization_id=kwargs["calendar_materialization_id"],
                session_bindings=kwargs["session_bindings"],
                corporate_action_snapshot_id=substituted_snapshot_id,
                cutoff=kwargs["cutoff"],
            )
            tampered = PromotedGraphPublicationManifest(
                spec_id=tampered_spec_id, **kwargs
            )
            self.assertNotEqual(tampered.manifest_id, _GRAPH_MANIFEST.manifest_id)

            payload = encode_promoted_graph_publication_manifest(tampered)
            path = _GRAPH_STORES.publications.path_for(tampered.manifest_id)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(payload)

            root_substitution_request = _base_request(
                graph_manifest_id=tampered.manifest_id
            )
            with self.assertRaises(PromotedResearchError):
                PromotedResearchOrchestrator().run(root_substitution_request, stores)


class PromotedResearchOrchestratorAcceptanceTests(unittest.TestCase):
    """Exactly one test in this module executes the entire real graph ->
    real PromotedEngineRunner -> combined-manifest path from a fresh call
    (plus one repeat and one fresh-store replay against the same durable
    roots) -- happy path, idempotent retry, fresh-restart, combined-lineage
    replay, and per-operation call-count coverage are consolidated into
    this one test rather than each independently rebuilding the same
    expensive real fixture from scratch."""

    def test_happy_path_idempotent_restart_lineage_and_call_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stores = _fresh_stores(root)
            request = _base_request()
            manifest = PromotedResearchOrchestrator().run(request, stores)

            self.assertTrue(manifest.paper_only)
            self.assertFalse(manifest.notification_eligible)
            self.assertFalse(manifest.execution_eligible)
            self.assertEqual(manifest.graph_manifest_id, _GRAPH_MANIFEST.manifest_id)
            self.assertEqual(manifest.graph_spec_id, _GRAPH_MANIFEST.spec_id)
            self.assertEqual(
                manifest.adjustment_bridge_id, _GRAPH_MANIFEST.adjustment_bridge_id
            )
            self.assertEqual(
                manifest.effective_tick_panel_id, _GRAPH_MANIFEST.effective_tick_panel_id
            )
            self.assertEqual(
                manifest.expected_reference_promotion_ids,
                tuple(
                    sorted(
                        value.promotion_id
                        for value in _GRAPH_MANIFEST.promotion_bindings
                    )
                ),
            )
            self.assertGreater(manifest.candidate_count, 0)
            self.assertGreaterEqual(manifest.intent_count, 0)
            self.assertLessEqual(manifest.intent_count, manifest.candidate_count)
            # The graph's real, permanently collection-only/non-actionable
            # readiness is preserved exactly, never upgraded.
            self.assertEqual(
                manifest.adjustment_readiness, _GRAPH_MANIFEST.adjustment_readiness
            )
            self.assertEqual(
                manifest.adjustment_actionable, _GRAPH_MANIFEST.adjustment_actionable
            )
            self.assertEqual(
                manifest.effective_tick_readiness,
                _GRAPH_MANIFEST.effective_tick_readiness,
            )
            self.assertEqual(
                manifest.effective_tick_actionable,
                _GRAPH_MANIFEST.effective_tick_actionable,
            )

            # Idempotent retry: same request+stores, same terminal ID.
            second = PromotedResearchOrchestrator().run(request, stores)
            self.assertEqual(manifest, second)
            self.assertEqual(manifest.research_run_id, second.research_run_id)

            # Fresh-process restart and combined-lineage replay: brand-new
            # store objects rooted at the same durable paths independently
            # re-verify the entire graph+engine+combined chain from scratch.
            restarted_stores = _fresh_stores(root)
            replayed = restarted_stores.research_runs.get(manifest.research_run_id)
            self.assertEqual(replayed, manifest)

            # Call-count: within one operation each exact graph/engine
            # ancestor is resolved at most once through the shared scope;
            # a second, separate top-level get performs fresh resolver
            # calls again -- caching never survives past one operation.
            counting_graph = _CountingResolver(
                restarted_stores.research_runs.graph_publications
            )
            counting_engine = _CountingResolver(
                restarted_stores.research_runs.engine_runs
            )
            probe = LocalPromotedResearchRunStore(
                restarted_stores.research_runs.root.parent,
                graph_publications=counting_graph,
                engine_runs=counting_engine,
                replay_scope=restarted_stores._replay_scope,
            )
            probe.get(manifest.research_run_id)
            self.assertEqual(counting_graph.calls, 1)
            self.assertEqual(counting_engine.calls, 1)
            probe.get(manifest.research_run_id)
            self.assertEqual(counting_graph.calls, 2)
            self.assertEqual(counting_engine.calls, 2)


if __name__ == "__main__":
    unittest.main()
