from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import fields
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest import mock

import india_swing.promoted_operational_preparation as promoted_operational_preparation_module
from india_swing._exact_replay import ExactReplayScope
from india_swing.evaluation.promoted_intents import (
    EvaluationTradeIntent,
    LimitEntryOrder,
    PROMOTED_INTENT_BATCH_SCHEMA_VERSION,
    PROMOTED_INTENT_POLICY_VERSION,
    PromotedCandidateDecision,
    PromotedCandidateDecisionStatus,
    PromotedIntentPolicyConfig,
    PromotedResearchTradeIntent,
    VerifiedPromotedResearchIntentBatch,
)
from india_swing.identity import content_id
from india_swing.promoted_engine import (
    PromotedCrossSectionConfig,
    PromotedEngineRunManifest,
    PromotedTechnicalFeatureConfig,
    _compute_request_id,
)
from india_swing.promoted_graph_publisher import ReferenceReadiness
from india_swing.promoted_operational_preparation import (
    LocalPromotedOperationalPreparationStore,
    PromotedOperationalCandidate,
    PromotedOperationalPreparationConflict,
    PromotedOperationalPreparationError,
    PromotedOperationalPreparationManifest,
    PromotedOperationalPreparationNotFound,
    PromotedOperationalPreparationService,
    VerifiedPromotedOperationalPreparation,
    decode_promoted_operational_preparation_manifest,
    encode_promoted_operational_preparation_manifest,
)
from india_swing.promoted_research_run import (
    PromotedResearchRunManifest,
    _compute_research_request_id,
)

UTC = timezone.utc

_TECHNICAL_CONFIG = PromotedTechnicalFeatureConfig()
_CROSS_SECTION_CONFIG = PromotedCrossSectionConfig()
_INTENT_CONFIG = PromotedIntentPolicyConfig()

_GRAPH_MANIFEST_ID = "1" * 64
_GRAPH_SPEC_ID = "2" * 64
_ADJUSTMENT_BRIDGE_ID = "3" * 64
_EFFECTIVE_TICK_PANEL_ID = "4" * 64
_PROMOTION_ID = "5" * 64
_CORPORATE_ACTION_SNAPSHOT_ID = "6" * 64
_FEATURE_INPUT_PANEL_ID = "7" * 64
_TECHNICAL_PANEL_ID = "8" * 64
_CROSS_SECTION_PANEL_ID = "9" * 64
_REPLAY_RUN_ID = "a" * 64

_SIGNAL_SESSION = date(2026, 7, 16)
_ENTRY_SESSION = date(2026, 7, 17)
_CUTOFF = datetime(2026, 7, 18, tzinfo=UTC)
_INITIAL_CAPITAL = Decimal("1000000")


def _compute_batch_id(
    *,
    source_panel_id: str,
    config_id: str,
    signal_session: date,
    entry_session: date,
    initial_capital: Decimal,
    decision_ids: tuple[str, ...],
    research_intent_ids: tuple[str, ...],
    selected_count: int,
    blocked_count: int,
    source_universe_complete: bool,
    readiness: ReferenceReadiness,
    actionable: bool,
    alert_eligible: bool,
    execution_eligible: bool,
) -> str:
    """Mirrors VerifiedPromotedResearchIntentBatch._calculated_id's exact
    preimage. batch_id is a required (not auto-computed) constructor field
    on that type, so a caller constructing one directly must compute it
    externally with this exact same preimage first."""

    return content_id(
        {
            "schema": PROMOTED_INTENT_BATCH_SCHEMA_VERSION,
            "policy_version": PROMOTED_INTENT_POLICY_VERSION,
            "source_panel_id": source_panel_id,
            "config_id": config_id,
            "signal_session": signal_session,
            "entry_session": entry_session,
            "initial_capital": initial_capital,
            "decision_ids": decision_ids,
            "research_intent_ids": research_intent_ids,
            "selected_count": selected_count,
            "blocked_count": blocked_count,
            "source_universe_complete": source_universe_complete,
            "readiness": readiness,
            "actionable": actionable,
            "alert_eligible": alert_eligible,
            "execution_eligible": execution_eligible,
        },
        length=64,
    )


def _reason_codes(status: PromotedCandidateDecisionStatus) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                status.value,
                "NO_LIVE_ALERT_AUTHORITY",
                "NO_EXECUTION_AUTHORITY",
                "SCORE_IS_NOT_A_PROBABILITY",
            }
        )
    )


def _selected_decision(
    *,
    source_result_id: str,
    source_feature_id: str,
    opportunity_id: str,
    stable_instrument_id: str,
    stable_listing_id: str,
) -> PromotedCandidateDecision:
    return PromotedCandidateDecision(
        source_result_id=source_result_id,
        source_feature_id=source_feature_id,
        opportunity_id=opportunity_id,
        stable_instrument_id=stable_instrument_id,
        stable_listing_id=stable_listing_id,
        signal_session=_SIGNAL_SESSION,
        ensemble_score=Decimal("0.75"),
        rank_tier=1,
        tie_size=1,
        status=PromotedCandidateDecisionStatus.SELECTED_RESEARCH_ONLY,
        reason_codes=_reason_codes(
            PromotedCandidateDecisionStatus.SELECTED_RESEARCH_ONLY
        ),
        selected=True,
    )


def _selected_intent(
    *, decision: PromotedCandidateDecision, symbol: str, universe_snapshot_id: str
) -> PromotedResearchTradeIntent:
    entry_order = LimitEntryOrder(
        symbol=symbol,
        signal_session=_SIGNAL_SESSION,
        first_eligible_session=_ENTRY_SESSION,
        expiry_session=_ENTRY_SESSION,
        quantity=10,
        limit_price=Decimal("100.0"),
        tick_size=Decimal("0.05"),
        maximum_participation=Decimal("0.0025"),
    )
    evaluation_intent = EvaluationTradeIntent(
        signal_id=decision.decision_id,
        universe_snapshot_id=universe_snapshot_id,
        isin="INE002A01018",
        entry_order=entry_order,
        stop_price=Decimal("90.5"),
        target_price=Decimal("130.5"),
        max_holding_sessions=10,
    )
    return PromotedResearchTradeIntent(
        decision_id=decision.decision_id,
        source_cross_section_panel_id=_CROSS_SECTION_PANEL_ID,
        source_feature_id=decision.source_feature_id,
        opportunity_id=decision.opportunity_id,
        stable_instrument_id=decision.stable_instrument_id,
        stable_listing_id=decision.stable_listing_id,
        universe_snapshot_id=universe_snapshot_id,
        evaluation_intent=evaluation_intent,
        estimated_cost_buffer=Decimal("0.5"),
        planned_net_reward_risk=Decimal("3"),
    )


def _build_batch(
    *, decisions, intents
) -> VerifiedPromotedResearchIntentBatch:
    selected_count = sum(1 for value in decisions if value.selected)
    blocked_count = len(decisions) - selected_count
    decision_ids = tuple(value.decision_id for value in decisions)
    research_intent_ids = tuple(value.research_intent_id for value in intents)
    batch_id = _compute_batch_id(
        source_panel_id=_CROSS_SECTION_PANEL_ID,
        config_id=_INTENT_CONFIG.config_id,
        signal_session=_SIGNAL_SESSION,
        entry_session=_ENTRY_SESSION,
        initial_capital=_INITIAL_CAPITAL,
        decision_ids=decision_ids,
        research_intent_ids=research_intent_ids,
        selected_count=selected_count,
        blocked_count=blocked_count,
        source_universe_complete=True,
        readiness=ReferenceReadiness.COLLECTION_ONLY,
        actionable=False,
        alert_eligible=False,
        execution_eligible=False,
    )
    return VerifiedPromotedResearchIntentBatch(
        schema_version=PROMOTED_INTENT_BATCH_SCHEMA_VERSION,
        policy_version=PROMOTED_INTENT_POLICY_VERSION,
        source_panel_id=_CROSS_SECTION_PANEL_ID,
        config_id=_INTENT_CONFIG.config_id,
        signal_session=_SIGNAL_SESSION,
        entry_session=_ENTRY_SESSION,
        initial_capital=_INITIAL_CAPITAL,
        decisions=decisions,
        intents=intents,
        selected_count=selected_count,
        blocked_count=blocked_count,
        source_universe_complete=True,
        readiness=ReferenceReadiness.COLLECTION_ONLY,
        actionable=False,
        alert_eligible=False,
        execution_eligible=False,
        batch_id=batch_id,
    )


def _build_nonempty_batch() -> VerifiedPromotedResearchIntentBatch:
    decision_one = _selected_decision(
        source_result_id="e" * 64,
        source_feature_id="b1" * 32,
        opportunity_id="f" * 64,
        stable_instrument_id="c1" * 32,
        stable_listing_id="d1" * 32,
    )
    decision_two = _selected_decision(
        source_result_id="0" * 64,
        source_feature_id="b2" * 32,
        opportunity_id="12" * 32,
        stable_instrument_id="c2" * 32,
        stable_listing_id="d2" * 32,
    )
    intent_one = _selected_intent(
        decision=decision_one, symbol="RELIANCE", universe_snapshot_id="3" * 64
    )
    intent_two = _selected_intent(
        decision=decision_two, symbol="TCS", universe_snapshot_id="4" * 64
    )
    return _build_batch(
        decisions=(decision_one, decision_two), intents=(intent_one, intent_two)
    )


def _build_empty_batch() -> VerifiedPromotedResearchIntentBatch:
    blocked_decision = PromotedCandidateDecision(
        source_result_id="5" * 64,
        source_feature_id=None,
        opportunity_id=None,
        stable_instrument_id=None,
        stable_listing_id=None,
        signal_session=None,
        ensemble_score=None,
        rank_tier=None,
        tie_size=None,
        status=PromotedCandidateDecisionStatus.SOURCE_RESULT_BLOCKED,
        reason_codes=_reason_codes(
            PromotedCandidateDecisionStatus.SOURCE_RESULT_BLOCKED
        ),
        selected=False,
    )
    return _build_batch(decisions=(blocked_decision,), intents=())


def _engine_run_manifest_for(
    batch: VerifiedPromotedResearchIntentBatch,
) -> PromotedEngineRunManifest:
    expected_promotion_ids = (_PROMOTION_ID,)
    request_id = _compute_request_id(
        adjustment_bridge_id=_ADJUSTMENT_BRIDGE_ID,
        effective_tick_panel_id=_EFFECTIVE_TICK_PANEL_ID,
        expected_reference_promotion_ids=expected_promotion_ids,
        expected_corporate_action_snapshot_id=_CORPORATE_ACTION_SNAPSHOT_ID,
        signal_session=_SIGNAL_SESSION,
        entry_session=_ENTRY_SESSION,
        cutoff=_CUTOFF,
        initial_capital=_INITIAL_CAPITAL,
        technical_config_id=_TECHNICAL_CONFIG.config_id,
        cross_section_config_id=_CROSS_SECTION_CONFIG.config_id,
        intent_config_id=_INTENT_CONFIG.config_id,
    )
    return PromotedEngineRunManifest(
        schema_version="promoted-engine-run-manifest/v1",
        request_id=request_id,
        adjustment_bridge_id=_ADJUSTMENT_BRIDGE_ID,
        effective_tick_panel_id=_EFFECTIVE_TICK_PANEL_ID,
        expected_reference_promotion_ids=expected_promotion_ids,
        expected_corporate_action_snapshot_id=_CORPORATE_ACTION_SNAPSHOT_ID,
        feature_input_panel_id=_FEATURE_INPUT_PANEL_ID,
        technical_config_id=_TECHNICAL_CONFIG.config_id,
        technical_panel_id=_TECHNICAL_PANEL_ID,
        cross_section_config_id=_CROSS_SECTION_CONFIG.config_id,
        cross_section_panel_id=_CROSS_SECTION_PANEL_ID,
        intent_config_id=_INTENT_CONFIG.config_id,
        research_intent_batch_id=batch.batch_id,
        replay_run_id=_REPLAY_RUN_ID,
        signal_session=_SIGNAL_SESSION,
        entry_session=_ENTRY_SESSION,
        cutoff=_CUTOFF,
        initial_capital=_INITIAL_CAPITAL,
        candidate_count=len(batch.decisions),
        intent_count=batch.selected_count,
        paper_only=True,
    )


def _research_run_manifest_for(
    engine_run_manifest: PromotedEngineRunManifest,
) -> PromotedResearchRunManifest:
    research_request_id = _compute_research_request_id(
        graph_manifest_id=_GRAPH_MANIFEST_ID,
        signal_session=_SIGNAL_SESSION,
        entry_session=_ENTRY_SESSION,
        cutoff=_CUTOFF,
        initial_capital=_INITIAL_CAPITAL,
        technical_config_id=_TECHNICAL_CONFIG.config_id,
        cross_section_config_id=_CROSS_SECTION_CONFIG.config_id,
        intent_config_id=_INTENT_CONFIG.config_id,
    )
    return PromotedResearchRunManifest(
        schema_version="promoted-research-run-manifest/v1",
        research_request_id=research_request_id,
        graph_manifest_id=_GRAPH_MANIFEST_ID,
        graph_spec_id=_GRAPH_SPEC_ID,
        adjustment_bridge_id=_ADJUSTMENT_BRIDGE_ID,
        effective_tick_panel_id=_EFFECTIVE_TICK_PANEL_ID,
        expected_reference_promotion_ids=(_PROMOTION_ID,),
        expected_corporate_action_snapshot_id=_CORPORATE_ACTION_SNAPSHOT_ID,
        engine_request_id=engine_run_manifest.request_id,
        engine_run_id=engine_run_manifest.run_id,
        feature_input_panel_id=_FEATURE_INPUT_PANEL_ID,
        technical_config_id=_TECHNICAL_CONFIG.config_id,
        technical_panel_id=_TECHNICAL_PANEL_ID,
        cross_section_config_id=_CROSS_SECTION_CONFIG.config_id,
        cross_section_panel_id=_CROSS_SECTION_PANEL_ID,
        intent_config_id=_INTENT_CONFIG.config_id,
        research_intent_batch_id=engine_run_manifest.research_intent_batch_id,
        replay_run_id=_REPLAY_RUN_ID,
        signal_session=_SIGNAL_SESSION,
        entry_session=_ENTRY_SESSION,
        cutoff=_CUTOFF,
        initial_capital=_INITIAL_CAPITAL,
        candidate_count=engine_run_manifest.candidate_count,
        intent_count=engine_run_manifest.intent_count,
        adjustment_readiness=ReferenceReadiness.COLLECTION_ONLY,
        adjustment_actionable=False,
        effective_tick_readiness=ReferenceReadiness.COLLECTION_ONLY,
        effective_tick_actionable=False,
        paper_only=True,
        notification_eligible=False,
        execution_eligible=False,
    )


def _build_lineage(batch: VerifiedPromotedResearchIntentBatch):
    """Build one fully self-consistent, in-memory (no disk I/O) research-
    run/engine-run/intent-batch lineage triple for the given batch. Every ID
    is either an arbitrary valid-shaped placeholder or computed via the
    exact same shared identity functions the production types themselves
    use, so each object is genuinely internally valid on its own terms --
    this is deliberately never resolved from (or written to) any real
    store."""

    engine_run_manifest = _engine_run_manifest_for(batch)
    research_run_manifest = _research_run_manifest_for(engine_run_manifest)
    return research_run_manifest, engine_run_manifest, batch


# Built once at import time and reused, unmodified, by every test in this
# module: constructing these is pure, in-memory dataclass validation (no
# disk I/O, no real promoted graph/engine materialization), so sharing them
# costs nothing per test while still exercising every real production type's
# own __post_init__/verify_content_identity validation.
_NONEMPTY_BATCH = _build_nonempty_batch()
_NONEMPTY_LINEAGE = _build_lineage(_NONEMPTY_BATCH)
_EMPTY_BATCH = _build_empty_batch()
_EMPTY_LINEAGE = _build_lineage(_EMPTY_BATCH)


class _StubResolver:
    def __init__(self, values: dict[str, object]) -> None:
        self._values = values

    def get(self, identity: str) -> object:
        if identity not in self._values:
            raise PromotedOperationalPreparationNotFound(identity)
        return self._values[identity]


def _stub_stores(
    root: Path,
    research_run_manifest: PromotedResearchRunManifest,
    engine_run_manifest: PromotedEngineRunManifest,
    batch: VerifiedPromotedResearchIntentBatch,
    *,
    replay_scope: ExactReplayScope,
) -> LocalPromotedOperationalPreparationStore:
    return LocalPromotedOperationalPreparationStore(
        root,
        research_runs=_StubResolver(
            {research_run_manifest.research_run_id: research_run_manifest}
        ),
        engine_runs=_StubResolver({engine_run_manifest.run_id: engine_run_manifest}),
        research_intents=_StubResolver({batch.batch_id: batch}),
        replay_scope=replay_scope,
    )


class _CountingResolver:
    def __init__(self, target: object) -> None:
        self.target = target
        self.calls = 0

    def get(self, identity: str) -> object:
        self.calls += 1
        return self.target.get(identity)


class PromotedOperationalPreparationTests(unittest.TestCase):
    def test_real_research_run_prepares_exact_candidate_coverage(self) -> None:
        research_run_manifest, engine_run_manifest, batch = _NONEMPTY_LINEAGE
        preparation = PromotedOperationalPreparationService().prepare(
            research_run_manifest=research_run_manifest,
            engine_run_manifest=engine_run_manifest,
            research_intent_batch=batch,
        )
        self.assertEqual(len(preparation.candidates), 2)
        self.assertEqual(preparation.manifest.selected_count, 2)
        self.assertEqual(preparation.manifest.blocked_count, 0)
        self.assertTrue(preparation.manifest.paper_only)
        self.assertFalse(preparation.manifest.notification_eligible)
        self.assertFalse(preparation.manifest.execution_eligible)
        self.assertEqual(
            preparation.manifest.readiness, ReferenceReadiness.COLLECTION_ONLY
        )
        # Exact canonical order preserved from the batch's own intents.
        for candidate, intent, candidate_id, research_intent_id, listing_key in zip(
            preparation.candidates,
            batch.intents,
            preparation.manifest.candidate_ids,
            preparation.manifest.research_intent_ids,
            preparation.manifest.listing_keys,
        ):
            self.assertEqual(candidate.research_intent, intent)
            self.assertEqual(candidate.candidate_id, candidate_id)
            self.assertEqual(candidate.research_intent.research_intent_id, research_intent_id)
            self.assertEqual(candidate.listing_key, listing_key)
            symbol = intent.evaluation_intent.entry_order.symbol
            self.assertEqual(candidate.listing_key, f"NSE:{symbol}")
            self.assertEqual(
                candidate.target_session,
                intent.evaluation_intent.entry_order.first_eligible_session,
            )
        self.assertEqual(
            preparation.manifest.listing_keys, ("NSE:RELIANCE", "NSE:TCS")
        )
        # No value invented beyond the retained intent itself.
        self.assertEqual(
            preparation.candidates[0].research_intent.planned_net_reward_risk,
            Decimal("3"),
        )

    def test_zero_intent_run_is_valid_auditable_empty_preparation(self) -> None:
        research_run_manifest, engine_run_manifest, batch = _EMPTY_LINEAGE
        preparation = PromotedOperationalPreparationService().prepare(
            research_run_manifest=research_run_manifest,
            engine_run_manifest=engine_run_manifest,
            research_intent_batch=batch,
        )
        self.assertEqual(preparation.candidates, ())
        self.assertEqual(preparation.manifest.candidate_ids, ())
        self.assertEqual(preparation.manifest.research_intent_ids, ())
        self.assertEqual(preparation.manifest.listing_keys, ())
        self.assertEqual(preparation.manifest.selected_count, 0)
        self.assertEqual(preparation.manifest.blocked_count, 1)
        self.assertTrue(preparation.manifest.paper_only)
        self.assertFalse(preparation.manifest.notification_eligible)
        self.assertFalse(preparation.manifest.execution_eligible)
        # A zero-candidate preparation still round-trips through the codec.
        payload = encode_promoted_operational_preparation_manifest(
            preparation.manifest
        )
        replayed = decode_promoted_operational_preparation_manifest(payload)
        self.assertEqual(replayed, preparation.manifest)

    def test_mismatched_lineage_and_authority_fail_closed(self) -> None:
        research_run_manifest, engine_run_manifest, batch = _NONEMPTY_LINEAGE

        wrong_engine_run_manifest = _engine_run_manifest_for(_EMPTY_BATCH)
        with self.assertRaises(PromotedOperationalPreparationError):
            PromotedOperationalPreparationService().prepare(
                research_run_manifest=research_run_manifest,
                engine_run_manifest=wrong_engine_run_manifest,
                research_intent_batch=batch,
            )

        wrong_batch = _EMPTY_BATCH
        with self.assertRaises(PromotedOperationalPreparationError):
            PromotedOperationalPreparationService().prepare(
                research_run_manifest=research_run_manifest,
                engine_run_manifest=engine_run_manifest,
                research_intent_batch=wrong_batch,
            )

        # A mismatched listing key fails closed on the candidate's own
        # construction (it independently recomputes the expected key from
        # the retained intent's own entry order).
        intent = batch.intents[0]
        with self.assertRaises(PromotedOperationalPreparationError):
            PromotedOperationalCandidate(
                research_run_id=research_run_manifest.research_run_id,
                research_intent_batch_id=batch.batch_id,
                research_intent=intent,
                listing_key="NSE:WRONG",
                target_session=intent.evaluation_intent.entry_order.first_eligible_session,
            )

        # A self-consistent but tampered manifest field (paper_only=False)
        # must fail closed on its own construction.
        real_preparation = PromotedOperationalPreparationService().prepare(
            research_run_manifest=research_run_manifest,
            engine_run_manifest=engine_run_manifest,
            research_intent_batch=batch,
        )
        tampered_kwargs = {
            value.name: getattr(real_preparation.manifest, value.name)
            for value in fields(real_preparation.manifest)
            if value.name != "preparation_id"
        }
        tampered_kwargs["paper_only"] = False
        with self.assertRaises(PromotedOperationalPreparationError):
            PromotedOperationalPreparationManifest(**tampered_kwargs)

        # A candidate that is individually self-consistent (a validly-
        # shaped, but foreign, research_run_id its own __post_init__ cannot
        # detect in isolation) still fails closed once cross-checked inside
        # VerifiedPromotedOperationalPreparation, which independently
        # re-verifies every candidate against the manifest's own retained
        # research_run_id.
        foreign_candidate = PromotedOperationalCandidate(
            research_run_id="0" * 64,
            research_intent_batch_id=batch.batch_id,
            research_intent=intent,
            listing_key=f"NSE:{intent.evaluation_intent.entry_order.symbol}",
            target_session=intent.evaluation_intent.entry_order.first_eligible_session,
        )
        tampered_candidates = (foreign_candidate,) + real_preparation.candidates[1:]
        with self.assertRaises(PromotedOperationalPreparationError):
            VerifiedPromotedOperationalPreparation(
                research_run_manifest=research_run_manifest,
                engine_run_manifest=engine_run_manifest,
                research_intent_batch=batch,
                manifest=real_preparation.manifest,
                candidates=tampered_candidates,
            )

    def test_candidate_reverification_rejects_self_consistent_field_tampering(
        self,
    ) -> None:
        research_run_manifest, engine_run_manifest, batch = _NONEMPTY_LINEAGE
        intent = batch.intents[0]
        candidate = PromotedOperationalCandidate(
            research_run_id=research_run_manifest.research_run_id,
            research_intent_batch_id=batch.batch_id,
            research_intent=intent,
            listing_key=f"NSE:{intent.evaluation_intent.entry_order.symbol}",
            target_session=intent.evaluation_intent.entry_order.first_eligible_session,
        )
        # Sanity: an untampered candidate verifies cleanly.
        candidate.verify_content_identity()

        # Mutate listing_key via object.__setattr__ (bypassing the frozen
        # dataclass's normal construction path) and recompute a
        # self-consistent candidate_id from the tampered fields -- a naive
        # hash-of-current-fields re-check would accept this.
        tampered_listing_key = PromotedOperationalCandidate(
            research_run_id=candidate.research_run_id,
            research_intent_batch_id=candidate.research_intent_batch_id,
            research_intent=candidate.research_intent,
            listing_key=candidate.listing_key,
            target_session=candidate.target_session,
        )
        object.__setattr__(tampered_listing_key, "listing_key", "NSE:WRONG")
        object.__setattr__(
            tampered_listing_key,
            "candidate_id",
            tampered_listing_key._calculated_id(),
        )
        with self.assertRaises(PromotedOperationalPreparationError):
            tampered_listing_key.verify_content_identity()

        # Same tamper-and-recompute pattern against target_session.
        tampered_session = PromotedOperationalCandidate(
            research_run_id=candidate.research_run_id,
            research_intent_batch_id=candidate.research_intent_batch_id,
            research_intent=candidate.research_intent,
            listing_key=candidate.listing_key,
            target_session=candidate.target_session,
        )
        wrong_session = date(2026, 7, 20)
        self.assertNotEqual(
            wrong_session,
            intent.evaluation_intent.entry_order.first_eligible_session,
        )
        object.__setattr__(tampered_session, "target_session", wrong_session)
        object.__setattr__(
            tampered_session, "candidate_id", tampered_session._calculated_id()
        )
        with self.assertRaises(PromotedOperationalPreparationError):
            tampered_session.verify_content_identity()

    def test_manifest_rejects_duplicate_listing_keys_invalid_chronology_and_non_collection_readiness(
        self,
    ) -> None:
        research_run_manifest, engine_run_manifest, batch = _NONEMPTY_LINEAGE
        preparation = PromotedOperationalPreparationService().prepare(
            research_run_manifest=research_run_manifest,
            engine_run_manifest=engine_run_manifest,
            research_intent_batch=batch,
        )
        valid_kwargs = {
            value.name: getattr(preparation.manifest, value.name)
            for value in fields(preparation.manifest)
            if value.name != "preparation_id"
        }
        # Sanity: the untouched valid kwargs construct cleanly.
        PromotedOperationalPreparationManifest(**valid_kwargs)

        duplicate_listing_kwargs = dict(valid_kwargs)
        duplicate_listing_kwargs["listing_keys"] = (
            valid_kwargs["listing_keys"][0],
            valid_kwargs["listing_keys"][0],
        )
        with self.assertRaises(PromotedOperationalPreparationError):
            PromotedOperationalPreparationManifest(**duplicate_listing_kwargs)

        invalid_chronology_kwargs = dict(valid_kwargs)
        invalid_chronology_kwargs["target_session"] = valid_kwargs["signal_session"]
        with self.assertRaises(PromotedOperationalPreparationError):
            PromotedOperationalPreparationManifest(**invalid_chronology_kwargs)

        non_collection_readiness = next(
            value
            for value in ReferenceReadiness
            if value is not ReferenceReadiness.COLLECTION_ONLY
        )
        non_collection_readiness_kwargs = dict(valid_kwargs)
        non_collection_readiness_kwargs["readiness"] = non_collection_readiness
        with self.assertRaises(PromotedOperationalPreparationError):
            PromotedOperationalPreparationManifest(**non_collection_readiness_kwargs)

    def test_codec_rejects_oversized_encode_and_malformed_payloads(self) -> None:
        research_run_manifest, engine_run_manifest, batch = _NONEMPTY_LINEAGE
        preparation = PromotedOperationalPreparationService().prepare(
            research_run_manifest=research_run_manifest,
            engine_run_manifest=engine_run_manifest,
            research_intent_batch=batch,
        )
        real_payload = encode_promoted_operational_preparation_manifest(
            preparation.manifest
        )

        original_maximum = (
            promoted_operational_preparation_module._MAXIMUM_MANIFEST_BYTES
        )
        promoted_operational_preparation_module._MAXIMUM_MANIFEST_BYTES = (
            len(real_payload) - 1
        )
        try:
            with self.assertRaises(PromotedOperationalPreparationError):
                encode_promoted_operational_preparation_manifest(
                    preparation.manifest
                )
            with self.assertRaises(PromotedOperationalPreparationError):
                decode_promoted_operational_preparation_manifest(real_payload)
        finally:
            promoted_operational_preparation_module._MAXIMUM_MANIFEST_BYTES = (
                original_maximum
            )

        # Sanity: the bound restored, encode/decode succeed again.
        self.assertEqual(
            decode_promoted_operational_preparation_manifest(real_payload),
            preparation.manifest,
        )

        canonical_dict = json.loads(real_payload.decode("utf-8"))
        canonical_text = real_payload.decode("utf-8").rstrip("\n")

        # Duplicate JSON key.
        duplicate_key_text = (
            canonical_text[:-1]
            + f',"schema_version":"{preparation.manifest.schema_version}"'
            + "}"
        )
        with self.assertRaises(PromotedOperationalPreparationError):
            decode_promoted_operational_preparation_manifest(
                (duplicate_key_text + "\n").encode("utf-8")
            )

        # A bare JSON float where an int is expected.
        float_dict = dict(canonical_dict)
        float_dict["selected_count"] = float(canonical_dict["selected_count"])
        float_payload = (
            json.dumps(float_dict, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode("utf-8")
        with self.assertRaises(PromotedOperationalPreparationError):
            decode_promoted_operational_preparation_manifest(float_payload)

        # NaN/Infinity constants.
        nan_dict = dict(canonical_dict)
        nan_dict["selected_count"] = float("nan")
        nan_payload = (
            json.dumps(
                nan_dict, sort_keys=True, separators=(",", ":"), allow_nan=True
            )
            + "\n"
        ).encode("utf-8")
        with self.assertRaises(PromotedOperationalPreparationError):
            decode_promoted_operational_preparation_manifest(nan_payload)

        infinity_dict = dict(canonical_dict)
        infinity_dict["selected_count"] = float("inf")
        infinity_payload = (
            json.dumps(
                infinity_dict,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=True,
            )
            + "\n"
        ).encode("utf-8")
        with self.assertRaises(PromotedOperationalPreparationError):
            decode_promoted_operational_preparation_manifest(infinity_payload)

        # Non-UTF-8 bytes.
        with self.assertRaises(PromotedOperationalPreparationError):
            decode_promoted_operational_preparation_manifest(
                b"\xff\xfe" + real_payload
            )

        # Unknown extra key.
        unknown_key_dict = dict(canonical_dict)
        unknown_key_dict["unexpected_field"] = "x"
        unknown_key_payload = (
            json.dumps(unknown_key_dict, sort_keys=True, separators=(",", ":"))
            + "\n"
        ).encode("utf-8")
        with self.assertRaises(PromotedOperationalPreparationError):
            decode_promoted_operational_preparation_manifest(unknown_key_payload)

        # Missing required key.
        missing_key_dict = dict(canonical_dict)
        del missing_key_dict["preparation_id"]
        missing_key_payload = (
            json.dumps(missing_key_dict, sort_keys=True, separators=(",", ":"))
            + "\n"
        ).encode("utf-8")
        with self.assertRaises(PromotedOperationalPreparationError):
            decode_promoted_operational_preparation_manifest(missing_key_payload)

        # Noncanonical bytes: valid JSON, all correct keys/values, but not
        # byte-identical to the manifest's own canonical re-encoding (extra
        # whitespace after a colon).
        noncanonical_text = canonical_text.replace(
            '"selected_count":', '"selected_count": ', 1
        )
        self.assertNotEqual(noncanonical_text, canonical_text)
        with self.assertRaises(PromotedOperationalPreparationError):
            decode_promoted_operational_preparation_manifest(
                (noncanonical_text + "\n").encode("utf-8")
            )


class LocalPromotedOperationalPreparationStoreTests(unittest.TestCase):
    def test_create_once_round_trip_replays_exact_research_lineage(self) -> None:
        research_run_manifest, engine_run_manifest, batch = _NONEMPTY_LINEAGE
        preparation = PromotedOperationalPreparationService().prepare(
            research_run_manifest=research_run_manifest,
            engine_run_manifest=engine_run_manifest,
            research_intent_batch=batch,
        )
        with tempfile.TemporaryDirectory() as tmp:
            store = _stub_stores(
                Path(tmp),
                research_run_manifest,
                engine_run_manifest,
                batch,
                replay_scope=ExactReplayScope(),
            )
            published = store.put(preparation)
            self.assertEqual(published, preparation)

            # Idempotent repeat.
            again = store.put(preparation)
            self.assertEqual(again, preparation)

            # Fresh get through a brand-new store instance rooted at the
            # same path independently re-verifies the whole lineage.
            restarted = LocalPromotedOperationalPreparationStore(
                store.root.parent,
                research_runs=store.research_runs,
                engine_runs=store.engine_runs,
                research_intents=store.research_intents,
                replay_scope=ExactReplayScope(),
            )
            replayed = restarted.get(preparation.manifest.preparation_id)
            self.assertEqual(replayed, preparation)

            # Conflicting bytes at the same content-derived path fail closed.
            path = store.path_for(preparation.manifest.preparation_id)
            path.write_bytes(b'{"not":"the same payload"}\n')
            with self.assertRaises(PromotedOperationalPreparationConflict):
                store.put(preparation)

            # A missing preparation is not found.
            with self.assertRaises(PromotedOperationalPreparationNotFound):
                store.get("0" * 64)

    def test_get_deduplicates_within_operation_and_is_fresh_across_operations(
        self,
    ) -> None:
        research_run_manifest, engine_run_manifest, batch = _EMPTY_LINEAGE
        preparation = PromotedOperationalPreparationService().prepare(
            research_run_manifest=research_run_manifest,
            engine_run_manifest=engine_run_manifest,
            research_intent_batch=batch,
        )
        with tempfile.TemporaryDirectory() as tmp:
            replay_scope = ExactReplayScope()
            store = _stub_stores(
                Path(tmp),
                research_run_manifest,
                engine_run_manifest,
                batch,
                replay_scope=replay_scope,
            )
            store.put(preparation)

            counting_runs = _CountingResolver(store.research_runs)
            counting_engine = _CountingResolver(store.engine_runs)
            counting_intents = _CountingResolver(store.research_intents)
            probe = LocalPromotedOperationalPreparationStore(
                store.root.parent,
                research_runs=counting_runs,
                engine_runs=counting_engine,
                research_intents=counting_intents,
                replay_scope=replay_scope,
            )
            probe.get(preparation.manifest.preparation_id)
            self.assertEqual(counting_runs.calls, 1)
            self.assertEqual(counting_engine.calls, 1)
            self.assertEqual(counting_intents.calls, 1)

            # A second, separate top-level get performs fresh resolver
            # calls -- caching never survives past one operation.
            probe.get(preparation.manifest.preparation_id)
            self.assertEqual(counting_runs.calls, 2)
            self.assertEqual(counting_engine.calls, 2)
            self.assertEqual(counting_intents.calls, 2)

    def test_store_rejects_unsafe_paths_and_exposes_no_discovery(self) -> None:
        forbidden_substrings = ("list", "latest", "find", "nearest", "discover")
        public_members = [
            name
            for name in dir(LocalPromotedOperationalPreparationStore)
            if not name.startswith("_")
        ]
        self.assertTrue(public_members)
        for name in public_members:
            lowered = name.lower()
            for forbidden in forbidden_substrings:
                self.assertNotIn(
                    forbidden,
                    lowered,
                    f"public member {name!r} exposes a discovery-like operation",
                )

        research_run_manifest, engine_run_manifest, batch = _EMPTY_LINEAGE
        preparation = PromotedOperationalPreparationService().prepare(
            research_run_manifest=research_run_manifest,
            engine_run_manifest=engine_run_manifest,
            research_intent_batch=batch,
        )

        with tempfile.TemporaryDirectory() as tmp:
            store = _stub_stores(
                Path(tmp),
                research_run_manifest,
                engine_run_manifest,
                batch,
                replay_scope=ExactReplayScope(),
            )
            store.put(preparation)
            target_path = store.path_for(preparation.manifest.preparation_id)
            real_path = target_path.with_name(target_path.name + ".real")
            target_path.rename(real_path)
            try:
                target_path.symlink_to(real_path)
            except (OSError, NotImplementedError):
                self.skipTest(
                    "symlinks are not supported on this platform/user; the"
                    " no-discovery surface check above still ran"
                )

            with self.assertRaises(PromotedOperationalPreparationError):
                store.get(preparation.manifest.preparation_id)

        # A store whose root directory is itself a symlink/reparse point is
        # rejected by _publish before any content is written.
        with tempfile.TemporaryDirectory() as another_tmp:
            base = Path(another_tmp)
            real_directory = base / "real-preparations"
            real_directory.mkdir()
            linked_directory = (
                base / LocalPromotedOperationalPreparationStore._DIRECTORY
            )
            linked_directory.symlink_to(real_directory, target_is_directory=True)
            store_with_linked_root = _stub_stores(
                base,
                research_run_manifest,
                engine_run_manifest,
                batch,
                replay_scope=ExactReplayScope(),
            )
            with self.assertRaises(PromotedOperationalPreparationError):
                store_with_linked_root.put(preparation)


class PromotedOperationalPreparationBuilderTests(unittest.TestCase):
    def test_builder_reuses_existing_shared_scope_and_exact_resolvers(
        self,
    ) -> None:
        replay_scope = ExactReplayScope()
        research_runs = _StubResolver({})
        engine_runs = _StubResolver({})
        research_intents = _StubResolver({})

        class _FakeEngineStores:
            def __init__(self) -> None:
                self.engine_runs = engine_runs
                self.research_intents = research_intents

        class _FakeResearchStores:
            def __init__(self) -> None:
                self.research_runs = research_runs
                self.engine = _FakeEngineStores()
                self._replay_scope = replay_scope

        fake_research_stores = _FakeResearchStores()
        captured_kwargs: dict[str, object] = {}

        def _fake_build_promoted_research_stores(**kwargs: object) -> object:
            captured_kwargs.update(kwargs)
            return fake_research_stores

        roots = {
            "reference_root": Path("reference"),
            "identity_evidence_root": Path("identity-evidence"),
            "calendar_root": Path("calendar"),
            "daily_reports_root": Path("daily-reports"),
            "historical_corpus_root": Path("historical-corpus"),
            "promoted_root": Path("promoted"),
            "graph_publication_root": Path("graph-publication"),
            "engine_run_root": Path("engine-run"),
            "research_run_root": Path("research-run"),
        }
        operational_preparation_root = Path("operational-preparation")

        with mock.patch.object(
            promoted_operational_preparation_module,
            "build_promoted_research_stores",
            side_effect=_fake_build_promoted_research_stores,
        ):
            research_stores, preparations = (
                promoted_operational_preparation_module.build_promoted_operational_preparation_store(
                    **roots,
                    operational_preparation_root=operational_preparation_root,
                )
            )

        self.assertIs(research_stores, fake_research_stores)
        self.assertIs(preparations.research_runs, research_runs)
        self.assertIs(preparations.engine_runs, engine_runs)
        self.assertIs(preparations.research_intents, research_intents)
        self.assertIs(preparations._replay_scope, replay_scope)
        self.assertEqual(captured_kwargs, roots)


if __name__ == "__main__":
    unittest.main()
