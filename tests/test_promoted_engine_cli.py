from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from unittest import mock

from india_swing import promoted_engine_cli
from india_swing.corporate_actions.promoted_adjustments import (
    PromotedCorporateActionAdjustmentService,
)
from india_swing.evaluation.promoted_intent_store import (
    LocalPromotedResearchIntentStore,
)
from india_swing.features.store import (
    LocalPromotedCrossSectionStore,
    LocalPromotedFeatureInputStore,
    LocalPromotedTechnicalFeatureStore,
)
from india_swing.promoted_engine import (
    LocalPromotedEngineRunStore,
    PromotedEngineStores,
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
from india_swing.tick_sizes.effective_session import (
    PromotedEffectiveSessionTickService,
)
from tests.test_promoted_corporate_action_bridge import (
    BRIDGE_CUTOFF,
    _event,
    _snapshot,
)
from tests.test_promoted_stable_listing_history import _two_session_fixture


class _ExactResolver:
    def __init__(self, values, identity) -> None:
        self.values = {identity(value): value for value in values}

    def get(self, identity_value):
        return self.values[identity_value]


# The underlying fixture graph is expensive to materialize and identical,
# immutable content for every test in this module; it is built exactly once
# here and reused (never mutated) by each test's own fresh, disk-backed
# store instances -- matching the same caching approach used by the sibling
# programmatic test module tests/test_promoted_engine.py.
_FIXTURE_ROOT_HANDLE = tempfile.TemporaryDirectory()
_CALENDAR, _SNAPSHOTS, _HISTORY = _two_session_fixture(
    Path(_FIXTURE_ROOT_HANDLE.name)
)
_ACTIONS = _snapshot(_event(_HISTORY))
_ADJUSTMENT = PromotedCorporateActionAdjustmentService().materialize(
    source_panel=_HISTORY, corporate_actions=_ACTIONS, cutoff=BRIDGE_CUTOFF
)
_TICKS = PromotedEffectiveSessionTickService().materialize(
    source_panel=_HISTORY, cutoff=BRIDGE_CUTOFF
)


def _build_stores_for_cli(root: Path) -> tuple[PromotedEngineStores, dict[str, object]]:
    """Build a full PromotedEngineStores using the CLI's own resolver shapes.

    Wires the identical set of classes ``build_promoted_engine_stores``
    would construct for feature_inputs/technical_features/cross_sections/
    research_intents/engine_runs and for corporate_action_adjustments/
    effective_session_ticks; the deep identity-graph layer uses in-memory
    resolvers seeded from the same deterministic fixture values, matching
    the equivalent programmatic test in tests/test_promoted_engine.py.
    """

    history = _HISTORY
    actions = _ACTIONS
    adjustment = _ADJUSTMENT
    ticks = _TICKS

    graph_root = root / "graph"
    frames = tuple(value.frame for value in _SNAPSHOTS)
    universes = tuple(value.universe for value in frames)
    adjudication = universes[0].adjudication
    intake = adjudication.intake
    promotions = intake.promotions
    corpora = {
        value.corpus_index.corpus_id: (value.corpus_index, (value.partition,))
        for value in frames
    }
    promotion_resolver = _ExactResolver(promotions, lambda value: value.promotion_id)
    intake_store = LocalPromotedIdentityIntakeStore(graph_root, promotion_resolver)
    evidence_resolver = _ExactResolver(
        adjudication.evidence_artifacts, lambda value: value.manifest.artifact_id
    )
    review_resolver = _ExactResolver(
        adjudication.review_bundles, lambda value: value.manifest.bundle_id
    )
    adjudication_store = LocalPromotedIdentityAdjudicationStore(
        graph_root, intake_store, evidence_resolver, review_resolver
    )
    calendar_resolver = _ExactResolver(
        (_CALENDAR,), lambda value: value.materialization_id
    )
    universe_store = LocalPromotedIdentitySessionUniverseStore(
        graph_root, adjudication_store, calendar_resolver
    )
    corpus_resolver = _ExactResolver(
        tuple(corpora.values()), lambda value: value[0].corpus_id
    )
    frame_store = LocalPromotedSessionMarketDataFrameStore(
        graph_root, universe_store, corpus_resolver
    )
    tick_store = LocalPromotedSessionTickSnapshotStore(graph_root, frame_store)
    history_store = LocalPromotedStableListingHistoryStore(
        graph_root, tick_store, calendar_resolver
    )
    intake_store.put(intake)
    adjudication_store.put(adjudication)
    for universe in universes:
        universe_store.put(universe)
    for frame in frames:
        frame_store.put(frame)
    for snapshot in _SNAPSHOTS:
        tick_store.put(snapshot)
    history_store.put(history)

    action_resolver = _ExactResolver((actions,), lambda value: value.snapshot_id)
    adjustment_store = LocalPromotedCorporateActionAdjustmentStore(
        graph_root, history_store, action_resolver
    )
    effective_store = LocalPromotedEffectiveSessionTickStore(
        graph_root, history_store
    )
    adjustment_store.put(adjustment)
    effective_store.put(ticks)

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
    promotion_ids = tuple(
        sorted(
            {
                promotion.promotion_id
                for snap in history.tick_snapshots
                for promotion in snap.frame.universe.adjudication.intake.promotions
            }
        )
    )
    facts = {
        "adjustment_bridge_id": adjustment.bridge_id,
        "effective_tick_panel_id": ticks.panel_id,
        "corporate_action_snapshot_id": actions.snapshot_id,
        "signal_session": adjustment.signal_session,
        "reference_promotion_ids": promotion_ids,
    }
    return stores, facts


def _run_cli(argv: list[str]) -> tuple[int, dict[str, object]]:
    stdout = io.StringIO()
    with contextlib.redirect_stdout(stdout):
        exit_code = promoted_engine_cli.main(argv)
    return exit_code, json.loads(stdout.getvalue())


def _run_cli_capture_both(argv: list[str]) -> tuple[int, str, str]:
    """Capture both stdout and stderr, returning the raw stdout text unparsed.

    Used for argument-parsing failures, where the bug under correction was
    that argparse printed raw usage text (bypassing the sanitized JSON
    boundary entirely) rather than raising into main's own exception
    handler.
    """

    stdout = io.StringIO()
    stderr = io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        exit_code = promoted_engine_cli.main(argv)
    return exit_code, stdout.getvalue(), stderr.getvalue()


class PromotedEngineCliTests(unittest.TestCase):
    def _argv(self, root: Path, facts: dict[str, object], **overrides) -> list[str]:
        values = {
            "--reference-root": str(root / "unused-reference"),
            "--identity-evidence-root": str(root / "unused-identity-evidence"),
            "--calendar-root": str(root / "unused-calendar"),
            "--daily-reports-root": str(root / "unused-daily-reports"),
            "--historical-corpus-root": str(root / "unused-historical-corpus"),
            "--promoted-root": str(root / "unused-promoted"),
            "--engine-run-root": str(root / "unused-engine-run"),
            "--adjustment-bridge-id": facts["adjustment_bridge_id"],
            "--effective-tick-panel-id": facts["effective_tick_panel_id"],
            "--corporate-action-snapshot-id": facts["corporate_action_snapshot_id"],
            "--signal-session": facts["signal_session"].isoformat(),
            "--entry-session": (
                facts["signal_session"] + timedelta(days=1)
            ).isoformat(),
            "--cutoff": BRIDGE_CUTOFF.isoformat(),
            "--initial-capital": "1000000",
        }
        values.update(overrides)
        argv: list[str] = []
        for key, value in values.items():
            argv.extend([key, str(value)])
        for promotion_id in facts["reference_promotion_ids"]:
            argv.extend(["--reference-promotion-id", promotion_id])
        return argv

    def test_success_path_returns_sanitized_audit_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stores, facts = _build_stores_for_cli(root)
            argv = self._argv(root, facts)
            with mock.patch.object(
                promoted_engine_cli,
                "build_promoted_engine_stores",
                return_value=stores,
            ):
                exit_code, payload = _run_cli(argv)

        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["status"], "COMPLETE")
        self.assertTrue(payload["paper_only"])
        self.assertEqual(payload["signal_session"], facts["signal_session"].isoformat())
        self.assertGreater(payload["candidate_count"], 0)
        # The CLI always uses the default (61-session) technical config,
        # which the tiny two-session fixture cannot satisfy: zero selected
        # intents is the expected, still-successful, auditable outcome.
        self.assertEqual(payload["intent_count"], 0)
        expected_keys = {
            "status",
            "run_id",
            "request_id",
            "adjustment_bridge_id",
            "effective_tick_panel_id",
            "expected_reference_promotion_ids",
            "expected_corporate_action_snapshot_id",
            "feature_input_panel_id",
            "technical_panel_id",
            "cross_section_panel_id",
            "research_intent_batch_id",
            "replay_run_id",
            "technical_config_id",
            "cross_section_config_id",
            "intent_config_id",
            "signal_session",
            "entry_session",
            "cutoff",
            "candidate_count",
            "intent_count",
            "paper_only",
        }
        self.assertEqual(set(payload), expected_keys)
        self.assertEqual(
            payload["expected_reference_promotion_ids"],
            list(facts["reference_promotion_ids"]),
        )
        self.assertEqual(
            payload["expected_corporate_action_snapshot_id"],
            facts["corporate_action_snapshot_id"],
        )

    def test_repeated_invocation_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stores, facts = _build_stores_for_cli(root)
            argv = self._argv(root, facts)
            with mock.patch.object(
                promoted_engine_cli,
                "build_promoted_engine_stores",
                return_value=stores,
            ):
                first_code, first_payload = _run_cli(argv)
                second_code, second_payload = _run_cli(argv)

        self.assertEqual(first_code, 0)
        self.assertEqual(second_code, 0)
        self.assertEqual(first_payload["run_id"], second_payload["run_id"])

    def test_unresolvable_bridge_id_fails_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stores, facts = _build_stores_for_cli(root)
            argv = self._argv(
                root, facts, **{"--adjustment-bridge-id": "0" * 64}
            )
            with mock.patch.object(
                promoted_engine_cli,
                "build_promoted_engine_stores",
                return_value=stores,
            ):
                exit_code, payload = _run_cli(argv)

        self.assertEqual(exit_code, 2)
        self.assertEqual(set(payload), {"status", "error_type"})
        self.assertEqual(payload["status"], "FAILED")

    def test_malformed_cutoff_fails_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stores, facts = _build_stores_for_cli(root)
            argv = self._argv(root, facts, **{"--cutoff": "not-a-datetime"})
            with mock.patch.object(
                promoted_engine_cli,
                "build_promoted_engine_stores",
                return_value=stores,
            ):
                exit_code, payload = _run_cli(argv)

        self.assertEqual(exit_code, 2)
        self.assertEqual(payload["status"], "FAILED")
        self.assertIn("error_type", payload)

    def test_missing_required_argument_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stores, facts = _build_stores_for_cli(root)
            argv = self._argv(root, facts)
            # Drop the --adjustment-bridge-id flag and its value entirely.
            flag_index = argv.index("--adjustment-bridge-id")
            del argv[flag_index : flag_index + 2]
            with mock.patch.object(
                promoted_engine_cli,
                "build_promoted_engine_stores",
                return_value=stores,
            ):
                exit_code, stdout_text, stderr_text = _run_cli_capture_both(argv)

        self.assertEqual(exit_code, 2)
        self.assertEqual(stderr_text, "")
        payload = json.loads(stdout_text)
        self.assertEqual(set(payload), {"status", "error_type"})
        self.assertEqual(payload["status"], "FAILED")
        self.assertNotIn("adjustment-bridge-id", stdout_text)
        self.assertNotIn("usage:", stdout_text.lower())

    def test_unknown_option_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stores, facts = _build_stores_for_cli(root)
            argv = self._argv(root, facts) + [
                "--not-a-real-option",
                "some-value",
            ]
            with mock.patch.object(
                promoted_engine_cli,
                "build_promoted_engine_stores",
                return_value=stores,
            ):
                exit_code, stdout_text, stderr_text = _run_cli_capture_both(argv)

        self.assertEqual(exit_code, 2)
        self.assertEqual(stderr_text, "")
        payload = json.loads(stdout_text)
        self.assertEqual(set(payload), {"status", "error_type"})
        self.assertEqual(payload["status"], "FAILED")
        self.assertNotIn("not-a-real-option", stdout_text)
        self.assertNotIn("usage:", stdout_text.lower())

    def test_no_network_broker_or_discovery_flags_exist(self) -> None:
        parser = promoted_engine_cli.build_parser()
        option_strings = {
            option
            for action in parser._actions
            for option in action.option_strings
        }
        banned = ("list", "latest", "broker", "telegram", "network")
        for option in option_strings:
            lowered = option.lower()
            self.assertFalse(
                any(bad in lowered for bad in banned),
                f"unexpected CLI flag {option!r}",
            )


if __name__ == "__main__":
    unittest.main()
