from __future__ import annotations

import inspect
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path

from india_swing.corporate_actions.models import CorporateActionSnapshot
from india_swing.forward_paper import operational as operational_module
from india_swing.forward_paper.operational import (
    ForwardPaperOperationalGraphError,
    assemble_forward_paper_operational_research_graph,
)
from india_swing.reference.models import ReferenceReadiness
from india_swing.tick_sizes.effective_session import PromotedEffectiveSessionTickService

from tests.test_forward_paper_history import _window_for
from tests.test_nse_archive_research_dataset import _baseline_dataset, _fake_sha256
from tests.test_nse_archive_research_identity import _record, _session
from tests.test_promoted_feature_inputs import _panels


def _operational_artifacts(root: Path):
    _, _, tick_panel, _ = _panels(root)
    dataset = _baseline_dataset()
    verified = tuple(
        value for value in tick_panel.results if value.tick_specification is not None
    )
    assert verified
    first = verified[0]
    tick_entry = first.source_observation.tick_entry
    assert tick_entry is not None
    isin = tick_entry.frame_entry.universe_entry.validated_isin
    signal_session = first.market_session
    dates = tuple(
        signal_session - timedelta(days=59 - index) for index in range(60)
    )
    replay_sessions = tuple(
        _session(
            market_session,
            (_record(market_session, symbol="OPTEST", validated_isin=isin),),
            dataset_id=dataset.dataset_id,
        )
        for market_session in dates
    )
    cutoff = tick_panel.knowledge_time + timedelta(days=120)
    raw, _ = _window_for(
        dataset,
        replay_sessions,
        dates,
        dataset_id=dataset.dataset_id,
        decision_cutoff=cutoff,
    )
    snapshot = CorporateActionSnapshot(
        cutoff=cutoff - timedelta(seconds=1),
        coverage_start=dates[0],
        coverage_end=dates[-1],
        source_artifact_ids=(_fake_sha256("operational-action-source"),),
        events=(),
        readiness=ReferenceReadiness.POINT_IN_TIME_VERIFIED,
        complete=True,
        actionable=True,
        reason_codes=(),
    )
    return raw, snapshot, tick_panel, first


class ForwardPaperOperationalResearchGraphTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temporary = tempfile.TemporaryDirectory()
        cls.artifacts = _operational_artifacts(Path(cls.temporary.name))

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temporary.cleanup()

    def _artifacts(self):
        return self.artifacts

    def test_derives_identity_and_signal_tick_input_from_pinned_panel(self) -> None:
        raw, snapshot, tick_panel, first = self._artifacts()
        graph = assemble_forward_paper_operational_research_graph(
            source_window=raw,
            corporate_actions=snapshot,
            tick_panel=tick_panel,
        )
        self.assertEqual(len(graph.identity_bindings), 1)
        binding = graph.identity_bindings[0]
        self.assertEqual(binding.stable_instrument_id, first.stable_instrument_id)
        self.assertEqual(binding.stable_listing_id, first.stable_listing_id)
        self.assertEqual(binding.source_artifact_id, tick_panel.panel_id)
        self.assertTrue(graph.tick_specifications)
        self.assertTrue(
            all(
                value.instrument_id == binding.stable_instrument_id
                and value.listing_id == binding.stable_listing_id
                for value in graph.tick_specifications
            )
        )
        graph.verify_content_identity()

    def test_missing_historical_ticks_do_not_invent_or_veto_signal_input(self) -> None:
        raw, snapshot, tick_panel, _ = self._artifacts()
        graph = assemble_forward_paper_operational_research_graph(
            source_window=raw,
            corporate_actions=snapshot,
            tick_panel=tick_panel,
        )
        self.assertEqual(graph.adjusted_window.adjusted_candidate_count, 1)
        self.assertEqual(graph.feature_input_window.assembled_candidate_count, 1)
        self.assertEqual(graph.feature_input_window.veto_count, 0)
        self.assertEqual(graph.technical_feature_window.computed_feature_count, 1)
        self.assertTrue(graph.resolved_histories_feature_complete)
        candidate = graph.feature_input_window.outcomes[0]
        self.assertTrue(
            all(value.tick_specification is None for value in candidate.bars[:-1])
        )
        self.assertIsNotNone(candidate.bars[-1].tick_specification)

    def test_same_exact_inputs_produce_same_graph_identity(self) -> None:
        raw, snapshot, tick_panel, _ = self._artifacts()
        first = assemble_forward_paper_operational_research_graph(
            source_window=raw,
            corporate_actions=snapshot,
            tick_panel=tick_panel,
        )
        second = assemble_forward_paper_operational_research_graph(
            source_window=raw,
            corporate_actions=snapshot,
            tick_panel=tick_panel,
        )
        self.assertEqual(first.graph_id, second.graph_id)

    def test_future_known_tick_panel_fails_closed(self) -> None:
        raw, snapshot, tick_panel, _ = self._artifacts()
        future_panel = PromotedEffectiveSessionTickService().materialize(
            source_panel=tick_panel.source_panel,
            cutoff=raw.spec.decision_cutoff + timedelta(seconds=1),
        )
        with self.assertRaises(ForwardPaperOperationalGraphError):
            assemble_forward_paper_operational_research_graph(
                source_window=raw,
                corporate_actions=snapshot,
                tick_panel=future_panel,
            )

    def test_output_remains_collection_only(self) -> None:
        raw, snapshot, tick_panel, _ = self._artifacts()
        graph = assemble_forward_paper_operational_research_graph(
            source_window=raw,
            corporate_actions=snapshot,
            tick_panel=tick_panel,
        )
        self.assertTrue(graph.collection_only)
        for name in (
            "training_eligible",
            "ranking_eligible",
            "alert_eligible",
            "paper_trade_eligible",
            "notification_eligible",
            "execution_eligible",
        ):
            self.assertFalse(getattr(graph, name))

    def test_module_has_no_io_clock_cloud_provider_or_execution_capability(self) -> None:
        source = inspect.getsource(operational_module).lower()
        for token in (
            "builtins.open(",
            "path(",
            "os.environ",
            "datetime.now(",
            "requests.",
            "kite",
            "telegram",
            "gcs.",
            "place_order",
            "send_alert",
            "kronos",
        ):
            self.assertNotIn(token, source)


if __name__ == "__main__":
    unittest.main()
