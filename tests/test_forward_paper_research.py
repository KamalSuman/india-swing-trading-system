from __future__ import annotations

import inspect
import tempfile
import unittest
from decimal import Decimal, getcontext
from pathlib import Path

from india_swing.features.promoted_cross_section import PromotedCrossSectionConfig
from india_swing.forward_paper import research as research_module
from india_swing.forward_paper.operational import (
    assemble_forward_paper_operational_research_graph,
)
from india_swing.forward_paper.research import (
    ForwardPaperResearchError,
    run_forward_paper_baseline_challenger_research,
)

from tests.test_forward_paper_operational import _operational_artifacts


class ForwardPaperBaselineChallengerResearchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temporary = tempfile.TemporaryDirectory()
        raw, snapshot, tick_panel, _ = _operational_artifacts(
            Path(cls.temporary.name)
        )
        cls.graph = assemble_forward_paper_operational_research_graph(
            source_window=raw,
            corporate_actions=snapshot,
            tick_panel=tick_panel,
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temporary.cleanup()

    def _run(self, *, challenger_minimum: int = 1):
        return run_forward_paper_baseline_challenger_research(
            source_graph=self.graph,
            baseline_config=PromotedCrossSectionConfig(
                minimum_computed_instruments=1
            ),
            challenger_config=PromotedCrossSectionConfig(
                minimum_computed_instruments=challenger_minimum,
                high_volatility_threshold=Decimal("0.40"),
            ),
            comparison_top_tiers=10,
        )

    def test_same_exact_graph_runs_both_arms_through_shared_kernel(self) -> None:
        result = self._run()
        self.assertEqual(
            result.baseline.source_window.window_id,
            self.graph.technical_feature_window.window_id,
        )
        self.assertIs(result.baseline.source_window, result.challenger.source_window)
        self.assertEqual(len(result.baseline.opportunities), 1)
        self.assertEqual(len(result.challenger.opportunities), 1)
        self.assertEqual(result.baseline_top_count, 1)
        self.assertEqual(result.challenger_top_count, 1)
        self.assertEqual(result.overlap_count, 1)
        self.assertEqual(len(result.comparisons), 1)
        self.assertNotEqual(result.baseline.arm_id, result.challenger.arm_id)
        result.verify_content_identity()

    def test_insufficient_challenger_cross_section_is_blocked_not_dropped(self) -> None:
        result = self._run(challenger_minimum=2)
        self.assertEqual(len(result.baseline.opportunities), 1)
        self.assertEqual(len(result.challenger.opportunities), 0)
        self.assertEqual(
            len(result.challenger.blocked_result_ids),
            len(self.graph.technical_feature_window.results),
        )
        self.assertEqual(result.challenger_top_count, 0)
        self.assertEqual(result.overlap_count, 0)
        result.verify_content_identity()

    def test_output_has_no_promotion_notification_or_execution_authority(self) -> None:
        result = self._run()
        self.assertTrue(result.collection_only)
        for name in (
            "promotion_eligible",
            "paper_trade_eligible",
            "notification_eligible",
            "execution_eligible",
        ):
            self.assertFalse(getattr(result, name))
        for arm in (result.baseline, result.challenger):
            self.assertTrue(arm.collection_only)
            self.assertFalse(arm.ranking_eligible)
            self.assertFalse(arm.paper_trade_eligible)
            self.assertFalse(arm.notification_eligible)
            self.assertFalse(arm.execution_eligible)

    def test_same_inputs_are_deterministic_across_decimal_contexts(self) -> None:
        baseline = self._run()
        original = getcontext().copy()
        try:
            getcontext().prec = 6
            constrained = self._run()
        finally:
            getcontext().prec = original.prec
            getcontext().rounding = original.rounding
        self.assertEqual(baseline.run_id, constrained.run_id)

    def test_identical_configs_and_bool_top_tier_fail_closed(self) -> None:
        config = PromotedCrossSectionConfig(minimum_computed_instruments=1)
        with self.assertRaises(ForwardPaperResearchError):
            run_forward_paper_baseline_challenger_research(
                source_graph=self.graph,
                baseline_config=config,
                challenger_config=config,
            )
        with self.assertRaises(ForwardPaperResearchError):
            run_forward_paper_baseline_challenger_research(
                source_graph=self.graph,
                baseline_config=config,
                challenger_config=PromotedCrossSectionConfig(
                    minimum_computed_instruments=2
                ),
                comparison_top_tiers=True,
            )

    def test_nested_opportunity_tamper_is_detected(self) -> None:
        result = self._run()
        original = result.baseline.opportunities
        object.__setattr__(result.baseline, "opportunities", ())
        try:
            with self.assertRaises(ForwardPaperResearchError):
                result.baseline.verify_content_identity()
        finally:
            object.__setattr__(result.baseline, "opportunities", original)

    def test_module_has_no_io_clock_cloud_model_or_execution_capability(self) -> None:
        source = inspect.getsource(research_module).lower()
        for forbidden in (
            "import os",
            "import pathlib",
            "open(",
            "google.cloud",
            "datetime.now",
            "telegram",
            "kiteconnect",
            "requests.",
            "subprocess",
            "place_order",
        ):
            self.assertNotIn(forbidden, source)


if __name__ == "__main__":
    unittest.main()
