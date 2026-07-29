from __future__ import annotations

import dataclasses
import tempfile
import unittest
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from india_swing.evaluation.promoted_feature_inputs import (
    PROMOTED_FEATURE_INPUT_POLICY_VERSION,
    PROMOTED_FEATURE_INPUT_SCHEMA_VERSION,
    PromotedFeatureInputError,
    PromotedFeatureInputService,
    PromotedFeatureInputStatus,
    VerifiedPromotedFeatureInputPanel,
)
from india_swing.reference.models import ReferenceReadiness
from india_swing.tick_sizes.effective_session import (
    PromotedEffectiveSessionTickService,
)
from tests.test_promoted_corporate_action_bridge import (
    BRIDGE_CUTOFF,
    _materialize,
)
from tests.test_promoted_identity_session_universe import D2


def _panels(
    root: Path,
    *,
    omit_reliance_bar_on=None,
    conflict_reliance_bar_on=None,
):
    source, _, adjustment = _materialize(
        root,
        omit_reliance_bar_on=omit_reliance_bar_on,
        conflict_reliance_bar_on=conflict_reliance_bar_on,
    )
    ticks = PromotedEffectiveSessionTickService().materialize(
        source_panel=source,
        cutoff=BRIDGE_CUTOFF,
    )
    panel = PromotedFeatureInputService().materialize(
        adjustment_panel=adjustment,
        tick_panel=ticks,
        cutoff=BRIDGE_CUTOFF,
    )
    return source, adjustment, ticks, panel


def _kwargs(panel: VerifiedPromotedFeatureInputPanel) -> dict[str, object]:
    return {
        field.name: getattr(panel, field.name)
        for field in dataclasses.fields(VerifiedPromotedFeatureInputPanel)
    }


class PromotedFeatureInputAcceptanceTests(unittest.TestCase):
    def test_assembles_adjusted_bars_and_exact_session_ticks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source, adjustment, ticks, panel = _panels(Path(tmp))
            self.assertEqual(
                panel.schema_version,
                PROMOTED_FEATURE_INPUT_SCHEMA_VERSION,
            )
            self.assertEqual(
                panel.policy_version,
                PROMOTED_FEATURE_INPUT_POLICY_VERSION,
            )
            self.assertEqual(panel.adjustment_panel.bridge_id, adjustment.bridge_id)
            self.assertEqual(panel.tick_panel.panel_id, ticks.panel_id)
            self.assertEqual(
                panel.adjustment_panel.source_panel.panel_id,
                source.panel_id,
            )
            self.assertTrue(panel.resolved_histories_input_complete)
            self.assertEqual(
                panel.status_counts,
                (("INPUT_GRAPH_ASSEMBLED_COLLECTION_ONLY", 1),),
            )
            result = panel.results[0]
            self.assertIs(
                result.status,
                PromotedFeatureInputStatus.INPUT_GRAPH_ASSEMBLED_COLLECTION_ONLY,
            )
            history = result.input_history
            self.assertIsNotNone(history)
            assert history is not None
            self.assertEqual(
                tuple(value.market_session for value in history.bars),
                source.sessions,
            )
            adjusted = adjustment.results[0].adjusted_history
            self.assertIsNotNone(adjusted)
            assert adjusted is not None
            for input_bar, adjusted_bar, tick_result in zip(
                history.bars,
                adjusted.bars,
                ticks.results,
            ):
                self.assertEqual(input_bar.adjusted_open, adjusted_bar.adjusted_open)
                self.assertEqual(input_bar.adjusted_high, adjusted_bar.adjusted_high)
                self.assertEqual(input_bar.adjusted_low, adjusted_bar.adjusted_low)
                self.assertEqual(input_bar.adjusted_close, adjusted_bar.adjusted_close)
                self.assertEqual(
                    input_bar.adjusted_volume,
                    adjusted_bar.adjusted_volume,
                )
                self.assertEqual(
                    input_bar.tick_size,
                    tick_result.tick_specification.tick_size,
                )
                self.assertEqual(
                    input_bar.market_session,
                    tick_result.market_session,
                )
            panel.verify_content_identity()

    def test_split_adjustment_is_retained_without_computing_features(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, _, panel = _panels(Path(tmp))
        history = panel.results[0].input_history
        assert history is not None
        self.assertEqual(history.bars[0].adjusted_open, Decimal("50.00"))
        self.assertEqual(history.bars[0].adjusted_volume, Decimal("2000"))
        self.assertFalse(hasattr(history.bars[0], "return_value"))
        self.assertFalse(hasattr(history.bars[0], "score"))
        self.assertFalse(hasattr(history.bars[0], "signal"))

    def test_join_lineage_uses_stable_ids_and_market_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, _, panel = _panels(Path(tmp))
        history = panel.results[0].input_history
        assert history is not None
        for value in history.bars:
            self.assertEqual(
                value.stable_instrument_id,
                value.adjusted_bar.stable_instrument_id,
            )
            self.assertEqual(
                value.stable_instrument_id,
                value.tick_result.stable_instrument_id,
            )
            self.assertEqual(
                value.stable_listing_id,
                value.adjusted_bar.stable_listing_id,
            )
            self.assertEqual(
                value.stable_listing_id,
                value.tick_result.stable_listing_id,
            )
            self.assertEqual(
                value.market_session,
                value.adjusted_bar.source_bar.session,
            )
            self.assertEqual(value.market_session, value.tick_result.market_session)

    def test_content_identity_is_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, adjustment, ticks, first = _panels(root)
            second = PromotedFeatureInputService().materialize(
                adjustment_panel=adjustment,
                tick_panel=ticks,
                cutoff=BRIDGE_CUTOFF,
            )
        self.assertEqual(first.panel_id, second.panel_id)
        self.assertEqual(
            first.results[0].result_id,
            second.results[0].result_id,
        )
        self.assertEqual(
            first.results[0].input_history.history_id,
            second.results[0].input_history.history_id,
        )


class PromotedFeatureInputSafetyTests(unittest.TestCase):
    def test_all_authority_flags_remain_false(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, _, panel = _panels(Path(tmp))
        self.assertIs(panel.readiness, ReferenceReadiness.COLLECTION_ONLY)
        self.assertFalse(panel.actionable)
        self.assertFalse(panel.training_eligible)
        self.assertFalse(panel.feature_eligible)
        self.assertFalse(panel.cross_sectional_ranking_eligible)
        self.assertFalse(panel.alert_eligible)
        self.assertFalse(panel.execution_eligible)

    def test_unassigned_entries_remain_visible_and_block_ranking_authority(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source, _, _, panel = _panels(Path(tmp))
        self.assertEqual(
            panel.unassigned_entry_count,
            len(source.unassigned_entries),
        )
        self.assertGreater(panel.unassigned_entry_count, 0)
        self.assertFalse(panel.cross_sectional_ranking_eligible)

    def test_adjustment_failure_is_an_explicit_blocked_result(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, _, panel = _panels(
                Path(tmp),
                omit_reliance_bar_on=D2,
            )
        self.assertFalse(panel.resolved_histories_input_complete)
        self.assertEqual(
            panel.status_counts,
            (("CORPORATE_ACTION_ADJUSTMENT_BLOCKED", 1),),
        )
        result = panel.results[0]
        self.assertIs(
            result.status,
            PromotedFeatureInputStatus.CORPORATE_ACTION_ADJUSTMENT_BLOCKED,
        )
        self.assertIsNone(result.input_history)
        self.assertIn(
            "CORPORATE_ACTION_ADJUSTMENT_NOT_COMPLETE",
            result.reason_codes,
        )

    def test_identity_conflict_is_an_explicit_blocked_result(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, _, panel = _panels(
                Path(tmp),
                conflict_reliance_bar_on=D2,
            )
        self.assertFalse(panel.resolved_histories_input_complete)
        self.assertIs(
            panel.results[0].status,
            PromotedFeatureInputStatus.CORPORATE_ACTION_ADJUSTMENT_BLOCKED,
        )
        self.assertIsNone(panel.results[0].input_history)

    def test_reason_codes_never_grant_feature_or_trade_authority(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, _, panel = _panels(Path(tmp))
        reasons = panel.results[0].reason_codes
        self.assertIn("FEATURE_CALCULATION_NOT_AUTHORIZED", reasons)
        self.assertIn("COLLECTION_ONLY_NO_DECISION_AUTHORITY", reasons)
        self.assertIn("NO_CROSS_SESSION_TICK_INFERENCE", reasons)


class PromotedFeatureInputRejectionTests(unittest.TestCase):
    def test_rejects_mismatched_source_panel_lineage(self) -> None:
        with (
            tempfile.TemporaryDirectory() as first_tmp,
            tempfile.TemporaryDirectory() as second_tmp,
        ):
            _, adjustment, _, _ = _panels(Path(first_tmp))
            _, _, other_ticks, _ = _panels(
                Path(second_tmp),
                omit_reliance_bar_on=D2,
            )
            with self.assertRaisesRegex(
                PromotedFeatureInputError,
                "do not share exact lineage",
            ):
                PromotedFeatureInputService().materialize(
                    adjustment_panel=adjustment,
                    tick_panel=other_ticks,
                    cutoff=BRIDGE_CUTOFF,
                )

    def test_rejects_cutoff_before_either_source_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, adjustment, ticks, _ = _panels(Path(tmp))
            with self.assertRaisesRegex(
                PromotedFeatureInputError,
                "future-known evidence",
            ):
                PromotedFeatureInputService().materialize(
                    adjustment_panel=adjustment,
                    tick_panel=ticks,
                    cutoff=BRIDGE_CUTOFF - timedelta(microseconds=1),
                )

    def test_rejects_naive_cutoff(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, adjustment, ticks, _ = _panels(Path(tmp))
        with self.assertRaisesRegex(
            PromotedFeatureInputError,
            "cutoff is invalid",
        ):
            PromotedFeatureInputService().materialize(
                adjustment_panel=adjustment,
                tick_panel=ticks,
                cutoff=BRIDGE_CUTOFF.replace(tzinfo=None),
            )

    def test_rejects_wrong_concrete_input_type(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, adjustment, ticks, _ = _panels(Path(tmp))
        with self.assertRaisesRegex(
            PromotedFeatureInputError,
            "source is invalid",
        ):
            PromotedFeatureInputService().materialize(
                adjustment_panel=adjustment,
                tick_panel=object(),
                cutoff=BRIDGE_CUTOFF,
            )

    def test_rejects_tampered_upstream_tick_panel(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, adjustment, ticks, _ = _panels(Path(tmp))
        object.__setattr__(ticks, "panel_id", "0" * 64)
        with self.assertRaisesRegex(
            PromotedFeatureInputError,
            "source could not be verified",
        ):
            PromotedFeatureInputService().materialize(
                adjustment_panel=adjustment,
                tick_panel=ticks,
                cutoff=BRIDGE_CUTOFF,
            )


class PromotedFeatureInputReplayTests(unittest.TestCase):
    def test_direct_construction_rejects_changed_panel_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, _, panel = _panels(Path(tmp))
        values = _kwargs(panel)
        values["panel_id"] = "0" * 64
        with self.assertRaises(PromotedFeatureInputError):
            VerifiedPromotedFeatureInputPanel(**values)

    def test_direct_construction_rejects_changed_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, _, panel = _panels(Path(tmp))
        values = _kwargs(panel)
        values["results"] = ()
        with self.assertRaises(PromotedFeatureInputError):
            VerifiedPromotedFeatureInputPanel(**values)

    def test_direct_construction_rejects_true_integer_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, _, panel = _panels(Path(tmp))
        values = _kwargs(panel)
        values["unassigned_entry_count"] = True
        with self.assertRaises(PromotedFeatureInputError):
            VerifiedPromotedFeatureInputPanel(**values)

    def test_direct_construction_rejects_authority_upgrade(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, _, panel = _panels(Path(tmp))
        values = _kwargs(panel)
        values["feature_eligible"] = True
        with self.assertRaises(PromotedFeatureInputError):
            VerifiedPromotedFeatureInputPanel(**values)

    def test_post_construction_nested_mutation_fails_replay(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, _, panel = _panels(Path(tmp))
        history = panel.results[0].input_history
        assert history is not None
        object.__setattr__(history.bars[0], "tick_size", Decimal("99"))
        with self.assertRaises(PromotedFeatureInputError):
            panel.verify_content_identity()

    def test_rehashed_duplicate_value_cannot_disagree_with_source_bar(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, _, panel = _panels(Path(tmp))
        history = panel.results[0].input_history
        assert history is not None
        input_bar = history.bars[0]
        object.__setattr__(input_bar, "adjusted_open", Decimal("999"))
        object.__setattr__(input_bar, "input_bar_id", input_bar._calculated_id())
        with self.assertRaisesRegex(PromotedFeatureInputError, "graph is invalid"):
            input_bar.verify_content_identity()


class PromotedFeatureInputCapabilityTests(unittest.TestCase):
    def test_public_output_fields_have_no_symbol_name_or_isin_join_key(self) -> None:
        from india_swing.evaluation.promoted_feature_inputs import (
            PromotedFeatureInputBar,
            PromotedFeatureInputHistory,
            PromotedFeatureInputResult,
        )

        forbidden = {"symbol", "ticker", "name", "isin"}
        for output_type in (
            PromotedFeatureInputBar,
            PromotedFeatureInputHistory,
            PromotedFeatureInputResult,
            VerifiedPromotedFeatureInputPanel,
        ):
            field_names = {field.name.lower() for field in dataclasses.fields(output_type)}
            self.assertTrue(field_names.isdisjoint(forbidden))

    def test_service_exposes_no_io_or_decision_method(self) -> None:
        public_names = {
            value
            for value in dir(PromotedFeatureInputService)
            if not value.startswith("_")
        }
        self.assertEqual(public_names, {"materialize"})


if __name__ == "__main__":
    unittest.main()
