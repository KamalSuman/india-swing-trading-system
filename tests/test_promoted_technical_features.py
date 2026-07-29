from __future__ import annotations

import dataclasses
import tempfile
import unittest
from datetime import timedelta
from decimal import Decimal, getcontext
from pathlib import Path

from india_swing.features.promoted_technical import (
    PROMOTED_TECHNICAL_FEATURE_POLICY_VERSION,
    PROMOTED_TECHNICAL_FEATURE_SCHEMA_VERSION,
    PromotedTechnicalFeatureConfig,
    PromotedTechnicalFeatureError,
    PromotedTechnicalFeatureService,
    PromotedTechnicalFeatureStatus,
    VerifiedPromotedTechnicalFeaturePanel,
    _compute_vector,
)
from india_swing.reference.models import ReferenceReadiness
from tests.test_promoted_feature_inputs import _panels
from tests.test_promoted_identity_session_universe import D2


def _small_config() -> PromotedTechnicalFeatureConfig:
    return PromotedTechnicalFeatureConfig(
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
    )


def _feature_panel(
    root: Path,
    *,
    config: PromotedTechnicalFeatureConfig | None = None,
    omit_reliance_bar_on=None,
):
    _, _, _, source = _panels(
        root,
        omit_reliance_bar_on=omit_reliance_bar_on,
    )
    if config is None:
        config = _small_config()
    panel = PromotedTechnicalFeatureService().materialize(
        source_panel=source,
        config=config,
        cutoff=source.cutoff,
    )
    return source, config, panel


def _kwargs(panel: VerifiedPromotedTechnicalFeaturePanel) -> dict[str, object]:
    return {
        value.name: getattr(panel, value.name)
        for value in dataclasses.fields(VerifiedPromotedTechnicalFeaturePanel)
    }


class PromotedTechnicalFeatureConfigTests(unittest.TestCase):
    def test_default_requires_sixty_one_sessions(self) -> None:
        config = PromotedTechnicalFeatureConfig()
        self.assertEqual(config.minimum_history_sessions, 61)
        self.assertEqual(config.required_history_sessions, 61)
        config.verify_content_identity()

    def test_config_identity_changes_with_any_lookback(self) -> None:
        first = PromotedTechnicalFeatureConfig()
        second = PromotedTechnicalFeatureConfig(short_return_sessions=6)
        self.assertNotEqual(first.config_id, second.config_id)

    def test_rejects_bool_or_nonpositive_integer(self) -> None:
        with self.assertRaises(PromotedTechnicalFeatureError):
            PromotedTechnicalFeatureConfig(short_return_sessions=True)
        with self.assertRaises(PromotedTechnicalFeatureError):
            PromotedTechnicalFeatureConfig(short_return_sessions=0)
        with self.assertRaises(PromotedTechnicalFeatureError):
            PromotedTechnicalFeatureConfig(short_trend_sessions=1)

    def test_rejects_minimum_shorter_than_required_lookback(self) -> None:
        with self.assertRaisesRegex(
            PromotedTechnicalFeatureError,
            "configuration is invalid",
        ):
            PromotedTechnicalFeatureConfig(minimum_history_sessions=60)

    def test_rejects_semantically_reversed_windows(self) -> None:
        with self.assertRaises(PromotedTechnicalFeatureError):
            PromotedTechnicalFeatureConfig(
                short_return_sessions=21,
                medium_return_sessions=20,
            )
        with self.assertRaises(PromotedTechnicalFeatureError):
            PromotedTechnicalFeatureConfig(
                short_trend_sessions=51,
                long_trend_sessions=50,
            )
        with self.assertRaises(PromotedTechnicalFeatureError):
            PromotedTechnicalFeatureConfig(
                contraction_short_sessions=21,
                contraction_long_sessions=20,
            )

    def test_mutated_config_identity_fails(self) -> None:
        config = PromotedTechnicalFeatureConfig()
        object.__setattr__(config, "atr_sessions", 10)
        with self.assertRaises(PromotedTechnicalFeatureError):
            config.verify_content_identity()


class PromotedTechnicalFeatureCalculationTests(unittest.TestCase):
    def test_computes_versioned_descriptive_vector(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source, config, panel = _feature_panel(Path(tmp))
            panel.verify_content_identity()

        self.assertEqual(
            panel.schema_version,
            PROMOTED_TECHNICAL_FEATURE_SCHEMA_VERSION,
        )
        self.assertEqual(
            panel.policy_version,
            PROMOTED_TECHNICAL_FEATURE_POLICY_VERSION,
        )
        self.assertEqual(panel.source_panel.panel_id, source.panel_id)
        self.assertEqual(panel.config.config_id, config.config_id)
        self.assertEqual(panel.computed_history_count, 1)
        self.assertEqual(panel.blocked_history_count, 0)
        self.assertTrue(panel.resolved_histories_feature_complete)
        self.assertEqual(
            panel.status_counts,
            (("FEATURE_VECTOR_COMPUTED_COLLECTION_ONLY", 1),),
        )
        result = panel.results[0]
        self.assertIs(
            result.status,
            PromotedTechnicalFeatureStatus.FEATURE_VECTOR_COMPUTED_COLLECTION_ONLY,
        )
        self.assertIsNotNone(result.feature_vector)

    def test_exact_two_bar_formulas_are_replayable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source, _, panel = _feature_panel(Path(tmp))
        history = source.results[0].input_history
        vector = panel.results[0].feature_vector
        assert history is not None
        assert vector is not None
        first, second = history.bars

        expected_true_range = max(
            second.adjusted_high - second.adjusted_low,
            abs(second.adjusted_high - first.adjusted_close),
            abs(second.adjusted_low - first.adjusted_close),
        )
        expected_average = (
            first.adjusted_close + second.adjusted_close
        ) / Decimal("2")
        self.assertEqual(vector.return_short, Decimal("1"))
        self.assertEqual(vector.return_medium, vector.return_short)
        self.assertEqual(vector.return_long, vector.return_short)
        self.assertEqual(vector.simple_moving_average_short, expected_average)
        self.assertEqual(vector.simple_moving_average_long, expected_average)
        self.assertEqual(vector.positive_close_fraction_short, Decimal("1"))
        self.assertEqual(vector.average_true_range, expected_true_range)
        self.assertEqual(
            vector.average_true_range_fraction,
            expected_true_range / second.adjusted_close,
        )
        self.assertEqual(vector.annualized_realized_volatility, Decimal("0"))
        self.assertEqual(vector.prior_breakout_high, first.adjusted_high)
        self.assertEqual(vector.prior_breakout_low, first.adjusted_low)
        self.assertEqual(
            vector.breakout_distance,
            second.adjusted_close / first.adjusted_high - Decimal("1"),
        )
        self.assertEqual(
            vector.range_position,
            (second.adjusted_close - first.adjusted_low)
            / (first.adjusted_high - first.adjusted_low),
        )
        self.assertEqual(vector.maximum_drawdown, Decimal("0"))
        self.assertEqual(
            vector.signal_gap_return,
            second.adjusted_open / first.adjusted_close - Decimal("1"),
        )
        self.assertEqual(vector.median_prior_volume, first.adjusted_volume)
        self.assertEqual(
            vector.signal_volume_ratio,
            second.adjusted_volume / first.adjusted_volume,
        )
        self.assertEqual(
            vector.median_prior_traded_value,
            first.adjusted_close * first.adjusted_volume,
        )
        self.assertEqual(vector.zero_volume_fraction, Decimal("0"))
        self.assertEqual(vector.range_contraction_ratio, Decimal("1"))
        self.assertEqual(vector.signal_tick_size, second.tick_size)
        self.assertEqual(
            vector.signal_tick_fraction,
            second.tick_size / second.adjusted_close,
        )
        self.assertEqual(
            vector.average_true_range_in_ticks,
            expected_true_range / second.tick_size,
        )
        self.assertEqual(vector.tick_change_count, 0)
        vector.verify_content_identity()

    def test_vector_binds_every_input_bar_and_latest_knowledge_time(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source, _, panel = _feature_panel(Path(tmp))
        history = source.results[0].input_history
        vector = panel.results[0].feature_vector
        assert history is not None
        assert vector is not None
        self.assertEqual(
            vector.input_bar_ids,
            tuple(value.input_bar_id for value in history.bars),
        )
        self.assertEqual(
            vector.knowledge_time,
            max(value.knowledge_time for value in history.bars),
        )
        self.assertEqual(vector.signal_session, history.signal_session)

    def test_decimal_context_does_not_change_feature_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source, config, _ = _feature_panel(Path(tmp))
            history = source.results[0].input_history
            assert history is not None
            normal = _compute_vector(history, config, source.cutoff)
            previous_precision = getcontext().prec
            try:
                getcontext().prec = 6
                reduced_global_precision = _compute_vector(
                    history,
                    config,
                    source.cutoff,
                )
            finally:
                getcontext().prec = previous_precision
        self.assertEqual(normal.feature_id, reduced_global_precision.feature_id)
        self.assertEqual(normal, reduced_global_precision)

    def test_default_config_blocks_short_history_explicitly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, config, panel = _feature_panel(
                Path(tmp),
                config=PromotedTechnicalFeatureConfig(),
            )
        self.assertEqual(config.minimum_history_sessions, 61)
        self.assertEqual(panel.computed_history_count, 0)
        self.assertEqual(panel.blocked_history_count, 1)
        self.assertFalse(panel.resolved_histories_feature_complete)
        result = panel.results[0]
        self.assertIs(
            result.status,
            PromotedTechnicalFeatureStatus.INSUFFICIENT_HISTORY_BLOCKED,
        )
        self.assertEqual(result.observed_history_sessions, 2)
        self.assertEqual(result.required_history_sessions, 61)
        self.assertIsNone(result.feature_vector)
        self.assertIn(
            "CONFIGURED_FEATURE_WARMUP_INCOMPLETE",
            result.reason_codes,
        )

    def test_source_blocker_propagates_without_feature_calculation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source, _, panel = _feature_panel(
                Path(tmp),
                omit_reliance_bar_on=D2,
            )
        self.assertIsNone(source.results[0].input_history)
        result = panel.results[0]
        self.assertIs(
            result.status,
            PromotedTechnicalFeatureStatus.SOURCE_INPUT_BLOCKED,
        )
        self.assertEqual(result.observed_history_sessions, 0)
        self.assertIsNone(result.feature_vector)


class PromotedTechnicalFeatureSafetyTests(unittest.TestCase):
    def test_every_decision_authority_flag_remains_false(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source, _, panel = _feature_panel(Path(tmp))
        self.assertIs(panel.readiness, ReferenceReadiness.COLLECTION_ONLY)
        self.assertEqual(
            panel.unassigned_entry_count,
            source.unassigned_entry_count,
        )
        self.assertFalse(panel.actionable)
        self.assertFalse(panel.training_eligible)
        self.assertFalse(panel.feature_eligible)
        self.assertFalse(panel.cross_sectional_ranking_eligible)
        self.assertFalse(panel.alert_eligible)
        self.assertFalse(panel.execution_eligible)

    def test_feature_values_are_finite_decimals(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, panel = _feature_panel(Path(tmp))
        vector = panel.results[0].feature_vector
        assert vector is not None
        ignored = {
            "source_history_id",
            "config_id",
            "stable_instrument_id",
            "stable_listing_id",
            "signal_session",
            "cutoff",
            "knowledge_time",
            "input_bar_ids",
            "tick_change_count",
            "feature_id",
        }
        for field in dataclasses.fields(vector):
            if field.name not in ignored:
                value = getattr(vector, field.name)
                self.assertIs(type(value), Decimal)
                self.assertTrue(value.is_finite())

    def test_result_reasons_disclaim_ranking_forecast_and_trade_authority(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, panel = _feature_panel(Path(tmp))
        reasons = panel.results[0].reason_codes
        self.assertIn("NO_CROSS_SECTIONAL_RANKING_AUTHORITY", reasons)
        self.assertIn("NO_FORECAST_OR_PROBABILITY_AUTHORITY", reasons)
        self.assertIn("COLLECTION_ONLY_NO_DECISION_AUTHORITY", reasons)

    def test_service_has_only_materialize_as_public_capability(self) -> None:
        public_names = {
            value
            for value in dir(PromotedTechnicalFeatureService)
            if not value.startswith("_")
        }
        self.assertEqual(public_names, {"materialize"})


class PromotedTechnicalFeatureRejectionTests(unittest.TestCase):
    def test_rejects_cutoff_before_source_panel(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source, config, _ = _feature_panel(Path(tmp))
            with self.assertRaisesRegex(
                PromotedTechnicalFeatureError,
                "future-known evidence",
            ):
                PromotedTechnicalFeatureService().materialize(
                    source_panel=source,
                    config=config,
                    cutoff=source.cutoff - timedelta(microseconds=1),
                )

    def test_rejects_naive_cutoff(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source, config, _ = _feature_panel(Path(tmp))
            with self.assertRaisesRegex(
                PromotedTechnicalFeatureError,
                "cutoff is invalid",
            ):
                PromotedTechnicalFeatureService().materialize(
                    source_panel=source,
                    config=config,
                    cutoff=source.cutoff.replace(tzinfo=None),
                )

    def test_rejects_tampered_source_panel(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source, config, _ = _feature_panel(Path(tmp))
            object.__setattr__(source, "panel_id", "0" * 64)
            with self.assertRaisesRegex(
                PromotedTechnicalFeatureError,
                "source could not be verified",
            ):
                PromotedTechnicalFeatureService().materialize(
                    source_panel=source,
                    config=config,
                    cutoff=source.cutoff,
                )

    def test_rejects_wrong_exact_source_type(self) -> None:
        config = _small_config()
        with self.assertRaisesRegex(
            PromotedTechnicalFeatureError,
            "source is invalid",
        ):
            PromotedTechnicalFeatureService().materialize(
                source_panel=object(),
                config=config,
                cutoff=datetime_for_test(),
            )


def datetime_for_test():
    from datetime import datetime, timezone

    return datetime(2026, 7, 16, 16, tzinfo=timezone.utc)


class PromotedTechnicalFeatureReplayTests(unittest.TestCase):
    def test_direct_panel_construction_rejects_changed_counts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, panel = _feature_panel(Path(tmp))
            values = _kwargs(panel)
            values["computed_history_count"] = 0
            with self.assertRaises(PromotedTechnicalFeatureError):
                VerifiedPromotedTechnicalFeaturePanel(**values)

    def test_direct_panel_construction_rejects_true_integer_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, panel = _feature_panel(Path(tmp))
            values = _kwargs(panel)
            values["unassigned_entry_count"] = True
            with self.assertRaises(PromotedTechnicalFeatureError):
                VerifiedPromotedTechnicalFeaturePanel(**values)

    def test_direct_panel_construction_rejects_authority_upgrade(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, panel = _feature_panel(Path(tmp))
            values = _kwargs(panel)
            values["cross_sectional_ranking_eligible"] = True
            with self.assertRaises(PromotedTechnicalFeatureError):
                VerifiedPromotedTechnicalFeaturePanel(**values)

    def test_mutated_feature_value_fails_panel_replay(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, panel = _feature_panel(Path(tmp))
            vector = panel.results[0].feature_vector
            assert vector is not None
            object.__setattr__(vector, "return_short", Decimal("999"))
            with self.assertRaises(PromotedTechnicalFeatureError):
                panel.verify_content_identity()

    def test_rehashed_mutated_vector_still_fails_panel_replay(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, panel = _feature_panel(Path(tmp))
            vector = panel.results[0].feature_vector
            assert vector is not None
            object.__setattr__(vector, "return_short", Decimal("999"))
            object.__setattr__(vector, "feature_id", vector._calculated_id())
            with self.assertRaises(PromotedTechnicalFeatureError):
                panel.verify_content_identity()


if __name__ == "__main__":
    unittest.main()
