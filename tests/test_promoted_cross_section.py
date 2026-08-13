from __future__ import annotations

import dataclasses
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from india_swing.features.promoted_cross_section import (
    PROMOTED_CROSS_SECTION_POLICY_VERSION,
    PROMOTED_CROSS_SECTION_SCHEMA_VERSION,
    PromotedCrossSectionConfig,
    PromotedCrossSectionError,
    PromotedCrossSectionResultStatus,
    PromotedCrossSectionService,
    VerifiedPromotedCrossSectionPanel,
    _percentile_ranks,
    _rank_tiers,
    score_promoted_feature_vectors,
)
from india_swing.features.promoted_technical import (
    PromotedTechnicalFeatureConfig,
    PromotedTechnicalFeatureService,
)
from india_swing.forecasting.regime_ensemble import (
    AlphaSpecialist,
    MarketRegime,
)
from india_swing.reference.models import ReferenceReadiness
from tests.test_promoted_feature_inputs import _panels
from tests.test_promoted_identity_session_universe import D2
from tests.test_promoted_technical_features import _feature_panel


def _cross_section(
    root: Path,
    *,
    minimum_computed_instruments: int = 1,
):
    _, _, features = _feature_panel(root)
    config = PromotedCrossSectionConfig(
        minimum_computed_instruments=minimum_computed_instruments
    )
    panel = PromotedCrossSectionService().materialize(
        source_panel=features,
        config=config,
        cutoff=features.cutoff,
    )
    return features, config, panel


def _panel_kwargs(
    panel: VerifiedPromotedCrossSectionPanel,
) -> dict[str, object]:
    return {
        value.name: getattr(panel, value.name)
        for value in dataclasses.fields(VerifiedPromotedCrossSectionPanel)
    }


class PromotedCrossSectionConfigTests(unittest.TestCase):
    def test_default_requires_twenty_computed_instruments(self) -> None:
        config = PromotedCrossSectionConfig()
        self.assertEqual(config.minimum_computed_instruments, 20)
        self.assertEqual(
            {value.regime for value in config.weightings},
            set(MarketRegime),
        )
        for weighting in config.weightings:
            self.assertEqual(
                sum(
                    (
                        weighting.weight_for(specialist)
                        for specialist in AlphaSpecialist
                    ),
                    Decimal("0"),
                ),
                Decimal("1"),
            )
        config.verify_content_identity()

    def test_rejects_invalid_thresholds_and_bool_minimum(self) -> None:
        with self.assertRaises(PromotedCrossSectionError):
            PromotedCrossSectionConfig(minimum_computed_instruments=True)
        with self.assertRaises(PromotedCrossSectionError):
            PromotedCrossSectionConfig(
                risk_off_breadth_threshold=Decimal("0.70"),
                trending_breadth_threshold=Decimal("0.60"),
            )
        with self.assertRaises(PromotedCrossSectionError):
            PromotedCrossSectionConfig(
                high_volatility_threshold=Decimal("0"),
            )

    def test_configuration_identity_changes_with_threshold(self) -> None:
        first = PromotedCrossSectionConfig()
        second = PromotedCrossSectionConfig(
            high_volatility_threshold=Decimal("0.40")
        )
        self.assertNotEqual(first.config_id, second.config_id)


class PromotedCrossSectionRankKernelTests(unittest.TestCase):
    A = ("a" * 64, "1" * 64)
    B = ("b" * 64, "2" * 64)
    C = ("c" * 64, "3" * 64)

    def test_equal_values_receive_equal_percentiles(self) -> None:
        ranks = _percentile_ranks(
            (
                (self.A, Decimal("5")),
                (self.B, Decimal("5")),
                (self.C, Decimal("10")),
            ),
            higher_is_better=True,
        )
        self.assertEqual(ranks[self.A], ranks[self.B])
        self.assertEqual(ranks[self.A], Decimal("0"))
        self.assertEqual(ranks[self.C], Decimal("1"))

    def test_identifier_order_does_not_break_score_ties(self) -> None:
        tiers = _rank_tiers(
            (
                (self.C, Decimal("0.8")),
                (self.A, Decimal("0.8")),
                (self.B, Decimal("0.3")),
            )
        )
        self.assertEqual(tiers[self.C], (1, 2))
        self.assertEqual(tiers[self.A], (1, 2))
        self.assertEqual(tiers[self.B], (2, 1))

    def test_rank_is_insensitive_to_outlier_magnitude(self) -> None:
        first = _percentile_ranks(
            (
                (self.A, Decimal("1")),
                (self.B, Decimal("2")),
                (self.C, Decimal("1000")),
            ),
            higher_is_better=True,
        )
        second = _percentile_ranks(
            (
                (self.A, Decimal("1")),
                (self.B, Decimal("2")),
                (self.C, Decimal("1000000000000")),
            ),
            higher_is_better=True,
        )
        self.assertEqual(first, second)
        self.assertEqual(first[self.B], Decimal("0.5"))

    def test_lower_is_better_reverses_percentiles(self) -> None:
        ranks = _percentile_ranks(
            (
                (self.A, Decimal("1")),
                (self.B, Decimal("2")),
            ),
            higher_is_better=False,
        )
        self.assertEqual(ranks[self.A], Decimal("1"))
        self.assertEqual(ranks[self.B], Decimal("0"))


class PromotedCrossSectionCalculationTests(unittest.TestCase):
    def test_shared_vector_kernel_matches_legacy_panel_exactly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            features, config, panel = _cross_section(Path(tmp))
            vectors = tuple(
                result.feature_vector
                for result in features.results
                if result.feature_vector is not None
            )
            regime, opportunities = score_promoted_feature_vectors(
                vectors=vectors,
                source_feature_panel_id=features.panel_id,
                config=config,
            )
        self.assertEqual(regime, panel.regime_evidence)
        self.assertEqual(
            opportunities,
            tuple(
                result.opportunity_score
                for result in panel.results
                if result.opportunity_score is not None
            ),
        )

    def test_computes_regime_specialists_and_dense_rank(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            features, config, panel = _cross_section(Path(tmp))
            panel.verify_content_identity()

        self.assertEqual(panel.schema_version, PROMOTED_CROSS_SECTION_SCHEMA_VERSION)
        self.assertEqual(panel.policy_version, PROMOTED_CROSS_SECTION_POLICY_VERSION)
        self.assertEqual(panel.source_panel.panel_id, features.panel_id)
        self.assertEqual(panel.config.config_id, config.config_id)
        self.assertIsNotNone(panel.regime_evidence)
        assert panel.regime_evidence is not None
        self.assertIs(panel.regime_evidence.regime, MarketRegime.TRENDING)
        self.assertEqual(panel.regime_evidence.market_breadth, Decimal("1"))
        self.assertEqual(
            panel.regime_evidence.feature_ids,
            (features.results[0].feature_vector.feature_id,),
        )
        self.assertEqual(panel.scored_history_count, 1)
        self.assertEqual(panel.blocked_history_count, 0)
        self.assertTrue(panel.resolved_histories_scoring_complete)
        result = panel.results[0]
        self.assertIs(
            result.status,
            (
                PromotedCrossSectionResultStatus
                .SCORED_RESOLVED_SUBSET_COLLECTION_ONLY
            ),
        )
        score = result.opportunity_score
        self.assertIsNotNone(score)
        assert score is not None
        self.assertEqual(score.rank_tier, 1)
        self.assertEqual(score.tie_size, 1)
        self.assertEqual(
            tuple(value.specialist for value in score.specialist_scores),
            tuple(sorted(AlphaSpecialist, key=lambda value: value.value)),
        )
        self.assertEqual(
            score.ensemble_score,
            sum(
                (value.weighted_score for value in score.specialist_scores),
                Decimal("0"),
            ),
        )
        self.assertEqual(score.ensemble_score, Decimal("0.541250"))

    def test_specialist_components_are_machine_auditable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, panel = _cross_section(Path(tmp))
        score = panel.results[0].opportunity_score
        assert score is not None
        by_specialist = {
            value.specialist: dict(value.components)
            for value in score.specialist_scores
        }
        self.assertIn(
            "short_return_percentile",
            by_specialist[AlphaSpecialist.MOMENTUM_BREAKOUT],
        )
        self.assertIn(
            "pullback_quality",
            by_specialist[AlphaSpecialist.PULLBACK_CONTINUATION],
        )
        self.assertIn(
            "contraction_quality",
            by_specialist[AlphaSpecialist.VOLATILITY_CONTRACTION],
        )
        self.assertIn(
            "low_tick_friction_percentile",
            by_specialist[AlphaSpecialist.LIQUIDITY_QUALITY],
        )

    def test_default_cross_section_minimum_blocks_single_instrument(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, features = _feature_panel(Path(tmp))
            config = PromotedCrossSectionConfig()
            panel = PromotedCrossSectionService().materialize(
                source_panel=features,
                config=config,
                cutoff=features.cutoff,
            )
        self.assertIsNone(panel.regime_evidence)
        self.assertEqual(panel.scored_history_count, 0)
        self.assertEqual(panel.blocked_history_count, 1)
        self.assertIs(
            panel.results[0].status,
            PromotedCrossSectionResultStatus.CROSS_SECTION_TOO_SMALL_BLOCKED,
        )
        self.assertIsNone(panel.results[0].opportunity_score)

    def test_source_feature_blocker_is_retained(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, _, inputs = _panels(
                Path(tmp),
                omit_reliance_bar_on=D2,
            )
            feature_config = PromotedTechnicalFeatureConfig(
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
            features = PromotedTechnicalFeatureService().materialize(
                source_panel=inputs,
                config=feature_config,
                cutoff=inputs.cutoff,
            )
            panel = PromotedCrossSectionService().materialize(
                source_panel=features,
                config=PromotedCrossSectionConfig(
                    minimum_computed_instruments=1
                ),
                cutoff=features.cutoff,
            )
        self.assertIsNone(panel.regime_evidence)
        self.assertIs(
            panel.results[0].status,
            PromotedCrossSectionResultStatus.SOURCE_FEATURE_BLOCKED,
        )
        self.assertIsNone(panel.results[0].opportunity_score)


class PromotedCrossSectionSafetyTests(unittest.TestCase):
    def test_incomplete_identity_universe_never_gains_ranking_authority(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            features, _, panel = _cross_section(Path(tmp))
        self.assertEqual(
            panel.unassigned_entry_count,
            features.unassigned_entry_count,
        )
        self.assertGreater(panel.unassigned_entry_count, 0)
        self.assertEqual(
            panel.orphan_bar_count,
            len(
                features.source_panel.adjustment_panel.source_panel.orphan_bars
            ),
        )
        self.assertFalse(panel.source_universe_cross_section_complete)
        self.assertIs(panel.readiness, ReferenceReadiness.COLLECTION_ONLY)
        self.assertFalse(panel.actionable)
        self.assertFalse(panel.training_eligible)
        self.assertFalse(panel.feature_eligible)
        self.assertFalse(panel.ranking_eligible)
        self.assertFalse(panel.alert_eligible)
        self.assertFalse(panel.execution_eligible)

    def test_scores_make_no_probability_or_expected_return_claim(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, panel = _cross_section(Path(tmp))
        result = panel.results[0]
        self.assertIn(
            "NO_PROBABILITY_OR_EXPECTED_RETURN_CLAIM",
            result.reason_codes,
        )
        score = result.opportunity_score
        assert score is not None
        field_names = {
            value.name for value in dataclasses.fields(type(score))
        }
        self.assertNotIn("confidence", field_names)
        self.assertNotIn("probability", field_names)
        self.assertNotIn("expected_return", field_names)
        self.assertNotIn("selected", field_names)

    def test_service_exposes_only_materialize(self) -> None:
        public_names = {
            value
            for value in dir(PromotedCrossSectionService)
            if not value.startswith("_")
        }
        self.assertEqual(public_names, {"materialize"})


class PromotedCrossSectionRejectionTests(unittest.TestCase):
    def test_rejects_cutoff_before_feature_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            features, config, _ = _cross_section(Path(tmp))
            with self.assertRaisesRegex(
                PromotedCrossSectionError,
                "future-known evidence",
            ):
                PromotedCrossSectionService().materialize(
                    source_panel=features,
                    config=config,
                    cutoff=features.cutoff - timedelta(microseconds=1),
                )

    def test_rejects_naive_cutoff(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            features, config, _ = _cross_section(Path(tmp))
            with self.assertRaisesRegex(
                PromotedCrossSectionError,
                "cutoff is invalid",
            ):
                PromotedCrossSectionService().materialize(
                    source_panel=features,
                    config=config,
                    cutoff=features.cutoff.replace(tzinfo=None),
                )

    def test_rejects_wrong_source_type(self) -> None:
        with self.assertRaisesRegex(
            PromotedCrossSectionError,
            "source is invalid",
        ):
            PromotedCrossSectionService().materialize(
                source_panel=object(),
                config=PromotedCrossSectionConfig(),
                cutoff=datetime(2026, 7, 16, 16, tzinfo=timezone.utc),
            )

    def test_rejects_tampered_source_panel(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            features, config, _ = _cross_section(Path(tmp))
            object.__setattr__(features, "panel_id", "0" * 64)
            with self.assertRaisesRegex(
                PromotedCrossSectionError,
                "source could not be verified",
            ):
                PromotedCrossSectionService().materialize(
                    source_panel=features,
                    config=config,
                    cutoff=features.cutoff,
                )


class PromotedCrossSectionReplayTests(unittest.TestCase):
    def test_direct_construction_rejects_count_change(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, panel = _cross_section(Path(tmp))
            values = _panel_kwargs(panel)
            values["scored_history_count"] = 0
            with self.assertRaises(PromotedCrossSectionError):
                VerifiedPromotedCrossSectionPanel(**values)

    def test_direct_construction_rejects_true_integer(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, panel = _cross_section(Path(tmp))
            values = _panel_kwargs(panel)
            values["unassigned_entry_count"] = True
            with self.assertRaises(PromotedCrossSectionError):
                VerifiedPromotedCrossSectionPanel(**values)

    def test_direct_construction_rejects_ranking_authority_upgrade(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, panel = _cross_section(Path(tmp))
            values = _panel_kwargs(panel)
            values["ranking_eligible"] = True
            with self.assertRaises(PromotedCrossSectionError):
                VerifiedPromotedCrossSectionPanel(**values)

    def test_nested_score_mutation_fails_panel_replay(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, panel = _cross_section(Path(tmp))
            score = panel.results[0].opportunity_score
            assert score is not None
            object.__setattr__(score, "rank_tier", 99)
            with self.assertRaises(PromotedCrossSectionError):
                panel.verify_content_identity()


if __name__ == "__main__":
    unittest.main()
