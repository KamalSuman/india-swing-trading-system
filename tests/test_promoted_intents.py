from __future__ import annotations

import dataclasses
import tempfile
import unittest
from datetime import timedelta
from decimal import Decimal, localcontext
from pathlib import Path

from india_swing.evaluation.promoted_intents import (
    PromotedCandidateDecision,
    PromotedCandidateDecisionStatus,
    PromotedIntentError,
    PromotedIntentPolicyConfig,
    PromotedResearchIntentService,
    VerifiedPromotedResearchIntentBatch,
    _PreparedCandidate,
    _prepare,
    _preparation_status,
    _select_complete_tiers,
)
from india_swing.features.promoted_cross_section import (
    PromotedCrossSectionConfig,
    PromotedCrossSectionService,
)
from india_swing.forecasting.regime_ensemble import MarketRegime
from tests.test_promoted_technical_features import _feature_panel


def _panel(root: Path):
    _, _, technical = _feature_panel(root)
    return PromotedCrossSectionService().materialize(
        source_panel=technical,
        config=PromotedCrossSectionConfig(
            minimum_computed_instruments=1
        ),
        cutoff=technical.cutoff,
    )


def _permissive_config(**changes) -> PromotedIntentPolicyConfig:
    values = {
        "minimum_ensemble_score": Decimal("0.01"),
        "minimum_median_traded_value": Decimal("1"),
        "minimum_signal_traded_value_ratio": Decimal("0.01"),
        "maximum_tick_fraction": Decimal("0.50"),
        "minimum_average_true_range_ticks": Decimal("0.01"),
        "maximum_annualized_volatility": Decimal("10"),
        "maximum_zero_volume_fraction": Decimal("1"),
    }
    values.update(changes)
    return PromotedIntentPolicyConfig(**values)


def _prepared(
    panel,
    *,
    rank_tier: int,
    tie_size: int,
) -> _PreparedCandidate:
    result = panel.results[0]
    opportunity = result.opportunity_score
    vector = result.source_result.feature_vector
    assert opportunity is not None
    assert vector is not None
    ranked = dataclasses.replace(
        opportunity,
        rank_tier=rank_tier,
        tie_size=tie_size,
    )
    return _PreparedCandidate(
        source_result=result,
        opportunity=ranked,
        vector=vector,
        universe_snapshot_id="a" * 64,
        symbol="RELIANCE",
        isin="INE002A01018",
        signal_close=Decimal("100"),
        entry_price=Decimal("100"),
        stop_price=Decimal("95"),
        target_price=Decimal("115"),
        quantity=1,
    )


class PromotedIntentConfigTests(unittest.TestCase):
    def test_defaults_encode_wealth_protection_contract(self) -> None:
        config = PromotedIntentPolicyConfig()
        self.assertEqual(config.portfolio_risk_fraction, Decimal("0.02"))
        self.assertEqual(config.minimum_net_reward_risk, Decimal("2.50"))
        self.assertEqual(config.maximum_positions, 5)
        self.assertNotIn(MarketRegime.RISK_OFF, config.allowed_regimes)
        self.assertNotIn(
            MarketRegime.HIGH_VOLATILITY,
            config.allowed_regimes,
        )
        config.verify_content_identity()

    def test_rejects_reward_risk_below_two_and_a_half(self) -> None:
        with self.assertRaises(PromotedIntentError):
            PromotedIntentPolicyConfig(
                minimum_net_reward_risk=Decimal("2.49")
            )

    def test_rejects_risk_off_or_high_volatility_authorization(self) -> None:
        with self.assertRaises(PromotedIntentError):
            PromotedIntentPolicyConfig(
                allowed_regimes=(
                    MarketRegime.RANGE_BOUND,
                    MarketRegime.RISK_OFF,
                )
            )

    def test_any_policy_change_changes_identity(self) -> None:
        first = PromotedIntentPolicyConfig()
        second = PromotedIntentPolicyConfig(maximum_positions=4)
        self.assertNotEqual(first.config_id, second.config_id)


class CompleteTierSelectionTests(unittest.TestCase):
    def test_boundary_tie_is_rejected_in_full(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = _panel(Path(tmp))
            top = _prepared(panel, rank_tier=1, tie_size=1)
            tied_a = _prepared(panel, rank_tier=2, tie_size=2)
            tied_b = _prepared(panel, rank_tier=2, tie_size=2)
            object.__setattr__(
                tied_b.opportunity,
                "opportunity_id",
                "f" * 64,
            )
            selected, boundary = _select_complete_tiers(
                (top, tied_a, tied_b),
                2,
            )
        self.assertEqual(selected, {top.opportunity.opportunity_id})
        self.assertEqual(
            boundary,
            {
                tied_a.opportunity.opportunity_id,
                tied_b.opportunity.opportunity_id,
            },
        )

    def test_complete_tiers_are_selected_without_identifier_ordering(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = _panel(Path(tmp))
            first = _prepared(panel, rank_tier=1, tie_size=2)
            second = _prepared(panel, rank_tier=1, tie_size=2)
            object.__setattr__(
                second.opportunity,
                "opportunity_id",
                "e" * 64,
            )
            selected, boundary = _select_complete_tiers(
                (second, first),
                2,
            )
        self.assertEqual(
            selected,
            {
                first.opportunity.opportunity_id,
                second.opportunity.opportunity_id,
            },
        )
        self.assertEqual(boundary, set())

    def test_lower_tier_is_not_promoted_around_boundary_tie(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = _panel(Path(tmp))
            top = _prepared(panel, rank_tier=1, tie_size=1)
            tied_a = _prepared(panel, rank_tier=2, tie_size=2)
            tied_b = _prepared(panel, rank_tier=2, tie_size=2)
            lower = _prepared(panel, rank_tier=3, tie_size=1)
            object.__setattr__(
                tied_b.opportunity,
                "opportunity_id",
                "d" * 64,
            )
            object.__setattr__(
                lower.opportunity,
                "opportunity_id",
                "c" * 64,
            )
            selected, _ = _select_complete_tiers(
                (top, tied_a, tied_b, lower),
                2,
            )
        self.assertEqual(selected, {top.opportunity.opportunity_id})


class CandidateGateAndSizingTests(unittest.TestCase):
    def test_score_gate_is_applied_before_selection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = _panel(Path(tmp))
            result = panel.results[0]
            score = result.opportunity_score
            vector = result.source_result.feature_vector
            assert score is not None
            assert vector is not None
            status = _preparation_status(
                opportunity=score,
                vector=vector,
                config=PromotedIntentPolicyConfig(
                    minimum_ensemble_score=Decimal("0.99")
                ),
            )
        self.assertIs(
            status,
            PromotedCandidateDecisionStatus.SCORE_BELOW_MINIMUM,
        )

    def test_tick_friction_gate_is_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = _panel(Path(tmp))
            result = panel.results[0]
            score = result.opportunity_score
            vector = result.source_result.feature_vector
            assert score is not None
            assert vector is not None
            high_friction = dataclasses.replace(
                vector,
                signal_tick_fraction=Decimal("0.02"),
            )
            status = _preparation_status(
                opportunity=score,
                vector=high_friction,
                config=_permissive_config(
                    maximum_tick_fraction=Decimal("0.01")
                ),
            )
        self.assertIs(
            status,
            PromotedCandidateDecisionStatus.TICK_FRICTION_TOO_HIGH,
        )

    def test_position_size_obeys_risk_and_notional_caps(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = _panel(Path(tmp))
            result = panel.results[0]
            score = result.opportunity_score
            vector = result.source_result.feature_vector
            assert score is not None
            assert vector is not None
            config = _permissive_config()
            candidate = _prepare(
                result=result,
                opportunity=score,
                vector=vector,
                config=config,
                initial_capital=Decimal("100000"),
            )
        self.assertIsNotNone(candidate)
        assert candidate is not None
        expected_universe = (
            result.source_result.source_result.source_adjustment_result
            .identity_bindings[-1].identity_snapshot_id
        )
        self.assertEqual(
            candidate.universe_snapshot_id,
            expected_universe,
        )
        cost = (
            candidate.entry_price
            * config.round_trip_cost_buffer_fraction
        )
        risk = candidate.entry_price - candidate.stop_price + cost
        maximum_risk = (
            Decimal("100000")
            * config.portfolio_risk_fraction
            / config.maximum_positions
        )
        maximum_notional = (
            Decimal("100000")
            * config.gross_exposure_fraction
            / config.maximum_positions
        )
        self.assertLessEqual(Decimal(candidate.quantity) * risk, maximum_risk)
        self.assertLessEqual(
            Decimal(candidate.quantity) * candidate.entry_price,
            maximum_notional,
        )

    def test_target_meets_cost_adjusted_reward_risk_floor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = _panel(Path(tmp))
            result = panel.results[0]
            score = result.opportunity_score
            vector = result.source_result.feature_vector
            assert score is not None
            assert vector is not None
            config = _permissive_config()
            candidate = _prepare(
                result=result,
                opportunity=score,
                vector=vector,
                config=config,
                initial_capital=Decimal("100000"),
            )
        assert candidate is not None
        cost = (
            candidate.entry_price
            * config.round_trip_cost_buffer_fraction
        )
        reward = candidate.target_price - candidate.entry_price - cost
        risk = candidate.entry_price - candidate.stop_price + cost
        self.assertGreaterEqual(
            reward / risk,
            config.minimum_net_reward_risk,
        )

    def test_sizing_is_independent_of_callers_decimal_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = _panel(Path(tmp))
            result = panel.results[0]
            score = result.opportunity_score
            vector = result.source_result.feature_vector
            assert score is not None
            assert vector is not None
            config = _permissive_config()
            with localcontext() as context:
                context.prec = 9
                low_precision = _prepare(
                    result=result,
                    opportunity=score,
                    vector=vector,
                    config=config,
                    initial_capital=Decimal("100000"),
                )
            with localcontext() as context:
                context.prec = 50
                high_precision = _prepare(
                    result=result,
                    opportunity=score,
                    vector=vector,
                    config=config,
                    initial_capital=Decimal("100000"),
                )
        self.assertEqual(low_precision, high_precision)


class PromotedResearchIntentIntegrationTests(unittest.TestCase):
    def test_incomplete_source_universe_fails_closed_without_intents(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = _panel(Path(tmp))
            batch = PromotedResearchIntentService().generate(
                source_panel=panel,
                config=_permissive_config(),
                entry_session=(
                    panel.source_panel.results[0]
                    .feature_vector.signal_session
                    + timedelta(days=1)
                ),
                initial_capital=Decimal("100000"),
            )
        self.assertFalse(batch.source_universe_complete)
        self.assertEqual(batch.selected_count, 0)
        self.assertEqual(batch.intents, ())
        self.assertIs(
            batch.decisions[0].status,
            (
                PromotedCandidateDecisionStatus
                .SOURCE_UNIVERSE_INCOMPLETE
            ),
        )
        batch.verify_content_identity()

    def test_same_session_entry_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = _panel(Path(tmp))
            signal_session = (
                panel.source_panel.results[0].feature_vector.signal_session
            )
            with self.assertRaises(PromotedIntentError):
                PromotedResearchIntentService().generate(
                    source_panel=panel,
                    config=_permissive_config(),
                    entry_session=signal_session,
                    initial_capital=Decimal("100000"),
                )

    def test_fully_blocked_panel_produces_auditable_zero_intent_batch(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, technical = _feature_panel(Path(tmp))
            panel = PromotedCrossSectionService().materialize(
                source_panel=technical,
                config=PromotedCrossSectionConfig(),
                cutoff=technical.cutoff,
            )
            signal_session = (
                technical.source_panel.adjustment_panel.signal_session
            )
            batch = PromotedResearchIntentService().generate(
                source_panel=panel,
                config=_permissive_config(),
                entry_session=signal_session + timedelta(days=1),
                initial_capital=Decimal("100000"),
            )
        self.assertEqual(batch.intents, ())
        self.assertEqual(batch.selected_count, 0)
        self.assertIs(
            batch.decisions[0].status,
            PromotedCandidateDecisionStatus.SOURCE_RESULT_BLOCKED,
        )

    def test_batch_cannot_be_upgraded_to_actionable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = _panel(Path(tmp))
            signal_session = (
                panel.source_panel.results[0].feature_vector.signal_session
            )
            batch = PromotedResearchIntentService().generate(
                source_panel=panel,
                config=_permissive_config(),
                entry_session=signal_session + timedelta(days=1),
                initial_capital=Decimal("100000"),
            )
            values = {
                value.name: getattr(batch, value.name)
                for value in dataclasses.fields(
                    VerifiedPromotedResearchIntentBatch
                )
            }
            values["actionable"] = True
            with self.assertRaises(PromotedIntentError):
                VerifiedPromotedResearchIntentBatch(**values)

    def test_decisions_make_no_probability_claim(self) -> None:
        field_names = {
            value.name
            for value in dataclasses.fields(
                PromotedCandidateDecision
            )
        }
        self.assertNotIn("confidence", field_names)
        self.assertNotIn("probability", field_names)

    def test_service_exposes_only_generate(self) -> None:
        public = {
            value
            for value in dir(PromotedResearchIntentService)
            if not value.startswith("_")
        }
        self.assertEqual(public, {"generate"})


if __name__ == "__main__":
    unittest.main()
