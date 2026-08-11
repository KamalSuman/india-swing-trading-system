from __future__ import annotations

import inspect
import unittest
from unittest.mock import patch
from datetime import timedelta
from decimal import Decimal, getcontext

from india_swing.corporate_actions.models import CorporateActionSnapshot
from india_swing.evaluation.dataset_assembly import EffectiveTickSize
from india_swing.forward_paper.adjustments import (
    ForwardPaperCorporateActionIdentityBinding,
    build_forward_paper_adjusted_history_window,
)
from india_swing.forward_paper.feature_inputs import (
    build_forward_paper_feature_input_window,
)
from india_swing.forward_paper import features as feature_module
from india_swing.forward_paper.features import (
    FORWARD_PAPER_TECHNICAL_FEATURE_CONFIG,
    ForwardPaperTechnicalFeatureStatus,
    build_forward_paper_technical_feature_window,
)
from india_swing.reference.models import ReferenceReadiness

from tests.test_forward_paper_history import (
    ISIN_A,
    ISIN_B,
    _dates,
    _window_for,
)
from tests.test_nse_archive_research_dataset import _baseline_dataset, _fake_sha256
from tests.test_nse_archive_research_identity import _record, _session


class ForwardPaperTechnicalFeatureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dates = _dates()
        replay_sessions = []
        for index, market_session in enumerate(self.dates):
            base_a = Decimal("100") + Decimal(index)
            base_b = Decimal("200") + Decimal(index) * Decimal("0.5")
            record_a = self._priced_record(
                market_session, "AAA", ISIN_A, base_a, index
            )
            record_b = self._priced_record(
                market_session, "BBB", ISIN_B, base_b, index
            )
            replay_sessions.append(_session(market_session, (record_a, record_b)))
        raw, _ = _window_for(
            _baseline_dataset(), tuple(replay_sessions), self.dates
        )
        self.cutoff = raw.spec.decision_cutoff
        candidates = tuple(
            value for value in raw.outcomes if hasattr(value, "research_identity_id")
        )
        bindings = tuple(
            ForwardPaperCorporateActionIdentityBinding(
                research_identity_id=value.research_identity_id,
                stable_instrument_id=_fake_sha256(
                    f"feature-stable-instrument-{value.research_identity_id}"
                ),
                stable_listing_id=_fake_sha256(
                    f"feature-stable-listing-{value.research_identity_id}"
                ),
                knowledge_time=self.cutoff - timedelta(days=2),
                source_artifact_id=_fake_sha256(
                    f"feature-identity-source-{value.research_identity_id}"
                ),
            )
            for value in candidates
        )
        action_source = _fake_sha256("empty-action-source")
        snapshot = CorporateActionSnapshot(
            cutoff=self.cutoff - timedelta(days=1),
            coverage_start=self.dates[0],
            coverage_end=self.dates[-1],
            source_artifact_ids=(action_source,),
            events=(),
            readiness=ReferenceReadiness.POINT_IN_TIME_VERIFIED,
            complete=True,
            actionable=True,
            reason_codes=(),
        )
        adjusted = build_forward_paper_adjusted_history_window(
            source_window=raw,
            corporate_actions=snapshot,
            identity_bindings=bindings,
        )
        ticks = []
        for candidate in adjusted.outcomes:
            if not hasattr(candidate, "observations"):
                continue
            binding = candidate.identity_binding
            session = candidate.observations[-1].source_observation.market_session
            ticks.append(
                EffectiveTickSize(
                    instrument_id=binding.stable_instrument_id,
                    listing_id=binding.stable_listing_id,
                    effective_from_session=session,
                    effective_to_exclusive=session + timedelta(days=1),
                    tick_size=Decimal("0.05"),
                    knowledge_time=self.cutoff - timedelta(hours=2),
                    source_snapshot_id=_fake_sha256(
                        f"feature-tick-{binding.stable_listing_id}-{session}"
                    ),
                    readiness=ReferenceReadiness.POINT_IN_TIME_VERIFIED,
                )
            )
        self.inputs = build_forward_paper_feature_input_window(
            source_window=adjusted,
            tick_specifications=tuple(ticks),
        )

    @staticmethod
    def _priced_record(market_session, symbol, isin, close, index):
        previous = close - Decimal("1")
        open_price = close - Decimal("0.25")
        high = close + Decimal("1.25")
        low = close - Decimal("1.25")
        return _record(
            market_session,
            symbol=symbol,
            validated_isin=isin,
            previous_close=previous,
            open=open_price,
            high=high,
            low=low,
            last=close,
            close=close,
            average_price=close,
            volume=1000 + index * 10,
            turnover_lacs=close * Decimal(1000 + index * 10) / Decimal("100000"),
            trade_count=100 + index,
        )

    def test_configuration_explicitly_resolves_60_bars_to_59_return_intervals(self) -> None:
        config = FORWARD_PAPER_TECHNICAL_FEATURE_CONFIG
        self.assertEqual(config.minimum_history_sessions, 60)
        self.assertEqual(config.long_return_sessions, 59)
        self.assertEqual(config.drawdown_sessions, 60)
        self.assertEqual(config.tick_history_sessions, 1)
        self.assertEqual(config.required_history_sessions, 60)

    def test_computes_existing_technical_vector_contract_for_each_candidate(self) -> None:
        result = build_forward_paper_technical_feature_window(
            source_window=self.inputs
        )
        self.assertEqual(result.computed_feature_count, 2)
        self.assertEqual(result.blocked_feature_count, 0)
        self.assertTrue(result.resolved_histories_feature_complete)
        for item in result.results:
            self.assertIs(
                item.status,
                ForwardPaperTechnicalFeatureStatus.FEATURE_VECTOR_COMPUTED_COLLECTION_ONLY,
            )
            vector = item.feature_vector
            self.assertIsNotNone(vector)
            source = item.source_outcome
            self.assertEqual(vector.source_history_id, source.candidate_id)
            self.assertEqual(len(vector.input_bar_ids), 60)
            expected = (
                source.bars[-1].adjusted_close
                / source.bars[0].adjusted_close
                - Decimal("1")
            )
            self.assertEqual(vector.return_long, expected)
            self.assertEqual(vector.signal_tick_size, Decimal("0.05"))
            self.assertEqual(vector.tick_change_count, 0)
        result.verify_content_identity()

    def test_feature_identity_does_not_depend_on_global_decimal_context(self) -> None:
        baseline = build_forward_paper_technical_feature_window(
            source_window=self.inputs
        )
        original = getcontext().copy()
        try:
            getcontext().prec = 6
            constrained = build_forward_paper_technical_feature_window(
                source_window=self.inputs
            )
        finally:
            getcontext().prec = original.prec
            getcontext().rounding = original.rounding
        self.assertEqual(baseline.window_id, constrained.window_id)

    def test_source_tick_veto_remains_blocked_without_feature(self) -> None:
        incomplete_inputs = build_forward_paper_feature_input_window(
            source_window=self.inputs.source_window,
            tick_specifications=self.inputs.tick_specifications[1:],
        )
        result = build_forward_paper_technical_feature_window(
            source_window=incomplete_inputs
        )
        blocked = tuple(
            value
            for value in result.results
            if value.status is ForwardPaperTechnicalFeatureStatus.SOURCE_INPUT_VETO
        )
        self.assertEqual(len(blocked), 1)
        self.assertIsNone(blocked[0].feature_vector)
        self.assertFalse(result.resolved_histories_feature_complete)

    def test_degenerate_history_is_an_explicit_veto_not_a_partial_vector(self) -> None:
        with patch.object(
            feature_module,
            "_compute_vector",
            side_effect=feature_module._DegenerateInput,
        ):
            result = build_forward_paper_technical_feature_window(
                source_window=self.inputs
            )
        self.assertEqual(result.computed_feature_count, 0)
        self.assertEqual(result.blocked_feature_count, 2)
        self.assertTrue(
            all(
                value.status
                is ForwardPaperTechnicalFeatureStatus.DEGENERATE_INPUT_VETO
                and value.feature_vector is None
                for value in result.results
            )
        )

    def test_output_never_grants_model_ranking_alert_or_execution_authority(self) -> None:
        result = build_forward_paper_technical_feature_window(
            source_window=self.inputs
        )
        self.assertTrue(result.collection_only)
        for name in (
            "training_eligible",
            "feature_eligible",
            "label_eligible",
            "ranking_eligible",
            "alert_eligible",
            "paper_trade_eligible",
            "notification_eligible",
            "execution_eligible",
        ):
            self.assertFalse(getattr(result, name))

    def test_module_has_no_io_clock_provider_model_or_execution_capability(self) -> None:
        source = inspect.getsource(feature_module).lower()
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
