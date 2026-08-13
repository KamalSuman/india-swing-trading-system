from __future__ import annotations

import inspect
import unittest
from datetime import timedelta
from decimal import Decimal

from india_swing.corporate_actions.models import (
    CorporateActionEvent,
    CorporateActionStatus,
    CorporateActionType,
)
from india_swing.evaluation.dataset_assembly import EffectiveTickSize
from india_swing.forward_paper import feature_inputs as feature_input_module
from india_swing.forward_paper.feature_inputs import (
    ForwardPaperFeatureInputCandidate,
    ForwardPaperFeatureInputError,
    ForwardPaperFeatureInputVeto,
    ForwardPaperFeatureInputVetoReason,
    build_forward_paper_feature_input_window,
)
from india_swing.reference.models import ReferenceReadiness

from tests import test_forward_paper_adjustments as adjustment_fixtures
from tests.test_nse_archive_research_dataset import _fake_sha256


class ForwardPaperFeatureInputTests(unittest.TestCase):
    def setUp(self) -> None:
        fixture = adjustment_fixtures.ForwardPaperAdjustmentTests()
        fixture.setUp()
        self.fixture = fixture
        self.adjusted = fixture._build()
        self.ticks = self._ticks()

    def _ticks(self):
        values = []
        for outcome in self.adjusted.outcomes:
            if outcome.__class__.__name__ != "ForwardPaperAdjustedCandidate":
                continue
            binding = outcome.identity_binding
            session = outcome.observations[-1].source_observation.market_session
            values.append(
                EffectiveTickSize(
                    instrument_id=binding.stable_instrument_id,
                    listing_id=binding.stable_listing_id,
                    effective_from_session=session,
                    effective_to_exclusive=session + timedelta(days=1),
                    tick_size=Decimal("0.05"),
                    knowledge_time=(
                        self.adjusted.source_window.spec.decision_cutoff
                        - timedelta(days=1)
                    ),
                    source_snapshot_id=_fake_sha256(
                        f"tick-{binding.stable_listing_id}-{session.isoformat()}"
                    ),
                    readiness=ReferenceReadiness.POINT_IN_TIME_VERIFIED,
                )
            )
        return tuple(values)

    def _build(self, ticks=None, adjusted=None):
        return build_forward_paper_feature_input_window(
            source_window=self.adjusted if adjusted is None else adjusted,
            tick_specifications=self.ticks if ticks is None else ticks,
        )

    def test_exact_signal_tick_assembles_two_60_bar_candidates(self) -> None:
        result = self._build()
        self.assertEqual(result.assembled_candidate_count, 2)
        self.assertEqual(result.veto_count, 0)
        self.assertTrue(result.resolved_histories_input_complete)
        candidates = tuple(
            value
            for value in result.outcomes
            if type(value) is ForwardPaperFeatureInputCandidate
        )
        self.assertEqual(tuple(len(value.bars) for value in candidates), (60, 60))
        for candidate in candidates:
            for bar, adjusted in zip(
                candidate.bars,
                candidate.source_candidate.observations,
                strict=True,
            ):
                self.assertIs(bar.adjusted_observation, adjusted)
                self.assertEqual(bar.market_session, adjusted.source_observation.market_session)
            self.assertTrue(
                all(bar.tick_specification is None for bar in candidate.bars[:-1])
            )
            self.assertEqual(candidate.bars[-1].tick_size, Decimal("0.05"))
        result.verify_content_identity()

    def test_missing_tick_vetoes_only_affected_candidate_without_fallback(self) -> None:
        missing = self.ticks[0]
        result = self._build(
            ticks=tuple(value for value in self.ticks if value is not missing)
        )
        veto = next(
            value
            for value in result.outcomes
            if type(value) is ForwardPaperFeatureInputVeto
            and value.reason
            is ForwardPaperFeatureInputVetoReason.EXACT_SESSION_TICK_MISSING
        )
        self.assertEqual(veto.affected_sessions, (missing.effective_from_session,))
        self.assertEqual(veto.evidence_tick_specification_ids, ())
        self.assertEqual(result.assembled_candidate_count, 1)
        self.assertFalse(result.resolved_histories_input_complete)

    def test_ambiguous_tick_is_vetoed_and_binds_both_specifications(self) -> None:
        original = self.ticks[0]
        competing = EffectiveTickSize(
            instrument_id=original.instrument_id,
            listing_id=original.listing_id,
            effective_from_session=original.effective_from_session,
            effective_to_exclusive=original.effective_to_exclusive,
            tick_size=Decimal("0.10"),
            knowledge_time=original.knowledge_time,
            source_snapshot_id=_fake_sha256("competing-tick"),
            readiness=ReferenceReadiness.POINT_IN_TIME_VERIFIED,
        )
        result = self._build(ticks=self.ticks + (competing,))
        veto = next(
            value
            for value in result.outcomes
            if type(value) is ForwardPaperFeatureInputVeto
            and value.reason
            is ForwardPaperFeatureInputVetoReason.EXACT_SESSION_TICK_AMBIGUOUS
        )
        self.assertEqual(
            veto.evidence_tick_specification_ids,
            tuple(sorted((original.specification_id, competing.specification_id))),
        )

    def test_unverified_and_future_known_ticks_are_explicit_vetoes(self) -> None:
        original = self.ticks[0]
        unverified = EffectiveTickSize(
            instrument_id=original.instrument_id,
            listing_id=original.listing_id,
            effective_from_session=original.effective_from_session,
            effective_to_exclusive=original.effective_to_exclusive,
            tick_size=original.tick_size,
            knowledge_time=original.knowledge_time,
            source_snapshot_id=_fake_sha256("unverified-tick"),
            readiness=ReferenceReadiness.COLLECTION_ONLY,
        )
        unverified_ticks = (unverified,) + self.ticks[1:]
        unverified_result = self._build(ticks=unverified_ticks)
        self.assertTrue(
            any(
                type(value) is ForwardPaperFeatureInputVeto
                and value.reason
                is ForwardPaperFeatureInputVetoReason.EXACT_SESSION_TICK_UNVERIFIED
                for value in unverified_result.outcomes
            )
        )
        future = EffectiveTickSize(
            instrument_id=original.instrument_id,
            listing_id=original.listing_id,
            effective_from_session=original.effective_from_session,
            effective_to_exclusive=original.effective_to_exclusive,
            tick_size=original.tick_size,
            knowledge_time=(
                self.adjusted.source_window.spec.decision_cutoff
                + timedelta(seconds=1)
            ),
            source_snapshot_id=_fake_sha256("future-tick"),
            readiness=ReferenceReadiness.POINT_IN_TIME_VERIFIED,
        )
        future_result = self._build(ticks=(future,) + self.ticks[1:])
        self.assertTrue(
            any(
                type(value) is ForwardPaperFeatureInputVeto
                and value.reason
                is ForwardPaperFeatureInputVetoReason.EXACT_SESSION_TICK_FUTURE_KNOWN
                for value in future_result.outcomes
            )
        )

    def test_foreign_tick_fails_instead_of_expanding_or_reselecting_universe(self) -> None:
        foreign = EffectiveTickSize(
            instrument_id=_fake_sha256("foreign-instrument"),
            listing_id=_fake_sha256("foreign-listing"),
            effective_from_session=self.fixture.dates[0],
            effective_to_exclusive=self.fixture.dates[0] + timedelta(days=1),
            tick_size=Decimal("0.05"),
            knowledge_time=self.ticks[0].knowledge_time,
            source_snapshot_id=_fake_sha256("foreign-tick"),
            readiness=ReferenceReadiness.POINT_IN_TIME_VERIFIED,
        )
        with self.assertRaises(ForwardPaperFeatureInputError):
            self._build(ticks=self.ticks + (foreign,))

    def test_tick_input_order_is_canonical_and_deterministic(self) -> None:
        first = self._build(ticks=self.ticks)
        second = self._build(ticks=tuple(reversed(self.ticks)))
        self.assertEqual(first.window_id, second.window_id)
        self.assertEqual(first.tick_specifications, second.tick_specifications)

    def test_adjustment_veto_is_preserved_as_source_veto(self) -> None:
        partially_adjusted = self.fixture._build(bindings=(self.fixture.binding_a,))
        relevant_ticks = tuple(
            value
            for value in self.ticks
            if value.instrument_id == self.fixture.stable_a
        )
        result = self._build(adjusted=partially_adjusted, ticks=relevant_ticks)
        self.assertTrue(
            any(
                type(value) is ForwardPaperFeatureInputVeto
                and value.reason
                is ForwardPaperFeatureInputVetoReason.SOURCE_ADJUSTMENT_VETO
                for value in result.outcomes
            )
        )

    def test_policy_veto_preserves_its_pinned_tick_as_unused_evidence(self) -> None:
        dividend = CorporateActionEvent(
            stable_instrument_id=self.fixture.stable_a,
            stable_listing_id=self.fixture.listing_a,
            action_type=CorporateActionType.CASH_DIVIDEND,
            status=CorporateActionStatus.CONFIRMED,
            effective_session=self.fixture.effective_session,
            announcement_time=(
                self.fixture.window.spec.decision_cutoff - timedelta(days=10)
            ),
            knowledge_time=(
                self.fixture.window.spec.decision_cutoff - timedelta(days=9)
            ),
            source_artifact_id=self.fixture.artifact_id,
            source_row_id=_fake_sha256("feature-input-dividend-row"),
            cash_amount_per_share=Decimal("5"),
            currency="INR",
        )
        partially_adjusted = self.fixture._build(
            snapshot=self.fixture._snapshot((dividend,))
        )
        result = self._build(adjusted=partially_adjusted, ticks=self.ticks)
        self.assertEqual(result.assembled_candidate_count, 1)
        self.assertEqual(result.veto_count, 1)
        self.assertTrue(
            any(
                type(value) is ForwardPaperFeatureInputVeto
                and value.reason
                is ForwardPaperFeatureInputVetoReason.SOURCE_ADJUSTMENT_VETO
                for value in result.outcomes
            )
        )
        result.verify_content_identity()

    def test_output_has_no_feature_ranking_alert_or_execution_authority(self) -> None:
        result = self._build()
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
        source = inspect.getsource(feature_input_module).lower()
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
