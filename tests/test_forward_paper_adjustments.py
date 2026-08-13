from __future__ import annotations

import inspect
import unittest
from datetime import timedelta
from decimal import Decimal, getcontext

from india_swing.corporate_actions.models import (
    CorporateActionEvent,
    CorporateActionSnapshot,
    CorporateActionStatus,
    CorporateActionType,
)
from india_swing.forward_paper import adjustments as adjustment_module
from india_swing.forward_paper.adjustments import (
    ForwardPaperAdjustedCandidate,
    ForwardPaperAdjustmentError,
    ForwardPaperAdjustmentVeto,
    ForwardPaperAdjustmentVetoReason,
    ForwardPaperCorporateActionIdentityBinding,
    build_forward_paper_adjusted_history_window,
)
from india_swing.evaluation.nse_archive_research_identity import (
    research_identity_id_for_isin,
)
from india_swing.reference.models import ReferenceReadiness

from tests.test_forward_paper_history import (
    ISIN_A,
    ISIN_B,
    _dates,
    _two_identity_replay_sessions,
    _window_for,
)
from tests.test_nse_archive_research_dataset import _baseline_dataset, _fake_sha256


class ForwardPaperAdjustmentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dates = _dates()
        dataset = _baseline_dataset()
        self.window, _ = _window_for(
            dataset,
            _two_identity_replay_sessions(self.dates),
            self.dates,
        )
        candidates = tuple(
            value
            for value in self.window.outcomes
            if value.__class__.__name__ == "ForwardPaperHistoryCandidate"
        )
        self.candidate_a = next(
            value
            for value in candidates
            if value.research_identity_id == research_identity_id_for_isin(ISIN_A)
        )
        self.candidate_b = next(
            value
            for value in candidates
            if value.research_identity_id == research_identity_id_for_isin(ISIN_B)
        )
        self.artifact_id = _fake_sha256("corporate-action-source")
        self.stable_a = _fake_sha256("stable-instrument-a")
        self.listing_a = _fake_sha256("stable-listing-a")
        self.stable_b = _fake_sha256("stable-instrument-b")
        self.listing_b = _fake_sha256("stable-listing-b")
        self.binding_a = self._binding(
            self.candidate_a.research_identity_id, self.stable_a, self.listing_a
        )
        self.binding_b = self._binding(
            self.candidate_b.research_identity_id, self.stable_b, self.listing_b
        )
        self.effective_session = self.dates[30]
        self.split = CorporateActionEvent(
            stable_instrument_id=self.stable_a,
            stable_listing_id=self.listing_a,
            action_type=CorporateActionType.SPLIT,
            status=CorporateActionStatus.CONFIRMED,
            effective_session=self.effective_session,
            announcement_time=self.window.spec.decision_cutoff - timedelta(days=10),
            knowledge_time=self.window.spec.decision_cutoff - timedelta(days=9),
            source_artifact_id=self.artifact_id,
            source_row_id=_fake_sha256("split-row"),
            pre_action_shares=Decimal("1"),
            post_action_shares=Decimal("2"),
        )
        self.snapshot = self._snapshot((self.split,))

    def _binding(self, research_id, stable_id, listing_id, **overrides):
        values = dict(
            research_identity_id=research_id,
            stable_instrument_id=stable_id,
            stable_listing_id=listing_id,
            knowledge_time=self.window.spec.decision_cutoff - timedelta(days=1),
            source_artifact_id=_fake_sha256(f"binding-{research_id}"),
        )
        values.update(overrides)
        return ForwardPaperCorporateActionIdentityBinding(**values)

    def _snapshot(self, events, **overrides):
        values = dict(
            cutoff=self.window.spec.decision_cutoff - timedelta(hours=1),
            coverage_start=self.dates[0],
            coverage_end=self.dates[-1],
            source_artifact_ids=(self.artifact_id,),
            events=events,
            readiness=ReferenceReadiness.POINT_IN_TIME_VERIFIED,
            complete=True,
            actionable=True,
            reason_codes=(),
        )
        values.update(overrides)
        return CorporateActionSnapshot(**values)

    def _build(self, bindings=None, snapshot=None):
        return build_forward_paper_adjusted_history_window(
            source_window=self.window,
            corporate_actions=self.snapshot if snapshot is None else snapshot,
            identity_bindings=(self.binding_a, self.binding_b)
            if bindings is None
            else bindings,
        )

    def test_split_adjusts_only_pre_effective_prices_and_inverse_volume(self) -> None:
        result = self._build()
        adjusted_a = next(
            value
            for value in result.outcomes
            if type(value) is ForwardPaperAdjustedCandidate
            and value.source_candidate.research_identity_id
            == self.candidate_a.research_identity_id
        )
        before = adjusted_a.observations[29]
        on_date = adjusted_a.observations[30]
        self.assertEqual(before.price_factor, Decimal("0.5"))
        self.assertEqual(before.volume_factor, Decimal("2"))
        self.assertEqual(
            before.adjusted_close,
            before.source_observation.replay_record.close * Decimal("0.5"),
        )
        self.assertEqual(
            before.adjusted_volume,
            Decimal(before.source_observation.replay_record.volume) * Decimal("2"),
        )
        self.assertEqual(before.applied_event_ids, (self.split.event_id,))
        self.assertEqual(on_date.price_factor, Decimal("1"))
        self.assertEqual(on_date.volume_factor, Decimal("1"))
        self.assertEqual(on_date.applied_event_ids, ())
        self.assertTrue(result.resolved_histories_adjustment_complete)
        self.assertEqual(result.adjusted_candidate_count, 2)
        result.verify_content_identity()

    def test_unaffected_identity_remains_exactly_unscaled(self) -> None:
        result = self._build()
        adjusted_b = next(
            value
            for value in result.outcomes
            if type(value) is ForwardPaperAdjustedCandidate
            and value.source_candidate.research_identity_id
            == self.candidate_b.research_identity_id
        )
        self.assertTrue(all(value.price_factor == Decimal("1") for value in adjusted_b.observations))
        self.assertTrue(all(value.applied_event_ids == () for value in adjusted_b.observations))

    def test_missing_binding_becomes_auditable_veto_not_silent_drop(self) -> None:
        result = self._build(bindings=(self.binding_a,))
        veto = next(value for value in result.outcomes if type(value) is ForwardPaperAdjustmentVeto)
        self.assertIs(
            veto.reason,
            ForwardPaperAdjustmentVetoReason.IDENTITY_BINDING_MISSING,
        )
        self.assertEqual(veto.source_candidate.candidate_id, self.candidate_b.candidate_id)
        self.assertEqual(result.adjustment_veto_count, 1)
        self.assertFalse(result.resolved_histories_adjustment_complete)

    def test_duplicate_and_foreign_bindings_fail_closed(self) -> None:
        with self.assertRaises(ForwardPaperAdjustmentError):
            self._build(bindings=(self.binding_a, self.binding_a))
        foreign = self._binding(
            _fake_sha256("foreign-research-identity"),
            _fake_sha256("foreign-stable"),
            _fake_sha256("foreign-listing"),
        )
        with self.assertRaises(ForwardPaperAdjustmentError):
            self._build(bindings=(self.binding_a, self.binding_b, foreign))

    def test_binding_input_order_does_not_change_content_identity(self) -> None:
        first = self._build(bindings=(self.binding_a, self.binding_b))
        second = self._build(bindings=(self.binding_b, self.binding_a))
        self.assertEqual(first.window_id, second.window_id)
        self.assertEqual(first.identity_bindings, second.identity_bindings)

    def test_future_known_binding_and_snapshot_fail_closed(self) -> None:
        future_binding = self._binding(
            self.candidate_a.research_identity_id,
            self.stable_a,
            self.listing_a,
            knowledge_time=self.window.spec.decision_cutoff + timedelta(seconds=1),
        )
        with self.assertRaises(ForwardPaperAdjustmentError):
            self._build(bindings=(future_binding, self.binding_b))
        future_snapshot = self._snapshot(
            (self.split,),
            cutoff=self.window.spec.decision_cutoff + timedelta(seconds=1),
        )
        with self.assertRaises(ForwardPaperAdjustmentError):
            self._build(snapshot=future_snapshot)

    def test_incomplete_or_collection_only_snapshot_fails_closed(self) -> None:
        blocked = self._snapshot(
            (self.split,),
            readiness=ReferenceReadiness.COLLECTION_ONLY,
            complete=False,
            actionable=False,
            reason_codes=("SOURCE_INCOMPLETE",),
        )
        with self.assertRaises(ForwardPaperAdjustmentError):
            self._build(snapshot=blocked)
        short = self._snapshot((self.split,), coverage_start=self.dates[1])
        with self.assertRaises(ForwardPaperAdjustmentError):
            self._build(snapshot=short)

    def test_cash_dividend_vetoes_only_the_affected_candidate(self) -> None:
        dividend = CorporateActionEvent(
            stable_instrument_id=self.stable_a,
            stable_listing_id=self.listing_a,
            action_type=CorporateActionType.CASH_DIVIDEND,
            status=CorporateActionStatus.CONFIRMED,
            effective_session=self.effective_session,
            announcement_time=self.window.spec.decision_cutoff - timedelta(days=10),
            knowledge_time=self.window.spec.decision_cutoff - timedelta(days=9),
            source_artifact_id=self.artifact_id,
            source_row_id=_fake_sha256("dividend-row"),
            cash_amount_per_share=Decimal("5"),
            currency="INR",
        )
        snapshot = self._snapshot((dividend,))
        result = self._build(snapshot=snapshot)
        veto = next(
            value
            for value in result.outcomes
            if type(value) is ForwardPaperAdjustmentVeto
        )
        self.assertEqual(
            veto.reason,
            ForwardPaperAdjustmentVetoReason.CORPORATE_ACTION_POLICY_BLOCKED,
        )
        self.assertEqual(
            veto.source_candidate.candidate_id,
            self.candidate_a.candidate_id,
        )
        self.assertEqual(result.adjusted_candidate_count, 1)
        self.assertEqual(result.adjustment_veto_count, 1)
        self.assertFalse(result.resolved_histories_adjustment_complete)
        result.verify_content_identity()

    def test_decimal_results_do_not_depend_on_global_context(self) -> None:
        baseline = self._build()
        original = getcontext().copy()
        try:
            getcontext().prec = 6
            constrained = self._build()
        finally:
            getcontext().prec = original.prec
            getcontext().rounding = original.rounding
        self.assertEqual(constrained.window_id, baseline.window_id)

    def test_output_is_strictly_collection_only(self) -> None:
        result = self._build()
        self.assertTrue(result.collection_only)
        for name in (
            "training_eligible",
            "feature_eligible",
            "ranking_eligible",
            "alert_eligible",
            "paper_trade_eligible",
            "notification_eligible",
            "execution_eligible",
        ):
            self.assertFalse(getattr(result, name))

    def test_module_has_no_io_clock_broker_cloud_or_model_capability(self) -> None:
        source = inspect.getsource(adjustment_module).lower()
        for token in (
            "open(",
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

    def test_rejection_message_does_not_leak_input_ids(self) -> None:
        foreign = self._binding(
            _fake_sha256("secret-foreign"),
            _fake_sha256("secret-stable"),
            _fake_sha256("secret-listing"),
        )
        with self.assertRaises(ForwardPaperAdjustmentError) as caught:
            self._build(bindings=(self.binding_a, self.binding_b, foreign))
        self.assertNotIn(foreign.research_identity_id, str(caught.exception))
        self.assertIsNone(caught.exception.__cause__)


if __name__ == "__main__":
    unittest.main()
