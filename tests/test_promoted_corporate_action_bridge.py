from __future__ import annotations

import dataclasses
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from india_swing.corporate_actions.models import (
    CorporateActionEvent,
    CorporateActionSnapshot,
    CorporateActionStatus,
    CorporateActionType,
)
from india_swing.corporate_actions.promoted_adjustments import (
    PromotedCorporateActionAdjustmentService,
    PromotedCorporateActionAdjustmentStatus,
    PromotedCorporateActionBridgeError,
    VerifiedPromotedCorporateActionAdjustmentPanel,
)
from india_swing.reference.models import ReferenceReadiness
from tests.test_promoted_identity_session_universe import D1, D2
from tests.test_promoted_stable_listing_history import (
    PANEL_CUTOFF,
    _two_session_fixture,
)


UTC = timezone.utc
BRIDGE_CUTOFF = PANEL_CUTOFF + timedelta(hours=1)
SOURCE_ID = "8" * 64
ROW_ID = "9" * 64


def _event(
    panel,
    *,
    action_type: CorporateActionType = CorporateActionType.SPLIT,
    stable_listing_id: str | None | object = ...,
) -> CorporateActionEvent:
    history = panel.histories[0]
    listing_id = (
        history.stable_listing_id
        if stable_listing_id is ...
        else stable_listing_id
    )
    share_terms = action_type in {
        CorporateActionType.SPLIT,
        CorporateActionType.BONUS,
    }
    dividend = action_type is CorporateActionType.CASH_DIVIDEND
    knowledge_time = PANEL_CUTOFF + timedelta(minutes=10)
    return CorporateActionEvent(
        stable_instrument_id=history.stable_instrument_id,
        stable_listing_id=listing_id,
        action_type=action_type,
        status=CorporateActionStatus.CONFIRMED,
        effective_session=D2,
        announcement_time=knowledge_time - timedelta(minutes=5),
        knowledge_time=knowledge_time,
        source_artifact_id=SOURCE_ID,
        source_row_id=ROW_ID,
        pre_action_shares=Decimal("1") if share_terms else None,
        post_action_shares=Decimal("2") if share_terms else None,
        cash_amount_per_share=Decimal("5") if dividend else None,
        currency="INR" if dividend else None,
        supersedes_event_id=None,
    )


def _snapshot(
    *events: CorporateActionEvent,
    coverage_start=D1,
    coverage_end=D2,
    cutoff=PANEL_CUTOFF + timedelta(minutes=20),
    readiness: ReferenceReadiness = ReferenceReadiness.SYNTHETIC_TEST,
    complete: bool = True,
    actionable: bool = True,
    reason_codes: tuple[str, ...] = (),
) -> CorporateActionSnapshot:
    return CorporateActionSnapshot(
        cutoff=cutoff,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        source_artifact_ids=(SOURCE_ID,),
        events=tuple(
            sorted(
                events,
                key=lambda value: (
                    value.knowledge_time,
                    value.effective_session,
                    value.event_id,
                ),
            )
        ),
        readiness=readiness,
        complete=complete,
        actionable=actionable,
        reason_codes=reason_codes,
    )


def _materialize(
    root: Path,
    *,
    omit_reliance_bar_on=None,
    conflict_reliance_bar_on=None,
    snapshot_factory=None,
):
    _, _, source_panel = _two_session_fixture(
        root,
        omit_reliance_bar_on=omit_reliance_bar_on,
        conflict_reliance_bar_on=conflict_reliance_bar_on,
    )
    corporate_actions = (
        snapshot_factory(source_panel)
        if snapshot_factory is not None
        else _snapshot(_event(source_panel))
    )
    bridge = PromotedCorporateActionAdjustmentService().materialize(
        source_panel=source_panel,
        corporate_actions=corporate_actions,
        cutoff=BRIDGE_CUTOFF,
    )
    return source_panel, corporate_actions, bridge


class PromotedCorporateActionBridgeAcceptanceTests(unittest.TestCase):
    def test_applies_split_using_shared_adjustment_factor_policy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source, corporate_actions, bridge = _materialize(Path(tmp))
            self.assertTrue(bridge.resolved_histories_adjustment_complete)
            self.assertEqual(len(bridge.results), 1)
            result = bridge.results[0]
            self.assertIs(
                result.status,
                PromotedCorporateActionAdjustmentStatus.ADJUSTED_HISTORY_BUILT_COLLECTION_ONLY,
            )
            self.assertIsNotNone(result.adjusted_history)
            adjusted = result.adjusted_history
            assert adjusted is not None
            self.assertEqual(adjusted.corporate_action_snapshot_id, corporate_actions.snapshot_id)
            self.assertEqual(
                tuple(value.source_bar.session for value in adjusted.bars),
                source.sessions,
            )
            self.assertEqual(adjusted.bars[0].price_factor, Decimal("0.5"))
            self.assertEqual(adjusted.bars[0].volume_factor, Decimal("2"))
            self.assertEqual(adjusted.bars[1].price_factor, Decimal("1"))
            self.assertEqual(
                adjusted.bars[0].applied_event_ids,
                (corporate_actions.active_events[0].event_id,),
            )
            bridge.verify_content_identity()

    def test_identity_bindings_use_exact_session_universe_lineage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source, _, bridge = _materialize(Path(tmp))
            result = bridge.results[0]
            self.assertEqual(
                tuple(value.identity_snapshot_id for value in result.identity_bindings),
                tuple(
                    value.frame.universe.universe_id
                    for value in source.tick_snapshots
                ),
            )
            self.assertEqual(
                tuple(value.raw_bar_id for value in result.identity_bindings),
                tuple(
                    value.raw_bar_id
                    for value in source.histories[0].observations
                ),
            )

    def test_bridge_never_grants_feature_signal_or_execution_authority(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, bridge = _materialize(Path(tmp))
            self.assertIs(bridge.readiness, ReferenceReadiness.COLLECTION_ONLY)
            self.assertFalse(bridge.actionable)
            self.assertFalse(bridge.training_eligible)
            self.assertFalse(bridge.feature_eligible)
            self.assertFalse(bridge.alert_eligible)
            self.assertFalse(bridge.execution_eligible)
            self.assertTrue(bridge.source_panel.unassigned_entries)
            self.assertTrue(bridge.source_panel.orphan_bars)
            self.assertTrue(bridge.resolved_histories_adjustment_complete)

    def test_adjusted_corpus_bar_does_not_fabricate_report_only_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, bridge = _materialize(Path(tmp))
            adjusted = bridge.results[0].adjusted_history
            assert adjusted is not None
            bar = adjusted.bars[0]
            self.assertIsNotNone(bar.source_bar)
            for forbidden in (
                "traded_value",
                "trade_count",
                "delivery_quantity",
                "delivery_percent",
                "previous_close",
                "last",
            ):
                with self.subTest(forbidden=forbidden):
                    self.assertFalse(hasattr(bar, forbidden))


class PromotedCorporateActionBridgeBlockingTests(unittest.TestCase):
    def test_raw_history_gap_is_retained_as_blocked_result(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, bridge = _materialize(
                Path(tmp),
                omit_reliance_bar_on=D2,
            )
            result = bridge.results[0]
            self.assertIs(
                result.status,
                PromotedCorporateActionAdjustmentStatus.RAW_HISTORY_GAP_BLOCKED,
            )
            self.assertIsNone(result.adjusted_history)
            self.assertEqual(result.identity_bindings, ())
            self.assertFalse(bridge.resolved_histories_adjustment_complete)

    def test_non_actionable_snapshot_blocks_without_discarding_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source, _, bridge = _materialize(
                Path(tmp),
                snapshot_factory=lambda panel: _snapshot(
                    _event(panel),
                    readiness=ReferenceReadiness.COLLECTION_ONLY,
                    complete=False,
                    actionable=False,
                    reason_codes=("SOURCE_COVERAGE_INCOMPLETE",),
                ),
            )
            self.assertIs(
                bridge.results[0].status,
                PromotedCorporateActionAdjustmentStatus.CORPORATE_ACTION_EVIDENCE_NOT_ACTIONABLE,
            )
            self.assertEqual(
                bridge.results[0].source_history.history_id,
                source.histories[0].history_id,
            )

    def test_identity_conflict_is_not_treated_as_a_gap_or_adjusted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, bridge = _materialize(
                Path(tmp),
                conflict_reliance_bar_on=D2,
            )
            result = bridge.results[0]
            self.assertIs(
                result.status,
                PromotedCorporateActionAdjustmentStatus.RAW_HISTORY_IDENTITY_CONFLICT_BLOCKED,
            )
            self.assertIsNone(result.adjusted_history)
            self.assertEqual(result.identity_bindings, ())

    def test_incomplete_coverage_is_an_explicit_blocker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, bridge = _materialize(
                Path(tmp),
                snapshot_factory=lambda panel: _snapshot(
                    _event(panel),
                    coverage_start=D2,
                ),
            )
            self.assertIs(
                bridge.results[0].status,
                PromotedCorporateActionAdjustmentStatus.CORPORATE_ACTION_COVERAGE_INCOMPLETE,
            )

    def test_unsupported_cash_dividend_requires_manual_review(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, bridge = _materialize(
                Path(tmp),
                snapshot_factory=lambda panel: _snapshot(
                    _event(panel, action_type=CorporateActionType.CASH_DIVIDEND)
                ),
            )
            self.assertIs(
                bridge.results[0].status,
                PromotedCorporateActionAdjustmentStatus.CORPORATE_ACTION_MANUAL_REVIEW_REQUIRED,
            )
            self.assertIsNone(bridge.results[0].adjusted_history)


class PromotedCorporateActionBridgeRejectionTests(unittest.TestCase):
    def test_future_known_action_snapshot_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, source = _two_session_fixture(Path(tmp))
            corporate_actions = _snapshot(
                _event(source),
                cutoff=BRIDGE_CUTOFF + timedelta(seconds=1),
            )
            with self.assertRaises(PromotedCorporateActionBridgeError):
                PromotedCorporateActionAdjustmentService().materialize(
                    source_panel=source,
                    corporate_actions=corporate_actions,
                    cutoff=BRIDGE_CUTOFF,
                )

    def test_naive_cutoff_and_wrong_types_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, source = _two_session_fixture(Path(tmp))
            corporate_actions = _snapshot(_event(source))
            with self.assertRaises(PromotedCorporateActionBridgeError):
                PromotedCorporateActionAdjustmentService().materialize(
                    source_panel=source,
                    corporate_actions=corporate_actions,
                    cutoff=BRIDGE_CUTOFF.replace(tzinfo=None),
                )
            with self.assertRaises(PromotedCorporateActionBridgeError):
                PromotedCorporateActionAdjustmentService().materialize(
                    source_panel="wrong",  # type: ignore[arg-type]
                    corporate_actions=corporate_actions,
                    cutoff=BRIDGE_CUTOFF,
                )

    def test_direct_construction_and_nested_mutation_fail_replay(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, bridge = _materialize(Path(tmp))
            values = {
                value.name: getattr(bridge, value.name)
                for value in dataclasses.fields(bridge)
            }
            values["feature_eligible"] = 0
            with self.assertRaises(PromotedCorporateActionBridgeError):
                VerifiedPromotedCorporateActionAdjustmentPanel(**values)

            adjusted = bridge.results[0].adjusted_history
            assert adjusted is not None
            original = adjusted.bars[0].adjusted_close
            object.__setattr__(adjusted.bars[0], "adjusted_close", Decimal("999"))
            try:
                with self.assertRaises(PromotedCorporateActionBridgeError):
                    bridge.verify_content_identity()
            finally:
                object.__setattr__(adjusted.bars[0], "adjusted_close", original)


if __name__ == "__main__":
    unittest.main()
