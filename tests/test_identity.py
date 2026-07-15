from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from india_swing.demo import build_demo
from india_swing.domain.models import DecisionAction, RunStatus
from india_swing.identity import content_id


class ContentIdentityTests(unittest.TestCase):
    def test_equivalent_instants_have_the_same_content_identity(self) -> None:
        utc = datetime(2026, 7, 15, 11, 30, tzinfo=timezone.utc)
        ist = utc.astimezone(timezone(timedelta(hours=5, minutes=30)))

        self.assertEqual(content_id({"observed_at": utc}), content_id({"observed_at": ist}))

    def test_evidence_content_change_cannot_reuse_run_or_signal_id(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
        original = pipeline.run(snapshot, instruments, portfolio, reference_context)
        changed_evidence = replace(
            snapshot.evidence[0],
            content_hash="materially-different-announcement-content",
        )
        changed_snapshot = replace(
            snapshot,
            evidence=(changed_evidence, *snapshot.evidence[1:]),
        )

        changed = pipeline.run(changed_snapshot, instruments, portfolio, reference_context)

        self.assertNotEqual(original.run_id, changed.run_id)
        self.assertNotEqual(original.decision.signal_id, changed.decision.signal_id)
        self.assertIs(changed.status, RunStatus.FAILED)
        self.assertIs(changed.decision.action, DecisionAction.NO_TRADE)

    def test_portfolio_state_change_cannot_reuse_run_or_signal_id(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
        original = pipeline.run(snapshot, instruments, portfolio, reference_context)
        changed_portfolio = replace(portfolio, daily_realized_pnl=Decimal("-1"))

        changed = pipeline.run(snapshot, instruments, changed_portfolio, reference_context)

        self.assertEqual(original.decision.quantity, changed.decision.quantity)
        self.assertNotEqual(original.run_id, changed.run_id)
        self.assertNotEqual(original.decision.signal_id, changed.decision.signal_id)

    def test_provider_and_ranker_configuration_are_identity_material(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
        original = pipeline.run(snapshot, instruments, portfolio, reference_context)

        pipeline.forecast_provider.model_version = "same-output-new-provider-config"
        provider_changed = pipeline.run(snapshot, instruments, portfolio, reference_context)
        self.assertNotEqual(original.run_id, provider_changed.run_id)
        self.assertNotEqual(original.decision.signal_id, provider_changed.decision.signal_id)

        pipeline.ranker.weights = replace(
            pipeline.ranker.weights,
            expected_return=Decimal("0.31"),
        )
        ranker_changed = pipeline.run(snapshot, instruments, portfolio, reference_context)
        self.assertNotEqual(provider_changed.run_id, ranker_changed.run_id)
        self.assertNotEqual(
            provider_changed.decision.signal_id,
            ranker_changed.decision.signal_id,
        )

    def test_approved_signal_carries_the_setup_execution_window(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()

        result = pipeline.run(snapshot, instruments, portfolio, reference_context)
        setup = pipeline.signal_provider.values[result.decision.symbol][1]

        self.assertEqual(result.decision.earliest_entry_at, setup.earliest_entry_at)
        self.assertEqual(result.decision.entry_expires_at, setup.entry_expires_at)
        self.assertEqual(result.decision.max_holding_sessions, setup.max_holding_sessions)
        self.assertEqual(result.decision.order_type, "LIMIT")


if __name__ == "__main__":
    unittest.main()
