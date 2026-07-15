from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from india_swing.audit import AuditExistsError, AuditWriter
from india_swing.demo import build_demo
from india_swing.domain.models import DecisionAction, ResearchVerdict, RunStatus


class PipelineIntegrationTests(unittest.TestCase):
    def test_full_demo_produces_a_sized_buy(self) -> None:
        pipeline, snapshot, instruments, portfolio = build_demo()

        result = pipeline.run(snapshot, instruments, portfolio)

        self.assertEqual(snapshot.market_session, snapshot.decision_time.date())
        self.assertTrue(
            all(instrument.price_session == snapshot.market_session for instrument in instruments)
        )
        self.assertIs(result.decision.action, DecisionAction.BUY)
        self.assertEqual(result.decision.symbol, "DEMO-SMALL")
        self.assertGreater(result.decision.quantity, 0)
        self.assertGreater(result.decision.planned_max_loss, 0)
        self.assertGreater(result.decision.expected_r, 0)
        self.assertEqual(result.snapshot_id, snapshot.snapshot_id)
        self.assertEqual(result.pipeline_version, pipeline.version)
        self.assertIs(result.status, RunStatus.COMPLETE)

    def test_gsm_instrument_is_excluded_before_candidate_build(self) -> None:
        pipeline, snapshot, instruments, portfolio = build_demo()

        result = pipeline.run(snapshot, instruments, portfolio)

        gsm_rejections = [
            rejection for rejection in result.rejections if rejection.symbol == "DEMO-GSM"
        ]
        self.assertEqual(len(gsm_rejections), 1)
        self.assertEqual(gsm_rejections[0].stage, "eligibility")
        self.assertIn("surveillance category GSM is blocked", gsm_rejections[0].reasons)
        self.assertNotIn(
            "DEMO-GSM",
            {ranked.candidate.instrument.symbol for ranked in result.ranked},
        )
        self.assertNotIn(
            "DEMO-GSM",
            {assessment.symbol for assessment in result.research},
        )

    def test_uncertain_research_cannot_become_a_trade(self) -> None:
        pipeline, snapshot, instruments, portfolio = build_demo()
        large_cap = next(
            instrument for instrument in instruments if instrument.symbol == "DEMO-LARGE"
        )

        result = pipeline.run(snapshot, [large_cap], portfolio)

        self.assertIs(result.decision.action, DecisionAction.NO_TRADE)
        self.assertIsNone(result.decision.symbol)
        self.assertEqual(len(result.research), 1)
        self.assertIs(result.research[0].verdict, ResearchVerdict.UNCERTAIN)
        risk_rejections = [
            rejection
            for rejection in result.rejections
            if rejection.symbol == "DEMO-LARGE" and rejection.stage == "risk"
        ]
        self.assertEqual(len(risk_rejections), 1)
        self.assertIn("research verdict is UNCERTAIN", risk_rejections[0].reasons)

    def test_audit_record_is_immutable_and_cannot_be_overwritten(self) -> None:
        pipeline, snapshot, instruments, portfolio = build_demo()
        result = pipeline.run(snapshot, instruments, portfolio)
        writer = AuditWriter()

        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            path = writer.write(output_dir, result.run_id, {"result": result})
            original_bytes = path.read_bytes()
            envelope = json.loads(original_bytes)

            self.assertEqual(envelope["schema_version"], writer.schema_version)
            self.assertEqual(
                envelope["payload"]["result"]["decision"]["action"],
                DecisionAction.BUY.value,
            )
            with self.assertRaisesRegex(AuditExistsError, "already exists"):
                writer.write(output_dir, result.run_id, {"result": "tampered"})

            self.assertEqual(path.read_bytes(), original_bytes)
            self.assertEqual(list(output_dir.iterdir()), [path])

    def test_all_candidates_rejected_returns_no_trade(self) -> None:
        pipeline, snapshot, instruments, portfolio = build_demo()
        inactive_instruments = [replace(instrument, active=False) for instrument in instruments]

        result = pipeline.run(snapshot, inactive_instruments, portfolio)

        self.assertIs(result.decision.action, DecisionAction.NO_TRADE)
        self.assertIsNone(result.decision.symbol)
        self.assertEqual(result.decision.quantity, 0)
        self.assertEqual(result.decision.signal_id, f"no-trade-{result.run_id}")
        self.assertEqual(
            result.decision.reasons,
            ("no candidate passed every deterministic gate",),
        )
        self.assertEqual(result.ranked, ())
        self.assertEqual(result.research, ())
        self.assertEqual(len(result.rejections), len(inactive_instruments))
        self.assertTrue(all(rejection.stage == "eligibility" for rejection in result.rejections))
        self.assertTrue(
            all("instrument is not active" in rejection.reasons for rejection in result.rejections)
        )
        self.assertIs(result.status, RunStatus.COMPLETE)

    def test_adapter_exception_is_failed_run_not_ordinary_no_trade(self) -> None:
        pipeline, snapshot, instruments, portfolio = build_demo()

        with patch.object(
            pipeline.forecast_provider,
            "forecast",
            side_effect=RuntimeError("api_key=must-not-appear"),
        ):
            result = pipeline.run(snapshot, instruments, portfolio)

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertIs(result.decision.action, DecisionAction.NO_TRADE)
        self.assertEqual(result.failure_stage, "candidate_build")
        self.assertEqual(result.failure_type, "RuntimeError")
        self.assertNotIn("must-not-appear", repr(result))

    def test_lookahead_failure_returns_auditable_failed_result(self) -> None:
        pipeline, snapshot, instruments, portfolio = build_demo()
        future_evidence = replace(
            snapshot.evidence[0],
            published_at=snapshot.decision_time + timedelta(minutes=1),
            available_at=snapshot.decision_time + timedelta(minutes=2),
        )
        invalid_snapshot = replace(
            snapshot,
            evidence=(future_evidence, *snapshot.evidence[1:]),
        )

        result = pipeline.run(invalid_snapshot, instruments, portfolio)

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertIs(result.decision.action, DecisionAction.NO_TRADE)
        self.assertEqual(result.failure_stage, "snapshot_integrity")
        with tempfile.TemporaryDirectory() as temp_dir:
            path = AuditWriter().write(Path(temp_dir), result.run_id, {"result": result})
            self.assertTrue(path.exists())


if __name__ == "__main__":
    unittest.main()
