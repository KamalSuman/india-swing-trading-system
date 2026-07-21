from __future__ import annotations

import hashlib
import tempfile
import unittest
from dataclasses import replace
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from india_swing.paper_outcomes import (
    LocalPaperOutcomeRunStore,
    LocalPaperPortfolioStateStore,
    PaperOutcomeJobSpec,
    PaperOutcomeStatus,
    PaperPortfolioBatchSpec,
    PaperPortfolioError,
    decode_paper_portfolio_state,
    decode_paper_portfolio_batch_spec,
    encode_paper_portfolio_batch_spec,
    encode_paper_portfolio_state,
    publish_paper_portfolio_state,
    restore_paper_portfolio_state,
    run_paper_portfolio_batch,
)
from india_swing.paper_trades import LocalPaperTradeLedger
from tests.test_paper_outcome_operational import _EvidenceSource, _MemoryState, _evidence, _spec
from tests.test_paper_outcomes import _calendar, _observation


UTC = timezone.utc


class PaperPortfolioOrchestratorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.ledger = LocalPaperTradeLedger(self.root / "paper")
        self.outcome_store = LocalPaperOutcomeRunStore(self.root / "paper_outcomes")
        self.portfolio_store = LocalPaperPortfolioStateStore(
            self.root / "paper_portfolio"
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _batch(self, job, *, previous=None, daily="1000", cumulative="2000"):
        return PaperPortfolioBatchSpec(
            as_of=job.as_of,
            outcome_jobs=(job,),
            previous_batch_id=None if previous is None else previous.batch_id,
            expected_previous_state_id=None if previous is None else previous.state_id,
            daily_loss_limit=Decimal(daily),
            cumulative_loss_limit=Decimal(cumulative),
        )

    def _run(self, batch, evidence):
        return run_paper_portfolio_batch(
            spec=batch,
            evidence_source=_EvidenceSource(evidence),
            ledger=self.ledger,
            outcome_store=self.outcome_store,
            portfolio_store=self.portfolio_store,
        )

    def test_open_position_evolves_to_closed_and_updates_portfolio_metrics(self) -> None:
        calendar = _calendar()
        opening = _evidence((_observation(calendar, date(2026, 1, 2)),))
        self.ledger.register_value(opening.registration)
        open_job = _spec(opening, as_of=datetime(2026, 1, 3, tzinfo=UTC))
        first_batch = self._batch(open_job)
        self.assertEqual(
            decode_paper_portfolio_batch_spec(
                encode_paper_portfolio_batch_spec(first_batch)
            ),
            first_batch,
        )

        first = self._run(first_batch, opening)

        self.assertIs(first.positions[0].outcome_status, PaperOutcomeStatus.OPEN)
        self.assertEqual(first.closed_count, 0)
        self.assertGreater(first.open_risk, 0)
        self.assertGreater(first.open_notional, 0)
        self.assertEqual(first.daily_realized_pnl, Decimal("0"))
        self.assertEqual(first.risk_halt_reasons, ())

        closing = _evidence(
            (
                _observation(calendar, date(2026, 1, 2)),
                _observation(calendar, date(2026, 1, 3), high="111", low="95"),
            )
        )
        closed_job = _spec(closing, as_of=datetime(2026, 1, 10, tzinfo=UTC))
        second_batch = self._batch(closed_job, previous=first)

        second = self._run(second_batch, closing)

        self.assertEqual(second.previous_state_id, first.state_id)
        self.assertIs(second.positions[0].outcome_status, PaperOutcomeStatus.CLOSED)
        self.assertEqual(
            second.newly_closed_registration_ids,
            (opening.registration.registration_id,),
        )
        self.assertEqual(second.closed_count, 1)
        self.assertEqual(second.winning_count, 1)
        self.assertEqual(second.losing_count, 0)
        self.assertEqual(second.win_rate, Decimal("1"))
        self.assertGreater(second.daily_realized_pnl, 0)
        self.assertEqual(
            second.cumulative_realized_pnl,
            second.daily_realized_pnl,
        )
        self.assertEqual(second.open_risk, Decimal("0"))
        self.assertEqual(second.open_notional, Decimal("0"))
        self.assertIn("NOT BROKER PERFORMANCE", second.report_message)

        encoded = encode_paper_portfolio_state(second)
        self.assertEqual(decode_paper_portfolio_state(encoded), second)
        with self.assertRaises(PaperPortfolioError):
            decode_paper_portfolio_state(encoded.replace(b'"INFY"', b'"WIPRO"'))

        memory = _MemoryState()
        publication = publish_paper_portfolio_state(
            state=second,
            bucket="paper-state-bucket",
            writer=memory,
        )
        self.assertIn("/manifests/", memory.write_calls[-1]["object_name"])
        restored_store = LocalPaperPortfolioStateStore(self.root / "restored-portfolio")
        restored = restore_paper_portfolio_state(
            expected_batch_id=second.batch_id,
            bucket="paper-state-bucket",
            manifest_object_name=publication.manifest_object.object_name,
            manifest_generation=publication.manifest_object.generation,
            manifest_sha256=publication.manifest_object.sha256,
            reader=memory,
            store=restored_store,
        )
        self.assertEqual(restored, second)

    def test_batch_retry_returns_terminal_state_without_reloading_evidence(self) -> None:
        calendar = _calendar()
        evidence = _evidence((_observation(calendar, date(2026, 1, 2)),))
        self.ledger.register_value(evidence.registration)
        job = _spec(evidence, as_of=datetime(2026, 1, 3, tzinfo=UTC))
        batch = self._batch(job)
        source = _EvidenceSource(evidence)

        first = run_paper_portfolio_batch(
            spec=batch,
            evidence_source=source,
            ledger=self.ledger,
            outcome_store=self.outcome_store,
            portfolio_store=self.portfolio_store,
        )
        second = run_paper_portfolio_batch(
            spec=batch,
            evidence_source=source,
            ledger=self.ledger,
            outcome_store=self.outcome_store,
            portfolio_store=self.portfolio_store,
        )

        self.assertEqual(second, first)
        self.assertEqual(source.calls, [job.job_spec_id])

    def test_active_position_cannot_disappear_from_next_batch(self) -> None:
        calendar = _calendar()
        evidence = _evidence((_observation(calendar, date(2026, 1, 2)),))
        self.ledger.register_value(evidence.registration)
        job = _spec(evidence, as_of=datetime(2026, 1, 3, tzinfo=UTC))
        first = self._run(self._batch(job), evidence)
        foreign = PaperOutcomeJobSpec(
            registration_id="f" * 64,
            calendar_materialization_id=job.calendar_materialization_id,
            tick_snapshot_id=job.tick_snapshot_id,
            historical_artifact_ids=job.historical_artifact_ids,
            series=job.series,
            validated_isin=job.validated_isin,
            as_of=datetime(2026, 1, 10, tzinfo=UTC),
            policy=job.policy,
            expected_replay_id=job.expected_replay_id,
        )
        incomplete = self._batch(foreign, previous=first)

        with self.assertRaisesRegex(PaperPortfolioError, "omits an active"):
            self._run(incomplete, evidence)

    def test_large_daily_loss_triggers_both_configured_halts(self) -> None:
        calendar = _calendar()
        evidence = _evidence(
            (_observation(calendar, date(2026, 1, 2), high="105", low="89"),)
        )
        self.ledger.register_value(evidence.registration)
        job = _spec(evidence, as_of=datetime(2026, 1, 10, tzinfo=UTC))
        batch = self._batch(job, daily="100", cumulative="100")

        state = self._run(batch, evidence)

        self.assertLess(state.daily_realized_pnl, Decimal("-100"))
        self.assertEqual(
            state.risk_halt_reasons,
            (
                "CUMULATIVE_REALIZED_LOSS_HALT",
                "DAILY_REALIZED_LOSS_HALT",
            ),
        )
        self.assertEqual(state.drawdown, -state.cumulative_realized_pnl)

    def test_wrong_previous_state_id_fails_before_any_new_outcome(self) -> None:
        calendar = _calendar()
        opening = _evidence((_observation(calendar, date(2026, 1, 2)),))
        self.ledger.register_value(opening.registration)
        open_job = _spec(opening, as_of=datetime(2026, 1, 3, tzinfo=UTC))
        first = self._run(self._batch(open_job), opening)
        closing = _evidence(
            (
                _observation(calendar, date(2026, 1, 2)),
                _observation(calendar, date(2026, 1, 3), high="111"),
            )
        )
        closed_job = _spec(closing, as_of=datetime(2026, 1, 10, tzinfo=UTC))
        bad = PaperPortfolioBatchSpec(
            as_of=closed_job.as_of,
            outcome_jobs=(closed_job,),
            previous_batch_id=first.batch_id,
            expected_previous_state_id="0" * 64,
        )
        source = _EvidenceSource(closing)

        with self.assertRaisesRegex(PaperPortfolioError, "previous"):
            run_paper_portfolio_batch(
                spec=bad,
                evidence_source=source,
                ledger=self.ledger,
                outcome_store=self.outcome_store,
                portfolio_store=self.portfolio_store,
            )
        self.assertEqual(source.calls, [])

    def test_existing_portfolio_cannot_be_reset_to_a_new_genesis(self) -> None:
        calendar = _calendar()
        opening = _evidence((_observation(calendar, date(2026, 1, 2)),))
        self.ledger.register_value(opening.registration)
        first_job = _spec(opening, as_of=datetime(2026, 1, 3, tzinfo=UTC))
        self._run(self._batch(first_job), opening)
        later = _evidence(
            (
                _observation(calendar, date(2026, 1, 2)),
                _observation(calendar, date(2026, 1, 3), high="111"),
            )
        )
        later_job = _spec(later, as_of=datetime(2026, 1, 10, tzinfo=UTC))
        source = _EvidenceSource(later)

        with self.assertRaisesRegex(PaperPortfolioError, "predecessor is required"):
            run_paper_portfolio_batch(
                spec=self._batch(later_job),
                evidence_source=source,
                ledger=self.ledger,
                outcome_store=self.outcome_store,
                portfolio_store=self.portfolio_store,
            )
        self.assertEqual(source.calls, [])

    def test_state_rejects_fabricated_daily_pnl_attribution(self) -> None:
        calendar = _calendar()
        evidence = _evidence(
            (_observation(calendar, date(2026, 1, 2), high="105", low="89"),)
        )
        self.ledger.register_value(evidence.registration)
        job = _spec(evidence, as_of=datetime(2026, 1, 10, tzinfo=UTC))
        state = self._run(self._batch(job), evidence)

        with self.assertRaisesRegex(PaperPortfolioError, "daily paper P&L"):
            replace(
                state,
                daily_realized_pnl=state.daily_realized_pnl + Decimal("1"),
                cumulative_realized_pnl=state.cumulative_realized_pnl + Decimal("1"),
                drawdown=state.drawdown - Decimal("1"),
            )
        with self.assertRaisesRegex(PaperPortfolioError, "report message"):
            replace(state, report_message="PAPER PROFIT GUARANTEED")
        with self.assertRaisesRegex(PaperPortfolioError, "counts"):
            replace(state, closed_count=True)

    def test_restore_rejects_malformed_reference_before_any_read(self) -> None:
        memory = _MemoryState()
        with self.assertRaises(PaperPortfolioError):
            restore_paper_portfolio_state(
                expected_batch_id="a" * 64,
                bucket="paper-state-bucket",
                manifest_object_name="paper-portfolios/../../manifest.json",
                manifest_generation=True,
                manifest_sha256="b" * 64,
                reader=memory,
                store=self.portfolio_store,
            )
        self.assertEqual(memory.read_calls, [])

    def test_restore_tamper_does_not_create_local_terminal_state(self) -> None:
        calendar = _calendar()
        evidence = _evidence((_observation(calendar, date(2026, 1, 2)),))
        self.ledger.register_value(evidence.registration)
        job = _spec(evidence, as_of=datetime(2026, 1, 3, tzinfo=UTC))
        state = self._run(self._batch(job), evidence)
        memory = _MemoryState()
        publication = publish_paper_portfolio_state(
            state=state, bucket="paper-state-bucket", writer=memory
        )
        key = (
            "paper-state-bucket",
            publication.manifest_object.object_name,
            publication.manifest_object.generation,
        )
        tampered = memory.objects[key].replace(b'"bucket":', b'"bucket":null,"bucket":')
        memory.objects[key] = tampered
        restored_store = LocalPaperPortfolioStateStore(self.root / "tampered-restore")

        with self.assertRaises(PaperPortfolioError):
            restore_paper_portfolio_state(
                expected_batch_id=state.batch_id,
                bucket="paper-state-bucket",
                manifest_object_name=publication.manifest_object.object_name,
                manifest_generation=publication.manifest_object.generation,
                manifest_sha256=hashlib.sha256(tampered).hexdigest(),
                reader=memory,
                store=restored_store,
            )
        self.assertFalse(restored_store.path_for(state.batch_id).exists())


if __name__ == "__main__":
    unittest.main()
