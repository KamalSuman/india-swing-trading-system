from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, getcontext

from india_swing.operations import (
    SwingPortfolioEvidenceBinding,
    SwingPortfolioEvidenceKind,
    SwingPortfolioSnapshotArtifact,
    SwingPortfolioVerificationStatus,
    decode_swing_portfolio_artifact,
    encode_swing_portfolio_artifact,
)
from india_swing.paper_outcomes import (
    PaperOutcomeStatus,
    PaperPortfolioPosition,
    PaperPortfolioRolloverError,
    PaperPortfolioState,
    build_paper_portfolio_mark,
    decode_paper_portfolio_rollover,
    encode_paper_portfolio_rollover,
    roll_paper_portfolio,
)
from india_swing.risk import SwingPortfolioSnapshot
from tests.test_paper_outcomes import _calendar, _observation
from india_swing.paper_outcomes.portfolio import _report


UTC = timezone.utc


def _genesis(*, capital: str = "200000") -> SwingPortfolioSnapshotArtifact:
    as_of = datetime(2026, 1, 1, tzinfo=UTC)
    portfolio = SwingPortfolioSnapshot(
        capital=Decimal(capital),
        cash_available=Decimal(capital),
        gross_exposure=Decimal("0"),
        open_risk=Decimal("0"),
        open_positions=0,
        daily_realized_pnl=Decimal("0"),
        pilot_realized_pnl=Decimal("0"),
        as_of=as_of,
    )
    evidence = tuple(
        SwingPortfolioEvidenceBinding(
            kind=kind,
            evidence_id=f"{index:064x}",
            observed_at=as_of,
            source_version="test/manual/v1",
        )
        for index, kind in enumerate(SwingPortfolioEvidenceKind, start=1)
    )
    return SwingPortfolioSnapshotArtifact(
        portfolio=portfolio,
        portfolio_snapshot_id=portfolio.portfolio_snapshot_id,
        evidence=evidence,
        reconciled_at=as_of,
    )


class PaperPortfolioRolloverTests(unittest.TestCase):
    def _position(self, status=PaperOutcomeStatus.OPEN):
        closed = status is PaperOutcomeStatus.CLOSED
        return PaperPortfolioPosition(
            registration_id="1" * 64,
            symbol="INFY",
            outcome_status=status,
            job_spec_id="2" * 64,
            record_id="3" * 64,
            replay_id="4" * 64,
            event_ids=("5" * 64,),
            quantity=100,
            planned_risk=Decimal("500"),
            entry_notional=Decimal("10000") if status is PaperOutcomeStatus.OPEN or closed else Decimal("0"),
            estimated_cost=Decimal("50"),
            gross_pnl=Decimal("1000") if closed else None,
            estimated_net_pnl=Decimal("950") if closed else None,
            realized_r=Decimal("1.9") if closed else None,
        )

    def _state(self, *, status=PaperOutcomeStatus.OPEN, previous=None):
        position = self._position(status)
        closed = status is PaperOutcomeStatus.CLOSED
        as_of = datetime(2026, 1, 10 if closed else 3, tzinfo=UTC)
        positions = (position,)
        daily = Decimal("950") if closed else Decimal("0")
        prior_cumulative = Decimal("0") if previous is None else previous.cumulative_realized_pnl
        cumulative = prior_cumulative + daily
        prior_peak = Decimal("0") if previous is None else previous.peak_realized_pnl
        peak = max(prior_peak, cumulative)
        drawdown = peak - cumulative
        halt_reasons = ()
        report = _report(
            as_of=as_of,
            positions=positions,
            daily_pnl=daily,
            cumulative_pnl=cumulative,
            drawdown=drawdown,
            halt_reasons=halt_reasons,
        )
        return PaperPortfolioState(
            batch_id=("6" if previous is None else "7") * 64,
            outcome_job_spec_ids=(position.job_spec_id,),
            previous_batch_id=None if previous is None else previous.batch_id,
            previous_state_id=None if previous is None else previous.state_id,
            as_of=as_of,
            positions=positions,
            newly_closed_registration_ids=(position.registration_id,) if closed else (),
            daily_realized_pnl=daily,
            prior_cumulative_realized_pnl=prior_cumulative,
            cumulative_realized_pnl=cumulative,
            prior_peak_realized_pnl=prior_peak,
            peak_realized_pnl=peak,
            drawdown=drawdown,
            total_estimated_costs=position.estimated_cost if closed else Decimal("0"),
            open_risk=position.planned_risk if status is PaperOutcomeStatus.OPEN else Decimal("0"),
            open_notional=position.entry_notional if status is PaperOutcomeStatus.OPEN else Decimal("0"),
            closed_count=1 if closed else 0,
            winning_count=1 if closed else 0,
            losing_count=0,
            win_rate=Decimal("1") if closed else Decimal("0"),
            expectancy_pnl=position.estimated_net_pnl if closed else Decimal("0"),
            expectancy_r=position.realized_r if closed else Decimal("0"),
            daily_loss_limit=Decimal("2000"),
            cumulative_loss_limit=Decimal("4000"),
            risk_halt_reasons=halt_reasons,
            report_message=report,
        )

    def _open_state_and_mark(self):
        calendar = _calendar()
        observation = _observation(calendar, date(2026, 1, 2), close="103")
        state = self._state()
        mark = build_paper_portfolio_mark(
            position=state.positions[0],
            listing_key="NSE:INFY",
            observation=observation,
        )
        return calendar, observation, state, mark

    def test_open_position_rolls_into_reconciled_nav_snapshot(self) -> None:
        _, observation, state, mark = self._open_state_and_mark()

        result = roll_paper_portfolio(
            state=state,
            genesis_artifact=_genesis(),
            marks=(mark,),
            as_of=state.as_of,
        )

        position = state.positions[0]
        expected_gross = observation.close * position.quantity
        expected_cash = (
            Decimal("200000")
            + state.cumulative_realized_pnl
            - position.entry_notional
            - position.estimated_cost
        )
        self.assertEqual(result.cash_available, expected_cash)
        self.assertEqual(result.gross_exposure, expected_gross)
        self.assertEqual(result.nav, expected_cash + expected_gross)
        self.assertEqual(
            result.unrealized_net_pnl,
            expected_gross - position.entry_notional - position.estimated_cost,
        )
        self.assertEqual(result.open_listing_keys, ("NSE:INFY",))
        self.assertEqual(result.portfolio_artifact.portfolio.open_positions, 1)
        self.assertEqual(
            result.portfolio_artifact.verification_status,
            SwingPortfolioVerificationStatus.DERIVED_RECONCILED_PAPER_ONLY,
        )
        self.assertEqual(
            tuple(value.kind for value in result.portfolio_artifact.evidence),
            tuple(SwingPortfolioEvidenceKind),
        )

    def test_derived_snapshot_codec_round_trip(self) -> None:
        _, _, state, mark = self._open_state_and_mark()
        result = roll_paper_portfolio(
            state=state,
            genesis_artifact=_genesis(),
            marks=(mark,),
            as_of=state.as_of,
        )

        payload = encode_swing_portfolio_artifact(result.portfolio_artifact)
        self.assertEqual(
            decode_swing_portfolio_artifact(payload),
            result.portfolio_artifact,
        )
        rollover_payload = encode_paper_portfolio_rollover(result)
        self.assertEqual(
            decode_paper_portfolio_rollover(rollover_payload),
            result,
        )
        with self.assertRaises(PaperPortfolioRolloverError):
            decode_paper_portfolio_rollover(
                rollover_payload.replace(b'"nav":"', b'"nav":"9', 1)
            )

    def test_closed_position_releases_cash_and_chains_exact_predecessor(self) -> None:
        _, _, first_state, mark = self._open_state_and_mark()
        genesis = _genesis()
        first = roll_paper_portfolio(
            state=first_state,
            genesis_artifact=genesis,
            marks=(mark,),
            as_of=first_state.as_of,
        )
        second_state = self._state(status=PaperOutcomeStatus.CLOSED, previous=first_state)

        second = roll_paper_portfolio(
            state=second_state,
            genesis_artifact=genesis,
            marks=(),
            as_of=second_state.as_of,
            previous=first,
        )

        expected = Decimal("200000") + second_state.cumulative_realized_pnl
        self.assertEqual(second.cash_available, expected)
        self.assertEqual(second.nav, expected)
        self.assertEqual(second.gross_exposure, 0)
        self.assertEqual(second.portfolio_artifact.portfolio.open_positions, 0)
        self.assertEqual(second.previous_rollover_id, first.rollover_id)
        self.assertEqual(
            second.previous_portfolio_artifact_id,
            first.portfolio_artifact.artifact_id,
        )

    def test_missing_or_wrong_mark_lineage_fails_closed(self) -> None:
        _, _, state, mark = self._open_state_and_mark()
        with self.assertRaisesRegex(PaperPortfolioRolloverError, "coverage"):
            roll_paper_portfolio(
                state=state,
                genesis_artifact=_genesis(),
                marks=(),
                as_of=state.as_of,
            )
        wrong = replace(mark, position_id="f" * 64)
        with self.assertRaisesRegex(PaperPortfolioRolloverError, "lineage"):
            roll_paper_portfolio(
                state=state,
                genesis_artifact=_genesis(),
                marks=(wrong,),
                as_of=state.as_of,
            )

    def test_mark_unknown_at_state_cutoff_fails_closed(self) -> None:
        _, _, state, mark = self._open_state_and_mark()
        late = replace(mark, knowledge_time=state.as_of + timedelta(seconds=1))
        with self.assertRaisesRegex(PaperPortfolioRolloverError, "lineage"):
            roll_paper_portfolio(
                state=state,
                genesis_artifact=_genesis(),
                marks=(late,),
                as_of=late.knowledge_time,
            )

    def test_unresolved_waiting_position_cannot_roll(self) -> None:
        state = self._state(status=PaperOutcomeStatus.WAITING)

        with self.assertRaisesRegex(PaperPortfolioRolloverError, "unresolved"):
            roll_paper_portfolio(
                state=state,
                genesis_artifact=_genesis(),
                marks=(),
                as_of=state.as_of,
            )

    def test_insufficient_virtual_cash_fails_closed(self) -> None:
        _, _, state, mark = self._open_state_and_mark()
        with self.assertRaisesRegex(PaperPortfolioRolloverError, "exhausted"):
            roll_paper_portfolio(
                state=state,
                genesis_artifact=_genesis(capital="10"),
                marks=(mark,),
                as_of=state.as_of,
            )

    def test_wrong_predecessor_or_genesis_reset_fails_closed(self) -> None:
        _, _, first_state, mark = self._open_state_and_mark()
        genesis = _genesis()
        first = roll_paper_portfolio(
            state=first_state,
            genesis_artifact=genesis,
            marks=(mark,),
            as_of=first_state.as_of,
        )
        second_state = self._state(status=PaperOutcomeStatus.CLOSED, previous=first_state)
        with self.assertRaisesRegex(PaperPortfolioRolloverError, "predecessor"):
            roll_paper_portfolio(
                state=second_state,
                genesis_artifact=genesis,
                marks=(),
                as_of=second_state.as_of,
            )
        with self.assertRaisesRegex(PaperPortfolioRolloverError, "predecessor"):
            roll_paper_portfolio(
                state=second_state,
                genesis_artifact=_genesis(capital="300000"),
                marks=(),
                as_of=second_state.as_of,
                previous=first,
            )

    def test_accounting_is_independent_of_global_decimal_precision(self) -> None:
        _, _, state, mark = self._open_state_and_mark()
        original = getcontext().prec
        try:
            getcontext().prec = 6
            low_precision = roll_paper_portfolio(
                state=state,
                genesis_artifact=_genesis(),
                marks=(mark,),
                as_of=state.as_of,
            )
            getcontext().prec = 40
            high_precision = roll_paper_portfolio(
                state=state,
                genesis_artifact=_genesis(),
                marks=(mark,),
                as_of=state.as_of,
            )
        finally:
            getcontext().prec = original
        self.assertEqual(low_precision.rollover_id, high_precision.rollover_id)

    def test_tampering_is_detected_by_content_identity(self) -> None:
        _, _, state, mark = self._open_state_and_mark()
        result = roll_paper_portfolio(
            state=state,
            genesis_artifact=_genesis(),
            marks=(mark,),
            as_of=state.as_of,
        )
        object.__setattr__(result, "nav", result.nav + Decimal("1"))
        with self.assertRaises(PaperPortfolioRolloverError):
            result.verify_content_identity()


if __name__ == "__main__":
    unittest.main()
