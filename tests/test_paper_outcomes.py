from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

from india_swing.paper_outcomes import (
    PaperInstrumentBinding,
    PaperOutcomeError,
    PaperOutcomeExitReason,
    PaperOutcomeIntegrityError,
    PaperOutcomeObservation,
    PaperOutcomePolicy,
    PaperOutcomeStatus,
    bind_paper_instrument,
    observe_paper_session,
    replay_paper_outcome,
)
from india_swing.paper_trades import PaperTradeRegistration
from india_swing.reference.calendar import (
    CalendarDay,
    CalendarDayKind,
    CalendarSnapshot,
    SessionWindow,
    SessionWindowPhase,
)
from india_swing.reference.models import ExternalRecordRef, ReferenceReadiness
from tests import test_daily_pipeline as daily_pipeline_tests
from tests import test_shadow_scanner as shadow_scanner_tests


IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc
ISIN = "INE009A01021"


def _registration(*, holding_sessions: int = 3) -> PaperTradeRegistration:
    return PaperTradeRegistration(
        alert_id="a" * 64,
        source_run_id="run-1",
        source_pipeline_integrity_hash="b" * 64,
        source_decision_integrity_hash="c" * 64,
        signal_id="signal-1",
        symbol="INFY",
        quantity=10,
        decision_time=datetime(2026, 1, 1, 12, tzinfo=UTC),
        earliest_entry_at=datetime(2026, 1, 2, 9, 15, tzinfo=IST),
        entry_expires_at=datetime(2026, 1, 3, 15, 30, tzinfo=IST),
        entry_low=Decimal("99"),
        entry_high=Decimal("100"),
        stop=Decimal("90"),
        target=Decimal("110"),
        max_holding_sessions=holding_sessions,
        estimated_round_trip_cost=Decimal("10"),
    )


def _binding(registration: PaperTradeRegistration) -> PaperInstrumentBinding:
    return PaperInstrumentBinding(
        registration_id=registration.registration_id,
        symbol="INFY",
        series="EQ",
        validated_isin=ISIN,
        financial_instrument_id=1594,
        tick_size=Decimal("0.05"),
        tick_snapshot_id="d" * 64,
        tick_observation_id="e" * 64,
        tick_market_session_claim=date(2025, 12, 31),
        tick_knowledge_time=datetime(2025, 12, 31, 12, tzinfo=UTC),
    )


def _calendar(start: date = date(2026, 1, 2), count: int = 7) -> CalendarSnapshot:
    source_id = "f" * 64
    days = []
    for offset in range(count):
        day = start + timedelta(days=offset)
        reference = ExternalRecordRef(
            event_time=datetime.combine(day, time(0), tzinfo=IST),
            knowledge_time=datetime(2025, 12, 1, 12, tzinfo=IST),
            source="SYNTHETIC_CALENDAR",
            content_hash=f"{day.toordinal():064x}",
            source_snapshot_id=source_id,
        )
        days.append(
            CalendarDay(
                day=day,
                kind=CalendarDayKind.REGULAR,
                reference=reference,
                session_windows=(
                    SessionWindow(
                        datetime.combine(day, time(9, 15), tzinfo=IST),
                        datetime.combine(day, time(15, 30), tzinfo=IST),
                        SessionWindowPhase.LIVE_CONTINUOUS,
                    ),
                ),
            )
        )
    return CalendarSnapshot.create(
        exchange="NSE",
        segment="CM",
        cutoff=datetime(2026, 1, 1, 17, tzinfo=IST),
        coverage_start=start,
        coverage_end=start + timedelta(days=count - 1),
        days=tuple(days),
        source_snapshot_ids=(source_id,),
        readiness=ReferenceReadiness.SYNTHETIC_TEST,
    )


def _observation(
    calendar: CalendarSnapshot,
    session: date,
    *,
    open: str = "100",
    high: str = "105",
    low: str = "95",
    close: str = "102",
    traded: bool = True,
) -> PaperOutcomeObservation:
    close_at = datetime.combine(session, time(15, 30), tzinfo=IST)
    common = dict(
        artifact_id=f"{session.toordinal() + 1000:064x}",
        calendar_snapshot_id=calendar.snapshot_id,
        market_session=session,
        session_close_at=close_at,
        knowledge_time=datetime.combine(session, time(17), tzinfo=IST),
        symbol="INFY",
        series="EQ",
        validated_isin=ISIN,
    )
    if not traded:
        return PaperOutcomeObservation(
            **common,
            bar_id=None,
            open=None,
            high=None,
            low=None,
            close=None,
            volume=None,
        )
    return PaperOutcomeObservation(
        **common,
        bar_id=f"{session.toordinal() + 2000:064x}",
        open=Decimal(open),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=10000,
    )


class PaperOutcomeResolverTests(unittest.TestCase):
    def setUp(self) -> None:
        self.registration = _registration()
        self.binding = _binding(self.registration)
        self.calendar = _calendar()

    def replay(self, observations, *, as_of=None, policy=None):
        return replay_paper_outcome(
            registration=self.registration,
            binding=self.binding,
            calendar=self.calendar,
            observations=tuple(observations),
            as_of=as_of or datetime(2026, 1, 10, tzinfo=UTC),
            policy=policy,
        )

    def test_future_observation_is_not_used(self) -> None:
        observation = _observation(self.calendar, date(2026, 1, 2))

        result = self.replay(
            (observation,),
            as_of=observation.knowledge_time - timedelta(seconds=1),
        )

        self.assertIs(result.status, PaperOutcomeStatus.WAITING)
        self.assertEqual(result.source_observation_ids, ())
        self.assertEqual(result.reason_code, "WAITING_FOR_ENTRY_EVIDENCE")

    def test_gap_below_approved_entry_range_does_not_fill(self) -> None:
        observations = (
            _observation(self.calendar, date(2026, 1, 2), open="95", high="100", low="94", close="99"),
            _observation(self.calendar, date(2026, 1, 3), open="95", high="98", low="94", close="97"),
        )

        result = self.replay(observations)

        self.assertIs(result.status, PaperOutcomeStatus.EXPIRED)
        self.assertIsNone(result.entry)
        self.assertEqual(result.reason_code, "ENTRY_WINDOW_EXPIRED_UNFILLED")

    def test_same_bar_target_is_deferred_until_later_evidence(self) -> None:
        observations = (
            _observation(self.calendar, date(2026, 1, 2), high="111", low="95"),
            _observation(self.calendar, date(2026, 1, 3), high="111", low="95"),
        )

        result = self.replay(observations)

        self.assertIs(result.status, PaperOutcomeStatus.CLOSED)
        self.assertEqual(result.exit.market_session, date(2026, 1, 3))
        self.assertIs(result.exit.reason, PaperOutcomeExitReason.TARGET)
        self.assertEqual(result.reason_code, "TARGET_EXIT")

    def test_same_bar_stop_and_target_resolves_stop_first(self) -> None:
        result = self.replay(
            (_observation(self.calendar, date(2026, 1, 2), high="111", low="89"),)
        )

        self.assertIs(result.status, PaperOutcomeStatus.CLOSED)
        self.assertIs(result.exit.reason, PaperOutcomeExitReason.STOP)
        self.assertLess(result.exit.price, self.registration.stop)

    def test_gap_through_stop_uses_gap_open_with_adverse_slippage(self) -> None:
        observations = (
            _observation(self.calendar, date(2026, 1, 2)),
            _observation(self.calendar, date(2026, 1, 3), open="85", high="86", low="80", close="82"),
        )

        result = self.replay(observations)

        self.assertIs(result.exit.reason, PaperOutcomeExitReason.STOP)
        self.assertLess(result.exit.price, Decimal("85"))

    def test_holding_horizon_closes_at_conservative_time_exit(self) -> None:
        observations = (
            _observation(self.calendar, date(2026, 1, 2)),
            _observation(self.calendar, date(2026, 1, 3)),
            _observation(self.calendar, date(2026, 1, 4), close="104"),
        )

        result = self.replay(observations)

        self.assertIs(result.status, PaperOutcomeStatus.CLOSED)
        self.assertIs(result.exit.reason, PaperOutcomeExitReason.TIME)
        self.assertLess(result.exit.price, Decimal("104"))

    def test_missing_horizon_bar_blocks_instead_of_inventing_an_exit(self) -> None:
        observations = (
            _observation(self.calendar, date(2026, 1, 2)),
            _observation(self.calendar, date(2026, 1, 3)),
            _observation(self.calendar, date(2026, 1, 4), traded=False),
        )

        result = self.replay(observations)

        self.assertIs(result.status, PaperOutcomeStatus.BLOCKED)
        self.assertEqual(result.reason_code, "HORIZON_EXIT_BAR_MISSING")
        self.assertIsNone(result.exit)

    def test_calendar_session_gap_is_rejected(self) -> None:
        observations = (
            _observation(self.calendar, date(2026, 1, 2)),
            _observation(self.calendar, date(2026, 1, 4)),
        )

        with self.assertRaisesRegex(PaperOutcomeError, "gap"):
            self.replay(observations)

    def test_mutated_observation_and_policy_are_rejected(self) -> None:
        observation = _observation(self.calendar, date(2026, 1, 2))
        object.__setattr__(observation, "close", Decimal("999"))
        object.__setattr__(observation, "observation_id", observation._calculated_id())
        with self.assertRaisesRegex(PaperOutcomeIntegrityError, "observation"):
            self.replay((observation,))

        policy = PaperOutcomePolicy()
        object.__setattr__(policy, "maximum_participation", Decimal("2"))
        object.__setattr__(policy, "policy_id", policy._calculated_id())
        with self.assertRaisesRegex(PaperOutcomeIntegrityError, "identity"):
            self.replay((), policy=policy)

    def test_post_decision_tick_or_predecision_replay_is_rejected(self) -> None:
        late_binding = replace(
            self.binding,
            tick_knowledge_time=self.registration.decision_time + timedelta(seconds=1),
        )
        with self.assertRaisesRegex(PaperOutcomeError, "tick evidence"):
            replay_paper_outcome(
                registration=self.registration,
                binding=late_binding,
                calendar=self.calendar,
                observations=(),
                as_of=datetime(2026, 1, 10, tzinfo=UTC),
            )
        with self.assertRaisesRegex(PaperOutcomeError, "predates"):
            self.replay((), as_of=self.registration.decision_time - timedelta(seconds=1))

    def test_replay_is_always_provisional_and_non_actionable(self) -> None:
        result = self.replay(
            (_observation(self.calendar, date(2026, 1, 2), high="111", low="89"),)
        )

        self.assertFalse(result.actionable)
        self.assertTrue(result.provisional)
        self.assertEqual(result.mode, "PAPER_ONLY")
        self.assertIn("RAW_UNADJUSTED_PRICES", result.blockers)
        self.assertIn("CORPORATE_ACTIONS_UNAPPLIED", result.blockers)
        result.verify_content_identity()

        object.__setattr__(result, "actionable", True)
        object.__setattr__(result, "replay_id", result._calculated_id())
        with self.assertRaisesRegex(PaperOutcomeIntegrityError, "identity"):
            result.verify_content_identity()


class PaperOutcomeSealedBoundaryTests(unittest.TestCase):
    def test_observation_is_derived_from_exact_artifact_bar_and_calendar(self) -> None:
        fixture = daily_pipeline_tests.DailyPipelineTests(
            "test_bootstrap_run_persists_complete_collection_only_lineage"
        )
        fixture.setUp()
        try:
            run = fixture._run()
            artifact = fixture.historical_store.get(
                run.historical_price_artifact_id
            ).artifact
            calendar = daily_pipeline_tests._calendar()
            registration = _registration()
            binding = _binding(registration)

            observation = observe_paper_session(artifact, calendar, binding)

            bar = next(
                value
                for value in artifact.bars
                if (value.symbol, value.series, value.validated_isin)
                == (binding.symbol, binding.series, binding.validated_isin)
            )
            self.assertEqual(observation.artifact_id, artifact.artifact_id)
            self.assertEqual(observation.bar_id, bar.bar_id)
            self.assertEqual(observation.calendar_snapshot_id, calendar.snapshot_id)
            self.assertEqual(observation.open, bar.open)
            observation.verify_content_identity()
        finally:
            fixture.tearDown()

    def test_tick_binding_is_derived_from_exact_snapshot_observation(self) -> None:
        fixture = shadow_scanner_tests.CollectionShadowScannerTests(
            "test_default_policy_returns_typed_no_candidate_for_short_history"
        )
        fixture.setUp()
        try:
            _, _, _, ticks = fixture.inputs(1)
            registration = replace(
                _registration(),
                decision_time=ticks.knowledge_time + timedelta(seconds=1),
                earliest_entry_at=ticks.knowledge_time + timedelta(days=1),
                entry_expires_at=ticks.knowledge_time + timedelta(days=2),
            )
            bar = next(value for value in fixture.history[0].bars if value.symbol == "INFY")

            binding = bind_paper_instrument(
                registration,
                ticks,
                series=bar.series,
                validated_isin=bar.validated_isin,
            )

            tick = next(
                value
                for value in ticks.observations
                if (value.symbol, value.series, value.validated_isin)
                == (binding.symbol, binding.series, binding.validated_isin)
            )
            self.assertEqual(binding.tick_snapshot_id, ticks.snapshot_id)
            self.assertEqual(binding.tick_observation_id, tick.observation_id)
            self.assertEqual(binding.tick_size, tick.tick_size_rupees)
            binding.verify_content_identity()
        finally:
            fixture.tearDown()


if __name__ == "__main__":
    unittest.main()
