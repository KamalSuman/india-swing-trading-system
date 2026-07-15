from __future__ import annotations

import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from india_swing.data.asof import (
    DataIntegrityError,
    LookaheadViolation,
    validate_candidate,
    validate_snapshot,
)
from india_swing.domain.models import (
    Board,
    Candidate,
    DataSnapshot,
    EvidenceItem,
    ForecastSummary,
    InstrumentSnapshot,
    MarketCapBucket,
    SignalFeatures,
    Surveillance,
    TradeSetup,
)


IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc
MARKET_SESSION = date(2026, 7, 15)
DECISION_TIME = datetime(2026, 7, 15, 17, 0, tzinfo=IST)
NEXT_SESSION_ENTRY = datetime(2026, 7, 16, 9, 20, tzinfo=IST)


class AsOfValidationTests(unittest.TestCase):
    def evidence(
        self,
        evidence_id: str = "evidence-1",
        *,
        published_at: datetime | None = None,
        available_at: datetime | None = None,
    ) -> EvidenceItem:
        published_at = published_at or DECISION_TIME - timedelta(minutes=30)
        available_at = available_at or published_at + timedelta(minutes=1)
        return EvidenceItem(
            evidence_id=evidence_id,
            source="TEST",
            published_at=published_at,
            available_at=available_at,
            content_hash=f"hash-{evidence_id}",
        )

    def snapshot(self, evidence: tuple[EvidenceItem, ...] | None = None) -> DataSnapshot:
        return DataSnapshot(
            snapshot_id="snapshot-1",
            decision_time=DECISION_TIME,
            market_session=MARKET_SESSION,
            evidence=evidence if evidence is not None else (self.evidence(),),
            session_finalized_at=DECISION_TIME - timedelta(minutes=30),
            universe_snapshot_id="universe-1",
            calendar_version="calendar-1",
            trial_id="trial-1",
            model_bundle_id="bundle-1",
            data_content_hash="data-hash-1",
            source_revision="source-1",
            execution_policy_version="execution-1",
            cost_schedule_version="cost-1",
        )

    def instrument(self, *, data_available_at: datetime | None = None) -> InstrumentSnapshot:
        return InstrumentSnapshot(
            symbol="TEST",
            board=Board.MAIN,
            market_cap_bucket=MarketCapBucket.SMALL,
            active=True,
            suspended=False,
            surveillance=Surveillance.NONE,
            last_price=Decimal("100"),
            median_daily_traded_value=Decimal("10000000"),
            quoted_spread_bps=Decimal("20"),
            lower_circuit_locked=False,
            history_sessions=250,
            price_session=MARKET_SESSION,
            data_available_at=data_available_at or DECISION_TIME - timedelta(minutes=10),
        )

    def candidate(
        self,
        *,
        evidence_ids: tuple[str, ...] = ("evidence-1",),
        forecast_as_of: datetime = DECISION_TIME,
        decision_time: datetime = DECISION_TIME,
        entry_at: datetime = NEXT_SESSION_ENTRY,
    ) -> Candidate:
        forecast = ForecastSummary(
            symbol="TEST",
            as_of=forecast_as_of,
            horizon_sessions=8,
            median_return_pct=Decimal("4"),
            downside_return_pct=Decimal("-2"),
            uncertainty=Decimal("0.25"),
            sample_count=32,
            model_version="test-model-v1",
        )
        signals = SignalFeatures(
            relative_strength=Decimal("0.8"),
            trend_quality=Decimal("0.7"),
            volume_confirmation=Decimal("0.6"),
            liquidity_quality=Decimal("0.9"),
            news_score=Decimal("0.1"),
            estimated_cost_bps=Decimal("30"),
        )
        setup = TradeSetup(
            symbol="TEST",
            decision_time=decision_time,
            earliest_entry_at=entry_at,
            entry_low=Decimal("100"),
            entry_high=Decimal("101"),
            stop=Decimal("96"),
            target=Decimal("114"),
            target_probability=Decimal("0.45"),
            stop_probability=Decimal("0.35"),
            expected_time_exit_r=Decimal("0"),
            max_holding_sessions=8,
            setup_reason="test setup",
            stop_reason="test invalidation",
            target_reason="test target",
            entry_expires_at=entry_at + timedelta(hours=6),
        )
        return Candidate(self.instrument(), forecast, signals, setup, evidence_ids)

    def test_naive_timestamps_are_rejected(self) -> None:
        naive = datetime(2026, 7, 15, 17, 0)
        factories = {
            "snapshot decision": lambda: DataSnapshot(
                "s",
                naive,
                MARKET_SESSION,
                (),
                DECISION_TIME - timedelta(minutes=30),
                "universe-1",
                "calendar-1",
                "trial-1",
                "bundle-1",
                "data-hash-1",
                "source-1",
                "execution-1",
                "cost-1",
            ),
            "evidence published": lambda: self.evidence(published_at=naive),
            "evidence available": lambda: self.evidence(available_at=naive),
            "instrument data": lambda: self.instrument(data_available_at=naive),
            "forecast as_of": lambda: ForecastSummary(
                "TEST", naive, 8, Decimal("1"), Decimal("-1"), Decimal("0.2"), 10, "v1"
            ),
            "setup entry": lambda: TradeSetup(
                "TEST",
                DECISION_TIME,
                naive,
                Decimal("100"),
                Decimal("101"),
                Decimal("95"),
                Decimal("115"),
                Decimal("0.4"),
                Decimal("0.3"),
                Decimal("0"),
                8,
                "setup",
                "stop",
                "target",
                entry_expires_at=NEXT_SESSION_ENTRY + timedelta(hours=6),
            ),
        }
        for label, factory in factories.items():
            with self.subTest(field=label):
                with self.assertRaisesRegex(ValueError, "timezone-aware"):
                    factory()

    def test_equivalent_aware_timestamps_across_timezones_are_valid(self) -> None:
        evidence = self.evidence(
            published_at=datetime(2026, 7, 15, 10, 30, tzinfo=UTC),
            available_at=datetime(2026, 7, 15, 10, 31, tzinfo=UTC),
        )
        validate_snapshot(self.snapshot((evidence,)))

    def test_snapshot_rejects_future_evidence(self) -> None:
        future = self.evidence(
            "future",
            published_at=DECISION_TIME + timedelta(minutes=1),
            available_at=DECISION_TIME + timedelta(minutes=2),
        )
        with self.assertRaisesRegex(LookaheadViolation, "future"):
            validate_snapshot(self.snapshot((future,)))

    def test_snapshot_rejects_decision_before_session_finalization(self) -> None:
        with self.assertRaisesRegex(ValueError, "finalized before"):
            DataSnapshot(
                snapshot_id="snapshot-early",
                decision_time=DECISION_TIME,
                market_session=MARKET_SESSION,
                evidence=(),
                session_finalized_at=DECISION_TIME + timedelta(seconds=1),
                universe_snapshot_id="universe-1",
                calendar_version="calendar-1",
                trial_id="trial-1",
                model_bundle_id="bundle-1",
                data_content_hash="data-hash-1",
                source_revision="source-1",
                execution_policy_version="execution-1",
                cost_schedule_version="cost-1",
            )

    def test_candidate_rejects_price_available_before_session_finality(self) -> None:
        candidate = self.candidate()
        impossible_instrument = self.instrument(
            data_available_at=DECISION_TIME - timedelta(hours=1)
        )
        candidate = Candidate(
            impossible_instrument,
            candidate.forecast,
            candidate.signals,
            candidate.setup,
            candidate.evidence_ids,
        )
        with self.assertRaisesRegex(DataIntegrityError, "before session finalization"):
            validate_candidate(candidate, self.snapshot())

    def test_candidate_rejects_referenced_future_evidence(self) -> None:
        future = self.evidence(
            "future",
            published_at=DECISION_TIME + timedelta(minutes=1),
            available_at=DECISION_TIME + timedelta(minutes=2),
        )
        with self.assertRaisesRegex(LookaheadViolation, "future evidence"):
            validate_candidate(
                self.candidate(evidence_ids=("future",)),
                self.snapshot((future,)),
            )

    def test_candidate_rejects_missing_evidence(self) -> None:
        with self.assertRaisesRegex(DataIntegrityError, "missing evidence: absent"):
            validate_candidate(
                self.candidate(evidence_ids=("evidence-1", "absent")),
                self.snapshot(),
            )

    def test_next_session_entry_is_valid(self) -> None:
        validate_candidate(self.candidate(entry_at=NEXT_SESSION_ENTRY), self.snapshot())

    def test_same_market_session_entry_is_rejected(self) -> None:
        after_cutoff_same_session = DECISION_TIME + timedelta(minutes=1)
        with self.assertRaisesRegex(LookaheadViolation, "after the signal market session"):
            validate_candidate(
                self.candidate(entry_at=after_cutoff_same_session),
                self.snapshot(),
            )

    def test_entry_at_or_before_decision_cutoff_is_rejected(self) -> None:
        for entry_at in (DECISION_TIME, DECISION_TIME - timedelta(minutes=1)):
            with self.subTest(entry_at=entry_at):
                with self.assertRaisesRegex(LookaheadViolation, "strictly after"):
                    validate_candidate(self.candidate(entry_at=entry_at), self.snapshot())


if __name__ == "__main__":
    unittest.main()
