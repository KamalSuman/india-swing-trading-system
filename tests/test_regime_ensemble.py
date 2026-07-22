from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

from india_swing.domain.models import (
    Board,
    DataSnapshot,
    EvidenceItem,
    InstrumentSnapshot,
    MarketCapBucket,
    Surveillance,
)
from india_swing.forecasting.regime_ensemble import (
    AlphaSpecialist,
    MarketRegime,
    RegimeAwareForecastProvider,
    RegimeCrossSection,
    RegimeEnsembleConfig,
    RegimeEnsembleError,
    calculate_alpha_instrument_metrics,
    calculate_regime_cross_section,
)
from india_swing.reference.calendar import (
    CalendarDay,
    CalendarDayKind,
    CalendarSnapshot,
    SessionWindow,
    SessionWindowPhase,
)
from india_swing.reference.models import ExternalRecordRef, ReferenceReadiness
from india_swing.signals.deterministic_swing import AsOfSwingBar, InstrumentSwingHistory


IST = timezone(timedelta(hours=5, minutes=30))
START = date(2026, 1, 1)
SIGNAL_SESSION = START + timedelta(days=59)
DECISION_TIME = datetime.combine(SIGNAL_SESSION, time(17), tzinfo=IST)
SOURCE_ID = "a" * 64


def D(value: object) -> Decimal:
    return Decimal(str(value))


def calendar() -> CalendarSnapshot:
    days = tuple(
        CalendarDay(
            day=START + timedelta(days=offset),
            kind=CalendarDayKind.REGULAR,
            reference=ExternalRecordRef(
                event_time=datetime.combine(
                    START + timedelta(days=offset), time(0), tzinfo=IST
                ),
                knowledge_time=datetime(2025, 12, 1, 12, tzinfo=IST),
                source="TEST",
                content_hash=f"{offset + 1:064x}",
                source_snapshot_id=SOURCE_ID,
            ),
            session_windows=(
                SessionWindow(
                    opens_at=datetime.combine(
                        START + timedelta(days=offset), time(9, 15), tzinfo=IST
                    ),
                    closes_at=datetime.combine(
                        START + timedelta(days=offset), time(15, 30), tzinfo=IST
                    ),
                    phase=SessionWindowPhase.LIVE_CONTINUOUS,
                ),
            ),
        )
        for offset in range(70)
    )
    return CalendarSnapshot.create(
        exchange="NSE",
        segment="CM",
        cutoff=datetime.combine(SIGNAL_SESSION, time(16, 30), tzinfo=IST),
        coverage_start=days[0].day,
        coverage_end=days[-1].day,
        days=days,
        source_snapshot_ids=(SOURCE_ID,),
        readiness=ReferenceReadiness.SYNTHETIC_TEST,
    )


def history(
    instrument_id: str,
    *,
    direction: Decimal = D("0.6"),
    alternating_volatility: bool = False,
    contraction: bool = False,
    flat_range: bool = False,
) -> InstrumentSwingHistory:
    bars = []
    price = D("100")
    for offset in range(60):
        session = START + timedelta(days=offset)
        if alternating_volatility:
            price *= D("1.08") if offset % 2 == 0 else D("0.92")
        else:
            price = D("100") + direction * D(offset)
        width = D("0") if flat_range else D("1")
        if contraction and offset >= 55:
            width = D("0.2")
        volume = D("100000") + D(offset * 1000)
        bars.append(
            AsOfSwingBar(
                market_session=session,
                open=price if flat_range else price - min(width, D("0.1")),
                high=price + width,
                low=price - width,
                close=price,
                volume=volume,
                traded_value=price * volume,
                available_at=datetime.combine(session, time(16), tzinfo=IST),
                evidence_id=f"{instrument_id}-bar-{offset:03d}",
                content_hash=f"{instrument_id}-hash-{offset:03d}",
            )
        )
    return InstrumentSwingHistory(
        instrument_id=instrument_id,
        listing_id=f"listing-{instrument_id}",
        tick_size=D("0.05"),
        tick_available_at=datetime(2026, 1, 1, 10, tzinfo=IST),
        tick_evidence_id=f"{instrument_id}-tick",
        tick_content_hash=f"{instrument_id}-tick-hash",
        adjustment_available_at=datetime(2026, 1, 1, 10, 1, tzinfo=IST),
        adjustment_evidence_id=f"{instrument_id}-adjustment",
        adjustment_content_hash=f"{instrument_id}-adjustment-hash",
        price_basis="CORPORATE_ACTION_ADJUSTED_AS_OF_CUTOFF",
        bars=tuple(bars),
    )


def evidence(histories: tuple[InstrumentSwingHistory, ...]) -> tuple[EvidenceItem, ...]:
    result = []
    for value in histories:
        result.extend(
            EvidenceItem(
                evidence_id=bar.evidence_id,
                source="TEST_BAR",
                published_at=bar.available_at - timedelta(minutes=1),
                available_at=bar.available_at,
                content_hash=bar.content_hash,
            )
            for bar in value.bars
        )
        result.extend(
            (
                EvidenceItem(
                    evidence_id=value.tick_evidence_id,
                    source="TEST_TICK",
                    published_at=value.tick_available_at - timedelta(minutes=1),
                    available_at=value.tick_available_at,
                    content_hash=value.tick_content_hash,
                ),
                EvidenceItem(
                    evidence_id=value.adjustment_evidence_id,
                    source="TEST_ADJUSTMENT",
                    published_at=value.adjustment_available_at - timedelta(minutes=1),
                    available_at=value.adjustment_available_at,
                    content_hash=value.adjustment_content_hash,
                ),
            )
        )
    return tuple(result)


def snapshot(
    histories: tuple[InstrumentSwingHistory, ...],
    *,
    current_calendar: CalendarSnapshot,
) -> DataSnapshot:
    return DataSnapshot(
        snapshot_id="snapshot-regime-ensemble",
        decision_time=DECISION_TIME,
        market_session=SIGNAL_SESSION,
        evidence=evidence(histories),
        session_finalized_at=datetime.combine(
            SIGNAL_SESSION, time(16, 30), tzinfo=IST
        ),
        universe_snapshot_id="universe-regime-ensemble",
        calendar_version=current_calendar.version,
        trial_id="trial-regime-ensemble",
        model_bundle_id="model-regime-ensemble",
        data_content_hash="data-regime-ensemble",
        source_revision="source-regime-ensemble",
        execution_policy_version="execution-regime-ensemble",
        cost_schedule_version="cost-regime-ensemble",
    )


def instrument(
    value: InstrumentSwingHistory,
    current_snapshot: DataSnapshot,
) -> InstrumentSnapshot:
    return InstrumentSnapshot(
        instrument_id=value.instrument_id,
        listing_id=value.listing_id,
        universe_snapshot_id=current_snapshot.universe_snapshot_id,
        exchange="NSE",
        segment="CM",
        symbol=value.instrument_id.upper().replace("-", ""),
        board=Board.MAIN,
        market_cap_bucket=MarketCapBucket.SMALL,
        active=True,
        suspended=False,
        surveillance=Surveillance.NONE,
        last_price=value.bars[-1].close,
        median_daily_traded_value=D("20000000"),
        quoted_spread_bps=D("40"),
        lower_circuit_locked=False,
        history_sessions=len(value.bars),
        price_session=current_snapshot.market_session,
        data_available_at=value.bars[-1].available_at,
    )


def trending_histories() -> tuple[InstrumentSwingHistory, ...]:
    return tuple(
        sorted(
            (
                history("inst-a", direction=D("0.8")),
                history("inst-b", direction=D("0.6")),
                history("inst-c", direction=D("0.4"), contraction=True),
                history("inst-d", direction=D("-0.45")),
            ),
            key=lambda value: value.instrument_id,
        )
    )


class RegimeAwareForecastProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.calendar = calendar()
        self.histories = trending_histories()
        self.snapshot = snapshot(self.histories, current_calendar=self.calendar)

    def provider(
        self,
        histories: tuple[InstrumentSwingHistory, ...] | None = None,
        current_snapshot: DataSnapshot | None = None,
    ) -> RegimeAwareForecastProvider:
        return RegimeAwareForecastProvider(
            snapshot=current_snapshot or self.snapshot,
            histories=histories or self.histories,
            calendar=self.calendar,
        )

    def test_builds_deterministic_lineage_bound_forecasts(self) -> None:
        provider = self.provider()
        value = instrument(self.histories[0], self.snapshot)

        first = provider.forecast(value, self.snapshot)
        second = provider.forecast(value, self.snapshot)

        self.assertEqual(first, second)
        self.assertIs(provider.regime, MarketRegime.TRENDING)
        self.assertEqual(first.sample_count, len(AlphaSpecialist))
        self.assertEqual(first.data_snapshot_fingerprint, self.snapshot.content_fingerprint)
        self.assertEqual(first.instrument_fingerprint, value.content_fingerprint)
        self.assertTrue(first.model_version.startswith("regime-aware-alpha-ensemble/v1:"))

    def test_assessment_has_one_ordered_score_per_specialist_and_exact_sum(self) -> None:
        assessment = self.provider().assessment_for("inst-a")

        self.assertEqual(
            tuple(value.specialist for value in assessment.specialist_scores),
            tuple(sorted(AlphaSpecialist, key=lambda value: value.value)),
        )
        self.assertEqual(
            assessment.ensemble_score,
            sum((value.weighted_score for value in assessment.specialist_scores), D("0")),
        )
        assessment.verify_content_identity()

    def test_cross_sectional_momentum_ranking_rewards_stronger_trend(self) -> None:
        provider = self.provider()
        strong = provider.assessment_for("inst-a")
        weak = provider.assessment_for("inst-d")
        strong_score = next(
            value.raw_score
            for value in strong.specialist_scores
            if value.specialist is AlphaSpecialist.MOMENTUM_BREAKOUT
        )
        weak_score = next(
            value.raw_score
            for value in weak.specialist_scores
            if value.specialist is AlphaSpecialist.MOMENTUM_BREAKOUT
        )

        self.assertGreater(strong_score, weak_score)
        self.assertGreater(strong.median_return_pct, weak.median_return_pct)

    def test_equal_metrics_receive_equal_scores_without_identifier_tiebreak(self) -> None:
        histories = (
            history("inst-a", direction=D("0.5")),
            history("inst-b", direction=D("0.5")),
        )
        current_snapshot = snapshot(histories, current_calendar=self.calendar)
        provider = self.provider(histories, current_snapshot)

        first = provider.assessment_for("inst-a")
        second = provider.assessment_for("inst-b")

        self.assertEqual(
            tuple(value.raw_score for value in first.specialist_scores),
            tuple(value.raw_score for value in second.specialist_scores),
        )
        self.assertEqual(first.ensemble_score, second.ensemble_score)

    def test_detects_risk_off_and_uses_liquidity_heavy_weights(self) -> None:
        histories = tuple(
            history(f"inst-{suffix}", direction=D("-0.5") - D(index) / D("10"))
            for index, suffix in enumerate(("a", "b", "c", "d"))
        )
        current_snapshot = snapshot(histories, current_calendar=self.calendar)
        provider = self.provider(histories, current_snapshot)

        assessment = provider.assessment_for("inst-a")
        liquidity = next(
            value
            for value in assessment.specialist_scores
            if value.specialist is AlphaSpecialist.LIQUIDITY_QUALITY
        )

        self.assertIs(provider.regime, MarketRegime.RISK_OFF)
        self.assertEqual(liquidity.regime_weight, D("0.60"))

    def test_detects_high_volatility_before_other_regimes(self) -> None:
        histories = tuple(
            history(f"inst-{suffix}", alternating_volatility=True)
            for suffix in ("a", "b", "c", "d")
        )
        current_snapshot = snapshot(histories, current_calendar=self.calendar)

        self.assertIs(
            self.provider(histories, current_snapshot).regime,
            MarketRegime.HIGH_VOLATILITY,
        )

    def test_rejects_future_known_bar_even_when_snapshot_lists_it(self) -> None:
        changed_bar = replace(
            self.histories[0].bars[-1],
            available_at=DECISION_TIME + timedelta(minutes=1),
        )
        changed_history = replace(
            self.histories[0],
            bars=self.histories[0].bars[:-1] + (changed_bar,),
        )
        histories = (changed_history,) + self.histories[1:]
        current_snapshot = snapshot(histories, current_calendar=self.calendar)

        with self.assertRaisesRegex(RegimeEnsembleError, "future-known evidence"):
            self.provider(histories, current_snapshot)

    def test_rejects_mismatched_cross_sectional_session_coverage(self) -> None:
        shifted_bars = tuple(
            replace(
                value,
                market_session=value.market_session + timedelta(days=1),
                available_at=value.available_at + timedelta(days=1),
                evidence_id=f"shifted-{index}",
                content_hash=f"shifted-hash-{index}",
            )
            for index, value in enumerate(self.histories[1].bars)
        )
        shifted = replace(self.histories[1], bars=shifted_bars)
        histories = (self.histories[0], shifted) + self.histories[2:]
        current_snapshot = snapshot(histories, current_calendar=self.calendar)

        with self.assertRaisesRegex(RegimeEnsembleError, "identical session coverage"):
            self.provider(histories, current_snapshot)

    def test_rejects_bound_history_mutation_after_construction(self) -> None:
        provider = self.provider()
        object.__setattr__(self.histories[0].bars[-1], "close", D("999"))

        with self.assertRaisesRegex(RegimeEnsembleError, "content identity"):
            provider.assessment_for("inst-a")

    def test_rejects_a_different_runtime_snapshot(self) -> None:
        provider = self.provider()
        other = replace(self.snapshot, source_revision="different")

        with self.assertRaisesRegex(RegimeEnsembleError, "another snapshot"):
            provider.forecast(instrument(self.histories[0], self.snapshot), other)

    def test_provider_output_equals_the_public_cross_section_kernel(self) -> None:
        provider = self.provider()
        config = RegimeEnsembleConfig()
        cross_section = calculate_regime_cross_section(self.histories, config)

        self.assertIs(provider.regime, cross_section.regime)
        for history in self.histories:
            assessment = provider.assessment_for(history.instrument_id)
            score = cross_section.score_for(history.instrument_id)
            self.assertEqual(assessment.metrics.metrics_id, score.metrics.metrics_id)
            self.assertEqual(
                tuple(value.score_id for value in assessment.specialist_scores),
                tuple(value.score_id for value in score.specialist_scores),
            )
            self.assertEqual(assessment.ensemble_score, score.ensemble_score)
            self.assertEqual(assessment.median_return_pct, score.median_return_pct)
            self.assertEqual(assessment.downside_return_pct, score.downside_return_pct)
            self.assertEqual(assessment.uncertainty, score.uncertainty)

    def test_cross_section_detects_nested_mutation(self) -> None:
        cross_section = calculate_regime_cross_section(
            self.histories, RegimeEnsembleConfig()
        )
        cross_section.verify_content_identity()
        untouched_id = cross_section.cross_section_id

        object.__setattr__(
            cross_section.scores[0], "ensemble_score", Decimal("0.99")
        )

        with self.assertRaisesRegex(RegimeEnsembleError, "content identity"):
            cross_section.verify_content_identity()
        self.assertEqual(cross_section.cross_section_id, untouched_id)


class RegimeEnsembleMetricAndConfigTests(unittest.TestCase):
    def test_config_rejects_feature_lookback_longer_than_minimum_history(self) -> None:
        with self.assertRaisesRegex(RegimeEnsembleError, "minimum history"):
            RegimeEnsembleConfig(
                minimum_history_sessions=20,
                long_momentum_sessions=50,
            )

    def test_metrics_reject_history_with_no_positive_intraday_range(self) -> None:
        with self.assertRaisesRegex(RegimeEnsembleError, "positive intraday range"):
            calculate_alpha_instrument_metrics(
                history("inst-flat", direction=D("0.5"), flat_range=True),
                RegimeEnsembleConfig(),
            )

    def test_cross_section_rejects_duplicate_listing_id_across_distinct_instruments(
        self,
    ) -> None:
        first = history("inst-a", direction=D("0.6"))
        second = replace(history("inst-b", direction=D("0.4")), listing_id=first.listing_id)
        histories = tuple(sorted((first, second), key=lambda value: value.instrument_id))

        with self.assertRaisesRegex(RegimeEnsembleError, "listing"):
            calculate_regime_cross_section(histories, RegimeEnsembleConfig())


if __name__ == "__main__":
    unittest.main()
