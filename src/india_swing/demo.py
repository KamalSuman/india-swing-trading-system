from __future__ import annotations

import argparse
import json
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from india_swing.audit import AuditWriter, json_value
from india_swing.domain.models import (
    Board,
    DataSnapshot,
    EvidenceItem,
    ForecastSummary,
    InstrumentSnapshot,
    MarketCapBucket,
    PortfolioState,
    ResearchAssessment,
    ResearchVerdict,
    RiskPolicy,
    SignalFeatures,
    Surveillance,
    TradeSetup,
)
from india_swing.pipeline import Pipeline
from india_swing.reference.calendar import (
    CalendarDay,
    CalendarDayKind,
    CalendarSnapshot,
    SessionWindow,
    SessionWindowPhase,
)
from india_swing.reference.context import ReferenceContext
from india_swing.reference.models import (
    EffectiveExternalRecordRef,
    ExternalRecordRef,
    ReferenceReadiness,
)
from india_swing.reference.universe import (
    EligibilityStateRef,
    ListingMapping,
    ListingState,
    UniverseDisposition,
    UniverseEntry,
    UniverseSnapshot,
)
from india_swing.shadow_alerts import (
    LocalShadowNotificationOutbox,
    build_shadow_alert,
)


IST = timezone(timedelta(hours=5, minutes=30))


class StaticForecastProvider:
    model_version = "synthetic-kronos-adapter-v0"

    def __init__(self, forecasts: dict[str, ForecastSummary]) -> None:
        self.forecasts = forecasts

    def forecast(self, instrument, snapshot):
        return self.forecasts[instrument.symbol]


class StaticSignalProvider:
    version = "synthetic-signal-provider-v0"

    def __init__(self, values: dict[str, tuple[SignalFeatures, TradeSetup, tuple[str, ...]]]):
        self.values = values

    def generate(self, instrument, forecast, snapshot):
        return self.values[instrument.symbol]


class StaticResearchProvider:
    model_version = "synthetic-tradingagents-adapter-v0"

    def __init__(self, assessments: dict[str, ResearchAssessment]) -> None:
        self.assessments = assessments

    def assess(self, candidate, snapshot):
        return self.assessments[candidate.instrument.symbol]


def build_demo():
    decision_time = datetime(2026, 7, 15, 17, 0, tzinfo=IST)
    entry_time = datetime(2026, 7, 16, 9, 20, tzinfo=IST)
    evidence = (
        EvidenceItem(
            evidence_id="demo-small-announcement",
            source="SYNTHETIC",
            published_at=datetime(2026, 7, 15, 15, 45, tzinfo=IST),
            available_at=datetime(2026, 7, 15, 15, 46, tzinfo=IST),
            content_hash="demo-small-hash-v1",
        ),
        EvidenceItem(
            evidence_id="demo-market-snapshot",
            source="SYNTHETIC",
            published_at=datetime(2026, 7, 15, 16, 30, tzinfo=IST),
            available_at=datetime(2026, 7, 15, 16, 35, tzinfo=IST),
            content_hash="demo-market-hash-v1",
        ),
    )

    calendar_source_id = "a" * 64
    calendar_start = date(2026, 7, 15)
    calendar_end = date(2026, 7, 28)
    calendar_days: list[CalendarDay] = []
    for offset in range((calendar_end - calendar_start).days + 1):
        day = calendar_start + timedelta(days=offset)
        reference = ExternalRecordRef(
            event_time=datetime.combine(day, time(0), tzinfo=IST),
            knowledge_time=datetime(2026, 7, 1, 12, 0, tzinfo=IST),
            source="SYNTHETIC_CALENDAR_FIXTURE",
            content_hash=f"{day.toordinal():064x}",
            source_snapshot_id=calendar_source_id,
        )
        if day.weekday() < 5:
            calendar_days.append(
                CalendarDay(
                    day=day,
                    kind=CalendarDayKind.REGULAR,
                    reference=reference,
                    session_windows=(
                        SessionWindow(
                            opens_at=datetime.combine(day, time(9, 15), tzinfo=IST),
                            closes_at=datetime.combine(day, time(15, 30), tzinfo=IST),
                            phase=SessionWindowPhase.LIVE_CONTINUOUS,
                        ),
                    ),
                    data_ready_at=datetime.combine(day, time(16, 0), tzinfo=IST),
                )
            )
        else:
            calendar_days.append(
                CalendarDay(
                    day=day,
                    kind=CalendarDayKind.WEEKEND,
                    reference=reference,
                )
            )
    calendar = CalendarSnapshot.create(
        exchange="NSE",
        segment="CM",
        cutoff=decision_time,
        coverage_start=calendar_start,
        coverage_end=calendar_end,
        days=tuple(calendar_days),
        source_snapshot_ids=(calendar_source_id,),
        readiness=ReferenceReadiness.SYNTHETIC_TEST,
    )

    master_source_id = "b" * 64
    eligibility_source_id = "c" * 64
    liquidity_source_id = "d" * 64

    def universe_entry(
        *,
        number: int,
        instrument_id: str,
        listing_id: str,
        symbol: str,
        surveillance: Surveillance,
        disposition: UniverseDisposition,
        reason_codes: tuple[str, ...],
    ) -> UniverseEntry:
        listing_reference = ExternalRecordRef(
            event_time=datetime(2020, 1, 1, tzinfo=IST),
            knowledge_time=datetime(2026, 7, 15, 8, 30, tzinfo=IST),
            source="SYNTHETIC_SECURITY_MASTER_FIXTURE",
            content_hash=f"{number:064x}",
            source_snapshot_id=master_source_id,
        )
        eligibility_reference = ExternalRecordRef(
            event_time=datetime.combine(decision_time.date(), time(0), tzinfo=IST),
            knowledge_time=datetime(2026, 7, 15, 16, 0, tzinfo=IST),
            source="SYNTHETIC_ELIGIBILITY_FIXTURE",
            content_hash=f"{number + 100:064x}",
            source_snapshot_id=eligibility_source_id,
        )
        return UniverseEntry(
            source_record_id=f"{number + 200:064x}",
            listing=ListingMapping(
                instrument_id=instrument_id,
                listing_id=listing_id,
                exchange="NSE",
                segment="CM",
                tradingsymbol=symbol,
                series="EQ",
                isin=f"INE{number:04d}A0101",
                valid_from=date(2020, 1, 1),
                valid_to_exclusive=None,
                reference=listing_reference,
            ),
            board=Board.MAIN,
            listing_state=ListingState.ACTIVE,
            suspended=False,
            surveillance=surveillance,
            disposition=disposition,
            reason_codes=reason_codes,
            eligibility_refs=(
                EligibilityStateRef(
                    effective=EffectiveExternalRecordRef(
                        reference=eligibility_reference,
                        effective_from_session=decision_time.date(),
                        effective_to_exclusive=None,
                        schema_version="synthetic-eligibility/v1",
                    ),
                    instrument_id=instrument_id,
                    listing_id=listing_id,
                    board=Board.MAIN,
                    listing_state=ListingState.ACTIVE,
                    suspended=False,
                    surveillance=surveillance,
                ),
            ),
            liquidity_snapshot_id=liquidity_source_id,
            liquidity_cutoff_session=decision_time.date(),
        )

    universe_entries = tuple(
        sorted(
            (
                universe_entry(
                    number=1,
                    instrument_id="synthetic-instrument-small",
                    listing_id="synthetic-listing-small",
                    symbol="DEMO-SMALL",
                    surveillance=Surveillance.NONE,
                    disposition=UniverseDisposition.ACTIONABLE,
                    reason_codes=(),
                ),
                universe_entry(
                    number=2,
                    instrument_id="synthetic-instrument-large",
                    listing_id="synthetic-listing-large",
                    symbol="DEMO-LARGE",
                    surveillance=Surveillance.NONE,
                    disposition=UniverseDisposition.ACTIONABLE,
                    reason_codes=(),
                ),
                universe_entry(
                    number=3,
                    instrument_id="synthetic-instrument-gsm",
                    listing_id="synthetic-listing-gsm",
                    symbol="DEMO-GSM",
                    surveillance=Surveillance.GSM,
                    disposition=UniverseDisposition.EXCLUDED,
                    reason_codes=("GSM_BLOCKED",),
                ),
            ),
            key=lambda entry: entry.source_record_id,
        )
    )
    universe = UniverseSnapshot.create(
        exchange="NSE",
        segment="CM",
        market_session=decision_time.date(),
        cutoff=decision_time,
        calendar_snapshot_id=calendar.snapshot_id,
        universe_rules_version="synthetic-universe-rules/v1",
        selection_key="NSE:CM:SYNTHETIC_ALL_ROWS",
        scoped_source_row_ids=tuple(
            entry.source_record_id for entry in universe_entries
        ),
        security_master_snapshot_ids=(master_source_id,),
        eligibility_snapshot_ids=(eligibility_source_id,),
        liquidity_snapshot_ids=(liquidity_source_id,),
        readiness=ReferenceReadiness.SYNTHETIC_TEST,
        entries=universe_entries,
    )
    reference_context = ReferenceContext(calendar=calendar, universe=universe)

    snapshot = DataSnapshot(
        snapshot_id="synthetic-2026-07-15-v1",
        decision_time=decision_time,
        market_session=decision_time.date(),
        evidence=evidence,
        session_finalized_at=datetime(2026, 7, 15, 16, 30, tzinfo=IST),
        universe_snapshot_id=universe.snapshot_id,
        calendar_version=calendar.version,
        trial_id="synthetic-demo-trial-v1",
        model_bundle_id="synthetic-demo-bundle-v1",
        data_content_hash="synthetic-data-hash-v1",
        source_revision="working-tree-synthetic-demo",
        execution_policy_version="next-session-limit-v1",
        cost_schedule_version="synthetic-flat-cost-v1",
    )

    instruments = [
        InstrumentSnapshot(
            instrument_id="synthetic-instrument-small",
            listing_id="synthetic-listing-small",
            universe_snapshot_id=universe.snapshot_id,
            exchange="NSE",
            segment="CM",
            symbol="DEMO-SMALL",
            board=Board.MAIN,
            market_cap_bucket=MarketCapBucket.SMALL,
            active=True,
            suspended=False,
            surveillance=Surveillance.NONE,
            last_price=Decimal("100"),
            median_daily_traded_value=Decimal("20000000"),
            quoted_spread_bps=Decimal("25"),
            lower_circuit_locked=False,
            history_sessions=500,
            price_session=decision_time.date(),
            data_available_at=datetime(2026, 7, 15, 16, 35, tzinfo=IST),
        ),
        InstrumentSnapshot(
            instrument_id="synthetic-instrument-large",
            listing_id="synthetic-listing-large",
            universe_snapshot_id=universe.snapshot_id,
            exchange="NSE",
            segment="CM",
            symbol="DEMO-LARGE",
            board=Board.MAIN,
            market_cap_bucket=MarketCapBucket.LARGE,
            active=True,
            suspended=False,
            surveillance=Surveillance.NONE,
            last_price=Decimal("1000"),
            median_daily_traded_value=Decimal("900000000"),
            quoted_spread_bps=Decimal("8"),
            lower_circuit_locked=False,
            history_sessions=1000,
            price_session=decision_time.date(),
            data_available_at=datetime(2026, 7, 15, 16, 35, tzinfo=IST),
        ),
    ]

    forecasts = {
        "DEMO-SMALL": ForecastSummary(
            "DEMO-SMALL",
            decision_time,
            8,
            Decimal("6.0"),
            Decimal("-2.0"),
            Decimal("0.25"),
            50,
            "synthetic-kronos-adapter-v0",
            "synthetic-instrument-small",
            "synthetic-listing-small",
            universe.snapshot_id,
            snapshot.snapshot_id,
            snapshot.content_fingerprint,
            instruments[0].content_fingerprint,
        ),
        "DEMO-LARGE": ForecastSummary(
            "DEMO-LARGE",
            decision_time,
            8,
            Decimal("3.0"),
            Decimal("-1.5"),
            Decimal("0.15"),
            50,
            "synthetic-kronos-adapter-v0",
            "synthetic-instrument-large",
            "synthetic-listing-large",
            universe.snapshot_id,
            snapshot.snapshot_id,
            snapshot.content_fingerprint,
            instruments[1].content_fingerprint,
        ),
    }
    signal_values = {
        "DEMO-SMALL": (
            SignalFeatures(
                Decimal("0.82"),
                Decimal("0.76"),
                Decimal("0.70"),
                Decimal("0.72"),
                Decimal("0.35"),
                Decimal("30"),
                "synthetic-instrument-small",
                "synthetic-listing-small",
                universe.snapshot_id,
                snapshot.snapshot_id,
                snapshot.content_fingerprint,
                instruments[0].content_fingerprint,
                "synthetic-signal-provider-v0",
            ),
            TradeSetup(
                "DEMO-SMALL",
                decision_time,
                entry_time,
                Decimal("100"),
                Decimal("101"),
                Decimal("96"),
                Decimal("115"),
                Decimal("0.45"),
                Decimal("0.35"),
                Decimal("0"),
                8,
                "Synthetic trend, relative-strength, and forecast alignment.",
                "The setup is invalid below the synthetic swing structure at 96.",
                "The first synthetic resistance zone above 2.5R begins near 115.",
                ("cancel above 104", "cancel on a new adverse filing"),
                entry_expires_at=datetime(2026, 7, 16, 15, 15, tzinfo=IST),
                instrument_id="synthetic-instrument-small",
                listing_id="synthetic-listing-small",
                universe_snapshot_id=universe.snapshot_id,
                data_snapshot_id=snapshot.snapshot_id,
                data_snapshot_fingerprint=snapshot.content_fingerprint,
                instrument_fingerprint=instruments[0].content_fingerprint,
                provider_version="synthetic-signal-provider-v0",
            ),
            ("demo-small-announcement", "demo-market-snapshot"),
        ),
        "DEMO-LARGE": (
            SignalFeatures(
                Decimal("0.58"),
                Decimal("0.64"),
                Decimal("0.55"),
                Decimal("0.95"),
                Decimal("0"),
                Decimal("18"),
                "synthetic-instrument-large",
                "synthetic-listing-large",
                universe.snapshot_id,
                snapshot.snapshot_id,
                snapshot.content_fingerprint,
                instruments[1].content_fingerprint,
                "synthetic-signal-provider-v0",
            ),
            TradeSetup(
                "DEMO-LARGE",
                decision_time,
                entry_time,
                Decimal("998"),
                Decimal("1002"),
                Decimal("982"),
                Decimal("1065"),
                Decimal("0.40"),
                Decimal("0.38"),
                Decimal("-0.05"),
                8,
                "Synthetic large-cap continuation setup.",
                "The setup is invalid below 982.",
                "Synthetic resistance and forecast range converge near 1065.",
                ("cancel above 1015",),
                entry_expires_at=datetime(2026, 7, 16, 15, 15, tzinfo=IST),
                instrument_id="synthetic-instrument-large",
                listing_id="synthetic-listing-large",
                universe_snapshot_id=universe.snapshot_id,
                data_snapshot_id=snapshot.snapshot_id,
                data_snapshot_fingerprint=snapshot.content_fingerprint,
                instrument_fingerprint=instruments[1].content_fingerprint,
                provider_version="synthetic-signal-provider-v0",
            ),
            ("demo-market-snapshot",),
        ),
    }
    assessments = {
        "DEMO-SMALL": ResearchAssessment(
            "DEMO-SMALL",
            ResearchVerdict.APPROVE,
            "Synthetic evidence supports the provisional long setup.",
            "Small-cap gap risk and forecast uncertainty remain material.",
            ("overnight gap", "liquidity deterioration"),
            ("demo-small-announcement", "demo-market-snapshot"),
            "synthetic-tradingagents-adapter-v0",
            "synthetic-instrument-small",
            "synthetic-listing-small",
            universe.snapshot_id,
            snapshot.snapshot_id,
            snapshot.content_fingerprint,
            instruments[0].content_fingerprint,
        ),
        "DEMO-LARGE": ResearchAssessment(
            "DEMO-LARGE",
            ResearchVerdict.UNCERTAIN,
            "Synthetic evidence is mixed.",
            "The expected advantage is not sufficiently differentiated.",
            ("weak catalyst",),
            ("demo-market-snapshot",),
            "synthetic-tradingagents-adapter-v0",
            "synthetic-instrument-large",
            "synthetic-listing-large",
            universe.snapshot_id,
            snapshot.snapshot_id,
            snapshot.content_fingerprint,
            instruments[1].content_fingerprint,
        ),
    }
    pipeline = Pipeline(
        StaticForecastProvider(forecasts),
        StaticSignalProvider(signal_values),
        StaticResearchProvider(assessments),
        # Provisional probabilities are permitted only in this fictional smoke test.
        RiskPolicy(require_validated_probabilities=False),
    )
    portfolio = PortfolioState(Decimal("100000"), Decimal("0"), Decimal("0"))
    return pipeline, snapshot, instruments, portfolio, reference_context


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the synthetic India Swing vertical slice")
    parser.add_argument("--output-dir", type=Path, default=Path("var/audit"))
    parser.add_argument(
        "--shadow-outbox-dir",
        type=Path,
        default=None,
        help="optionally publish a research-only local shadow notification",
    )
    args = parser.parse_args(argv)

    pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
    result = pipeline.run(snapshot, instruments, portfolio, reference_context)
    payload = {
        "mode": "SYNTHETIC_DEMO_ONLY",
        "run_status": result.status,
        "failure_stage": result.failure_stage,
        "failure_type": result.failure_type,
        "snapshot": snapshot,
        "portfolio": portfolio,
        "reference_context": reference_context,
        "policy": pipeline.policy,
        "result": result,
    }
    audit_path = AuditWriter().write_pipeline_result(args.output_dir, result, payload)
    shadow_notification_path = None
    if args.shadow_outbox_dir is not None:
        shadow_alert = build_shadow_alert(result)
        shadow_outbox = LocalShadowNotificationOutbox(args.shadow_outbox_dir)
        shadow_outbox.put(shadow_alert)
        shadow_notification_path = shadow_outbox.path_for(shadow_alert.alert_id)
    summary = {
        "mode": "SYNTHETIC_DEMO_ONLY",
        "run_status": result.status,
        "failure_stage": result.failure_stage,
        "failure_type": result.failure_type,
        "action": result.decision.action,
        "symbol": result.decision.symbol,
        "quantity": result.decision.quantity,
        "order_type": result.decision.order_type,
        "earliest_entry_at": result.decision.earliest_entry_at,
        "entry_expires_at": result.decision.entry_expires_at,
        "entry_range": (result.decision.entry_low, result.decision.entry_high),
        "stop": result.decision.stop,
        "target": result.decision.target,
        "max_holding_sessions": result.decision.max_holding_sessions,
        "planned_max_loss": result.decision.planned_max_loss,
        "net_reward_risk": result.decision.net_reward_risk,
        "expected_r": result.decision.expected_r,
        "target_probability": result.decision.target_probability,
        "stop_probability": result.decision.stop_probability,
        "probability_status": result.decision.probability_status,
        "calibration_sample_size": result.decision.calibration_sample_size,
        "rationale": result.decision.reasons,
        "thesis": result.decision.thesis,
        "bear_case": result.decision.bear_case,
        "cancel_conditions": result.decision.cancel_conditions,
        "audit_path": str(audit_path.resolve()),
        "shadow_notification_path": (
            str(shadow_notification_path.resolve())
            if shadow_notification_path is not None
            else None
        ),
    }
    print(json.dumps(json_value(summary), indent=2))
    return 0 if result.status.value == "COMPLETE" else 2


if __name__ == "__main__":
    raise SystemExit(main())
