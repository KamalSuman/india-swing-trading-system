from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
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
    snapshot = DataSnapshot(
        snapshot_id="synthetic-2026-07-15-v1",
        decision_time=decision_time,
        market_session=decision_time.date(),
        evidence=evidence,
        session_finalized_at=datetime(2026, 7, 15, 16, 30, tzinfo=IST),
        universe_snapshot_id="synthetic-universe-2026-07-15-v1",
        calendar_version="synthetic-calendar-v1",
        trial_id="synthetic-demo-trial-v1",
        model_bundle_id="synthetic-demo-bundle-v1",
        data_content_hash="synthetic-data-hash-v1",
        source_revision="working-tree-synthetic-demo",
        execution_policy_version="next-session-limit-v1",
        cost_schedule_version="synthetic-flat-cost-v1",
    )

    instruments = [
        InstrumentSnapshot(
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
        InstrumentSnapshot(
            symbol="DEMO-GSM",
            board=Board.MAIN,
            market_cap_bucket=MarketCapBucket.MICRO,
            active=True,
            suspended=False,
            surveillance=Surveillance.GSM,
            last_price=Decimal("40"),
            median_daily_traded_value=Decimal("2000000"),
            quoted_spread_bps=Decimal("40"),
            lower_circuit_locked=False,
            history_sessions=300,
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
        ),
        "DEMO-LARGE": ResearchAssessment(
            "DEMO-LARGE",
            ResearchVerdict.UNCERTAIN,
            "Synthetic evidence is mixed.",
            "The expected advantage is not sufficiently differentiated.",
            ("weak catalyst",),
            ("demo-market-snapshot",),
            "synthetic-tradingagents-adapter-v0",
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
    return pipeline, snapshot, instruments, portfolio


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the synthetic India Swing vertical slice")
    parser.add_argument("--output-dir", type=Path, default=Path("var/audit"))
    args = parser.parse_args(argv)

    pipeline, snapshot, instruments, portfolio = build_demo()
    result = pipeline.run(snapshot, instruments, portfolio)
    payload = {
        "mode": "SYNTHETIC_DEMO_ONLY",
        "run_status": result.status,
        "failure_stage": result.failure_stage,
        "failure_type": result.failure_type,
        "snapshot": snapshot,
        "portfolio": portfolio,
        "policy": pipeline.policy,
        "result": result,
    }
    audit_path = AuditWriter().write(args.output_dir, result.run_id, payload)
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
    }
    print(json.dumps(json_value(summary), indent=2))
    return 0 if result.status.value == "COMPLETE" else 2


if __name__ == "__main__":
    raise SystemExit(main())
