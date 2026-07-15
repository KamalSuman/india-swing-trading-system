from __future__ import annotations

from dataclasses import dataclass, fields, replace
from decimal import Decimal

from india_swing.data.asof import DataIntegrityError, validate_candidate, validate_snapshot
from india_swing.domain.models import (
    Candidate,
    DataSnapshot,
    DecisionAction,
    InstrumentSnapshot,
    PortfolioState,
    ResearchAssessment,
    RunStatus,
    RiskPolicy,
    TradeDecision,
)
from india_swing.forecasting.base import ForecastProvider
from india_swing.research.base import ResearchProvider
from india_swing.risk.engine import RiskEngine, component_identity, content_id
from india_swing.signals.base import SignalProvider
from india_swing.signals.ranking import RankedCandidate, WeightedRanker
from india_swing.universe.eligibility import evaluate_eligibility


@dataclass(frozen=True, slots=True)
class Rejection:
    symbol: str
    stage: str
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PipelineResult:
    run_id: str
    pipeline_version: str
    snapshot_id: str
    decision: TradeDecision
    ranked: tuple[RankedCandidate, ...]
    research: tuple[ResearchAssessment, ...]
    rejections: tuple[Rejection, ...]
    status: RunStatus
    failure_stage: str = ""
    failure_type: str = ""


class Pipeline:
    version = "pipeline-v1"

    def __init__(
        self,
        forecast_provider: ForecastProvider,
        signal_provider: SignalProvider,
        research_provider: ResearchProvider,
        policy: RiskPolicy | None = None,
        ranker: WeightedRanker | None = None,
        research_limit: int = 5,
    ) -> None:
        if research_limit <= 0:
            raise ValueError("research_limit must be positive")
        self.forecast_provider = forecast_provider
        self.signal_provider = signal_provider
        self.research_provider = research_provider
        self.policy = policy or RiskPolicy()
        self.ranker = ranker or WeightedRanker()
        self.research_limit = research_limit
        self.risk_engine = RiskEngine(self.policy)

    def run(
        self,
        snapshot: DataSnapshot,
        instruments: list[InstrumentSnapshot],
        portfolio: PortfolioState,
    ) -> PipelineResult:
        identity_context = {
            "identity_schema": "pipeline-input-v2",
            "pipeline": {
                "type": f"{type(self).__module__}.{type(self).__qualname__}",
                "version": self.version,
                "research_limit": self.research_limit,
            },
            "snapshot": snapshot,
            "instruments": tuple(sorted(instruments, key=lambda item: item.symbol)),
            "portfolio": portfolio,
            "risk_policy": self.policy,
            "components": {
                "forecast_provider": component_identity(self.forecast_provider),
                "signal_provider": component_identity(self.signal_provider),
                "research_provider": component_identity(self.research_provider),
                "ranker": component_identity(self.ranker),
            },
        }
        rejections: list[Rejection] = []
        candidates: list[Candidate] = []

        try:
            validate_snapshot(snapshot)
        except DataIntegrityError as exc:
            rejections.append(
                Rejection("*", "snapshot_integrity", (type(exc).__name__,))
            )
            return self._finish(
                snapshot,
                identity_context,
                (),
                (),
                tuple(rejections),
                status=RunStatus.FAILED,
                failure_stage="snapshot_integrity",
                failure_type=type(exc).__name__,
            )

        for instrument in instruments:
            eligibility = evaluate_eligibility(instrument, self.policy, snapshot)
            if not eligibility.actionable:
                rejections.append(Rejection(instrument.symbol, "eligibility", eligibility.reasons))
                continue
            try:
                forecast = self.forecast_provider.forecast(instrument, snapshot)
                signals, setup, evidence_ids = self.signal_provider.generate(
                    instrument, forecast, snapshot
                )
                candidate = Candidate(instrument, forecast, signals, setup, evidence_ids)
                validate_candidate(candidate, snapshot)
            except DataIntegrityError as exc:
                rejections.append(
                    Rejection(instrument.symbol, "candidate_integrity", (type(exc).__name__,))
                )
                return self._finish(
                    snapshot,
                    identity_context,
                    (),
                    (),
                    tuple(rejections),
                    status=RunStatus.FAILED,
                    failure_stage="candidate_integrity",
                    failure_type=type(exc).__name__,
                )
            except Exception as exc:
                rejections.append(
                    Rejection(instrument.symbol, "candidate_build", (type(exc).__name__,))
                )
                return self._finish(
                    snapshot,
                    identity_context,
                    (),
                    (),
                    tuple(rejections),
                    status=RunStatus.FAILED,
                    failure_stage="candidate_build",
                    failure_type=type(exc).__name__,
                )
            candidates.append(candidate)

        ranked = self.ranker.rank(candidates)
        assessments: list[ResearchAssessment] = []
        approved: list[tuple[Decimal, int, TradeDecision]] = []

        for rank, ranked_candidate in enumerate(ranked[: self.research_limit], start=1):
            candidate = ranked_candidate.candidate
            try:
                assessment = self.research_provider.assess(candidate, snapshot)
            except Exception as exc:
                rejections.append(
                    Rejection(
                        candidate.instrument.symbol,
                        "research",
                        (type(exc).__name__,),
                    )
                )
                return self._finish(
                    snapshot,
                    identity_context,
                    tuple(ranked),
                    tuple(assessments),
                    tuple(rejections),
                    status=RunStatus.FAILED,
                    failure_stage="research",
                    failure_type=type(exc).__name__,
                )
            if assessment.symbol != candidate.instrument.symbol:
                rejections.append(
                    Rejection(
                        candidate.instrument.symbol,
                        "research_integrity",
                        ("assessment symbol mismatch",),
                    )
                )
                return self._finish(
                    snapshot,
                    identity_context,
                    tuple(ranked),
                    tuple(assessments),
                    tuple(rejections),
                    status=RunStatus.FAILED,
                    failure_stage="research_integrity",
                    failure_type="DataIntegrityError",
                )
            unknown_evidence = sorted(set(assessment.evidence_ids) - set(candidate.evidence_ids))
            if unknown_evidence:
                rejections.append(
                    Rejection(
                        candidate.instrument.symbol,
                        "research_integrity",
                        ("assessment cited evidence outside curated context",),
                    )
                )
                return self._finish(
                    snapshot,
                    identity_context,
                    tuple(ranked),
                    tuple(assessments),
                    tuple(rejections),
                    status=RunStatus.FAILED,
                    failure_stage="research_integrity",
                    failure_type="DataIntegrityError",
                )
            assessments.append(assessment)
            evaluation = self.risk_engine.evaluate(
                candidate,
                assessment,
                portfolio,
                rank,
                identity_context=identity_context,
            )
            if evaluation.approved and evaluation.decision is not None:
                approved.append((evaluation.decision.expected_r, rank, evaluation.decision))
            else:
                rejections.append(
                    Rejection(candidate.instrument.symbol, "risk", evaluation.reasons)
                )

        if approved:
            approved.sort(key=lambda item: (-item[0], item[1], item[2].symbol or ""))
            decision = approved[0][2]
        else:
            decision = self._no_trade_decision(
                snapshot,
                ("no candidate passed every deterministic gate",),
            )

        return self._finish(
            snapshot,
            identity_context,
            tuple(ranked),
            tuple(assessments),
            tuple(rejections),
            status=RunStatus.COMPLETE,
            decision=decision,
        )

    @staticmethod
    def _no_trade_decision(
        snapshot: DataSnapshot,
        reasons: tuple[str, ...],
    ) -> TradeDecision:
        return TradeDecision(
            action=DecisionAction.NO_TRADE,
            signal_id="pending-no-trade-id",
            decision_time=snapshot.decision_time,
            symbol=None,
            quantity=0,
            entry_low=None,
            entry_high=None,
            stop=None,
            target=None,
            planned_max_loss=Decimal("0"),
            estimated_cost=Decimal("0"),
            net_reward_risk=Decimal("0"),
            expected_r=Decimal("0"),
            reasons=reasons,
        )

    def _finish(
        self,
        snapshot: DataSnapshot,
        identity_context: object,
        ranked: tuple[RankedCandidate, ...],
        assessments: tuple[ResearchAssessment, ...],
        rejections: tuple[Rejection, ...],
        *,
        status: RunStatus,
        decision: TradeDecision | None = None,
        failure_stage: str = "",
        failure_type: str = "",
    ) -> PipelineResult:
        if decision is None:
            decision = self._no_trade_decision(
                snapshot,
                (f"run failed closed at {failure_stage}: {failure_type}",),
            )
        decision_material = {
            field.name: getattr(decision, field.name)
            for field in fields(decision)
            if field.name != "signal_id"
        }
        run_id = content_id(
            {
                "identity_schema": "pipeline-run-v2",
                "inputs": identity_context,
                "outputs": {
                    "status": status,
                    "failure_stage": failure_stage,
                    "failure_type": failure_type,
                    "ranked": ranked,
                    "research": assessments,
                    "rejections": rejections,
                    "final_decision": decision_material,
                },
            }
        )
        if decision.action is DecisionAction.NO_TRADE:
            decision = replace(decision, signal_id=f"no-trade-{run_id}")
        return PipelineResult(
            run_id=run_id,
            pipeline_version=self.version,
            snapshot_id=snapshot.snapshot_id,
            decision=decision,
            ranked=ranked,
            research=assessments,
            rejections=rejections,
            status=status,
            failure_stage=failure_stage,
            failure_type=failure_type,
        )
