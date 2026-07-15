from __future__ import annotations

from dataclasses import dataclass, field, fields, replace
from datetime import datetime, timezone
from decimal import Decimal

from india_swing.data.asof import DataIntegrityError, validate_candidate, validate_snapshot
from india_swing.domain.models import (
    Candidate,
    DataSnapshot,
    DecisionAction,
    ForecastSummary,
    InstrumentSnapshot,
    PortfolioState,
    ResearchAssessment,
    RunStatus,
    RiskPolicy,
    SignalFeatures,
    TradeDecision,
    TradeSetup,
)
from india_swing.forecasting.base import ForecastProvider
from india_swing.identity import component_identity, content_id
from india_swing.research.base import ResearchProvider
from india_swing.reference.context import (
    ReferenceContext,
    validate_reference_context,
)
from india_swing.reference.universe import UniverseDisposition
from india_swing.risk.engine import RiskEngine
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
    snapshot_fingerprint: str
    universe_snapshot_id: str
    calendar_snapshot_id: str
    calendar_version: str
    reference_readiness: str
    declared_universe_snapshot_id: str
    declared_calendar_version: str
    context_universe_snapshot_id: str
    context_calendar_snapshot_id: str
    context_calendar_version: str
    context_reference_readiness: str
    context_universe_readiness: str
    validated_input_fingerprint: str
    final_input_fingerprint: str
    trial_id: str
    model_bundle_id: str
    data_content_hash: str
    source_revision: str
    execution_policy_version: str
    cost_schedule_version: str
    decision: TradeDecision
    ranked: tuple[RankedCandidate, ...]
    research: tuple[ResearchAssessment, ...]
    rejections: tuple[Rejection, ...]
    status: RunStatus
    failure_stage: str = ""
    failure_type: str = ""
    integrity_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for name in (
            "trial_id",
            "model_bundle_id",
            "data_content_hash",
            "source_revision",
            "execution_policy_version",
            "cost_schedule_version",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"pipeline result {name} is required")
        if type(self.decision) is not TradeDecision:
            raise TypeError("pipeline decision must be an exact TradeDecision")
        if self.decision.reference_readiness != self.reference_readiness:
            raise ValueError("decision and pipeline reference readiness disagree")
        self.decision.verify_integrity()
        if type(self.ranked) is not tuple or any(
            type(item) is not RankedCandidate for item in self.ranked
        ):
            raise TypeError("pipeline ranked output must be an immutable typed tuple")
        if type(self.research) is not tuple or any(
            type(item) is not ResearchAssessment for item in self.research
        ):
            raise TypeError("pipeline research output must be an immutable typed tuple")
        if type(self.rejections) is not tuple or any(
            type(item) is not Rejection for item in self.rejections
        ):
            raise TypeError("pipeline rejections must be an immutable typed tuple")
        object.__setattr__(self, "integrity_hash", self._calculated_integrity_hash())

    def _calculated_integrity_hash(self) -> str:
        material = {
            item.name: getattr(self, item.name)
            for item in fields(self)
            if item.name != "integrity_hash"
        }
        return content_id(material, length=64)

    def verify_integrity(self) -> None:
        if self.integrity_hash != self._calculated_integrity_hash():
            raise ValueError("pipeline result integrity verification failed")
        self.decision.verify_integrity()


class Pipeline:
    version = "pipeline-v2"

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
        for component, attribute in (
            (self.forecast_provider, "model_version"),
            (self.signal_provider, "version"),
            (self.research_provider, "model_version"),
            (self.ranker, "version"),
        ):
            value = getattr(component, attribute, None)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(
                    f"{type(component).__name__}.{attribute} must be a nonempty string"
                )

    def run(
        self,
        snapshot: DataSnapshot,
        instruments: list[InstrumentSnapshot],
        portfolio: PortfolioState,
        reference_context: ReferenceContext,
    ) -> PipelineResult:
        rejections: list[Rejection] = []
        candidates: list[Candidate] = []

        try:
            if type(snapshot) is not DataSnapshot:
                raise TypeError("snapshot must be an exact DataSnapshot")
            if type(instruments) is not list or any(
                type(instrument) is not InstrumentSnapshot
                for instrument in instruments
            ):
                raise TypeError(
                    "instruments must be a list of exact InstrumentSnapshot values"
                )
            if type(portfolio) is not PortfolioState:
                raise TypeError("portfolio must be an exact PortfolioState")
            if type(reference_context) is not ReferenceContext:
                raise TypeError("reference_context must be an exact ReferenceContext")
            validate_snapshot(snapshot)
        except Exception as exc:
            return self._invalid_input_result(
                snapshot,
                reference_context,
                stage="snapshot_integrity",
                error=exc,
            )

        try:
            validate_reference_context(snapshot, instruments, reference_context)
        except Exception as exc:
            return self._invalid_input_result(
                snapshot,
                reference_context,
                stage="reference_integrity",
                error=exc,
            )

        try:
            captured_policy = replace(self.policy)
            captured_portfolio = replace(portfolio)
            forecast_provider = self.forecast_provider
            signal_provider = self.signal_provider
            research_provider = self.research_provider
            ranker = self.ranker
            research_limit = self.research_limit
            provider_versions = {
                "forecast": forecast_provider.model_version,
                "signals": signal_provider.version,
                "research": research_provider.model_version,
            }
            identity_context = self._input_identity_context(
                snapshot=snapshot,
                instruments=tuple(instruments),
                portfolio=captured_portfolio,
                reference_context=reference_context,
                policy=captured_policy,
                research_limit=research_limit,
                forecast_provider=forecast_provider,
                signal_provider=signal_provider,
                research_provider=research_provider,
                ranker=ranker,
            )
            validated_input_fingerprint = content_id(identity_context, length=64)
            risk_engine = RiskEngine(captured_policy)
        except Exception as exc:
            return self._invalid_input_result(
                snapshot,
                reference_context,
                stage="input_identity",
                error=exc,
            )

        for entry in reference_context.universe.entries:
            if entry.disposition is not UniverseDisposition.ACTIONABLE:
                rejections.append(
                    Rejection(
                        entry.listing.tradingsymbol,
                        "universe",
                        entry.reason_codes,
                    )
                )

        for instrument in instruments:
            eligibility = evaluate_eligibility(instrument, captured_policy, snapshot)
            if not eligibility.actionable:
                rejections.append(Rejection(instrument.symbol, "eligibility", eligibility.reasons))
                continue
            try:
                raw_forecast = forecast_provider.forecast(instrument, snapshot)
                if type(raw_forecast) is not ForecastSummary:
                    raise TypeError(
                        "forecast provider must return an exact ForecastSummary"
                    )
                if raw_forecast.model_version != provider_versions["forecast"]:
                    raise DataIntegrityError(
                        "forecast model version does not match its provider"
                    )
                forecast = replace(raw_forecast)
                forecast_fingerprint = content_id(forecast, length=64)
                generated = signal_provider.generate(
                    instrument, forecast, snapshot
                )
                if content_id(forecast, length=64) != forecast_fingerprint:
                    raise DataIntegrityError(
                        "signal provider mutated its forecast input"
                    )
                if type(generated) is not tuple or len(generated) != 3:
                    raise TypeError(
                        "signal provider must return a three-item immutable tuple"
                    )
                raw_signals, raw_setup, evidence_ids = generated
                if type(raw_signals) is not SignalFeatures:
                    raise TypeError(
                        "signal provider must return exact SignalFeatures"
                    )
                if type(raw_setup) is not TradeSetup:
                    raise TypeError("signal provider must return an exact TradeSetup")
                if (
                    raw_signals.provider_version != provider_versions["signals"]
                    or raw_setup.provider_version != provider_versions["signals"]
                ):
                    raise DataIntegrityError(
                        "signal output version does not match its provider"
                    )
                candidate = Candidate(
                    replace(instrument),
                    replace(forecast),
                    replace(raw_signals),
                    replace(raw_setup),
                    tuple(evidence_ids) if type(evidence_ids) is tuple else evidence_ids,
                )
                validate_candidate(candidate, snapshot, reference_context.calendar)
            except DataIntegrityError as exc:
                rejections.append(
                    Rejection(instrument.symbol, "candidate_integrity", (type(exc).__name__,))
                )
                return self._finish(
                    snapshot,
                    identity_context,
                    reference_context,
                    (),
                    (),
                    tuple(rejections),
                    instruments=tuple(instruments),
                    validated_input_fingerprint=validated_input_fingerprint,
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
                    reference_context,
                    (),
                    (),
                    tuple(rejections),
                    instruments=tuple(instruments),
                    validated_input_fingerprint=validated_input_fingerprint,
                    status=RunStatus.FAILED,
                    failure_stage="candidate_build",
                    failure_type=type(exc).__name__,
                )
            candidates.append(candidate)

        try:
            expected_candidates = {
                content_id(candidate, length=64): candidate for candidate in candidates
            }
            raw_ranked = ranker.rank(candidates)
            if type(raw_ranked) is not list or len(raw_ranked) != len(candidates):
                raise TypeError(
                    "ranker must return one list item for every candidate"
                )
            ranked: list[RankedCandidate] = []
            seen_candidate_ids: set[str] = set()
            for item in raw_ranked:
                if type(item) is not RankedCandidate:
                    raise TypeError("ranker must return exact RankedCandidate values")
                candidate_id = content_id(item.candidate, length=64)
                if (
                    candidate_id not in expected_candidates
                    or candidate_id in seen_candidate_ids
                ):
                    raise DataIntegrityError(
                        "ranker changed, duplicated, or substituted a candidate"
                    )
                if type(item.score) is not Decimal or type(item.components) is not tuple:
                    raise TypeError("ranked score/components have invalid types")
                if any(
                    type(component) is not tuple
                    or len(component) != 2
                    or not isinstance(component[0], str)
                    or type(component[1]) is not Decimal
                    for component in item.components
                ):
                    raise TypeError("ranked components have invalid types")
                seen_candidate_ids.add(candidate_id)
                ranked.append(
                    RankedCandidate(
                        self._defensive_candidate_copy(item.candidate),
                        item.score,
                        tuple(item.components),
                    )
                )
        except Exception as exc:
            rejections.append(Rejection("*", "ranking", (type(exc).__name__,)))
            return self._finish(
                snapshot,
                identity_context,
                reference_context,
                (),
                (),
                tuple(rejections),
                instruments=tuple(instruments),
                validated_input_fingerprint=validated_input_fingerprint,
                status=RunStatus.FAILED,
                failure_stage="ranking",
                failure_type=type(exc).__name__,
            )
        assessments: list[ResearchAssessment] = []
        approved: list[tuple[Decimal, int, TradeDecision]] = []

        for rank, ranked_candidate in enumerate(ranked[:research_limit], start=1):
            candidate = ranked_candidate.candidate
            candidate_fingerprint = content_id(candidate, length=64)
            try:
                raw_assessment = research_provider.assess(candidate, snapshot)
                if content_id(candidate, length=64) != candidate_fingerprint:
                    raise DataIntegrityError(
                        "research provider mutated its candidate input"
                    )
                if type(raw_assessment) is not ResearchAssessment:
                    raise TypeError(
                        "research provider must return an exact ResearchAssessment"
                    )
                if raw_assessment.model_version != provider_versions["research"]:
                    raise DataIntegrityError(
                        "research model version does not match its provider"
                    )
                assessment = replace(raw_assessment)
                candidate = self._defensive_candidate_copy(candidate)
                validate_candidate(candidate, snapshot, reference_context.calendar)
            except (DataIntegrityError, TypeError, ValueError) as exc:
                rejections.append(
                    Rejection(
                        candidate.instrument.symbol,
                        "research_integrity",
                        (type(exc).__name__,),
                    )
                )
                return self._finish(
                    snapshot,
                    identity_context,
                    reference_context,
                    (),
                    (),
                    tuple(rejections),
                    instruments=tuple(instruments),
                    validated_input_fingerprint=validated_input_fingerprint,
                    status=RunStatus.FAILED,
                    failure_stage="research_integrity",
                    failure_type=type(exc).__name__,
                )
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
                    reference_context,
                    (),
                    (),
                    tuple(rejections),
                    instruments=tuple(instruments),
                    validated_input_fingerprint=validated_input_fingerprint,
                    status=RunStatus.FAILED,
                    failure_stage="research",
                    failure_type=type(exc).__name__,
                )
            if (
                assessment.symbol != candidate.instrument.symbol
                or assessment.instrument_id != candidate.instrument.instrument_id
                or assessment.listing_id != candidate.instrument.listing_id
                or assessment.universe_snapshot_id
                != candidate.instrument.universe_snapshot_id
                or assessment.data_snapshot_id != snapshot.snapshot_id
                or assessment.data_snapshot_fingerprint
                != snapshot.content_fingerprint
                or assessment.instrument_fingerprint
                != candidate.instrument.content_fingerprint
            ):
                rejections.append(
                    Rejection(
                        candidate.instrument.symbol,
                        "research_integrity",
                        ("assessment identity or snapshot lineage mismatch",),
                    )
                )
                return self._finish(
                    snapshot,
                    identity_context,
                    reference_context,
                    (),
                    (),
                    tuple(rejections),
                    instruments=tuple(instruments),
                    validated_input_fingerprint=validated_input_fingerprint,
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
                    reference_context,
                    (),
                    (),
                    tuple(rejections),
                    instruments=tuple(instruments),
                    validated_input_fingerprint=validated_input_fingerprint,
                    status=RunStatus.FAILED,
                    failure_stage="research_integrity",
                    failure_type="DataIntegrityError",
                )
            assessments.append(assessment)
            try:
                evaluation = risk_engine.evaluate(
                    candidate,
                    assessment,
                    captured_portfolio,
                    rank,
                    identity_context=identity_context,
                    reference_readiness=reference_context.calendar.readiness.value,
                    execution_eligible=False,
                )
            except Exception as exc:
                rejections.append(
                    Rejection(
                        candidate.instrument.symbol,
                        "risk_engine",
                        (type(exc).__name__,),
                    )
                )
                return self._finish(
                    snapshot,
                    identity_context,
                    reference_context,
                    (),
                    (),
                    tuple(rejections),
                    instruments=tuple(instruments),
                    validated_input_fingerprint=validated_input_fingerprint,
                    status=RunStatus.FAILED,
                    failure_stage="risk_engine",
                    failure_type=type(exc).__name__,
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
            reference_context,
            tuple(ranked),
            tuple(assessments),
            tuple(rejections),
            instruments=tuple(instruments),
            validated_input_fingerprint=validated_input_fingerprint,
            status=RunStatus.COMPLETE,
            decision=decision,
        )

    def _input_identity_context(
        self,
        *,
        snapshot: DataSnapshot,
        instruments: tuple[InstrumentSnapshot, ...],
        portfolio: PortfolioState,
        reference_context: ReferenceContext,
        policy: RiskPolicy,
        research_limit: int,
        forecast_provider: object,
        signal_provider: object,
        research_provider: object,
        ranker: object,
    ) -> dict[str, object]:
        if type(research_limit) is not int or research_limit <= 0:
            raise ValueError("research_limit must remain a positive integer")
        component_versions = (
            getattr(forecast_provider, "model_version", None),
            getattr(signal_provider, "version", None),
            getattr(research_provider, "model_version", None),
            getattr(ranker, "version", None),
        )
        if any(
            not isinstance(version, str) or not version.strip()
            for version in component_versions
        ):
            raise ValueError("every pipeline component requires a stable version")
        return {
            "identity_schema": "pipeline-input-v4",
            "pipeline": {
                "type": f"{type(self).__module__}.{type(self).__qualname__}",
                "version": self.version,
                "research_limit": research_limit,
            },
            "snapshot": snapshot,
            "instruments": tuple(
                sorted(
                    instruments,
                    key=lambda item: (item.instrument_id, item.symbol),
                )
            ),
            "portfolio": portfolio,
            "reference_context": reference_context,
            "risk_policy": policy,
            "components": {
                "forecast_provider": component_identity(forecast_provider),
                "signal_provider": component_identity(signal_provider),
                "research_provider": component_identity(research_provider),
                "ranker": component_identity(ranker),
            },
        }

    @staticmethod
    def _safe_text(owner: object, name: str, default: str = "INVALID") -> str:
        value = getattr(owner, name, None)
        return value if isinstance(value, str) and value.strip() else default

    @staticmethod
    def _safe_decision_time(snapshot: object) -> datetime:
        value = getattr(snapshot, "decision_time", None)
        if (
            isinstance(value, datetime)
            and value.tzinfo is not None
            and value.utcoffset() is not None
        ):
            return value
        return datetime(1970, 1, 1, tzinfo=timezone.utc)

    @staticmethod
    def _safe_readiness(owner: object) -> str:
        readiness = getattr(owner, "readiness", None)
        value = getattr(readiness, "value", None)
        return value if isinstance(value, str) and value.strip() else "INVALID"

    def _invalid_input_result(
        self,
        snapshot: object,
        reference_context: object,
        *,
        stage: str,
        error: Exception,
    ) -> PipelineResult:
        calendar = getattr(reference_context, "calendar", None)
        universe = getattr(reference_context, "universe", None)
        failure_type = type(error).__name__
        safe_input = {
            "snapshot_id": self._safe_text(snapshot, "snapshot_id"),
            "snapshot_fingerprint": self._safe_text(
                snapshot,
                "content_fingerprint",
            ),
            "declared_universe_snapshot_id": self._safe_text(
                snapshot,
                "universe_snapshot_id",
            ),
            "declared_calendar_version": self._safe_text(
                snapshot,
                "calendar_version",
            ),
            "context_universe_snapshot_id": self._safe_text(universe, "snapshot_id"),
            "context_calendar_snapshot_id": self._safe_text(calendar, "snapshot_id"),
            "context_calendar_version": self._safe_text(calendar, "version"),
            "trial_id": self._safe_text(snapshot, "trial_id"),
            "model_bundle_id": self._safe_text(snapshot, "model_bundle_id"),
            "data_content_hash": self._safe_text(snapshot, "data_content_hash"),
            "source_revision": self._safe_text(snapshot, "source_revision"),
            "execution_policy_version": self._safe_text(
                snapshot,
                "execution_policy_version",
            ),
            "cost_schedule_version": self._safe_text(
                snapshot,
                "cost_schedule_version",
            ),
        }
        final_input_fingerprint = content_id(
            {
                "identity_schema": "invalid-pipeline-input/v1",
                "stage": stage,
                "failure_type": failure_type,
                "safe_input": safe_input,
            },
            length=64,
        )
        run_id = content_id(
            {
                "identity_schema": "invalid-pipeline-run/v1",
                "final_input_fingerprint": final_input_fingerprint,
                "stage": stage,
                "failure_type": failure_type,
            }
        )
        decision = self._no_trade_decision_at(
            self._safe_decision_time(snapshot),
            (f"run failed closed at {stage}: {failure_type}",),
        )
        decision = replace(
            decision,
            signal_id=f"no-trade-{run_id}",
            reference_readiness="INVALID",
            execution_eligible=False,
        )
        return PipelineResult(
            run_id=run_id,
            pipeline_version=self.version,
            snapshot_id=safe_input["snapshot_id"],
            snapshot_fingerprint=safe_input["snapshot_fingerprint"],
            universe_snapshot_id=safe_input["declared_universe_snapshot_id"],
            calendar_snapshot_id="INVALID",
            calendar_version=safe_input["declared_calendar_version"],
            reference_readiness="INVALID",
            declared_universe_snapshot_id=safe_input[
                "declared_universe_snapshot_id"
            ],
            declared_calendar_version=safe_input["declared_calendar_version"],
            context_universe_snapshot_id=safe_input[
                "context_universe_snapshot_id"
            ],
            context_calendar_snapshot_id=safe_input[
                "context_calendar_snapshot_id"
            ],
            context_calendar_version=safe_input["context_calendar_version"],
            context_reference_readiness=self._safe_readiness(calendar),
            context_universe_readiness=self._safe_readiness(universe),
            validated_input_fingerprint="",
            final_input_fingerprint=final_input_fingerprint,
            trial_id=safe_input["trial_id"],
            model_bundle_id=safe_input["model_bundle_id"],
            data_content_hash=safe_input["data_content_hash"],
            source_revision=safe_input["source_revision"],
            execution_policy_version=safe_input["execution_policy_version"],
            cost_schedule_version=safe_input["cost_schedule_version"],
            decision=decision,
            ranked=(),
            research=(),
            rejections=(Rejection("*", stage, (failure_type,)),),
            status=RunStatus.FAILED,
            failure_stage=stage,
            failure_type=failure_type,
        )

    @staticmethod
    def _defensive_candidate_copy(candidate: Candidate) -> Candidate:
        if type(candidate) is not Candidate:
            raise TypeError("candidate must be an exact Candidate")
        return Candidate(
            instrument=replace(candidate.instrument),
            forecast=replace(candidate.forecast),
            signals=replace(candidate.signals),
            setup=replace(candidate.setup),
            evidence_ids=tuple(candidate.evidence_ids),
        )

    @staticmethod
    def _no_trade_decision(
        snapshot: DataSnapshot,
        reasons: tuple[str, ...],
    ) -> TradeDecision:
        return Pipeline._no_trade_decision_at(snapshot.decision_time, reasons)

    @staticmethod
    def _no_trade_decision_at(
        decision_time: datetime,
        reasons: tuple[str, ...],
    ) -> TradeDecision:
        return TradeDecision(
            action=DecisionAction.NO_TRADE,
            signal_id="pending-no-trade-id",
            decision_time=decision_time,
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
        identity_context: dict[str, object],
        reference_context: ReferenceContext,
        ranked: tuple[RankedCandidate, ...],
        assessments: tuple[ResearchAssessment, ...],
        rejections: tuple[Rejection, ...],
        *,
        instruments: tuple[InstrumentSnapshot, ...],
        validated_input_fingerprint: str | None = None,
        status: RunStatus,
        decision: TradeDecision | None = None,
        failure_stage: str = "",
        failure_type: str = "",
    ) -> PipelineResult:
        reference_is_valid = False
        final_input_fingerprint = content_id(
            {
                "identity_schema": "unavailable-final-input/v1",
                "validated_input_fingerprint": validated_input_fingerprint or "",
            },
            length=64,
        )
        if validated_input_fingerprint is not None:
            try:
                captured_portfolio = identity_context.get("portfolio")
                if type(captured_portfolio) is not PortfolioState:
                    raise TypeError("validated portfolio identity is unavailable")
                final_identity_context = self._input_identity_context(
                    snapshot=snapshot,
                    instruments=instruments,
                    portfolio=captured_portfolio,
                    reference_context=reference_context,
                    policy=replace(self.policy),
                    research_limit=self.research_limit,
                    forecast_provider=self.forecast_provider,
                    signal_provider=self.signal_provider,
                    research_provider=self.research_provider,
                    ranker=self.ranker,
                )
                final_input_fingerprint = content_id(
                    final_identity_context,
                    length=64,
                )
                validate_snapshot(snapshot)
                validate_reference_context(
                    snapshot,
                    list(instruments),
                    reference_context,
                )
                if (
                    final_input_fingerprint != validated_input_fingerprint
                ):
                    raise DataIntegrityError(
                        "validated pipeline inputs changed during the run"
                    )
                reference_is_valid = True
            except Exception as exc:
                if final_input_fingerprint == validated_input_fingerprint:
                    final_input_fingerprint = content_id(
                        {
                            "identity_schema": "invalid-final-input/v1",
                            "validated_input_fingerprint": validated_input_fingerprint,
                            "failure_type": type(exc).__name__,
                        },
                        length=64,
                    )
                rejections = rejections + (
                    Rejection("*", "final_integrity", (type(exc).__name__,)),
                )
                ranked = ()
                assessments = ()
                status = RunStatus.FAILED
                failure_stage = "final_integrity"
                failure_type = type(exc).__name__
                decision = None

        if reference_is_valid:
            reference_readiness = reference_context.calendar.readiness.value
            universe_snapshot_id = reference_context.universe.snapshot_id
            calendar_snapshot_id = reference_context.calendar.snapshot_id
            calendar_version = reference_context.calendar.version
        else:
            reference_readiness = "INVALID"
            universe_snapshot_id = self._safe_text(
                snapshot,
                "universe_snapshot_id",
            )
            calendar_snapshot_id = "INVALID"
            calendar_version = self._safe_text(snapshot, "calendar_version")

        if decision is None:
            decision = self._no_trade_decision_at(
                self._safe_decision_time(snapshot),
                (f"run failed closed at {failure_stage}: {failure_type}",),
            )
        decision = replace(
            decision,
            reference_readiness=reference_readiness,
            execution_eligible=False,
        )
        decision_material = {
            field.name: getattr(decision, field.name)
            for field in fields(decision)
            if field.name not in {"signal_id", "integrity_hash"}
        }
        run_id = content_id(
            {
                "identity_schema": "pipeline-run-v4",
                "validated_input_fingerprint": validated_input_fingerprint or "",
                "final_input_fingerprint": final_input_fingerprint,
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
            snapshot_id=self._safe_text(snapshot, "snapshot_id"),
            snapshot_fingerprint=self._safe_text(
                snapshot,
                "content_fingerprint",
            ),
            universe_snapshot_id=universe_snapshot_id,
            calendar_snapshot_id=calendar_snapshot_id,
            calendar_version=calendar_version,
            reference_readiness=reference_readiness,
            declared_universe_snapshot_id=self._safe_text(
                snapshot,
                "universe_snapshot_id",
            ),
            declared_calendar_version=self._safe_text(
                snapshot,
                "calendar_version",
            ),
            context_universe_snapshot_id=self._safe_text(
                reference_context.universe,
                "snapshot_id",
            ),
            context_calendar_snapshot_id=self._safe_text(
                reference_context.calendar,
                "snapshot_id",
            ),
            context_calendar_version=self._safe_text(
                reference_context.calendar,
                "version",
            ),
            context_reference_readiness=self._safe_readiness(
                reference_context.calendar
            ),
            context_universe_readiness=self._safe_readiness(
                reference_context.universe
            ),
            validated_input_fingerprint=validated_input_fingerprint or "",
            final_input_fingerprint=final_input_fingerprint,
            trial_id=self._safe_text(snapshot, "trial_id"),
            model_bundle_id=self._safe_text(snapshot, "model_bundle_id"),
            data_content_hash=self._safe_text(snapshot, "data_content_hash"),
            source_revision=self._safe_text(snapshot, "source_revision"),
            execution_policy_version=self._safe_text(
                snapshot,
                "execution_policy_version",
            ),
            cost_schedule_version=self._safe_text(
                snapshot,
                "cost_schedule_version",
            ),
            decision=decision,
            ranked=ranked,
            research=assessments,
            rejections=rejections,
            status=status,
            failure_stage=failure_stage,
            failure_type=failure_type,
        )
