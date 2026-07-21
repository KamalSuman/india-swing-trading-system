from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timezone
from enum import Enum

from india_swing.identity import content_id
from india_swing.market_data.models import FullQuoteBatch, MAXIMUM_QUOTE_KEYS
from india_swing.paper_trades.models import PaperTradeRegistration
from india_swing.recommendations.models import (
    RESEARCH_WARNING,
    SwingDecisionAction,
    SwingDecisionPackage,
)
from india_swing.risk.swing_portfolio import (
    SwingPortfolioSizingPolicy,
    SwingPortfolioSnapshot,
)
from india_swing.signals.deterministic_swing import calculate_next_entry_window
from india_swing.signals.opportunity_ranking import SwingOpportunityRankingPolicy
from india_swing.signals.proposal_batch import SwingProposalBatch
from india_swing.signals.quote_gate import SwingQuoteGatePolicy


SPEC_SCHEMA_VERSION = "swing-operational-run-spec/v1"
RESULT_SCHEMA_VERSION = "swing-operational-run-result/v1"
RECORD_SCHEMA_VERSION = "swing-operational-run-record/v1"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class SwingOperationalError(ValueError):
    pass


class SwingOperationalStatus(str, Enum):
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"


class SwingOperationalFailureCode(str, Enum):
    START_BEFORE_WINDOW = "START_BEFORE_WINDOW"
    START_AFTER_DEADLINE = "START_AFTER_DEADLINE"
    QUOTE_ACQUISITION_FAILED = "QUOTE_ACQUISITION_FAILED"
    QUOTE_COVERAGE_INVALID = "QUOTE_COVERAGE_INVALID"
    PORTFOLIO_ACQUISITION_FAILED = "PORTFOLIO_ACQUISITION_FAILED"
    CLOCK_NON_MONOTONIC = "CLOCK_NON_MONOTONIC"
    EVALUATION_AFTER_DEADLINE = "EVALUATION_AFTER_DEADLINE"
    DECISION_ASSEMBLY_FAILED = "DECISION_ASSEMBLY_FAILED"


def _aware_utc(value: datetime, name: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is None:
        raise SwingOperationalError(f"{name} must be timezone-aware")
    try:
        offset = value.utcoffset()
    except Exception:
        raise SwingOperationalError(f"{name} has invalid timezone behavior") from None
    if offset is None:
        raise SwingOperationalError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _sha(value: str, name: str) -> None:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise SwingOperationalError(f"{name} must be a lowercase SHA-256")


@dataclass(frozen=True, slots=True)
class SwingOperationalRunSpec:
    proposal_batch: SwingProposalBatch
    target_session: date
    decision_not_before: datetime
    decision_deadline: datetime
    quote_policy: SwingQuoteGatePolicy
    ranking_policy: SwingOpportunityRankingPolicy
    sizing_policy: SwingPortfolioSizingPolicy
    quote_chunk_size: int = MAXIMUM_QUOTE_KEYS
    mode: str = "PAPER_ONLY"
    schema_version: str = SPEC_SCHEMA_VERSION
    spec_id: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "decision_not_before",
            _aware_utc(self.decision_not_before, "decision_not_before"),
        )
        object.__setattr__(
            self,
            "decision_deadline",
            _aware_utc(self.decision_deadline, "decision_deadline"),
        )
        self._verify()
        object.__setattr__(self, "spec_id", self._calculated_id())

    def _verify(self) -> None:
        if type(self.proposal_batch) is not SwingProposalBatch:
            raise SwingOperationalError("proposal_batch must be exact")
        self.proposal_batch.verify_content_identity()
        if type(self.target_session) is not date:
            raise SwingOperationalError("target_session must be an exact date")
        if self.decision_not_before >= self.decision_deadline:
            raise SwingOperationalError("operational decision window is invalid")
        if type(self.quote_policy) is not SwingQuoteGatePolicy:
            raise SwingOperationalError("quote_policy must be exact")
        if type(self.ranking_policy) is not SwingOpportunityRankingPolicy:
            raise SwingOperationalError("ranking_policy must be exact")
        if type(self.sizing_policy) is not SwingPortfolioSizingPolicy:
            raise SwingOperationalError("sizing_policy must be exact")
        self.quote_policy.verify_content_identity()
        self.ranking_policy.verify_content_identity()
        self.sizing_policy.verify_content_identity()
        if self.sizing_policy.maximum_new_positions_per_run != 1:
            raise SwingOperationalError(
                "operational decisions require exactly one maximum new position"
            )
        if (
            type(self.quote_chunk_size) is not int
            or self.quote_chunk_size <= 0
            or self.quote_chunk_size > MAXIMUM_QUOTE_KEYS
        ):
            raise SwingOperationalError(
                "quote_chunk_size must fit one Kite full-quote request"
            )
        expected_window = calculate_next_entry_window(
            self.proposal_batch.calendar,
            self.proposal_batch.universe_batch.signal_session,
            self.proposal_batch.config,
        )
        if self.target_session != expected_window.entry_day:
            raise SwingOperationalError("target_session differs from the proposal window")
        if (
            self.decision_not_before
            != _aware_utc(expected_window.earliest_entry_at, "entry window start")
            or self.decision_deadline
            != _aware_utc(expected_window.entry_expires_at, "entry window end")
        ):
            raise SwingOperationalError("operational window differs from the proposal window")
        for proposal in self.proposal_batch.proposals:
            if proposal.entry_window.window_id != expected_window.window_id:
                raise SwingOperationalError("proposal entry windows are inconsistent")
        if self.mode != "PAPER_ONLY" or self.schema_version != SPEC_SCHEMA_VERSION:
            raise SwingOperationalError("operational authority boundary is invalid")

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "spec_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._verify()
        if self.spec_id != self._calculated_id():
            raise SwingOperationalError("operational run spec content identity failed")


def build_swing_operational_run_spec(
    *,
    proposal_batch: SwingProposalBatch,
    quote_policy: SwingQuoteGatePolicy | None = None,
    ranking_policy: SwingOpportunityRankingPolicy | None = None,
    sizing_policy: SwingPortfolioSizingPolicy | None = None,
    quote_chunk_size: int = MAXIMUM_QUOTE_KEYS,
) -> SwingOperationalRunSpec:
    if type(proposal_batch) is not SwingProposalBatch:
        raise SwingOperationalError("proposal_batch must be exact")
    proposal_batch.verify_content_identity()
    window = calculate_next_entry_window(
        proposal_batch.calendar,
        proposal_batch.universe_batch.signal_session,
        proposal_batch.config,
    )
    return SwingOperationalRunSpec(
        proposal_batch=proposal_batch,
        target_session=window.entry_day,
        decision_not_before=window.earliest_entry_at,
        decision_deadline=window.entry_expires_at,
        quote_policy=quote_policy or SwingQuoteGatePolicy(),
        ranking_policy=ranking_policy or SwingOpportunityRankingPolicy(),
        sizing_policy=sizing_policy or SwingPortfolioSizingPolicy(),
        quote_chunk_size=quote_chunk_size,
    )


def paper_registration_from_decision(
    *,
    package: SwingDecisionPackage,
    source_run_id: str,
) -> PaperTradeRegistration | None:
    if type(package) is not SwingDecisionPackage:
        raise SwingOperationalError("decision package must be exact")
    package.verify_content_identity()
    if type(source_run_id) is not str or not source_run_id:
        raise SwingOperationalError("source_run_id is required")
    decision = package.decision
    if decision.action is SwingDecisionAction.NO_TRADE:
        return None
    recommendation = decision.recommendation
    if recommendation is None:
        raise SwingOperationalError("BUY decision lost its recommendation")
    outcome = recommendation.sizing_outcome
    proposal = outcome.opportunity.quote_gate_outcome.proposal
    levels = recommendation.levels
    return PaperTradeRegistration(
        alert_id=package.notification.notification_id,
        source_run_id=source_run_id,
        source_pipeline_integrity_hash=package.package_id,
        source_decision_integrity_hash=decision.decision_id,
        signal_id=outcome.opportunity.opportunity_id,
        symbol=recommendation.symbol,
        quantity=recommendation.quantity,
        decision_time=decision.evaluated_at,
        earliest_entry_at=proposal.entry_window.earliest_entry_at,
        entry_expires_at=proposal.entry_window.entry_expires_at,
        entry_low=levels.entry_low,
        entry_high=levels.entry_high,
        stop=levels.stop,
        target=levels.target,
        max_holding_sessions=proposal.config.maximum_holding_sessions,
        estimated_round_trip_cost=outcome.estimated_round_trip_cost,
    )


@dataclass(frozen=True, slots=True)
class SwingOperationalRunResult:
    spec: SwingOperationalRunSpec
    quote_source_id: str
    portfolio_source_id: str
    started_at: datetime
    completed_at: datetime
    status: SwingOperationalStatus
    action: SwingDecisionAction
    failure_codes: tuple[SwingOperationalFailureCode, ...]
    evaluated_at: datetime | None = None
    quote_batch: FullQuoteBatch | None = None
    portfolio: SwingPortfolioSnapshot | None = None
    decision_package: SwingDecisionPackage | None = None
    paper_registration: PaperTradeRegistration | None = None
    schema_version: str = RESULT_SCHEMA_VERSION
    run_id: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "started_at", _aware_utc(self.started_at, "started_at"))
        object.__setattr__(self, "completed_at", _aware_utc(self.completed_at, "completed_at"))
        if self.evaluated_at is not None:
            object.__setattr__(
                self,
                "evaluated_at",
                _aware_utc(self.evaluated_at, "evaluated_at"),
            )
        self._verify()
        object.__setattr__(self, "run_id", self._calculated_id())

    def _verify(self) -> None:
        if type(self.spec) is not SwingOperationalRunSpec:
            raise SwingOperationalError("operational spec must be exact")
        self.spec.verify_content_identity()
        _sha(self.quote_source_id, "quote_source_id")
        _sha(self.portfolio_source_id, "portfolio_source_id")
        if self.completed_at < self.started_at:
            raise SwingOperationalError("operational run time moved backwards")
        if type(self.status) is not SwingOperationalStatus:
            raise SwingOperationalError("operational status must be exact")
        if type(self.action) is not SwingDecisionAction:
            raise SwingOperationalError("operational action must be exact")
        if type(self.failure_codes) is not tuple or any(
            type(value) is not SwingOperationalFailureCode for value in self.failure_codes
        ):
            raise SwingOperationalError("failure codes must be an exact enum tuple")
        if self.failure_codes != tuple(sorted(set(self.failure_codes), key=lambda value: value.value)):
            raise SwingOperationalError("failure codes must be sorted and unique")
        if self.schema_version != RESULT_SCHEMA_VERSION:
            raise SwingOperationalError("unsupported operational result schema")

        if self.quote_batch is not None:
            if type(self.quote_batch) is not FullQuoteBatch:
                raise SwingOperationalError("quote_batch must be exact")
            self.quote_batch.verify_content_identity()
        if self.portfolio is not None:
            if type(self.portfolio) is not SwingPortfolioSnapshot:
                raise SwingOperationalError("portfolio must be exact")
            self.portfolio.verify_content_identity()

        if self.status is SwingOperationalStatus.COMPLETE:
            if self.failure_codes:
                raise SwingOperationalError("complete run cannot carry failure codes")
            if self.evaluated_at is None or self.quote_batch is None or self.portfolio is None:
                raise SwingOperationalError("complete run lacks acquired inputs")
            if (
                self.started_at < self.spec.decision_not_before
                or self.started_at > self.spec.decision_deadline
                or self.evaluated_at < self.started_at
                or self.evaluated_at > self.spec.decision_deadline
                or self.completed_at > self.spec.decision_deadline
            ):
                raise SwingOperationalError("complete run lies outside its decision window")
            if type(self.decision_package) is not SwingDecisionPackage:
                raise SwingOperationalError("complete run lacks a decision package")
            self.decision_package.verify_content_identity()
            decision = self.decision_package.decision
            if self.action is not decision.action or self.evaluated_at != decision.evaluated_at:
                raise SwingOperationalError("operational decision summary differs")
            sizing = decision.sizing_batch
            bound_gate = sizing.ranking_batch.quote_gate_batch
            if bound_gate.quote_batch.batch_id != self.quote_batch.batch_id:
                raise SwingOperationalError("decision differs from acquired quotes")
            if sizing.portfolio.portfolio_snapshot_id != self.portfolio.portfolio_snapshot_id:
                raise SwingOperationalError("decision differs from acquired portfolio")
            expected_registration = paper_registration_from_decision(
                package=self.decision_package,
                source_run_id=self.spec.spec_id,
            )
            if expected_registration is None:
                if self.paper_registration is not None:
                    raise SwingOperationalError("NO_TRADE cannot register a paper trade")
            else:
                if type(self.paper_registration) is not PaperTradeRegistration:
                    raise SwingOperationalError("BUY run lacks a paper registration")
                self.paper_registration.verify_content_identity()
                if self.paper_registration.registration_id != expected_registration.registration_id:
                    raise SwingOperationalError("paper registration does not replay")
        else:
            if not self.failure_codes or self.action is not SwingDecisionAction.NO_TRADE:
                raise SwingOperationalError("failed run must be an explained NO_TRADE")
            if self.decision_package is not None or self.paper_registration is not None:
                raise SwingOperationalError("failed run cannot publish a trade decision")

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "run_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._verify()
        if self.run_id != self._calculated_id():
            raise SwingOperationalError("operational result content identity failed")

    @property
    def notification_message(self) -> str:
        if self.decision_package is not None:
            return self.decision_package.notification.message
        lines = (
            RESEARCH_WARNING,
            "Decision: NO_TRADE",
            f"Operational run: {self.status.value}",
            f"Target session: {self.spec.target_session.isoformat()}",
            "Failure codes:",
            *[f"- {value.value}" for value in self.failure_codes],
            "No order can be placed from this operational result.",
        )
        return "\n".join(lines) + "\n"

    @property
    def execution_eligible(self) -> bool:
        return False


@dataclass(frozen=True, slots=True)
class SwingOperationalRunRecord:
    spec_id: str
    run_id: str
    target_session: date
    status: SwingOperationalStatus
    action: SwingDecisionAction
    started_at: datetime
    completed_at: datetime
    evaluated_at: datetime | None
    quote_source_id: str
    portfolio_source_id: str
    proposal_batch_id: str
    quote_batch_id: str | None
    portfolio_snapshot_id: str | None
    decision_id: str | None
    package_id: str | None
    notification_id: str | None
    paper_registration_id: str | None
    failure_codes: tuple[SwingOperationalFailureCode, ...]
    message: str
    message_sha256: str
    mode: str = "PAPER_ONLY"
    schema_version: str = RECORD_SCHEMA_VERSION
    record_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.spec_id, "spec_id"),
            (self.run_id, "run_id"),
            (self.quote_source_id, "quote_source_id"),
            (self.portfolio_source_id, "portfolio_source_id"),
            (self.proposal_batch_id, "proposal_batch_id"),
        ):
            _sha(value, name)
        for value, name in (
            (self.quote_batch_id, "quote_batch_id"),
            (self.portfolio_snapshot_id, "portfolio_snapshot_id"),
            (self.decision_id, "decision_id"),
            (self.package_id, "package_id"),
            (self.notification_id, "notification_id"),
            (self.paper_registration_id, "paper_registration_id"),
        ):
            if value is not None:
                _sha(value, name)
        if type(self.target_session) is not date:
            raise SwingOperationalError("record target_session must be exact")
        object.__setattr__(self, "started_at", _aware_utc(self.started_at, "started_at"))
        object.__setattr__(self, "completed_at", _aware_utc(self.completed_at, "completed_at"))
        if self.evaluated_at is not None:
            object.__setattr__(self, "evaluated_at", _aware_utc(self.evaluated_at, "evaluated_at"))
        if type(self.status) is not SwingOperationalStatus or type(self.action) is not SwingDecisionAction:
            raise SwingOperationalError("record status and action must be exact")
        if self.completed_at < self.started_at:
            raise SwingOperationalError("record time moved backwards")
        if type(self.failure_codes) is not tuple or any(
            type(value) is not SwingOperationalFailureCode for value in self.failure_codes
        ):
            raise SwingOperationalError("record failure codes must be exact")
        if self.failure_codes != tuple(sorted(set(self.failure_codes), key=lambda value: value.value)):
            raise SwingOperationalError("record failure codes must be sorted and unique")
        if type(self.message) is not str or not self.message.startswith(RESEARCH_WARNING + "\n"):
            raise SwingOperationalError("record warning is missing")
        _sha(self.message_sha256, "message_sha256")
        if hashlib.sha256(self.message.encode("utf-8")).hexdigest() != self.message_sha256:
            raise SwingOperationalError("record message hash differs")
        if self.mode != "PAPER_ONLY" or self.schema_version != RECORD_SCHEMA_VERSION:
            raise SwingOperationalError("record authority boundary is invalid")
        if self.status is SwingOperationalStatus.FAILED:
            if self.action is not SwingDecisionAction.NO_TRADE or not self.failure_codes:
                raise SwingOperationalError("failed record must be an explained NO_TRADE")
            if any(
                value is not None
                for value in (
                    self.decision_id,
                    self.package_id,
                    self.notification_id,
                    self.paper_registration_id,
                )
            ):
                raise SwingOperationalError("failed record cannot imply a trade decision")
        else:
            if self.failure_codes or self.evaluated_at is None:
                raise SwingOperationalError("complete record cannot carry failures")
            if any(
                value is None
                for value in (
                    self.quote_batch_id,
                    self.portfolio_snapshot_id,
                    self.decision_id,
                    self.package_id,
                    self.notification_id,
                )
            ):
                raise SwingOperationalError("complete record lacks decision lineage")
            if self.action is SwingDecisionAction.BUY:
                if self.paper_registration_id is None:
                    raise SwingOperationalError("BUY record lacks paper registration")
            elif self.paper_registration_id is not None:
                raise SwingOperationalError("NO_TRADE record cannot register a position")
        object.__setattr__(self, "record_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "record_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        try:
            fresh = SwingOperationalRunRecord(
                **{
                    item.name: getattr(self, item.name)
                    for item in fields(self)
                    if item.name != "record_id"
                }
            )
        except Exception:
            raise SwingOperationalError(
                "operational record content identity failed"
            ) from None
        if self.record_id != fresh.record_id:
            raise SwingOperationalError("operational record content identity failed")


def operational_record_from_result(
    result: SwingOperationalRunResult,
) -> SwingOperationalRunRecord:
    if type(result) is not SwingOperationalRunResult:
        raise SwingOperationalError("operational result must be exact")
    result.verify_content_identity()
    package = result.decision_package
    registration = result.paper_registration
    message = result.notification_message
    return SwingOperationalRunRecord(
        spec_id=result.spec.spec_id,
        run_id=result.run_id,
        target_session=result.spec.target_session,
        status=result.status,
        action=result.action,
        started_at=result.started_at,
        completed_at=result.completed_at,
        evaluated_at=result.evaluated_at,
        quote_source_id=result.quote_source_id,
        portfolio_source_id=result.portfolio_source_id,
        proposal_batch_id=result.spec.proposal_batch.batch_id,
        quote_batch_id=None if result.quote_batch is None else result.quote_batch.batch_id,
        portfolio_snapshot_id=(
            None if result.portfolio is None else result.portfolio.portfolio_snapshot_id
        ),
        decision_id=None if package is None else package.decision.decision_id,
        package_id=None if package is None else package.package_id,
        notification_id=None if package is None else package.notification.notification_id,
        paper_registration_id=(
            None if registration is None else registration.registration_id
        ),
        failure_codes=result.failure_codes,
        message=message,
        message_sha256=hashlib.sha256(message.encode("utf-8")).hexdigest(),
    )
