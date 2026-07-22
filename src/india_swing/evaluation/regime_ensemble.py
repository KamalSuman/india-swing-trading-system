from __future__ import annotations

import re
from dataclasses import dataclass, field, fields
from datetime import date
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Deferred to function bodies at runtime: india_swing.forecasting.regime_ensemble
    # and india_swing.signals.proposal_batch both transitively import this evaluation
    # package (via history_adapter/input_assembly's EffectiveTickSize dependency), so
    # importing them at module scope here would deadlock the partially initialized
    # forecasting module during interpreter startup.
    from india_swing.forecasting.regime_ensemble import (
        RegimeCrossSection,
        RegimeEnsembleConfig,
    )
    from india_swing.signals.proposal_batch import SwingProposalBatch

from india_swing.execution.simulator import LimitEntryOrder
from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness

from .baselines import (
    GeneratedIntentBatch,
    GeneratedIntentRole,
    GeneratedSignalDecision,
    PointInTimeInstrument,
)
from .engine import (
    DailyExecutionPolicy,
    EvaluationDataReadiness,
    EvaluationDataset,
    EvaluationTradeIntent,
)
from .models import PurgedWalkForwardPlan, WalkForwardFold


ZERO = Decimal("0")
ONE = Decimal("1")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class RegimeEnsembleEvaluationError(ValueError):
    pass


def _verify_sanitized(verify: object, message: str) -> None:
    """Run a nested object's own verify_content_identity, sanitizing its error type.

    Nested contracts (WalkForwardFold, SwingProposalBatch, RegimeCrossSection,
    GeneratedSignalDecision, EvaluationTradeIntent, GeneratedIntentBatch,
    PointInTimeInstrument, DailyExecutionPolicy, EvaluationDataset,
    PurgedWalkForwardPlan) each raise their own module's error type. This
    module's own errors must remain a single static sanitized
    RegimeEnsembleEvaluationError with no nested exception text.
    """

    try:
        verify()
    except Exception:
        raise RegimeEnsembleEvaluationError(message) from None


class RegimeEnsembleDecisionReason(str, Enum):
    SELECTED = "SELECTED"
    BELOW_MINIMUM_ENSEMBLE_SCORE = "BELOW_MINIMUM_ENSEMBLE_SCORE"
    ABOVE_MAXIMUM_UNCERTAINTY = "ABOVE_MAXIMUM_UNCERTAINTY"
    NON_POSITIVE_SCORE_IMPLIED_RETURN = "NON_POSITIVE_SCORE_IMPLIED_RETURN"
    INSUFFICIENT_SLOT_NOTIONAL = "INSUFFICIENT_SLOT_NOTIONAL"
    OUTSIDE_POSITION_RANK = "OUTSIDE_POSITION_RANK"
    TIED_AT_POSITION_CUTOFF = "TIED_AT_POSITION_CUTOFF"


def _sha(value: str, name: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise RegimeEnsembleEvaluationError(f"{name} must be a full lowercase SHA-256")


def _positive_decimal(value: Decimal, name: str) -> None:
    if type(value) is not Decimal or not value.is_finite() or value <= ZERO:
        raise RegimeEnsembleEvaluationError(f"{name} must be a finite positive Decimal")


def _unit_decimal(value: Decimal, name: str) -> None:
    if type(value) is not Decimal or not value.is_finite():
        raise RegimeEnsembleEvaluationError(f"{name} must be a finite Decimal")
    if value < ZERO or value > ONE:
        raise RegimeEnsembleEvaluationError(f"{name} must be between zero and one")


def _positive_integer(value: int, name: str) -> None:
    if type(value) is not int or value <= 0:
        raise RegimeEnsembleEvaluationError(f"{name} must be a positive integer")


@dataclass(frozen=True, slots=True)
class RegimeEnsembleIntentConfig:
    """Preregisterable, conservative selection policy over the ensemble kernel.

    These version-1 defaults are research/paper thresholds only; they carry
    no claim of profitability or production validation.
    """

    ensemble_config: RegimeEnsembleConfig
    minimum_ensemble_score: Decimal = Decimal("0.60")
    maximum_uncertainty: Decimal = Decimal("0.65")
    maximum_positions: int = 4
    gross_exposure_fraction: Decimal = Decimal("0.80")
    version: str = "regime-ensemble-intent-generator/v1"
    config_id: str = field(init=False)

    def __post_init__(self) -> None:
        from india_swing.forecasting.regime_ensemble import RegimeEnsembleConfig

        if type(self.ensemble_config) is not RegimeEnsembleConfig:
            raise RegimeEnsembleEvaluationError("ensemble_config must be exact")
        _verify_sanitized(
            self.ensemble_config.verify_content_identity,
            "ensemble config content identity failed",
        )
        _unit_decimal(self.minimum_ensemble_score, "minimum_ensemble_score")
        _unit_decimal(self.maximum_uncertainty, "maximum_uncertainty")
        _positive_integer(self.maximum_positions, "maximum_positions")
        if (
            type(self.gross_exposure_fraction) is not Decimal
            or not self.gross_exposure_fraction.is_finite()
        ):
            raise RegimeEnsembleEvaluationError(
                "gross_exposure_fraction must be a finite Decimal"
            )
        if self.gross_exposure_fraction <= ZERO or self.gross_exposure_fraction > ONE:
            raise RegimeEnsembleEvaluationError("gross_exposure_fraction must be in (0, 1]")
        if self.version != "regime-ensemble-intent-generator/v1":
            raise RegimeEnsembleEvaluationError("unsupported intent generator version")
        object.__setattr__(self, "config_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "config_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        _verify_sanitized(
            self.ensemble_config.verify_content_identity,
            "ensemble config content identity failed",
        )
        if self.config_id != self._calculated_id():
            raise RegimeEnsembleEvaluationError("intent config content identity failed")


@dataclass(frozen=True, slots=True)
class RegimeEnsembleFoldResult:
    """One fold's complete deterministic strategy-decision coverage.

    Every proposal in ``proposal_batch`` receives exactly one decision;
    selected decisions receive exactly one matching intent. This object never
    claims execution authority.
    """

    fold: WalkForwardFold
    signal_session: date
    universe_snapshot_id: str
    proposal_batch: SwingProposalBatch
    cross_section: RegimeCrossSection
    decisions: tuple[GeneratedSignalDecision, ...]
    intents: tuple[EvaluationTradeIntent, ...]
    fold_result_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._verify()
        object.__setattr__(self, "fold_result_id", self._calculated_id())

    def _verify(self) -> None:
        if type(self.fold) is not WalkForwardFold:
            raise RegimeEnsembleEvaluationError("fold must be exact")
        _verify_sanitized(self.fold.verify_content_identity, "fold content identity failed")
        if type(self.signal_session) is not date:
            raise RegimeEnsembleEvaluationError("signal_session must be a date")
        if self.signal_session != self.fold.test_sessions[0]:
            raise RegimeEnsembleEvaluationError(
                "signal_session differs from the fold's first test session"
            )
        from india_swing.forecasting.regime_ensemble import RegimeCrossSection
        from india_swing.signals.proposal_batch import SwingProposalBatch

        _sha(self.universe_snapshot_id, "universe_snapshot_id")
        if type(self.proposal_batch) is not SwingProposalBatch:
            raise RegimeEnsembleEvaluationError("proposal_batch must be exact")
        _verify_sanitized(
            self.proposal_batch.verify_content_identity, "proposal batch content identity failed"
        )
        if self.universe_snapshot_id != self.proposal_batch.universe_batch.universe_snapshot_id:
            raise RegimeEnsembleEvaluationError(
                "universe_snapshot_id differs from the bound proposal batch"
            )
        if type(self.cross_section) is not RegimeCrossSection:
            raise RegimeEnsembleEvaluationError("cross_section must be exact")
        _verify_sanitized(
            self.cross_section.verify_content_identity, "cross-section content identity failed"
        )

        if (
            type(self.decisions) is not tuple
            or not self.decisions
            or any(type(value) is not GeneratedSignalDecision for value in self.decisions)
            or self.decisions
            != tuple(sorted(self.decisions, key=lambda value: (value.signal_session, value.symbol)))
        ):
            raise RegimeEnsembleEvaluationError("fold decisions must be an ordered exact tuple")
        for value in self.decisions:
            _verify_sanitized(
                value.verify_content_identity, "fold decision content identity failed"
            )
            if (
                value.fold_id != self.fold.fold_id
                or value.role is not GeneratedIntentRole.STRATEGY
                or value.signal_session != self.signal_session
                or value.score_name != "REGIME_ENSEMBLE_SCORE"
            ):
                raise RegimeEnsembleEvaluationError("fold decision binding differs from the fold")

        if (
            type(self.intents) is not tuple
            or any(type(value) is not EvaluationTradeIntent for value in self.intents)
            or self.intents
            != tuple(
                sorted(
                    self.intents,
                    key=lambda value: (value.entry_order.signal_session, value.intent_id),
                )
            )
        ):
            raise RegimeEnsembleEvaluationError("fold intents must be an ordered exact tuple")
        for value in self.intents:
            _verify_sanitized(value.verify_content_identity, "fold intent content identity failed")
            if (
                value.entry_order.signal_session != self.signal_session
                or value.universe_snapshot_id != self.universe_snapshot_id
            ):
                raise RegimeEnsembleEvaluationError("fold intent binding differs from the fold")

        proposal_ids = tuple(
            sorted(value.assembly.stable_instrument_id for value in self.proposal_batch.proposals)
        )
        decision_ids = tuple(sorted(value.instrument_id for value in self.decisions))
        if proposal_ids != decision_ids or len(set(decision_ids)) != len(decision_ids):
            raise RegimeEnsembleEvaluationError(
                "fold decisions do not exactly cover the proposal batch"
            )
        cross_section_ids = tuple(sorted(value.instrument_id for value in self.cross_section.scores))
        if cross_section_ids != proposal_ids:
            raise RegimeEnsembleEvaluationError(
                "cross-section coverage differs from the proposal batch"
            )
        selected_keys = {value.decision_id for value in self.decisions if value.selected}
        intent_keys = {value.signal_id for value in self.intents}
        if selected_keys != intent_keys:
            raise RegimeEnsembleEvaluationError(
                "selected decisions and intents disagree"
            )

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "regime-ensemble-fold-result/v1",
                "fold_id": self.fold.fold_id,
                "signal_session": self.signal_session,
                "universe_snapshot_id": self.universe_snapshot_id,
                "proposal_batch_id": self.proposal_batch.batch_id,
                "cross_section_id": self.cross_section.cross_section_id,
                "decisions": self.decisions,
                "intents": self.intents,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._verify()
        if self.fold_result_id != self._calculated_id():
            raise RegimeEnsembleEvaluationError("fold result content identity failed")

    @property
    def execution_eligible(self) -> bool:
        return False


def _binding_snapshot(instrument: PointInTimeInstrument, session: date) -> str:
    if instrument.eligibility_bindings:
        return dict(instrument.eligibility_bindings)[session]
    return instrument.universe_snapshot_id


def _match_instruments(
    *,
    signal_session: date,
    proposal_batch: SwingProposalBatch,
    instruments: tuple[PointInTimeInstrument, ...],
) -> dict[str, PointInTimeInstrument]:
    eligible = tuple(
        instrument for instrument in instruments if signal_session in instrument.eligible_sessions
    )
    proposals = proposal_batch.proposals
    if len(eligible) != len(proposals):
        raise RegimeEnsembleEvaluationError(
            "eligible instrument coverage differs from the proposal batch"
        )
    by_stable_id: dict[str, list[PointInTimeInstrument]] = {}
    for instrument in instruments:
        if instrument.stable_instrument_id is not None:
            by_stable_id.setdefault(instrument.stable_instrument_id, []).append(instrument)

    matched: dict[str, PointInTimeInstrument] = {}
    for proposal in proposals:
        stable_id = proposal.assembly.stable_instrument_id
        candidates = by_stable_id.get(stable_id, ())
        matches = tuple(
            instrument
            for instrument in candidates
            if instrument.symbol == proposal.symbol
            and instrument.isin == proposal.universe_entry.listing.isin
            and signal_session in instrument.eligible_sessions
        )
        if len(matches) != 1:
            raise RegimeEnsembleEvaluationError(
                "proposal does not match exactly one eligible point-in-time instrument"
            )
        instrument = matches[0]
        if _binding_snapshot(instrument, signal_session) != proposal.universe_snapshot_id:
            raise RegimeEnsembleEvaluationError(
                "instrument eligibility universe binding differs from the proposal batch"
            )
        if stable_id in matched:
            raise RegimeEnsembleEvaluationError(
                "eligible instrument coverage differs from the proposal batch"
            )
        matched[stable_id] = instrument

    if len(matched) != len(eligible):
        raise RegimeEnsembleEvaluationError(
            "eligible instrument coverage differs from the proposal batch"
        )
    return matched


def _fold_signal_decisions_and_intents(
    *,
    config: RegimeEnsembleIntentConfig,
    fold: WalkForwardFold,
    proposal_batch: SwingProposalBatch,
    cross_section: RegimeCrossSection,
    instruments: tuple[PointInTimeInstrument, ...],
    execution_policy: DailyExecutionPolicy,
    initial_capital: Decimal,
) -> tuple[tuple[GeneratedSignalDecision, ...], tuple[EvaluationTradeIntent, ...]]:
    signal_session = fold.test_sessions[0]
    entry_session = fold.test_sessions[1]
    matched = _match_instruments(
        signal_session=signal_session,
        proposal_batch=proposal_batch,
        instruments=instruments,
    )

    slot_notional = initial_capital * config.gross_exposure_fraction / config.maximum_positions

    candidate_rows: list[tuple[object, int, object]] = []
    reasons: dict[str, str] = {}
    for proposal in proposal_batch.proposals:
        stable_id = proposal.assembly.stable_instrument_id
        score = cross_section.score_for(stable_id)
        if score.ensemble_score < config.minimum_ensemble_score:
            reasons[stable_id] = RegimeEnsembleDecisionReason.BELOW_MINIMUM_ENSEMBLE_SCORE.value
            continue
        if score.uncertainty > config.maximum_uncertainty:
            reasons[stable_id] = RegimeEnsembleDecisionReason.ABOVE_MAXIMUM_UNCERTAINTY.value
            continue
        if score.median_return_pct <= ZERO:
            reasons[stable_id] = RegimeEnsembleDecisionReason.NON_POSITIVE_SCORE_IMPLIED_RETURN.value
            continue
        limit_price = proposal.levels.entry_high
        quantity = int(slot_notional / limit_price)
        if quantity <= 0:
            reasons[stable_id] = RegimeEnsembleDecisionReason.INSUFFICIENT_SLOT_NOTIONAL.value
            continue
        candidate_rows.append((score, quantity, proposal))

    ordered = sorted(
        candidate_rows,
        key=lambda row: (-row[0].ensemble_score, row[0].uncertainty, -row[0].median_return_pct),
    )
    groups: list[list[tuple[object, int, object]]] = []
    for row in ordered:
        key = (row[0].ensemble_score, row[0].uncertainty, row[0].median_return_pct)
        if groups and groups[-1][0] == key:
            groups[-1][1].append(row)
        else:
            groups.append([key, [row]])

    remaining = config.maximum_positions
    selected_quantity: dict[str, int] = {}
    stopped = False
    for key, group in groups:
        stable_ids = tuple(row[2].assembly.stable_instrument_id for row in group)
        if stopped or remaining <= 0:
            for stable_id in stable_ids:
                reasons[stable_id] = RegimeEnsembleDecisionReason.OUTSIDE_POSITION_RANK.value
            stopped = True
            continue
        if len(group) <= remaining:
            for row in group:
                stable_id = row[2].assembly.stable_instrument_id
                reasons[stable_id] = RegimeEnsembleDecisionReason.SELECTED.value
                selected_quantity[stable_id] = row[1]
            remaining -= len(group)
        else:
            for stable_id in stable_ids:
                reasons[stable_id] = RegimeEnsembleDecisionReason.TIED_AT_POSITION_CUTOFF.value
            remaining = 0
            stopped = True

    decisions: list[GeneratedSignalDecision] = []
    intents: list[EvaluationTradeIntent] = []
    for proposal in sorted(proposal_batch.proposals, key=lambda value: value.symbol):
        stable_id = proposal.assembly.stable_instrument_id
        score = cross_section.score_for(stable_id)
        reason = reasons[stable_id]
        selected = reason == RegimeEnsembleDecisionReason.SELECTED.value
        history = proposal.assembly.signal_materialization.history
        decision = GeneratedSignalDecision(
            generator_id=config.config_id,
            role=GeneratedIntentRole.STRATEGY,
            fold_id=fold.fold_id,
            signal_session=signal_session,
            instrument_id=stable_id,
            symbol=proposal.symbol,
            score_name="REGIME_ENSEMBLE_SCORE",
            score=score.ensemble_score,
            selected=selected,
            reason=reason,
            evidence_bar_ids=tuple(bar.bar_id for bar in history.bars),
        )
        decisions.append(decision)
        if selected:
            instrument = matched[stable_id]
            entry_order = LimitEntryOrder(
                symbol=proposal.symbol,
                signal_session=signal_session,
                first_eligible_session=entry_session,
                expiry_session=entry_session,
                quantity=selected_quantity[stable_id],
                limit_price=proposal.levels.entry_high,
                tick_size=history.tick_size,
                maximum_participation=execution_policy.maximum_participation,
            )
            intents.append(
                EvaluationTradeIntent(
                    signal_id=decision.decision_id,
                    universe_snapshot_id=_binding_snapshot(instrument, signal_session),
                    isin=instrument.isin,
                    entry_order=entry_order,
                    stop_price=proposal.levels.stop,
                    target_price=proposal.levels.target,
                    max_holding_sessions=proposal.config.maximum_holding_sessions,
                )
            )

    ordered_decisions = tuple(
        sorted(decisions, key=lambda value: (value.signal_session, value.symbol))
    )
    ordered_intents = tuple(
        sorted(intents, key=lambda value: (value.entry_order.signal_session, value.intent_id))
    )
    return ordered_decisions, ordered_intents


def _validate_run_inputs(
    *,
    config: RegimeEnsembleIntentConfig,
    split_plan: PurgedWalkForwardPlan,
    dataset: EvaluationDataset,
    instruments: tuple[PointInTimeInstrument, ...],
    proposal_batches: tuple[SwingProposalBatch, ...],
    execution_policy: DailyExecutionPolicy,
    initial_capital: Decimal,
) -> None:
    from india_swing.signals.proposal_batch import SwingProposalBatch

    if type(config) is not RegimeEnsembleIntentConfig:
        raise RegimeEnsembleEvaluationError("config must be exact")
    config.verify_content_identity()
    if type(split_plan) is not PurgedWalkForwardPlan:
        raise RegimeEnsembleEvaluationError("split_plan must be exact")
    _verify_sanitized(split_plan.verify_content_identity, "split plan content identity failed")
    if type(dataset) is not EvaluationDataset:
        raise RegimeEnsembleEvaluationError("dataset must be exact")
    _verify_sanitized(dataset.verify_content_identity, "dataset content identity failed")
    if dataset.sessions != split_plan.ordered_sessions:
        raise RegimeEnsembleEvaluationError("dataset calendar differs from split plan")
    if dataset.readiness is EvaluationDataReadiness.COLLECTION_ONLY:
        raise RegimeEnsembleEvaluationError("collection-only dataset cannot enter the generator")
    if type(execution_policy) is not DailyExecutionPolicy:
        raise RegimeEnsembleEvaluationError("execution_policy must be exact")
    _verify_sanitized(
        execution_policy.verify_content_identity, "execution policy content identity failed"
    )
    _positive_decimal(initial_capital, "initial_capital")

    if (
        type(instruments) is not tuple
        or not instruments
        or instruments != tuple(sorted(instruments, key=lambda value: value.symbol))
    ):
        raise RegimeEnsembleEvaluationError(
            "instruments must be a non-empty symbol-ordered tuple"
        )
    symbols: set[str] = set()
    isins: set[str] = set()
    for instrument in instruments:
        if type(instrument) is not PointInTimeInstrument:
            raise RegimeEnsembleEvaluationError("instruments must contain exact values")
        _verify_sanitized(
            instrument.verify_content_identity, "instrument content identity failed"
        )
        if instrument.symbol in symbols or instrument.isin in isins:
            raise RegimeEnsembleEvaluationError("instrument symbols and ISINs must be unique")
        symbols.add(instrument.symbol)
        isins.add(instrument.isin)
        if instrument.universe_snapshot_id not in dataset.universe_snapshot_ids:
            raise RegimeEnsembleEvaluationError("instrument universe is absent from dataset")
        if any(
            snapshot_id not in dataset.universe_snapshot_ids
            for _, snapshot_id in instrument.eligibility_bindings
        ):
            raise RegimeEnsembleEvaluationError(
                "instrument eligibility binding is absent from dataset"
            )
        if any(session not in dataset.sessions for session in instrument.eligible_sessions):
            raise RegimeEnsembleEvaluationError(
                "instrument eligibility exceeds dataset calendar"
            )

    if (
        type(proposal_batches) is not tuple
        or not proposal_batches
        or any(type(value) is not SwingProposalBatch for value in proposal_batches)
    ):
        raise RegimeEnsembleEvaluationError("proposal_batches must be a non-empty exact tuple")
    if len(proposal_batches) != len(split_plan.folds):
        raise RegimeEnsembleEvaluationError("exactly one proposal batch is required per fold")
    if proposal_batches != tuple(
        sorted(proposal_batches, key=lambda value: value.universe_batch.signal_session)
    ):
        raise RegimeEnsembleEvaluationError("proposal batches must be signal-session ordered")

    expected_signal_sessions = tuple(fold.test_sessions[0] for fold in split_plan.folds)
    actual_signal_sessions = tuple(
        value.universe_batch.signal_session for value in proposal_batches
    )
    if actual_signal_sessions != expected_signal_sessions:
        raise RegimeEnsembleEvaluationError(
            "proposal batch signal sessions differ from the fold plan"
        )

    first_config = proposal_batches[0].config
    bars_by_key = {(bar.session, bar.symbol): bar for bar in dataset.bars}
    for fold, batch in zip(split_plan.folds, proposal_batches):
        _verify_sanitized(batch.verify_content_identity, "proposal batch content identity failed")
        if batch.readiness is ReferenceReadiness.SYNTHETIC_TEST:
            if dataset.readiness is not EvaluationDataReadiness.SYNTHETIC:
                raise RegimeEnsembleEvaluationError(
                    "synthetic proposal batch requires a synthetic dataset"
                )
        elif batch.readiness is ReferenceReadiness.POINT_IN_TIME_VERIFIED:
            if dataset.readiness is not EvaluationDataReadiness.POINT_IN_TIME_VERIFIED:
                raise RegimeEnsembleEvaluationError(
                    "point-in-time-verified proposal batch requires point-in-time-verified data"
                )
        else:
            raise RegimeEnsembleEvaluationError(
                "collection-only proposal batch cannot enter the generator"
            )
        if batch.config.config_id != first_config.config_id:
            raise RegimeEnsembleEvaluationError(
                "fold proposal batches must share the exact deterministic signal config"
            )
        if batch.config.maximum_holding_sessions > split_plan.label_horizon_sessions:
            raise RegimeEnsembleEvaluationError(
                "proposal holding horizon exceeds the registered label horizon"
            )
        if batch.universe_batch.universe_snapshot_id not in dataset.universe_snapshot_ids:
            raise RegimeEnsembleEvaluationError(
                "proposal batch universe is absent from the dataset"
            )
        for proposal in batch.proposals:
            if proposal.entry_window.entry_day != fold.test_sessions[1]:
                raise RegimeEnsembleEvaluationError(
                    "proposal entry window differs from the fold's next session"
                )
            history = proposal.assembly.signal_materialization.history
            terminal = history.bars[-1]
            if terminal.market_session != fold.test_sessions[0]:
                raise RegimeEnsembleEvaluationError(
                    "proposal history does not end at the fold signal session"
                )
            bar = bars_by_key.get((fold.test_sessions[0], proposal.symbol))
            if bar is None:
                raise RegimeEnsembleEvaluationError(
                    "dataset is missing the fold signal bar"
                )
            if (
                bar.open != terminal.open
                or bar.high != terminal.high
                or bar.low != terminal.low
                or bar.close != terminal.close
                or Decimal(bar.volume) != terminal.volume
            ):
                raise RegimeEnsembleEvaluationError(
                    "proposal terminal bar differs from the dataset"
                )
            if not bar.tradable or bar.volume <= 0:
                raise RegimeEnsembleEvaluationError(
                    "signal bar is not tradable with positive volume"
                )


@dataclass(frozen=True, slots=True)
class RegimeEnsembleIntentRun:
    """One complete, replay-verifiable regime-ensemble evaluation run.

    The exact ``PurgedWalkForwardPlan`` and ``EvaluationDataset`` are embedded
    (not merely referenced by caller-supplied ID/readiness strings) so that
    readiness, calendar, and source lineage can be fully replayed and cannot
    be laundered by constructing a fresh, internally self-consistent outer
    object around unchanged fold results. A run is research-only: every fold
    result and the generated batch remain ``execution_eligible=False``, and
    no probability or profit claim is made.
    """

    config: RegimeEnsembleIntentConfig
    split_plan: PurgedWalkForwardPlan
    dataset: EvaluationDataset
    execution_policy: DailyExecutionPolicy
    execution_policy_id: str
    initial_capital: Decimal
    instruments: tuple[PointInTimeInstrument, ...]
    fold_results: tuple[RegimeEnsembleFoldResult, ...]
    generated_batch: GeneratedIntentBatch
    run_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._verify()
        object.__setattr__(self, "run_id", self._calculated_id())

    @property
    def split_plan_id(self) -> str:
        return self.split_plan.plan_id

    @property
    def dataset_id(self) -> str:
        return self.dataset.dataset_id

    @property
    def dataset_readiness(self) -> EvaluationDataReadiness:
        return self.dataset.readiness

    def _verify(self) -> None:
        if type(self.execution_policy) is not DailyExecutionPolicy:
            raise RegimeEnsembleEvaluationError("execution_policy must be exact")
        _sha(self.execution_policy_id, "execution_policy_id")
        if self.execution_policy_id != self.execution_policy.policy_id:
            raise RegimeEnsembleEvaluationError(
                "execution_policy_id differs from the bound execution policy"
            )

        if (
            type(self.fold_results) is not tuple
            or not self.fold_results
            or any(type(value) is not RegimeEnsembleFoldResult for value in self.fold_results)
            or self.fold_results
            != tuple(sorted(self.fold_results, key=lambda value: value.signal_session))
        ):
            raise RegimeEnsembleEvaluationError("fold results must be an ordered exact tuple")
        if len({value.fold.fold_id for value in self.fold_results}) != len(self.fold_results):
            raise RegimeEnsembleEvaluationError("fold results cannot repeat a fold")
        for value in self.fold_results:
            value.verify_content_identity()

        # Reuse the exact generator-time validation rather than duplicating it:
        # this rechecks dataset/plan calendar equality, readiness mapping,
        # signal/entry sessions, one batch per fold, common deterministic
        # config, label horizon, universe presence, eligibility bindings,
        # tradable terminal OHLCV equality, and every nested content identity
        # -- both at direct-construction time and on every later replay.
        proposal_batches = tuple(result.proposal_batch for result in self.fold_results)
        _validate_run_inputs(
            config=self.config,
            split_plan=self.split_plan,
            dataset=self.dataset,
            instruments=self.instruments,
            proposal_batches=proposal_batches,
            execution_policy=self.execution_policy,
            initial_capital=self.initial_capital,
        )

        if tuple(result.fold for result in self.fold_results) != self.split_plan.folds:
            raise RegimeEnsembleEvaluationError(
                "fold results do not match the embedded split plan one-for-one and in order"
            )

        if type(self.generated_batch) is not GeneratedIntentBatch:
            raise RegimeEnsembleEvaluationError("generated_batch must be exact")
        _verify_sanitized(
            self.generated_batch.verify_content_identity,
            "generated batch content identity failed",
        )
        if self.generated_batch.role is not GeneratedIntentRole.STRATEGY:
            raise RegimeEnsembleEvaluationError("generated batch must use the strategy role")
        if self.generated_batch.generator_id != self.config.config_id:
            raise RegimeEnsembleEvaluationError(
                "generated batch generator differs from the config"
            )
        if self.generated_batch.split_plan_id != self.split_plan.plan_id:
            raise RegimeEnsembleEvaluationError(
                "generated batch split plan differs from the embedded split plan"
            )
        if self.generated_batch.source_snapshot_ids != self.dataset.source_snapshot_ids:
            raise RegimeEnsembleEvaluationError(
                "generated batch source snapshots differ from the embedded dataset"
            )
        expected_decisions = tuple(
            sorted(
                (decision for result in self.fold_results for decision in result.decisions),
                key=lambda value: (value.signal_session, value.symbol),
            )
        )
        expected_intents = tuple(
            sorted(
                (intent for result in self.fold_results for intent in result.intents),
                key=lambda value: (value.entry_order.signal_session, value.intent_id),
            )
        )
        if (
            self.generated_batch.decisions != expected_decisions
            or self.generated_batch.intents != expected_intents
        ):
            raise RegimeEnsembleEvaluationError(
                "generated batch does not replay from the fold results"
            )

        from india_swing.forecasting.regime_ensemble import calculate_regime_cross_section

        for fold_result in self.fold_results:
            histories = tuple(
                sorted(
                    (
                        proposal.assembly.signal_materialization.history
                        for proposal in fold_result.proposal_batch.proposals
                    ),
                    key=lambda value: value.instrument_id,
                )
            )
            expected_cross_section = calculate_regime_cross_section(
                histories, self.config.ensemble_config
            )
            if (
                expected_cross_section.cross_section_id
                != fold_result.cross_section.cross_section_id
            ):
                raise RegimeEnsembleEvaluationError(
                    "fold cross-section does not replay from its proposal histories"
                )
            replayed_decisions, replayed_intents = _fold_signal_decisions_and_intents(
                config=self.config,
                fold=fold_result.fold,
                proposal_batch=fold_result.proposal_batch,
                cross_section=fold_result.cross_section,
                instruments=self.instruments,
                execution_policy=self.execution_policy,
                initial_capital=self.initial_capital,
            )
            if (
                tuple(value.decision_id for value in replayed_decisions)
                != tuple(value.decision_id for value in fold_result.decisions)
                or tuple(value.intent_id for value in replayed_intents)
                != tuple(value.intent_id for value in fold_result.intents)
            ):
                raise RegimeEnsembleEvaluationError(
                    "fold result does not replay from its embedded inputs"
                )

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "regime-ensemble-intent-run/v1",
                "config_id": self.config.config_id,
                "split_plan_id": self.split_plan.plan_id,
                "dataset_id": self.dataset.dataset_id,
                "dataset_readiness": self.dataset.readiness,
                "execution_policy_id": self.execution_policy_id,
                "initial_capital": self.initial_capital,
                "instruments": self.instruments,
                "fold_result_ids": tuple(value.fold_result_id for value in self.fold_results),
                "generated_batch_id": self.generated_batch.batch_id,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._verify()
        if self.run_id != self._calculated_id():
            raise RegimeEnsembleEvaluationError("intent run content identity failed")

    @property
    def execution_eligible(self) -> bool:
        return False


class RegimeEnsembleIntentGenerator:
    """Bind one fold-safe SwingProposalBatch per fold to strategy decisions/intents.

    This generator never copies or reimplements regime, feature, ranking,
    return, downside, or uncertainty math -- it calls the public
    ``calculate_regime_cross_section`` kernel once per fold and applies only
    static preregistered thresholds and capacity ranking on top of the
    kernel's output.
    """

    def generate(
        self,
        *,
        config: RegimeEnsembleIntentConfig,
        split_plan: PurgedWalkForwardPlan,
        dataset: EvaluationDataset,
        instruments: tuple[PointInTimeInstrument, ...],
        proposal_batches: tuple[SwingProposalBatch, ...],
        execution_policy: DailyExecutionPolicy,
        initial_capital: Decimal,
    ) -> RegimeEnsembleIntentRun:
        from india_swing.forecasting.regime_ensemble import calculate_regime_cross_section

        _validate_run_inputs(
            config=config,
            split_plan=split_plan,
            dataset=dataset,
            instruments=instruments,
            proposal_batches=proposal_batches,
            execution_policy=execution_policy,
            initial_capital=initial_capital,
        )

        fold_results: list[RegimeEnsembleFoldResult] = []
        for fold, batch in zip(split_plan.folds, proposal_batches):
            histories = tuple(
                sorted(
                    (
                        proposal.assembly.signal_materialization.history
                        for proposal in batch.proposals
                    ),
                    key=lambda value: value.instrument_id,
                )
            )
            cross_section = calculate_regime_cross_section(histories, config.ensemble_config)
            decisions, intents = _fold_signal_decisions_and_intents(
                config=config,
                fold=fold,
                proposal_batch=batch,
                cross_section=cross_section,
                instruments=instruments,
                execution_policy=execution_policy,
                initial_capital=initial_capital,
            )
            fold_results.append(
                RegimeEnsembleFoldResult(
                    fold=fold,
                    signal_session=fold.test_sessions[0],
                    universe_snapshot_id=batch.universe_batch.universe_snapshot_id,
                    proposal_batch=batch,
                    cross_section=cross_section,
                    decisions=decisions,
                    intents=intents,
                )
            )

        ordered_fold_results = tuple(fold_results)
        generated_batch = GeneratedIntentBatch(
            generator_id=config.config_id,
            role=GeneratedIntentRole.STRATEGY,
            split_plan_id=split_plan.plan_id,
            source_snapshot_ids=dataset.source_snapshot_ids,
            decisions=tuple(
                sorted(
                    (
                        decision
                        for result in ordered_fold_results
                        for decision in result.decisions
                    ),
                    key=lambda value: (value.signal_session, value.symbol),
                )
            ),
            intents=tuple(
                sorted(
                    (intent for result in ordered_fold_results for intent in result.intents),
                    key=lambda value: (value.entry_order.signal_session, value.intent_id),
                )
            ),
        )
        return RegimeEnsembleIntentRun(
            config=config,
            split_plan=split_plan,
            dataset=dataset,
            execution_policy=execution_policy,
            execution_policy_id=execution_policy.policy_id,
            initial_capital=initial_capital,
            instruments=instruments,
            fold_results=ordered_fold_results,
            generated_batch=generated_batch,
        )
