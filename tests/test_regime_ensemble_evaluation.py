from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

from india_swing.corporate_actions.adjustments import (
    AdjustedPriceBar,
    CorporateActionAdjustedHistory,
    StableRawBarBinding,
)
from india_swing.domain.models import Board, EvidenceItem, Surveillance
from india_swing.evaluation import (
    DailyExecutionPolicy,
    EvaluationDataReadiness,
    EvaluationDataset,
    PointInTimeInstrument,
    PurgedWalkForwardPlan,
    RegimeEnsembleDecisionReason,
    RegimeEnsembleEvaluationError,
    RegimeEnsembleFoldResult,
    RegimeEnsembleIntentConfig,
    RegimeEnsembleIntentGenerator,
    RegimeEnsembleIntentRun,
    build_expanding_purged_walk_forward_plan,
)
from india_swing.execution.simulator import SimulationBar
from india_swing.forecasting.regime_ensemble import (
    RegimeEnsembleConfig,
    calculate_regime_cross_section,
)
from india_swing.promotion import PromotionCapability, PromotionEvidence, evaluate_promotion
from india_swing.reference.calendar import (
    CalendarDay,
    CalendarDayKind,
    CalendarSnapshot,
    SessionWindow,
    SessionWindowPhase,
)
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
from india_swing.signals.deterministic_swing import (
    AsOfSwingBar,
    DeterministicSwingSignalConfig,
    InstrumentSwingHistory,
)
from india_swing.signals.history_adapter import SwingHistoryMaterialization
from india_swing.signals.input_assembly import SwingInputAssembly
from india_swing.signals.proposal_batch import assemble_swing_proposal_batch
from india_swing.signals.universe_batch import assemble_universe_input_batch

from tests.test_swing_proposal_batch import _hex_id, _non_actionable_entry, _reference, _vetoed_entries


IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc
START = date(2026, 1, 1)
NUM_SESSIONS = 65
SESSIONS = tuple(START + timedelta(days=index) for index in range(NUM_SESSIONS))
CALENDAR_SOURCE_ID = "e1" * 32
CALENDAR_CUTOFF = datetime.combine(START, time(0, 0), tzinfo=IST)
HISTORY_SESSIONS = 15

IDENTITY_SNAPSHOT_ID = "f9" * 32
CORPORATE_ACTION_SNAPSHOT_ID = "f8" * 32


def D(value: str) -> Decimal:
    return Decimal(value)


def _calendar() -> CalendarSnapshot:
    days = []
    for offset in range(NUM_SESSIONS):
        day = START + timedelta(days=offset)
        days.append(
            CalendarDay(
                day=day,
                kind=CalendarDayKind.REGULAR,
                reference=ExternalRecordRef(
                    event_time=datetime.combine(day, time(0), tzinfo=IST),
                    knowledge_time=datetime(2025, 12, 1, 12, tzinfo=IST),
                    source="TEST",
                    content_hash=f"{offset + 1:064x}",
                    source_snapshot_id=CALENDAR_SOURCE_ID,
                ),
                session_windows=(
                    SessionWindow(
                        opens_at=datetime.combine(day, time(9, 15), tzinfo=IST),
                        closes_at=datetime.combine(day, time(15, 30), tzinfo=IST),
                        phase=SessionWindowPhase.LIVE_CONTINUOUS,
                    ),
                ),
            )
        )
    return CalendarSnapshot.create(
        exchange="NSE",
        segment="CM",
        cutoff=CALENDAR_CUTOFF,
        coverage_start=days[0].day,
        coverage_end=days[-1].day,
        days=tuple(days),
        source_snapshot_ids=(CALENDAR_SOURCE_ID,),
        readiness=ReferenceReadiness.SYNTHETIC_TEST,
    )


def _signal_config() -> DeterministicSwingSignalConfig:
    return DeterministicSwingSignalConfig(
        minimum_history_sessions=HISTORY_SESSIONS,
        momentum_lookback_sessions=5,
        trend_lookback_sessions=10,
        atr_lookback_sessions=5,
        volume_lookback_sessions=5,
        breakout_lookback_sessions=5,
        maximum_holding_sessions=3,
    )


def _ensemble_config() -> RegimeEnsembleConfig:
    return RegimeEnsembleConfig(
        minimum_history_sessions=HISTORY_SESSIONS,
        short_momentum_sessions=5,
        long_momentum_sessions=10,
        trend_sessions=12,
        volatility_sessions=5,
        volume_sessions=5,
        breakout_sessions=5,
        contraction_sessions=3,
        horizon_sessions=5,
    )


def _intent_config(**overrides: object) -> RegimeEnsembleIntentConfig:
    values: dict[str, object] = dict(
        ensemble_config=_ensemble_config(),
        minimum_ensemble_score=D("0.60"),
        maximum_uncertainty=D("0.65"),
        maximum_positions=4,
        gross_exposure_fraction=D("0.80"),
    )
    values.update(overrides)
    return RegimeEnsembleIntentConfig(**values)


def _execution_policy() -> DailyExecutionPolicy:
    return DailyExecutionPolicy(slippage_bps=D("10"), maximum_participation=D("0.0025"))


def _split_plan() -> PurgedWalkForwardPlan:
    return build_expanding_purged_walk_forward_plan(
        calendar_version="synthetic-regime-ensemble-evaluation-v1",
        ordered_sessions=SESSIONS,
        initial_training_sessions=15,
        validation_sessions=5,
        test_sessions=5,
        step_sessions=5,
        label_horizon_sessions=10,
        embargo_sessions=10,
    )


def _closes(*, direction: Decimal, base_close: Decimal) -> tuple[Decimal, ...]:
    return tuple(base_close + direction * D(str(index)) for index in range(NUM_SESSIONS))


def _promotion(
    *,
    cutoff: datetime,
    coverage_start: date,
    coverage_end: date,
    raw_ids: tuple[str, ...],
    universe_ids: tuple[str, ...],
    tick_id: str,
) -> object:
    expected = (
        (PromotionCapability.RAW_PRICES, tuple(sorted(raw_ids))),
        (PromotionCapability.UNIVERSE, tuple(sorted(universe_ids))),
        (PromotionCapability.STABLE_IDENTITY, (IDENTITY_SNAPSHOT_ID,)),
        (PromotionCapability.CORPORATE_ACTIONS, (CORPORATE_ACTION_SNAPSHOT_ID,)),
        (PromotionCapability.TICK_SIZES, (tick_id,)),
    )
    evidence = tuple(
        sorted(
            (
                PromotionEvidence(
                    capability=capability,
                    cutoff=cutoff,
                    coverage_start=coverage_start,
                    coverage_end=coverage_end,
                    source_snapshot_ids=source_ids,
                    readiness=ReferenceReadiness.SYNTHETIC_TEST,
                    complete=True,
                    actionable=True,
                    reason_codes=(),
                )
                for capability, source_ids in expected
            ),
            key=lambda value: value.capability.value,
        )
    )
    return evaluate_promotion(
        market_session=coverage_end,
        history_start=coverage_start,
        decision_cutoff=cutoff,
        evidence=evidence,
    )


def _build_assembly(
    *,
    prefix: int,
    instrument_id: str,
    listing_id: str,
    symbol: str,
    signal_session: date,
    cutoff: datetime,
    tick_size: Decimal,
    closes: tuple[Decimal, ...],
    raw_ids: tuple[str, ...],
    universe_ids: tuple[str, ...],
) -> SwingInputAssembly:
    end_index = SESSIONS.index(signal_session)
    start_index = end_index - HISTORY_SESSIONS + 1
    assert start_index >= 0
    window_sessions = SESSIONS[start_index : end_index + 1]
    window_closes = closes[start_index : end_index + 1]

    tick_evidence_id = _hex_id(0xD0 + prefix, end_index)

    bindings = []
    adjusted_bars = []
    history_bars = []
    for index, session in enumerate(window_sessions):
        knowledge_time = datetime.combine(session, time(16, 0), tzinfo=IST)
        close = window_closes[index]
        volume = Decimal(1000 + (start_index + index) * 10)
        binding = StableRawBarBinding(
            market_session=session,
            raw_bar_id=raw_ids[index],
            stable_instrument_id=instrument_id,
            stable_listing_id=listing_id,
            identity_snapshot_id=IDENTITY_SNAPSHOT_ID,
            knowledge_time=knowledge_time,
        )
        bindings.append(binding)
        adjusted_bar = AdjustedPriceBar(
            market_session=session,
            symbol=symbol,
            series="EQ",
            validated_isin="INE" + f"{prefix:06d}" + "021",
            open=close - Decimal("0.20"),
            high=close + Decimal("1.00"),
            low=close - Decimal("1.00"),
            close=close,
            volume=volume,
            traded_value=close * volume,
            price_factor=Decimal("1"),
            volume_factor=Decimal("1"),
            applied_event_ids=(),
            raw_bar_id=raw_ids[index],
            identity_binding_id=binding.binding_id,
            identity_snapshot_id=IDENTITY_SNAPSHOT_ID,
            raw_knowledge_time=knowledge_time,
            knowledge_time=knowledge_time,
            adjustment_snapshot_id=CORPORATE_ACTION_SNAPSHOT_ID,
        )
        adjusted_bars.append(adjusted_bar)
        history_bars.append(
            AsOfSwingBar(
                market_session=session,
                open=adjusted_bar.open,
                high=adjusted_bar.high,
                low=adjusted_bar.low,
                close=adjusted_bar.close,
                volume=adjusted_bar.volume,
                traded_value=adjusted_bar.traded_value,
                available_at=knowledge_time,
                evidence_id=f"{prefix}-{end_index}-bar-{index:03d}",
                content_hash=f"{prefix}-{end_index}-hash-bar-{index:03d}",
            )
        )

    adjusted_history = CorporateActionAdjustedHistory(
        stable_instrument_id=instrument_id,
        stable_listing_id=listing_id,
        signal_session=signal_session,
        cutoff=cutoff,
        adjustment_knowledge_time=cutoff - timedelta(minutes=5),
        corporate_action_snapshot_id=CORPORATE_ACTION_SNAPSHOT_ID,
        bars=tuple(adjusted_bars),
    )
    tick_content_hash = f"{prefix}-{end_index}-tick-hash"
    adjustment_evidence_id = f"{prefix}-{end_index}-adjustment-evidence"
    adjustment_content_hash = f"{prefix}-{end_index}-adjustment-hash"
    history = InstrumentSwingHistory(
        instrument_id=instrument_id,
        listing_id=listing_id,
        tick_size=tick_size,
        tick_available_at=cutoff - timedelta(minutes=10),
        tick_evidence_id=tick_evidence_id,
        tick_content_hash=tick_content_hash,
        adjustment_available_at=cutoff - timedelta(minutes=5),
        adjustment_evidence_id=adjustment_evidence_id,
        adjustment_content_hash=adjustment_content_hash,
        price_basis="CORPORATE_ACTION_ADJUSTED_AS_OF_CUTOFF",
        bars=tuple(history_bars),
    )
    materialization = SwingHistoryMaterialization(
        adjusted_history_id=adjusted_history.history_id,
        history=history,
        evidence=tuple(
            EvidenceItem(
                evidence_id=bar.evidence_id,
                source="TEST_BAR",
                published_at=bar.available_at - timedelta(minutes=1),
                available_at=bar.available_at,
                content_hash=bar.content_hash,
            )
            for bar in history.bars
        )
        + (
            EvidenceItem(
                evidence_id=tick_evidence_id,
                source="TEST_TICK",
                published_at=history.tick_available_at - timedelta(minutes=1),
                available_at=history.tick_available_at,
                content_hash=tick_content_hash,
            ),
            EvidenceItem(
                evidence_id=adjustment_evidence_id,
                source="TEST_ADJUSTMENT",
                published_at=history.adjustment_available_at - timedelta(minutes=1),
                available_at=history.adjustment_available_at,
                content_hash=adjustment_content_hash,
            ),
        ),
    )

    promotion = _promotion(
        cutoff=cutoff,
        coverage_start=window_sessions[0],
        coverage_end=signal_session,
        raw_ids=raw_ids,
        universe_ids=universe_ids,
        tick_id=tick_evidence_id,
    )

    return SwingInputAssembly(
        promotion_decision_id=promotion.decision_id,
        promotion=promotion,
        stable_identity_snapshot_id=IDENTITY_SNAPSHOT_ID,
        stable_instrument_id=instrument_id,
        stable_listing_id=listing_id,
        signal_session=signal_session,
        cutoff=cutoff,
        raw_artifact_ids=raw_ids,
        universe_snapshot_ids=universe_ids,
        identity_bindings=tuple(bindings),
        adjusted_history=adjusted_history,
        signal_materialization=materialization,
        readiness=ReferenceReadiness.SYNTHETIC_TEST,
        actionable=False,
    )


def _actionable_entry(
    *,
    instrument_id: str,
    listing_id: str,
    tradingsymbol: str,
    isin: str,
    master_id: str,
    eligibility_id: str,
    liquidity_id: str,
    source_row_id: str,
    digit: str,
    signal_session: date,
) -> UniverseEntry:
    return UniverseEntry(
        source_record_id=source_row_id,
        listing=ListingMapping(
            instrument_id=instrument_id,
            listing_id=listing_id,
            exchange="NSE",
            segment="CM",
            tradingsymbol=tradingsymbol,
            series="EQ",
            isin=isin,
            valid_from=date(2020, 1, 1),
            valid_to_exclusive=None,
            reference=_reference(master_id, digit),
        ),
        board=Board.MAIN,
        listing_state=ListingState.ACTIVE,
        suspended=False,
        surveillance=Surveillance.NONE,
        disposition=UniverseDisposition.ACTIONABLE,
        reason_codes=(),
        eligibility_refs=(
            EligibilityStateRef(
                effective=EffectiveExternalRecordRef(
                    reference=_reference(eligibility_id, digit),
                    effective_from_session=date(2020, 1, 1),
                    effective_to_exclusive=None,
                    schema_version="verified-eligibility/v1",
                ),
                instrument_id=instrument_id,
                listing_id=listing_id,
                board=Board.MAIN,
                listing_state=ListingState.ACTIVE,
                suspended=False,
                surveillance=Surveillance.NONE,
            ),
        ),
        liquidity_snapshot_id=liquidity_id,
        liquidity_cutoff_session=signal_session,
    )


class _InstrumentProfile:
    def __init__(
        self,
        *,
        prefix: int,
        symbol: str,
        direction: Decimal,
        base_close: Decimal,
        tick_size: Decimal = D("0.05"),
    ) -> None:
        self.prefix = prefix
        self.symbol = symbol
        self.direction = direction
        self.base_close = base_close
        self.tick_size = tick_size
        self.instrument_id = _hex_id(0xC0 + prefix, 1)
        self.listing_id = _hex_id(0xC1 + prefix, 1)
        self.master_id = _hex_id(0xC2 + prefix, 1)
        self.eligibility_id = _hex_id(0xC3 + prefix, 1)
        self.liquidity_id = _hex_id(0xC4 + prefix, 1)
        self.source_row_id = _hex_id(0xC5 + prefix, 1)
        self.isin = "INE" + f"{prefix:06d}" + "021"
        self.closes = _closes(direction=direction, base_close=base_close)


def _fold_universe(
    *, profiles: tuple[_InstrumentProfile, ...], signal_session: date, calendar: CalendarSnapshot
) -> UniverseSnapshot:
    entries = [
        _actionable_entry(
            instrument_id=profile.instrument_id,
            listing_id=profile.listing_id,
            tradingsymbol=profile.symbol,
            isin=profile.isin,
            master_id=profile.master_id,
            eligibility_id=profile.eligibility_id,
            liquidity_id=profile.liquidity_id,
            source_row_id=profile.source_row_id,
            digit=str(profile.prefix % 10),
            signal_session=signal_session,
        )
        for profile in profiles
    ]
    vetoes = list(_vetoed_entries())
    entries = sorted(entries + vetoes, key=lambda value: value.source_record_id)
    return UniverseSnapshot.create(
        exchange="NSE",
        segment="CM",
        market_session=signal_session,
        cutoff=datetime.combine(signal_session, time(16, 0), tzinfo=IST),
        calendar_snapshot_id=calendar.snapshot_id,
        universe_rules_version="verified-main-board/v1",
        selection_key="ALL_SCOPED_ROWS",
        scoped_source_row_ids=tuple(value.source_record_id for value in entries),
        security_master_snapshot_ids=tuple(
            sorted({value.listing.reference.source_snapshot_id for value in entries})
        ),
        eligibility_snapshot_ids=tuple(
            sorted(
                {
                    state.effective.reference.source_snapshot_id
                    for value in entries
                    for state in value.eligibility_refs
                }
            )
        ),
        liquidity_snapshot_ids=tuple(
            sorted(
                {
                    value.liquidity_snapshot_id
                    for value in entries
                    if value.liquidity_snapshot_id is not None
                }
            )
        ),
        readiness=ReferenceReadiness.SYNTHETIC_TEST,
        entries=tuple(entries),
    )


def _fold_proposal_batch(
    *, profiles: tuple[_InstrumentProfile, ...], fold, calendar: CalendarSnapshot
):
    signal_session = fold.test_sessions[0]
    universe = _fold_universe(profiles=profiles, signal_session=signal_session, calendar=calendar)
    cutoff = datetime.combine(signal_session, time(16, 0), tzinfo=IST)
    end_index = SESSIONS.index(signal_session)
    raw_ids = tuple(_hex_id(0xA0, end_index * 100 + index) for index in range(HISTORY_SESSIONS))
    universe_ids = raw_ids[:-1] + (universe.snapshot_id,)
    assemblies = tuple(
        _build_assembly(
            prefix=profile.prefix,
            instrument_id=profile.instrument_id,
            listing_id=profile.listing_id,
            symbol=profile.symbol,
            signal_session=signal_session,
            cutoff=cutoff,
            tick_size=profile.tick_size,
            closes=profile.closes,
            raw_ids=raw_ids,
            universe_ids=universe_ids,
        )
        for profile in sorted(profiles, key=lambda value: (value.instrument_id, value.listing_id))
    )
    universe_batch = assemble_universe_input_batch(
        current_universe=universe, assemblies=assemblies
    )
    return assemble_swing_proposal_batch(
        universe_batch=universe_batch, calendar=calendar, config=_signal_config()
    )


def _instruments(
    *,
    profiles: tuple[_InstrumentProfile, ...],
    signal_sessions: tuple[date, ...],
    universe_snapshot_ids: tuple[str, ...],
) -> tuple[PointInTimeInstrument, ...]:
    bindings = tuple(sorted(zip(signal_sessions, universe_snapshot_ids)))
    values = tuple(
        PointInTimeInstrument(
            symbol=profile.symbol,
            isin=profile.isin,
            universe_snapshot_id=bindings[0][1],
            eligible_sessions=tuple(session for session, _ in bindings),
            tick_size=profile.tick_size,
            stable_instrument_id=profile.instrument_id,
            eligibility_bindings=bindings,
        )
        for profile in profiles
    )
    return tuple(sorted(values, key=lambda value: value.symbol))


def _dataset_bars(
    *, proposal_batches: tuple, universe_snapshot_ids: tuple[str, ...]
) -> EvaluationDataset:
    bars = []
    for batch in proposal_batches:
        for proposal in batch.proposals:
            terminal = proposal.assembly.signal_materialization.history.bars[-1]
            bars.append(
                SimulationBar(
                    session=terminal.market_session,
                    symbol=proposal.symbol,
                    open=terminal.open,
                    high=terminal.high,
                    low=terminal.low,
                    close=terminal.close,
                    volume=int(terminal.volume),
                )
            )
    return EvaluationDataset(
        sessions=SESSIONS,
        bars=tuple(sorted(bars, key=lambda value: (value.session, value.symbol))),
        source_snapshot_ids=(CALENDAR_SOURCE_ID,),
        universe_snapshot_ids=tuple(sorted(universe_snapshot_ids)),
        readiness=EvaluationDataReadiness.SYNTHETIC,
    )


def _single_fold_plan(plan: PurgedWalkForwardPlan, fold) -> PurgedWalkForwardPlan:
    return PurgedWalkForwardPlan(
        calendar_version=plan.calendar_version,
        ordered_sessions=plan.ordered_sessions,
        label_horizon_sessions=plan.label_horizon_sessions,
        embargo_sessions=plan.embargo_sessions,
        folds=(fold,),
    )


def _build_run(
    *,
    profiles: tuple[_InstrumentProfile, ...],
    intent_config: RegimeEnsembleIntentConfig | None = None,
    initial_capital: Decimal = D("100000"),
    num_folds: int | None = None,
) -> tuple[RegimeEnsembleIntentRun, tuple, PurgedWalkForwardPlan]:
    calendar = _calendar()
    full_plan = _split_plan()
    folds = full_plan.folds if num_folds is None else full_plan.folds[:num_folds]
    plan = full_plan if num_folds is None else _single_fold_plan(full_plan, folds[0])
    if num_folds is not None and num_folds > 1:
        raise NotImplementedError("only single-fold slicing is supported by this helper")

    proposal_batches = tuple(
        _fold_proposal_batch(profiles=profiles, fold=fold, calendar=calendar) for fold in folds
    )
    universe_snapshot_ids = tuple(
        batch.universe_batch.universe_snapshot_id for batch in proposal_batches
    )
    signal_sessions = tuple(fold.test_sessions[0] for fold in folds)
    instruments = _instruments(
        profiles=profiles,
        signal_sessions=signal_sessions,
        universe_snapshot_ids=universe_snapshot_ids,
    )
    dataset = _dataset_bars(
        proposal_batches=proposal_batches, universe_snapshot_ids=universe_snapshot_ids
    )
    config = intent_config or _intent_config()
    execution_policy = _execution_policy()
    run = RegimeEnsembleIntentGenerator().generate(
        config=config,
        split_plan=plan,
        dataset=dataset,
        instruments=instruments,
        proposal_batches=proposal_batches,
        execution_policy=execution_policy,
        initial_capital=initial_capital,
    )
    return run, proposal_batches, plan


STRONG = _InstrumentProfile(prefix=1, symbol="STRONGCO", direction=D("5"), base_close=D("100"))
WEAK = _InstrumentProfile(prefix=2, symbol="WEAKCO", direction=D("-0.7"), base_close=D("300"))
TIE_A = _InstrumentProfile(prefix=3, symbol="TIEACO", direction=D("4"), base_close=D("150"))
TIE_B = _InstrumentProfile(prefix=4, symbol="TIEBCO", direction=D("4"), base_close=D("150"))


class RegimeEnsembleIntentConfigTests(unittest.TestCase):
    def test_config_is_deterministic_and_validates_exact_types(self) -> None:
        first = _intent_config()
        second = _intent_config()
        self.assertEqual(first.config_id, second.config_id)

        with self.assertRaisesRegex(RegimeEnsembleEvaluationError, "finite Decimal"):
            _intent_config(minimum_ensemble_score=True)
        with self.assertRaisesRegex(RegimeEnsembleEvaluationError, "positive integer"):
            _intent_config(maximum_positions=True)
        with self.assertRaisesRegex(RegimeEnsembleEvaluationError, "positive integer"):
            _intent_config(maximum_positions=0)
        with self.assertRaisesRegex(RegimeEnsembleEvaluationError, "between zero and one"):
            _intent_config(minimum_ensemble_score=D("1.5"))
        with self.assertRaisesRegex(RegimeEnsembleEvaluationError, "in \\(0, 1\\]"):
            _intent_config(gross_exposure_fraction=D("0"))
        with self.assertRaisesRegex(RegimeEnsembleEvaluationError, "finite Decimal"):
            _intent_config(minimum_ensemble_score=Decimal("nan"))

    def test_config_mutation_is_detected(self) -> None:
        config = _intent_config()
        config.verify_content_identity()
        untouched = config.config_id
        object.__setattr__(config, "maximum_positions", 99)
        with self.assertRaises(RegimeEnsembleEvaluationError):
            config.verify_content_identity()
        self.assertEqual(config.config_id, untouched)


class RegimeEnsembleIntentGeneratorHappyPathTests(unittest.TestCase):
    def test_multi_fold_run_covers_every_proposal_with_deterministic_ids(self) -> None:
        profiles = (STRONG, WEAK)
        run, proposal_batches, plan = _build_run(profiles=profiles)

        self.assertEqual(len(run.fold_results), len(plan.folds))
        self.assertFalse(run.execution_eligible)
        self.assertEqual(run.generated_batch.role.value, "STRATEGY")

        for fold_result, batch in zip(run.fold_results, proposal_batches):
            self.assertEqual(len(fold_result.decisions), len(batch.proposals))
            self.assertEqual(fold_result.proposal_batch.vetoes, batch.universe_batch.vetoes)
            self.assertTrue(fold_result.proposal_batch.vetoes)
            self.assertFalse(fold_result.execution_eligible)

        # Deterministic replay: rebuilding an identical run yields the same IDs.
        other_run, _, _ = _build_run(profiles=profiles)
        self.assertEqual(run.run_id, other_run.run_id)
        self.assertEqual(
            tuple(value.fold_result_id for value in run.fold_results),
            tuple(value.fold_result_id for value in other_run.fold_results),
        )
        run.verify_content_identity()

    def test_selected_intent_uses_exact_proposal_levels_and_fold_entry_session(self) -> None:
        profiles = (STRONG, WEAK)
        run, proposal_batches, _ = _build_run(profiles=profiles, num_folds=1)
        fold_result = run.fold_results[0]
        batch = proposal_batches[0]
        strong_proposal = next(
            value for value in batch.proposals if value.symbol == STRONG.symbol
        )
        selected_decision = next(
            value for value in fold_result.decisions if value.symbol == STRONG.symbol
        )
        self.assertTrue(selected_decision.selected)
        self.assertEqual(selected_decision.reason, RegimeEnsembleDecisionReason.SELECTED.value)
        intent = next(
            value for value in fold_result.intents if value.signal_id == selected_decision.decision_id
        )
        self.assertEqual(intent.entry_order.limit_price, strong_proposal.levels.entry_high)
        self.assertEqual(intent.stop_price, strong_proposal.levels.stop)
        self.assertEqual(intent.target_price, strong_proposal.levels.target)
        self.assertEqual(
            intent.entry_order.tick_size,
            strong_proposal.assembly.signal_materialization.history.tick_size,
        )
        self.assertEqual(intent.max_holding_sessions, strong_proposal.config.maximum_holding_sessions)
        self.assertEqual(intent.entry_order.first_eligible_session, fold_result.fold.test_sessions[1])
        self.assertEqual(intent.entry_order.expiry_session, fold_result.fold.test_sessions[1])
        self.assertEqual(
            intent.entry_order.maximum_participation, _execution_policy().maximum_participation
        )

    def test_declining_instrument_is_vetoed_with_non_positive_score_implied_return(self) -> None:
        # With score/uncertainty thresholds fully permissive (0 / 1), neither
        # earlier threshold can mask the return-sign veto -- this proves
        # NON_POSITIVE_SCORE_IMPLIED_RETURN independently rather than
        # accepting any of the three possible veto reasons.
        config = _intent_config(minimum_ensemble_score=D("0"), maximum_uncertainty=D("1"))
        run, _, _ = _build_run(profiles=(STRONG, WEAK), intent_config=config, num_folds=1)
        fold_result = run.fold_results[0]
        weak_decision = next(
            value for value in fold_result.decisions if value.symbol == WEAK.symbol
        )
        self.assertFalse(weak_decision.selected)
        self.assertEqual(
            weak_decision.reason,
            RegimeEnsembleDecisionReason.NON_POSITIVE_SCORE_IMPLIED_RETURN.value,
        )


def _observed_scores(
    *, profiles: tuple[_InstrumentProfile, ...]
) -> dict[str, object]:
    calendar = _calendar()
    plan = _split_plan()
    fold = plan.folds[0]
    batch = _fold_proposal_batch(profiles=profiles, fold=fold, calendar=calendar)
    histories = tuple(
        sorted(
            (proposal.assembly.signal_materialization.history for proposal in batch.proposals),
            key=lambda value: value.instrument_id,
        )
    )
    cross_section = calculate_regime_cross_section(histories, _ensemble_config())
    return {
        profile.symbol: cross_section.score_for(profile.instrument_id) for profile in profiles
    }


_PERMISSIVE = dict(minimum_ensemble_score=D("0"), maximum_uncertainty=D("1"))


class RegimeEnsembleIntentGeneratorThresholdTests(unittest.TestCase):
    def test_insufficient_slot_notional_does_not_consume_a_slot(self) -> None:
        config = _intent_config(**_PERMISSIVE)
        run, _, _ = _build_run(
            profiles=(STRONG,), intent_config=config, initial_capital=D("1"), num_folds=1
        )
        decision = run.fold_results[0].decisions[0]
        self.assertEqual(
            decision.reason, RegimeEnsembleDecisionReason.INSUFFICIENT_SLOT_NOTIONAL.value
        )
        self.assertFalse(decision.selected)
        self.assertEqual(run.fold_results[0].intents, ())

    def test_minimum_score_threshold_can_veto_a_real_candidate(self) -> None:
        scores = _observed_scores(profiles=(STRONG,))
        score = scores[STRONG.symbol]

        strict_config = _intent_config(
            minimum_ensemble_score=score.ensemble_score + D("0.01"),
            maximum_uncertainty=D("1"),
        )
        run, _, _ = _build_run(profiles=(STRONG,), intent_config=strict_config, num_folds=1)
        decision = run.fold_results[0].decisions[0]
        self.assertEqual(
            decision.reason, RegimeEnsembleDecisionReason.BELOW_MINIMUM_ENSEMBLE_SCORE.value
        )
        self.assertEqual(decision.score, score.ensemble_score)

    def test_maximum_uncertainty_threshold_can_veto_a_real_candidate(self) -> None:
        scores = _observed_scores(profiles=(STRONG,))
        score = scores[STRONG.symbol]

        tight_uncertainty_config = _intent_config(
            minimum_ensemble_score=D("0"),
            maximum_uncertainty=max(score.uncertainty - D("0.01"), D("0")),
        )
        run, _, _ = _build_run(
            profiles=(STRONG,), intent_config=tight_uncertainty_config, num_folds=1
        )
        decision = run.fold_results[0].decisions[0]
        self.assertEqual(
            decision.reason, RegimeEnsembleDecisionReason.ABOVE_MAXIMUM_UNCERTAINTY.value
        )

    def test_tied_group_crossing_capacity_is_rejected_as_a_group_without_identifier_alpha(
        self,
    ) -> None:
        profiles = (TIE_A, TIE_B)
        config = _intent_config(maximum_positions=1, **_PERMISSIVE)
        run, _, _ = _build_run(profiles=profiles, intent_config=config, num_folds=1)
        decisions = {value.symbol: value for value in run.fold_results[0].decisions}
        self.assertEqual(decisions[TIE_A.symbol].score, decisions[TIE_B.symbol].score)
        for decision in decisions.values():
            self.assertEqual(
                decision.reason, RegimeEnsembleDecisionReason.TIED_AT_POSITION_CUTOFF.value
            )
            self.assertFalse(decision.selected)

    def test_outside_position_rank_is_used_once_capacity_is_exhausted(self) -> None:
        profiles = (STRONG, TIE_A, TIE_B)
        config = _intent_config(maximum_positions=1, **_PERMISSIVE)
        run, _, _ = _build_run(profiles=profiles, intent_config=config, num_folds=1)
        decisions = {value.symbol: value for value in run.fold_results[0].decisions}
        self.assertTrue(decisions[STRONG.symbol].selected)
        for symbol in (TIE_A.symbol, TIE_B.symbol):
            self.assertEqual(
                decisions[symbol].reason,
                RegimeEnsembleDecisionReason.OUTSIDE_POSITION_RANK.value,
            )
            self.assertFalse(decisions[symbol].selected)


class RegimeEnsembleIntentGeneratorRejectionTests(unittest.TestCase):
    def test_rejects_wrong_type_inputs(self) -> None:
        run, proposal_batches, plan = _build_run(profiles=(STRONG,), num_folds=1)
        universe_snapshot_ids = tuple(
            batch.universe_batch.universe_snapshot_id for batch in proposal_batches
        )
        dataset = _dataset_bars(
            proposal_batches=proposal_batches, universe_snapshot_ids=universe_snapshot_ids
        )
        instruments = _instruments(
            profiles=(STRONG,),
            signal_sessions=tuple(value.test_sessions[0] for value in plan.folds),
            universe_snapshot_ids=universe_snapshot_ids,
        )
        execution_policy = _execution_policy()
        base_kwargs = dict(
            config=_intent_config(),
            split_plan=plan,
            dataset=dataset,
            instruments=instruments,
            proposal_batches=proposal_batches,
            execution_policy=execution_policy,
            initial_capital=D("100000"),
        )
        generator = RegimeEnsembleIntentGenerator()
        for field_name, bad_value in (
            ("config", "not-a-config"),
            ("split_plan", "not-a-split-plan"),
            ("dataset", "not-a-dataset"),
            ("execution_policy", "not-an-execution-policy"),
        ):
            kwargs = dict(base_kwargs)
            kwargs[field_name] = bad_value
            with self.assertRaisesRegex(RegimeEnsembleEvaluationError, "must be exact"):
                generator.generate(**kwargs)

    def test_rejects_wrong_proposal_batch_count_for_folds(self) -> None:
        calendar = _calendar()
        plan = _split_plan()
        proposal_batches = tuple(
            _fold_proposal_batch(profiles=(STRONG,), fold=fold, calendar=calendar)
            for fold in plan.folds
        )
        universe_snapshot_ids = tuple(
            batch.universe_batch.universe_snapshot_id for batch in proposal_batches
        )
        instruments = _instruments(
            profiles=(STRONG,),
            signal_sessions=tuple(value.test_sessions[0] for value in plan.folds),
            universe_snapshot_ids=universe_snapshot_ids,
        )
        dataset = _dataset_bars(
            proposal_batches=proposal_batches, universe_snapshot_ids=universe_snapshot_ids
        )
        with self.assertRaisesRegex(
            RegimeEnsembleEvaluationError, "exactly one proposal batch"
        ):
            RegimeEnsembleIntentGenerator().generate(
                config=_intent_config(),
                split_plan=plan,
                dataset=dataset,
                instruments=instruments,
                proposal_batches=proposal_batches[:1],
                execution_policy=_execution_policy(),
                initial_capital=D("100000"),
            )

    def test_rejects_dataset_calendar_mismatch(self) -> None:
        run, proposal_batches, plan = _build_run(profiles=(STRONG,), num_folds=1)
        universe_snapshot_ids = tuple(
            batch.universe_batch.universe_snapshot_id for batch in proposal_batches
        )
        dataset = _dataset_bars(
            proposal_batches=proposal_batches, universe_snapshot_ids=universe_snapshot_ids
        )
        wrong_dataset = EvaluationDataset(
            sessions=dataset.sessions[:-1],
            bars=tuple(value for value in dataset.bars if value.session in dataset.sessions[:-1]),
            source_snapshot_ids=dataset.source_snapshot_ids,
            universe_snapshot_ids=dataset.universe_snapshot_ids,
            readiness=dataset.readiness,
        )
        with self.assertRaisesRegex(RegimeEnsembleEvaluationError, "calendar differs"):
            RegimeEnsembleIntentGenerator().generate(
                config=_intent_config(),
                split_plan=plan,
                dataset=wrong_dataset,
                instruments=_instruments(
                    profiles=(STRONG,),
                    signal_sessions=tuple(value.test_sessions[0] for value in plan.folds),
                    universe_snapshot_ids=universe_snapshot_ids,
                ),
                proposal_batches=proposal_batches,
                execution_policy=_execution_policy(),
                initial_capital=D("100000"),
            )

    def test_rejects_missing_eligible_instrument_coverage(self) -> None:
        calendar = _calendar()
        plan = _split_plan()
        fold = plan.folds[0]
        batch = _fold_proposal_batch(profiles=(STRONG, WEAK), fold=fold, calendar=calendar)
        universe_snapshot_id = batch.universe_batch.universe_snapshot_id
        dataset = _dataset_bars(
            proposal_batches=(batch,), universe_snapshot_ids=(universe_snapshot_id,)
        )
        instruments = _instruments(
            profiles=(STRONG,),
            signal_sessions=(fold.test_sessions[0],),
            universe_snapshot_ids=(universe_snapshot_id,),
        )
        reduced_plan = _single_fold_plan(plan, fold)
        with self.assertRaisesRegex(
            RegimeEnsembleEvaluationError, "eligible instrument coverage"
        ):
            RegimeEnsembleIntentGenerator().generate(
                config=_intent_config(),
                split_plan=reduced_plan,
                dataset=dataset,
                instruments=instruments,
                proposal_batches=(batch,),
                execution_policy=_execution_policy(),
                initial_capital=D("100000"),
            )

    def test_rejects_symbol_isin_mismatch_between_instrument_and_proposal(self) -> None:
        calendar = _calendar()
        plan = _split_plan()
        fold = plan.folds[0]
        batch = _fold_proposal_batch(profiles=(STRONG,), fold=fold, calendar=calendar)
        universe_snapshot_id = batch.universe_batch.universe_snapshot_id
        dataset = _dataset_bars(
            proposal_batches=(batch,), universe_snapshot_ids=(universe_snapshot_id,)
        )
        mismatched_instrument = PointInTimeInstrument(
            symbol=STRONG.symbol,
            isin="INE999999999",
            universe_snapshot_id=universe_snapshot_id,
            eligible_sessions=SESSIONS,
            tick_size=STRONG.tick_size,
            stable_instrument_id=STRONG.instrument_id,
        )
        reduced_plan = _single_fold_plan(plan, fold)
        with self.assertRaisesRegex(
            RegimeEnsembleEvaluationError, "exactly one eligible point-in-time instrument"
        ):
            RegimeEnsembleIntentGenerator().generate(
                config=_intent_config(),
                split_plan=reduced_plan,
                dataset=dataset,
                instruments=(mismatched_instrument,),
                proposal_batches=(batch,),
                execution_policy=_execution_policy(),
                initial_capital=D("100000"),
            )

    def test_rejects_malformed_nested_proposal_batch(self) -> None:
        run, proposal_batches, plan = _build_run(profiles=(STRONG,), num_folds=1)
        universe_snapshot_ids = tuple(
            batch.universe_batch.universe_snapshot_id for batch in proposal_batches
        )
        instruments = _instruments(
            profiles=(STRONG,),
            signal_sessions=tuple(value.test_sessions[0] for value in plan.folds),
            universe_snapshot_ids=universe_snapshot_ids,
        )
        dataset = _dataset_bars(
            proposal_batches=proposal_batches, universe_snapshot_ids=universe_snapshot_ids
        )
        with self.assertRaises(RegimeEnsembleEvaluationError):
            RegimeEnsembleIntentGenerator().generate(
                config=_intent_config(),
                split_plan=plan,
                dataset=dataset,
                instruments=instruments,
                proposal_batches=("not-a-batch",),
                execution_policy=_execution_policy(),
                initial_capital=D("100000"),
            )


class RegimeEnsembleIntentRunIdentityTests(unittest.TestCase):
    def test_verify_content_identity_detects_forged_score(self) -> None:
        run, _, _ = _build_run(profiles=(STRONG,), num_folds=1)
        run.verify_content_identity()
        untouched_run_id = run.run_id
        decision = run.fold_results[0].decisions[0]
        object.__setattr__(decision, "score", decision.score + Decimal("0.01"))

        with self.assertRaises(RegimeEnsembleEvaluationError):
            run.verify_content_identity()
        self.assertEqual(run.run_id, untouched_run_id)

    def test_verify_content_identity_detects_forged_selected_flag(self) -> None:
        run, _, _ = _build_run(profiles=(STRONG, WEAK), num_folds=1)
        run.verify_content_identity()
        untouched_run_id = run.run_id
        decision = next(
            value for value in run.fold_results[0].decisions if not value.selected
        )
        object.__setattr__(decision, "selected", True)
        object.__setattr__(decision, "reason", RegimeEnsembleDecisionReason.SELECTED.value)

        with self.assertRaises(RegimeEnsembleEvaluationError):
            run.verify_content_identity()
        self.assertEqual(run.run_id, untouched_run_id)

    def test_verify_content_identity_detects_forged_fold_result_binding(self) -> None:
        run, _, _ = _build_run(profiles=(STRONG,), num_folds=1)
        run.verify_content_identity()
        untouched_run_id = run.run_id
        object.__setattr__(
            run.fold_results[0].proposal_batch, "scoped_subject_count", 999
        )

        with self.assertRaises(RegimeEnsembleEvaluationError):
            run.verify_content_identity()
        self.assertEqual(run.run_id, untouched_run_id)

    def test_run_embeds_exact_split_plan_and_dataset_with_derived_properties(self) -> None:
        run, _, plan = _build_run(profiles=(STRONG,), num_folds=1)
        self.assertIs(run.split_plan, plan)
        self.assertEqual(run.split_plan_id, run.split_plan.plan_id)
        self.assertEqual(run.dataset_id, run.dataset.dataset_id)
        self.assertEqual(run.dataset_readiness, run.dataset.readiness)

    def test_rejects_point_in_time_verified_dataset_paired_with_synthetic_fold_results(
        self,
    ) -> None:
        run, _, _ = _build_run(profiles=(STRONG,), num_folds=1)
        verified_dataset = replace(
            run.dataset, readiness=EvaluationDataReadiness.POINT_IN_TIME_VERIFIED
        )
        with self.assertRaises(RegimeEnsembleEvaluationError):
            RegimeEnsembleIntentRun(
                config=run.config,
                split_plan=run.split_plan,
                dataset=verified_dataset,
                execution_policy=run.execution_policy,
                execution_policy_id=run.execution_policy_id,
                initial_capital=run.initial_capital,
                instruments=run.instruments,
                fold_results=run.fold_results,
                generated_batch=run.generated_batch,
            )

    def test_rejects_dataset_with_changed_source_snapshot_ids_disagreeing_with_generated_batch(
        self,
    ) -> None:
        run, _, _ = _build_run(profiles=(STRONG,), num_folds=1)
        changed_dataset = replace(run.dataset, source_snapshot_ids=("f" * 64,))
        with self.assertRaisesRegex(RegimeEnsembleEvaluationError, "source snapshots differ"):
            RegimeEnsembleIntentRun(
                config=run.config,
                split_plan=run.split_plan,
                dataset=changed_dataset,
                execution_policy=run.execution_policy,
                execution_policy_id=run.execution_policy_id,
                initial_capital=run.initial_capital,
                instruments=run.instruments,
                fold_results=run.fold_results,
                generated_batch=run.generated_batch,
            )

    def test_rejects_different_split_plan_paired_with_existing_fold_results(self) -> None:
        run, _, _ = _build_run(profiles=(STRONG,), num_folds=1)
        full_plan = _split_plan()
        different_plan = _single_fold_plan(full_plan, full_plan.folds[1])
        with self.assertRaises(RegimeEnsembleEvaluationError):
            RegimeEnsembleIntentRun(
                config=run.config,
                split_plan=different_plan,
                dataset=run.dataset,
                execution_policy=run.execution_policy,
                execution_policy_id=run.execution_policy_id,
                initial_capital=run.initial_capital,
                instruments=run.instruments,
                fold_results=run.fold_results,
                generated_batch=run.generated_batch,
            )

    def test_verify_content_identity_detects_embedded_dataset_mutation(self) -> None:
        run, _, _ = _build_run(profiles=(STRONG,), num_folds=1)
        run.verify_content_identity()
        untouched_run_id = run.run_id
        object.__setattr__(
            run.dataset, "readiness", EvaluationDataReadiness.POINT_IN_TIME_VERIFIED
        )

        with self.assertRaises(RegimeEnsembleEvaluationError):
            run.verify_content_identity()
        self.assertEqual(run.run_id, untouched_run_id)

    def test_verify_content_identity_detects_embedded_split_plan_mutation(self) -> None:
        run, _, _ = _build_run(profiles=(STRONG,), num_folds=1)
        run.verify_content_identity()
        untouched_run_id = run.run_id
        object.__setattr__(run.split_plan, "label_horizon_sessions", 999)

        with self.assertRaises(RegimeEnsembleEvaluationError):
            run.verify_content_identity()
        self.assertEqual(run.run_id, untouched_run_id)


if __name__ == "__main__":
    unittest.main()
