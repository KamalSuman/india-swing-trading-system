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
from india_swing.domain.models import (
    Board,
    DataSnapshot,
    EvidenceItem,
    ForecastSummary,
    InstrumentSnapshot,
    MarketCapBucket,
    Surveillance,
)
from india_swing.promotion import (
    PromotionCapability,
    PromotionDecision,
    PromotionEvidence,
    evaluate_promotion,
)
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
    DeterministicSwingSignalProvider,
    InstrumentSwingHistory,
)
from india_swing.signals.history_adapter import SwingHistoryMaterialization
from india_swing.signals.input_assembly import SwingInputAssembly
from india_swing.signals.proposal_batch import (
    SwingProposalBatch,
    SwingProposalBatchError,
    SwingTechnicalProposal,
    assemble_swing_proposal_batch,
)
from india_swing.signals.universe_batch import assemble_universe_input_batch


UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))

HISTORY_SESSIONS = 25
START = date(2026, 1, 1)
SESSIONS = tuple(START + timedelta(days=index) for index in range(HISTORY_SESSIONS))
SIGNAL_SESSION = SESSIONS[-1]
ASSEMBLY_CUTOFF = datetime.combine(SIGNAL_SESSION, time(16, 0), tzinfo=IST)
CALENDAR_CUTOFF = datetime.combine(SIGNAL_SESSION, time(15, 30), tzinfo=IST)
DECISION_TIME = datetime.combine(SIGNAL_SESSION, time(17, 0), tzinfo=IST)
CALENDAR_SOURCE_ID = "c1" * 32

INSTRUMENT_A = "1" * 64
LISTING_A = "2" * 64
INSTRUMENT_B = "d" * 64
LISTING_B = "e" * 64
IDENTITY_SNAPSHOT_ID = "9" * 64
CORPORATE_ACTION_SNAPSHOT_ID = "8" * 64
TICK_EVIDENCE_A = "a1" * 32
TICK_EVIDENCE_B = "b1" * 32

MASTER_A = "a2" * 32
ELIGIBILITY_A = "a3" * 32
LIQUIDITY_A = "a4" * 32
SOURCE_ROW_A = "a5" * 32
MASTER_B = "b2" * 32
ELIGIBILITY_B = "b3" * 32
LIQUIDITY_B = "b4" * 32
SOURCE_ROW_B = "b5" * 32

INSTRUMENT_WATCH = "c2" * 32
LISTING_WATCH = "c3" * 32
MASTER_WATCH = "c4" * 32
SOURCE_ROW_WATCH = "c5" * 32
INSTRUMENT_EXCLUDED = "c6" * 32
LISTING_EXCLUDED = "c7" * 32
MASTER_EXCLUDED = "c8" * 32
SOURCE_ROW_EXCLUDED = "c9" * 32
INSTRUMENT_UNVERIFIED = "ca" * 32
LISTING_UNVERIFIED = "cb" * 32
MASTER_UNVERIFIED = "cc" * 32
SOURCE_ROW_UNVERIFIED = "cd" * 32


def _hex_id(prefix: int, index: int) -> str:
    return f"{prefix:04x}{index:060x}"


def _config() -> DeterministicSwingSignalConfig:
    return DeterministicSwingSignalConfig(
        minimum_history_sessions=HISTORY_SESSIONS,
        momentum_lookback_sessions=10,
        trend_lookback_sessions=20,
        atr_lookback_sessions=10,
        volume_lookback_sessions=10,
        breakout_lookback_sessions=10,
        maximum_holding_sessions=3,
    )


def _calendar() -> CalendarSnapshot:
    days = []
    for offset in range(60):
        day = START + timedelta(days=offset)
        days.append(
            CalendarDay(
                day=day,
                kind=CalendarDayKind.REGULAR,
                reference=ExternalRecordRef(
                    event_time=datetime.combine(day, time(0), tzinfo=IST),
                    knowledge_time=datetime(2025, 12, 1, 12, 0, tzinfo=IST),
                    source="TEST",
                    content_hash=f"{day.toordinal():064x}",
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


def _promotion(
    *,
    raw_ids: tuple[str, ...],
    universe_ids: tuple[str, ...],
    identity_id: str,
    action_id: str,
    tick_id: str,
) -> PromotionDecision:
    expected = (
        (PromotionCapability.RAW_PRICES, tuple(sorted(raw_ids))),
        (PromotionCapability.UNIVERSE, tuple(sorted(universe_ids))),
        (PromotionCapability.STABLE_IDENTITY, (identity_id,)),
        (PromotionCapability.CORPORATE_ACTIONS, (action_id,)),
        (PromotionCapability.TICK_SIZES, (tick_id,)),
    )
    evidence = tuple(
        sorted(
            (
                PromotionEvidence(
                    capability=capability,
                    cutoff=ASSEMBLY_CUTOFF,
                    coverage_start=SESSIONS[0],
                    coverage_end=SIGNAL_SESSION,
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
        market_session=SIGNAL_SESSION,
        history_start=SESSIONS[0],
        decision_cutoff=ASSEMBLY_CUTOFF,
        evidence=evidence,
    )


def _build_assembly(
    *,
    prefix: int,
    instrument_id: str,
    listing_id: str,
    symbol: str,
    raw_ids: tuple[str, ...],
    universe_ids: tuple[str, ...],
    tick_size: Decimal,
    tick_evidence_id: str,
    base_close: Decimal,
) -> SwingInputAssembly:
    bindings = []
    adjusted_bars = []
    history_bars = []
    for index, session in enumerate(SESSIONS):
        knowledge_time = datetime.combine(session, time(16, 0), tzinfo=IST)
        binding = StableRawBarBinding(
            market_session=session,
            raw_bar_id=raw_ids[index],
            stable_instrument_id=instrument_id,
            stable_listing_id=listing_id,
            identity_snapshot_id=IDENTITY_SNAPSHOT_ID,
            knowledge_time=knowledge_time,
        )
        bindings.append(binding)
        close = base_close + Decimal("0.50") * index
        volume = Decimal(1000 + index * 10)
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
                evidence_id=f"{prefix}-bar-{index:03d}",
                content_hash=f"{prefix}-hash-bar-{index:03d}",
            )
        )

    adjusted_history = CorporateActionAdjustedHistory(
        stable_instrument_id=instrument_id,
        stable_listing_id=listing_id,
        signal_session=SIGNAL_SESSION,
        cutoff=ASSEMBLY_CUTOFF,
        adjustment_knowledge_time=ASSEMBLY_CUTOFF - timedelta(minutes=5),
        corporate_action_snapshot_id=CORPORATE_ACTION_SNAPSHOT_ID,
        bars=tuple(adjusted_bars),
    )
    tick_content_hash = f"{prefix}-tick-hash"
    adjustment_evidence_id = f"{prefix}-adjustment-evidence"
    adjustment_content_hash = f"{prefix}-adjustment-hash"
    history = InstrumentSwingHistory(
        instrument_id=instrument_id,
        listing_id=listing_id,
        tick_size=tick_size,
        tick_available_at=ASSEMBLY_CUTOFF - timedelta(minutes=10),
        tick_evidence_id=tick_evidence_id,
        tick_content_hash=tick_content_hash,
        adjustment_available_at=ASSEMBLY_CUTOFF - timedelta(minutes=5),
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
        raw_ids=raw_ids,
        universe_ids=universe_ids,
        identity_id=IDENTITY_SNAPSHOT_ID,
        action_id=CORPORATE_ACTION_SNAPSHOT_ID,
        tick_id=tick_evidence_id,
    )

    return SwingInputAssembly(
        promotion_decision_id=promotion.decision_id,
        promotion=promotion,
        stable_identity_snapshot_id=IDENTITY_SNAPSHOT_ID,
        stable_instrument_id=instrument_id,
        stable_listing_id=listing_id,
        signal_session=SIGNAL_SESSION,
        cutoff=ASSEMBLY_CUTOFF,
        raw_artifact_ids=raw_ids,
        universe_snapshot_ids=universe_ids,
        identity_bindings=tuple(bindings),
        adjusted_history=adjusted_history,
        signal_materialization=materialization,
        readiness=ReferenceReadiness.SYNTHETIC_TEST,
        actionable=False,
    )


def _reference(source_id: str, digit: str) -> ExternalRecordRef:
    return ExternalRecordRef(
        event_time=datetime(2020, 1, 1, tzinfo=UTC),
        knowledge_time=datetime(2025, 12, 1, tzinfo=UTC),
        source="VERIFIED_TEST_EVIDENCE",
        content_hash=digit * 64,
        source_snapshot_id=source_id,
    )


def _actionable_entry(
    *,
    instrument_id: str,
    listing_id: str,
    tradingsymbol: str,
    master_id: str,
    eligibility_id: str,
    liquidity_id: str,
    source_row_id: str,
    digit: str,
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
            isin=None,
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
        liquidity_cutoff_session=SIGNAL_SESSION,
    )


def _non_actionable_entry(
    *,
    instrument_id: str,
    listing_id: str,
    tradingsymbol: str,
    master_id: str,
    source_row_id: str,
    digit: str,
    board: Board,
    listing_state: ListingState,
    suspended: bool | None,
    surveillance: Surveillance,
    disposition: UniverseDisposition,
    reason_codes: tuple[str, ...],
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
            isin=None,
            valid_from=date(2020, 1, 1),
            valid_to_exclusive=None,
            reference=_reference(master_id, digit),
        ),
        board=board,
        listing_state=listing_state,
        suspended=suspended,
        surveillance=surveillance,
        disposition=disposition,
        reason_codes=reason_codes,
        eligibility_refs=(),
    )


def _vetoed_entries() -> tuple[UniverseEntry, ...]:
    return (
        _non_actionable_entry(
            instrument_id=INSTRUMENT_WATCH,
            listing_id=LISTING_WATCH,
            tradingsymbol="SMESTOCK",
            master_id=MASTER_WATCH,
            source_row_id=SOURCE_ROW_WATCH,
            digit="c",
            board=Board.SME,
            listing_state=ListingState.ACTIVE,
            suspended=False,
            surveillance=Surveillance.NONE,
            disposition=UniverseDisposition.WATCH_ONLY,
            reason_codes=("SME_WATCH_ONLY",),
        ),
        _non_actionable_entry(
            instrument_id=INSTRUMENT_EXCLUDED,
            listing_id=LISTING_EXCLUDED,
            tradingsymbol="BLOCKEDCO",
            master_id=MASTER_EXCLUDED,
            source_row_id=SOURCE_ROW_EXCLUDED,
            digit="e",
            board=Board.MAIN,
            listing_state=ListingState.ACTIVE,
            suspended=False,
            surveillance=Surveillance.NONE,
            disposition=UniverseDisposition.EXCLUDED,
            reason_codes=("SURVEILLANCE_BLOCKED",),
        ),
        _non_actionable_entry(
            instrument_id=INSTRUMENT_UNVERIFIED,
            listing_id=LISTING_UNVERIFIED,
            tradingsymbol="UNKNOWNCO",
            master_id=MASTER_UNVERIFIED,
            source_row_id=SOURCE_ROW_UNVERIFIED,
            digit="f",
            board=Board.UNKNOWN,
            listing_state=ListingState.UNKNOWN,
            suspended=None,
            surveillance=Surveillance.UNKNOWN,
            disposition=UniverseDisposition.UNVERIFIED,
            reason_codes=("SOURCE_NOT_YET_VERIFIED",),
        ),
    )


def _current_universe(*, calendar_snapshot_id: str) -> UniverseSnapshot:
    entry_a = _actionable_entry(
        instrument_id=INSTRUMENT_A,
        listing_id=LISTING_A,
        tradingsymbol="STOCKA",
        master_id=MASTER_A,
        eligibility_id=ELIGIBILITY_A,
        liquidity_id=LIQUIDITY_A,
        source_row_id=SOURCE_ROW_A,
        digit="9",
    )
    entry_b = _actionable_entry(
        instrument_id=INSTRUMENT_B,
        listing_id=LISTING_B,
        tradingsymbol="STOCKB",
        master_id=MASTER_B,
        eligibility_id=ELIGIBILITY_B,
        liquidity_id=LIQUIDITY_B,
        source_row_id=SOURCE_ROW_B,
        digit="b",
    )
    entries = sorted(
        (entry_a, entry_b, *_vetoed_entries()),
        key=lambda value: value.source_record_id,
    )
    return UniverseSnapshot.create(
        exchange="NSE",
        segment="CM",
        market_session=SIGNAL_SESSION,
        cutoff=ASSEMBLY_CUTOFF,
        calendar_snapshot_id=calendar_snapshot_id,
        universe_rules_version="verified-main-board/v1",
        selection_key="ALL_SCOPED_ROWS",
        scoped_source_row_ids=tuple(value.source_record_id for value in entries),
        security_master_snapshot_ids=tuple(
            sorted({MASTER_A, MASTER_B, MASTER_WATCH, MASTER_EXCLUDED, MASTER_UNVERIFIED})
        ),
        eligibility_snapshot_ids=tuple(sorted({ELIGIBILITY_A, ELIGIBILITY_B})),
        liquidity_snapshot_ids=tuple(sorted({LIQUIDITY_A, LIQUIDITY_B})),
        readiness=ReferenceReadiness.SYNTHETIC_TEST,
        entries=tuple(entries),
    )


class SwingProposalBatchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = _config()
        self.calendar = _calendar()
        self.current_universe = _current_universe(
            calendar_snapshot_id=self.calendar.snapshot_id
        )

        raw_ids = tuple(_hex_id(0xA1, index) for index in range(HISTORY_SESSIONS))
        universe_ids = tuple(
            _hex_id(0xB1, index) for index in range(HISTORY_SESSIONS - 1)
        ) + (self.current_universe.snapshot_id,)

        self.assembly_a = _build_assembly(
            prefix=1,
            instrument_id=INSTRUMENT_A,
            listing_id=LISTING_A,
            symbol="STOCKA",
            raw_ids=raw_ids,
            universe_ids=universe_ids,
            tick_size=Decimal("0.05"),
            tick_evidence_id=TICK_EVIDENCE_A,
            base_close=Decimal("100"),
        )
        self.assembly_b = _build_assembly(
            prefix=2,
            instrument_id=INSTRUMENT_B,
            listing_id=LISTING_B,
            symbol="STOCKB",
            raw_ids=raw_ids,
            universe_ids=universe_ids,
            tick_size=Decimal("0.10"),
            tick_evidence_id=TICK_EVIDENCE_B,
            base_close=Decimal("500"),
        )
        self.universe_batch = assemble_universe_input_batch(
            current_universe=self.current_universe,
            assemblies=(self.assembly_a, self.assembly_b),
        )

    def _batch(self, **overrides) -> SwingProposalBatch:
        kwargs = dict(
            universe_batch=self.universe_batch,
            calendar=self.calendar,
            config=self.config,
        )
        kwargs.update(overrides)
        return assemble_swing_proposal_batch(**kwargs)

    def test_full_batch_has_two_proposals_and_three_unchanged_vetoes(self) -> None:
        batch = self._batch()

        self.assertEqual(len(batch.proposals), 2)
        self.assertEqual(len(batch.vetoes), 3)
        self.assertEqual(batch.vetoes, self.universe_batch.vetoes)
        self.assertEqual(batch.scoped_subject_count, 5)
        self.assertEqual(batch.proposal_subject_count, 2)
        self.assertEqual(batch.veto_subject_count, 3)
        self.assertEqual(
            tuple(value.symbol for value in batch.proposals), ("STOCKA", "STOCKB")
        )
        self.assertTrue(all(value.research_only for value in batch.proposals))
        self.assertTrue(all(not value.execution_eligible for value in batch.proposals))
        self.assertFalse(batch.execution_eligible)
        self.assertIs(batch.readiness, ReferenceReadiness.SYNTHETIC_TEST)
        batch.verify_content_identity()

    def test_batch_id_and_proposal_ids_are_deterministic(self) -> None:
        first = self._batch()
        second = self._batch()

        self.assertEqual(first.batch_id, second.batch_id)
        self.assertEqual(
            tuple(value.proposal_id for value in first.proposals),
            tuple(value.proposal_id for value in second.proposals),
        )

    def test_legacy_provider_matches_proposal_when_spread_is_at_or_below_base_cost(
        self,
    ) -> None:
        batch = self._batch()
        proposal_a = batch.proposals[0]
        history_a = self.assembly_a.signal_materialization.history

        evidence = tuple(
            EvidenceItem(
                evidence_id=bar.evidence_id,
                source="TEST_BAR",
                published_at=bar.available_at - timedelta(minutes=1),
                available_at=bar.available_at,
                content_hash=bar.content_hash,
            )
            for bar in history_a.bars
        ) + (
            EvidenceItem(
                evidence_id=history_a.tick_evidence_id,
                source="TEST_TICK",
                published_at=history_a.tick_available_at - timedelta(minutes=1),
                available_at=history_a.tick_available_at,
                content_hash=history_a.tick_content_hash,
            ),
            EvidenceItem(
                evidence_id=history_a.adjustment_evidence_id,
                source="TEST_ADJUSTMENT",
                published_at=history_a.adjustment_available_at - timedelta(minutes=1),
                available_at=history_a.adjustment_available_at,
                content_hash=history_a.adjustment_content_hash,
            ),
        )
        legacy_snapshot = DataSnapshot(
            snapshot_id="snapshot-a",
            decision_time=DECISION_TIME,
            market_session=SIGNAL_SESSION,
            evidence=evidence,
            session_finalized_at=DECISION_TIME - timedelta(minutes=30),
            universe_snapshot_id=self.current_universe.snapshot_id,
            calendar_version=self.calendar.version,
            trial_id="trial-test",
            model_bundle_id="models-test",
            data_content_hash="data-test",
            source_revision="source-test",
            execution_policy_version="execution-test",
            cost_schedule_version="cost-test",
        )
        low_spread_instrument = InstrumentSnapshot(
            instrument_id=INSTRUMENT_A,
            listing_id=LISTING_A,
            universe_snapshot_id=self.current_universe.snapshot_id,
            exchange="NSE",
            segment="CM",
            symbol="STOCKA",
            board=Board.MAIN,
            market_cap_bucket=MarketCapBucket.SMALL,
            active=True,
            suspended=False,
            surveillance=Surveillance.NONE,
            last_price=history_a.bars[-1].close,
            median_daily_traded_value=Decimal("20000000"),
            quoted_spread_bps=Decimal("10"),
            lower_circuit_locked=False,
            history_sessions=len(history_a.bars),
            price_session=SIGNAL_SESSION,
            data_available_at=history_a.bars[-1].available_at,
        )
        low_spread_forecast = ForecastSummary(
            symbol="STOCKA",
            as_of=legacy_snapshot.decision_time,
            horizon_sessions=8,
            median_return_pct=Decimal("4"),
            downside_return_pct=Decimal("-2"),
            uncertainty=Decimal("0.30"),
            sample_count=100,
            model_version="test-forecast/v1",
            instrument_id=INSTRUMENT_A,
            listing_id=LISTING_A,
            universe_snapshot_id=low_spread_instrument.universe_snapshot_id,
            data_snapshot_id=legacy_snapshot.snapshot_id,
            data_snapshot_fingerprint=legacy_snapshot.content_fingerprint,
            instrument_fingerprint=low_spread_instrument.content_fingerprint,
        )
        provider = DeterministicSwingSignalProvider(
            snapshot=legacy_snapshot,
            histories=(history_a,),
            calendar=self.calendar,
            config=self.config,
        )
        signals, setup, evidence_ids = provider.generate(
            low_spread_instrument, low_spread_forecast, legacy_snapshot
        )

        self.assertEqual(signals.estimated_cost_bps, self.config.base_round_trip_cost_bps)
        self.assertEqual(setup.entry_low, proposal_a.levels.entry_low)
        self.assertEqual(setup.entry_high, proposal_a.levels.entry_high)
        self.assertEqual(setup.stop, proposal_a.levels.stop)
        self.assertEqual(setup.target, proposal_a.levels.target)
        self.assertEqual(setup.earliest_entry_at, proposal_a.entry_window.earliest_entry_at)
        self.assertEqual(setup.entry_expires_at, proposal_a.entry_window.entry_expires_at)
        self.assertEqual(evidence_ids, proposal_a.evidence_ids)

        high_spread_instrument = replace(
            low_spread_instrument, quoted_spread_bps=Decimal("50")
        )
        high_spread_forecast = replace(
            low_spread_forecast,
            instrument_fingerprint=high_spread_instrument.content_fingerprint,
        )
        _, high_setup, _ = provider.generate(
            high_spread_instrument, high_spread_forecast, legacy_snapshot
        )

        self.assertEqual(high_setup.entry_low, proposal_a.levels.entry_low)
        self.assertNotEqual(high_setup.target, proposal_a.levels.target)
        self.assertGreater(high_setup.target, proposal_a.levels.target)
        self.assertEqual(proposal_a.levels.estimated_cost_bps, self.config.base_round_trip_cost_bps)
        self.assertFalse(proposal_a.execution_eligible)

    def test_rejects_missing_extra_duplicate_and_reordered_proposal_coverage(self) -> None:
        batch = self._batch()
        proposal_a, proposal_b = batch.proposals

        with self.assertRaises(SwingProposalBatchError):
            replace(batch, proposals=(proposal_a,))
        with self.assertRaises(SwingProposalBatchError):
            replace(batch, proposals=(proposal_a, proposal_a))
        with self.assertRaises(SwingProposalBatchError):
            replace(batch, proposals=(proposal_b, proposal_a))

    def test_rejects_wrong_universe_entry_subject_or_symbol_on_direct_construction(
        self,
    ) -> None:
        batch = self._batch()
        proposal_a = batch.proposals[0]
        wrong_entry = next(
            entry
            for entry in self.current_universe.entries
            if entry.listing.instrument_id == INSTRUMENT_B
        )

        with self.assertRaisesRegex(
            SwingProposalBatchError, "does not match the universe entry"
        ):
            replace(proposal_a, universe_entry=wrong_entry)

    def test_rejects_wrong_final_universe_snapshot_on_direct_construction(self) -> None:
        batch = self._batch()
        proposal_a = batch.proposals[0]

        with self.assertRaises(SwingProposalBatchError):
            replace(proposal_a, universe_snapshot_id="f" * 64)

    def test_rejects_calendar_without_required_next_or_holding_sessions(self) -> None:
        short_days = []
        for offset in range(HISTORY_SESSIONS):
            day = START + timedelta(days=offset)
            short_days.append(
                CalendarDay(
                    day=day,
                    kind=CalendarDayKind.REGULAR,
                    reference=ExternalRecordRef(
                        event_time=datetime.combine(day, time(0), tzinfo=IST),
                        knowledge_time=datetime(2025, 12, 1, 12, 0, tzinfo=IST),
                        source="TEST",
                        content_hash=f"{day.toordinal():064x}",
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
        short_calendar = CalendarSnapshot.create(
            exchange="NSE",
            segment="CM",
            cutoff=CALENDAR_CUTOFF,
            coverage_start=short_days[0].day,
            coverage_end=short_days[-1].day,
            days=tuple(short_days),
            source_snapshot_ids=(CALENDAR_SOURCE_ID,),
            readiness=ReferenceReadiness.SYNTHETIC_TEST,
        )

        with self.assertRaises(Exception):
            self._batch(calendar=short_calendar)

    def test_rejects_calendar_not_pinned_by_the_current_universe(self) -> None:
        different_calendar = replace(
            self.calendar,
            cutoff=self.calendar.cutoff - timedelta(minutes=1),
        )

        with self.assertRaisesRegex(
            SwingProposalBatchError, "calendar snapshot differs"
        ):
            self._batch(calendar=different_calendar)

    def test_rejects_calendar_known_after_the_decision_cutoff(self) -> None:
        batch = self._batch()
        future_calendar = replace(
            self.calendar,
            cutoff=ASSEMBLY_CUTOFF + timedelta(minutes=1),
        )

        with self.assertRaisesRegex(
            SwingProposalBatchError, "calendar postdates"
        ):
            replace(batch.proposals[0], calendar=future_calendar)

    def test_rejects_calendar_where_signal_day_is_not_a_session(self) -> None:
        batch = self._batch()
        signal_offset = (SIGNAL_SESSION - self.calendar.coverage_start).days
        holiday = replace(
            self.calendar.days[signal_offset],
            kind=CalendarDayKind.HOLIDAY,
            session_windows=(),
        )
        days = list(self.calendar.days)
        days[signal_offset] = holiday
        holiday_calendar = replace(self.calendar, days=tuple(days))

        with self.assertRaisesRegex(
            SwingProposalBatchError, "assembly signal session"
        ):
            replace(batch.proposals[0], calendar=holiday_calendar)

    def test_direct_construction_cannot_forge_counts_readiness_or_veto_tuple(self) -> None:
        batch = self._batch()

        with self.assertRaises(SwingProposalBatchError):
            replace(batch, scoped_subject_count=batch.scoped_subject_count + 1)
        with self.assertRaises(SwingProposalBatchError):
            replace(batch, proposal_subject_count=batch.proposal_subject_count + 1)
        with self.assertRaises(SwingProposalBatchError):
            replace(batch, vetoes=batch.vetoes[:-1])

    def test_verify_content_identity_detects_nested_mutation_without_disturbing_batch_id(
        self,
    ) -> None:
        batch = self._batch()
        original_batch_id = batch.batch_id
        object.__setattr__(
            batch.proposals[0].metrics, "atr", batch.proposals[0].metrics.atr + Decimal("1")
        )

        self.assertEqual(batch.batch_id, original_batch_id)
        with self.assertRaises(Exception):
            batch.verify_content_identity()

    def test_verify_content_identity_detects_levels_mutation(self) -> None:
        batch = self._batch()
        original_batch_id = batch.batch_id
        object.__setattr__(
            batch.proposals[0].levels,
            "target",
            batch.proposals[0].levels.target + batch.proposals[0].levels.entry_high,
        )

        self.assertEqual(batch.batch_id, original_batch_id)
        with self.assertRaises(Exception):
            batch.verify_content_identity()

    def test_verify_content_identity_detects_changed_evidence_ids(self) -> None:
        batch = self._batch()
        proposal_a = batch.proposals[0]

        with self.assertRaisesRegex(SwingProposalBatchError, "evidence IDs"):
            replace(proposal_a, evidence_ids=proposal_a.evidence_ids[:-1])

    def test_batch_detects_each_nested_identity_mutation(self) -> None:
        batch = self._batch()
        original_batch_id = batch.batch_id
        proposal = batch.proposals[0]
        cases = (
            ("assembly", proposal.assembly, "assembly_id", "f" * 64),
            ("promotion", proposal.assembly.promotion, "decision_id", "f" * 64),
            ("universe entry", proposal.universe_entry.listing, "tradingsymbol", "FORGED"),
            ("calendar", batch.calendar, "version", "forged-calendar-version"),
            ("config", batch.config, "config_id", "f" * 64),
            ("metrics", proposal.metrics, "metrics_id", "f" * 64),
            ("levels", proposal.levels, "levels_id", "f" * 64),
            ("proposal", proposal, "proposal_id", "f" * 64),
            ("veto", batch.vetoes[0], "veto_id", "f" * 64),
        )

        for label, target, attribute, forged in cases:
            with self.subTest(label=label):
                original = getattr(target, attribute)
                try:
                    object.__setattr__(target, attribute, forged)
                    self.assertEqual(batch.batch_id, original_batch_id)
                    with self.assertRaises(Exception):
                        batch.verify_content_identity()
                finally:
                    object.__setattr__(target, attribute, original)
                batch.verify_content_identity()

    def test_batch_fails_closed_when_history_is_insufficient_for_every_subject(self) -> None:
        insufficient_config = replace(
            self.config, minimum_history_sessions=HISTORY_SESSIONS + 1
        )

        with self.assertRaisesRegex(Exception, "shorter"):
            self._batch(config=insufficient_config)


if __name__ == "__main__":
    unittest.main()
