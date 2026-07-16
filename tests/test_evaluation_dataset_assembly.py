from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from india_swing.evaluation import (
    EffectiveTickSize,
    EvaluationDataReadiness,
    EvaluationDatasetAssemblyError,
    EvaluationDatasetStoreConflict,
    LocalEvaluationDatasetStore,
    PointInTimePriceBar,
    PointInTimePriceSession,
    assemble_evaluation_dataset,
)
from india_swing.domain.models import Board, Surveillance
from india_swing.reference import (
    CalendarDay,
    CalendarDayKind,
    CalendarSnapshot,
    EffectiveExternalRecordRef,
    EligibilityStateRef,
    ExternalRecordRef,
    ListingMapping,
    ListingState,
    ReferenceReadiness,
    SessionWindow,
    SessionWindowPhase,
    UniverseDisposition,
    UniverseEntry,
    UniverseSnapshot,
)


IST = timezone(timedelta(hours=5, minutes=30))
SESSIONS = (date(2026, 7, 13), date(2026, 7, 14), date(2026, 7, 15))
CALENDAR_SOURCE_ID = "1" * 64
MASTER_SOURCE_ID = "2" * 64
ELIGIBILITY_SOURCE_ID = "3" * 64
LIQUIDITY_SOURCE_ID = "4" * 64
TICK_SOURCE_ID = "5" * 64
SOURCE_ROW_ID = "6" * 64
ISIN = "INE002A01018"


def _at(day: date, hour: int, minute: int = 0) -> datetime:
    return datetime.combine(day, time(hour, minute), tzinfo=IST)


def calendar(as_of: date) -> CalendarSnapshot:
    days = tuple(
        CalendarDay(
            day=session,
            kind=CalendarDayKind.REGULAR,
            reference=ExternalRecordRef(
                event_time=_at(session, 0),
                knowledge_time=_at(SESSIONS[0], 8),
                source="SYNTHETIC_CALENDAR_FIXTURE",
                content_hash=f"{index + 10:064x}",
                source_snapshot_id=CALENDAR_SOURCE_ID,
            ),
            session_windows=(
                SessionWindow(
                    opens_at=_at(session, 9, 15),
                    closes_at=_at(session, 15, 30),
                    phase=SessionWindowPhase.LIVE_CONTINUOUS,
                ),
            ),
            data_ready_at=_at(session, 16),
        )
        for index, session in enumerate(SESSIONS)
    )
    return CalendarSnapshot.create(
        exchange="NSE",
        segment="CM",
        cutoff=_at(as_of, 16, 40),
        coverage_start=SESSIONS[0],
        coverage_end=SESSIONS[-1],
        days=days,
        source_snapshot_ids=(CALENDAR_SOURCE_ID,),
        readiness=ReferenceReadiness.SYNTHETIC_TEST,
    )


def _entry(*, session: date, symbol: str = "RELIANCE", isin: str | None = ISIN) -> UniverseEntry:
    listing_reference = ExternalRecordRef(
        event_time=_at(date(2020, 1, 1), 0),
        knowledge_time=_at(SESSIONS[0], 8),
        source="SYNTHETIC_SECURITY_MASTER_FIXTURE",
        content_hash="7" * 64,
        source_snapshot_id=MASTER_SOURCE_ID,
    )
    eligibility_reference = ExternalRecordRef(
        event_time=_at(SESSIONS[0], 0),
        knowledge_time=_at(SESSIONS[0], 8),
        source="SYNTHETIC_ELIGIBILITY_FIXTURE",
        content_hash="8" * 64,
        source_snapshot_id=ELIGIBILITY_SOURCE_ID,
    )
    return UniverseEntry(
        source_record_id=SOURCE_ROW_ID,
        listing=ListingMapping(
            instrument_id="stable-reliance",
            listing_id="listing-reliance-eq",
            exchange="NSE",
            segment="CM",
            tradingsymbol=symbol,
            series="EQ",
            isin=isin,
            valid_from=date(2020, 1, 1),
            valid_to_exclusive=None,
            reference=listing_reference,
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
                    reference=eligibility_reference,
                    effective_from_session=SESSIONS[0],
                    effective_to_exclusive=None,
                    schema_version="synthetic-eligibility/v1",
                ),
                instrument_id="stable-reliance",
                listing_id="listing-reliance-eq",
                board=Board.MAIN,
                listing_state=ListingState.ACTIVE,
                suspended=False,
                surveillance=Surveillance.NONE,
            ),
        ),
        liquidity_snapshot_id=LIQUIDITY_SOURCE_ID,
        liquidity_cutoff_session=session,
    )


def universe(
    value: CalendarSnapshot,
    session: date,
    *,
    symbol: str = "RELIANCE",
    isin: str | None = ISIN,
) -> UniverseSnapshot:
    entry = _entry(session=session, symbol=symbol, isin=isin)
    return UniverseSnapshot.create(
        exchange="NSE",
        segment="CM",
        market_session=session,
        cutoff=_at(session, 16, 30),
        calendar_snapshot_id=value.snapshot_id,
        universe_rules_version="synthetic-main-board/v1",
        selection_key="ALL_SCOPED_ROWS",
        scoped_source_row_ids=(SOURCE_ROW_ID,),
        security_master_snapshot_ids=(MASTER_SOURCE_ID,),
        eligibility_snapshot_ids=(ELIGIBILITY_SOURCE_ID,),
        liquidity_snapshot_ids=(LIQUIDITY_SOURCE_ID,),
        readiness=ReferenceReadiness.SYNTHETIC_TEST,
        entries=(entry,),
    )


def price_session(session: date, *, symbol: str = "RELIANCE", isin: str = ISIN) -> PointInTimePriceSession:
    bar = PointInTimePriceBar(
        session=session,
        symbol=symbol,
        series="EQ",
        isin=isin,
        open=Decimal("100"),
        high=Decimal("105"),
        low=Decimal("99"),
        close=Decimal("104"),
        volume=100000,
        raw_bar_id=f"{session.toordinal():064x}",
    )
    source_id = f"{session.toordinal() + 100:064x}"
    return PointInTimePriceSession(
        market_session=session,
        cutoff=_at(session, 17),
        knowledge_time=_at(session, 16, 45),
        source_artifact_id=source_id,
        source_snapshot_ids=(source_id,),
        bars=(bar,),
        explicit_nontrading_listing_ids=(),
        readiness=EvaluationDataReadiness.SYNTHETIC,
        actionable=True,
    )


def tick_size() -> EffectiveTickSize:
    return EffectiveTickSize(
        instrument_id="stable-reliance",
        listing_id="listing-reliance-eq",
        effective_from_session=SESSIONS[0],
        effective_to_exclusive=None,
        tick_size=Decimal("0.05"),
        knowledge_time=_at(SESSIONS[0], 8),
        source_snapshot_id=TICK_SOURCE_ID,
        readiness=ReferenceReadiness.SYNTHETIC_TEST,
    )


def calendar_vintages() -> tuple[CalendarSnapshot, ...]:
    return tuple(calendar(session) for session in SESSIONS)


def daily_universes(
    values: tuple[CalendarSnapshot, ...],
) -> tuple[UniverseSnapshot, ...]:
    return tuple(
        universe(value, session) for value, session in zip(values, SESSIONS)
    )


def assembled():
    values = calendar_vintages()
    return assemble_evaluation_dataset(
        calendars=values,
        universes=daily_universes(values),
        price_sessions=tuple(price_session(session) for session in SESSIONS),
        tick_sizes=(tick_size(),),
    )


class EvaluationDatasetAssemblyTests(unittest.TestCase):
    def test_assembles_content_bound_dataset_and_daily_identity_bindings(self) -> None:
        result = assembled()

        self.assertEqual(result.dataset.sessions, SESSIONS)
        self.assertEqual(len(result.dataset.bars), 3)
        self.assertEqual(result.dataset.readiness, EvaluationDataReadiness.SYNTHETIC)
        self.assertEqual(len(result.instruments), 1)
        instrument = result.instruments[0]
        self.assertEqual(instrument.stable_instrument_id, "stable-reliance")
        self.assertEqual(
            tuple(session for session, _ in instrument.eligibility_bindings),
            SESSIONS,
        )
        self.assertEqual(len(result.session_evidence), 3)
        result.verify_content_identity()

    def test_collection_only_prices_are_rejected(self) -> None:
        values = calendar_vintages()
        prices = tuple(price_session(session) for session in SESSIONS)
        prices = (
            replace(
                prices[0],
                readiness=EvaluationDataReadiness.COLLECTION_ONLY,
                actionable=False,
            ),
        ) + prices[1:]

        with self.assertRaisesRegex(
            EvaluationDatasetAssemblyError,
            "price sessions must be actionable",
        ):
            assemble_evaluation_dataset(
                calendars=values,
                universes=daily_universes(values),
                price_sessions=prices,
                tick_sizes=(tick_size(),),
            )

    def test_calendar_session_gaps_are_rejected(self) -> None:
        values = calendar_vintages()
        universes = daily_universes(values)
        prices = tuple(price_session(session) for session in SESSIONS)

        with self.assertRaisesRegex(
            EvaluationDatasetAssemblyError,
            "as-of calendar without gaps",
        ):
            assemble_evaluation_dataset(
                calendars=(values[0], values[2]),
                universes=(universes[0], universes[2]),
                price_sessions=(prices[0], prices[2]),
                tick_sizes=(tick_size(),),
            )

    def test_future_calendar_vintage_cannot_enter_an_earlier_decision(self) -> None:
        values = calendar_vintages()
        future_first = calendar(SESSIONS[-1])
        future_bound = (future_first, values[1], values[2])

        with self.assertRaisesRegex(
            EvaluationDatasetAssemblyError,
            "calendar vintage was not sealed",
        ):
            assemble_evaluation_dataset(
                calendars=future_bound,
                universes=daily_universes(future_bound),
                price_sessions=tuple(price_session(session) for session in SESSIONS),
                tick_sizes=(tick_size(),),
            )

    def test_unresolved_or_mismatched_identity_is_rejected(self) -> None:
        values = calendar_vintages()
        unresolved = tuple(
            universe(value, session, isin=None)
            for value, session in zip(values, SESSIONS)
        )
        with self.assertRaisesRegex(
            EvaluationDatasetAssemblyError,
            "adjudicated ISIN",
        ):
            assemble_evaluation_dataset(
                calendars=values,
                universes=unresolved,
                price_sessions=tuple(price_session(session) for session in SESSIONS),
                tick_sizes=(tick_size(),),
            )

    def test_nontrading_evidence_must_exactly_match_missing_actionable_rows(self) -> None:
        values = calendar_vintages()
        prices = list(price_session(session) for session in SESSIONS)
        prices[0] = replace(
            prices[0],
            explicit_nontrading_listing_ids=("listing-reliance-eq",),
        )

        with self.assertRaisesRegex(
            EvaluationDatasetAssemblyError,
            "exact explicit nontrading evidence",
        ):
            assemble_evaluation_dataset(
                calendars=values,
                universes=daily_universes(values),
                price_sessions=tuple(prices),
                tick_sizes=(tick_size(),),
            )

        mismatched = list(price_session(session) for session in SESSIONS)
        mismatched[1] = price_session(SESSIONS[1], isin="INE009A01021")
        with self.assertRaisesRegex(
            EvaluationDatasetAssemblyError,
            "ISIN differs",
        ):
            assemble_evaluation_dataset(
                calendars=values,
                universes=daily_universes(values),
                price_sessions=tuple(mismatched),
                tick_sizes=(tick_size(),),
            )

    def test_tick_size_evidence_is_mandatory_and_timely(self) -> None:
        values = calendar_vintages()
        late_tick = replace(tick_size(), knowledge_time=_at(SESSIONS[-1], 18))

        with self.assertRaisesRegex(
            EvaluationDatasetAssemblyError,
            "exactly one timely tick size",
        ):
            assemble_evaluation_dataset(
                calendars=values,
                universes=daily_universes(values),
                price_sessions=tuple(price_session(session) for session in SESSIONS),
                tick_sizes=(late_tick,),
            )

        unrelated = EffectiveTickSize(
            instrument_id="stable-other",
            listing_id="listing-other-eq",
            effective_from_session=SESSIONS[0],
            effective_to_exclusive=None,
            tick_size=Decimal("0.05"),
            knowledge_time=_at(SESSIONS[0], 8),
            source_snapshot_id="9" * 64,
            readiness=ReferenceReadiness.SYNTHETIC_TEST,
        )
        with self.assertRaisesRegex(
            EvaluationDatasetAssemblyError,
            "exactly equal the evidence used",
        ):
            assemble_evaluation_dataset(
                calendars=values,
                universes=daily_universes(values),
                price_sessions=tuple(price_session(session) for session in SESSIONS),
                tick_sizes=tuple(
                    sorted(
                        (tick_size(), unrelated),
                        key=lambda item: (
                            item.instrument_id,
                            item.listing_id,
                            item.effective_from_session,
                            item.specification_id,
                        ),
                    )
                ),
            )

    def test_symbol_transitions_fail_closed_until_baseline_supports_them(self) -> None:
        values = calendar_vintages()
        universes = (
            universe(values[0], SESSIONS[0]),
            universe(values[1], SESSIONS[1], symbol="RELIANCE-NEW"),
            universe(values[2], SESSIONS[2], symbol="RELIANCE-NEW"),
        )
        prices = (
            price_session(SESSIONS[0]),
            price_session(SESSIONS[1], symbol="RELIANCE-NEW"),
            price_session(SESSIONS[2], symbol="RELIANCE-NEW"),
        )

        with self.assertRaisesRegex(
            EvaluationDatasetAssemblyError,
            "cannot cross listing, symbol, ISIN, or tick-size transitions",
        ):
            assemble_evaluation_dataset(
                calendars=values,
                universes=universes,
                price_sessions=prices,
                tick_sizes=(tick_size(),),
            )

    def test_nested_tampering_is_detected(self) -> None:
        result = assembled()
        object.__setattr__(result.dataset.bars[0], "close", Decimal("101"))

        with self.assertRaisesRegex(ValueError, "content identity"):
            result.verify_content_identity()


class EvaluationDatasetStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.store = LocalEvaluationDatasetStore(self.root)
        self.value = assembled()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_store_is_create_once_and_round_trips_exact_content(self) -> None:
        first = self.store.put(self.value)
        second = self.store.put(self.value)

        self.assertEqual(first, self.value)
        self.assertEqual(second, self.value)
        self.assertEqual(self.store.get(self.value.assembly_id), self.value)
        self.assertEqual(self.store.list_datasets(), (self.value,))

    def test_tampered_payload_is_rejected(self) -> None:
        self.store.put(self.value)
        path = self.store.path_for(self.value.assembly_id)
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["dataset"]["bars"][0]["close"] = "101"
        path.write_text(json.dumps(payload), encoding="utf-8")

        with self.assertRaises(EvaluationDatasetStoreConflict):
            self.store.get(self.value.assembly_id)


if __name__ == "__main__":
    unittest.main()
