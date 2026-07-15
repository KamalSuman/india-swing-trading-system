from __future__ import annotations

import unittest
from dataclasses import fields, replace
from datetime import date, datetime, time, timedelta, timezone

from india_swing.domain.models import Board, Surveillance
from india_swing.market_data.models import InstrumentBatch
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
    UniverseIntegrityError,
    UniverseSnapshot,
)


IST = timezone(timedelta(hours=5, minutes=30))
SESSION = date(2026, 7, 15)
CUTOFF = datetime(2026, 7, 15, 17, 0, tzinfo=IST)
CALENDAR_ID = "c" * 64
MASTER_ID = "a" * 64
ELIGIBILITY_ID = "b" * 64
LIQUIDITY_ID = "d" * 64
ROW_ONE = "1" * 64
ROW_TWO = "2" * 64


def record_ref(
    *,
    source_snapshot_id: str = MASTER_ID,
    content_hash: str = "e" * 64,
    knowledge_time: datetime = CUTOFF - timedelta(hours=1),
    event_day: date = SESSION,
) -> ExternalRecordRef:
    return ExternalRecordRef(
        event_time=datetime.combine(event_day, time(0), tzinfo=IST),
        knowledge_time=knowledge_time,
        source="NSE_SYNTHETIC_FIXTURE",
        content_hash=content_hash,
        source_snapshot_id=source_snapshot_id,
    )


def listing(
    *,
    instrument_id: str = "instrument-opaque-1",
    listing_id: str = "listing-opaque-1",
    symbol: str = "ALPHA",
    series: str = "EQ",
    valid_from: date = date(2020, 1, 1),
    valid_to_exclusive: date | None = None,
    reference: ExternalRecordRef | None = None,
) -> ListingMapping:
    return ListingMapping(
        instrument_id=instrument_id,
        listing_id=listing_id,
        exchange="NSE",
        segment="CM",
        tradingsymbol=symbol,
        series=series,
        isin="INE000A01001",
        valid_from=valid_from,
        valid_to_exclusive=valid_to_exclusive,
        reference=reference or record_ref(),
    )


def actionable_entry(
    *,
    source_record_id: str = ROW_ONE,
    listing_value: ListingMapping | None = None,
    liquidity_cutoff_session: date = SESSION,
    eligibility_from_session: date | None = None,
    eligibility_to_exclusive: date | None = None,
    eligibility_event_day: date = SESSION,
) -> UniverseEntry:
    listing_mapping = listing_value or listing()
    eligibility = EligibilityStateRef(
        effective=EffectiveExternalRecordRef(
            reference=record_ref(
                source_snapshot_id=ELIGIBILITY_ID,
                content_hash="f" * 64,
                event_day=eligibility_event_day,
            ),
            effective_from_session=eligibility_from_session or liquidity_cutoff_session,
            effective_to_exclusive=eligibility_to_exclusive,
            schema_version="synthetic-eligibility/v1",
        ),
        instrument_id=listing_mapping.instrument_id,
        listing_id=listing_mapping.listing_id,
        board=Board.MAIN,
        listing_state=ListingState.ACTIVE,
        suspended=False,
        surveillance=Surveillance.NONE,
    )
    return UniverseEntry(
        source_record_id=source_record_id,
        listing=listing_mapping,
        board=Board.MAIN,
        listing_state=ListingState.ACTIVE,
        suspended=False,
        surveillance=Surveillance.NONE,
        disposition=UniverseDisposition.ACTIONABLE,
        reason_codes=(),
        eligibility_refs=(eligibility,),
        liquidity_snapshot_id=LIQUIDITY_ID,
        liquidity_cutoff_session=liquidity_cutoff_session,
    )


def unverified_entry(
    *,
    source_record_id: str = ROW_ONE,
    listing_value: ListingMapping | None = None,
) -> UniverseEntry:
    return UniverseEntry(
        source_record_id=source_record_id,
        listing=listing_value or listing(),
        board=Board.UNKNOWN,
        listing_state=ListingState.UNKNOWN,
        suspended=None,
        surveillance=Surveillance.UNKNOWN,
        disposition=UniverseDisposition.UNVERIFIED,
        reason_codes=("MISSING_REFERENCE_ENRICHMENT",),
        eligibility_refs=(),
    )


def universe(
    *entries: UniverseEntry,
    session: date = SESSION,
    cutoff: datetime = CUTOFF,
    readiness: ReferenceReadiness = ReferenceReadiness.SYNTHETIC_TEST,
    scoped_source_row_ids: tuple[str, ...] | None = None,
) -> UniverseSnapshot:
    ordered_entries = tuple(sorted(entries, key=lambda entry: entry.source_record_id))
    scoped = scoped_source_row_ids or tuple(
        entry.source_record_id for entry in ordered_entries
    )
    eligibility_ids = tuple(
        sorted(
            {
                state.effective.reference.source_snapshot_id
                for entry in ordered_entries
                for state in entry.eligibility_refs
            }
        )
    )
    liquidity_ids = tuple(
        sorted(
            {
                entry.liquidity_snapshot_id
                for entry in ordered_entries
                if entry.liquidity_snapshot_id is not None
            }
        )
    )
    master_ids = tuple(
        sorted({entry.listing.reference.source_snapshot_id for entry in ordered_entries})
    )
    return UniverseSnapshot.create(
        exchange="NSE",
        segment="CM",
        market_session=session,
        cutoff=cutoff,
        calendar_snapshot_id=CALENDAR_ID,
        universe_rules_version="synthetic-rules/v1",
        selection_key="NSE:CM:ALL_SCOPED_ROWS",
        scoped_source_row_ids=scoped,
        security_master_snapshot_ids=master_ids,
        eligibility_snapshot_ids=eligibility_ids,
        liquidity_snapshot_ids=liquidity_ids,
        readiness=readiness,
        entries=ordered_entries,
    )


class PointInTimeUniverseTests(unittest.TestCase):
    def test_eligibility_lineage_is_bound_to_its_subject(self) -> None:
        entry = actionable_entry()
        state = entry.eligibility_refs[0]

        with self.assertRaisesRegex(UniverseIntegrityError, "subject does not match"):
            replace(
                entry,
                eligibility_refs=(
                    replace(state, instrument_id="another-instrument"),
                ),
            )

    def test_effective_facts_must_match_the_universe_session_state(self) -> None:
        entry = actionable_entry()
        mismatched_state = replace(
            entry.eligibility_refs[0],
            surveillance=Surveillance.GSM,
        )

        with self.assertRaisesRegex(
            UniverseIntegrityError,
            "facts do not match the universe session",
        ):
            universe(replace(entry, eligibility_refs=(mismatched_state,)))

    def test_adjacent_eligibility_vintages_resolve_without_overlap(self) -> None:
        entry = actionable_entry()
        state = entry.eligibility_refs[0]
        next_session = SESSION + timedelta(days=1)
        current_state = replace(
            state,
            effective=replace(
                state.effective,
                effective_to_exclusive=next_session,
            ),
        )
        next_state = replace(
            state,
            effective=replace(
                state.effective,
                reference=replace(
                    state.effective.reference,
                    content_hash="0" * 64,
                ),
                effective_from_session=next_session,
                effective_to_exclusive=None,
            ),
        )
        eligibility_refs = tuple(
            sorted(
                (current_state, next_state),
                key=lambda item: item.effective.reference.content_hash,
            )
        )
        rolled_entry = replace(entry, eligibility_refs=eligibility_refs)

        built = universe(rolled_entry)

        self.assertEqual(
            built.entries[0].eligibility_state_on(SESSION),
            current_state,
        )
        self.assertEqual(
            built.entries[0].eligibility_state_on(next_session),
            next_state,
        )

    def test_overlapping_eligibility_vintages_are_rejected(self) -> None:
        entry = actionable_entry()
        state = entry.eligibility_refs[0]
        overlapping = replace(
            state,
            effective=replace(
                state.effective,
                reference=replace(
                    state.effective.reference,
                    content_hash="0" * 64,
                ),
                effective_from_session=SESSION + timedelta(days=1),
            ),
        )
        eligibility_refs = tuple(
            sorted(
                (state, overlapping),
                key=lambda item: item.effective.reference.content_hash,
            )
        )

        with self.assertRaisesRegex(UniverseIntegrityError, "cannot overlap"):
            replace(entry, eligibility_refs=eligibility_refs)

    def test_universe_entry_subclass_cannot_override_state_resolution(self) -> None:
        class ForgedUniverseEntry(UniverseEntry):
            def eligibility_state_on(self, session: date) -> EligibilityStateRef:
                return self.eligibility_refs[0]

        entry = actionable_entry()
        forged_entry = ForgedUniverseEntry(
            **{
                item.name: getattr(entry, item.name)
                for item in fields(UniverseEntry)
                if item.init
            }
        )

        with self.assertRaisesRegex(TypeError, "exact UniverseEntry"):
            universe(forged_entry)

    def test_verified_label_is_locked_until_an_official_importer_exists(self) -> None:
        with self.assertRaisesRegex(UniverseIntegrityError, "importer"):
            universe(
                actionable_entry(),
                readiness=ReferenceReadiness.POINT_IN_TIME_VERIFIED,
            )

    def test_every_scoped_master_record_has_exactly_one_disposition(self) -> None:
        entry = unverified_entry()

        with self.assertRaisesRegex(UniverseIntegrityError, "exactly one"):
            universe(
                entry,
                readiness=ReferenceReadiness.COLLECTION_ONLY,
                scoped_source_row_ids=(ROW_ONE, ROW_TWO),
            )

    def test_universe_rejects_record_known_after_cutoff(self) -> None:
        late_listing = listing(
            reference=record_ref(knowledge_time=CUTOFF + timedelta(microseconds=1))
        )

        with self.assertRaisesRegex(UniverseIntegrityError, "known after"):
            universe(unverified_entry(listing_value=late_listing))

    def test_source_event_date_is_distinct_from_effective_session(self) -> None:
        produced_on_prior_day = actionable_entry(
            eligibility_event_day=date(2026, 7, 14),
            eligibility_from_session=SESSION,
        )

        built = universe(produced_on_prior_day)

        self.assertEqual(built.actionable_entries, (produced_on_prior_day,))

    def test_eligibility_interval_must_cover_the_universe_session(self) -> None:
        expired = actionable_entry(
            eligibility_from_session=date(2026, 7, 14),
            eligibility_to_exclusive=SESSION,
        )

        with self.assertRaisesRegex(UniverseIntegrityError, "effective"):
            universe(expired)

    def test_kite_inventory_cannot_create_verified_universe(self) -> None:
        inventory = InstrumentBatch(
            exchange="NSE",
            observed_at=CUTOFF,
            provider_version="kiteconnect/5.2.0",
            instruments=(),
        )

        with self.assertRaisesRegex(TypeError, "UniverseEntry"):
            UniverseSnapshot.create(
                exchange="NSE",
                segment="CM",
                market_session=SESSION,
                cutoff=CUTOFF,
                calendar_snapshot_id=CALENDAR_ID,
                universe_rules_version="rules/v1",
                selection_key="NSE:CM",
                scoped_source_row_ids=(ROW_ONE,),
                security_master_snapshot_ids=(MASTER_ID,),
                eligibility_snapshot_ids=(),
                liquidity_snapshot_ids=(),
                readiness=ReferenceReadiness.SYNTHETIC_TEST,
                entries=(inventory,),  # type: ignore[arg-type]
            )

    def test_collection_only_universe_can_contain_only_unverified_entries(self) -> None:
        collected = universe(
            unverified_entry(),
            readiness=ReferenceReadiness.COLLECTION_ONLY,
        )
        self.assertEqual(collected.actionable_entries, ())

        with self.assertRaisesRegex(UniverseIntegrityError, "collection-only"):
            universe(
                actionable_entry(),
                readiness=ReferenceReadiness.COLLECTION_ONLY,
            )

    def test_unknown_facts_cannot_default_to_actionable(self) -> None:
        with self.assertRaises(UniverseIntegrityError):
            UniverseEntry(
                source_record_id=ROW_ONE,
                listing=listing(),
                board=Board.UNKNOWN,
                listing_state=ListingState.ACTIVE,
                suspended=False,
                surveillance=Surveillance.NONE,
                disposition=UniverseDisposition.ACTIONABLE,
                reason_codes=(),
                eligibility_refs=(
                    EligibilityStateRef(
                        effective=EffectiveExternalRecordRef(
                            reference=record_ref(
                                source_snapshot_id=ELIGIBILITY_ID,
                                content_hash="f" * 64,
                            ),
                            effective_from_session=SESSION,
                            effective_to_exclusive=None,
                            schema_version="synthetic-eligibility/v1",
                        ),
                        instrument_id="instrument-opaque-1",
                        listing_id="listing-opaque-1",
                        board=Board.UNKNOWN,
                        listing_state=ListingState.ACTIVE,
                        suspended=False,
                        surveillance=Surveillance.NONE,
                    ),
                ),
                liquidity_snapshot_id=LIQUIDITY_ID,
                liquidity_cutoff_session=SESSION,
            )

    def test_symbol_change_preserves_stable_instrument_identity(self) -> None:
        old_mapping = listing(
            symbol="OLDNAME",
            listing_id="listing-old",
            valid_to_exclusive=date(2026, 7, 1),
        )
        new_mapping = listing(
            symbol="NEWNAME",
            listing_id="listing-new",
            valid_from=date(2026, 7, 1),
        )

        prior = universe(
            actionable_entry(
                listing_value=old_mapping,
                liquidity_cutoff_session=date(2026, 6, 30),
            ),
            session=date(2026, 6, 30),
        )
        current = universe(actionable_entry(listing_value=new_mapping))

        self.assertEqual(
            prior.entries[0].listing.instrument_id,
            current.entries[0].listing.instrument_id,
        )
        self.assertNotEqual(
            prior.entries[0].listing.tradingsymbol,
            current.entries[0].listing.tradingsymbol,
        )

    def test_reused_ticker_does_not_join_distinct_instruments(self) -> None:
        original = listing(
            instrument_id="instrument-original",
            listing_id="listing-original",
            symbol="REUSED",
            valid_to_exclusive=date(2026, 1, 1),
        )
        replacement = listing(
            instrument_id="instrument-replacement",
            listing_id="listing-replacement",
            symbol="REUSED",
            valid_from=date(2026, 1, 1),
        )

        prior = universe(
            actionable_entry(
                listing_value=original,
                liquidity_cutoff_session=date(2025, 12, 31),
            ),
            session=date(2025, 12, 31),
        )
        current = universe(actionable_entry(listing_value=replacement))

        self.assertNotEqual(
            prior.entries[0].listing.instrument_id,
            current.entries[0].listing.instrument_id,
        )

    def test_prior_universe_cannot_be_rebuilt_from_latest_master(self) -> None:
        historical_cutoff = datetime(2025, 12, 31, 17, 0, tzinfo=IST)
        latest_record = record_ref(
            knowledge_time=datetime(2026, 7, 15, 8, 30, tzinfo=IST),
            event_day=date(2020, 1, 1),
        )

        with self.assertRaisesRegex(UniverseIntegrityError, "known after"):
            universe(
                unverified_entry(listing_value=listing(reference=latest_record)),
                session=date(2025, 12, 31),
                cutoff=historical_cutoff,
            )

    def test_snapshot_identity_is_deterministic_and_content_derived(self) -> None:
        first = universe(actionable_entry())
        second = universe(actionable_entry())
        changed = universe(
            unverified_entry(),
            readiness=ReferenceReadiness.COLLECTION_ONLY,
        )

        self.assertEqual(first.snapshot_id, second.snapshot_id)
        self.assertEqual(first.selected_records_hash, second.selected_records_hash)
        self.assertEqual(len(first.snapshot_id), 64)
        self.assertNotEqual(first.snapshot_id, changed.snapshot_id)


if __name__ == "__main__":
    unittest.main()
