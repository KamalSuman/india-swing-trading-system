from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from india_swing.corporate_actions import CorporateActionSnapshot
from india_swing.daily_reports.artifact_store import LocalDailyBundleArtifactStore
from india_swing.daily_reports.parser import NSE_DAILY_BUNDLE_FILENAME
from india_swing.evaluation import EffectiveTickSize
from india_swing.reference import (
    EffectiveExternalRecordRef,
    EligibilityStateRef,
    ExternalRecordRef,
    ListingMapping,
    ListingState,
    ReferenceReadiness,
    UniverseDisposition,
    UniverseEntry,
    UniverseSnapshot,
)
from india_swing.domain.models import Board, Surveillance
from india_swing.signals.input_assembly import assemble_swing_inputs
from india_swing.signals.universe_batch import (
    SwingUniverseBatchError,
    SwingUniverseVeto,
    assemble_universe_input_batch,
)
from tests.test_historical_prices import (
    CUTOFF,
    FIRST_SEEN,
    SESSION,
    VALIDATED,
    _bundle_bytes,
    _clock,
)
from tests.test_swing_input_assembly import (
    IDENTITY_SNAPSHOT_ID,
    INSTRUMENT_ID,
    LISTING_ID,
    promotion,
)


UTC = timezone.utc

INSTRUMENT_A = INSTRUMENT_ID
LISTING_A = LISTING_ID
MASTER_A = "a1" * 32
ELIGIBILITY_A = "a2" * 32
LIQUIDITY_A = "a3" * 32
SOURCE_ROW_A = "f0" * 32
TICK_SOURCE_A = "a4" * 32

INSTRUMENT_B = "d" * 64
LISTING_B = "e" * 64
MASTER_B = "b1" * 32
ELIGIBILITY_B = "b2" * 32
LIQUIDITY_B = "b3" * 32
SOURCE_ROW_B = "01" * 32
TICK_SOURCE_B = "b4" * 32

INSTRUMENT_WATCH = "c5" * 32
LISTING_WATCH = "c6" * 32
MASTER_WATCH = "c7" * 32
SOURCE_ROW_WATCH = "40" * 32

INSTRUMENT_EXCLUDED = "c8" * 32
LISTING_EXCLUDED = "c9" * 32
MASTER_EXCLUDED = "ca" * 32
SOURCE_ROW_EXCLUDED = "50" * 32

INSTRUMENT_UNVERIFIED = "cb" * 32
LISTING_UNVERIFIED = "cc" * 32
MASTER_UNVERIFIED = "cd" * 32
SOURCE_ROW_UNVERIFIED = "60" * 32

CORPORATE_SOURCE_ID = "7" * 64
ALT_CORPORATE_SOURCE_ID = "77" * 32
ALT_IDENTITY_SNAPSHOT_ID = "9c" * 32


def _reference(source_id: str, digit: str) -> ExternalRecordRef:
    return ExternalRecordRef(
        event_time=datetime(2020, 1, 1, tzinfo=UTC),
        knowledge_time=FIRST_SEEN - timedelta(hours=1),
        source="VERIFIED_TEST_EVIDENCE",
        content_hash=digit * 64,
        source_snapshot_id=source_id,
    )


def _actionable_entry(
    *,
    instrument_id: str,
    listing_id: str,
    tradingsymbol: str,
    series: str,
    isin: str | None,
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
            series=series,
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
        liquidity_cutoff_session=SESSION,
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


def _watch_entry() -> UniverseEntry:
    return _non_actionable_entry(
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
    )


def _excluded_entry() -> UniverseEntry:
    return _non_actionable_entry(
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
    )


def _unverified_entry() -> UniverseEntry:
    return _non_actionable_entry(
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
    )


def _multi_universe(
    *,
    second_symbol: str,
    second_series: str,
    second_isin: str | None,
    cutoff: datetime = CUTOFF,
) -> UniverseSnapshot:
    entry_a = _actionable_entry(
        instrument_id=INSTRUMENT_A,
        listing_id=LISTING_A,
        tradingsymbol="INFY",
        series="EQ",
        isin="INE009A01021",
        master_id=MASTER_A,
        eligibility_id=ELIGIBILITY_A,
        liquidity_id=LIQUIDITY_A,
        source_row_id=SOURCE_ROW_A,
        digit="9",
    )
    entry_b = _actionable_entry(
        instrument_id=INSTRUMENT_B,
        listing_id=LISTING_B,
        tradingsymbol=second_symbol,
        series=second_series,
        isin=second_isin,
        master_id=MASTER_B,
        eligibility_id=ELIGIBILITY_B,
        liquidity_id=LIQUIDITY_B,
        source_row_id=SOURCE_ROW_B,
        digit="b",
    )
    entries = sorted(
        (entry_a, entry_b, _watch_entry(), _excluded_entry(), _unverified_entry()),
        key=lambda value: value.source_record_id,
    )
    return UniverseSnapshot.create(
        exchange="NSE",
        segment="CM",
        market_session=SESSION,
        cutoff=cutoff,
        calendar_snapshot_id="b0" * 32,
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


def _collection_only_universe() -> UniverseSnapshot:
    entry = _non_actionable_entry(
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
        reason_codes=("COLLECTION_ONLY_SOURCE",),
    )
    return UniverseSnapshot.create(
        exchange="NSE",
        segment="CM",
        market_session=SESSION,
        cutoff=VALIDATED + timedelta(minutes=10),
        calendar_snapshot_id="b0" * 32,
        universe_rules_version="verified-main-board/v1",
        selection_key="ALL_SCOPED_ROWS",
        scoped_source_row_ids=(SOURCE_ROW_UNVERIFIED,),
        security_master_snapshot_ids=(MASTER_UNVERIFIED,),
        eligibility_snapshot_ids=(),
        liquidity_snapshot_ids=(),
        readiness=ReferenceReadiness.COLLECTION_ONLY,
        entries=(entry,),
    )


def _actions(
    *,
    source_id: str = CORPORATE_SOURCE_ID,
    cutoff: datetime = VALIDATED + timedelta(minutes=5),
) -> CorporateActionSnapshot:
    return CorporateActionSnapshot(
        cutoff=cutoff,
        coverage_start=SESSION,
        coverage_end=SESSION,
        source_artifact_ids=(source_id,),
        events=(),
        readiness=ReferenceReadiness.SYNTHETIC_TEST,
        complete=True,
        actionable=True,
        reason_codes=(),
    )


def _tick(instrument_id: str, listing_id: str, source_id: str) -> EffectiveTickSize:
    return EffectiveTickSize(
        instrument_id=instrument_id,
        listing_id=listing_id,
        effective_from_session=date(2020, 1, 1),
        effective_to_exclusive=None,
        tick_size=Decimal("0.05"),
        knowledge_time=FIRST_SEEN - timedelta(hours=1),
        source_snapshot_id=source_id,
        readiness=ReferenceReadiness.SYNTHETIC_TEST,
    )


class SwingUniverseInputBatchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        source = root / "source" / NSE_DAILY_BUNDLE_FILENAME
        source.parent.mkdir()
        source.write_bytes(_bundle_bytes())
        bundle = LocalDailyBundleArtifactStore(
            root / "daily",
            clock=_clock(FIRST_SEEN, VALIDATED),
        ).import_bundle(source)
        from india_swing.historical_prices import materialize_nse_eod_session

        self.raw = materialize_nse_eod_session(
            bundle,
            market_session=SESSION,
            cutoff=CUTOFF,
        )
        other_bar = self.raw.bars[0]
        self.actions = _actions()
        self.current_universe = _multi_universe(
            second_symbol=other_bar.symbol,
            second_series=other_bar.series,
            second_isin=other_bar.validated_isin,
        )
        self.assembly_a = self._assemble(
            instrument_id=INSTRUMENT_A,
            listing_id=LISTING_A,
            tick_source_id=TICK_SOURCE_A,
        )
        self.assembly_b = self._assemble(
            instrument_id=INSTRUMENT_B,
            listing_id=LISTING_B,
            tick_source_id=TICK_SOURCE_B,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _assemble(
        self,
        *,
        instrument_id: str,
        listing_id: str,
        tick_source_id: str,
        universe_value: UniverseSnapshot | None = None,
        actions_value: CorporateActionSnapshot | None = None,
        identity_id: str = IDENTITY_SNAPSHOT_ID,
    ):
        universe_value = universe_value or self.current_universe
        actions_value = actions_value or self.actions
        tick_size = _tick(instrument_id, listing_id, tick_source_id)
        return assemble_swing_inputs(
            history=(self.raw,),
            universes=(universe_value,),
            stable_identity_snapshot_id=identity_id,
            stable_instrument_id=instrument_id,
            stable_listing_id=listing_id,
            signal_session=SESSION,
            cutoff=CUTOFF,
            corporate_actions=actions_value,
            tick_size=tick_size,
            promotion=promotion(
                self.raw.artifact_id,
                universe_value.snapshot_id,
                actions_value.snapshot_id,
                tick_size.specification_id,
                identity_id,
            ),
        )

    def _batch(self, **overrides):
        kwargs = dict(
            current_universe=self.current_universe,
            assemblies=(self.assembly_a, self.assembly_b),
        )
        kwargs.update(overrides)
        return assemble_universe_input_batch(**kwargs)

    def test_full_universe_coverage_with_no_market_cap_filter(self) -> None:
        batch = self._batch()

        self.assertEqual(len(batch.assemblies), 2)
        self.assertEqual(len(batch.vetoes), 3)
        self.assertEqual(batch.scoped_subject_count, 5)
        self.assertEqual(batch.actionable_subject_count, 2)
        self.assertEqual(batch.veto_subject_count, 3)
        self.assertFalse(batch.actionable)
        batch.verify_content_identity()

        veto_by_symbol = {value.tradingsymbol: value for value in batch.vetoes}
        self.assertEqual(
            veto_by_symbol["SMESTOCK"].disposition, UniverseDisposition.WATCH_ONLY
        )
        self.assertEqual(
            veto_by_symbol["BLOCKEDCO"].disposition, UniverseDisposition.EXCLUDED
        )
        self.assertEqual(
            veto_by_symbol["UNKNOWNCO"].disposition, UniverseDisposition.UNVERIFIED
        )
        self.assertEqual(
            veto_by_symbol["BLOCKEDCO"].reason_codes, ("SURVEILLANCE_BLOCKED",)
        )

    def test_assemblies_and_vetoes_are_canonically_ordered_independent_of_universe_order(
        self,
    ) -> None:
        # SOURCE_ROW_B ("01"*32) sorts before SOURCE_ROW_A ("f0"*32), so the
        # underlying universe.entries order lists subject B before subject A --
        # the opposite of subject-key order. The batch must still emerge in
        # canonical (instrument_id, listing_id) order.
        entry_source_rows = tuple(
            value.source_record_id for value in self.current_universe.entries
        )
        self.assertLess(
            entry_source_rows.index(SOURCE_ROW_B), entry_source_rows.index(SOURCE_ROW_A)
        )

        batch = self._batch()

        self.assertEqual(
            tuple(value.stable_instrument_id for value in batch.assemblies),
            (INSTRUMENT_A, INSTRUMENT_B),
        )
        veto_keys = tuple(value.stable_instrument_id for value in batch.vetoes)
        self.assertEqual(veto_keys, tuple(sorted(veto_keys)))

    def test_rejects_missing_actionable_assembly(self) -> None:
        with self.assertRaises(SwingUniverseBatchError):
            self._batch(assemblies=(self.assembly_a,))

    def test_rejects_duplicate_subject(self) -> None:
        with self.assertRaises(SwingUniverseBatchError):
            self._batch(assemblies=(self.assembly_a, self.assembly_a, self.assembly_b))

    def test_rejects_assembly_for_a_non_actionable_subject(self) -> None:
        original_a = next(
            value
            for value in self.current_universe.entries
            if value.listing.instrument_id == INSTRUMENT_A
        )
        excluded_a = replace(
            original_a,
            disposition=UniverseDisposition.EXCLUDED,
            reason_codes=("SURVEILLANCE_BLOCKED",),
            liquidity_snapshot_id=None,
            liquidity_cutoff_session=None,
            eligibility_refs=(),
        )
        other_entries = tuple(
            value
            for value in self.current_universe.entries
            if value.listing.instrument_id != INSTRUMENT_A
        )
        modified = UniverseSnapshot.create(
            exchange=self.current_universe.exchange,
            segment=self.current_universe.segment,
            market_session=self.current_universe.market_session,
            cutoff=self.current_universe.cutoff,
            calendar_snapshot_id=self.current_universe.calendar_snapshot_id,
            universe_rules_version=self.current_universe.universe_rules_version,
            selection_key=self.current_universe.selection_key,
            scoped_source_row_ids=tuple(
                sorted(value.source_record_id for value in (excluded_a, *other_entries))
            ),
            security_master_snapshot_ids=self.current_universe.security_master_snapshot_ids,
            eligibility_snapshot_ids=self.current_universe.eligibility_snapshot_ids,
            liquidity_snapshot_ids=self.current_universe.liquidity_snapshot_ids,
            readiness=self.current_universe.readiness,
            entries=tuple(
                sorted(
                    (excluded_a, *other_entries), key=lambda value: value.source_record_id
                )
            ),
        )
        with self.assertRaises(SwingUniverseBatchError):
            self._batch(current_universe=modified, assemblies=(self.assembly_a, self.assembly_b))

    def test_rejects_wrong_current_universe_snapshot(self) -> None:
        other_bar = self.raw.bars[0]
        different_universe = _multi_universe(
            second_symbol=other_bar.symbol,
            second_series=other_bar.series,
            second_isin=other_bar.validated_isin,
            cutoff=VALIDATED + timedelta(minutes=45),
        )
        with self.assertRaises(SwingUniverseBatchError):
            self._batch(current_universe=different_universe)

    def test_rejects_wrong_signal_session_cutoff_or_readiness_on_direct_construction(
        self,
    ) -> None:
        batch = self._batch()
        with self.assertRaisesRegex(SwingUniverseBatchError, "signal session"):
            replace(batch, signal_session=SESSION + timedelta(days=1))
        with self.assertRaisesRegex(SwingUniverseBatchError, "cutoff"):
            replace(batch, cutoff=CUTOFF + timedelta(minutes=1))
        with self.assertRaisesRegex(SwingUniverseBatchError, "readiness"):
            replace(batch, readiness=ReferenceReadiness.POINT_IN_TIME_VERIFIED)

    def test_rejects_listing_invalid_on_signal_session(self) -> None:
        # UniverseSnapshot itself already guarantees every entry's listing is
        # valid on market_session, so an entry invalid on the shared session
        # cannot be constructed via the production UniverseSnapshot.create()
        # constructor. This proves the invariant "no invalid-listing subject
        # enters a batch" holds end-to-end -- caught by UniverseSnapshot's own
        # layer here, and independently re-checked inside the batch itself.
        entries = list(self.current_universe.entries)
        for index, entry in enumerate(entries):
            if entry.listing.instrument_id == INSTRUMENT_A:
                entries[index] = replace(
                    entry, listing=replace(entry.listing, valid_from=SESSION + timedelta(days=1))
                )
        with self.assertRaises(Exception):
            UniverseSnapshot.create(
                exchange=self.current_universe.exchange,
                segment=self.current_universe.segment,
                market_session=self.current_universe.market_session,
                cutoff=self.current_universe.cutoff,
                calendar_snapshot_id=self.current_universe.calendar_snapshot_id,
                universe_rules_version=self.current_universe.universe_rules_version,
                selection_key=self.current_universe.selection_key,
                scoped_source_row_ids=self.current_universe.scoped_source_row_ids,
                security_master_snapshot_ids=self.current_universe.security_master_snapshot_ids,
                eligibility_snapshot_ids=self.current_universe.eligibility_snapshot_ids,
                liquidity_snapshot_ids=self.current_universe.liquidity_snapshot_ids,
                readiness=self.current_universe.readiness,
                entries=tuple(entries),
            )

    def test_promotion_and_tick_ids_may_differ_while_shared_lineage_stays_exact(self) -> None:
        self.assertNotEqual(
            self.assembly_a.promotion_decision_id, self.assembly_b.promotion_decision_id
        )
        self.assertNotEqual(
            self.assembly_a.signal_materialization.history.tick_evidence_id,
            self.assembly_b.signal_materialization.history.tick_evidence_id,
        )
        self.assertEqual(
            self.assembly_a.stable_identity_snapshot_id,
            self.assembly_b.stable_identity_snapshot_id,
        )
        self.assertEqual(self.assembly_a.raw_artifact_ids, self.assembly_b.raw_artifact_ids)
        self.assertEqual(
            self.assembly_a.universe_snapshot_ids, self.assembly_b.universe_snapshot_ids
        )
        self.assertEqual(
            self.assembly_a.adjusted_history.corporate_action_snapshot_id,
            self.assembly_b.adjusted_history.corporate_action_snapshot_id,
        )
        self._batch()

    def test_rejects_wrong_shared_stable_identity_lineage(self) -> None:
        mismatched_b = self._assemble(
            instrument_id=INSTRUMENT_B,
            listing_id=LISTING_B,
            tick_source_id=TICK_SOURCE_B,
            identity_id=ALT_IDENTITY_SNAPSHOT_ID,
        )
        with self.assertRaisesRegex(SwingUniverseBatchError, "share exact engine lineage"):
            self._batch(assemblies=(self.assembly_a, mismatched_b))

    def test_rejects_wrong_shared_raw_artifact_lineage(self) -> None:
        wrong_raw_id = "ab" * 32
        wrong_promotion = promotion(
            wrong_raw_id,
            self.current_universe.snapshot_id,
            self.actions.snapshot_id,
            self.assembly_b.signal_materialization.history.tick_evidence_id,
            IDENTITY_SNAPSHOT_ID,
        )
        mismatched_b = replace(
            self.assembly_b,
            raw_artifact_ids=(wrong_raw_id,),
            promotion=wrong_promotion,
            promotion_decision_id=wrong_promotion.decision_id,
        )
        mismatched_b.verify_content_identity()

        with self.assertRaisesRegex(SwingUniverseBatchError, "share exact engine lineage"):
            self._batch(assemblies=(self.assembly_a, mismatched_b))

    def test_rejects_wrong_shared_corporate_action_lineage(self) -> None:
        alt_actions = _actions(
            source_id=ALT_CORPORATE_SOURCE_ID, cutoff=VALIDATED + timedelta(minutes=6)
        )
        mismatched_b = self._assemble(
            instrument_id=INSTRUMENT_B,
            listing_id=LISTING_B,
            tick_source_id=TICK_SOURCE_B,
            actions_value=alt_actions,
        )
        with self.assertRaisesRegex(SwingUniverseBatchError, "share exact engine lineage"):
            self._batch(assemblies=(self.assembly_a, mismatched_b))

    def test_rejects_wrong_shared_universe_history_lineage(self) -> None:
        other_bar = self.raw.bars[0]
        alt_universe = _multi_universe(
            second_symbol=other_bar.symbol,
            second_series=other_bar.series,
            second_isin=other_bar.validated_isin,
            cutoff=CUTOFF - timedelta(minutes=1),
        )
        mismatched_b = self._assemble(
            instrument_id=INSTRUMENT_B,
            listing_id=LISTING_B,
            tick_source_id=TICK_SOURCE_B,
            universe_value=alt_universe,
        )
        with self.assertRaises((SwingUniverseBatchError, ValueError)):
            self._batch(assemblies=(self.assembly_a, mismatched_b))

    def test_rejects_collection_only_current_universe(self) -> None:
        with self.assertRaisesRegex(SwingUniverseBatchError, "collection-only"):
            assemble_universe_input_batch(
                current_universe=_collection_only_universe(),
                assemblies=(),
            )

    def test_direct_construction_cannot_forge_counts(self) -> None:
        batch = self._batch()
        with self.assertRaisesRegex(SwingUniverseBatchError, "counts are inconsistent"):
            replace(batch, scoped_subject_count=batch.scoped_subject_count + 1)
        with self.assertRaisesRegex(SwingUniverseBatchError, "counts are inconsistent"):
            replace(batch, actionable_subject_count=batch.actionable_subject_count + 1)
        with self.assertRaisesRegex(SwingUniverseBatchError, "counts are inconsistent"):
            replace(batch, veto_subject_count=batch.veto_subject_count + 1)

    def test_direct_construction_cannot_forge_duplicated_snapshot_or_readiness_fields(
        self,
    ) -> None:
        batch = self._batch()
        with self.assertRaisesRegex(
            SwingUniverseBatchError, "universe snapshot ID does not match"
        ):
            replace(batch, universe_snapshot_id="f" * 64)

    def test_direct_construction_cannot_forge_veto_reasons_or_disposition(self) -> None:
        batch = self._batch()
        first_veto = batch.vetoes[0]
        forged_disposition = (
            UniverseDisposition.WATCH_ONLY
            if first_veto.disposition is not UniverseDisposition.WATCH_ONLY
            else UniverseDisposition.EXCLUDED
        )
        forged_veto = replace(first_veto, disposition=forged_disposition)
        forged_vetoes = tuple(
            forged_veto if value is first_veto else value for value in batch.vetoes
        )
        with self.assertRaisesRegex(SwingUniverseBatchError, "veto content differs"):
            replace(batch, vetoes=forged_vetoes)

    def test_direct_construction_cannot_forge_batch_id(self) -> None:
        batch = self._batch()
        with self.assertRaises(ValueError):
            replace(batch, batch_id="f" * 64)

    def test_verify_content_identity_detects_nested_universe_mutation(self) -> None:
        batch = self._batch()
        original_batch_id = batch.batch_id
        object.__setattr__(batch.current_universe, "cutoff", CUTOFF + timedelta(minutes=1))

        self.assertEqual(batch.batch_id, original_batch_id)
        with self.assertRaises(Exception):
            batch.verify_content_identity()

    def test_verify_content_identity_detects_nested_assembly_promotion_mutation(self) -> None:
        batch = self._batch()
        original_batch_id = batch.batch_id
        raw_evidence = batch.assemblies[0].promotion.evidence_for(
            batch.assemblies[0].promotion.evidence[0].capability
        )
        object.__setattr__(raw_evidence, "complete", not raw_evidence.complete)

        self.assertEqual(batch.batch_id, original_batch_id)
        with self.assertRaises(Exception):
            batch.verify_content_identity()

    def test_verify_content_identity_detects_nested_veto_mutation(self) -> None:
        batch = self._batch()
        original_batch_id = batch.batch_id
        object.__setattr__(batch.vetoes[0], "reason_codes", ("FORGED_REASON",))

        self.assertEqual(batch.batch_id, original_batch_id)
        with self.assertRaisesRegex(Exception, "content identity"):
            batch.verify_content_identity()

    def test_verify_content_identity_detects_coverage_graph_mutation(self) -> None:
        batch = self._batch()
        original_batch_id = batch.batch_id
        object.__setattr__(batch, "assemblies", (batch.assemblies[0],))

        self.assertEqual(batch.batch_id, original_batch_id)
        with self.assertRaises(SwingUniverseBatchError):
            batch.verify_content_identity()

    def test_synthetic_batch_is_never_actionable(self) -> None:
        batch = self._batch()
        self.assertIs(batch.readiness, ReferenceReadiness.SYNTHETIC_TEST)
        self.assertFalse(batch.actionable)


if __name__ == "__main__":
    unittest.main()
