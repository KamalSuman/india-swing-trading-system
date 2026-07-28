from __future__ import annotations

import dataclasses
import hashlib
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from india_swing.identity_decisions import PromotedIdentityAdjudicationService
from india_swing.identity_registry.promoted_intake import PromotedIdentityIntakeService
from india_swing.market_data.promoted_session_frame import (
    PromotedSessionMarketDataEntry,
    PromotedSessionMarketDataFrameService,
    VerifiedPromotedSessionMarketDataFrame,
)
from india_swing.reference.models import ReferenceReadiness
from india_swing.tick_sizes.models import CollectedTickSizeObservation
from india_swing.tick_sizes.promoted_session import (
    PromotedSessionTickEntry,
    PromotedSessionTickSizeError,
    PromotedSessionTickSizeService,
    PromotedSessionTickStatus,
    VerifiedPromotedSessionTickSnapshot,
)
from india_swing.universe.promoted_identity import PromotedIdentitySessionUniverseService
from tests.test_promoted_identity_session_universe import (
    D1,
    D2,
    ADJUDICATION_CUTOFF,
    INTAKE_CUTOFF,
    SESSION_CUTOFF,
    build_calendar,
    build_promotion,
    happy_path_fixture,
    security_row,
)
from tests.test_promoted_session_market_data import (
    BUILT_AT,
    FRAME_CUTOFF,
    _bar,
    _corpus,
    _happy_frame,
)


UTC = timezone.utc
TICK_CUTOFF = FRAME_CUTOFF + timedelta(hours=1)


def _kwargs_from(snapshot: VerifiedPromotedSessionTickSnapshot) -> dict[str, object]:
    return {field.name: getattr(snapshot, field.name) for field in dataclasses.fields(snapshot)}


def _minimal_frame(
    root: Path,
    *,
    d2_row_overrides: dict[str, str] | None = None,
) -> VerifiedPromotedSessionMarketDataFrame:
    """A single-retained-row frame with full control over the D2 source row,
    used for tests that need to tamper with the raw security-master row
    (e.g. a populated reserved TickSz column) that the shared 5-row
    happy_path_fixture in test_promoted_identity_session_universe.py does
    not expose a way to construct."""

    row_overrides = {
        "FinInstrmId": "60001",
        "TckrSymb": "TESTCO",
        "ISIN": "INE001A01036",
        "BidIntrvl": "15",
    }
    row_overrides.update(d2_row_overrides or {})
    p1 = build_promotion(
        root,
        report_date=D1,
        generation=910,
        rows=[security_row(FinInstrmId="60001", TckrSymb="TESTCO", ISIN="INE001A01036")],
        first_seen=datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
        validated=datetime(2026, 7, 15, 12, 0, 2, tzinfo=UTC),
    )
    p2 = build_promotion(
        root,
        report_date=D2,
        generation=911,
        rows=[security_row(**row_overrides)],
        first_seen=datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
        validated=datetime(2026, 7, 16, 12, 0, 2, tzinfo=UTC),
    )
    intake = PromotedIdentityIntakeService().materialize(
        promotions=(p1, p2), expected_report_dates=(D1, D2), cutoff=INTAKE_CUTOFF
    )
    adjudication = PromotedIdentityAdjudicationService().materialize(
        intake=intake, evidence_artifacts=(), review_bundles=(), cutoff=ADJUDICATION_CUTOFF
    )
    calendar = build_calendar(root)
    universe = PromotedIdentitySessionUniverseService().materialize(
        adjudication=adjudication, calendar=calendar, market_session=D2, cutoff=SESSION_CUTOFF
    )
    testco_entry = next(value for value in universe.entries if value.symbol == "TESTCO")
    bar = _bar(testco_entry, market_session=D2, label="testco")
    index, partition = _corpus(market_session=D2, bars=(bar,))
    return PromotedSessionMarketDataFrameService().materialize(
        universe=universe,
        corpus_index=index,
        partition=partition,
        cutoff=BUILT_AT + timedelta(hours=2),
    )


def _snapshot_from_minimal(root: Path, **kwargs) -> VerifiedPromotedSessionTickSnapshot:
    frame = _minimal_frame(root, **kwargs)
    return PromotedSessionTickSizeService().materialize(
        frame=frame, cutoff=BUILT_AT + timedelta(hours=3)
    )


class PromotedSessionTickSizeAcceptanceTests(unittest.TestCase):
    def _snapshot(self, tmp: str) -> VerifiedPromotedSessionTickSnapshot:
        root = Path(tmp)
        _, _, _, frame = _happy_frame(root)
        return PromotedSessionTickSizeService().materialize(frame=frame, cutoff=TICK_CUTOFF)

    def test_every_frame_row_covered_exactly_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, _, frame = _happy_frame(root)
            snapshot = self._snapshot(tmp)
            self.assertEqual(
                {entry.source_record_id for entry in snapshot.entries},
                {entry.source_record_id for entry in frame.entries},
            )
            self.assertEqual(
                snapshot.entries,
                tuple(sorted(snapshot.entries, key=lambda value: value.source_record_id)),
            )

    def test_resolved_and_unresolved_equities_both_retain_observations(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = self._snapshot(tmp)
            by_symbol = {
                entry.frame_entry.universe_entry.symbol: entry for entry in snapshot.entries
            }
            resolved = by_symbol["RELIANCE"]
            self.assertIs(
                resolved.status,
                PromotedSessionTickStatus.TICK_OBSERVED_IDENTITY_RESOLVED_COLLECTION_ONLY,
            )
            self.assertIsNotNone(resolved.observation)
            unresolved = by_symbol["SMALL1"]
            self.assertIs(
                unresolved.status, PromotedSessionTickStatus.TICK_OBSERVED_IDENTITY_UNRESOLVED
            )
            self.assertIsNotNone(unresolved.observation)
            self.assertFalse(resolved.effective_interval_verified)
            self.assertFalse(unresolved.effective_interval_verified)

    def test_deleted_unresolved_equity_still_retains_observation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = self._snapshot(tmp)
            deleted = next(
                entry
                for entry in snapshot.entries
                if entry.frame_entry.universe_entry.symbol == "DELISTD"
            )
            self.assertIsNotNone(deleted.observation)
            self.assertIs(
                deleted.status, PromotedSessionTickStatus.TICK_OBSERVED_IDENTITY_UNRESOLVED
            )

    def test_excluded_rows_never_receive_an_observation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = self._snapshot(tmp)
            excluded_statuses = {
                PromotedSessionTickStatus.TICK_SOURCE_EXCLUDED_NON_EQUITY,
                PromotedSessionTickStatus.TICK_SOURCE_EXCLUDED_TEST_SECURITY,
            }
            excluded_entries = [
                entry for entry in snapshot.entries if entry.status in excluded_statuses
            ]
            self.assertEqual(len(excluded_entries), 2)
            for entry in excluded_entries:
                self.assertIsNone(entry.observation)
                self.assertIn("SOURCE_EXCLUDED_NO_TICK_AUTHORITY", entry.reason_codes)

    def test_mandatory_reason_codes_present(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = self._snapshot(tmp)
            for entry in snapshot.entries:
                if entry.observation is not None:
                    self.assertIn(
                        "SINGLE_SESSION_TICK_OBSERVATION_NOT_EFFECTIVE_INTERVAL",
                        entry.reason_codes,
                    )
                    self.assertIn("COLLECTION_ONLY_TICK_SIZE_EVIDENCE", entry.reason_codes)

    def test_all_safety_flags_false_regardless_of_identity_coverage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = self._snapshot(tmp)
            self.assertIs(snapshot.readiness, ReferenceReadiness.COLLECTION_ONLY)
            self.assertFalse(snapshot.actionable)
            self.assertFalse(snapshot.training_eligible)
            self.assertFalse(snapshot.alert_eligible)
            self.assertFalse(snapshot.execution_eligible)
            for entry in snapshot.entries:
                self.assertFalse(entry.effective_interval_verified)

    def test_bid_interval_matches_source_row_exactly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = self._snapshot(tmp)
            reliance = next(
                entry
                for entry in snapshot.entries
                if entry.frame_entry.universe_entry.symbol == "RELIANCE"
            )
            self.assertEqual(reliance.observation.bid_interval_paise, 5)
            self.assertEqual(reliance.observation.tick_size_rupees, Decimal("0.05"))

    def test_verify_content_identity_passes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = self._snapshot(tmp)
            snapshot.verify_content_identity()


class PromotedSessionTickSizeBarIndependenceTests(unittest.TestCase):
    def test_bar_presence_absence_never_changes_tick_observations(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            root_a, root_b = Path(tmp_a), Path(tmp_b)
            _, _, _, frame_with_bars = _happy_frame(root_a)
            snapshot_with_bars = PromotedSessionTickSizeService().materialize(
                frame=frame_with_bars, cutoff=TICK_CUTOFF
            )

            _, _, _, _, universe_b = happy_path_fixture(root_b)
            # A corpus partition requires at least one bar, so bind the only
            # bar to a fully orphaned lane (absent from the universe) rather
            # than to RELIANCE/SMALL1 -- proving their own tick observations
            # do not depend on whether their own bar exists.
            reliance_entry = next(
                value for value in universe_b.entries if value.symbol == "RELIANCE"
            )
            orphan_proxy = dataclasses.replace(
                reliance_entry, symbol="ORPHANONLY", validated_isin="INE999A01019"
            )
            orphan_bar = _bar(orphan_proxy, market_session=universe_b.market_session, label="orphan-only")
            index_b, partition_b = _corpus(
                market_session=universe_b.market_session, bars=(orphan_bar,)
            )
            frame_without_own_bars = PromotedSessionMarketDataFrameService().materialize(
                universe=universe_b,
                corpus_index=index_b,
                partition=partition_b,
                cutoff=FRAME_CUTOFF,
            )
            snapshot_without_bars = PromotedSessionTickSizeService().materialize(
                frame=frame_without_own_bars, cutoff=TICK_CUTOFF
            )

            observed_a = {
                entry.frame_entry.universe_entry.symbol: (
                    None if entry.observation is None else entry.observation.bid_interval_paise
                )
                for entry in snapshot_with_bars.entries
            }
            observed_b = {
                entry.frame_entry.universe_entry.symbol: (
                    None if entry.observation is None else entry.observation.bid_interval_paise
                )
                for entry in snapshot_without_bars.entries
            }
            self.assertEqual(observed_a, observed_b)
            self.assertTrue(frame_with_bars.orphan_bars)
            self.assertTrue(frame_without_own_bars.orphan_bars)
            self.assertTrue(
                all(entry.bar is None for entry in frame_without_own_bars.entries)
            )


class PromotedSessionTickSizeReservedFieldTests(unittest.TestCase):
    def test_populated_reserved_tick_size_field_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            frame = _minimal_frame(root, d2_row_overrides={"TickSz": "0.05"})
            with self.assertRaises(PromotedSessionTickSizeError):
                PromotedSessionTickSizeService().materialize(
                    frame=frame, cutoff=BUILT_AT + timedelta(hours=3)
                )

    def test_empty_reserved_field_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = _snapshot_from_minimal(Path(tmp))
            testco = next(
                entry
                for entry in snapshot.entries
                if entry.frame_entry.universe_entry.symbol == "TESTCO"
            )
            self.assertIsNotNone(testco.observation)
            self.assertEqual(testco.observation.bid_interval_paise, 15)


class PromotedSessionTickSizeRejectionTests(unittest.TestCase):
    def test_wrong_type_frame_is_rejected(self) -> None:
        with self.assertRaises(PromotedSessionTickSizeError):
            PromotedSessionTickSizeService().materialize(
                frame="not-a-frame", cutoff=TICK_CUTOFF  # type: ignore[arg-type]
            )

    def test_naive_cutoff_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, _, frame = _happy_frame(root)
            with self.assertRaises(PromotedSessionTickSizeError):
                PromotedSessionTickSizeService().materialize(
                    frame=frame, cutoff=TICK_CUTOFF.replace(tzinfo=None)
                )

    def test_datetime_subclass_cutoff_is_rejected(self) -> None:
        class DatetimeSubclass(datetime):
            pass

        with tempfile.TemporaryDirectory() as tmp:
            _, _, _, frame = _happy_frame(Path(tmp))
            cutoff = DatetimeSubclass(
                2026,
                7,
                17,
                12,
                0,
                tzinfo=UTC,
            )
            with self.assertRaises(PromotedSessionTickSizeError):
                PromotedSessionTickSizeService().materialize(
                    frame=frame,
                    cutoff=cutoff,
                )

    def test_cutoff_before_frame_knowledge_time_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, _, frame = _happy_frame(root)
            with self.assertRaises(PromotedSessionTickSizeError):
                PromotedSessionTickSizeService().materialize(
                    frame=frame, cutoff=frame.knowledge_time - timedelta(days=1)
                )

    def test_frame_actionable_mutation_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, _, frame = _happy_frame(root)
            object.__setattr__(frame, "actionable", True)
            with self.assertRaises(PromotedSessionTickSizeError):
                PromotedSessionTickSizeService().materialize(frame=frame, cutoff=TICK_CUTOFF)

    def test_frame_training_eligible_mutation_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, _, frame = _happy_frame(root)
            object.__setattr__(frame, "training_eligible", True)
            with self.assertRaises(PromotedSessionTickSizeError):
                PromotedSessionTickSizeService().materialize(frame=frame, cutoff=TICK_CUTOFF)

    def test_frame_alert_eligible_mutation_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, _, frame = _happy_frame(root)
            object.__setattr__(frame, "alert_eligible", True)
            with self.assertRaises(PromotedSessionTickSizeError):
                PromotedSessionTickSizeService().materialize(frame=frame, cutoff=TICK_CUTOFF)

    def test_frame_execution_eligible_mutation_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, _, frame = _happy_frame(root)
            object.__setattr__(frame, "execution_eligible", True)
            with self.assertRaises(PromotedSessionTickSizeError):
                PromotedSessionTickSizeService().materialize(frame=frame, cutoff=TICK_CUTOFF)

    def test_future_promotion_knowledge_relative_to_frame_is_rejected(self) -> None:
        # Mutate the frame's own retained knowledge_time backward so that the
        # (unmutatable, independently re-derived) selected promotion's own
        # knowledge_time now appears to be "in the future" relative to it,
        # and confirm materialize() still rejects via the cutoff-vs-
        # knowledge-time check rather than silently using stale knowledge.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, _, frame = _happy_frame(root)
            with self.assertRaises(PromotedSessionTickSizeError):
                PromotedSessionTickSizeService().materialize(
                    frame=frame,
                    cutoff=frame.universe.adjudication.intake.promotions[0].knowledge_time
                    - timedelta(days=365),
                )


class PromotedSessionTickSizeWhiteBoxLineageTests(unittest.TestCase):
    """Selection/lineage checks required by the architecture (duplicate
    selected promotion, missing/mismatched source record) are structurally
    unreachable through the public service: the frame's own
    verify_content_identity() (always called first) already independently
    re-verifies its retained universe, which itself already guarantees the
    selected promotion is unique and lineage-consistent. These tests import
    the private _build_tick_snapshot_facts/_select_promotion/_build_tick_entry
    helpers directly against a hand-mutated (object.__setattr__) fixture,
    bypassing that outer replay, to prove each check is still genuinely
    load-bearing defense-in-depth."""

    def test_missing_source_record_for_retained_row_is_rejected(self) -> None:
        from india_swing.tick_sizes.promoted_session import (
            _build_tick_entry,
            _select_promotion,
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, _, frame = _happy_frame(root)
            promotion = _select_promotion(frame)
            frame_entry = next(
                value
                for value in frame.entries
                if value.universe_entry.symbol == "RELIANCE"
            )
            with self.assertRaises(PromotedSessionTickSizeError):
                _build_tick_entry(
                    frame_entry,
                    {},
                    promotion,
                    frame.market_session,
                )

    def test_mismatched_retained_source_fields_are_rejected(self) -> None:
        from india_swing.tick_sizes.promoted_session import (
            _build_tick_entry,
            _select_promotion,
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, _, frame = _happy_frame(root)
            promotion = _select_promotion(frame)
            frame_entry = next(
                value
                for value in frame.entries
                if value.universe_entry.symbol == "RELIANCE"
            )
            source_record = next(
                record
                for record in promotion.artifact.parsed.records
                if record.ticker_symbol == "RELIANCE"
            )
            mutations = (
                ("financial_instrument_id", 999999),
                ("ticker_symbol", "WRONG"),
                ("security_series", "BE"),
                ("validated_isin", "INE999A01019"),
            )
            for field_name, replacement in mutations:
                with self.subTest(field_name=field_name):
                    original = getattr(source_record, field_name)
                    object.__setattr__(source_record, field_name, replacement)
                    try:
                        with self.assertRaises(PromotedSessionTickSizeError):
                            _build_tick_entry(
                                frame_entry,
                                {source_record.source_record_id: source_record},
                                promotion,
                                frame.market_session,
                            )
                    finally:
                        object.__setattr__(source_record, field_name, original)

    def test_mismatched_frame_source_artifact_or_manifest_is_rejected(self) -> None:
        from india_swing.tick_sizes.promoted_session import (
            _build_tick_entry,
            _select_promotion,
        )

        with tempfile.TemporaryDirectory() as tmp:
            _, _, _, frame = _happy_frame(Path(tmp))
            promotion = _select_promotion(frame)
            frame_entry = next(
                value
                for value in frame.entries
                if value.universe_entry.symbol == "RELIANCE"
            )
            source_record = next(
                record
                for record in promotion.artifact.parsed.records
                if record.ticker_symbol == "RELIANCE"
            )
            for field_name in ("source_artifact_id", "source_manifest_id"):
                with self.subTest(field_name=field_name):
                    original = getattr(frame_entry.universe_entry, field_name)
                    object.__setattr__(frame_entry.universe_entry, field_name, "0" * 64)
                    try:
                        with self.assertRaises(PromotedSessionTickSizeError):
                            _build_tick_entry(
                                frame_entry,
                                {source_record.source_record_id: source_record},
                                promotion,
                                frame.market_session,
                            )
                    finally:
                        object.__setattr__(
                            frame_entry.universe_entry,
                            field_name,
                            original,
                        )

    def test_duplicate_selected_promotion_id_is_rejected(self) -> None:
        from india_swing.tick_sizes.promoted_session import _select_promotion

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, _, frame = _happy_frame(root)
            promotions = frame.universe.adjudication.intake.promotions
            selected = next(
                value
                for value in promotions
                if value.promotion_id == frame.universe.selected_promotion_id
            )
            duplicated = promotions + (selected,)
            object.__setattr__(frame.universe.adjudication.intake, "promotions", duplicated)
            with self.assertRaises(PromotedSessionTickSizeError):
                _select_promotion(frame)

    def test_selected_promotion_is_independently_replayed(self) -> None:
        from india_swing.tick_sizes.promoted_session import _select_promotion

        with tempfile.TemporaryDirectory() as tmp:
            _, _, _, frame = _happy_frame(Path(tmp))
            promotion = next(
                value
                for value in frame.universe.adjudication.intake.promotions
                if value.promotion_id == frame.universe.selected_promotion_id
            )
            original = promotion.join.join_id
            object.__setattr__(promotion.join, "join_id", "0" * 64)
            try:
                with self.assertRaises(PromotedSessionTickSizeError):
                    _select_promotion(frame)
            finally:
                object.__setattr__(promotion.join, "join_id", original)


class PromotedSessionTickSizeDirectConstructionMismatchTests(unittest.TestCase):
    def _snapshot(self, tmp: str) -> VerifiedPromotedSessionTickSnapshot:
        root = Path(tmp)
        _, _, _, frame = _happy_frame(root)
        return PromotedSessionTickSizeService().materialize(frame=frame, cutoff=TICK_CUTOFF)

    def test_replacing_schema_version_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = self._snapshot(tmp)
            kwargs = _kwargs_from(snapshot)
            kwargs["schema_version"] = "promoted-session-tick-size/v2"
            with self.assertRaises(PromotedSessionTickSizeError):
                VerifiedPromotedSessionTickSnapshot(**kwargs)

    def test_replacing_policy_version_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = self._snapshot(tmp)
            kwargs = _kwargs_from(snapshot)
            kwargs["policy_version"] = "different-policy/v1"
            with self.assertRaises(PromotedSessionTickSizeError):
                VerifiedPromotedSessionTickSnapshot(**kwargs)

    def test_replacing_frame_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            snapshot = self._snapshot(tmp_a)
            other_frame = _minimal_frame(Path(tmp_b))
            kwargs = _kwargs_from(snapshot)
            kwargs["frame"] = other_frame
            with self.assertRaises(PromotedSessionTickSizeError):
                VerifiedPromotedSessionTickSnapshot(**kwargs)

    def test_replacing_market_session_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = self._snapshot(tmp)
            kwargs = _kwargs_from(snapshot)
            kwargs["market_session"] = D1
            with self.assertRaises(PromotedSessionTickSizeError):
                VerifiedPromotedSessionTickSnapshot(**kwargs)

    def test_replacing_cutoff_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = self._snapshot(tmp)
            kwargs = _kwargs_from(snapshot)
            kwargs["cutoff"] = snapshot.cutoff + timedelta(days=1)
            with self.assertRaises(PromotedSessionTickSizeError):
                VerifiedPromotedSessionTickSnapshot(**kwargs)

    def test_replacing_entries_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = self._snapshot(tmp)
            kwargs = _kwargs_from(snapshot)
            kwargs["entries"] = snapshot.entries[:-1]
            with self.assertRaises(PromotedSessionTickSizeError):
                VerifiedPromotedSessionTickSnapshot(**kwargs)

    def test_replacing_status_counts_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = self._snapshot(tmp)
            kwargs = _kwargs_from(snapshot)
            kwargs["status_counts"] = ()
            with self.assertRaises(PromotedSessionTickSizeError):
                VerifiedPromotedSessionTickSnapshot(**kwargs)

    def test_replacing_reason_counts_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = self._snapshot(tmp)
            kwargs = _kwargs_from(snapshot)
            kwargs["reason_counts"] = ()
            with self.assertRaises(PromotedSessionTickSizeError):
                VerifiedPromotedSessionTickSnapshot(**kwargs)

    def test_replacing_readiness_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = self._snapshot(tmp)
            kwargs = _kwargs_from(snapshot)
            kwargs["readiness"] = ReferenceReadiness.POINT_IN_TIME_VERIFIED
            with self.assertRaises(PromotedSessionTickSizeError):
                VerifiedPromotedSessionTickSnapshot(**kwargs)

    def test_replacing_actionable_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = self._snapshot(tmp)
            kwargs = _kwargs_from(snapshot)
            kwargs["actionable"] = True
            with self.assertRaises(PromotedSessionTickSizeError):
                VerifiedPromotedSessionTickSnapshot(**kwargs)

    def test_replacing_training_eligible_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = self._snapshot(tmp)
            kwargs = _kwargs_from(snapshot)
            kwargs["training_eligible"] = True
            with self.assertRaises(PromotedSessionTickSizeError):
                VerifiedPromotedSessionTickSnapshot(**kwargs)

    def test_replacing_alert_eligible_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = self._snapshot(tmp)
            kwargs = _kwargs_from(snapshot)
            kwargs["alert_eligible"] = True
            with self.assertRaises(PromotedSessionTickSizeError):
                VerifiedPromotedSessionTickSnapshot(**kwargs)

    def test_replacing_execution_eligible_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = self._snapshot(tmp)
            kwargs = _kwargs_from(snapshot)
            kwargs["execution_eligible"] = True
            with self.assertRaises(PromotedSessionTickSizeError):
                VerifiedPromotedSessionTickSnapshot(**kwargs)

    def test_replacing_snapshot_id_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = self._snapshot(tmp)
            kwargs = _kwargs_from(snapshot)
            kwargs["snapshot_id"] = hashlib.sha256(b"different").hexdigest()
            with self.assertRaises(PromotedSessionTickSizeError):
                VerifiedPromotedSessionTickSnapshot(**kwargs)

    def test_replacing_selected_promotion_id_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = self._snapshot(tmp)
            kwargs = _kwargs_from(snapshot)
            kwargs["selected_promotion_id"] = "0" * 64
            with self.assertRaises(PromotedSessionTickSizeError):
                VerifiedPromotedSessionTickSnapshot(**kwargs)


class PromotedSessionTickSizeMutationTests(unittest.TestCase):
    def _snapshot(self, tmp: str) -> VerifiedPromotedSessionTickSnapshot:
        root = Path(tmp)
        _, _, _, frame = _happy_frame(root)
        return PromotedSessionTickSizeService().materialize(frame=frame, cutoff=TICK_CUTOFF)

    def test_mutating_top_level_snapshot_id_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = self._snapshot(tmp)
            object.__setattr__(snapshot, "snapshot_id", "0" * 64)
            with self.assertRaises(PromotedSessionTickSizeError):
                snapshot.verify_content_identity()

    def test_mutating_nested_frame_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = self._snapshot(tmp)
            object.__setattr__(snapshot.frame, "frame_id", "0" * 64)
            with self.assertRaises(PromotedSessionTickSizeError):
                snapshot.verify_content_identity()

    def test_mutating_nested_universe_entry_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = self._snapshot(tmp)
            entry = snapshot.frame.entries[0].universe_entry
            object.__setattr__(entry, "symbol", "TAMPERED")
            with self.assertRaises(PromotedSessionTickSizeError):
                snapshot.verify_content_identity()

    def test_mutating_entry_observation_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = self._snapshot(tmp)
            entry = next(value for value in snapshot.entries if value.observation is not None)
            object.__setattr__(entry.observation, "bid_interval_paise", 999)
            with self.assertRaises(PromotedSessionTickSizeError):
                snapshot.verify_content_identity()

    def test_mutating_entry_status_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = self._snapshot(tmp)
            entry = next(value for value in snapshot.entries if value.observation is not None)
            other_status = (
                PromotedSessionTickStatus.TICK_OBSERVED_IDENTITY_UNRESOLVED
                if entry.status
                is PromotedSessionTickStatus.TICK_OBSERVED_IDENTITY_RESOLVED_COLLECTION_ONLY
                else PromotedSessionTickStatus.TICK_OBSERVED_IDENTITY_RESOLVED_COLLECTION_ONLY
            )
            object.__setattr__(entry, "status", other_status)
            with self.assertRaises(PromotedSessionTickSizeError):
                snapshot.verify_content_identity()


class _EvilEq:
    def __init__(self, secret: str) -> None:
        self._secret = secret

    def __eq__(self, other: object) -> bool:
        raise RuntimeError(f"secret-leak-{self._secret}")

    def __hash__(self) -> int:
        return 0


class _ComparisonBoundaryBaseException(BaseException):
    pass


class _EvilEqBaseException:
    def __eq__(self, other: object) -> bool:
        raise _ComparisonBoundaryBaseException("comparison-boundary-control")

    def __hash__(self) -> int:
        return 0


class PromotedSessionTickSizeComparisonBoundaryTests(unittest.TestCase):
    def _snapshot(self, tmp: str) -> VerifiedPromotedSessionTickSnapshot:
        root = Path(tmp)
        _, _, _, frame = _happy_frame(root)
        return PromotedSessionTickSizeService().materialize(frame=frame, cutoff=TICK_CUTOFF)

    def _assert_sanitized(self, secret: str, exc: BaseException) -> None:
        self.assertIsInstance(exc, PromotedSessionTickSizeError)
        message = str(exc)
        self.assertNotIn("RuntimeError", message)
        self.assertNotIn(secret, message)

    def test_malicious_universe_entry_equality_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = self._snapshot(tmp)
            secret = "universe-entry-secret-1a2b"
            entry = snapshot.frame.entries[0].universe_entry
            object.__setattr__(entry, "symbol", _EvilEq(secret))
            with self.assertRaises(PromotedSessionTickSizeError) as ctx:
                snapshot.verify_content_identity()
            self._assert_sanitized(secret, ctx.exception)

    def test_malicious_observation_equality_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = self._snapshot(tmp)
            secret = "observation-secret-3c4d"
            entry = next(value for value in snapshot.entries if value.observation is not None)
            object.__setattr__(entry.observation, "symbol", _EvilEq(secret))
            with self.assertRaises(PromotedSessionTickSizeError) as ctx:
                snapshot.verify_content_identity()
            self._assert_sanitized(secret, ctx.exception)

    def test_base_exception_from_equality_is_not_swallowed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = self._snapshot(tmp)
            entry = snapshot.frame.entries[0].universe_entry
            object.__setattr__(entry, "symbol", _EvilEqBaseException())
            with self.assertRaises(_ComparisonBoundaryBaseException):
                snapshot.verify_content_identity()


class PromotedSessionTickSizeSubclassImpostorTests(unittest.TestCase):
    def test_snapshot_subclass_is_rejected(self) -> None:
        class _SnapshotSubclass(VerifiedPromotedSessionTickSnapshot):
            pass

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, _, frame = _happy_frame(root)
            snapshot = PromotedSessionTickSizeService().materialize(
                frame=frame, cutoff=TICK_CUTOFF
            )
            with self.assertRaises(PromotedSessionTickSizeError):
                _SnapshotSubclass(**_kwargs_from(snapshot))

    def test_wrong_type_entries_tuple_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, _, frame = _happy_frame(root)
            snapshot = PromotedSessionTickSizeService().materialize(
                frame=frame, cutoff=TICK_CUTOFF
            )
            kwargs = _kwargs_from(snapshot)
            kwargs["entries"] = list(snapshot.entries)  # type: ignore[assignment]
            with self.assertRaises(PromotedSessionTickSizeError):
                VerifiedPromotedSessionTickSnapshot(**kwargs)


class PromotedSessionTickSizeContentIdCompletenessTests(unittest.TestCase):
    def test_different_bid_interval_changes_snapshot_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            snapshot_a = _snapshot_from_minimal(Path(tmp_a))
            snapshot_b = _snapshot_from_minimal(Path(tmp_b), d2_row_overrides={"BidIntrvl": "25"})
            self.assertNotEqual(snapshot_a.snapshot_id, snapshot_b.snapshot_id)

    def test_different_cutoff_changes_snapshot_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            root_a, root_b = Path(tmp_a), Path(tmp_b)
            frame_a = _minimal_frame(root_a)
            frame_b = _minimal_frame(root_b)
            snapshot_a = PromotedSessionTickSizeService().materialize(
                frame=frame_a, cutoff=BUILT_AT + timedelta(hours=3)
            )
            snapshot_b = PromotedSessionTickSizeService().materialize(
                frame=frame_b, cutoff=BUILT_AT + timedelta(hours=4)
            )
            self.assertNotEqual(snapshot_a.snapshot_id, snapshot_b.snapshot_id)

    def test_different_entry_status_set_changes_snapshot_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            root_a, root_b = Path(tmp_a), Path(tmp_b)
            _, _, _, frame_with_bars = _happy_frame(root_a)
            snapshot_a = PromotedSessionTickSizeService().materialize(
                frame=frame_with_bars, cutoff=TICK_CUTOFF
            )
            snapshot_b = _snapshot_from_minimal(root_b)
            self.assertNotEqual(snapshot_a.snapshot_id, snapshot_b.snapshot_id)

    def test_filesystem_path_alone_does_not_change_snapshot_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            root_a, root_b = Path(tmp_a), Path(tmp_b)
            snapshot_a = _snapshot_from_minimal(root_a)
            snapshot_b = _snapshot_from_minimal(root_b)
            self.assertNotEqual(
                snapshot_a.frame.universe.adjudication.intake.promotions[0].artifact.path,
                snapshot_b.frame.universe.adjudication.intake.promotions[0].artifact.path,
            )
            self.assertEqual(snapshot_a.snapshot_id, snapshot_b.snapshot_id)


class PromotedSessionTickSizeCapabilityTests(unittest.TestCase):
    def test_no_effective_interval_or_price_rounding_capability_exists(self) -> None:
        # effective_interval_verified is the required always-False safety
        # flag itself (asserted separately below), not a capability leak, so
        # it is deliberately excluded from this banned-substring list.
        banned_substrings = (
            "round",
            "price_target",
            "signal",
            "model",
            "notif",
            "broker",
            "order",
            "position_size",
            "capital",
            "rank",
        )
        for candidate in (
            PromotedSessionTickSizeService,
            VerifiedPromotedSessionTickSnapshot,
            PromotedSessionTickEntry,
        ):
            members = [name for name in dir(candidate) if not name.startswith("__")]
            for name in members:
                lowered = name.lower()
                self.assertFalse(
                    any(bad in lowered for bad in banned_substrings),
                    f"{candidate!r} unexpectedly exposes {name!r}",
                )

    def test_no_io_shaped_capability_exists(self) -> None:
        banned_substrings = (
            "list",
            "latest",
            "find",
            "download",
            "fetch",
            "network",
            "filesystem",
            "environ",
            "clock",
            "write",
            "rename",
            "chmod",
            "touch",
            "delete",
            "notif",
            "broker",
            "order",
            "capital",
        )
        for candidate in (
            PromotedSessionTickSizeService,
            VerifiedPromotedSessionTickSnapshot,
        ):
            members = [name for name in dir(candidate) if not name.startswith("__")]
            for name in members:
                lowered = name.lower()
                self.assertFalse(
                    any(bad in lowered for bad in banned_substrings),
                    f"{candidate!r} unexpectedly exposes {name!r}",
                )
                self.assertFalse(
                    lowered.startswith("select_") or lowered == "select",
                    f"{candidate!r} unexpectedly exposes {name!r}",
                )

    def test_effective_interval_verified_is_always_false(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, _, frame = _happy_frame(root)
            snapshot = PromotedSessionTickSizeService().materialize(
                frame=frame, cutoff=TICK_CUTOFF
            )
            for entry in snapshot.entries:
                self.assertFalse(entry.effective_interval_verified)

    def test_no_forbidden_capability_field_exists(self) -> None:
        field_names = {
            field.name for field in dataclasses.fields(VerifiedPromotedSessionTickSnapshot)
        }
        for banned in (
            "price",
            "liquidity",
            "corporate_action",
            "model",
            "signal",
            "ranking",
            "recommendation",
            "notification",
            "broker",
            "order",
            "position_size",
            "capital",
            "effective_interval",
        ):
            self.assertFalse(any(banned in name for name in field_names))

    def test_observation_type_is_the_legacy_collected_tick_size_observation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, _, frame = _happy_frame(root)
            snapshot = PromotedSessionTickSizeService().materialize(
                frame=frame, cutoff=TICK_CUTOFF
            )
            observed = next(
                entry.observation for entry in snapshot.entries if entry.observation is not None
            )
            self.assertIsInstance(observed, CollectedTickSizeObservation)


class PromotedSessionTickSizeLegacyRegressionTests(unittest.TestCase):
    def test_legacy_tick_size_module_untouched(self) -> None:
        from india_swing.tick_sizes.materialize import materialize_collection_tick_sizes

        self.assertTrue(callable(materialize_collection_tick_sizes))


if __name__ == "__main__":
    unittest.main()
