from __future__ import annotations

import dataclasses
import hashlib
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from india_swing.historical_prices.promoted_history import (
    PROMOTED_STABLE_LISTING_HISTORY_PRICE_BASIS,
    PromotedHistorySessionStatus,
    PromotedStableListingHistoryError,
    PromotedStableListingHistoryService,
    PromotedStableListingObservationStatus,
    PromotedUnassignedHistoryCategory,
    VerifiedPromotedStableListingHistoryPanel,
)
from india_swing.identity_decisions import PromotedIdentityAdjudicationService
from india_swing.identity_registry.promoted_intake import (
    PromotedIdentityIntakeService,
)
from india_swing.market_data.promoted_session_frame import (
    PromotedSessionMarketDataFrameService,
)
from india_swing.reference.models import ReferenceReadiness
from india_swing.tick_sizes.promoted_session import PromotedSessionTickSizeService
from india_swing.universe.promoted_identity import (
    PromotedIdentitySessionUniverseService,
)
from tests.test_promoted_identity_session_universe import (
    ADJUDICATION_CUTOFF,
    D0,
    D1,
    D2,
    INTAKE_CUTOFF,
    SESSION_CUTOFF,
    build_calendar,
    build_intake_and_adjudication,
    build_promotion,
    security_row,
)
from tests.test_promoted_session_market_data import (
    BUILT_AT,
    FRAME_CUTOFF,
    _bar,
    _corpus,
)
from tests.test_promoted_session_tick_sizes import TICK_CUTOFF


UTC = timezone.utc
PANEL_CUTOFF = TICK_CUTOFF + timedelta(hours=1)


def _id(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _two_session_fixture(
    root: Path,
    *,
    omit_reliance_bar_on: date | None = None,
):
    _, _, _, _, _, adjudication = build_intake_and_adjudication(root)
    calendar = build_calendar(root)
    snapshots = []
    for session in (D1, D2):
        universe = PromotedIdentitySessionUniverseService().materialize(
            adjudication=adjudication,
            calendar=calendar,
            market_session=session,
            cutoff=SESSION_CUTOFF,
        )
        by_symbol = {entry.symbol: entry for entry in universe.entries}
        bars = [
            _bar(
                by_symbol["SMALL1"],
                market_session=session,
                label=f"small-{session}",
            ),
        ]
        if session != omit_reliance_bar_on:
            bars.append(
                _bar(
                    by_symbol["RELIANCE"],
                    market_session=session,
                    label=f"reliance-{session}",
                )
            )
        if session == D2:
            orphan_proxy = dataclasses.replace(
                by_symbol["RELIANCE"],
                symbol="ORPHAN",
                validated_isin="INE999A01019",
            )
            bars.append(
                _bar(
                    orphan_proxy,
                    market_session=session,
                    label="orphan-history",
                )
            )
        index, partition = _corpus(
            market_session=session,
            bars=tuple(bars),
        )
        frame = PromotedSessionMarketDataFrameService().materialize(
            universe=universe,
            corpus_index=index,
            partition=partition,
            cutoff=FRAME_CUTOFF,
        )
        snapshots.append(
            PromotedSessionTickSizeService().materialize(
                frame=frame,
                cutoff=TICK_CUTOFF,
            )
        )
    panel = PromotedStableListingHistoryService().materialize(
        tick_snapshots=tuple(snapshots),
        calendar=calendar,
        cutoff=PANEL_CUTOFF,
    )
    return calendar, tuple(snapshots), panel


def _endpoint_snapshots_with_middle_session_missing(root: Path):
    promotions = tuple(
        build_promotion(
            root,
            report_date=session,
            generation=300 + index,
            rows=[security_row(FinInstrmId="77777", TckrSymb="GAPTEST")],
            first_seen=datetime(
                session.year,
                session.month,
                session.day,
                12,
                0,
                tzinfo=UTC,
            ),
            validated=datetime(
                session.year,
                session.month,
                session.day,
                12,
                0,
                2,
                tzinfo=UTC,
            ),
        )
        for index, session in enumerate((D0, D1, D2))
    )
    intake = PromotedIdentityIntakeService().materialize(
        promotions=promotions,
        expected_report_dates=(D0, D1, D2),
        cutoff=INTAKE_CUTOFF,
    )
    adjudication = PromotedIdentityAdjudicationService().materialize(
        intake=intake,
        evidence_artifacts=(),
        review_bundles=(),
        cutoff=ADJUDICATION_CUTOFF,
    )
    calendar = build_calendar(root)
    snapshots = []
    for session in (D0, D2):
        universe = PromotedIdentitySessionUniverseService().materialize(
            adjudication=adjudication,
            calendar=calendar,
            market_session=session,
            cutoff=SESSION_CUTOFF,
        )
        entry = next(value for value in universe.entries if value.symbol == "GAPTEST")
        index, partition = _corpus(
            market_session=session,
            bars=(
                _bar(
                    entry,
                    market_session=session,
                    label=f"gaptest-{session}",
                ),
            ),
        )
        frame = PromotedSessionMarketDataFrameService().materialize(
            universe=universe,
            corpus_index=index,
            partition=partition,
            cutoff=FRAME_CUTOFF,
        )
        snapshots.append(
            PromotedSessionTickSizeService().materialize(
                frame=frame,
                cutoff=TICK_CUTOFF,
            )
        )
    panel = PromotedStableListingHistoryService().materialize(
        tick_snapshots=tuple(snapshots),
        calendar=calendar,
        cutoff=PANEL_CUTOFF,
    )
    return tuple(snapshots), panel


class PromotedStableListingHistoryAcceptanceTests(unittest.TestCase):
    def test_builds_ordered_raw_history_for_resolved_listing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, snapshots, panel = _two_session_fixture(Path(tmp))
            self.assertEqual(panel.sessions, (D1, D2))
            self.assertEqual(
                tuple(value.tick_snapshot for value in panel.session_bindings),
                snapshots,
            )
            self.assertEqual(len(panel.histories), 1)
            history = panel.histories[0]
            self.assertEqual(
                tuple(value.market_session for value in history.observations),
                (D1, D2),
            )
            self.assertTrue(
                all(
                    value.status
                    is PromotedStableListingObservationStatus.RAW_BAR_OBSERVED
                    for value in history.observations
                )
            )
            self.assertEqual(history.raw_bar_count, 2)
            self.assertEqual(history.gap_count, 0)
            self.assertEqual(
                history.price_basis,
                PROMOTED_STABLE_LISTING_HISTORY_PRICE_BASIS,
            )
            panel.verify_content_identity()

    def test_unresolved_excluded_and_orphan_evidence_is_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, panel = _two_session_fixture(Path(tmp))
            categories = {value.category for value in panel.unassigned_entries}
            self.assertEqual(
                categories,
                {
                    PromotedUnassignedHistoryCategory.IDENTITY_UNRESOLVED,
                    PromotedUnassignedHistoryCategory.SOURCE_EXCLUDED,
                },
            )
            self.assertTrue(
                any(
                    value.tick_entry.frame_entry.universe_entry.symbol == "DELISTD"
                    for value in panel.unassigned_entries
                )
            )
            self.assertEqual(len(panel.orphan_bars), 1)
            self.assertEqual(panel.orphan_bars[0].orphan.bar.listing_key, "NSE:ORPHAN")

    def test_all_outputs_remain_collection_only_and_ineligible(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, panel = _two_session_fixture(Path(tmp))
            self.assertIs(panel.readiness, ReferenceReadiness.COLLECTION_ONLY)
            self.assertFalse(panel.actionable)
            self.assertFalse(panel.training_eligible)
            self.assertFalse(panel.feature_eligible)
            self.assertFalse(panel.alert_eligible)
            self.assertFalse(panel.execution_eligible)
            self.assertTrue(
                all(not value.corporate_action_adjusted for value in panel.histories)
            )

    def test_tick_values_and_raw_bar_ids_are_retained_per_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, panel = _two_session_fixture(Path(tmp))
            observations = panel.histories[0].observations
            self.assertTrue(all(value.raw_bar_id is not None for value in observations))
            self.assertTrue(
                all(value.bid_interval_paise is not None for value in observations)
            )
            self.assertEqual(
                len({value.raw_bar_id for value in observations}),
                len(observations),
            )


class PromotedStableListingHistoryGapTests(unittest.TestCase):
    def test_missing_bar_remains_explicit_gap_without_interpolation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, panel = _two_session_fixture(
                Path(tmp),
                omit_reliance_bar_on=D2,
            )
            history = panel.histories[0]
            self.assertEqual(history.raw_bar_count, 1)
            self.assertEqual(history.gap_count, 1)
            self.assertEqual(history.identity_conflict_count, 0)
            self.assertEqual(
                history.observations[1].status,
                PromotedStableListingObservationStatus.BAR_NOT_OBSERVED_NO_STATE_INFERENCE,
            )
            self.assertIsNone(history.observations[1].raw_bar_id)
            self.assertIsNotNone(history.observations[1].tick_entry)
            self.assertIn(
                "PRICE_BAR_NOT_OBSERVED_NO_STATE_INFERENCE",
                history.observations[1].reason_codes,
            )

    def test_missing_whole_session_is_retained_in_calendar_grid(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshots, panel = _endpoint_snapshots_with_middle_session_missing(
                Path(tmp)
            )
            self.assertEqual(
                tuple(value.market_session for value in snapshots),
                (D0, D2),
            )
            self.assertEqual(panel.sessions, (D0, D1, D2))
            missing = panel.session_bindings[1]
            self.assertEqual(missing.market_session, D1)
            self.assertIs(
                missing.status,
                PromotedHistorySessionStatus.SNAPSHOT_MISSING_NO_STATE_INFERENCE,
            )
            self.assertIsNone(missing.tick_snapshot)
            self.assertEqual(
                dict(panel.session_status_counts)[
                    PromotedHistorySessionStatus.SNAPSHOT_MISSING_NO_STATE_INFERENCE.value
                ],
                1,
            )

    def test_session_binding_rejects_missing_snapshot_with_attached_snapshot(self) -> None:
        from india_swing.historical_prices.promoted_history import (
            PromotedHistorySessionBinding,
        )

        with tempfile.TemporaryDirectory() as tmp:
            _, snapshots, _ = _two_session_fixture(Path(tmp))
            with self.assertRaises(PromotedStableListingHistoryError):
                PromotedHistorySessionBinding(
                    market_session=D1,
                    status=PromotedHistorySessionStatus.SNAPSHOT_MISSING_NO_STATE_INFERENCE,
                    tick_snapshot=snapshots[0],
                    reason_codes=(
                        "RAW_UNADJUSTED_HISTORY_COLLECTION_ONLY",
                        "SESSION_SNAPSHOT_MISSING_NO_STATE_INFERENCE",
                    ),
                )


class PromotedStableListingHistoryRejectionTests(unittest.TestCase):
    def test_unordered_or_duplicate_sessions_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            calendar, snapshots, _ = _two_session_fixture(Path(tmp))
            for values in (
                tuple(reversed(snapshots)),
                (snapshots[0], snapshots[0]),
            ):
                with self.subTest(values=tuple(value.market_session for value in values)):
                    with self.assertRaises(PromotedStableListingHistoryError):
                        PromotedStableListingHistoryService().materialize(
                            tick_snapshots=values,
                            calendar=calendar,
                            cutoff=PANEL_CUTOFF,
                        )

    def test_cutoff_before_input_knowledge_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            calendar, snapshots, _ = _two_session_fixture(Path(tmp))
            with self.assertRaises(PromotedStableListingHistoryError):
                PromotedStableListingHistoryService().materialize(
                    tick_snapshots=snapshots,
                    calendar=calendar,
                    cutoff=BUILT_AT - timedelta(seconds=1),
                )

    def test_wrong_calendar_lineage_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, snapshots, _ = _two_session_fixture(root)
            different_calendar = build_calendar(
                root,
                cutoff=datetime(2026, 7, 16, 16, 0, 1, tzinfo=UTC),
            )
            with self.assertRaises(PromotedStableListingHistoryError):
                PromotedStableListingHistoryService().materialize(
                    tick_snapshots=snapshots,
                    calendar=different_calendar,
                    cutoff=PANEL_CUTOFF,
                )

    def test_naive_cutoff_and_wrong_input_types_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            calendar, snapshots, _ = _two_session_fixture(Path(tmp))
            with self.assertRaises(PromotedStableListingHistoryError):
                PromotedStableListingHistoryService().materialize(
                    tick_snapshots=snapshots,
                    calendar=calendar,
                    cutoff=PANEL_CUTOFF.replace(tzinfo=None),
                )
            with self.assertRaises(PromotedStableListingHistoryError):
                PromotedStableListingHistoryService().materialize(
                    tick_snapshots=list(snapshots),  # type: ignore[arg-type]
                    calendar=calendar,
                    cutoff=PANEL_CUTOFF,
                )


class PromotedStableListingHistoryReplayTests(unittest.TestCase):
    def test_direct_construction_with_wrong_panel_id_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, panel = _two_session_fixture(Path(tmp))
            values = {
                item.name: getattr(panel, item.name)
                for item in dataclasses.fields(panel)
            }
            values["panel_id"] = "0" * 64
            with self.assertRaises(PromotedStableListingHistoryError):
                VerifiedPromotedStableListingHistoryPanel(**values)

    def test_bool_and_enum_lookalikes_cannot_pass_exact_type_checks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, panel = _two_session_fixture(Path(tmp))
            base = {
                item.name: getattr(panel, item.name)
                for item in dataclasses.fields(panel)
            }
            for field_name, replacement in (
                ("actionable", 0),
                ("readiness", ReferenceReadiness.COLLECTION_ONLY.value),
                (
                    "session_status_counts",
                    tuple(
                        (status, True if index == 0 else count)
                        for index, (status, count) in enumerate(
                            panel.session_status_counts
                        )
                    ),
                ),
            ):
                with self.subTest(field_name=field_name):
                    values = {**base, field_name: replacement}
                    with self.assertRaises(PromotedStableListingHistoryError):
                        VerifiedPromotedStableListingHistoryPanel(**values)

    def test_nested_tick_snapshot_mutation_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, snapshots, panel = _two_session_fixture(Path(tmp))
            object.__setattr__(snapshots[0], "snapshot_id", "0" * 64)
            with self.assertRaises(PromotedStableListingHistoryError):
                panel.verify_content_identity()

    def test_content_id_changes_when_cutoff_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            calendar, snapshots, panel = _two_session_fixture(Path(tmp))
            later = PromotedStableListingHistoryService().materialize(
                tick_snapshots=snapshots,
                calendar=calendar,
                cutoff=PANEL_CUTOFF + timedelta(seconds=1),
            )
            self.assertNotEqual(panel.panel_id, later.panel_id)

    def test_no_model_signal_execution_or_effective_interval_capability(self) -> None:
        forbidden = (
            "model",
            "signal",
            "notification",
            "broker",
            "order",
            "position_size",
            "capital",
            "effective_interval",
            "interpolate",
        )
        public_types = (
            VerifiedPromotedStableListingHistoryPanel,
            PromotedStableListingHistoryService,
        )
        names = {
            name.lower()
            for value in public_types
            for name in dir(value)
            if not name.startswith("_")
        }
        self.assertFalse(any(token in name for token in forbidden for name in names))


if __name__ == "__main__":
    unittest.main()
