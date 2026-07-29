from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

from india_swing.corporate_actions.promoted_adjustments import (
    PromotedCorporateActionAdjustmentService,
)
from india_swing.promoted_graph_store import (
    LocalPromotedCorporateActionAdjustmentStore,
    LocalPromotedEffectiveSessionTickStore,
    LocalPromotedIdentityAdjudicationStore,
    LocalPromotedIdentityIntakeStore,
    LocalPromotedIdentitySessionUniverseStore,
    LocalPromotedSessionMarketDataFrameStore,
    LocalPromotedSessionTickSnapshotStore,
    LocalPromotedStableListingHistoryStore,
    PromotedGraphReplayRecord,
    PromotedGraphStoreConflict,
    PromotedGraphStoreError,
    decode_promoted_graph_record,
    encode_promoted_graph_record,
)
from india_swing.tick_sizes.effective_session import (
    PromotedEffectiveSessionTickService,
)
from tests.test_promoted_corporate_action_bridge import (
    BRIDGE_CUTOFF,
    _event,
    _snapshot,
)
from tests.test_promoted_stable_listing_history import (
    _two_session_fixture,
)


UTC = timezone.utc


class ExactResolver:
    def __init__(self, values, identity) -> None:
        self.values = {identity(value): value for value in values}

    def get(self, identity):
        return self.values[identity]


def _stores(root: Path, calendar, snapshots):
    frames = tuple(value.frame for value in snapshots)
    universes = tuple(value.universe for value in frames)
    adjudication = universes[0].adjudication
    intake = adjudication.intake
    promotions = intake.promotions
    corpora = {
        value.corpus_index.corpus_id: (
            value.corpus_index,
            (value.partition,),
        )
        for value in frames
    }

    promotion_resolver = ExactResolver(
        promotions, lambda value: value.promotion_id
    )
    intake_store = LocalPromotedIdentityIntakeStore(
        root, promotion_resolver
    )
    evidence_resolver = ExactResolver(
        adjudication.evidence_artifacts,
        lambda value: value.manifest.artifact_id,
    )
    review_resolver = ExactResolver(
        adjudication.review_bundles,
        lambda value: value.manifest.bundle_id,
    )
    adjudication_store = LocalPromotedIdentityAdjudicationStore(
        root,
        intake_store,
        evidence_resolver,
        review_resolver,
    )
    calendar_resolver = ExactResolver(
        (calendar,), lambda value: value.materialization_id
    )
    universe_store = LocalPromotedIdentitySessionUniverseStore(
        root, adjudication_store, calendar_resolver
    )
    corpus_resolver = ExactResolver(
        tuple(corpora.values()),
        lambda value: value[0].corpus_id,
    )
    frame_store = LocalPromotedSessionMarketDataFrameStore(
        root, universe_store, corpus_resolver
    )
    tick_store = LocalPromotedSessionTickSnapshotStore(
        root, frame_store
    )
    history_store = LocalPromotedStableListingHistoryStore(
        root, tick_store, calendar_resolver
    )
    return (
        intake_store,
        adjudication_store,
        universe_store,
        frame_store,
        tick_store,
        history_store,
        intake,
        adjudication,
        universes,
        frames,
    )


class PromotedGraphCodecTests(unittest.TestCase):
    def test_round_trip_is_canonical(self) -> None:
        value = PromotedGraphReplayRecord(
            kind="PROMOTED_IDENTITY_INTAKE",
            artifact_id="a" * 64,
            primary_ids=("b" * 64,),
            secondary_ids=(),
            tertiary_ids=(),
            dates=(date(2026, 7, 15),),
            cutoff=datetime(2026, 7, 16, tzinfo=UTC),
        )
        payload = encode_promoted_graph_record(value)
        self.assertEqual(
            decode_promoted_graph_record(
                payload,
                expected_kind="PROMOTED_IDENTITY_INTAKE",
            ),
            value,
        )
        self.assertEqual(payload, encode_promoted_graph_record(value))

    def test_rejects_duplicate_keys(self) -> None:
        payload = (
            '{"artifact_id":"'
            + "a" * 64
            + '","artifact_id":"'
            + "a" * 64
            + '"}'
        ).encode()
        with self.assertRaises(PromotedGraphStoreError):
            decode_promoted_graph_record(
                payload,
                expected_kind="PROMOTED_IDENTITY_INTAKE",
            )

    def test_rejects_noncanonical_json(self) -> None:
        value = PromotedGraphReplayRecord(
            kind="PROMOTED_IDENTITY_INTAKE",
            artifact_id="a" * 64,
            primary_ids=("b" * 64,),
            secondary_ids=(),
            tertiary_ids=(),
            dates=(date(2026, 7, 15),),
            cutoff=datetime(2026, 7, 16, tzinfo=UTC),
        )
        decoded = json.loads(encode_promoted_graph_record(value))
        payload = json.dumps(decoded, indent=2, sort_keys=True).encode()
        with self.assertRaises(PromotedGraphStoreError):
            decode_promoted_graph_record(
                payload,
                expected_kind="PROMOTED_IDENTITY_INTAKE",
            )

    def test_rejects_wrong_kind_and_naive_cutoff(self) -> None:
        value = PromotedGraphReplayRecord(
            kind="PROMOTED_IDENTITY_INTAKE",
            artifact_id="a" * 64,
            primary_ids=("b" * 64,),
            secondary_ids=(),
            tertiary_ids=(),
            dates=(date(2026, 7, 15),),
            cutoff=datetime(2026, 7, 16),
        )
        with self.assertRaises(PromotedGraphStoreError):
            encode_promoted_graph_record(value)


class PromotedGraphStoreIntegrationTests(unittest.TestCase):
    def test_replays_complete_graph_through_adjustment_and_ticks(self) -> None:
        with tempfile.TemporaryDirectory() as fixture_tmp:
            calendar, snapshots, history = _two_session_fixture(
                Path(fixture_tmp)
            )
            actions = _snapshot(_event(history))
            adjustment = (
                PromotedCorporateActionAdjustmentService().materialize(
                    source_panel=history,
                    corporate_actions=actions,
                    cutoff=BRIDGE_CUTOFF,
                )
            )
            effective_ticks = (
                PromotedEffectiveSessionTickService().materialize(
                    source_panel=history,
                    cutoff=BRIDGE_CUTOFF,
                )
            )
            with tempfile.TemporaryDirectory() as store_tmp:
                root = Path(store_tmp)
                (
                    intake_store,
                    adjudication_store,
                    universe_store,
                    frame_store,
                    tick_store,
                    history_store,
                    intake,
                    adjudication,
                    universes,
                    frames,
                ) = _stores(root, calendar, snapshots)

                self.assertEqual(intake_store.put(intake), intake)
                self.assertEqual(
                    adjudication_store.put(adjudication), adjudication
                )
                for universe in universes:
                    self.assertEqual(
                        universe_store.put(universe), universe
                    )
                for frame in frames:
                    self.assertEqual(frame_store.put(frame), frame)
                for snapshot in snapshots:
                    self.assertEqual(
                        tick_store.put(snapshot), snapshot
                    )
                self.assertEqual(history_store.put(history), history)

                action_resolver = ExactResolver(
                    (actions,), lambda value: value.snapshot_id
                )
                adjustment_store = (
                    LocalPromotedCorporateActionAdjustmentStore(
                        root, history_store, action_resolver
                    )
                )
                effective_store = LocalPromotedEffectiveSessionTickStore(
                    root, history_store
                )
                self.assertEqual(
                    adjustment_store.put(adjustment), adjustment
                )
                self.assertEqual(
                    effective_store.put(effective_ticks),
                    effective_ticks,
                )
                self.assertEqual(
                    adjustment_store.get(adjustment.bridge_id),
                    adjustment,
                )
                self.assertEqual(
                    effective_store.get(effective_ticks.panel_id),
                    effective_ticks,
                )

    def test_tampered_source_id_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as fixture_tmp:
            calendar, snapshots, _history = _two_session_fixture(
                Path(fixture_tmp)
            )
            with tempfile.TemporaryDirectory() as store_tmp:
                root = Path(store_tmp)
                (
                    intake_store,
                    _adjudication_store,
                    _universe_store,
                    _frame_store,
                    _tick_store,
                    _history_store,
                    intake,
                    _adjudication,
                    _universes,
                    _frames,
                ) = _stores(root, calendar, snapshots)
                intake_store.put(intake)
                path = intake_store.path_for(intake.intake_id)
                decoded = json.loads(path.read_text("utf-8"))
                decoded["primary_ids"][0] = "f" * 64
                path.write_text(
                    json.dumps(
                        decoded,
                        allow_nan=False,
                        ensure_ascii=True,
                        separators=(",", ":"),
                        sort_keys=True,
                    )
                    + "\n",
                    encoding="utf-8",
                )
                with self.assertRaises(PromotedGraphStoreError):
                    intake_store.get(intake.intake_id)

    def test_create_once_conflict_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as fixture_tmp:
            calendar, snapshots, _history = _two_session_fixture(
                Path(fixture_tmp)
            )
            with tempfile.TemporaryDirectory() as store_tmp:
                root = Path(store_tmp)
                (
                    intake_store,
                    _adjudication_store,
                    _universe_store,
                    _frame_store,
                    _tick_store,
                    _history_store,
                    intake,
                    _adjudication,
                    _universes,
                    _frames,
                ) = _stores(root, calendar, snapshots)
                path = intake_store.path_for(intake.intake_id)
                path.parent.mkdir(parents=True)
                path.write_bytes(b"{}\n")
                with self.assertRaises(PromotedGraphStoreConflict):
                    intake_store.put(intake)

    def test_no_listing_or_latest_capability_exists(self) -> None:
        names = dir(LocalPromotedIdentityIntakeStore)
        self.assertFalse(any("list" in value.lower() for value in names))
        self.assertFalse(any("latest" in value.lower() for value in names))


if __name__ == "__main__":
    unittest.main()
