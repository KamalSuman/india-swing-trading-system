from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from india_swing.corporate_actions.models import (
    CorporateActionEvent,
    CorporateActionIntegrityError,
    CorporateActionSnapshot,
    CorporateActionStatus,
    CorporateActionType,
)
from india_swing.corporate_actions.promoted_adjustments import (
    PromotedCorporateActionAdjustmentService,
)
from india_swing.corporate_actions.snapshot_store import (
    CorporateActionSnapshotStoreConflict,
    CorporateActionSnapshotStoreError,
    CorporateActionSnapshotStoreNotFound,
    LocalCorporateActionSnapshotStore,
    decode_corporate_action_snapshot,
    encode_corporate_action_snapshot,
)
from india_swing.promoted_graph_store import (
    LocalPromotedCorporateActionAdjustmentStore,
    PromotedGraphStoreError,
)
from india_swing.reference.models import ReferenceReadiness
from tests.test_promoted_corporate_action_bridge import BRIDGE_CUTOFF, _event, _snapshot
from tests.test_promoted_stable_listing_history import _two_session_fixture


IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc


def _write_canonical_json(path: Path, value: dict) -> None:
    """Write canonical JSON bytes with an explicit single trailing LF.

    Path.write_text on Windows translates "\\n" to CRLF by default, which
    would make every mutation fixture below fail at this store's own
    generic noncanonical-byte check before ever exercising the intended
    field-specific rejection. write_bytes with an explicit UTF-8 encode
    sidesteps any newline translation entirely.
    """

    payload = (json.dumps(value, separators=(",", ":"), sort_keys=True) + "\n").encode(
        "utf-8"
    )
    assert payload.endswith(b"\n") and b"\r" not in payload
    path.write_bytes(payload)


INSTRUMENT_ID = "1" * 64
LISTING_ID = "2" * 64
SOURCE_ID = "3" * 64
SECOND_SOURCE_ID = "4" * 64
ROW_ID = "5" * 64
SECOND_ROW_ID = "6" * 64
ANNOUNCED = datetime(2026, 7, 1, 18, 0, tzinfo=IST)
KNOWN = datetime(2026, 7, 1, 18, 5, tzinfo=IST)
EFFECTIVE = date(2026, 7, 15)


def _split_event() -> CorporateActionEvent:
    return CorporateActionEvent(
        stable_instrument_id=INSTRUMENT_ID,
        stable_listing_id=LISTING_ID,
        action_type=CorporateActionType.SPLIT,
        status=CorporateActionStatus.CONFIRMED,
        effective_session=EFFECTIVE,
        announcement_time=ANNOUNCED,
        knowledge_time=KNOWN,
        source_artifact_id=SOURCE_ID,
        source_row_id=ROW_ID,
        pre_action_shares=Decimal("1"),
        post_action_shares=Decimal("2"),
    )


def _dividend_event() -> CorporateActionEvent:
    return CorporateActionEvent(
        stable_instrument_id=INSTRUMENT_ID,
        stable_listing_id=LISTING_ID,
        action_type=CorporateActionType.CASH_DIVIDEND,
        status=CorporateActionStatus.CONFIRMED,
        effective_session=EFFECTIVE,
        announcement_time=ANNOUNCED,
        knowledge_time=KNOWN,
        source_artifact_id=SOURCE_ID,
        source_row_id=ROW_ID,
        cash_amount_per_share=Decimal("5.50"),
        currency="INR",
    )


def _collection_snapshot(*events: CorporateActionEvent) -> CorporateActionSnapshot:
    return CorporateActionSnapshot(
        cutoff=datetime(2026, 7, 16, 17, 0, tzinfo=IST),
        coverage_start=date(2020, 1, 1),
        coverage_end=date(2026, 7, 16),
        source_artifact_ids=tuple(
            sorted({SOURCE_ID, *(value.source_artifact_id for value in events)})
        ),
        events=tuple(
            sorted(
                events,
                key=lambda value: (
                    value.knowledge_time,
                    value.effective_session,
                    value.event_id,
                ),
            )
        ),
        readiness=ReferenceReadiness.COLLECTION_ONLY,
        complete=False,
        actionable=False,
        reason_codes=("OFFICIAL_IMPORTER_NOT_IMPLEMENTED",),
    )


def _empty_blocked_snapshot() -> CorporateActionSnapshot:
    return CorporateActionSnapshot(
        cutoff=datetime(2026, 7, 16, 17, 0, tzinfo=IST),
        coverage_start=date(2020, 1, 1),
        coverage_end=date(2026, 7, 16),
        source_artifact_ids=(SOURCE_ID,),
        events=(),
        readiness=ReferenceReadiness.COLLECTION_ONLY,
        complete=False,
        actionable=False,
        reason_codes=("NO_EVENTS_KNOWN",),
    )


class CorporateActionSnapshotStoreAcceptanceTests(unittest.TestCase):
    def test_put_get_and_reinstantiation_replay_exact_equality(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = _collection_snapshot(_split_event())
            store = LocalCorporateActionSnapshotStore(root)
            put_result = store.put(snapshot)
            self.assertEqual(put_result, snapshot)

            got = store.get(snapshot.snapshot_id)
            self.assertEqual(got, snapshot)
            got.verify_content_identity()

            fresh_store = LocalCorporateActionSnapshotStore(root)
            replayed = fresh_store.get(snapshot.snapshot_id)
            self.assertEqual(replayed, snapshot)

    def test_empty_event_blocked_snapshot_round_trips(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = _empty_blocked_snapshot()
            store = LocalCorporateActionSnapshotStore(root)
            store.put(snapshot)
            self.assertEqual(store.get(snapshot.snapshot_id), snapshot)

    def test_split_bonus_and_dividend_economic_terms_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bonus = replace(
                _split_event(),
                action_type=CorporateActionType.BONUS,
                source_row_id=SECOND_ROW_ID,
            )
            snapshot = _collection_snapshot(_split_event(), bonus, _dividend_event())
            store = LocalCorporateActionSnapshotStore(root)
            store.put(snapshot)
            got = store.get(snapshot.snapshot_id)
            self.assertEqual(got, snapshot)
            by_type = {value.action_type: value for value in got.events}
            self.assertEqual(by_type[CorporateActionType.SPLIT].pre_action_shares, Decimal("1"))
            self.assertEqual(by_type[CorporateActionType.SPLIT].post_action_shares, Decimal("2"))
            self.assertEqual(
                by_type[CorporateActionType.CASH_DIVIDEND].cash_amount_per_share,
                Decimal("5.50"),
            )
            self.assertEqual(by_type[CorporateActionType.CASH_DIVIDEND].currency, "INR")

    def test_cancellation_and_amendment_lineage_round_trips(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            original = _split_event()
            cancellation = CorporateActionEvent(
                stable_instrument_id=INSTRUMENT_ID,
                stable_listing_id=LISTING_ID,
                action_type=CorporateActionType.SPLIT,
                status=CorporateActionStatus.CANCELLED,
                effective_session=EFFECTIVE,
                announcement_time=ANNOUNCED + timedelta(days=1),
                knowledge_time=KNOWN + timedelta(days=1),
                source_artifact_id=SECOND_SOURCE_ID,
                source_row_id=SECOND_ROW_ID,
                supersedes_event_id=original.event_id,
            )
            snapshot = _collection_snapshot(original, cancellation)
            store = LocalCorporateActionSnapshotStore(root)
            store.put(snapshot)
            got = store.get(snapshot.snapshot_id)
            self.assertEqual(got, snapshot)
            self.assertEqual(got.active_events, ())
            cancelled = next(
                value for value in got.events if value.status is CorporateActionStatus.CANCELLED
            )
            self.assertEqual(cancelled.supersedes_event_id, original.event_id)

    def test_exact_decimal_textual_preservation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            event = replace(_split_event(), pre_action_shares=Decimal("1"), post_action_shares=Decimal("2.50"))
            snapshot = _collection_snapshot(event)
            store = LocalCorporateActionSnapshotStore(root)
            store.put(snapshot)
            path = store.path_for(snapshot.snapshot_id)
            raw = json.loads(path.read_text("utf-8"))
            self.assertEqual(raw["events"][0]["post_action_shares"], "2.50")
            got = store.get(snapshot.snapshot_id)
            self.assertEqual(got.events[0].post_action_shares, Decimal("2.50"))

    def test_scientific_notation_decimal_exponent_forms_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            event = replace(
                _split_event(),
                pre_action_shares=Decimal("1E+2"),
                post_action_shares=Decimal("1E-7"),
            )
            snapshot = _collection_snapshot(event)
            store = LocalCorporateActionSnapshotStore(root)
            store.put(snapshot)
            path = store.path_for(snapshot.snapshot_id)
            raw = json.loads(path.read_text("utf-8"))
            self.assertEqual(raw["events"][0]["pre_action_shares"], "1E+2")
            self.assertEqual(raw["events"][0]["post_action_shares"], "1E-7")
            got = store.get(snapshot.snapshot_id)
            self.assertEqual(got.events[0].pre_action_shares, Decimal("1E+2"))
            self.assertEqual(got.events[0].post_action_shares, Decimal("1E-7"))

    def test_all_valid_readiness_and_actionability_states_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = LocalCorporateActionSnapshotStore(root)
            collection_only = _collection_snapshot(_split_event())
            actionable = CorporateActionSnapshot(
                cutoff=datetime(2026, 7, 16, 17, 0, tzinfo=IST),
                coverage_start=date(2020, 1, 1),
                coverage_end=date(2026, 7, 16),
                source_artifact_ids=(SOURCE_ID,),
                events=(_split_event(),),
                readiness=ReferenceReadiness.SYNTHETIC_TEST,
                complete=True,
                actionable=True,
                reason_codes=(),
            )
            for snapshot in (collection_only, actionable):
                store.put(snapshot)
                self.assertEqual(store.get(snapshot.snapshot_id), snapshot)

    def test_create_once_idempotence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = _collection_snapshot(_split_event())
            store = LocalCorporateActionSnapshotStore(root)
            first = store.put(snapshot)
            second = store.put(snapshot)
            self.assertEqual(first, second)

    def test_canonical_deterministic_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            snapshot_a = _collection_snapshot(_split_event())
            snapshot_b = _collection_snapshot(_split_event())
            self.assertEqual(
                encode_corporate_action_snapshot(snapshot_a),
                encode_corporate_action_snapshot(snapshot_b),
            )
            store_a = LocalCorporateActionSnapshotStore(Path(tmp_a))
            store_b = LocalCorporateActionSnapshotStore(Path(tmp_b))
            store_a.put(snapshot_a)
            store_b.put(snapshot_b)
            self.assertEqual(
                store_a.path_for(snapshot_a.snapshot_id).read_bytes(),
                store_b.path_for(snapshot_b.snapshot_id).read_bytes(),
            )


class CorporateActionSnapshotStoreRejectionTests(unittest.TestCase):
    def _store_with(self, root: Path, snapshot: CorporateActionSnapshot) -> LocalCorporateActionSnapshotStore:
        store = LocalCorporateActionSnapshotStore(root)
        store.put(snapshot)
        return store

    def _mutate(self, store: LocalCorporateActionSnapshotStore, snapshot_id: str, mutator) -> None:
        path = store.path_for(snapshot_id)
        value = json.loads(path.read_text("utf-8"))
        mutator(value)
        _write_canonical_json(path, value)

    def test_invalid_snapshot_id_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = LocalCorporateActionSnapshotStore(Path(tmp))
            with self.assertRaises(CorporateActionSnapshotStoreError):
                store.get("not-a-sha256")

    def test_missing_snapshot_is_not_found(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = LocalCorporateActionSnapshotStore(Path(tmp))
            with self.assertRaises(CorporateActionSnapshotStoreNotFound):
                store.get("0" * 64)

    def test_put_with_wrong_type_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = LocalCorporateActionSnapshotStore(Path(tmp))
            with self.assertRaises(TypeError):
                store.put("not-a-snapshot")  # type: ignore[arg-type]

    def test_future_known_event_is_rejected_at_model_construction(self) -> None:
        event = replace(_split_event(), knowledge_time=datetime(2026, 7, 17, 9, 0, tzinfo=IST))
        with self.assertRaisesRegex(CorporateActionIntegrityError, "after its cutoff"):
            _collection_snapshot(event)

    def test_malformed_supersession_target_is_rejected(self) -> None:
        cancellation = CorporateActionEvent(
            stable_instrument_id=INSTRUMENT_ID,
            stable_listing_id=LISTING_ID,
            action_type=CorporateActionType.SPLIT,
            status=CorporateActionStatus.CANCELLED,
            effective_session=EFFECTIVE,
            announcement_time=ANNOUNCED,
            knowledge_time=KNOWN,
            source_artifact_id=SOURCE_ID,
            source_row_id=ROW_ID,
            supersedes_event_id="9" * 64,
        )
        with self.assertRaisesRegex(CorporateActionIntegrityError, "target is absent"):
            _collection_snapshot(cancellation)

    def test_source_lineage_mismatch_is_rejected_at_model_construction(self) -> None:
        event = replace(_split_event(), source_artifact_id="a" * 64)
        with self.assertRaises(CorporateActionIntegrityError):
            CorporateActionSnapshot(
                cutoff=datetime(2026, 7, 16, 17, 0, tzinfo=IST),
                coverage_start=date(2020, 1, 1),
                coverage_end=date(2026, 7, 16),
                source_artifact_ids=(SOURCE_ID,),  # deliberately excludes event's own source
                events=(event,),
                readiness=ReferenceReadiness.COLLECTION_ONLY,
                complete=False,
                actionable=False,
                reason_codes=("OFFICIAL_IMPORTER_NOT_IMPLEMENTED",),
            )

    def test_readiness_and_actionable_mutation_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = _collection_snapshot(_split_event())
            store = self._store_with(root, snapshot)
            self._mutate(
                store,
                snapshot.snapshot_id,
                lambda value: value.__setitem__("actionable", True),
            )
            with self.assertRaises(CorporateActionSnapshotStoreError):
                store.get(snapshot.snapshot_id)

    def test_readiness_value_mutation_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = _collection_snapshot(_split_event())
            store = self._store_with(root, snapshot)
            self._mutate(
                store,
                snapshot.snapshot_id,
                lambda value: value.__setitem__("readiness", "POINT_IN_TIME_VERIFIED"),
            )
            with self.assertRaises(CorporateActionSnapshotStoreError):
                store.get(snapshot.snapshot_id)

    def test_path_identity_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = _collection_snapshot(_split_event())
            store = self._store_with(root, snapshot)
            self._mutate(
                store,
                snapshot.snapshot_id,
                lambda value: value.__setitem__("snapshot_id", "f" * 64),
            )
            with self.assertRaises(CorporateActionSnapshotStoreError):
                store.get(snapshot.snapshot_id)

    def test_event_id_projection_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = _collection_snapshot(_split_event())
            store = self._store_with(root, snapshot)
            self._mutate(
                store,
                snapshot.snapshot_id,
                lambda value: value["events"][0].__setitem__("event_id", "e" * 64),
            )
            with self.assertRaises(CorporateActionSnapshotStoreError):
                store.get(snapshot.snapshot_id)

    def test_duplicate_keys_are_rejected(self) -> None:
        payload = ('{"snapshot_id":"' + "a" * 64 + '","snapshot_id":"' + "a" * 64 + '"}').encode(
            "utf-8"
        )
        with self.assertRaises(CorporateActionSnapshotStoreError):
            decode_corporate_action_snapshot(payload)

    def test_unknown_key_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = _collection_snapshot(_split_event())
            store = self._store_with(root, snapshot)
            self._mutate(
                store,
                snapshot.snapshot_id,
                lambda value: value.__setitem__("unexpected_field", "x"),
            )
            with self.assertRaises(CorporateActionSnapshotStoreError):
                store.get(snapshot.snapshot_id)

    def test_float_token_is_rejected(self) -> None:
        payload = b'{"snapshot_id": 1.5}'
        with self.assertRaises(CorporateActionSnapshotStoreError):
            decode_corporate_action_snapshot(payload)

    def test_numeric_coercion_for_decimal_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = _collection_snapshot(_split_event())
            store = self._store_with(root, snapshot)
            self._mutate(
                store,
                snapshot.snapshot_id,
                lambda value: value["events"][0].__setitem__("post_action_shares", 2),
            )
            with self.assertRaises(CorporateActionSnapshotStoreError):
                store.get(snapshot.snapshot_id)

    def test_noncanonical_decimal_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = _collection_snapshot(_split_event())
            store = self._store_with(root, snapshot)
            self._mutate(
                store,
                snapshot.snapshot_id,
                lambda value: value["events"][0].__setitem__("post_action_shares", "2.50"),
            )
            with self.assertRaises(CorporateActionSnapshotStoreError):
                store.get(snapshot.snapshot_id)

    def test_signed_zero_decimal_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = _collection_snapshot(_split_event())
            store = self._store_with(root, snapshot)
            self._mutate(
                store,
                snapshot.snapshot_id,
                lambda value: value["events"][0].__setitem__("post_action_shares", "-0"),
            )
            with self.assertRaises(CorporateActionSnapshotStoreError):
                store.get(snapshot.snapshot_id)

    def test_negative_decimal_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = _collection_snapshot(_split_event())
            store = self._store_with(root, snapshot)
            self._mutate(
                store,
                snapshot.snapshot_id,
                lambda value: value["events"][0].__setitem__("post_action_shares", "-2"),
            )
            with self.assertRaises(CorporateActionSnapshotStoreError):
                store.get(snapshot.snapshot_id)

    def test_ordering_violation_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bonus = replace(
                _split_event(),
                action_type=CorporateActionType.BONUS,
                source_row_id=SECOND_ROW_ID,
                knowledge_time=KNOWN + timedelta(minutes=1),
            )
            snapshot = _collection_snapshot(_split_event(), bonus)
            store = self._store_with(root, snapshot)
            self._mutate(
                store,
                snapshot.snapshot_id,
                lambda value: value.__setitem__("events", list(reversed(value["events"]))),
            )
            with self.assertRaises(CorporateActionSnapshotStoreError):
                store.get(snapshot.snapshot_id)

    def test_duplicate_event_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = _collection_snapshot(_split_event())
            store = self._store_with(root, snapshot)
            self._mutate(
                store,
                snapshot.snapshot_id,
                lambda value: value.__setitem__("events", value["events"] * 2),
            )
            with self.assertRaises(CorporateActionSnapshotStoreError):
                store.get(snapshot.snapshot_id)

    def test_malformed_hash_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = _collection_snapshot(_split_event())
            store = self._store_with(root, snapshot)
            self._mutate(
                store,
                snapshot.snapshot_id,
                lambda value: value["events"][0].__setitem__(
                    "stable_instrument_id", "not-a-hash"
                ),
            )
            with self.assertRaises(CorporateActionSnapshotStoreError):
                store.get(snapshot.snapshot_id)

    def test_malformed_date_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = _collection_snapshot(_split_event())
            store = self._store_with(root, snapshot)
            self._mutate(
                store,
                snapshot.snapshot_id,
                lambda value: value.__setitem__("coverage_start", "01-01-2020"),
            )
            with self.assertRaises(CorporateActionSnapshotStoreError):
                store.get(snapshot.snapshot_id)

    def test_malformed_datetime_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = _collection_snapshot(_split_event())
            store = self._store_with(root, snapshot)
            self._mutate(
                store,
                snapshot.snapshot_id,
                lambda value: value.__setitem__("cutoff", "2026-07-16 17:00:00"),
            )
            with self.assertRaises(CorporateActionSnapshotStoreError):
                store.get(snapshot.snapshot_id)

    def test_malformed_enum_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = _collection_snapshot(_split_event())
            store = self._store_with(root, snapshot)
            self._mutate(
                store,
                snapshot.snapshot_id,
                lambda value: value["events"][0].__setitem__("action_type", "NOT_A_TYPE"),
            )
            with self.assertRaises(CorporateActionSnapshotStoreError):
                store.get(snapshot.snapshot_id)

    def test_bool_as_int_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = _collection_snapshot(_split_event())
            store = self._store_with(root, snapshot)
            self._mutate(
                store,
                snapshot.snapshot_id,
                lambda value: value.__setitem__("actionable", 0),
            )
            with self.assertRaises(CorporateActionSnapshotStoreError):
                store.get(snapshot.snapshot_id)

    def test_noncanonical_json_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = _collection_snapshot(_split_event())
            store = self._store_with(root, snapshot)
            path = store.path_for(snapshot.snapshot_id)
            decoded = json.loads(path.read_text("utf-8"))
            path.write_bytes(json.dumps(decoded, indent=2, sort_keys=True).encode("utf-8"))
            with self.assertRaises(CorporateActionSnapshotStoreError):
                store.get(snapshot.snapshot_id)

    def test_oversized_payload_is_rejected(self) -> None:
        payload = b'{"padding": "' + b"a" * (9 * 1024 * 1024) + b'"}'
        with self.assertRaises(CorporateActionSnapshotStoreError):
            decode_corporate_action_snapshot(payload)

    def test_empty_payload_is_rejected(self) -> None:
        with self.assertRaises(CorporateActionSnapshotStoreError):
            decode_corporate_action_snapshot(b"")

    def test_symlinked_target_is_rejected_when_supported(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = _collection_snapshot(_split_event())
            store = self._store_with(root, snapshot)
            path = store.path_for(snapshot.snapshot_id)
            real = path.with_suffix(".real.json")
            path.rename(real)
            try:
                path.symlink_to(real)
            except OSError:
                self.skipTest("symbolic links are unavailable in this environment")
            with self.assertRaises(CorporateActionSnapshotStoreError):
                store.get(snapshot.snapshot_id)

    def test_create_once_conflict_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = _collection_snapshot(_split_event())
            store = LocalCorporateActionSnapshotStore(root)
            path = store.path_for(snapshot.snapshot_id)
            path.parent.mkdir(parents=True)
            path.write_bytes(b"{}\n")
            with self.assertRaises(CorporateActionSnapshotStoreConflict):
                store.put(snapshot)

    def test_put_leaves_no_target_when_pre_publication_replay_disagrees(self) -> None:
        # Blocker 1's underlying invariant, tested directly rather than via a
        # source rewrite: put() decodes its own freshly encoded payload
        # before publishing and requires exact agreement. Temporarily
        # replacing the module-level decoder with one that returns a
        # content-mismatched snapshot proves put() rejects the value AND
        # leaves no target artifact on disk, rather than publishing
        # something that later fails to read back. The real decoder is
        # restored immediately afterward, mirroring the existing
        # test_base_exception_from_verify_is_not_swallowed monkeypatch
        # pattern in this same test module.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = _collection_snapshot(_split_event())
            store = LocalCorporateActionSnapshotStore(root)

            import india_swing.corporate_actions.snapshot_store as snapshot_store_module

            original = snapshot_store_module.decode_corporate_action_snapshot
            mismatched = replace(snapshot, cutoff=snapshot.cutoff + timedelta(days=1))

            def _mismatched_decode(payload: bytes) -> CorporateActionSnapshot:
                return mismatched

            snapshot_store_module.decode_corporate_action_snapshot = _mismatched_decode
            try:
                with self.assertRaises(CorporateActionSnapshotStoreError):
                    store.put(snapshot)
            finally:
                snapshot_store_module.decode_corporate_action_snapshot = original

            self.assertFalse(store.path_for(snapshot.snapshot_id).exists())


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


class CorporateActionSnapshotStoreSanitizationTests(unittest.TestCase):
    def test_error_text_never_includes_raw_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = _collection_snapshot(_split_event())
            store = LocalCorporateActionSnapshotStore(root)
            store.put(snapshot)
            path = store.path_for(snapshot.snapshot_id)
            value = json.loads(path.read_text("utf-8"))
            secret_hash = "d" * 64
            value["events"][0]["stable_instrument_id"] = secret_hash
            _write_canonical_json(path, value)
            with self.assertRaises(CorporateActionSnapshotStoreError) as ctx:
                store.get(snapshot.snapshot_id)
            message = str(ctx.exception)
            self.assertNotIn(secret_hash, message)
            self.assertNotIn(str(path), message)

    def test_base_exception_from_verify_is_not_swallowed(self) -> None:
        # put() wraps value.verify_content_identity() in a broad
        # `except Exception` (never BaseException) sanitizing boundary.
        # Temporarily replacing the real method with one that raises a
        # custom BaseException subclass proves that boundary genuinely
        # lets BaseException propagate rather than silently swallowing it;
        # the original method is restored immediately afterward.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = _collection_snapshot(_split_event())
            store = LocalCorporateActionSnapshotStore(root)

            original = CorporateActionSnapshot.verify_content_identity

            def _evil_verify(self) -> None:
                raise _ComparisonBoundaryBaseException("verify-boundary-control")

            CorporateActionSnapshot.verify_content_identity = _evil_verify
            try:
                with self.assertRaises(_ComparisonBoundaryBaseException):
                    store.put(snapshot)
            finally:
                CorporateActionSnapshot.verify_content_identity = original


class CorporateActionSnapshotStoreCapabilityTests(unittest.TestCase):
    def test_no_listing_latest_nearest_find_or_io_capability_exists(self) -> None:
        banned_substrings = (
            "list",
            "latest",
            "nearest",
            "find",
            "network",
            "gcp",
            "environ",
            "clock",
        )
        members = [
            name for name in dir(LocalCorporateActionSnapshotStore) if not name.startswith("__")
        ]
        for name in members:
            lowered = name.lower()
            self.assertFalse(
                any(bad in lowered for bad in banned_substrings),
                f"LocalCorporateActionSnapshotStore unexpectedly exposes {name!r}",
            )
        public_members = {name for name in members if not name.startswith("_")}
        self.assertEqual(public_members, {"path_for", "put", "get"})


class CorporateActionSnapshotStoreCompatibilityTests(unittest.TestCase):
    def test_satisfies_local_promoted_corporate_action_adjustment_store_resolver(self) -> None:
        with tempfile.TemporaryDirectory() as fixture_tmp:
            _, _, history = _two_session_fixture(Path(fixture_tmp))
            corporate_actions = _snapshot(_event(history))
            adjustment = PromotedCorporateActionAdjustmentService().materialize(
                source_panel=history,
                corporate_actions=corporate_actions,
                cutoff=BRIDGE_CUTOFF,
            )
            with tempfile.TemporaryDirectory() as store_tmp:
                root = Path(store_tmp)
                action_store = LocalCorporateActionSnapshotStore(root / "actions")
                action_store.put(corporate_actions)

                class _HistoryResolver:
                    def get(self, panel_id: str):
                        assert panel_id == history.panel_id
                        return history

                adjustment_store = LocalPromotedCorporateActionAdjustmentStore(
                    root / "adjustments", _HistoryResolver(), action_store
                )
                put_result = adjustment_store.put(adjustment)
                self.assertEqual(put_result, adjustment)
                self.assertEqual(
                    adjustment_store.get(adjustment.bridge_id), adjustment
                )

    def test_tampered_action_snapshot_fails_closed_through_the_bridge_store(self) -> None:
        with tempfile.TemporaryDirectory() as fixture_tmp:
            _, _, history = _two_session_fixture(Path(fixture_tmp))
            corporate_actions = _snapshot(_event(history))
            adjustment = PromotedCorporateActionAdjustmentService().materialize(
                source_panel=history,
                corporate_actions=corporate_actions,
                cutoff=BRIDGE_CUTOFF,
            )
            with tempfile.TemporaryDirectory() as store_tmp:
                root = Path(store_tmp)
                action_store = LocalCorporateActionSnapshotStore(root / "actions")
                action_store.put(corporate_actions)
                path = action_store.path_for(corporate_actions.snapshot_id)
                value = json.loads(path.read_text("utf-8"))
                value["events"][0]["stable_instrument_id"] = "f" * 64
                _write_canonical_json(path, value)

                class _HistoryResolver:
                    def get(self, panel_id: str):
                        return history

                adjustment_store = LocalPromotedCorporateActionAdjustmentStore(
                    root / "adjustments", _HistoryResolver(), action_store
                )
                with self.assertRaises(PromotedGraphStoreError):
                    adjustment_store.put(adjustment)


if __name__ == "__main__":
    unittest.main()
