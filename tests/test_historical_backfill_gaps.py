from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

from india_swing.market_data.backfill_gaps import (
    GAP_FILENAME_SUFFIX,
    HISTORICAL_BACKFILL_SESSION_GAP_DATASET,
    HistoricalBackfillGapClassification,
    HistoricalBackfillGapError,
    HistoricalBackfillGapIntegrityError,
    HistoricalBackfillSessionGapEvidence,
    LocalHistoricalBackfillSessionGapStore,
)


UTC = timezone.utc


def gap_evidence(**overrides) -> HistoricalBackfillSessionGapEvidence:
    values = {
        "plan_id": "a" * 64,
        "request_id": "b" * 64,
        "provider": "ZERODHA_KITE",
        "provider_version": "kiteconnect/5.2.0",
        "provider_instrument_id": "408065",
        "listing_key": "NSE:INFY",
        "security_series": "EQ",
        "isin": "INE009A01021",
        "session": date(2026, 7, 15),
        "response_observed_at": datetime(2026, 7, 15, 17, 0, tzinfo=UTC),
        "normalized_response_sha256": "c" * 64,
    }
    values.update(overrides)
    return HistoricalBackfillSessionGapEvidence(**values)


def gap_path(root: Path, evidence: HistoricalBackfillSessionGapEvidence) -> Path:
    return (
        root
        / HISTORICAL_BACKFILL_SESSION_GAP_DATASET
        / evidence.plan_id
        / evidence.request_id
        / f"{evidence.session.isoformat()}{GAP_FILENAME_SUFFIX}"
    )


class HistoricalBackfillSessionGapEvidenceTests(unittest.TestCase):
    def test_evidence_id_is_deterministic_and_sensitive_to_every_field(self) -> None:
        first = gap_evidence()
        second = gap_evidence()
        self.assertEqual(first.evidence_id, second.evidence_id)

        changed = gap_evidence(session=date(2026, 7, 16))
        self.assertNotEqual(first.evidence_id, changed.evidence_id)

    def test_defaults_are_collection_only_and_non_actionable(self) -> None:
        value = gap_evidence()
        self.assertIs(value.collection_only, True)
        self.assertIs(value.actionable, False)
        self.assertIs(
            value.classification,
            HistoricalBackfillGapClassification.UNRESOLVED_EMPTY_PROVIDER_RESPONSE,
        )

    def test_request_rejection_classification_is_collection_only(self) -> None:
        value = gap_evidence(
            classification=(
                HistoricalBackfillGapClassification.UNRESOLVED_PROVIDER_REQUEST_REJECTION
            )
        )

        self.assertIs(value.collection_only, True)
        self.assertIs(value.actionable, False)
        self.assertIs(
            value.classification,
            HistoricalBackfillGapClassification.UNRESOLVED_PROVIDER_REQUEST_REJECTION,
        )

    def test_malformed_fields_are_rejected(self) -> None:
        cases = dict(
            plan_id="not-a-hash",
            request_id="not-a-hash",
            provider="zerodha_kite",
            provider_version="",
            provider_instrument_id="",
            listing_key="INFY",
            security_series="eq",
            isin="NOTANISIN",
            session=datetime(2026, 7, 15, tzinfo=UTC),
            normalized_response_sha256="not-a-hash",
        )
        for field_name, bad_value in cases.items():
            with self.subTest(field=field_name):
                with self.assertRaises((TypeError, ValueError)):
                    gap_evidence(**{field_name: bad_value})

    def test_collection_only_and_actionable_cannot_be_overridden(self) -> None:
        with self.assertRaises(ValueError):
            gap_evidence(collection_only=False)
        with self.assertRaises(ValueError):
            gap_evidence(actionable=True)

    def test_verify_content_identity_detects_tampering(self) -> None:
        value = gap_evidence()

        object.__setattr__(value, "session", date(2026, 7, 16))

        with self.assertRaises(HistoricalBackfillGapIntegrityError):
            value.verify_content_identity()


class LocalHistoricalBackfillSessionGapStoreTests(unittest.TestCase):
    def test_round_trip_is_exact(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalHistoricalBackfillSessionGapStore(Path(temp_dir))
            evidence = gap_evidence()

            stored = store.put(evidence)

            self.assertEqual(stored, evidence)
            self.assertEqual(store.load_unresolved(evidence.plan_id), (evidence,))

    def test_same_content_save_is_atomic_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalHistoricalBackfillSessionGapStore(Path(temp_dir))
            evidence = gap_evidence()

            store.put(evidence)
            again = store.put(evidence)

            self.assertEqual(again, evidence)
            self.assertEqual(len(store.load_unresolved(evidence.plan_id)), 1)

    def test_conflicting_overwrite_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalHistoricalBackfillSessionGapStore(Path(temp_dir))
            evidence = gap_evidence()
            store.put(evidence)

            conflicting = gap_evidence(normalized_response_sha256="d" * 64)

            with self.assertRaises(HistoricalBackfillGapError):
                store.put(conflicting)

    def test_exact_plan_loading_is_scoped_and_sorted(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalHistoricalBackfillSessionGapStore(Path(temp_dir))
            plan_one = "a" * 64
            plan_two = "f" * 64
            first = gap_evidence(
                plan_id=plan_one, request_id="b" * 64, session=date(2026, 7, 15)
            )
            second = gap_evidence(
                plan_id=plan_one, request_id="c" * 64, session=date(2026, 7, 14)
            )
            other_plan = gap_evidence(plan_id=plan_two, request_id="e" * 64)

            store.put(first)
            store.put(second)
            store.put(other_plan)

            loaded = store.load_unresolved(plan_one)

            self.assertEqual(loaded, (first, second))

    def test_unknown_plan_returns_empty_without_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalHistoricalBackfillSessionGapStore(Path(temp_dir))

            self.assertEqual(store.load_unresolved("a" * 64), ())

    def test_no_latest_or_generic_listing_operation_exists(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalHistoricalBackfillSessionGapStore(Path(temp_dir))

            for banned in (
                "latest",
                "latest_at_or_before",
                "list",
                "list_all",
                "find_by_selection_key",
            ):
                self.assertFalse(hasattr(store, banned))

    def test_path_traversal_shaped_plan_ids_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalHistoricalBackfillSessionGapStore(Path(temp_dir))

            for bad in ("../escape", "a" * 63, "A" * 64, "../../etc/passwd", ""):
                with self.subTest(bad=bad):
                    with self.assertRaises(ValueError):
                        store.load_unresolved(bad)

    def test_duplicate_json_keys_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = LocalHistoricalBackfillSessionGapStore(root)
            evidence = gap_evidence()
            store.put(evidence)
            path = gap_path(root, evidence)
            original = json.loads(path.read_text(encoding="utf-8"))
            pairs = ",".join(
                f"{json.dumps(key)}:{json.dumps(value)}"
                for key, value in original.items()
            )
            duplicated = (
                "{" + pairs + f',"plan_id":{json.dumps(original["plan_id"])}' + "}"
            )
            path.write_text(duplicated, encoding="utf-8")

            with self.assertRaises(HistoricalBackfillGapError):
                store.load_unresolved(evidence.plan_id)

    def test_missing_key_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = LocalHistoricalBackfillSessionGapStore(root)
            evidence = gap_evidence()
            store.put(evidence)
            path = gap_path(root, evidence)
            original = json.loads(path.read_text(encoding="utf-8"))
            del original["isin"]
            path.write_text(json.dumps(original), encoding="utf-8")

            with self.assertRaises(HistoricalBackfillGapIntegrityError):
                store.load_unresolved(evidence.plan_id)

    def test_unknown_key_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = LocalHistoricalBackfillSessionGapStore(root)
            evidence = gap_evidence()
            store.put(evidence)
            path = gap_path(root, evidence)
            original = json.loads(path.read_text(encoding="utf-8"))
            original["unexpected_field"] = "x"
            path.write_text(json.dumps(original), encoding="utf-8")

            with self.assertRaises(HistoricalBackfillGapIntegrityError):
                store.load_unresolved(evidence.plan_id)

    def test_bool_as_int_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = LocalHistoricalBackfillSessionGapStore(root)
            evidence = gap_evidence()
            store.put(evidence)
            path = gap_path(root, evidence)
            original = json.loads(path.read_text(encoding="utf-8"))
            original["collection_only"] = 1
            path.write_text(json.dumps(original), encoding="utf-8")

            with self.assertRaises(HistoricalBackfillGapIntegrityError):
                store.load_unresolved(evidence.plan_id)

    def test_nan_and_infinity_tokens_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = LocalHistoricalBackfillSessionGapStore(root)
            evidence = gap_evidence()
            store.put(evidence)
            path = gap_path(root, evidence)
            text = path.read_text(encoding="utf-8")
            self.assertIn('"collection_only":true', text)
            corrupted = text.replace('"collection_only":true', '"collection_only":NaN')
            path.write_text(corrupted, encoding="utf-8")

            with self.assertRaises(HistoricalBackfillGapIntegrityError):
                store.load_unresolved(evidence.plan_id)

    def test_json_floats_are_rejected_before_model_construction(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = LocalHistoricalBackfillSessionGapStore(root)
            evidence = gap_evidence()
            store.put(evidence)
            path = gap_path(root, evidence)
            text = path.read_text(encoding="utf-8")
            corrupted = text.replace('"collection_only":true', '"collection_only":1.0')
            path.write_text(corrupted, encoding="utf-8")

            with self.assertRaises(HistoricalBackfillGapIntegrityError):
                store.load_unresolved(evidence.plan_id)

    def test_noncanonical_utc_timestamp_alias_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = LocalHistoricalBackfillSessionGapStore(root)
            evidence = gap_evidence()
            store.put(evidence)
            path = gap_path(root, evidence)
            text = path.read_text(encoding="utf-8")
            self.assertIn("+00:00", text)
            path.write_text(text.replace("+00:00", "Z"), encoding="utf-8")

            with self.assertRaises(HistoricalBackfillGapIntegrityError):
                store.load_unresolved(evidence.plan_id)

    def test_tampered_nested_field_without_matching_hash_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = LocalHistoricalBackfillSessionGapStore(root)
            evidence = gap_evidence()
            store.put(evidence)
            path = gap_path(root, evidence)
            original = json.loads(path.read_text(encoding="utf-8"))
            original["provider_instrument_id"] = "999999"
            path.write_text(json.dumps(original), encoding="utf-8")

            with self.assertRaises(HistoricalBackfillGapIntegrityError):
                store.load_unresolved(evidence.plan_id)

    def test_tampered_evidence_id_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = LocalHistoricalBackfillSessionGapStore(root)
            evidence = gap_evidence()
            store.put(evidence)
            path = gap_path(root, evidence)
            original = json.loads(path.read_text(encoding="utf-8"))
            original["evidence_id"] = "0" * 64
            path.write_text(json.dumps(original), encoding="utf-8")

            with self.assertRaises(HistoricalBackfillGapIntegrityError):
                store.load_unresolved(evidence.plan_id)

    def test_tampered_session_disagreeing_with_its_file_path_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = LocalHistoricalBackfillSessionGapStore(root)
            evidence = gap_evidence()
            store.put(evidence)
            path = gap_path(root, evidence)
            original = json.loads(path.read_text(encoding="utf-8"))
            original["session"] = "2026-07-16"
            path.write_text(json.dumps(original), encoding="utf-8")

            with self.assertRaises(HistoricalBackfillGapIntegrityError):
                store.load_unresolved(evidence.plan_id)


if __name__ == "__main__":
    unittest.main()
