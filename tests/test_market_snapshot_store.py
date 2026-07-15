from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from india_swing.market_data.snapshot_store import (
    LocalMarketSnapshotStore,
    MarketSnapshotIntegrityError,
    MarketSnapshotNotFound,
    MarketSnapshotSecretError,
    MarketSnapshotStale,
)


UTC = timezone.utc
DATASET = "kite-instruments-NSE"
SELECTION_KEY = "exchange=NSE"


class LocalMarketSnapshotStoreTests(unittest.TestCase):
    def put(self, store, observed_at, value="one"):
        return store.put(
            dataset=DATASET,
            selection_key=SELECTION_KEY,
            provider="ZERODHA_KITE",
            provider_version="kiteconnect/5.2.0",
            observed_at=observed_at,
            normalized_payload={"records": [{"symbol": value}]},
        )

    def test_snapshot_is_create_once_hash_verified_and_idempotent(self) -> None:
        observed = datetime(2026, 7, 15, 8, 30, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalMarketSnapshotStore(Path(temp_dir))
            first = self.put(store, observed)
            original_bytes = (first.path / "payload.json").read_bytes()
            second = self.put(store, observed)
            loaded = store.get(first.manifest.dataset, first.manifest.snapshot_id)

            self.assertEqual(first.path, second.path)
            self.assertEqual(loaded.manifest, first.manifest)
            self.assertEqual(loaded.normalized_payload, {"records": [{"symbol": "one"}]})
            self.assertEqual((loaded.path / "payload.json").read_bytes(), original_bytes)
            self.assertEqual(
                sorted(path.name for path in loaded.path.iterdir()),
                ["manifest.json", "payload.json"],
            )

    def test_concurrent_identical_puts_publish_one_complete_snapshot(self) -> None:
        observed = datetime(2026, 7, 15, 8, 30, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalMarketSnapshotStore(Path(temp_dir))
            with ThreadPoolExecutor(max_workers=8) as pool:
                results = list(pool.map(lambda _: self.put(store, observed), range(32)))

            paths = {result.path for result in results}
            self.assertEqual(len(paths), 1)
            target = paths.pop()
            self.assertEqual(
                sorted(item.name for item in target.iterdir()),
                ["manifest.json", "payload.json"],
            )
            self.assertEqual(
                [path for path in Path(temp_dir).rglob(".*") if path.is_dir()],
                [],
            )

    def test_latest_snapshot_is_semantic_cutoff_and_freshness_bounded(self) -> None:
        first_time = datetime(2026, 7, 14, 8, 30, tzinfo=UTC)
        second_time = datetime(2026, 7, 15, 8, 30, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalMarketSnapshotStore(Path(temp_dir))
            first = self.put(store, first_time, "old")
            self.put(store, second_time, "new")

            selected = store.latest_at_or_before(
                DATASET,
                SELECTION_KEY,
                first_time + timedelta(hours=1),
                max_age=timedelta(hours=2),
            )

            self.assertEqual(selected.manifest.snapshot_id, first.manifest.snapshot_id)
            with self.assertRaises(MarketSnapshotNotFound):
                store.latest_at_or_before(
                    DATASET,
                    SELECTION_KEY,
                    first_time - timedelta(seconds=1),
                    max_age=timedelta(hours=2),
                )
            with self.assertRaises(MarketSnapshotNotFound):
                store.latest_at_or_before(
                    DATASET,
                    "exchange=BSE",
                    second_time,
                    max_age=timedelta(hours=2),
                )
            with self.assertRaises(MarketSnapshotStale):
                store.latest_at_or_before(
                    DATASET,
                    SELECTION_KEY,
                    second_time + timedelta(days=30),
                    max_age=timedelta(hours=36),
                )

    def test_snapshot_tampering_is_detected(self) -> None:
        observed = datetime(2026, 7, 15, 8, 30, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalMarketSnapshotStore(Path(temp_dir))
            stored = self.put(store, observed)
            (stored.path / "payload.json").write_text(
                json.dumps({"records": [{"symbol": "tampered"}]}),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(MarketSnapshotIntegrityError, "hash mismatch"):
                store.get(stored.manifest.dataset, stored.manifest.snapshot_id)

    def test_manifest_fields_and_record_count_are_content_bound(self) -> None:
        observed = datetime(2026, 7, 15, 8, 30, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalMarketSnapshotStore(Path(temp_dir))
            stored = self.put(store, observed)
            manifest_path = stored.path / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["record_count"] = 999
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            with self.assertRaises(MarketSnapshotIntegrityError):
                store.get(DATASET, stored.manifest.snapshot_id)

    def test_dataset_and_date_partitions_are_bound_to_manifest(self) -> None:
        observed = datetime(2026, 7, 15, 8, 30, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = LocalMarketSnapshotStore(root)
            stored = self.put(store, observed)
            copied = root / "other" / "2026-07-15" / stored.manifest.snapshot_id
            copied.parent.mkdir(parents=True)
            shutil.copytree(stored.path, copied)

            with self.assertRaisesRegex(MarketSnapshotIntegrityError, "dataset"):
                store.get("other", stored.manifest.snapshot_id)

            misplaced = root / DATASET / "2026-07-14" / stored.manifest.snapshot_id
            misplaced.parent.mkdir(parents=True)
            shutil.copytree(stored.path, misplaced)
            with self.assertRaises(MarketSnapshotIntegrityError):
                store.get(DATASET, stored.manifest.snapshot_id)

    def test_unsafe_path_components_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalMarketSnapshotStore(Path(temp_dir))
            with self.assertRaisesRegex(ValueError, "unsafe"):
                store.put(
                    dataset="../escape",
                    selection_key=SELECTION_KEY,
                    provider="ZERODHA_KITE",
                    provider_version="v1",
                    observed_at=datetime(2026, 7, 15, tzinfo=UTC),
                    normalized_payload={"records": []},
                )
            with self.assertRaises(ValueError):
                store.get(DATASET, "../escape")

    def test_failed_publish_leaves_no_partial_snapshot(self) -> None:
        observed = datetime(2026, 7, 15, 8, 30, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalMarketSnapshotStore(Path(temp_dir))
            with patch(
                "india_swing.market_data.snapshot_store.os.rename",
                side_effect=OSError("publish failed"),
            ):
                with self.assertRaisesRegex(OSError, "publish failed"):
                    self.put(store, observed)

            remaining = [path for path in Path(temp_dir).rglob("*") if path.is_file()]
            self.assertEqual(remaining, [])

    def test_sensitive_normalized_fields_are_rejected_before_any_write(self) -> None:
        observed = datetime(2026, 7, 15, 8, 30, tzinfo=UTC)
        sensitive_payloads = (
            {"records": [{"access_token": "distinct-secret-token"}]},
            {"records": [{"Cookie": "session=distinct-cookie"}]},
            {"records": [{"Set-Cookie": "distinct-cookie"}]},
            {"records": [{"note": "Authorization: token distinct-secret"}]},
        )
        for payload in sensitive_payloads:
            with self.subTest(payload=payload), tempfile.TemporaryDirectory() as temp_dir:
                store = LocalMarketSnapshotStore(Path(temp_dir))
                with self.assertRaises(MarketSnapshotSecretError):
                    store.put(
                        dataset=DATASET,
                        selection_key=SELECTION_KEY,
                        provider="ZERODHA_KITE",
                        provider_version="v1",
                        observed_at=observed,
                        normalized_payload=payload,
                    )
                self.assertEqual(list(Path(temp_dir).rglob("*")), [])

    def test_arbitrary_raw_payloads_are_disabled_before_any_write(self) -> None:
        observed = datetime(2026, 7, 15, 8, 30, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalMarketSnapshotStore(Path(temp_dir))
            with self.assertRaises(MarketSnapshotSecretError):
                store.put(
                    dataset=DATASET,
                    selection_key=SELECTION_KEY,
                    provider="ZERODHA_KITE",
                    provider_version="v1",
                    observed_at=observed,
                    normalized_payload={"records": []},
                    raw_payload=b"Authorization: token api:distinct-secret",
                )
            self.assertEqual(list(Path(temp_dir).rglob("*")), [])


if __name__ == "__main__":
    unittest.main()
