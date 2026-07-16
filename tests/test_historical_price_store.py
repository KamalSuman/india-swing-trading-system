from __future__ import annotations

import io
import json
import os
import shutil
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from india_swing.daily_reports.artifact_store import (
    RAW_FILENAME as DAILY_RAW_FILENAME,
    LocalDailyBundleArtifactStore,
)
from india_swing.historical_prices import (
    ARTIFACT_FILENAME,
    HISTORICAL_PRICE_DATASET,
    MANIFEST_FILENAME,
    HistoricalPriceArtifactNotFound,
    HistoricalPriceIntegrityError,
    HistoricalPricesConfig,
    LocalHistoricalPriceArtifactStore,
    materialize_nse_eod_session,
)
from india_swing.historical_prices.cli import main as historical_prices_main
from tests.test_historical_prices import (
    CUTOFF,
    FIRST_SEEN,
    SESSION,
    VALIDATED,
    _bundle_bytes,
    _clock,
)
from india_swing.daily_reports.parser import NSE_DAILY_BUNDLE_FILENAME


class HistoricalPriceArtifactStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        source_path = self.root / "source" / NSE_DAILY_BUNDLE_FILENAME
        source_path.parent.mkdir()
        source_path.write_bytes(_bundle_bytes())
        self.daily_root = self.root / "daily"
        self.bundle = LocalDailyBundleArtifactStore(
            self.daily_root,
            clock=_clock(FIRST_SEEN, VALIDATED),
        ).import_bundle(source_path)
        self.artifact = materialize_nse_eod_session(
            self.bundle,
            market_session=SESSION,
            cutoff=CUTOFF,
        )
        self.price_root = self.root / "prices"
        self.store = LocalHistoricalPriceArtifactStore(
            self.price_root,
            self.daily_root,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_store_is_create_once_and_get_replays_exact_source(self) -> None:
        first = self.store.put(self.artifact)
        second = self.store.put(self.artifact)
        loaded = self.store.get(self.artifact.artifact_id)

        self.assertEqual(first.path, second.path)
        self.assertEqual(loaded.path, first.path)
        self.assertEqual(loaded.artifact, self.artifact)
        self.assertEqual(loaded.payload_bytes, first.payload_bytes)
        self.assertEqual(
            sorted(path.name for path in loaded.path.iterdir()),
            [ARTIFACT_FILENAME, MANIFEST_FILENAME],
        )
        self.assertEqual(loaded.manifest.bar_count, 2)
        self.assertEqual(loaded.manifest.udiff_row_count, 2)
        self.assertEqual(loaded.manifest.full_delivery_row_count, 1)
        self.assertEqual(
            loaded.manifest.source_bundle_manifest_id,
            self.bundle.manifest.manifest_id,
        )

    def test_payload_and_manifest_tampering_are_rejected(self) -> None:
        stored = self.store.put(self.artifact)
        payload_path = stored.path / ARTIFACT_FILENAME
        payload_path.write_bytes(payload_path.read_bytes() + b" ")
        with self.assertRaisesRegex(HistoricalPriceIntegrityError, "hash mismatch"):
            self.store.get(self.artifact.artifact_id)

        shutil.rmtree(stored.path)
        stored = self.store.put(self.artifact)
        manifest_path = stored.path / MANIFEST_FILENAME
        value = json.loads(manifest_path.read_text(encoding="utf-8"))
        value["payload_byte_count"] += 1
        manifest_path.write_text(json.dumps(value), encoding="utf-8")
        with self.assertRaises(HistoricalPriceIntegrityError):
            self.store.get(self.artifact.artifact_id)

    def test_source_tampering_is_rejected_during_get_replay(self) -> None:
        self.store.put(self.artifact)
        (self.bundle.path / DAILY_RAW_FILENAME).write_bytes(b"not-the-sealed-bundle")

        with self.assertRaisesRegex(HistoricalPriceIntegrityError, "source"):
            self.store.get(self.artifact.artifact_id)

    def test_duplicate_partition_and_unexpected_entries_fail_closed(self) -> None:
        stored = self.store.put(self.artifact)
        duplicate = (
            self.price_root
            / HISTORICAL_PRICE_DATASET
            / "2026-07-14"
            / self.artifact.artifact_id
        )
        duplicate.parent.mkdir()
        shutil.copytree(stored.path, duplicate)
        with self.assertRaisesRegex(HistoricalPriceIntegrityError, "multiple partitions"):
            self.store.get(self.artifact.artifact_id)

        shutil.rmtree(duplicate)
        (stored.path / "unexpected.txt").write_text("x", encoding="utf-8")
        with self.assertRaisesRegex(HistoricalPriceIntegrityError, "unexpected"):
            self.store.get(self.artifact.artifact_id)

    def test_link_payload_is_rejected_when_platform_allows_links(self) -> None:
        stored = self.store.put(self.artifact)
        payload = stored.path / ARTIFACT_FILENAME
        external = self.root / "external-artifact.json"
        external.write_bytes(payload.read_bytes())
        payload.unlink()
        try:
            os.symlink(external, payload)
        except OSError as exc:
            self.skipTest(f"platform does not permit symlinks: {type(exc).__name__}")
        with self.assertRaisesRegex(HistoricalPriceIntegrityError, "regular files"):
            self.store.get(self.artifact.artifact_id)

    def test_failed_atomic_publish_leaves_no_partial_artifact(self) -> None:
        with patch(
            "india_swing.historical_prices.artifact_store.os.rename",
            side_effect=OSError("publish failed"),
        ):
            with self.assertRaisesRegex(OSError, "publish failed"):
                self.store.put(self.artifact)

        artifact_files = [
            path
            for path in self.price_root.rglob("*")
            if path.is_file() and path.name in {ARTIFACT_FILENAME, MANIFEST_FILENAME}
        ]
        self.assertEqual(artifact_files, [])

    def test_not_found_and_invalid_identifiers_are_distinct(self) -> None:
        with self.assertRaises(ValueError):
            self.store.get("not-a-hash")
        with self.assertRaises(HistoricalPriceArtifactNotFound):
            self.store.get("a" * 64)

    def test_config_reads_both_roots(self) -> None:
        config = HistoricalPricesConfig.from_env(
            {
                "INDIA_SWING_HISTORICAL_PRICES_ROOT": "custom/prices",
                "INDIA_SWING_DAILY_REPORTS_ROOT": "custom/daily",
            }
        )
        self.assertEqual(config.data_root, Path("custom/prices"))
        self.assertEqual(config.daily_reports_root, Path("custom/daily"))
        with self.assertRaises(ValueError):
            HistoricalPricesConfig.from_env(
                {"INDIA_SWING_HISTORICAL_PRICES_ROOT": ""}
            )

    def test_cli_materializes_summary_and_sanitizes_failures(self) -> None:
        output = io.StringIO()
        error = io.StringIO()
        environment = {
            "INDIA_SWING_HISTORICAL_PRICES_ROOT": str(self.price_root),
            "INDIA_SWING_DAILY_REPORTS_ROOT": str(self.daily_root),
        }
        with (
            patch.dict(os.environ, environment, clear=False),
            redirect_stdout(output),
            redirect_stderr(error),
        ):
            result = historical_prices_main(
                [
                    "materialize",
                    "--daily-bundle-id",
                    self.bundle.manifest.artifact_id,
                    "--market-session",
                    SESSION.isoformat(),
                    "--cutoff",
                    CUTOFF.isoformat(),
                ]
            )
        self.assertEqual(result, 0)
        self.assertEqual(error.getvalue(), "")
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["status"], "COMPLETE")
        self.assertEqual(payload["bar_count"], 2)
        self.assertEqual(payload["readiness"], "COLLECTION_ONLY")
        self.assertFalse(payload["actionable"])

        output = io.StringIO()
        error = io.StringIO()
        secret = "must-not-appear"
        with redirect_stdout(output), redirect_stderr(error):
            result = historical_prices_main(
                ["materialize", "--daily-bundle-id", secret]
            )
        self.assertEqual(result, 2)
        self.assertEqual(output.getvalue(), "")
        self.assertNotIn(secret, error.getvalue())
        self.assertEqual(
            json.loads(error.getvalue()),
            {
                "status": "FAILED",
                "error_type": "HistoricalPricesArgumentError",
            },
        )


if __name__ == "__main__":
    unittest.main()
