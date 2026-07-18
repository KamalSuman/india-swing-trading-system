from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from india_swing.promotion import PromotionCapability
from india_swing.reference import ReferenceReadiness
from india_swing.reference_data import LocalReferenceArtifactStore
from india_swing.reference_data.config import REFERENCE_DATA_ROOT_ENV
from india_swing.universe import (
    COLLECTION_UNIVERSE_ROOT_ENV,
    CollectionUniverseDisposition,
    CollectionUniverseIntegrityError,
    LocalCollectionUniverseSnapshotStore,
    decode_collection_universe_snapshot,
    encode_collection_universe_snapshot,
    materialize_collection_universe,
    universe_promotion_evidence,
)
from india_swing.universe.cli import main as universe_main
from tests.test_reference_data_import import (
    FIRST_SEEN,
    VALIDATED,
    clock_sequence,
    security_master_bytes,
    security_row,
)


CALENDAR_ID = "c" * 64


class CollectionUniverseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.reference_root = self.root / "reference"
        self.universe_root = self.root / "universe"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def source(self, rows=None):
        path = self.root / "NSE_CM_security_15072026.csv.gz"
        path.write_bytes(security_master_bytes(rows))
        return LocalReferenceArtifactStore(
            self.reference_root,
            clock=clock_sequence(FIRST_SEEN, VALIDATED),
        ).import_security_master(path)

    def snapshot(self, source=None):
        source = self.source() if source is None else source
        return materialize_collection_universe(
            source,
            cutoff=VALIDATED + timedelta(minutes=1),
            calendar_snapshot_id=CALENDAR_ID,
        )

    def test_preserves_full_source_scope_without_market_cap_cutoff(self) -> None:
        source = self.source(
            [
                security_row(),
                security_row(
                    FinInstrmId="2000",
                    TckrSymb="BOND",
                    SctyTpFlg="1",
                ),
                security_row(
                    FinInstrmId="3000",
                    TckrSymb="NSETEST",
                ),
            ]
        )
        snapshot = self.snapshot(source)

        self.assertEqual(len(snapshot.observations), 3)
        self.assertEqual(len(snapshot.in_scope_observations), 1)
        self.assertEqual(
            {value.disposition for value in snapshot.observations},
            {
                CollectionUniverseDisposition.IN_SCOPE_UNVERIFIED_EQUITY,
                CollectionUniverseDisposition.EXCLUDED_NON_EQUITY,
                CollectionUniverseDisposition.EXCLUDED_TEST_SECURITY,
            },
        )
        self.assertEqual(snapshot.readiness, ReferenceReadiness.COLLECTION_ONLY)
        self.assertFalse(snapshot.actionable)
        self.assertIn("STABLE_IDENTITY_UNAVAILABLE", snapshot.reason_codes)
        snapshot.verify_content_identity()

    def test_cutoff_before_source_validation_is_rejected(self) -> None:
        source = self.source()

        with self.assertRaisesRegex(ValueError, "unavailable"):
            materialize_collection_universe(
                source,
                cutoff=VALIDATED - timedelta(seconds=1),
                calendar_snapshot_id=CALENDAR_ID,
            )

    def test_codec_store_replay_and_promotion_evidence(self) -> None:
        snapshot = self.snapshot()
        self.assertEqual(
            decode_collection_universe_snapshot(
                encode_collection_universe_snapshot(snapshot)
            ),
            snapshot,
        )
        store = LocalCollectionUniverseSnapshotStore(
            self.universe_root,
            self.reference_root,
        )
        self.assertEqual(store.put(snapshot), snapshot)
        self.assertEqual(store.put(snapshot), snapshot)
        self.assertEqual(store.list_snapshots(), (snapshot,))

        evidence = universe_promotion_evidence(snapshot)
        self.assertEqual(evidence.capability, PromotionCapability.UNIVERSE)
        self.assertEqual(evidence.source_snapshot_ids, (snapshot.snapshot_id,))
        self.assertFalse(evidence.actionable)

    def test_stored_snapshot_tampering_is_detected(self) -> None:
        snapshot = self.snapshot()
        store = LocalCollectionUniverseSnapshotStore(
            self.universe_root,
            self.reference_root,
        )
        store.put(snapshot)
        path = store.path_for(snapshot.snapshot_id)
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["observations"][0]["normal_market_eligible"] = not payload[
            "observations"
        ][0]["normal_market_eligible"]
        path.write_text(json.dumps(payload), encoding="utf-8")

        with self.assertRaises(CollectionUniverseIntegrityError):
            store.get(snapshot.snapshot_id)

    def test_cli_materializes_and_sanitizes_failures(self) -> None:
        source = self.source()
        stdout = io.StringIO()
        with patch.dict(
            os.environ,
            {
                REFERENCE_DATA_ROOT_ENV: str(self.reference_root),
                COLLECTION_UNIVERSE_ROOT_ENV: str(self.universe_root),
            },
            clear=True,
        ), patch("sys.stdout", stdout):
            result = universe_main(
                [
                    "materialize",
                    "--security-master-id",
                    source.manifest.artifact_id,
                    "--calendar-snapshot-id",
                    CALENDAR_ID,
                    "--cutoff",
                    (VALIDATED + timedelta(minutes=1)).isoformat(),
                ]
            )
            response = json.loads(stdout.getvalue())

        self.assertEqual(result, 0)
        self.assertEqual(response["broad_equity_scope_count"], 1)
        self.assertIsNone(response["market_cap_cutoff"])

        stderr = io.StringIO()
        with patch("sys.stderr", stderr):
            bad_result = universe_main(
                ["show", "--snapshot-id", "access_token=distinct-secret"]
            )
        self.assertEqual(bad_result, 2)
        self.assertNotIn("distinct-secret", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
