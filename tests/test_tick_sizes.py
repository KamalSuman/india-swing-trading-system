from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from india_swing.promotion import PromotionCapability
from india_swing.reference import ReferenceReadiness
from india_swing.reference_data import LocalReferenceArtifactStore
from india_swing.reference_data.config import REFERENCE_DATA_ROOT_ENV
from india_swing.tick_sizes import (
    TICK_SIZE_ROOT_ENV,
    LocalTickSizeSnapshotStore,
    TickSizeIntegrityError,
    decode_tick_size_snapshot,
    encode_tick_size_snapshot,
    materialize_collection_tick_sizes,
    tick_size_promotion_evidence,
)
from india_swing.tick_sizes.cli import main as tick_size_main
from tests.test_reference_data_import import (
    FIRST_SEEN,
    VALIDATED,
    clock_sequence,
    security_master_bytes,
    security_row,
)


class TickSizeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.reference_root = self.root / "reference"
        self.tick_root = self.root / "ticks"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def source(self, rows=None):
        path = self.root / "NSE_CM_security_15072026.csv.gz"
        path.write_bytes(security_master_bytes(rows))
        return LocalReferenceArtifactStore(
            self.reference_root,
            clock=clock_sequence(FIRST_SEEN, VALIDATED),
        ).import_security_master(path)

    def test_materializes_bid_interval_as_exact_rupees_without_iso_substitution(self) -> None:
        source = self.source(
            [
                security_row(BidIntrvl="25"),
                security_row(
                    FinInstrmId="2000",
                    TckrSymb="BOND",
                    SctyTpFlg="1",
                ),
            ]
        )
        snapshot = materialize_collection_tick_sizes(
            source,
            cutoff=VALIDATED + timedelta(minutes=1),
        )

        self.assertEqual(len(snapshot.observations), 1)
        self.assertEqual(snapshot.observations[0].bid_interval_paise, 25)
        self.assertEqual(snapshot.observations[0].tick_size_rupees, Decimal("0.25"))
        self.assertEqual(snapshot.readiness, ReferenceReadiness.COLLECTION_ONLY)
        self.assertFalse(snapshot.actionable)
        snapshot.verify_content_identity()

    def test_cutoff_before_source_validation_is_rejected(self) -> None:
        source = self.source()

        with self.assertRaisesRegex(ValueError, "unavailable"):
            materialize_collection_tick_sizes(
                source,
                cutoff=VALIDATED - timedelta(seconds=1),
            )

    def test_populated_reserved_tick_size_field_requires_contract_review(self) -> None:
        source = self.source([security_row(TickSz="0.05")])

        with self.assertRaisesRegex(ValueError, "requires review"):
            materialize_collection_tick_sizes(
                source,
                cutoff=VALIDATED + timedelta(minutes=1),
            )

    def test_codec_store_replay_and_promotion_evidence(self) -> None:
        source = self.source([security_row(BidIntrvl="5")])
        snapshot = materialize_collection_tick_sizes(
            source,
            cutoff=VALIDATED + timedelta(minutes=1),
        )
        self.assertEqual(
            decode_tick_size_snapshot(encode_tick_size_snapshot(snapshot)),
            snapshot,
        )
        store = LocalTickSizeSnapshotStore(self.tick_root, self.reference_root)
        self.assertEqual(store.put(snapshot), snapshot)
        self.assertEqual(store.put(snapshot), snapshot)
        self.assertEqual(store.list_snapshots(), (snapshot,))

        evidence = tick_size_promotion_evidence(snapshot)
        self.assertEqual(evidence.capability, PromotionCapability.TICK_SIZES)
        self.assertEqual(evidence.source_snapshot_ids, (snapshot.snapshot_id,))
        self.assertFalse(evidence.actionable)

    def test_stored_snapshot_tampering_is_detected(self) -> None:
        source = self.source()
        snapshot = materialize_collection_tick_sizes(
            source,
            cutoff=VALIDATED + timedelta(minutes=1),
        )
        store = LocalTickSizeSnapshotStore(self.tick_root, self.reference_root)
        store.put(snapshot)
        path = store.path_for(snapshot.snapshot_id)
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["observations"][0]["bid_interval_paise"] = 999
        path.write_text(json.dumps(payload), encoding="utf-8")

        with self.assertRaises(TickSizeIntegrityError):
            store.get(snapshot.snapshot_id)

    def test_cli_materializes_and_lists_with_sanitized_failures(self) -> None:
        source = self.source([security_row(BidIntrvl="10")])
        stdout = io.StringIO()
        with patch.dict(
            os.environ,
            {
                REFERENCE_DATA_ROOT_ENV: str(self.reference_root),
                TICK_SIZE_ROOT_ENV: str(self.tick_root),
            },
            clear=True,
        ), patch("sys.stdout", stdout):
            result = tick_size_main(
                [
                    "materialize",
                    "--security-master-id",
                    source.manifest.artifact_id,
                    "--cutoff",
                    (VALIDATED + timedelta(minutes=1)).isoformat(),
                ]
            )
            response = json.loads(stdout.getvalue())
            stdout.seek(0)
            stdout.truncate(0)
            list_result = tick_size_main(["list"])
            listed = json.loads(stdout.getvalue())

        self.assertEqual((result, list_result), (0, 0))
        self.assertEqual(response["observation_count"], 1)
        self.assertEqual(response["bid_interval_paise_distribution"], {"10": 1})
        self.assertEqual(len(listed["snapshots"]), 1)

        stderr = io.StringIO()
        with patch("sys.stderr", stderr):
            bad_result = tick_size_main(
                ["show", "--snapshot-id", "access_token=distinct-secret"]
            )
        self.assertEqual(bad_result, 2)
        self.assertNotIn("distinct-secret", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
