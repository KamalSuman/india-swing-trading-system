from __future__ import annotations

import json
import tempfile
import unittest
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from india_swing.daily_reports import LocalDailyBundleArtifactStore
from india_swing.daily_reports.parser import NSE_DAILY_BUNDLE_FILENAME
from india_swing.historical_prices import (
    LocalHistoricalPriceArtifactStore,
    materialize_nse_eod_session,
)
from india_swing.liquidity import (
    LocalLiquiditySnapshotStore,
    LiquidityIntegrityError,
    decode_liquidity_snapshot,
    encode_liquidity_snapshot,
    liquidity_promotion_evidence,
    materialize_collection_liquidity,
)
from india_swing.promotion import PromotionCapability
from tests.test_historical_prices import (
    CUTOFF,
    FIRST_SEEN,
    SESSION,
    VALIDATED,
    _bundle_bytes,
    _clock,
)


class LiquidityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.daily_root = self.root / "daily"
        self.history_root = self.root / "history"
        self.liquidity_root = self.root / "liquidity"
        source = self.root / NSE_DAILY_BUNDLE_FILENAME
        source.write_bytes(_bundle_bytes())
        bundle = LocalDailyBundleArtifactStore(
            self.daily_root,
            clock=_clock(FIRST_SEEN, VALIDATED),
        ).import_bundle(source)
        self.price = materialize_nse_eod_session(
            bundle,
            market_session=SESSION,
            cutoff=CUTOFF,
        )
        LocalHistoricalPriceArtifactStore(
            self.history_root,
            self.daily_root,
        ).put(self.price)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def snapshot(self):
        return materialize_collection_liquidity(
            (self.price,),
            decision_cutoff=CUTOFF,
            minimum_history_sessions=120,
        )

    def test_materializes_exact_medians_and_keeps_short_history_blocked(self) -> None:
        snapshot = self.snapshot()
        infy = next(value for value in snapshot.observations if value.series == "EQ")

        self.assertEqual(infy.median_daily_traded_value, Decimal("160500.00"))
        self.assertEqual(infy.median_daily_volume, Decimal("100"))
        self.assertEqual(infy.median_delivery_percent, Decimal("50.00"))
        self.assertEqual(infy.observed_session_count, 1)
        self.assertFalse(infy.meets_minimum_history)
        self.assertIn("INSUFFICIENT_HISTORY", snapshot.reason_codes)
        snapshot.verify_content_identity()

    def test_future_source_and_duplicate_session_fail_closed(self) -> None:
        with self.assertRaisesRegex(LiquidityIntegrityError, "unavailable"):
            materialize_collection_liquidity(
                (self.price,),
                decision_cutoff=VALIDATED - timedelta(seconds=1),
            )
        with self.assertRaisesRegex(LiquidityIntegrityError, "unique"):
            materialize_collection_liquidity(
                (self.price, self.price),
                decision_cutoff=CUTOFF,
            )

    def test_codec_store_replay_and_promotion_evidence(self) -> None:
        snapshot = self.snapshot()
        self.assertEqual(
            decode_liquidity_snapshot(encode_liquidity_snapshot(snapshot)),
            snapshot,
        )
        store = LocalLiquiditySnapshotStore(
            self.liquidity_root,
            self.history_root,
            self.daily_root,
        )
        self.assertEqual(store.put(snapshot), snapshot)
        self.assertEqual(store.put(snapshot), snapshot)
        self.assertEqual(store.list_snapshots(), (snapshot,))

        evidence = liquidity_promotion_evidence(snapshot)
        self.assertEqual(evidence.capability, PromotionCapability.LIQUIDITY)
        self.assertEqual(evidence.source_snapshot_ids, (snapshot.snapshot_id,))
        self.assertFalse(evidence.actionable)

    def test_stored_snapshot_tampering_is_detected(self) -> None:
        snapshot = self.snapshot()
        store = LocalLiquiditySnapshotStore(
            self.liquidity_root,
            self.history_root,
            self.daily_root,
        )
        store.put(snapshot)
        path = store.path_for(snapshot.snapshot_id)
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["observations"][0]["median_daily_volume"] = "999999"
        path.write_text(json.dumps(payload), encoding="utf-8")

        with self.assertRaises(LiquidityIntegrityError):
            store.get(snapshot.snapshot_id)


if __name__ == "__main__":
    unittest.main()
