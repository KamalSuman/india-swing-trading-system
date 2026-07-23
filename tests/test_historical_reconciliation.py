from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from india_swing.daily_reports.artifact_store import LocalDailyBundleArtifactStore
from india_swing.daily_reports.parser import NSE_DAILY_BUNDLE_FILENAME
from india_swing.historical_prices import materialize_nse_eod_session
from india_swing.market_data.collection import HistoricalReconciliationCollector
from india_swing.market_data.models import HistoricalDailyCandleBatch
from india_swing.market_data.reconciliation import (
    HistoricalCandleReconciliationReport,
    HistoricalReconciliationError,
    HistoricalReconciliationIntegrityError,
    HistoricalReconciliationStatus,
    reconcile_historical_batch,
)
from india_swing.market_data.snapshot_store import LocalMarketSnapshotStore
from tests.test_historical_prices import (
    CUTOFF,
    FIRST_SEEN,
    SESSION,
    VALIDATED,
    _bundle_bytes,
    _clock,
)
from tests.test_upstox_market_data import (
    FakeTransport,
    adapter,
    binding,
    candle_row,
    historical_request,
    response,
    success_body,
)


UTC = timezone.utc
RECONCILED_AT = datetime(2026, 7, 22, 10, 0, tzinfo=UTC)


def nse_artifact(root: Path):
    source = root / "source" / NSE_DAILY_BUNDLE_FILENAME
    source.parent.mkdir(parents=True)
    source.write_bytes(_bundle_bytes())
    bundle = LocalDailyBundleArtifactStore(
        root / "daily",
        clock=_clock(FIRST_SEEN, VALIDATED),
    ).import_bundle(source)
    return materialize_nse_eod_session(
        bundle,
        market_session=SESSION,
        cutoff=CUTOFF,
    )


def provider_batch(
    *,
    close: object = 1610.0,
    volume: object = 100,
    instrument_binding=None,
) -> HistoricalDailyCandleBatch:
    request = historical_request(
        (SESSION,),
        instrument_binding=instrument_binding,
    )
    body = success_body(
        [
            candle_row(
                SESSION,
                open_value=1600.0,
                high=1620.0,
                low=1590.0,
                close=close,
                volume=volume,
            )
        ]
    )
    return adapter(FakeTransport(response(body))).fetch_historical_daily(request)


class HistoricalCandleReconciliationTests(unittest.TestCase):
    def test_exact_nse_ohlcv_match_passes_and_round_trips(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            artifact = nse_artifact(root)
            batch = provider_batch()

            report = reconcile_historical_batch(
                batch,
                (artifact,),
                reconciled_at=RECONCILED_AT,
            )
            store = LocalMarketSnapshotStore(root / "market")
            stored = HistoricalReconciliationCollector(store).collect(report)
            loaded = store.get(
                stored.manifest.dataset,
                stored.manifest.snapshot_id,
            )

        self.assertTrue(report.passed)
        self.assertFalse(report.actionable)
        self.assertEqual(
            report.rows[0].status,
            HistoricalReconciliationStatus.MATCH,
        )
        self.assertEqual(report.rows[0].differences, ())
        self.assertEqual(stored.manifest.record_count, 1)
        self.assertIsInstance(
            loaded.normalized_payload,
            HistoricalCandleReconciliationReport,
        )
        self.assertEqual(loaded.normalized_payload, report)
        loaded.normalized_payload.verify_content_identity()

    def test_close_and_volume_mismatches_are_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact = nse_artifact(Path(temp_dir))
            report = reconcile_historical_batch(
                provider_batch(close=1608.0, volume=99),
                (artifact,),
                reconciled_at=RECONCILED_AT,
            )

        self.assertFalse(report.passed)
        row = report.rows[0]
        self.assertEqual(
            row.status,
            HistoricalReconciliationStatus.MISMATCH,
        )
        self.assertEqual(
            tuple(value.field_name for value in row.differences),
            ("close", "volume"),
        )
        self.assertEqual(row.differences[0].provider_value, "1608.0")
        self.assertEqual(row.differences[0].nse_value, "1610.00")

    def test_missing_listing_bar_is_a_failed_result_not_a_false_match(self) -> None:
        tcs_binding = replace(
            binding(),
            provider_instrument_id="NSE_EQ|INE467B01029",
            listing_key="NSE:TCS",
            isin="INE467B01029",
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact = nse_artifact(Path(temp_dir))
            report = reconcile_historical_batch(
                provider_batch(instrument_binding=tcs_binding),
                (artifact,),
                reconciled_at=RECONCILED_AT,
            )

        self.assertFalse(report.passed)
        self.assertEqual(
            report.rows[0].status,
            HistoricalReconciliationStatus.MISSING_NSE_BAR,
        )
        self.assertIsNone(report.rows[0].nse_bar_id)

    def test_artifact_sessions_must_exactly_cover_provider_sessions(self) -> None:
        second = date(2026, 7, 16)
        request = historical_request((SESSION, second))
        body = success_body(
            [
                candle_row(
                    second,
                    open_value=1600,
                    high=1620,
                    low=1590,
                    close=1610,
                    volume=100,
                ),
                candle_row(
                    SESSION,
                    open_value=1600,
                    high=1620,
                    low=1590,
                    close=1610,
                    volume=100,
                ),
            ]
        )
        batch = adapter(FakeTransport(response(body))).fetch_historical_daily(
            request
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact = nse_artifact(Path(temp_dir))
            with self.assertRaisesRegex(
                HistoricalReconciliationError,
                "exactly equal",
            ):
                reconcile_historical_batch(
                    batch,
                    (artifact,),
                    reconciled_at=RECONCILED_AT,
                )

    def test_reconciliation_cannot_predate_provider_or_nse_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact = nse_artifact(Path(temp_dir))
            batch = provider_batch()

        with self.assertRaisesRegex(
            HistoricalReconciliationError,
            "predates",
        ):
            reconcile_historical_batch(
                batch,
                (artifact,),
                reconciled_at=batch.observed_at - timedelta(seconds=1),
            )

    def test_nested_report_tampering_is_detected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact = nse_artifact(Path(temp_dir))
            report = reconcile_historical_batch(
                provider_batch(),
                (artifact,),
                reconciled_at=RECONCILED_AT,
            )
        object.__setattr__(
            report.rows[0],
            "nse_artifact_id",
            "0" * 64,
        )

        with self.assertRaises(HistoricalReconciliationIntegrityError):
            report.verify_content_identity()


if __name__ == "__main__":
    unittest.main()
