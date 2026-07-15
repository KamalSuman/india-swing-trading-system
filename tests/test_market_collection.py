from __future__ import annotations

import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from india_swing.market_data.collection import InstrumentLineageError, MarketDataCollector
from india_swing.market_data.models import (
    DailyCandle,
    DailyCandleArchive,
    DailyCandleBatch,
    InstrumentBatch,
    KiteInstrument,
    NseSessionFinality,
)
from india_swing.market_data.snapshot_store import LocalMarketSnapshotStore


IST = timezone(timedelta(hours=5, minutes=30))
SESSION = date(2026, 7, 15)
MASTER_OBSERVED = datetime(2026, 7, 15, 8, 30, tzinfo=IST)
CANDLE_OBSERVED = datetime(2026, 7, 15, 17, 0, tzinfo=IST)


def instrument_batch() -> InstrumentBatch:
    return InstrumentBatch(
        exchange="NSE",
        observed_at=MASTER_OBSERVED,
        provider_version="kiteconnect/5.2.0",
        instruments=(
            KiteInstrument(
                instrument_token=408065,
                exchange_token="1594",
                tradingsymbol="INFY",
                name="INFOSYS",
                dump_last_price=Decimal("1500.10"),
                expiry=None,
                strike=Decimal("0"),
                tick_size=Decimal("0.05"),
                lot_size=1,
                instrument_type="EQ",
                segment="NSE",
                exchange="NSE",
            ),
        ),
    )


class StubAdapter:
    def __init__(self) -> None:
        self.daily_calls = 0

    def fetch_instruments(self, exchange="NSE") -> InstrumentBatch:
        return instrument_batch()

    def fetch_daily_candle(
        self,
        instrument_token: int,
        session: date,
        *,
        session_finality: NseSessionFinality,
    ) -> DailyCandleBatch:
        self.daily_calls += 1
        return DailyCandleBatch(
            instrument_token=instrument_token,
            session_finality=session_finality,
            observed_at=CANDLE_OBSERVED,
            provider_version="kiteconnect/5.2.0",
            candles=(
                DailyCandle(
                    instrument_token=instrument_token,
                    timestamp=datetime.combine(session, datetime.min.time(), tzinfo=IST),
                    open=Decimal("100"),
                    high=Decimal("105"),
                    low=Decimal("99"),
                    close=Decimal("103"),
                    volume=123456,
                ),
            ),
        )


class MarketDataCollectorTests(unittest.TestCase):
    def test_daily_archive_round_trips_with_exact_instrument_vintage_lineage(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            adapter = StubAdapter()
            store = LocalMarketSnapshotStore(Path(temp_dir))
            collector = MarketDataCollector(adapter, store)
            master = collector.collect_instruments("NSE")

            daily = collector.collect_daily_candle(
                instrument_master_snapshot_id=master.manifest.snapshot_id,
                instrument_token=408065,
                session=SESSION,
            )
            loaded = store.get(daily.manifest.dataset, daily.manifest.snapshot_id)

            self.assertIsInstance(master.normalized_payload, InstrumentBatch)
            self.assertIsInstance(loaded.normalized_payload, DailyCandleArchive)
            archive = loaded.normalized_payload
            self.assertEqual(
                archive.instrument_master_snapshot_id,
                master.manifest.snapshot_id,
            )
            self.assertEqual(archive.listing_key, "NSE:INFY")
            self.assertEqual(archive.batch.candles[0].close, Decimal("103"))
            self.assertFalse(archive.batch.session_finality.actionable)

    def test_unknown_token_fails_before_vendor_history_call(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            adapter = StubAdapter()
            store = LocalMarketSnapshotStore(Path(temp_dir))
            collector = MarketDataCollector(adapter, store)
            master = collector.collect_instruments("NSE")

            with self.assertRaises(InstrumentLineageError):
                collector.collect_daily_candle(
                    instrument_master_snapshot_id=master.manifest.snapshot_id,
                    instrument_token=999999,
                    session=SESSION,
                )

            self.assertEqual(adapter.daily_calls, 0)

    def test_current_token_cannot_backfill_before_master_vintage(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            adapter = StubAdapter()
            store = LocalMarketSnapshotStore(Path(temp_dir))
            collector = MarketDataCollector(adapter, store)
            master = collector.collect_instruments("NSE")

            with self.assertRaisesRegex(ValueError, "pre-vintage"):
                collector.collect_daily_candle(
                    instrument_master_snapshot_id=master.manifest.snapshot_id,
                    instrument_token=408065,
                    session=date(2026, 7, 14),
                )


if __name__ == "__main__":
    unittest.main()
