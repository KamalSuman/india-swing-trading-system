from __future__ import annotations

from datetime import date

from india_swing.identity import content_id

from .kite import KiteMarketDataAdapter
from .models import (
    DailyCandleArchive,
    InstrumentBatch,
    NseSessionFinality,
)
from .snapshot_store import (
    LocalMarketSnapshotStore,
    MarketSnapshotIntegrityError,
    StoredMarketSnapshot,
)


class InstrumentLineageError(ValueError):
    pass


class MarketDataCollector:
    def __init__(
        self,
        adapter: KiteMarketDataAdapter,
        store: LocalMarketSnapshotStore,
    ) -> None:
        self.adapter = adapter
        self.store = store

    def collect_instruments(self, exchange: str = "NSE") -> StoredMarketSnapshot:
        batch = self.adapter.fetch_instruments(exchange)
        return self.store.put(
            dataset=f"kite-instruments-{batch.exchange}",
            selection_key=f"exchange={batch.exchange}",
            provider="ZERODHA_KITE",
            provider_version=batch.provider_version,
            observed_at=batch.observed_at,
            normalized_payload=batch,
        )

    def collect_daily_candle(
        self,
        *,
        instrument_master_snapshot_id: str,
        instrument_token: int,
        session: date,
        exchange: str = "NSE",
    ) -> StoredMarketSnapshot:
        exchange = exchange.strip().upper()
        master = self.store.get(
            f"kite-instruments-{exchange}",
            instrument_master_snapshot_id,
        )
        master_payload = master.normalized_payload
        if not isinstance(master_payload, InstrumentBatch):
            raise MarketSnapshotIntegrityError(
                "instrument-master snapshot does not decode to an InstrumentBatch"
            )
        matches = [
            instrument
            for instrument in master_payload.instruments
            if instrument.instrument_token == instrument_token
        ]
        if len(matches) != 1:
            raise InstrumentLineageError(
                "instrument token is not unique in the selected master vintage"
            )
        instrument = matches[0]
        if instrument.exchange != exchange:
            raise InstrumentLineageError("instrument and selected exchange disagree")

        finality = NseSessionFinality.regular_collection_guard(session)
        batch = self.adapter.fetch_daily_candle(
            instrument_token,
            session,
            session_finality=finality,
        )
        archive = DailyCandleArchive(
            instrument_master_snapshot_id=master.manifest.snapshot_id,
            instrument_master_observed_at=master.manifest.observed_at,
            listing_key=instrument.listing_key,
            batch=batch,
        )
        selection_key = content_id(
            {
                "instrument_master_snapshot_id": archive.instrument_master_snapshot_id,
                "listing_key": archive.listing_key,
                "session": archive.batch.session,
            },
            length=64,
        )
        return self.store.put(
            dataset=f"kite-daily-{exchange}",
            selection_key=selection_key,
            provider="ZERODHA_KITE",
            provider_version=batch.provider_version,
            observed_at=batch.observed_at,
            normalized_payload=archive,
        )
