from __future__ import annotations

from datetime import date

from india_swing.identity import content_id

from .kite import KiteMarketDataAdapter
from .models import (
    DailyCandleArchive,
    HistoricalDailyCandleBatch,
    HistoricalDailyRequest,
    InstrumentBatch,
    NseSessionFinality,
)
from .provider import HistoricalDailyDataConnector
from .snapshot_store import (
    LocalMarketSnapshotStore,
    MarketSnapshotIntegrityError,
    StoredMarketSnapshot,
)


class InstrumentLineageError(ValueError):
    pass


class HistoricalCollectionError(ValueError):
    pass


class HistoricalMarketDataCollector:
    """Persist provider-neutral history without weakening point-in-time lineage."""

    def __init__(
        self,
        connector: HistoricalDailyDataConnector,
        store: LocalMarketSnapshotStore,
    ) -> None:
        self.connector = connector
        self.store = store

    def collect(self, request: HistoricalDailyRequest) -> StoredMarketSnapshot:
        if type(request) is not HistoricalDailyRequest:
            raise TypeError("request must be an exact HistoricalDailyRequest")
        try:
            request.verify_content_identity()
        except (TypeError, ValueError):
            raise HistoricalCollectionError(
                "historical request failed canonical identity verification"
            ) from None
        if self.connector.provider != request.binding.provider:
            raise HistoricalCollectionError(
                "historical connector and instrument binding providers disagree"
            )

        batch = self.connector.fetch_historical_daily(request)
        if type(batch) is not HistoricalDailyCandleBatch:
            raise HistoricalCollectionError(
                "historical connector returned an unsupported payload"
            )
        try:
            batch.verify_content_identity()
        except (TypeError, ValueError):
            raise HistoricalCollectionError(
                "historical batch failed canonical identity verification"
            ) from None
        if (
            batch.request.request_id != request.request_id
            or batch.provider != self.connector.provider
            or batch.provider_version != self.connector.provider_version
        ):
            raise HistoricalCollectionError(
                "historical connector response lineage does not match the request"
            )

        provider_component = batch.provider.casefold().replace("_", "-")
        return self.store.put(
            dataset=f"historical-daily-{provider_component}-nse",
            selection_key=request.request_id,
            provider=batch.provider,
            provider_version=batch.provider_version,
            observed_at=batch.observed_at,
            normalized_payload=batch,
        )


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
