from __future__ import annotations

import json
import tempfile
import types
import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from india_swing.identity_registry.models import IdentityObservation
from india_swing.market_data.collection import MarketDataCollector
from india_swing.market_data.kite_instruments import (
    KITE_INSTRUMENTS_DATASET,
    KITE_INSTRUMENTS_SELECTION_KEY,
    KITE_PROVIDER,
    KiteInstrumentResolverError,
    KiteInstrumentSnapshotResolver,
)
from india_swing.market_data.models import InstrumentBatch, KiteInstrument
from india_swing.market_data.snapshot_store import (
    LocalMarketSnapshotStore,
    MarketSnapshotIntegrityError,
)
from tests.test_market_data import FakeKiteClient, adapter, instrument_row


UTC = timezone.utc


def observation(**overrides: object) -> IdentityObservation:
    values: dict[str, object] = {
        "source_artifact_id": "a" * 64,
        "source_manifest_id": "b" * 64,
        "source_record_id": "c" * 64,
        "normalized_row_sha256": "d" * 64,
        "claimed_report_date": date(2026, 7, 15),
        "knowledge_time": datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
        "financial_instrument_id": 1594,
        "ticker_symbol": "INFY",
        "security_series": "EQ",
        "instrument_name": "INFOSYS LIMITED",
        "raw_source_identifier": "INE009A01021",
        "validated_isin": "INE009A01021",
        "delete_flag": "N",
    }
    values.update(overrides)
    return IdentityObservation(**values)


def instrument_snapshot(root: Path, *, rows: list | None = None):
    client = FakeKiteClient(
        instruments=rows if rows is not None else [instrument_row()]
    )
    store = LocalMarketSnapshotStore(root)
    return MarketDataCollector(adapter(client), store).collect_instruments("NSE")


class KiteInstrumentSnapshotResolverHappyPathTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_exact_valid_routing_and_deterministic_resolver_metadata(self) -> None:
        stored = instrument_snapshot(self.root)
        resolver = KiteInstrumentSnapshotResolver(stored)

        token = resolver.resolve(observation())

        self.assertEqual(token, "408065")
        self.assertEqual(resolver.provider, KITE_PROVIDER)
        self.assertLessEqual(len(resolver.resolver_version), 128)
        self.assertEqual(resolver.knowledge_time, stored.manifest.observed_at)
        second = KiteInstrumentSnapshotResolver(stored)
        self.assertEqual(resolver.resolver_version, second.resolver_version)

    def test_catalog_presence_and_miss_return_bool_without_raising(self) -> None:
        stored = instrument_snapshot(self.root)
        resolver = KiteInstrumentSnapshotResolver(stored)

        self.assertTrue(resolver.catalog_contains(observation()))
        self.assertFalse(
            resolver.catalog_contains(observation(ticker_symbol="MISSING"))
        )

    def test_non_eq_kite_row_never_routes(self) -> None:
        stored = instrument_snapshot(
            self.root,
            rows=[
                instrument_row(instrument_type="FUT"),
                instrument_row(
                    instrument_token=2,
                    exchange_token="2",
                    tradingsymbol="TCS",
                ),
            ],
        )
        resolver = KiteInstrumentSnapshotResolver(stored)

        with self.assertRaises(KiteInstrumentResolverError):
            resolver.resolve(observation())
        self.assertFalse(resolver.catalog_contains(observation()))

    def test_no_latest_or_listing_operation_exists(self) -> None:
        stored = instrument_snapshot(self.root)
        resolver = KiteInstrumentSnapshotResolver(stored)

        for name in ("latest", "list", "latest_snapshot", "all_symbols"):
            self.assertFalse(hasattr(resolver, name))


class KiteInstrumentSnapshotResolverRejectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.stored = instrument_snapshot(self.root)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_wrong_snapshot_type_is_rejected(self) -> None:
        with self.assertRaises(TypeError):
            KiteInstrumentSnapshotResolver("not-a-snapshot")

    def test_manifest_lineage_mismatches_are_rejected(self) -> None:
        cases = {
            "dataset": replace(self.stored.manifest, dataset="wrong-dataset"),
            "selection_key": replace(
                self.stored.manifest, selection_key="exchange=BSE"
            ),
            "provider": replace(self.stored.manifest, provider="ZERODHA"),
            "provider_version": replace(
                self.stored.manifest, provider_version="tampered/v1"
            ),
            "observed_at": replace(
                self.stored.manifest,
                observed_at=self.stored.manifest.observed_at + timedelta(days=1),
            ),
        }
        for name, manifest in cases.items():
            with self.subTest(name=name):
                forged = replace(self.stored, manifest=manifest)
                with self.assertRaises(KiteInstrumentResolverError):
                    KiteInstrumentSnapshotResolver(forged)

    def test_payload_type_mismatch_is_rejected(self) -> None:
        forged = replace(self.stored, normalized_payload="not-an-instrument-batch")
        with self.assertRaises(KiteInstrumentResolverError):
            KiteInstrumentSnapshotResolver(forged)

    def test_payload_manifest_lineage_disagreement_is_rejected(self) -> None:
        batch = self.stored.normalized_payload
        mismatched_version = replace(batch, provider_version="other/v1")
        forged = replace(self.stored, normalized_payload=mismatched_version)
        with self.assertRaises(KiteInstrumentResolverError):
            KiteInstrumentSnapshotResolver(forged)

        mismatched_observed_at = replace(
            batch, observed_at=batch.observed_at + timedelta(days=1)
        )
        forged = replace(self.stored, normalized_payload=mismatched_observed_at)
        with self.assertRaises(KiteInstrumentResolverError):
            KiteInstrumentSnapshotResolver(forged)

    def test_wrong_exchange_payload_is_rejected(self) -> None:
        row = KiteInstrument(
            instrument_token=1,
            exchange_token="1",
            tradingsymbol="RELIANCE",
            name="RELIANCE INDUSTRIES",
            dump_last_price=self.stored.normalized_payload.instruments[
                0
            ].dump_last_price,
            expiry=None,
            strike=None,
            tick_size=self.stored.normalized_payload.instruments[0].tick_size,
            lot_size=1,
            instrument_type="EQ",
            segment="BSE",
            exchange="BSE",
        )
        bse_batch = InstrumentBatch(
            exchange="BSE",
            observed_at=self.stored.manifest.observed_at,
            provider_version=self.stored.manifest.provider_version,
            instruments=(row,),
        )
        market_store = LocalMarketSnapshotStore(self.root)
        stored_bse = market_store.put(
            dataset=KITE_INSTRUMENTS_DATASET,
            selection_key=KITE_INSTRUMENTS_SELECTION_KEY,
            provider=KITE_PROVIDER,
            provider_version=self.stored.manifest.provider_version,
            observed_at=self.stored.manifest.observed_at,
            normalized_payload=bse_batch,
        )
        with self.assertRaises(KiteInstrumentResolverError):
            KiteInstrumentSnapshotResolver(stored_bse)

    def test_wrong_row_type_is_rejected(self) -> None:
        batch = self.stored.normalized_payload
        fake_row = types.SimpleNamespace(
            listing_key="NSE:FAKE",
            instrument_token=1,
            exchange_token="1",
            exchange="NSE",
        )
        object.__setattr__(batch, "instruments", (fake_row,))

        with self.assertRaises(KiteInstrumentResolverError):
            KiteInstrumentSnapshotResolver(self.stored)

    def test_ambiguous_symbol_match_is_rejected(self) -> None:
        batch = self.stored.normalized_payload
        duplicate = replace(
            batch.instruments[0],
            instrument_token=999999,
            exchange_token="999999",
        )
        object.__setattr__(batch, "instruments", batch.instruments + (duplicate,))

        with self.assertRaises(KiteInstrumentResolverError):
            KiteInstrumentSnapshotResolver(self.stored).resolve(observation())

    def test_non_eq_observation_series_is_rejected(self) -> None:
        resolver = KiteInstrumentSnapshotResolver(self.stored)
        with self.assertRaises(KiteInstrumentResolverError):
            resolver.resolve(observation(security_series="SM"))
        with self.assertRaises(KiteInstrumentResolverError):
            resolver.catalog_contains(observation(security_series="SM"))

    def test_missing_symbol_is_rejected(self) -> None:
        resolver = KiteInstrumentSnapshotResolver(self.stored)
        with self.assertRaises(KiteInstrumentResolverError):
            resolver.resolve(observation(ticker_symbol="MISSINGCO"))

    def test_wrong_observation_type_is_rejected(self) -> None:
        resolver = KiteInstrumentSnapshotResolver(self.stored)
        with self.assertRaises(TypeError):
            resolver.resolve("not-an-observation")
        with self.assertRaises(TypeError):
            resolver.catalog_contains("not-an-observation")

    def test_tampered_observation_content_identity_is_rejected(self) -> None:
        resolver = KiteInstrumentSnapshotResolver(self.stored)
        value = observation()
        object.__setattr__(value, "ticker_symbol", "TAMPERED")
        with self.assertRaises(KiteInstrumentResolverError):
            resolver.resolve(value)

    def test_resolve_isin_and_catalog_contains_isin_fail_closed(self) -> None:
        resolver = KiteInstrumentSnapshotResolver(self.stored)
        with self.assertRaises(KiteInstrumentResolverError):
            resolver.resolve_isin("INE009A01021")
        with self.assertRaises(KiteInstrumentResolverError):
            resolver.catalog_contains_isin("INE009A01021")

    def test_forged_snapshot_on_disk_fails_before_resolver_construction(self) -> None:
        store = LocalMarketSnapshotStore(self.root)
        base = store.root / KITE_INSTRUMENTS_DATASET
        manifest_paths = list(base.glob("*/*/manifest.json"))
        self.assertEqual(len(manifest_paths), 1)
        manifest_path = manifest_paths[0]
        manifest_value = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest_value["selection_key"] = "exchange=BSE"
        manifest_path.write_text(json.dumps(manifest_value), encoding="utf-8")

        with self.assertRaises(MarketSnapshotIntegrityError):
            store.get(KITE_INSTRUMENTS_DATASET, self.stored.manifest.snapshot_id)


if __name__ == "__main__":
    unittest.main()
