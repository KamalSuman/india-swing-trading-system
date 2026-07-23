from __future__ import annotations

import gzip
import json
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from india_swing.market_data.backfill import (
    HistoricalBackfillError,
    HistoricalBackfillIssueCode,
    UpstoxCatalogInstrumentResolver,
    build_historical_backfill_plan,
)
from india_swing.market_data.upstox import UpstoxHttpResponse
from india_swing.market_data.upstox_instruments import (
    NORMALIZED_FILENAME,
    UPSTOX_NSE_INSTRUMENTS_URL,
    LocalUpstoxInstrumentCatalogStore,
    UpstoxInstrumentCatalogError,
    UpstoxInstrumentCatalogIntegrityError,
    fetch_upstox_nse_instrument_catalog,
    parse_upstox_nse_instrument_catalog,
)
from tests.test_historical_backfill import (
    DAY_ONE,
    DAY_TWO,
    REQUESTED_AT,
    calendar,
    registry,
    security_master_sources,
)
from tests.test_identity_registry import security_row


UTC = timezone.utc
OBSERVED_AT = datetime(2026, 7, 23, 7, 0, tzinfo=UTC)


def equity_row(
    *,
    isin: str = "INE009A01021",
    instrument_type: str = "EQ",
    trading_symbol: str = "INFY",
    exchange_token: object = "1594",
) -> dict[str, object]:
    return {
        "segment": "NSE_EQ",
        "exchange": "NSE",
        "isin": isin,
        "instrument_type": instrument_type,
        "instrument_key": f"NSE_EQ|{isin}",
        "lot_size": 1,
        "exchange_token": exchange_token,
        "tick_size": 5.0,
        "trading_symbol": trading_symbol,
        "name": f"{trading_symbol} LIMITED",
        "security_type": "NORMAL",
        "future_vendor_field": {"ignored": True},
    }


def raw_catalog(*rows: dict[str, object]) -> bytes:
    return gzip.compress(
        json.dumps(list(rows), separators=(",", ":")).encode("utf-8"),
        mtime=0,
    )


def catalog():
    return parse_upstox_nse_instrument_catalog(
        raw_catalog(
            equity_row(),
            equity_row(
                isin="INE123A01016",
                instrument_type="SM",
                trading_symbol="SMALLCO",
                exchange_token=9001,
            ),
            {
                "segment": "NSE_FO",
                "exchange": "NSE",
                "instrument_key": "NSE_FO|1",
            },
            {
                **equity_row(),
                "isin": "IN9139R01028",
                "instrument_key": "NSE_EQ|IN9139R01028",
            },
        ),
        observed_at=OBSERVED_AT,
    )


class FakeTransport:
    def __init__(self, response: UpstoxHttpResponse) -> None:
        self.response = response
        self.calls: list[dict[str, object]] = []

    def get(
        self,
        url: str,
        *,
        headers,
        timeout_seconds: float,
        maximum_bytes: int,
    ) -> UpstoxHttpResponse:
        self.calls.append(
            {
                "url": url,
                "headers": dict(headers),
                "timeout_seconds": timeout_seconds,
                "maximum_bytes": maximum_bytes,
            }
        )
        return self.response


class UpstoxInstrumentCatalogTests(unittest.TestCase):
    def test_parser_retains_only_exact_nse_equity_rows(self) -> None:
        value = catalog()

        self.assertEqual(value.source_row_count, 4)
        self.assertEqual(len(value.instruments), 2)
        self.assertEqual(
            {
                (item.instrument_type, item.trading_symbol)
                for item in value.instruments
            },
            {("EQ", "INFY"), ("SM", "SMALLCO")},
        )
        self.assertTrue(
            all(type(item.tick_size) is Decimal for item in value.instruments)
        )
        self.assertFalse(value.actionable)
        value.verify_content_identity()

    def test_duplicate_json_keys_and_ambiguous_lanes_fail_closed(self) -> None:
        duplicate_key = gzip.compress(
            (
                '[{"segment":"NSE_EQ","segment":"NSE_EQ",'
                '"exchange":"NSE"}]'
            ).encode("utf-8"),
            mtime=0,
        )
        with self.assertRaises(UpstoxInstrumentCatalogError):
            parse_upstox_nse_instrument_catalog(
                duplicate_key,
                observed_at=OBSERVED_AT,
            )

        duplicate_lane = raw_catalog(equity_row(), equity_row(exchange_token="2"))
        with self.assertRaisesRegex(
            ValueError,
            "ambiguous normalized listing lane",
        ):
            parse_upstox_nse_instrument_catalog(
                duplicate_lane,
                observed_at=OBSERVED_AT,
            )

        malformed_token = raw_catalog(
            equity_row(exchange_token={"not": "a token"})
        )
        with self.assertRaises(UpstoxInstrumentCatalogError):
            parse_upstox_nse_instrument_catalog(
                malformed_token,
                observed_at=OBSERVED_AT,
            )

    def test_local_store_replays_raw_source_and_detects_tampering(self) -> None:
        raw = raw_catalog(equity_row())
        value = parse_upstox_nse_instrument_catalog(
            raw,
            observed_at=OBSERVED_AT,
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalUpstoxInstrumentCatalogStore(Path(temp_dir))
            stored = store.put(raw, value)
            self.assertEqual(store.get(stored.catalog_id), value)
            normalized = (
                store.dataset_root
                / stored.catalog_id
                / NORMALIZED_FILENAME
            )
            normalized.write_bytes(normalized.read_bytes() + b" ")
            with self.assertRaises(UpstoxInstrumentCatalogIntegrityError):
                store.get(stored.catalog_id)

    def test_public_fetch_uses_exact_url_and_seals_response(self) -> None:
        raw = raw_catalog(equity_row())
        transport = FakeTransport(UpstoxHttpResponse(200, raw))
        with tempfile.TemporaryDirectory() as temp_dir:
            value = fetch_upstox_nse_instrument_catalog(
                store=LocalUpstoxInstrumentCatalogStore(Path(temp_dir)),
                transport=transport,
                clock=lambda: OBSERVED_AT,
            )

        self.assertEqual(value.observed_at, OBSERVED_AT)
        self.assertEqual(len(transport.calls), 1)
        self.assertEqual(
            transport.calls[0]["url"],
            UPSTOX_NSE_INSTRUMENTS_URL,
        )
        self.assertNotIn("Authorization", transport.calls[0]["headers"])

    def test_catalog_resolver_uses_isin_without_leaking_current_series(self) -> None:
        resolver = UpstoxCatalogInstrumentResolver(catalog())
        with tempfile.TemporaryDirectory() as temp_dir:
            identity = registry(
                Path(temp_dir),
                [security_row()],
                [security_row()],
            )
        observation = next(
            value
            for value in identity.observations
            if value.claimed_report_date == DAY_ONE
        )
        self.assertEqual(
            resolver.resolve(observation),
            "NSE_EQ|INE009A01021",
        )
        changed_series = replace(observation, security_series="SM")
        self.assertEqual(
            resolver.resolve(changed_series),
            "NSE_EQ|INE009A01021",
        )
        self.assertTrue(resolver.catalog_contains(changed_series))

        absent = replace(
            observation,
            validated_isin="INE467B01029",
            raw_source_identifier="INE467B01029",
            ticker_symbol="TCS",
        )
        self.assertEqual(
            resolver.resolve(absent),
            "NSE_EQ|INE467B01029",
        )
        self.assertFalse(resolver.catalog_contains(absent))

    def test_current_catalog_absence_is_visible_but_not_survivorship_filter(self) -> None:
        resolver = UpstoxCatalogInstrumentResolver(catalog())
        tcs = security_row(
            TckrSymb="TCS",
            FinInstrmId="11536",
            ISIN="INE467B01029",
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            identity = registry(root, [tcs], [tcs])
            value = build_historical_backfill_plan(
                registry=identity,
                security_master_sources=security_master_sources(
                    root, identity
                ),
                calendar=calendar(),
                resolver=resolver,
                coverage_start=DAY_ONE,
                coverage_end=DAY_TWO,
                requested_at=OBSERVED_AT,
            )

        self.assertEqual(value.safe_request_count, 1)
        self.assertFalse(value.has_blocking_issues)
        self.assertEqual(value.blocking_issue_count, 0)
        self.assertEqual(value.exclusion_issue_count, 0)
        self.assertEqual(value.warning_issue_count, 2)
        self.assertEqual(
            {item.code for item in value.issues},
            {HistoricalBackfillIssueCode.PROVIDER_CATALOG_ABSENT},
        )

    def test_broken_catalog_capability_check_blocks_instead_of_warning(self) -> None:
        class BrokenResolver:
            provider = "UPSTOX"
            resolver_version = "broken-catalog-check/v1"

            @staticmethod
            def resolve(observation):
                return f"NSE_EQ|{observation.validated_isin}"

            @staticmethod
            def catalog_contains(observation):
                raise RuntimeError("untrusted resolver failure")

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            identity = registry(
                root,
                [security_row()],
                [security_row()],
            )
            value = build_historical_backfill_plan(
                registry=identity,
                security_master_sources=security_master_sources(
                    root, identity
                ),
                calendar=calendar(),
                resolver=BrokenResolver(),
                coverage_start=DAY_ONE,
                coverage_end=DAY_TWO,
                requested_at=OBSERVED_AT,
            )

        self.assertEqual(value.requests, ())
        self.assertTrue(value.has_blocking_issues)
        self.assertEqual(value.blocking_issue_count, 2)
        self.assertEqual(
            {item.code for item in value.issues},
            {HistoricalBackfillIssueCode.PROVIDER_KEY_UNAVAILABLE},
        )

    def test_future_catalog_cannot_enter_an_earlier_plan(self) -> None:
        resolver = UpstoxCatalogInstrumentResolver(
            replace(catalog(), observed_at=REQUESTED_AT + timedelta(seconds=1))
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            identity = registry(
                root,
                [security_row()],
                [security_row()],
            )
            with self.assertRaisesRegex(
                HistoricalBackfillError,
                "not known",
            ):
                build_historical_backfill_plan(
                    registry=identity,
                    security_master_sources=security_master_sources(
                        root, identity
                    ),
                    calendar=calendar(),
                    resolver=resolver,
                    coverage_start=DAY_ONE,
                    coverage_end=DAY_TWO,
                    requested_at=REQUESTED_AT,
                )

    def test_expected_exclusions_do_not_mask_real_blockers(self) -> None:
        deleted = security_row(
            TckrSymb="DELETED",
            FinInstrmId="8000",
            ISIN="INE467B01029",
            DelFlg="Y",
        )
        unsupported = security_row(
            TckrSymb="BLOCKLANE",
            FinInstrmId="8001",
            ISIN="INE002A01018",
            SctySrs="BL",
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            identity = registry(
                root,
                [security_row(), deleted, unsupported],
                [security_row(), deleted, unsupported],
            )
            value = build_historical_backfill_plan(
                registry=identity,
                security_master_sources=security_master_sources(
                    root, identity
                ),
                calendar=calendar(),
                resolver=UpstoxCatalogInstrumentResolver(catalog()),
                coverage_start=DAY_ONE,
                coverage_end=DAY_TWO,
                requested_at=OBSERVED_AT,
            )

        self.assertEqual(value.safe_request_count, 1)
        self.assertFalse(value.has_blocking_issues)
        self.assertEqual(value.blocking_issue_count, 0)
        self.assertEqual(value.exclusion_issue_count, 4)
        self.assertEqual(value.warning_issue_count, 0)
        self.assertEqual(
            {item.code for item in value.issues},
            {
                HistoricalBackfillIssueCode.DELETED_SECURITY,
                HistoricalBackfillIssueCode.UNSUPPORTED_LISTING_LANE,
            },
        )


if __name__ == "__main__":
    unittest.main()
