from __future__ import annotations

import tempfile
import unittest
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path

from india_swing.daily_reports.parser import (
    FULL_BHAVCOPY_DELIVERY_HEADER,
    REG1_SURVEILLANCE_HEADER,
    UDIFF_BHAVCOPY_HEADER,
)
from india_swing.market_data.nse_archive import (
    NSE_HISTORICAL_ARCHIVE_EQ_DATASET,
    NseHistoricalArchiveIntegrityError,
    import_nse_historical_range,
    parse_nse_historical_archive_bytes,
)
from india_swing.market_data.snapshot_store import LocalMarketSnapshotStore
from india_swing.reference_data.models import (
    NSE_CM_SECURITY_SOURCE_SCHEMA_VERSION_V2,
)
from tests.test_reconciliation import (
    _csv,
    _full_row,
    _master_bytes,
    _reg1_row,
    _udiff_row,
    _zip,
)
from tests.test_reference_data_import import (
    security_master_bytes,
    security_master_v2_bytes,
    security_row,
)


UTC = timezone.utc
SESSION = date(2026, 7, 15)
OBSERVED_AT = datetime(2026, 8, 2, 0, 0, tzinfo=UTC)


def _replace(row: list[str], header: tuple[str, ...], **values: str) -> list[str]:
    result = list(row)
    stripped = tuple(value.strip() for value in header)
    for name, value in values.items():
        result[stripped.index(name)] = value if stripped.index(name) == 0 else f" {value}"
    return result


def archive_bytes(
    session: date = SESSION,
    *,
    security_master: bytes | None = None,
    contradict_eq_close: bool = False,
    include_bad_t0: bool = False,
) -> bytes:
    udiff_rows = [_udiff_row(trade_date=session)]
    full_rows = [_full_row(trade_date=session)]
    if contradict_eq_close:
        full_rows[0] = _replace(
            full_rows[0], FULL_BHAVCOPY_DELIVERY_HEADER, CLOSE_PRICE="1601.00"
        )
    if include_bad_t0:
        udiff_rows.append(
            _udiff_row(
                trade_date=session,
                FinInstrmId="9999",
                ISIN="INE669E01016",
                TckrSymb="IDEA",
                SctySrs="T0",
                FinInstrmNm="VODAFONE IDEA LIMITED",
                OpnPric="11.27",
                HghPric="11.27",
                LwPric="11.27",
                LastPric="11.27",
                ClsPric="10.98",
                PrvsClsgPric="10.98",
                TtlTradgVol="1",
                TtlTrfVal="11.27",
                TtlNbOfTxsExctd="1",
            )
        )
        full_rows.append(
            _replace(
                _full_row(symbol="IDEA", trade_date=session),
                FULL_BHAVCOPY_DELIVERY_HEADER,
                SERIES="T0",
                PREV_CLOSE="10.98",
                OPEN_PRICE="11.27",
                HIGH_PRICE="11.27",
                LOW_PRICE="11.27",
                LAST_PRICE="11.27",
                CLOSE_PRICE="10.98",
                AVG_PRICE="11.27",
                TTL_TRD_QNTY="1",
                TURNOVER_LACS="0.00",
                NO_OF_TRADES="1",
                DELIV_QTY="-",
                DELIV_PER="-",
            )
        )
    udiff_inner = f"BhavCopy_NSE_CM_0_0_0_{session:%Y%m%d}_F_0000.csv"
    entries = [
        (
            f"sec_bhavdata_full_{session:%d%m%Y}.csv",
            _csv(FULL_BHAVCOPY_DELIVERY_HEADER, full_rows),
        ),
        (
            f"{udiff_inner}.zip",
            _zip([(udiff_inner, _csv(UDIFF_BHAVCOPY_HEADER, udiff_rows))]),
        ),
        (
            f"REG1_IND{session:%d%m%y}.csv",
            _csv(REG1_SURVEILLANCE_HEADER, [_reg1_row("INFY", "EQ")]),
        ),
        (
            f"NSE_CM_security_{session:%d%m%Y}.csv.gz",
            _master_bytes() if security_master is None else security_master,
        ),
    ]
    return _zip(entries)


class NseHistoricalArchiveParserTests(unittest.TestCase):
    def test_parses_exact_eq_session_with_identity_delivery_and_surveillance(self) -> None:
        parsed = parse_nse_historical_archive_bytes(
            archive_bytes(),
            original_filename="Reports-Archives-Multiple-15072026.zip",
        )

        payload = parsed.normalized_payload
        self.assertEqual(parsed.session, SESSION)
        self.assertEqual(len(parsed.source_entry_sha256), 4)
        self.assertEqual(payload["series_scope"], ("EQ",))
        self.assertTrue(payload["collection_only"])
        self.assertFalse(payload["actionable"])
        self.assertFalse(payload["training_eligible"])
        self.assertEqual(len(payload["records"]), 1)
        record = payload["records"][0]
        self.assertEqual(record["listing_key"], "NSE:INFY")
        self.assertEqual(record["validated_isin"], "INE009A01021")
        self.assertEqual(record["delivery_quantity"], 50)
        self.assertIn("GSM", record["surveillance_indicators"])

    def test_non_eq_anomaly_is_excluded_without_weakening_eq_validation(self) -> None:
        parsed = parse_nse_historical_archive_bytes(
            archive_bytes(include_bad_t0=True),
            original_filename="Reports-Archives-Multiple-15072026.zip",
        )

        self.assertEqual(len(parsed.normalized_payload["records"]), 1)
        self.assertEqual(
            parsed.normalized_payload["records"][0]["symbol"], "INFY"
        )

    def test_eq_contradiction_and_nonexact_entry_set_fail_closed(self) -> None:
        with self.assertRaises(NseHistoricalArchiveIntegrityError):
            parse_nse_historical_archive_bytes(
                archive_bytes(contradict_eq_close=True),
                original_filename="Reports-Archives-Multiple-15072026.zip",
            )

        malformed = _zip([("unexpected.txt", b"unsafe")])
        with self.assertRaises(NseHistoricalArchiveIntegrityError):
            parse_nse_historical_archive_bytes(
                malformed,
                original_filename="Reports-Archives-Multiple-15072026.zip",
            )

    def test_v2_security_master_is_preserved_as_distinct_source_schema(self) -> None:
        session = date(2026, 7, 31)
        parsed = parse_nse_historical_archive_bytes(
            archive_bytes(session, security_master=security_master_v2_bytes()),
            original_filename="Reports-Archives-Multiple-31072026.zip",
        )

        self.assertEqual(
            parsed.normalized_payload["security_master_source_schema_version"],
            NSE_CM_SECURITY_SOURCE_SCHEMA_VERSION_V2,
        )

    def test_missing_same_session_security_record_is_retained_but_unresolved(self) -> None:
        master = security_master_bytes(
            [
                security_row(
                    FinInstrmId="2000",
                    TckrSymb="TCS",
                    FinInstrmNm="TATA CONSULTANCY SERVICE",
                    ISIN="INE467B01029",
                )
            ]
        )
        parsed = parse_nse_historical_archive_bytes(
            archive_bytes(security_master=master),
            original_filename="Reports-Archives-Multiple-15072026.zip",
        )

        self.assertEqual(parsed.normalized_payload["identity_issue_count"], 1)
        record = parsed.normalized_payload["records"][0]
        self.assertEqual(record["identity_status"], "SECURITY_MASTER_MISSING")
        self.assertIsNone(record["validated_isin"])
        self.assertIsNone(record["normal_market_eligible"])

    def test_same_session_identifier_mismatch_is_retained_and_quarantined(self) -> None:
        master = security_master_bytes(
            [security_row(ISIN="INE467B01029")]
        )
        parsed = parse_nse_historical_archive_bytes(
            archive_bytes(security_master=master),
            original_filename="Reports-Archives-Multiple-15072026.zip",
        )

        payload = parsed.normalized_payload
        self.assertEqual(payload["identity_issue_count"], 1)
        self.assertFalse(payload["actionable"])
        self.assertFalse(payload["training_eligible"])
        record = payload["records"][0]
        self.assertEqual(record["identity_status"], "SOURCE_IDENTIFIER_MISMATCH")
        self.assertEqual(record["udiff_source_identifier"], "INE009A01021")
        self.assertEqual(
            record["security_master_source_identifier"], "INE467B01029"
        )
        self.assertIsNone(record["validated_isin"])


class NseHistoricalArchiveStoreTests(unittest.TestCase):
    def test_range_import_is_content_addressed_idempotent_and_indexed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            staging = root / "staging" / SESSION.isoformat()
            archives = root / "source-archives" / SESSION.isoformat()
            staging.mkdir(parents=True)
            archives.mkdir(parents=True)
            payload = archive_bytes(
                security_master=security_master_bytes(
                    [security_row(ISIN="INE467B01029")]
                )
            )
            archive_path = archives / "Reports-Archives-Multiple-15072026.zip"
            archive_path.write_bytes(payload)
            with zipfile.ZipFile(archive_path) as archive:
                for name in archive.namelist():
                    (staging / name).write_bytes(archive.read(name))
            store = LocalMarketSnapshotStore(root / "canonical")

            first, first_index = import_nse_historical_range(
                staging_root=root / "staging",
                archive_root=root / "source-archives",
                store=store,
                start=SESSION,
                end=SESSION,
                observed_at=OBSERVED_AT,
            )
            second, second_index = import_nse_historical_range(
                staging_root=root / "staging",
                archive_root=root / "source-archives",
                store=store,
                start=SESSION,
                end=SESSION,
                observed_at=OBSERVED_AT,
            )

            self.assertEqual(first, second)
            self.assertEqual(
                first_index.manifest.snapshot_id,
                second_index.manifest.snapshot_id,
            )
            stored = store.get(
                NSE_HISTORICAL_ARCHIVE_EQ_DATASET,
                first[0].snapshot_id,
            )
            self.assertEqual(stored.manifest.record_count, 1)
            self.assertEqual(stored.normalized_payload["session"], SESSION)
            self.assertEqual(first[0].identity_issue_count, 1)
            self.assertEqual(
                first_index.normalized_payload["identity_issue_count"], 1
            )
            self.assertEqual(
                first_index.normalized_payload[
                    "identity_quarantined_session_count"
                ],
                1,
            )
            self.assertEqual(
                first_index.normalized_payload["records"][0][
                    "identity_issue_count"
                ],
                1,
            )


if __name__ == "__main__":
    unittest.main()
