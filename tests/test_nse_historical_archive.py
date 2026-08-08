from __future__ import annotations

import decimal
import io
import tempfile
import unittest
import zipfile
from dataclasses import replace
from datetime import date, datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Mapping

from india_swing.daily_reports.parser import (
    FULL_BHAVCOPY_DELIVERY_HEADER,
    REG1_SURVEILLANCE_HEADER,
    UDIFF_BHAVCOPY_HEADER,
)
from india_swing.identity import content_id
from india_swing.market_data.nse_archive import (
    EVIDENCE_PROFILE_COMPLETE,
    EVIDENCE_PROFILE_PRICE_UDIFF,
    EVIDENCE_PROFILE_PRICE_UDIFF_SECURITY,
    EVIDENCE_PROFILE_UNRECONCILED,
    IDENTITY_STATUS_UDIFF_AND_SECURITY_MASTER_EVIDENCE_UNAVAILABLE,
    NSE_HISTORICAL_ARCHIVE_EQ_DATASET,
    NSE_HISTORICAL_ARCHIVE_IMPORTER_VERSION,
    NSE_HISTORICAL_ARCHIVE_INDEX_DATASET,
    NSE_HISTORICAL_ARCHIVE_INDEX_SCHEMA_VERSION,
    NSE_HISTORICAL_ARCHIVE_PROVIDER,
    NseHistoricalArchiveIntegrityError,
    _LEGACY_FULL_BHAVCOPY_HEADER,
    _MTO_CONTROL_RECORD_TYPE,
    _MTO_DATA_RECORD_TYPE,
    _MTO_HEADER_RECORD,
    _MTO_TITLE_LINE,
    _REG1_HISTORICAL_HEADER,
    _expected_legacy_names,
    import_nse_historical_range,
    parse_nse_historical_archive_bytes,
)
from india_swing.market_data.snapshot_store import LocalMarketSnapshotStore
from india_swing.market_data.nse_archive_range import (
    NseHistoricalArchiveRangeError,
    _replay_issue_id,
    _replay_record_id,
    load_verified_nse_historical_archive_range,
)
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
    include_reg1: bool = True,
    include_security_master: bool = True,
    report_date: date | None = None,
    udiff_trailing_empty_header: bool = False,
    udiff_zero_padded_reserved_header: bool = False,
    reg1_historical_header: bool = False,
    contradict_eq_close: bool = False,
    include_bad_t0: bool = False,
) -> bytes:
    effective_report_date = session if report_date is None else report_date
    udiff_rows = [_udiff_row(trade_date=effective_report_date)]
    full_rows = [_full_row(trade_date=effective_report_date)]
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
    udiff_payload = _csv(UDIFF_BHAVCOPY_HEADER, udiff_rows)
    if udiff_zero_padded_reserved_header:
        for current, historical in (
            (b"Rsvd1", b"Rsvd01"),
            (b"Rsvd2", b"Rsvd02"),
            (b"Rsvd3", b"Rsvd03"),
            (b"Rsvd4", b"Rsvd04"),
        ):
            udiff_payload = udiff_payload.replace(current, historical, 1)
    if udiff_trailing_empty_header:
        lines = udiff_payload.decode("utf-8").splitlines()
        udiff_payload = ("\n".join((f"{lines[0]},", *lines[1:])) + "\n").encode(
            "utf-8"
        )
    entries = [
        (
            f"sec_bhavdata_full_{session:%d%m%Y}.csv",
            _csv(FULL_BHAVCOPY_DELIVERY_HEADER, full_rows),
        ),
        (
            f"{udiff_inner}.zip",
            _zip([(udiff_inner, udiff_payload)]),
        ),
    ]
    if include_reg1:
        entries.append(
            (
                f"REG1_IND{session:%d%m%y}.csv",
                _csv(
                    (
                        _REG1_HISTORICAL_HEADER
                        if reg1_historical_header
                        else REG1_SURVEILLANCE_HEADER
                    ),
                    [_reg1_row("INFY", "EQ")],
                ),
            )
        )
    if include_security_master:
        entries.append(
            (
                f"NSE_CM_security_{session:%d%m%Y}.csv.gz",
                _master_bytes() if security_master is None else security_master,
            )
        )
    return _zip(entries)


def _one_file_archive_bytes(
    session: date = SESSION,
    *,
    report_date: date | None = None,
    full_rows: list[list[str]] | None = None,
) -> bytes:
    effective_report_date = session if report_date is None else report_date
    rows = (
        [_full_row(trade_date=effective_report_date)]
        if full_rows is None
        else full_rows
    )
    return _zip(
        [
            (
                f"sec_bhavdata_full_{session:%d%m%Y}.csv",
                _csv(FULL_BHAVCOPY_DELIVERY_HEADER, rows),
            )
        ]
    )


def _one_file_normalized_payload(session: date = SESSION) -> dict[str, object]:
    parsed = parse_nse_historical_archive_bytes(
        _one_file_archive_bytes(session),
        original_filename=f"Reports-Archives-Multiple-{session:%d%m%Y}.zip",
    )
    payload = dict(parsed.normalized_payload)
    payload["records"] = tuple(dict(record) for record in payload["records"])
    payload["identity_issues"] = tuple(
        dict(issue) for issue in payload["identity_issues"]
    )
    return payload


def _recompute_record_id(record: dict[str, object]) -> None:
    without_id = {key: value for key, value in record.items() if key != "record_id"}
    record["record_id"] = content_id(
        {"schema": "nse-historical-archive-eq-record/v1", **without_id},
        length=64,
    )


def _recompute_issue_id(issue: dict[str, object]) -> None:
    without_id = {key: value for key, value in issue.items() if key != "issue_id"}
    issue["issue_id"] = content_id(
        {"schema": "nse-historical-archive-identity-issue/v1", **without_id},
        length=64,
    )


def _store_one_file_session(
    store: LocalMarketSnapshotStore,
    normalized_payload: Mapping[str, object],
    *,
    session: date = SESSION,
    observed_at: datetime = OBSERVED_AT,
):
    return store.put(
        dataset=NSE_HISTORICAL_ARCHIVE_EQ_DATASET,
        selection_key=session.isoformat(),
        provider=NSE_HISTORICAL_ARCHIVE_PROVIDER,
        provider_version=NSE_HISTORICAL_ARCHIVE_IMPORTER_VERSION,
        observed_at=observed_at,
        normalized_payload=normalized_payload,
    )


def _store_one_file_index(
    store: LocalMarketSnapshotStore,
    session_snapshot,
    normalized_payload: Mapping[str, object],
    *,
    session: date = SESSION,
    observed_at: datetime = OBSERVED_AT,
):
    identity_issues = normalized_payload["identity_issues"]
    index_payload = {
        "schema_version": NSE_HISTORICAL_ARCHIVE_INDEX_SCHEMA_VERSION,
        "range_start": session,
        "range_end": session,
        "collection_only": True,
        "actionable": False,
        "training_eligible": False,
        "identity_issue_count": len(identity_issues),
        "identity_quarantined_session_count": 1 if identity_issues else 0,
        "incomplete_evidence_session_count": 1,
        "evidence_profile_counts": {
            EVIDENCE_PROFILE_PRICE_UDIFF: 0,
            EVIDENCE_PROFILE_PRICE_UDIFF_SECURITY: 0,
            EVIDENCE_PROFILE_COMPLETE: 0,
            EVIDENCE_PROFILE_UNRECONCILED: 1,
        },
        "records": (
            {
                "session": session,
                "snapshot_id": session_snapshot.manifest.snapshot_id,
                "record_count": session_snapshot.manifest.record_count,
                "source_container_sha256": normalized_payload[
                    "source_container_sha256"
                ],
                "identity_issue_count": len(identity_issues),
                "evidence_profile": EVIDENCE_PROFILE_UNRECONCILED,
            },
        ),
    }
    return store.put(
        dataset=NSE_HISTORICAL_ARCHIVE_INDEX_DATASET,
        selection_key=f"{session.isoformat()}:{session.isoformat()}",
        provider=NSE_HISTORICAL_ARCHIVE_PROVIDER,
        provider_version=NSE_HISTORICAL_ARCHIVE_IMPORTER_VERSION,
        observed_at=observed_at,
        normalized_payload=index_payload,
    )


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
        self.assertEqual(payload["evidence_profile"], EVIDENCE_PROFILE_COMPLETE)
        self.assertEqual(payload["missing_evidence"], ())
        self.assertEqual(len(payload["records"]), 1)
        record = payload["records"][0]
        self.assertEqual(record["listing_key"], "NSE:INFY")
        self.assertEqual(record["validated_isin"], "INE009A01021")
        self.assertEqual(record["delivery_quantity"], 50)
        self.assertIn("GSM", record["surveillance_indicators"])

    def test_two_file_archive_is_explicitly_price_only_and_identity_unresolved(self) -> None:
        parsed = parse_nse_historical_archive_bytes(
            archive_bytes(include_reg1=False, include_security_master=False),
            original_filename="Reports-Archives-Multiple-15072026.zip",
        )

        payload = parsed.normalized_payload
        self.assertEqual(payload["evidence_profile"], EVIDENCE_PROFILE_PRICE_UDIFF)
        self.assertEqual(
            payload["missing_evidence"],
            ("REG1_SURVEILLANCE", "NSE_CM_SECURITY_MASTER"),
        )
        self.assertEqual(len(parsed.source_entry_sha256), 2)
        self.assertIsNone(payload["security_master_source_schema_version"])
        self.assertIsNone(payload["security_master_header_sha256"])
        self.assertIsNone(payload["reg1_row_count"])
        self.assertEqual(payload["identity_issue_count"], 1)
        record = payload["records"][0]
        self.assertEqual(
            record["identity_status"],
            "SECURITY_MASTER_EVIDENCE_UNAVAILABLE",
        )
        self.assertIsNone(record["validated_isin"])
        self.assertEqual(record["surveillance_indicators"], {})

    def test_three_file_archive_preserves_same_session_identity_but_marks_reg1_missing(self) -> None:
        parsed = parse_nse_historical_archive_bytes(
            archive_bytes(include_reg1=False),
            original_filename="Reports-Archives-Multiple-15072026.zip",
        )

        payload = parsed.normalized_payload
        self.assertEqual(
            payload["evidence_profile"],
            EVIDENCE_PROFILE_PRICE_UDIFF_SECURITY,
        )
        self.assertEqual(payload["missing_evidence"], ("REG1_SURVEILLANCE",))
        self.assertEqual(len(parsed.source_entry_sha256), 3)
        self.assertIsNone(payload["reg1_row_count"])
        record = payload["records"][0]
        self.assertEqual(record["identity_status"], "MATCHED_SAME_SESSION")
        self.assertEqual(record["validated_isin"], "INE009A01021")
        self.assertEqual(record["surveillance_indicators"], {})

    def test_historical_udiff_single_trailing_empty_header_is_normalized_exactly(self) -> None:
        parsed = parse_nse_historical_archive_bytes(
            archive_bytes(
                include_reg1=False,
                include_security_master=False,
                udiff_trailing_empty_header=True,
                udiff_zero_padded_reserved_header=True,
            ),
            original_filename="Reports-Archives-Multiple-15072026.zip",
        )

        self.assertEqual(
            parsed.normalized_payload["evidence_profile"],
            EVIDENCE_PROFILE_PRICE_UDIFF,
        )
        self.assertEqual(len(parsed.normalized_payload["records"]), 1)

    def test_exact_historical_reg1_header_is_canonicalized(self) -> None:
        parsed = parse_nse_historical_archive_bytes(
            archive_bytes(reg1_historical_header=True),
            original_filename="Reports-Archives-Multiple-15072026.zip",
        )

        self.assertEqual(
            parsed.normalized_payload["evidence_profile"],
            EVIDENCE_PROFILE_COMPLETE,
        )
        self.assertIn(
            "GSM",
            parsed.normalized_payload["records"][0]["surveillance_indicators"],
        )

    def test_unsupported_partial_profile_and_stale_report_date_fail_closed(self) -> None:
        with self.assertRaises(NseHistoricalArchiveIntegrityError):
            parse_nse_historical_archive_bytes(
                archive_bytes(include_security_master=False),
                original_filename="Reports-Archives-Multiple-15072026.zip",
            )

        with self.assertRaises(NseHistoricalArchiveIntegrityError):
            parse_nse_historical_archive_bytes(
                archive_bytes(
                    include_reg1=False,
                    include_security_master=False,
                    report_date=date(2026, 7, 14),
                ),
                original_filename="Reports-Archives-Multiple-15072026.zip",
            )

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

    def test_one_file_archive_is_unreconciled_with_fully_unavailable_identity(self) -> None:
        parsed = parse_nse_historical_archive_bytes(
            _one_file_archive_bytes(),
            original_filename="Reports-Archives-Multiple-15072026.zip",
        )

        payload = parsed.normalized_payload
        self.assertEqual(payload["evidence_profile"], EVIDENCE_PROFILE_UNRECONCILED)
        self.assertEqual(
            payload["missing_evidence"],
            ("UDIFF_BHAVCOPY", "NSE_CM_SECURITY_MASTER", "REG1_SURVEILLANCE"),
        )
        self.assertEqual(len(parsed.source_entry_sha256), 1)
        self.assertIsNone(payload["security_master_source_schema_version"])
        self.assertIsNone(payload["security_master_header_sha256"])
        self.assertIsNone(payload["reg1_row_count"])
        self.assertEqual(payload["identity_issue_count"], 1)
        self.assertEqual(len(payload["identity_issues"]), 1)
        self.assertTrue(payload["collection_only"])
        self.assertFalse(payload["actionable"])
        self.assertFalse(payload["training_eligible"])

        record = payload["records"][0]
        self.assertEqual(record["listing_key"], "NSE:INFY")
        self.assertEqual(
            record["identity_status"],
            IDENTITY_STATUS_UDIFF_AND_SECURITY_MASTER_EVIDENCE_UNAVAILABLE,
        )
        self.assertIsNone(record["financial_instrument_id"])
        self.assertIsNone(record["security_master_financial_instrument_id"])
        self.assertIsNone(record["security_source_record_id"])
        self.assertIsNone(record["security_master_source_identifier"])
        self.assertIsNone(record["udiff_source_identifier"])
        self.assertIsNone(record["validated_isin"])
        self.assertIsNone(record["normal_market_status"])
        self.assertIsNone(record["normal_market_eligible"])
        self.assertIsNone(record["permitted_to_trade"])
        self.assertIsNone(record["delete_flag"])
        self.assertEqual(record["surveillance_indicators"], {})
        self.assertEqual(record["delivery_quantity"], 50)

        issue = payload["identity_issues"][0]
        self.assertEqual(
            issue["status"],
            IDENTITY_STATUS_UDIFF_AND_SECURITY_MASTER_EVIDENCE_UNAVAILABLE,
        )
        self.assertIsNone(issue["udiff_financial_instrument_id"])
        self.assertIsNone(issue["security_master_financial_instrument_id"])
        self.assertIsNone(issue["security_master_source_identifier"])
        self.assertIsNone(issue["udiff_source_identifier"])

    def test_one_file_archive_with_prior_session_report_date_fails_closed(self) -> None:
        with self.assertRaises(NseHistoricalArchiveIntegrityError):
            parse_nse_historical_archive_bytes(
                _one_file_archive_bytes(report_date=date(2026, 7, 14)),
                original_filename="Reports-Archives-Multiple-15072026.zip",
            )

    def test_one_file_archive_alternate_extra_duplicate_malformed_and_no_eq_fail_closed(
        self,
    ) -> None:
        accepted_name = f"sec_bhavdata_full_{SESSION:%d%m%Y}.csv"
        good_row = _full_row(trade_date=SESSION)
        alternate = _zip(
            [
                (
                    f"sec_bhavdata_full_{SESSION:%Y%m%d}.csv",
                    _csv(FULL_BHAVCOPY_DELIVERY_HEADER, [good_row]),
                )
            ]
        )
        extra = _zip(
            [
                (accepted_name, _csv(FULL_BHAVCOPY_DELIVERY_HEADER, [good_row])),
                ("unexpected.txt", b"unsafe"),
            ]
        )
        duplicate = _zip(
            [
                (accepted_name, _csv(FULL_BHAVCOPY_DELIVERY_HEADER, [good_row])),
                (accepted_name, _csv(FULL_BHAVCOPY_DELIVERY_HEADER, [good_row])),
            ]
        )
        malformed = _zip([(accepted_name, b"NOT,A,VALID,HEADER\n1,2,3,4\n")])
        non_eq_row = _replace(good_row, FULL_BHAVCOPY_DELIVERY_HEADER, SERIES="BE")
        no_eq = _zip(
            [(accepted_name, _csv(FULL_BHAVCOPY_DELIVERY_HEADER, [non_eq_row]))]
        )

        cases = {
            "alternate_filename": alternate,
            "extra_entry": extra,
            "duplicate_entry": duplicate,
            "malformed_csv": malformed,
            "no_eq_rows": no_eq,
        }
        for label, payload in cases.items():
            with self.subTest(label), self.assertRaises(
                NseHistoricalArchiveIntegrityError
            ):
                parse_nse_historical_archive_bytes(
                    payload,
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
    def test_range_import_preserves_incomplete_evidence_profile_accounting(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            staging = root / "staging" / SESSION.isoformat()
            archives = root / "source-archives" / SESSION.isoformat()
            staging.mkdir(parents=True)
            archives.mkdir(parents=True)
            archive_path = archives / "Reports-Archives-Multiple-15072026.zip"
            archive_path.write_bytes(
                archive_bytes(include_reg1=False, include_security_master=False)
            )
            with zipfile.ZipFile(archive_path) as archive:
                for name in archive.namelist():
                    (staging / name).write_bytes(archive.read(name))
            store = LocalMarketSnapshotStore(root / "canonical")

            sessions, index = import_nse_historical_range(
                staging_root=root / "staging",
                archive_root=root / "source-archives",
                store=store,
                start=SESSION,
                end=SESSION,
                observed_at=OBSERVED_AT,
            )
            verified = load_verified_nse_historical_archive_range(
                store,
                index_snapshot_id=index.manifest.snapshot_id,
            )

            self.assertEqual(
                sessions[0].evidence_profile,
                EVIDENCE_PROFILE_PRICE_UDIFF,
            )
            self.assertEqual(index.normalized_payload["incomplete_evidence_session_count"], 1)
            self.assertEqual(verified.incomplete_evidence_session_count, 1)
            self.assertEqual(
                verified.evidence_profile_counts[EVIDENCE_PROFILE_PRICE_UDIFF],
                1,
            )
            self.assertGreater(verified.identity_issue_count, 0)

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

            verified = load_verified_nse_historical_archive_range(
                store,
                index_snapshot_id=first_index.manifest.snapshot_id,
            )
            self.assertEqual(verified.range_start, SESSION)
            self.assertEqual(verified.range_end, SESSION)
            self.assertEqual(verified.record_count, 1)
            self.assertEqual(verified.identity_issue_count, 1)
            self.assertEqual(verified.identity_quarantined_session_count, 1)
            self.assertEqual(verified.incomplete_evidence_session_count, 0)
            self.assertEqual(
                verified.evidence_profile_counts[EVIDENCE_PROFILE_COMPLETE],
                1,
            )

    def test_range_verifier_rejects_mutated_session_record_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            staging = root / "staging" / SESSION.isoformat()
            archives = root / "source-archives" / SESSION.isoformat()
            staging.mkdir(parents=True)
            archives.mkdir(parents=True)
            archive_path = archives / "Reports-Archives-Multiple-15072026.zip"
            archive_path.write_bytes(archive_bytes())
            with zipfile.ZipFile(archive_path) as archive:
                for name in archive.namelist():
                    (staging / name).write_bytes(archive.read(name))
            store = LocalMarketSnapshotStore(root / "canonical")
            sessions, index = import_nse_historical_range(
                staging_root=root / "staging",
                archive_root=root / "source-archives",
                store=store,
                start=SESSION,
                end=SESSION,
                observed_at=OBSERVED_AT,
            )
            session = store.get(
                NSE_HISTORICAL_ARCHIVE_EQ_DATASET,
                sessions[0].snapshot_id,
            )
            payload = dict(session.normalized_payload)
            record = dict(payload["records"][0])
            record["close"] = record["open"]
            payload["records"] = (record,)
            mutated = replace(session, normalized_payload=payload)

            class Reader:
                def get(self, dataset: str, snapshot_id: str):
                    if dataset == NSE_HISTORICAL_ARCHIVE_EQ_DATASET:
                        return mutated
                    return index

            with self.assertRaisesRegex(
                NseHistoricalArchiveRangeError,
                "archive range session payload bytes are invalid",
            ):
                load_verified_nse_historical_archive_range(
                    Reader(),
                    index_snapshot_id=index.manifest.snapshot_id,
                )

    def test_range_import_of_one_file_session_reports_unreconciled_profile_and_rejects_tampering(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            staging = root / "staging" / SESSION.isoformat()
            archives = root / "source-archives" / SESSION.isoformat()
            staging.mkdir(parents=True)
            archives.mkdir(parents=True)
            archive_path = archives / "Reports-Archives-Multiple-15072026.zip"
            archive_path.write_bytes(_one_file_archive_bytes())
            with zipfile.ZipFile(archive_path) as archive:
                for name in archive.namelist():
                    (staging / name).write_bytes(archive.read(name))
            store = LocalMarketSnapshotStore(root / "canonical")

            sessions, index = import_nse_historical_range(
                staging_root=root / "staging",
                archive_root=root / "source-archives",
                store=store,
                start=SESSION,
                end=SESSION,
                observed_at=OBSERVED_AT,
            )

            self.assertEqual(
                sessions[0].evidence_profile, EVIDENCE_PROFILE_UNRECONCILED
            )
            self.assertEqual(
                index.normalized_payload["incomplete_evidence_session_count"], 1
            )
            self.assertEqual(
                index.normalized_payload["evidence_profile_counts"][
                    EVIDENCE_PROFILE_UNRECONCILED
                ],
                1,
            )

            verified = load_verified_nse_historical_archive_range(
                store,
                index_snapshot_id=index.manifest.snapshot_id,
            )
            self.assertEqual(verified.incomplete_evidence_session_count, 1)
            self.assertEqual(
                verified.evidence_profile_counts[EVIDENCE_PROFILE_UNRECONCILED],
                1,
            )
            self.assertGreater(verified.identity_issue_count, 0)

            session_stored = store.get(
                NSE_HISTORICAL_ARCHIVE_EQ_DATASET,
                sessions[0].snapshot_id,
            )

            def tamper_case(mutate) -> None:
                payload = dict(session_stored.normalized_payload)
                record = dict(payload["records"][0])
                mutate(payload, record)
                payload["records"] = (record,)
                mutated = replace(session_stored, normalized_payload=payload)

                class Reader:
                    def get(self, dataset: str, snapshot_id: str):
                        if dataset == NSE_HISTORICAL_ARCHIVE_EQ_DATASET:
                            return mutated
                        return index

                with self.assertRaises(NseHistoricalArchiveRangeError):
                    load_verified_nse_historical_archive_range(
                        Reader(),
                        index_snapshot_id=index.manifest.snapshot_id,
                    )

            cases = {
                "adds_an_isin": lambda payload, record: record.__setitem__(
                    "validated_isin", "INE009A01021"
                ),
                "adds_an_instrument_id": lambda payload, record: record.__setitem__(
                    "financial_instrument_id", 1594
                ),
                "adds_surveillance": lambda payload, record: record.__setitem__(
                    "surveillance_indicators", {"GSM": "1"}
                ),
                "changes_unavailable_status": lambda payload, record: record.__setitem__(
                    "identity_status", "SECURITY_MASTER_EVIDENCE_UNAVAILABLE"
                ),
                "claims_mismatched_profile": lambda payload, record: payload.__setitem__(
                    "evidence_profile", EVIDENCE_PROFILE_PRICE_UDIFF
                ),
            }
            for label, mutate in cases.items():
                with self.subTest(label):
                    tamper_case(mutate)

    def test_replayed_record_and_issue_ids_match_parser_output(self) -> None:
        payload = _one_file_normalized_payload()
        record = payload["records"][0]
        issue = payload["identity_issues"][0]

        self.assertEqual(_replay_record_id(record), record["record_id"])
        self.assertEqual(_replay_issue_id(issue), issue["issue_id"])

    def test_self_consistent_record_field_violations_are_rejected_by_semantic_guard(
        self,
    ) -> None:
        mutations = {
            "validated_isin": "INE009A01021",
            "financial_instrument_id": 1594,
            "normal_market_eligible": True,
        }
        for field, value in mutations.items():
            with self.subTest(field):
                payload = _one_file_normalized_payload()
                records = list(payload["records"])
                record = dict(records[0])
                record[field] = value
                _recompute_record_id(record)
                records[0] = record
                payload["records"] = tuple(records)

                with tempfile.TemporaryDirectory() as temporary:
                    store = LocalMarketSnapshotStore(Path(temporary))
                    session_snapshot = _store_one_file_session(store, payload)
                    index_snapshot = _store_one_file_index(
                        store, session_snapshot, payload
                    )

                    with self.assertRaises(NseHistoricalArchiveRangeError):
                        load_verified_nse_historical_archive_range(
                            store,
                            index_snapshot_id=index_snapshot.manifest.snapshot_id,
                        )

    def test_self_consistent_issue_field_violations_are_rejected(self) -> None:
        def add_identifier(issue: dict[str, object]) -> None:
            issue["udiff_source_identifier"] = "INE009A01021"

        def change_status(issue: dict[str, object]) -> None:
            issue["status"] = "SECURITY_MASTER_EVIDENCE_UNAVAILABLE"

        cases = {
            "adds_udiff_identifier": add_identifier,
            "changes_status": change_status,
        }
        for label, mutate in cases.items():
            with self.subTest(label):
                payload = _one_file_normalized_payload()
                issues = list(payload["identity_issues"])
                issue = dict(issues[0])
                mutate(issue)
                _recompute_issue_id(issue)
                issues[0] = issue
                payload["identity_issues"] = tuple(issues)

                with tempfile.TemporaryDirectory() as temporary:
                    store = LocalMarketSnapshotStore(Path(temporary))
                    session_snapshot = _store_one_file_session(store, payload)
                    index_snapshot = _store_one_file_index(
                        store, session_snapshot, payload
                    )

                    with self.assertRaises(NseHistoricalArchiveRangeError):
                        load_verified_nse_historical_archive_range(
                            store,
                            index_snapshot_id=index_snapshot.manifest.snapshot_id,
                        )

    def test_duplicate_issue_entries_cannot_satisfy_record_to_issue_correspondence(
        self,
    ) -> None:
        payload = _one_file_normalized_payload()
        issue = payload["identity_issues"][0]
        payload["identity_issues"] = (issue, dict(issue))
        payload["identity_issue_count"] = 2

        with tempfile.TemporaryDirectory() as temporary:
            store = LocalMarketSnapshotStore(Path(temporary))
            session_snapshot = _store_one_file_session(store, payload)
            index_snapshot = _store_one_file_index(store, session_snapshot, payload)

            with self.assertRaises(NseHistoricalArchiveRangeError):
                load_verified_nse_historical_archive_range(
                    store,
                    index_snapshot_id=index_snapshot.manifest.snapshot_id,
                )

    def test_self_consistent_but_stale_ids_are_rejected_by_replay_check(self) -> None:
        stale_sha256 = "0" * 64

        def stale_record(payload: dict[str, object]) -> None:
            records = list(payload["records"])
            record = dict(records[0])
            record["record_id"] = stale_sha256
            records[0] = record
            payload["records"] = tuple(records)

        def stale_issue(payload: dict[str, object]) -> None:
            issues = list(payload["identity_issues"])
            issue = dict(issues[0])
            issue["issue_id"] = stale_sha256
            issues[0] = issue
            payload["identity_issues"] = tuple(issues)

        for label, mutate in {
            "stale_record_id": stale_record,
            "stale_issue_id": stale_issue,
        }.items():
            with self.subTest(label):
                payload = _one_file_normalized_payload()
                mutate(payload)

                with tempfile.TemporaryDirectory() as temporary:
                    store = LocalMarketSnapshotStore(Path(temporary))
                    session_snapshot = _store_one_file_session(store, payload)
                    index_snapshot = _store_one_file_index(
                        store, session_snapshot, payload
                    )

                    with self.assertRaises(NseHistoricalArchiveRangeError):
                        load_verified_nse_historical_archive_range(
                            store,
                            index_snapshot_id=index_snapshot.manifest.snapshot_id,
                        )


LEGACY_SESSION = date(2019, 1, 2)
_LEGACY_MONTH_ABBR = {
    1: "JAN", 2: "FEB", 3: "MAR", 4: "APR", 5: "MAY", 6: "JUN",
    7: "JUL", 8: "AUG", 9: "SEP", 10: "OCT", 11: "NOV", 12: "DEC",
}


def _legacy_date_text(session: date) -> str:
    return f"{session:%d}-{_LEGACY_MONTH_ABBR[session.month]}-{session:%Y}"


def _legacy_bhavcopy_row(
    *,
    symbol: str = "20MICRONS",
    series: str = "EQ",
    session: date = LEGACY_SESSION,
    open_price: str = "30.00",
    high: str = "31.00",
    low: str = "29.50",
    close: str = "30.50",
    last: str = "30.50",
    previous_close: str = "29.80",
    traded_quantity: str = "10000",
    traded_value: str = "305000.00",
    timestamp: str | None = None,
    total_trades: str = "50",
    isin: str = "INE144J01027",
    terminal_field: str = "",
) -> list[str]:
    values = {
        "SYMBOL": symbol,
        "SERIES": series,
        "OPEN": open_price,
        "HIGH": high,
        "LOW": low,
        "CLOSE": close,
        "LAST": last,
        "PREVCLOSE": previous_close,
        "TOTTRDQTY": traded_quantity,
        "TOTTRDVAL": traded_value,
        "TIMESTAMP": timestamp if timestamp is not None else _legacy_date_text(session),
        "TOTALTRADES": total_trades,
        "ISIN": isin,
    }
    # _LEGACY_FULL_BHAVCOPY_HEADER's last element is the canonical trailing
    # empty column caused by the real file's terminal comma.
    named_header = _LEGACY_FULL_BHAVCOPY_HEADER[:-1]
    return [values[name] for name in named_header] + [terminal_field]


def _mto_row(
    *,
    serial: object = 1,
    symbol: str = "20MICRONS",
    series: str = "EQ",
    traded_quantity: object = 10000,
    deliverable_quantity: object = 6789,
    percent: str | None = None,
    record_type: str = _MTO_DATA_RECORD_TYPE,
) -> list[str]:
    if percent is None:
        computed = (
            Decimal(100) * Decimal(deliverable_quantity) / Decimal(traded_quantity)
        ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        percent = str(computed)
    return [
        record_type,
        str(serial),
        symbol,
        series,
        str(traded_quantity),
        str(deliverable_quantity),
        percent,
    ]


def _mto_bytes(
    rows: list[list[str]],
    *,
    session: date = LEGACY_SESSION,
    record_count: object = None,
    total_deliverable_quantity: object = None,
    control_date_text: str | None = None,
    trade_date_text: str | None = None,
    settlement_date_text: str | None = None,
    settlement_type: str = "N",
    settlement_no: str = "2019002",
    title_line: str = _MTO_TITLE_LINE,
    header_record: tuple[str, ...] = _MTO_HEADER_RECORD,
) -> bytes:
    control_date = (
        control_date_text if control_date_text is not None else f"{session:%d%m%Y}"
    )
    trade_date = trade_date_text if trade_date_text is not None else _legacy_date_text(session)
    settlement_date = (
        settlement_date_text if settlement_date_text is not None else _legacy_date_text(session)
    )
    effective_count = len(rows) if record_count is None else record_count
    effective_total_deliverable = (
        sum(int(row[5]) for row in rows)
        if total_deliverable_quantity is None
        else total_deliverable_quantity
    )
    body = [
        [
            _MTO_CONTROL_RECORD_TYPE,
            "MTO",
            control_date,
            str(effective_total_deliverable),
            str(effective_count),
        ],
        [
            f"Trade Date <{trade_date}>",
            f"Settlement Type <{settlement_type}>",
            f"Settlement No <{settlement_no}>",
            f"Settlement Date <{settlement_date}>",
        ],
        list(header_record),
        *rows,
    ]
    return _csv((title_line,), body)


def _legacy_zip_bytes(
    session: date = LEGACY_SESSION,
    *,
    inner_name: str | None = None,
    csv_bytes: bytes | None = None,
    extra_entries: tuple[tuple[str, bytes], ...] = (),
) -> bytes:
    legacy_zip_name, _ = _expected_legacy_names(session)
    effective_inner_name = (
        inner_name if inner_name is not None else legacy_zip_name[: -len(".zip")]
    )
    effective_csv = (
        csv_bytes
        if csv_bytes is not None
        else _csv(_LEGACY_FULL_BHAVCOPY_HEADER, [_legacy_bhavcopy_row(session=session)])
    )
    entries = [(effective_inner_name, effective_csv), *extra_entries]
    return _zip(entries)


def _legacy_outer_zip_bytes(
    session: date = LEGACY_SESSION,
    *,
    legacy_zip_payload: bytes | None = None,
    mto_payload: bytes | None = None,
    extra_entries: tuple[tuple[str, bytes], ...] = (),
    omit_zip: bool = False,
    omit_mto: bool = False,
) -> bytes:
    legacy_zip_name, mto_name = _expected_legacy_names(session)
    effective_zip_payload = (
        legacy_zip_payload if legacy_zip_payload is not None else _legacy_zip_bytes(session)
    )
    effective_mto_payload = (
        mto_payload
        if mto_payload is not None
        else _mto_bytes([_mto_row()], session=session)
    )
    entries: list[tuple[str, bytes]] = []
    if not omit_zip:
        entries.append((legacy_zip_name, effective_zip_payload))
    if not omit_mto:
        entries.append((mto_name, effective_mto_payload))
    entries.extend(extra_entries)
    return _zip(entries)


class NseLegacyBhavcopyMtoProfileTests(unittest.TestCase):
    def test_valid_legacy_pair_parses_to_unreconciled_with_derived_values(self) -> None:
        parsed = parse_nse_historical_archive_bytes(
            _legacy_outer_zip_bytes(),
            original_filename=f"Reports-Archives-Multiple-{LEGACY_SESSION:%d%m%Y}.zip",
        )

        payload = parsed.normalized_payload
        self.assertEqual(payload["evidence_profile"], EVIDENCE_PROFILE_UNRECONCILED)
        self.assertEqual(
            payload["missing_evidence"],
            ("UDIFF_BHAVCOPY", "NSE_CM_SECURITY_MASTER", "REG1_SURVEILLANCE"),
        )
        self.assertEqual(len(parsed.source_entry_sha256), 2)
        legacy_zip_name, mto_name = _expected_legacy_names(LEGACY_SESSION)
        self.assertEqual(
            {name for name, _ in parsed.source_entry_sha256},
            {legacy_zip_name, mto_name},
        )
        self.assertTrue(payload["collection_only"])
        self.assertFalse(payload["actionable"])
        self.assertFalse(payload["training_eligible"])
        self.assertEqual(payload["identity_issue_count"], 1)
        self.assertEqual(len(payload["records"]), 1)

        record = payload["records"][0]
        self.assertEqual(record["listing_key"], "NSE:20MICRONS")
        self.assertEqual(
            record["identity_status"],
            IDENTITY_STATUS_UDIFF_AND_SECURITY_MASTER_EVIDENCE_UNAVAILABLE,
        )
        self.assertIsNone(record["validated_isin"])
        self.assertIsNone(record["financial_instrument_id"])
        self.assertIsNone(record["udiff_source_identifier"])
        self.assertEqual(record["surveillance_indicators"], {})
        self.assertEqual(record["volume"], 10000)
        self.assertEqual(record["trade_count"], 50)
        self.assertEqual(record["delivery_quantity"], 6789)
        self.assertEqual(record["delivery_percent"], Decimal("67.89"))
        # 305000.00 / 10000 and 305000.00 / 100_000, both ROUND_HALF_UP 2dp.
        self.assertEqual(record["average_price"], Decimal("30.50"))
        self.assertEqual(record["turnover_lacs"], Decimal("3.05"))
        self.assertEqual(record["open"], Decimal("30.00"))
        self.assertEqual(record["close"], Decimal("30.50"))

        issue = payload["identity_issues"][0]
        self.assertEqual(
            issue["status"],
            IDENTITY_STATUS_UDIFF_AND_SECURITY_MASTER_EVIDENCE_UNAVAILABLE,
        )
        self.assertIsNone(issue["udiff_financial_instrument_id"])
        self.assertEqual(
            record["record_id"], _replay_record_id(record)
        )
        self.assertEqual(issue["issue_id"], _replay_issue_id(issue))

    def test_valid_legacy_pair_range_import_and_verify_replays(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            staging = root / "staging" / LEGACY_SESSION.isoformat()
            archives = root / "source-archives" / LEGACY_SESSION.isoformat()
            staging.mkdir(parents=True)
            archives.mkdir(parents=True)
            archive_path = (
                archives
                / f"Reports-Archives-Multiple-{LEGACY_SESSION:%d%m%Y}.zip"
            )
            archive_path.write_bytes(_legacy_outer_zip_bytes())
            with zipfile.ZipFile(archive_path) as archive:
                for name in archive.namelist():
                    (staging / name).write_bytes(archive.read(name))
            store = LocalMarketSnapshotStore(root / "canonical")

            sessions, index = import_nse_historical_range(
                staging_root=root / "staging",
                archive_root=root / "source-archives",
                store=store,
                start=LEGACY_SESSION,
                end=LEGACY_SESSION,
                observed_at=OBSERVED_AT,
            )
            self.assertEqual(sessions[0].evidence_profile, EVIDENCE_PROFILE_UNRECONCILED)
            self.assertEqual(sessions[0].record_count, 1)

            verified = load_verified_nse_historical_archive_range(
                store,
                index_snapshot_id=index.manifest.snapshot_id,
            )
            self.assertEqual(
                verified.evidence_profile_counts[EVIDENCE_PROFILE_UNRECONCILED],
                1,
            )
            self.assertEqual(verified.identity_issue_count, 1)
            self.assertEqual(verified.record_count, 1)

    def test_zero_padded_control_totals_and_differing_settlement_date_stage(self) -> None:
        # Matches the real file's exact conventions: zero-padded control
        # fields (e.g. "0001716") and a Settlement Date that genuinely
        # differs from Trade Date (T+n settlement) -- neither is rejected.
        mto_payload = _mto_bytes(
            [_mto_row()],
            record_count="0000001",
            total_deliverable_quantity="0006789",
            settlement_type="T",
            settlement_no="0002019002",
            settlement_date_text="04-JAN-2019",
        )
        parsed = parse_nse_historical_archive_bytes(
            _legacy_outer_zip_bytes(mto_payload=mto_payload),
            original_filename=f"Reports-Archives-Multiple-{LEGACY_SESSION:%d%m%Y}.zip",
        )
        self.assertEqual(len(parsed.normalized_payload["records"]), 1)
        self.assertEqual(
            parsed.normalized_payload["records"][0]["delivery_quantity"], 6789
        )

    def test_outer_entry_set_missing_extra_mixed_duplicate_fail_closed(self) -> None:
        modern_name = f"sec_bhavdata_full_{LEGACY_SESSION:%d%m%Y}.csv"
        cases = {
            "missing_mto": _legacy_outer_zip_bytes(omit_mto=True),
            "missing_zip": _legacy_outer_zip_bytes(omit_zip=True),
            "extra_entry": _legacy_outer_zip_bytes(
                extra_entries=(("unexpected.txt", b"unsafe"),)
            ),
            "mixed_with_modern_single_file": _legacy_outer_zip_bytes(
                extra_entries=(
                    (
                        modern_name,
                        _csv(
                            FULL_BHAVCOPY_DELIVERY_HEADER,
                            [_full_row(trade_date=LEGACY_SESSION)],
                        ),
                    ),
                )
            ),
        }
        for label, payload in cases.items():
            with self.subTest(label), self.assertRaises(
                NseHistoricalArchiveIntegrityError
            ):
                parse_nse_historical_archive_bytes(
                    payload,
                    original_filename=(
                        f"Reports-Archives-Multiple-{LEGACY_SESSION:%d%m%Y}.zip"
                    ),
                )

        legacy_zip_name, mto_name = _expected_legacy_names(LEGACY_SESSION)
        duplicate_stream_entries = [
            (legacy_zip_name, _legacy_zip_bytes(LEGACY_SESSION)),
            (mto_name, _mto_bytes([_mto_row()])),
        ]
        # Duplicate outer entry names: build the zip by hand since _zip()
        # de-duplicates nothing but ZipFile.writestr allows repeats.
        stream = io.BytesIO(_zip(duplicate_stream_entries))
        with zipfile.ZipFile(stream, "a") as archive:
            archive.writestr(legacy_zip_name, _legacy_zip_bytes(LEGACY_SESSION))
        with self.subTest("duplicate_entry"), self.assertRaises(
            NseHistoricalArchiveIntegrityError
        ):
            parse_nse_historical_archive_bytes(
                stream.getvalue(),
                original_filename=(
                    f"Reports-Archives-Multiple-{LEGACY_SESSION:%d%m%Y}.zip"
                ),
            )

    def test_nested_zip_structural_violations_fail_closed(self) -> None:
        legacy_zip_name, _ = _expected_legacy_names(LEGACY_SESSION)
        inner_name = legacy_zip_name[: -len(".zip")]
        good_csv = _csv(
            _LEGACY_FULL_BHAVCOPY_HEADER, [_legacy_bhavcopy_row()]
        )

        empty_inner = _zip([])
        extra_inner = _zip([(inner_name, good_csv), ("extra.csv", good_csv)])
        wrong_name_inner = _zip([("wrong_name.csv", good_csv)])
        empty_content_inner = _zip([(inner_name, b"")])
        oversized_inner = _zip([(inner_name, b"A" * (64 * 1024 * 1024 + 1024))])
        invalid_container = b"not a zip file at all"

        directory_stream = io.BytesIO(_zip([]))
        with zipfile.ZipFile(directory_stream, "a") as archive:
            archive.writestr(zipfile.ZipInfo(f"{inner_name}/"), b"")
        directory_inner = directory_stream.getvalue()

        encrypted_buffer = _zip([(inner_name, good_csv)])
        # Simulate an encrypted entry by setting the encryption flag bit
        # directly on the local/central-directory metadata.
        raw = bytearray(encrypted_buffer)
        # Central directory general-purpose bit flag is a 2-byte field at
        # offset 8 within each 46-byte central-directory file header; set
        # bit 0 (encrypted) there. Locate it by the local file header
        # signature's matching general-purpose flag at offset 6 instead,
        # which is simpler and sufficient for zipfile to see flag_bits & 1.
        local_signature = b"PK\x03\x04"
        index = raw.find(local_signature)
        self.assertNotEqual(index, -1)
        raw[index + 6] |= 0x01
        central_signature = b"PK\x01\x02"
        central_index = raw.find(central_signature)
        self.assertNotEqual(central_index, -1)
        raw[central_index + 8] |= 0x01
        encrypted_inner = bytes(raw)

        cases = {
            "empty_inner_zip": empty_inner,
            "extra_inner_entry": extra_inner,
            "wrong_inner_name": wrong_name_inner,
            "empty_inner_content": empty_content_inner,
            "oversized_inner_content": oversized_inner,
            "invalid_container": invalid_container,
            "directory_inner_entry": directory_inner,
            "encrypted_inner_entry": encrypted_inner,
        }
        for label, legacy_zip_payload in cases.items():
            with self.subTest(label), self.assertRaises(
                NseHistoricalArchiveIntegrityError
            ):
                parse_nse_historical_archive_bytes(
                    _legacy_outer_zip_bytes(legacy_zip_payload=legacy_zip_payload),
                    original_filename=(
                        f"Reports-Archives-Multiple-{LEGACY_SESSION:%d%m%Y}.zip"
                    ),
                )

    def test_legacy_bhavcopy_row_violations_fail_closed(self) -> None:
        bad_header = _csv(
            tuple(_LEGACY_FULL_BHAVCOPY_HEADER[:-1]) + ("EXTRA",),
            [_legacy_bhavcopy_row()],
        )
        bad_row_width = _csv(
            _LEGACY_FULL_BHAVCOPY_HEADER,
            [_legacy_bhavcopy_row()[:-1]],
        )
        wrong_date = _csv(
            _LEGACY_FULL_BHAVCOPY_HEADER,
            [_legacy_bhavcopy_row(timestamp="01-JAN-2019")],
        )
        invalid_date_text = _csv(
            _LEGACY_FULL_BHAVCOPY_HEADER,
            [_legacy_bhavcopy_row(timestamp="2019-01-02")],
        )
        duplicate_key = _csv(
            _LEGACY_FULL_BHAVCOPY_HEADER,
            [_legacy_bhavcopy_row(), _legacy_bhavcopy_row()],
        )
        malformed_numeric = _csv(
            _LEGACY_FULL_BHAVCOPY_HEADER,
            [_legacy_bhavcopy_row(open_price="not-a-number")],
        )
        bad_ohlc = _csv(
            _LEGACY_FULL_BHAVCOPY_HEADER,
            [_legacy_bhavcopy_row(high="10.00")],
        )
        no_data_rows = _csv(_LEGACY_FULL_BHAVCOPY_HEADER, [])
        invalid_utf8 = (
            _csv(_LEGACY_FULL_BHAVCOPY_HEADER, [_legacy_bhavcopy_row()])
            + b"\xff"
        )
        non_empty_terminal_field = _csv(
            _LEGACY_FULL_BHAVCOPY_HEADER,
            [_legacy_bhavcopy_row(terminal_field="X")],
        )

        cases = {
            "bad_header": bad_header,
            "bad_row_width": bad_row_width,
            "wrong_date": wrong_date,
            "invalid_date_text": invalid_date_text,
            "duplicate_key": duplicate_key,
            "malformed_numeric": malformed_numeric,
            "bad_ohlc": bad_ohlc,
            "no_data_rows": no_data_rows,
            "invalid_utf8": invalid_utf8,
            "non_empty_terminal_field": non_empty_terminal_field,
        }
        for label, csv_bytes in cases.items():
            with self.subTest(label), self.assertRaises(
                NseHistoricalArchiveIntegrityError
            ):
                parse_nse_historical_archive_bytes(
                    _legacy_outer_zip_bytes(
                        legacy_zip_payload=_legacy_zip_bytes(csv_bytes=csv_bytes)
                    ),
                    original_filename=(
                        f"Reports-Archives-Multiple-{LEGACY_SESSION:%d%m%Y}.zip"
                    ),
                )

    def test_non_eq_bhavcopy_row_anomaly_is_excluded_without_weakening_eq_validation(
        self,
    ) -> None:
        # Mirrors a real observed case in the official 02-Jan-2019 archive:
        # a non-EQ row (there, SRTRANSFIN/YT with LAST=0.0) must never
        # block the EQ session. Only the EQ-gated business constraints
        # (strictly positive prices, OHLC consistency, strictly positive
        # traded quantity/trade count) are relaxed for excluded rows --
        # every structural check still runs on them (see the next test).
        rows = [
            _legacy_bhavcopy_row(),
            _legacy_bhavcopy_row(
                symbol="BADROW",
                series="YT",
                last="0.0",
                isin="INE467B01029",
            ),
        ]
        parsed = parse_nse_historical_archive_bytes(
            _legacy_outer_zip_bytes(
                legacy_zip_payload=_legacy_zip_bytes(
                    csv_bytes=_csv(_LEGACY_FULL_BHAVCOPY_HEADER, rows)
                )
            ),
            original_filename=f"Reports-Archives-Multiple-{LEGACY_SESSION:%d%m%Y}.zip",
        )
        self.assertEqual(len(parsed.normalized_payload["records"]), 1)
        self.assertEqual(
            parsed.normalized_payload["records"][0]["symbol"], "20MICRONS"
        )

    def test_non_eq_bhavcopy_row_structural_violations_fail_closed(self) -> None:
        # The adversarial gap Codex's probe found in revision 2: an
        # excluded non-EQ row must still be structurally validated (wrong
        # date, invalid symbol/series/ISIN, duplicate key, malformed
        # numeric text) -- none of these are "business" constraints, and
        # none may slip through unnoticed just because the row is excluded
        # from the EQ join.
        def _archive_with_bad_row(bad_row: list[str]) -> bytes:
            rows = [_legacy_bhavcopy_row(), bad_row]
            return _legacy_outer_zip_bytes(
                legacy_zip_payload=_legacy_zip_bytes(
                    csv_bytes=_csv(_LEGACY_FULL_BHAVCOPY_HEADER, rows)
                )
            )

        wrong_date = _archive_with_bad_row(
            _legacy_bhavcopy_row(
                symbol="BADROW", series="YT", timestamp="01-JAN-1900",
                isin="INE467B01029",
            )
        )
        invalid_symbol = _archive_with_bad_row(
            _legacy_bhavcopy_row(
                symbol="bad symbol!", series="YT", isin="INE467B01029",
            )
        )
        invalid_series = _archive_with_bad_row(
            _legacy_bhavcopy_row(
                symbol="BADROW", series="too-long-series", isin="INE467B01029",
            )
        )
        invalid_isin = _archive_with_bad_row(
            _legacy_bhavcopy_row(symbol="BADROW", series="YT", isin="not an isin")
        )
        duplicate_key = _legacy_outer_zip_bytes(
            legacy_zip_payload=_legacy_zip_bytes(
                csv_bytes=_csv(
                    _LEGACY_FULL_BHAVCOPY_HEADER,
                    [
                        _legacy_bhavcopy_row(),
                        _legacy_bhavcopy_row(series="YT", isin="INE467B01029"),
                        _legacy_bhavcopy_row(series="YT", isin="INE467B01029"),
                    ],
                )
            )
        )
        malformed_numeric = _archive_with_bad_row(
            _legacy_bhavcopy_row(
                symbol="BADROW",
                series="YT",
                open_price="not-a-number",
                isin="INE467B01029",
            )
        )

        cases = {
            "wrong_date": wrong_date,
            "invalid_symbol": invalid_symbol,
            "invalid_series": invalid_series,
            "invalid_isin": invalid_isin,
            "duplicate_key": duplicate_key,
            "malformed_numeric": malformed_numeric,
        }
        for label, payload in cases.items():
            with self.subTest(label), self.assertRaises(
                NseHistoricalArchiveIntegrityError
            ):
                parse_nse_historical_archive_bytes(
                    payload,
                    original_filename=(
                        f"Reports-Archives-Multiple-{LEGACY_SESSION:%d%m%Y}.zip"
                    ),
                )

    def test_mto_structure_and_accounting_violations_fail_closed(self) -> None:
        bad_title = _mto_bytes([_mto_row()], title_line="Wrong Title Line")
        bad_control_record_type = _mto_bytes([_mto_row()]).replace(
            b"10,MTO,", b"99,MTO,", 1
        )
        bad_control_literal = _mto_bytes([_mto_row()]).replace(
            b"10,MTO,", b"10,XXX,", 1
        )
        wrong_control_date = _mto_bytes([_mto_row()], control_date_text="01011900")
        invalid_control_date_text = _mto_bytes(
            [_mto_row()], control_date_text="2019-01-02"
        )
        bad_settlement_structure = _mto_bytes([_mto_row()]).replace(
            b"Trade Date <02-JAN-2019>", b"Trade Date 02-JAN-2019", 1
        )
        wrong_trade_date = _mto_bytes([_mto_row()], trade_date_text="01-JAN-2019")
        invalid_trade_date_text = _mto_bytes(
            [_mto_row()], trade_date_text="2019-01-02"
        )
        invalid_settlement_date_text = _mto_bytes(
            [_mto_row()], settlement_date_text="not-a-date"
        )
        bad_header = _mto_bytes([_mto_row()]).replace(
            b"Record Type", b"RecordType", 1
        )
        bad_record_type = _mto_bytes([_mto_row(record_type="10")])
        bad_serial = _mto_bytes(
            [_mto_row(serial=1), _mto_row(serial=3, symbol="OTHERCO")]
        )
        duplicate_key = _mto_bytes(
            [_mto_row(serial=1), _mto_row(serial=2)]
        )
        malformed_row_width = _mto_bytes([_mto_row()]).replace(
            b"20,1,20MICRONS,EQ,10000,6789,67.89",
            b"20,1,20MICRONS,EQ,10000,6789",
            1,
        )
        deliverable_exceeds_traded = _mto_bytes(
            [_mto_row(deliverable_quantity=99999, percent="100.00")]
        )
        percent_mismatch = _mto_bytes([_mto_row(percent="1.23")])
        bad_record_count = _mto_bytes([_mto_row()], record_count=5)
        bad_total_deliverable_quantity = _mto_bytes(
            [_mto_row()], total_deliverable_quantity=1
        )
        no_data_rows = _csv(
            (_MTO_TITLE_LINE,),
            [
                [
                    _MTO_CONTROL_RECORD_TYPE,
                    "MTO",
                    f"{LEGACY_SESSION:%d%m%Y}",
                    "0",
                    "0",
                ],
                [
                    f"Trade Date <{_legacy_date_text(LEGACY_SESSION)}>",
                    "Settlement Type <N>",
                    "Settlement No <2019002>",
                    f"Settlement Date <{_legacy_date_text(LEGACY_SESSION)}>",
                ],
                list(_MTO_HEADER_RECORD),
            ],
        )

        cases = {
            "bad_title": bad_title,
            "bad_control_record_type": bad_control_record_type,
            "bad_control_literal": bad_control_literal,
            "wrong_control_date": wrong_control_date,
            "invalid_control_date_text": invalid_control_date_text,
            "bad_settlement_structure": bad_settlement_structure,
            "wrong_trade_date": wrong_trade_date,
            "invalid_trade_date_text": invalid_trade_date_text,
            "invalid_settlement_date_text": invalid_settlement_date_text,
            "bad_header": bad_header,
            "bad_record_type": bad_record_type,
            "bad_serial": bad_serial,
            "duplicate_key": duplicate_key,
            "malformed_row_width": malformed_row_width,
            "deliverable_exceeds_traded": deliverable_exceeds_traded,
            "percent_mismatch": percent_mismatch,
            "bad_record_count": bad_record_count,
            "bad_total_deliverable_quantity": bad_total_deliverable_quantity,
            "no_data_rows": no_data_rows,
        }
        for label, mto_payload in cases.items():
            with self.subTest(label), self.assertRaises(
                NseHistoricalArchiveIntegrityError
            ):
                parse_nse_historical_archive_bytes(
                    _legacy_outer_zip_bytes(mto_payload=mto_payload),
                    original_filename=(
                        f"Reports-Archives-Multiple-{LEGACY_SESSION:%d%m%Y}.zip"
                    ),
                )

    def test_eq_key_coverage_and_traded_quantity_disagreement_fail_closed(self) -> None:
        missing_mto_key = _legacy_zip_bytes(
            csv_bytes=_csv(
                _LEGACY_FULL_BHAVCOPY_HEADER,
                [
                    _legacy_bhavcopy_row(),
                    _legacy_bhavcopy_row(symbol="OTHERCO", isin="INE467B01029"),
                ],
            )
        )
        extra_mto_key = _mto_bytes(
            [_mto_row(), _mto_row(serial=2, symbol="OTHERCO")]
        )
        traded_quantity_disagrees = _mto_bytes(
            [_mto_row(traded_quantity=9999, deliverable_quantity=6789, percent="67.90")]
        )

        cases = {
            "bhavcopy_has_extra_eq_key": (missing_mto_key, None),
            "mto_has_extra_eq_key": (None, extra_mto_key),
            "traded_quantity_disagreement": (None, traded_quantity_disagrees),
        }
        for label, (legacy_zip_payload, mto_payload) in cases.items():
            with self.subTest(label), self.assertRaises(
                NseHistoricalArchiveIntegrityError
            ):
                parse_nse_historical_archive_bytes(
                    _legacy_outer_zip_bytes(
                        legacy_zip_payload=legacy_zip_payload,
                        mto_payload=mto_payload,
                    ),
                    original_filename=(
                        f"Reports-Archives-Multiple-{LEGACY_SESSION:%d%m%Y}.zip"
                    ),
                )

    def test_decimal_results_are_invariant_under_hostile_global_context(self) -> None:
        # Build the fixture under the normal ambient context first -- only
        # the parse call itself runs under the hostile context, so a
        # mis-isolated production computation (not a corrupted fixture) is
        # what would make this test fail.
        archive_payload = _legacy_outer_zip_bytes()
        original_prec = decimal.getcontext().prec
        original_traps = dict(decimal.getcontext().traps)
        try:
            decimal.getcontext().prec = 1
            for trap in decimal.getcontext().traps:
                decimal.getcontext().traps[trap] = False
            parsed = parse_nse_historical_archive_bytes(
                archive_payload,
                original_filename=(
                    f"Reports-Archives-Multiple-{LEGACY_SESSION:%d%m%Y}.zip"
                ),
            )
        finally:
            decimal.getcontext().prec = original_prec
            for trap, value in original_traps.items():
                decimal.getcontext().traps[trap] = value

        record = parsed.normalized_payload["records"][0]
        self.assertEqual(record["average_price"], Decimal("30.50"))
        self.assertEqual(record["turnover_lacs"], Decimal("3.05"))
        self.assertEqual(record["delivery_percent"], Decimal("67.89"))


if __name__ == "__main__":
    unittest.main()
