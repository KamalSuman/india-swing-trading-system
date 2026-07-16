from __future__ import annotations

import csv
import gzip
import io
import json
import os
import shutil
import tempfile
import unittest
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from india_swing.daily_reports.artifact_store import LocalDailyBundleArtifactStore
from india_swing.daily_reports.cli import main as daily_reports_main
from india_swing.daily_reports.codec import encode_daily_bundle
from india_swing.daily_reports.models import (
    BundleEntryDisposition,
    DailyReportFamily,
    DailyReportIntegrityError,
    ReportDateRole,
)
from india_swing.daily_reports.parser import (
    COMPLETE_PRICE_BANDS_HEADER,
    FULL_BHAVCOPY_DELIVERY_HEADER,
    PRICE_BAND_CHANGES_HEADER,
    REG1_SURVEILLANCE_HEADER,
    SERIES_CHANGES_HEADER,
    SME_PRICE_BANDS_HEADER,
    UDIFF_BHAVCOPY_HEADER,
    NSE_DAILY_BUNDLE_FILENAME,
    NseDailyBundleParser,
)
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference_data.security_master import NSE_CM_MII_SECURITY_HEADER


UTC = timezone.utc
FIRST_SEEN = datetime(2026, 7, 15, 14, 30, tzinfo=UTC)
VALIDATED = FIRST_SEEN + timedelta(seconds=2)
BUNDLE_NAME = NSE_DAILY_BUNDLE_FILENAME


def csv_payload(header: tuple[str, ...], rows: list[list[str]]) -> bytes:
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    return stream.getvalue().encode("utf-8")


def zip_payload(entries: list[tuple[str, bytes]]) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, payload in entries:
            info = zipfile.ZipInfo(name, date_time=(2026, 7, 15, 12, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, payload)
    return output.getvalue()


def udiff_row(**overrides: str) -> list[str]:
    values = {name: "" for name in UDIFF_BHAVCOPY_HEADER}
    values.update(
        {
            "TradDt": "2026-07-15",
            "BizDt": "2026-07-15",
            "Sgmt": "CM",
            "Src": "NSE",
            "FinInstrmTp": "STK",
            "FinInstrmId": "1594",
            "ISIN": "INE009A01021",
            "TckrSymb": "INFY",
            "SctySrs": "EQ",
            "FinInstrmNm": "INFOSYS LIMITED",
            "OpnPric": "1600.00",
            "HghPric": "1620.00",
            "LwPric": "1590.00",
            "ClsPric": "1610.00",
            "LastPric": "1609.00",
            "PrvsClsgPric": "1595.00",
            "TtlTradgVol": "100",
            "TtlTrfVal": "160500.00",
            "TtlNbOfTxsExctd": "10",
            "SsnId": "F1",
            "NewBrdLotQty": "1",
        }
    )
    values.update(overrides)
    return [values[name] for name in UDIFF_BHAVCOPY_HEADER]


def full_row(**overrides: str) -> list[str]:
    stripped = tuple(name.strip() for name in FULL_BHAVCOPY_DELIVERY_HEADER)
    values = {
        "SYMBOL": "INFY",
        "SERIES": "EQ",
        "DATE1": "15-Jul-2026",
        "PREV_CLOSE": "1595.00",
        "OPEN_PRICE": "1600.00",
        "HIGH_PRICE": "1620.00",
        "LOW_PRICE": "1590.00",
        "LAST_PRICE": "1609.00",
        "CLOSE_PRICE": "1610.00",
        "AVG_PRICE": "1605.00",
        "TTL_TRD_QNTY": "100",
        "TURNOVER_LACS": "1.61",
        "NO_OF_TRADES": "10",
        "DELIV_QTY": "50",
        "DELIV_PER": "50.00",
    }
    values.update(overrides)
    return [values[name] if index == 0 else f" {values[name]}" for index, name in enumerate(stripped)]


def reg1_row(**overrides: str) -> list[str]:
    values: dict[str, str] = {}
    for name in REG1_SURVEILLANCE_HEADER:
        if name.startswith("Filler"):
            values[name] = ""
        elif name in {"ScripCode", "Symbol", "Nse Exclusive", "Status", "Series"}:
            values[name] = ""
        else:
            values[name] = "100"
    values.update(
        {
            "ScripCode": "NA",
            "Symbol": "INFY",
            "Nse Exclusive": "N",
            "Status": "A",
            "Series": "EQ",
        }
    )
    values.update(overrides)
    return [values[name] for name in REG1_SURVEILLANCE_HEADER]


def interop_security_master() -> bytes:
    values = {name: "" for name in NSE_CM_MII_SECURITY_HEADER}
    values.update(
        {
            "FinInstrmId": "999999",
            "TckrSymb": "BSEONLY$",
            "SctySrs": "EQ",
            "FinInstrmNm": "BSE EXCLUSIVE SECURITY",
            "ISIN": "INE009A01021",
            "PrtdToTrad": "2",
        }
    )
    content = csv_payload(
        NSE_CM_MII_SECURITY_HEADER,
        [[values[name] for name in NSE_CM_MII_SECURITY_HEADER]],
    )
    return gzip.compress(content, mtime=0)


def selected_entries(
    *,
    udiff_rows: list[list[str]] | None = None,
    full_rows: list[list[str]] | None = None,
    reg_rows: list[list[str]] | None = None,
    series_rows: list[list[str]] | None = None,
) -> list[tuple[str, bytes]]:
    udiff_csv = csv_payload(UDIFF_BHAVCOPY_HEADER, udiff_rows or [udiff_row()])
    udiff_name = "BhavCopy_NSE_CM_0_0_0_20260715_F_0000.csv"
    return [
        (f"{udiff_name}.zip", zip_payload([(udiff_name, udiff_csv)])),
        (
            "sec_bhavdata_full_15072026.csv",
            csv_payload(FULL_BHAVCOPY_DELIVERY_HEADER, full_rows or [full_row()]),
        ),
        (
            "REG1_IND140726.csv",
            csv_payload(REG1_SURVEILLANCE_HEADER, reg_rows or [reg1_row()]),
        ),
        (
            "sec_list_14072026.csv",
            csv_payload(
                COMPLETE_PRICE_BANDS_HEADER,
                [["INFY", "EQ", "INFOSYS LIMITED", "20", "-"]],
            ),
        ),
        (
            "sme_bands_complete_15072026.csv",
            csv_payload(
                SME_PRICE_BANDS_HEADER,
                [["SMECO", "SM", "SME COMPANY", "5", "-"]],
            ),
        ),
        (
            "eq_band_changes_15072026.csv",
            csv_payload(
                PRICE_BAND_CHANGES_HEADER,
                [["1", "INFY", "EQ", "INFOSYS LIMITED", "10", "20"]],
            ),
        ),
        (
            "series_change.csv",
            csv_payload(
                SERIES_CHANGES_HEADER,
                series_rows
                or [["INFY", "INFOSYS LIMITED", "BE", "EQ", "15-JUL-2026", "-"]],
            ),
        ),
    ]


def complete_bundle(**kwargs) -> bytes:
    return zip_payload(
        selected_entries(**kwargs)
        + [
            ("NSE_CM_security_15072026.csv.gz", interop_security_master()),
            ("ignored-report.csv", b"not,selected\n1,2\n"),
        ]
    )


def clock_sequence(*values: datetime):
    iterator = iter(values)
    return lambda: next(iterator)


class NseDailyBundleParserTests(unittest.TestCase):
    def test_parses_selected_families_and_quarantines_interoperability(self) -> None:
        parsed = NseDailyBundleParser().parse_bytes(
            complete_bundle(),
            original_filename=BUNDLE_NAME,
        )

        self.assertEqual(len(parsed.entries), 9)
        selected = [
            report
            for report in parsed.reports
            if report.disposition is BundleEntryDisposition.SELECTED_VALIDATED
        ]
        self.assertEqual(len(selected), 7)
        self.assertEqual(
            {report.family for report in selected},
            {
                DailyReportFamily.UDIFF_BHAVCOPY,
                DailyReportFamily.FULL_BHAVCOPY_DELIVERY,
                DailyReportFamily.SURVEILLANCE_REG1,
                DailyReportFamily.COMPLETE_PRICE_BANDS,
                DailyReportFamily.SME_PRICE_BANDS,
                DailyReportFamily.PRICE_BAND_CHANGES,
                DailyReportFamily.SERIES_CHANGES,
            },
        )
        quarantined = next(
            report
            for report in parsed.reports
            if report.family is DailyReportFamily.SECURITY_MASTER
        )
        self.assertEqual(
            quarantined.disposition,
            BundleEntryDisposition.QUARANTINED_INTEROPERABILITY_SECURITY_MASTER,
        )
        self.assertEqual(quarantined.row_count, 1)
        self.assertEqual(quarantined.rows, ())

        roles = {report.family: report.date_role for report in selected}
        self.assertEqual(roles[DailyReportFamily.UDIFF_BHAVCOPY], ReportDateRole.TRADE_DATE)
        self.assertEqual(
            roles[DailyReportFamily.SURVEILLANCE_REG1],
            ReportDateRole.PUBLICATION_DATE_NEXT_SESSION_EFFECTIVE,
        )
        self.assertEqual(
            roles[DailyReportFamily.COMPLETE_PRICE_BANDS],
            ReportDateRole.PUBLICATION_DATE_NEXT_SESSION_EFFECTIVE,
        )
        self.assertEqual(
            roles[DailyReportFamily.SME_PRICE_BANDS],
            ReportDateRole.CLAIMED_EFFECTIVE_DATE,
        )

    def test_parser_and_normalized_codec_are_deterministic(self) -> None:
        payload = complete_bundle()
        parser = NseDailyBundleParser()
        first = parser.parse_bytes(payload, original_filename=BUNDLE_NAME)
        second = parser.parse_bytes(payload, original_filename=BUNDLE_NAME)
        self.assertEqual(first, second)
        self.assertEqual(encode_daily_bundle(first), encode_daily_bundle(second))

    def test_internal_and_filename_dates_must_agree(self) -> None:
        bad_udiff = complete_bundle(
            udiff_rows=[udiff_row(TradDt="2026-07-14")]
        )
        bad_full = complete_bundle(
            full_rows=[full_row(DATE1="14-Jul-2026")]
        )
        for payload in (bad_udiff, bad_full):
            with self.subTest(), self.assertRaisesRegex(
                DailyReportIntegrityError,
                "date",
            ):
                NseDailyBundleParser().parse_bytes(
                    payload,
                    original_filename=BUNDLE_NAME,
                )

    def test_udiff_identity_and_isin_fail_closed(self) -> None:
        duplicate_id = udiff_row(TckrSymb="TCS", SctySrs="BE", ISIN="INE467B01029")
        duplicate_listing = udiff_row(
            FinInstrmId="2000",
            ISIN="INE467B01029",
        )
        bad_isin = udiff_row(ISIN="INE009A01020")
        for rows in (
            [udiff_row(), duplicate_id],
            [udiff_row(), duplicate_listing],
            [bad_isin],
        ):
            with self.subTest(), self.assertRaises(DailyReportIntegrityError):
                NseDailyBundleParser().parse_bytes(
                    complete_bundle(udiff_rows=rows),
                    original_filename=BUNDLE_NAME,
                )

    def test_delivery_and_cross_report_contradictions_fail_closed(self) -> None:
        cases = (
            full_row(DELIV_PER="50.01"),
            full_row(CLOSE_PRICE="1611.00"),
            full_row(AVG_PRICE="1605.01"),
            full_row(TURNOVER_LACS="1.60"),
        )
        for row in cases:
            with self.subTest(row=row), self.assertRaises(DailyReportIntegrityError):
                NseDailyBundleParser().parse_bytes(
                    complete_bundle(full_rows=[row]),
                    original_filename=BUNDLE_NAME,
                )

    def test_prices_and_implied_average_fail_closed(self) -> None:
        zero_udiff = udiff_row(
            OpnPric="0",
            HghPric="0",
            LwPric="0",
            ClsPric="0",
            LastPric="0",
            PrvsClsgPric="0",
        )
        impossible_average = udiff_row(TtlTrfVal="200000.00")
        impossible_full = full_row(AVG_PRICE="2000.00", TURNOVER_LACS="2.00")
        impossible_udiff_only = udiff_row(
            FinInstrmId="2000",
            ISIN="INE467B01029",
            TckrSymb="TCS",
            SctySrs="N0",
            FinInstrmNm="TATA CONSULTANCY SERVICES LIMITED",
            OpnPric="100.00",
            HghPric="100.00",
            LwPric="100.00",
            ClsPric="100.00",
            LastPric="100.00",
            PrvsClsgPric="100.00",
            TtlTrfVal="200000.00",
        )
        cases = (
            complete_bundle(udiff_rows=[zero_udiff]),
            complete_bundle(
                udiff_rows=[impossible_average],
                full_rows=[impossible_full],
            ),
            complete_bundle(udiff_rows=[udiff_row(), impossible_udiff_only]),
        )
        for payload in cases:
            with self.subTest(), self.assertRaises(DailyReportIntegrityError):
                NseDailyBundleParser().parse_bytes(
                    payload,
                    original_filename=BUNDLE_NAME,
                )

    def test_full_report_must_cover_every_row_of_each_reported_series(self) -> None:
        second_udiff = udiff_row(
            FinInstrmId="2000",
            ISIN="INE467B01029",
            TckrSymb="TCS",
            FinInstrmNm="TATA CONSULTANCY SERVICES LIMITED",
        )
        with self.assertRaisesRegex(DailyReportIntegrityError, "incompletely covers"):
            NseDailyBundleParser().parse_bytes(
                complete_bundle(udiff_rows=[udiff_row(), second_udiff]),
                original_filename=BUNDLE_NAME,
            )

        udiff_only_full_row = udiff_row(
            FinInstrmId="2000",
            ISIN="INE467B01029",
            TckrSymb="TCS",
            SctySrs="N0",
            FinInstrmNm="TATA CONSULTANCY SERVICES LIMITED",
        )
        full_noncore_row = full_row(SYMBOL="TCS", SERIES="N0")
        with self.assertRaisesRegex(DailyReportIntegrityError, "core equity"):
            NseDailyBundleParser().parse_bytes(
                complete_bundle(
                    udiff_rows=[udiff_row(), udiff_only_full_row],
                    full_rows=[full_noncore_row],
                ),
                original_filename=BUNDLE_NAME,
            )

    def test_noncanonical_decimals_fail_closed(self) -> None:
        for value in ("01.00", "1e3", "+1.00", ".5"):
            with self.subTest(value=value), self.assertRaises(
                DailyReportIntegrityError
            ):
                NseDailyBundleParser().parse_bytes(
                    complete_bundle(udiff_rows=[udiff_row(OpnPric=value)]),
                    original_filename=BUNDLE_NAME,
                )

    def test_reg1_unknown_stage_filler_and_duplicate_fail_closed(self) -> None:
        cases = (
            [reg1_row(ESM="99")],
            [reg1_row(GSM="5")],
            [reg1_row(Filler17="unexpected")],
            [reg1_row(), reg1_row()],
        )
        for rows in cases:
            with self.subTest(), self.assertRaises(DailyReportIntegrityError):
                NseDailyBundleParser().parse_bytes(
                    complete_bundle(reg_rows=rows),
                    original_filename=BUNDLE_NAME,
                )

    def test_one_series_transition_per_symbol_and_effective_date(self) -> None:
        rows = [
            ["INFY", "INFOSYS LIMITED", "BE", "EQ", "15-JUL-2026", "-"],
            ["INFY", "INFOSYS LIMITED", "EQ", "BZ", "15-JUL-2026", "-"],
        ]
        with self.assertRaisesRegex(DailyReportIntegrityError, "duplicate transition"):
            NseDailyBundleParser().parse_bytes(
                complete_bundle(series_rows=rows),
                original_filename=BUNDLE_NAME,
            )

    def test_unknown_schema_missing_family_and_unsafe_path_fail_closed(self) -> None:
        bad_header = list(FULL_BHAVCOPY_DELIVERY_HEADER)
        bad_header[-1] = "UNKNOWN"
        entries = selected_entries()
        entries[1] = (
            "sec_bhavdata_full_15072026.csv",
            csv_payload(tuple(bad_header), [full_row()]),
        )
        missing = zip_payload(selected_entries()[:-1])
        unsafe = zip_payload(selected_entries() + [("../escape.csv", b"x")])
        for payload in (zip_payload(entries), missing, unsafe):
            with self.subTest(), self.assertRaises(DailyReportIntegrityError):
                NseDailyBundleParser().parse_bytes(
                    payload,
                    original_filename=BUNDLE_NAME,
                )

    def test_nonofficial_outer_filename_is_rejected_before_persistence(self) -> None:
        with self.assertRaisesRegex(DailyReportIntegrityError, "official NSE"):
            NseDailyBundleParser().parse_bytes(
                complete_bundle(),
                original_filename="access_token=VERYSECRET.zip",
            )


class LocalDailyBundleArtifactStoreTests(unittest.TestCase):
    def write_source(self, root: Path, payload: bytes | None = None) -> Path:
        source = root / BUNDLE_NAME
        source.write_bytes(payload or complete_bundle())
        return source

    def test_import_is_byte_exact_collection_only_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = self.write_source(root)
            store = LocalDailyBundleArtifactStore(
                root / "archive",
                clock=lambda: FIRST_SEEN,
            )

            first = store.import_bundle(source)
            second = store.import_bundle(source)
            loaded = store.get(first.manifest.artifact_id)

            self.assertEqual(first.path, second.path)
            self.assertEqual(loaded.raw_bytes, source.read_bytes())
            self.assertEqual(loaded.manifest.readiness, ReferenceReadiness.COLLECTION_ONLY)
            self.assertFalse(loaded.manifest.actionable)
            self.assertEqual(loaded.manifest.outer_entry_count, 9)
            self.assertEqual(loaded.manifest.selected_report_count, 7)
            self.assertEqual(loaded.manifest.quarantined_report_count, 1)
            self.assertEqual(loaded.manifest.ignored_entry_count, 1)
            self.assertEqual(
                sorted(path.name for path in loaded.path.iterdir()),
                ["bundle.zip", "manifest.json", "normalized.json"],
            )

    def test_released_advisory_lock_file_does_not_block_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = self.write_source(root)
            store = LocalDailyBundleArtifactStore(
                root / "archive",
                clock=lambda: FIRST_SEEN,
            )
            first = store.import_bundle(source)
            lock_files = list((root / "archive").glob("*/.locks/*"))
            self.assertEqual(len(lock_files), 1)
            shutil.rmtree(first.path)

            recovered = store.import_bundle(source)

            self.assertTrue(recovered.path.is_dir())
            self.assertEqual(recovered.raw_bytes, source.read_bytes())

    def test_same_metadata_path_swap_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = self.write_source(root)
            original = os.lstat(source)
            replacement = root / "replacement.zip"
            replacement.write_bytes(source.read_bytes())
            real_open = os.open
            swapped = False

            def swap_before_open(path, flags, mode=0o777, *, dir_fd=None):
                nonlocal swapped
                if Path(path) == source and not swapped:
                    swapped = True
                    os.replace(replacement, source)
                    os.utime(
                        source,
                        ns=(original.st_atime_ns, original.st_mtime_ns),
                    )
                if dir_fd is None:
                    return real_open(path, flags, mode)
                return real_open(path, flags, mode, dir_fd=dir_fd)

            with patch("india_swing._filesystem.os.open", side_effect=swap_before_open):
                with self.assertRaisesRegex(DailyReportIntegrityError, "changed"):
                    LocalDailyBundleArtifactStore(root / "archive").import_bundle(
                        source
                    )

    def test_tampering_and_failed_atomic_publish_are_detected(self) -> None:
        for filename, payload in (
            ("bundle.zip", b"tampered"),
            ("normalized.json", b"{}"),
            ("manifest.json", b"{}"),
        ):
            with self.subTest(filename=filename), tempfile.TemporaryDirectory() as temp_dir:
                root = Path(temp_dir)
                source = self.write_source(root)
                store = LocalDailyBundleArtifactStore(
                    root / "archive",
                    clock=lambda: FIRST_SEEN,
                )
                stored = store.import_bundle(source)
                (stored.path / filename).write_bytes(payload)
                with self.assertRaises(DailyReportIntegrityError):
                    store.get(stored.manifest.artifact_id)

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = self.write_source(root)
            store = LocalDailyBundleArtifactStore(
                root / "archive",
                clock=clock_sequence(FIRST_SEEN, VALIDATED),
            )
            with patch(
                "india_swing.daily_reports.artifact_store.os.rename",
                side_effect=OSError("publish failed"),
            ):
                with self.assertRaisesRegex(OSError, "publish failed"):
                    store.import_bundle(source)
            self.assertEqual(
                [
                    path
                    for path in (root / "archive").rglob("*")
                    if path.is_file() and ".locks" not in path.parts
                ],
                [],
            )

    def test_symbolic_link_input_is_rejected_when_supported(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = self.write_source(root)
            link = root / "linked.zip"
            try:
                link.symlink_to(source)
            except OSError:
                self.skipTest("symbolic links are unavailable in this environment")
            with self.assertRaises(DailyReportIntegrityError):
                LocalDailyBundleArtifactStore(root / "archive").import_bundle(link)


class DailyReportsCliTests(unittest.TestCase):
    def test_cli_imports_collection_only_and_sanitizes_failures(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / BUNDLE_NAME
            source.write_bytes(complete_bundle())
            stdout = io.StringIO()
            with patch.dict(
                os.environ,
                {"INDIA_SWING_DAILY_REPORTS_ROOT": str(root / "archive")},
                clear=True,
            ), patch("sys.stdout", stdout):
                exit_code = daily_reports_main(
                    ["bundle", "import", "--file", str(source)]
                )
            response = json.loads(stdout.getvalue())
            self.assertEqual(exit_code, 0)
            self.assertEqual(response["readiness"], "COLLECTION_ONLY")
            self.assertFalse(response["actionable"])
            self.assertEqual(response["selected_report_count"], 7)
            self.assertEqual(response["quarantined_report_count"], 1)

        stderr = io.StringIO()
        with patch("sys.stderr", stderr):
            exit_code = daily_reports_main(
                ["bundle", "import", "--file", "access_token=secret.zip"]
            )
        self.assertEqual(exit_code, 2)
        self.assertNotIn("secret", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
