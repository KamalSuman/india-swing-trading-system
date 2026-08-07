from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
import zipfile
from pathlib import Path

_SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "stage_nse_historical_year.ps1"
)

_HEADER = (
    "SYMBOL,SERIES, DATE1, PREV_CLOSE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE,"
    " LAST_PRICE, CLOSE_PRICE, AVG_PRICE, TTL_TRD_QNTY, TURNOVER_LACS,"
    " NO_OF_TRADES, DELIV_QTY, DELIV_PER"
)

_MONTH_ABBR = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}


def _full_bhavcopy_csv(date_texts: list[str], *, header: str = _HEADER) -> bytes:
    lines = [header]
    for index, date_text in enumerate(date_texts):
        lines.append(
            f"SYM{index},EQ, {date_text}, 100.00, 100.00, 101.00, 99.00,"
            f" 100.50, 100.50, 100.25, 1000, 10.00, 10, 500, 50.00"
        )
    return ("\r\n".join(lines) + "\r\n").encode("utf-8")


def _write_one_file_zip(
    download_directory: Path,
    *,
    day: int,
    month: int,
    year: int,
    csv_bytes: bytes,
) -> Path:
    outer_name = f"Reports-Archives-Multiple-{day:02d}{month:02d}{year:04d}.zip"
    entry_name = f"sec_bhavdata_full_{day:02d}{month:02d}{year:04d}.csv"
    destination = download_directory / outer_name
    with zipfile.ZipFile(destination, "w") as archive:
        archive.writestr(entry_name, csv_bytes)
    return destination


def _one_field_variant_csv(series_field: str, date_text: str) -> bytes:
    row = (
        f"SYM0,{series_field}, {date_text}, 100.00, 100.00, 101.00, 99.00,"
        f" 100.50, 100.50, 100.25, 1000, 10.00, 10, 500, 50.00"
    )
    return (_HEADER + "\r\n" + row + "\r\n").encode("utf-8")


def _write_two_file_zip(
    download_directory: Path, *, day: int, month: int, year: int
) -> Path:
    outer_name = f"Reports-Archives-Multiple-{day:02d}{month:02d}{year:04d}.zip"
    full_name = f"sec_bhavdata_full_{day:02d}{month:02d}{year:04d}.csv"
    udiff_name = (
        f"BhavCopy_NSE_CM_0_0_0_{year:04d}{month:02d}{day:02d}_F_0000.csv.zip"
    )
    destination = download_directory / outer_name
    date_text = f"{day:02d}-{_MONTH_ABBR[month]}-{year:04d}"
    with zipfile.ZipFile(destination, "w") as archive:
        archive.writestr(full_name, _full_bhavcopy_csv([date_text]))
        archive.writestr(udiff_name, b"udiff-placeholder-bytes")
    return destination


def _run_stage_script(
    *, year: int, download_directory: Path, data_root: Path
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "powershell.exe",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(_SCRIPT_PATH),
            "-Year",
            str(year),
            "-DownloadDirectory",
            str(download_directory),
            "-DataRoot",
            str(data_root),
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )


def _stage_result(result: subprocess.CompletedProcess[str]) -> dict:
    return json.loads(result.stdout)


class StageNseHistoricalYearTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        root = Path(self._tmp.name)
        self.downloads = root / "downloads"
        self.data_root = root / "data"
        self.downloads.mkdir()
        self.data_root.mkdir()

    def _assert_no_session_paths(self, year: int, month: int, day: int) -> None:
        session = f"{year:04d}-{month:02d}-{day:02d}"
        self.assertFalse((self.data_root / "source-archives" / session).exists())
        self.assertFalse((self.data_root / "staging" / session).exists())

    def test_valid_one_file_session_stages_single_session(self) -> None:
        date_text = "15-Jul-2022"
        _write_one_file_zip(
            self.downloads,
            day=15,
            month=7,
            year=2022,
            csv_bytes=_full_bhavcopy_csv([date_text, date_text]),
        )

        result = _run_stage_script(
            year=2022, download_directory=self.downloads, data_root=self.data_root
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        payload = _stage_result(result)
        self.assertEqual(payload["validated_session_count"], 1)
        self.assertEqual(payload["quarantined_wrapper_count"], 0)

        session_dir = self.data_root / "source-archives" / "2022-07-15"
        staging_dir = self.data_root / "staging" / "2022-07-15"
        self.assertTrue(
            (session_dir / "Reports-Archives-Multiple-15072022.zip").is_file()
        )
        self.assertTrue((staging_dir / "sec_bhavdata_full_15072022.csv").is_file())

    def test_stale_wrapper_is_quarantined_only_and_retry_is_idempotent(self) -> None:
        # Mirrors the real 2022-01-26 holiday wrapper that reported 25-Jan.
        _write_one_file_zip(
            self.downloads,
            day=26,
            month=1,
            year=2022,
            csv_bytes=_full_bhavcopy_csv(["25-Jan-2022"]),
        )

        first = _run_stage_script(
            year=2022, download_directory=self.downloads, data_root=self.data_root
        )
        self.assertEqual(first.returncode, 0, first.stderr)
        first_payload = _stage_result(first)
        self.assertEqual(first_payload["quarantined_wrapper_count"], 1)
        self.assertEqual(first_payload["validated_session_count"], 0)

        quarantine_dir = (
            self.data_root / "quarantine" / "nse-historical-archive" / "2022-01-26"
        )
        self._assert_no_session_paths(2022, 1, 26)
        quarantined_files = list(quarantine_dir.iterdir())
        self.assertEqual(len(quarantined_files), 1)
        quarantined_bytes = quarantined_files[0].read_bytes()

        second = _run_stage_script(
            year=2022, download_directory=self.downloads, data_root=self.data_root
        )
        self.assertEqual(second.returncode, 0, second.stderr)
        second_payload = _stage_result(second)
        self.assertEqual(second_payload["quarantined_wrapper_count"], 1)
        self._assert_no_session_paths(2022, 1, 26)
        quarantined_files_after = list(quarantine_dir.iterdir())
        self.assertEqual(len(quarantined_files_after), 1)
        self.assertEqual(quarantined_files_after[0].read_bytes(), quarantined_bytes)

    def test_mixed_dates_fail_closed(self) -> None:
        _write_one_file_zip(
            self.downloads,
            day=10,
            month=3,
            year=2022,
            csv_bytes=_full_bhavcopy_csv(["10-Mar-2022", "09-Mar-2022"]),
        )
        result = _run_stage_script(
            year=2022, download_directory=self.downloads, data_root=self.data_root
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = _stage_result(result)
        self.assertEqual(payload["quarantined_wrapper_count"], 1)
        self.assertEqual(payload["validated_session_count"], 0)
        self._assert_no_session_paths(2022, 3, 10)

    def test_missing_date1_header_fails_closed(self) -> None:
        header = _HEADER.replace("DATE1", "TRADE_DATE")
        _write_one_file_zip(
            self.downloads,
            day=11,
            month=3,
            year=2022,
            csv_bytes=_full_bhavcopy_csv(["11-Mar-2022"], header=header),
        )
        result = _run_stage_script(
            year=2022, download_directory=self.downloads, data_root=self.data_root
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = _stage_result(result)
        self.assertEqual(payload["quarantined_wrapper_count"], 1)
        self._assert_no_session_paths(2022, 3, 11)

    def test_duplicate_date1_header_fails_closed(self) -> None:
        header = _HEADER + ", DATE1"
        csv_bytes = (
            header + "\r\nSYM0,EQ, 12-Mar-2022, 100.00, 100.00, 101.00, 99.00,"
            " 100.50, 100.50, 100.25, 1000, 10.00, 10, 500, 50.00, 12-Mar-2022\r\n"
        ).encode("utf-8")
        _write_one_file_zip(
            self.downloads, day=12, month=3, year=2022, csv_bytes=csv_bytes
        )
        result = _run_stage_script(
            year=2022, download_directory=self.downloads, data_root=self.data_root
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = _stage_result(result)
        self.assertEqual(payload["quarantined_wrapper_count"], 1)
        self._assert_no_session_paths(2022, 3, 12)

    def test_malformed_row_width_fails_closed(self) -> None:
        csv_bytes = (_HEADER + "\r\nSYM0,EQ, 13-Mar-2022, 100.00\r\n").encode("utf-8")
        _write_one_file_zip(
            self.downloads, day=13, month=3, year=2022, csv_bytes=csv_bytes
        )
        result = _run_stage_script(
            year=2022, download_directory=self.downloads, data_root=self.data_root
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = _stage_result(result)
        self.assertEqual(payload["quarantined_wrapper_count"], 1)
        self._assert_no_session_paths(2022, 3, 13)

    def test_malformed_quoting_fails_closed(self) -> None:
        csv_bytes = (
            _HEADER + '\r\nSYM0,EQ, 14-Mar-2022, "unterminated,100.00,101.00,99.00,'
            "100.50,100.50,100.25,1000,10.00,10,500,50.00\r\n"
        ).encode("utf-8")
        _write_one_file_zip(
            self.downloads, day=14, month=3, year=2022, csv_bytes=csv_bytes
        )
        result = _run_stage_script(
            year=2022, download_directory=self.downloads, data_root=self.data_root
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = _stage_result(result)
        self.assertEqual(payload["quarantined_wrapper_count"], 1)
        self._assert_no_session_paths(2022, 3, 14)

    def test_invalid_date_text_fails_closed(self) -> None:
        _write_one_file_zip(
            self.downloads,
            day=15,
            month=3,
            year=2022,
            csv_bytes=_full_bhavcopy_csv(["2022-03-15"]),
        )
        result = _run_stage_script(
            year=2022, download_directory=self.downloads, data_root=self.data_root
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = _stage_result(result)
        self.assertEqual(payload["quarantined_wrapper_count"], 1)
        self._assert_no_session_paths(2022, 3, 15)

    def test_empty_data_fails_closed(self) -> None:
        csv_bytes = (_HEADER + "\r\n").encode("utf-8")
        _write_one_file_zip(
            self.downloads, day=16, month=3, year=2022, csv_bytes=csv_bytes
        )
        result = _run_stage_script(
            year=2022, download_directory=self.downloads, data_root=self.data_root
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = _stage_result(result)
        self.assertEqual(payload["quarantined_wrapper_count"], 1)
        self._assert_no_session_paths(2022, 3, 16)

    def test_ordinary_character_after_closing_quote_fails_closed(self) -> None:
        # Mirrors SYM,"EQ"x,... -- content trailing a closing quote.
        csv_bytes = _one_field_variant_csv('"EQ"x', "20-Mar-2022")
        _write_one_file_zip(
            self.downloads, day=20, month=3, year=2022, csv_bytes=csv_bytes
        )
        result = _run_stage_script(
            year=2022, download_directory=self.downloads, data_root=self.data_root
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = _stage_result(result)
        self.assertEqual(payload["quarantined_wrapper_count"], 1)
        self._assert_no_session_paths(2022, 3, 20)

    def test_whitespace_after_closing_quote_fails_closed(self) -> None:
        csv_bytes = _one_field_variant_csv('"EQ" ', "21-Mar-2022")
        _write_one_file_zip(
            self.downloads, day=21, month=3, year=2022, csv_bytes=csv_bytes
        )
        result = _run_stage_script(
            year=2022, download_directory=self.downloads, data_root=self.data_root
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = _stage_result(result)
        self.assertEqual(payload["quarantined_wrapper_count"], 1)
        self._assert_no_session_paths(2022, 3, 21)

    def test_invalid_utf8_entry_fails_closed_without_crashing_year(self) -> None:
        header_bytes = (_HEADER + "\r\n").encode("utf-8")
        row_bytes = (
            b"\xffSYM0,EQ, 23-Mar-2022, 100.00, 100.00, 101.00, 99.00,"
            b" 100.50, 100.50, 100.25, 1000, 10.00, 10, 500, 50.00\r\n"
        )
        _write_one_file_zip(
            self.downloads,
            day=23,
            month=3,
            year=2022,
            csv_bytes=header_bytes + row_bytes,
        )
        result = _run_stage_script(
            year=2022, download_directory=self.downloads, data_root=self.data_root
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = _stage_result(result)
        self.assertEqual(payload["quarantined_wrapper_count"], 1)
        self._assert_no_session_paths(2022, 3, 23)

    def test_empty_entry_fails_closed(self) -> None:
        _write_one_file_zip(
            self.downloads, day=24, month=3, year=2022, csv_bytes=b""
        )
        result = _run_stage_script(
            year=2022, download_directory=self.downloads, data_root=self.data_root
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = _stage_result(result)
        self.assertEqual(payload["quarantined_wrapper_count"], 1)
        self._assert_no_session_paths(2022, 3, 24)

    def test_oversized_entry_fails_closed_without_reading_content(self) -> None:
        oversized_bytes = b"A" * (64 * 1024 * 1024 + 1024)
        outer_name = "Reports-Archives-Multiple-25032022.zip"
        entry_name = "sec_bhavdata_full_25032022.csv"
        destination = self.downloads / outer_name
        with zipfile.ZipFile(destination, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr(entry_name, oversized_bytes)

        result = _run_stage_script(
            year=2022, download_directory=self.downloads, data_root=self.data_root
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = _stage_result(result)
        self.assertEqual(payload["quarantined_wrapper_count"], 1)
        self._assert_no_session_paths(2022, 3, 25)

    def test_valid_doubled_quotes_in_non_date_field_stages(self) -> None:
        csv_bytes = _one_field_variant_csv('"E""Q"', "26-Mar-2022")
        _write_one_file_zip(
            self.downloads, day=26, month=3, year=2022, csv_bytes=csv_bytes
        )
        result = _run_stage_script(
            year=2022, download_directory=self.downloads, data_root=self.data_root
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = _stage_result(result)
        self.assertEqual(payload["validated_session_count"], 1)
        self.assertEqual(payload["quarantined_wrapper_count"], 0)

    def test_valid_quoted_comma_in_non_date_field_stages(self) -> None:
        csv_bytes = _one_field_variant_csv('"E,Q"', "27-Mar-2022")
        _write_one_file_zip(
            self.downloads, day=27, month=3, year=2022, csv_bytes=csv_bytes
        )
        result = _run_stage_script(
            year=2022, download_directory=self.downloads, data_root=self.data_root
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = _stage_result(result)
        self.assertEqual(payload["validated_session_count"], 1)
        self.assertEqual(payload["quarantined_wrapper_count"], 0)

    def test_valid_embedded_quoted_newline_in_non_date_field_stages(self) -> None:
        csv_bytes = _one_field_variant_csv('"E\r\nQ"', "28-Mar-2022")
        _write_one_file_zip(
            self.downloads, day=28, month=3, year=2022, csv_bytes=csv_bytes
        )
        result = _run_stage_script(
            year=2022, download_directory=self.downloads, data_root=self.data_root
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = _stage_result(result)
        self.assertEqual(payload["validated_session_count"], 1)
        self.assertEqual(payload["quarantined_wrapper_count"], 0)

    def test_conflicting_quarantine_target_remains_unchanged_and_fails(self) -> None:
        _write_one_file_zip(
            self.downloads,
            day=17,
            month=3,
            year=2022,
            csv_bytes=_full_bhavcopy_csv(["16-Mar-2022"]),
        )
        quarantine_dir = (
            self.data_root / "quarantine" / "nse-historical-archive" / "2022-03-17"
        )
        quarantine_dir.mkdir(parents=True)
        conflict_path = (
            quarantine_dir
            / "Reports-Archives-Multiple-17032022-unsupported-or-stale-wrapper.zip"
        )
        conflicting_bytes = b"pre-existing-conflicting-bytes"
        conflict_path.write_bytes(conflicting_bytes)

        result = _run_stage_script(
            year=2022, download_directory=self.downloads, data_root=self.data_root
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(conflict_path.read_bytes(), conflicting_bytes)
        self._assert_no_session_paths(2022, 3, 17)

    def test_existing_two_file_profile_stages_unchanged(self) -> None:
        _write_two_file_zip(self.downloads, day=18, month=3, year=2022)

        result = _run_stage_script(
            year=2022, download_directory=self.downloads, data_root=self.data_root
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = _stage_result(result)
        self.assertEqual(payload["validated_session_count"], 1)
        self.assertEqual(payload["quarantined_wrapper_count"], 0)

        session_dir = self.data_root / "source-archives" / "2022-03-18"
        staging_dir = self.data_root / "staging" / "2022-03-18"
        self.assertTrue(
            (session_dir / "Reports-Archives-Multiple-18032022.zip").is_file()
        )
        self.assertTrue((staging_dir / "sec_bhavdata_full_18032022.csv").is_file())
        self.assertTrue(
            (staging_dir / "BhavCopy_NSE_CM_0_0_0_20220318_F_0000.csv.zip").is_file()
        )


if __name__ == "__main__":
    unittest.main()
