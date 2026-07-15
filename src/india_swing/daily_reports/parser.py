from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import zipfile
import zlib
from collections import Counter
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import PurePath

from india_swing.reference_data.security_master import (
    NSE_CM_MII_SECURITY_HEADER,
    NseCmSecurityMasterParser,
)
from india_swing.reference_data.models import validated_isin_or_none

from .models import (
    BundleEntryDisposition,
    BundleEntryInventory,
    DailyReportFamily,
    DailyReportIntegrityError,
    ParsedDailyReport,
    ParsedNseDailyBundle,
    ReportDateRole,
    ReportDateStatus,
)


UDIFF_BHAVCOPY_HEADER = (
    "TradDt",
    "BizDt",
    "Sgmt",
    "Src",
    "FinInstrmTp",
    "FinInstrmId",
    "ISIN",
    "TckrSymb",
    "SctySrs",
    "XpryDt",
    "FininstrmActlXpryDt",
    "StrkPric",
    "OptnTp",
    "FinInstrmNm",
    "OpnPric",
    "HghPric",
    "LwPric",
    "ClsPric",
    "LastPric",
    "PrvsClsgPric",
    "UndrlygPric",
    "SttlmPric",
    "OpnIntrst",
    "ChngInOpnIntrst",
    "TtlTradgVol",
    "TtlTrfVal",
    "TtlNbOfTxsExctd",
    "SsnId",
    "NewBrdLotQty",
    "Rmks",
    "Rsvd1",
    "Rsvd2",
    "Rsvd3",
    "Rsvd4",
)

FULL_BHAVCOPY_DELIVERY_HEADER = (
    "SYMBOL",
    " SERIES",
    " DATE1",
    " PREV_CLOSE",
    " OPEN_PRICE",
    " HIGH_PRICE",
    " LOW_PRICE",
    " LAST_PRICE",
    " CLOSE_PRICE",
    " AVG_PRICE",
    " TTL_TRD_QNTY",
    " TURNOVER_LACS",
    " NO_OF_TRADES",
    " DELIV_QTY",
    " DELIV_PER",
)

REG1_SURVEILLANCE_HEADER = (
    "ScripCode",
    "Symbol",
    "Nse Exclusive",
    "Status",
    "Series",
    "GSM",
    "Long_Term_Additional_Surveillance_Measure (Long Term ASM)",
    "Unsolicited_SMS",
    "Insolvency_Resolution_Process(IRP)",
    "Short_Term_Additional_Surveillance_Measure (Short Term ASM)",
    "Default",
    "ICA",
    "Filler4",
    "Filler5",
    "Pledge",
    "Add-on_PB",
    "Total Pledge",
    "Social Media Platforms",
    "ESM",
    "Loss making",
    "The Overall encumbered share in the scrip is more than 50 Percent.",
    "Under BZ/SZ Series",
    "Company has failed to pay Annual listing fee",
    "Filler12",
    "Derivative contracts in the scrip to be moved out of F and O",
    "Scrip PE is greater than 50 (4 trailing quarters)",
    "EPS in the scrip is zero (4 trailing quarters)",
    "Less than 100 unique PAN traded in previous 30 days",
    "Mandatory Market making period in SME scrip is over",
    "SME scrip is not regularly traded",
    "Close to Close price movement greater than 25perc in previous 5 trading days",
    "Close to Close price movement greater than 40perc in previous 15 trading days",
    "Close to Close price movement greater than 100perc in previous 60 trading Days",
    "Close to Close price movement greater than 25perc in previous 15 Days",
    "Close to Close price movement greater than 50perc in previous 1 month",
    "Close to Close price movement greater than 90perc in previous 3 months",
    "Close to Close price movement greater than 25perc in previous 1 month",
    "Close to Close price movement greater than 50perc in previous 3 months",
    "Close to Close price movement greater than 200perc in previous 365 Days",
    "Close to Close price movement greater than 75perc in previous 6 months",
    "Close to Close price movement greater than 100perc in previous 365 days",
    "High low price variation greater than 75perc in previous 1 month",
    "High low price variation greater than 150perc in previous 3 months",
    "High low price variation greater than 75perc in previous 3 months",
    "High low price variation greater than 300perc in previous 365 Days",
    "High low price variation greater than 100perc in previous 6 months",
    "High low price variation greater than 200perc in previous 365 Days",
    "High low price variation greater than 150perc in previous 12 months",
    "Filler17",
    "Filler18",
    "Filler19",
    "Filler20",
    "Filler21",
    "Filler22",
    "Filler23",
    "Filler24",
    "Filler25",
    "Filler26",
    "Filler27",
    "Filler28",
    "Filler29",
    "Filler30",
    "Filler31",
)

COMPLETE_PRICE_BANDS_HEADER = (
    "Symbol",
    "Series",
    "Security Name",
    "Band",
    "Remarks",
)

SME_PRICE_BANDS_HEADER = (
    "Symbol",
    "Series",
    "Name",
    "Band",
    "Remarks",
)

PRICE_BAND_CHANGES_HEADER = (
    "Sr. No",
    "Symbol",
    "Series",
    "Security Name",
    "From",
    "To",
)

SERIES_CHANGES_HEADER = (
    "Symbol",
    "Security",
    "From Series",
    "To Series",
    "Change Date",
    "Remarks",
)

_UDIFF_NAME = re.compile(
    r"BhavCopy_NSE_CM_0_0_0_(\d{8})_F_0000\.csv\.zip\Z"
)
_FULL_NAME = re.compile(r"sec_bhavdata_full_(\d{8})\.csv\Z")
_REG1_NAME = re.compile(r"REG1_IND(\d{6})\.csv\Z")
_MAIN_BANDS_NAME = re.compile(r"sec_list_(\d{8})\.csv\Z")
_SME_BANDS_NAME = re.compile(r"sme_bands_complete_(\d{8})\.csv\Z")
_BAND_CHANGES_NAME = re.compile(r"eq_band_changes_(\d{8})\.csv\Z")
_SECURITY_MASTER_NAME = re.compile(r"NSE_CM_security_(\d{8})\.csv\.gz\Z")
_INTEGER = re.compile(r"(?:0|[1-9][0-9]*)\Z")
_DECIMAL = re.compile(r"(?:0|[1-9][0-9]*)(?:\.[0-9]+)?\Z")
_SYMBOL = re.compile(r"[A-Z0-9&-]{1,25}\Z")
_SERIES = re.compile(r"[A-Z0-9]{1,3}\Z")
_RAW_IDENTIFIER = re.compile(r"[A-Z0-9]{1,12}\Z")
_TEXT_DATE = re.compile(r"(\d{2})-([A-Za-z]{3})-(\d{4})\Z")
_BANDS = {"2", "5", "10", "20", "40", "No Band"}
_MONTHS = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}

_REG1_INDICATOR_DOMAINS = {
    "GSM": {"0", "1", "2", "3", "4", "6", "100"},
    "Long_Term_Additional_Surveillance_Measure (Long Term ASM)": {
        "1",
        "2",
        "3",
        "4",
        "100",
    },
    "Short_Term_Additional_Surveillance_Measure (Short Term ASM)": {
        "1",
        "2",
        "100",
    },
    "ESM": {"1", "2", "100"},
    "Unsolicited_SMS": {"0", "1", "100"},
    "Insolvency_Resolution_Process(IRP)": {"0", "1", "2", "100"},
    "Default": {"0", "1", "100"},
    "ICA": {"0", "1", "100"},
    "Pledge": {"1", "100"},
    "Add-on_PB": {"1", "100"},
    "Total Pledge": {"1", "100"},
    "Social Media Platforms": {"2", "100"},
}

_REQUIRED_SELECTED_FAMILIES = {
    DailyReportFamily.UDIFF_BHAVCOPY,
    DailyReportFamily.FULL_BHAVCOPY_DELIVERY,
    DailyReportFamily.SURVEILLANCE_REG1,
    DailyReportFamily.COMPLETE_PRICE_BANDS,
    DailyReportFamily.SME_PRICE_BANDS,
    DailyReportFamily.PRICE_BAND_CHANGES,
    DailyReportFamily.SERIES_CHANGES,
}

NSE_DAILY_BUNDLE_FILENAME = "Reports-Daily-Multiple.zip"
_CORE_FULL_BHAVCOPY_SERIES = frozenset({"EQ"})


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _header_sha256(header: tuple[str, ...]) -> str:
    return _sha256(",".join(header).encode("utf-8"))


def _canonical_row_sha256(row: tuple[str, ...]) -> str:
    return _sha256(
        json.dumps(
            row,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
    )


def _ordered_row_digest(rows: tuple[tuple[str, ...], ...]) -> str:
    return _sha256(
        "\n".join(_canonical_row_sha256(row) for row in rows).encode("ascii")
    )


def _safe_basename(value: str) -> bool:
    return (
        bool(value)
        and PurePath(value).name == value
        and "/" not in value
        and "\\" not in value
        and "\x00" not in value
    )


def _bounded_gzip_decompress(payload: bytes, maximum_output_bytes: int) -> bytes:
    if not payload.startswith(b"\x1f\x8b\x08"):
        raise DailyReportIntegrityError("security-master entry is not gzip data")
    decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
    output = bytearray()
    position = 0
    try:
        while position < len(payload):
            chunk = payload[position : position + 65_536]
            position += len(chunk)
            while chunk:
                remaining = maximum_output_bytes + 1 - len(output)
                if remaining <= 0:
                    raise DailyReportIntegrityError(
                        "security-master entry exceeds the expanded-size limit"
                    )
                output.extend(decompressor.decompress(chunk, remaining))
                if len(output) > maximum_output_bytes:
                    raise DailyReportIntegrityError(
                        "security-master entry exceeds the expanded-size limit"
                    )
                chunk = decompressor.unconsumed_tail
            if decompressor.eof:
                if decompressor.unused_data or position != len(payload):
                    raise DailyReportIntegrityError(
                        "security-master entry has concatenated or trailing gzip data"
                    )
                break
        output.extend(decompressor.flush(max(maximum_output_bytes + 1 - len(output), 0)))
    except zlib.error as exc:
        raise DailyReportIntegrityError("security-master entry has invalid gzip data") from exc
    if not decompressor.eof:
        raise DailyReportIntegrityError("security-master entry has truncated gzip data")
    if len(output) > maximum_output_bytes:
        raise DailyReportIntegrityError(
            "security-master entry exceeds the expanded-size limit"
        )
    return bytes(output)


def _date_from_digits(value: str, date_format: str, field_name: str) -> date:
    try:
        return datetime.strptime(value, date_format).date()
    except ValueError as exc:
        raise DailyReportIntegrityError(f"{field_name} contains an invalid date") from exc


def _nse_text_date(value: str, field_name: str) -> date:
    match = _TEXT_DATE.fullmatch(value)
    if match is None or match.group(2).upper() not in _MONTHS:
        raise DailyReportIntegrityError(f"{field_name} contains an invalid date")
    try:
        return date(
            int(match.group(3)),
            _MONTHS[match.group(2).upper()],
            int(match.group(1)),
        )
    except ValueError as exc:
        raise DailyReportIntegrityError(f"{field_name} contains an invalid date") from exc


def _unsigned_integer(
    value: str,
    field_name: str,
    *,
    minimum: int = 0,
    maximum: int = 10**18,
) -> int:
    if _INTEGER.fullmatch(value) is None:
        raise DailyReportIntegrityError(f"{field_name} is not an unsigned integer")
    parsed = int(value)
    if not minimum <= parsed <= maximum:
        raise DailyReportIntegrityError(f"{field_name} is outside its supported range")
    return parsed


def _decimal(
    value: str,
    field_name: str,
    *,
    minimum: Decimal = Decimal("0"),
    maximum: Decimal = Decimal("100000000000000000000"),
) -> Decimal:
    if _DECIMAL.fullmatch(value) is None:
        raise DailyReportIntegrityError(f"{field_name} is not a canonical decimal")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise DailyReportIntegrityError(f"{field_name} is not a decimal") from exc
    if not parsed.is_finite() or not minimum <= parsed <= maximum:
        raise DailyReportIntegrityError(f"{field_name} is outside its supported range")
    return parsed


def _field_map(header: tuple[str, ...], row: tuple[str, ...]) -> dict[str, str]:
    return dict(zip(header, row, strict=True))


class NseDailyBundleParser:
    """Strict collection-only parser for NSE's multiple-report download ZIP."""

    def __init__(
        self,
        *,
        maximum_bundle_bytes: int = 128 * 1024 * 1024,
        maximum_entry_bytes: int = 64 * 1024 * 1024,
        maximum_total_entry_bytes: int = 512 * 1024 * 1024,
        maximum_entries: int = 256,
        maximum_rows_per_report: int = 250_000,
        maximum_field_characters: int = 8_192,
        maximum_compression_ratio: int = 250,
    ) -> None:
        limits = (
            maximum_bundle_bytes,
            maximum_entry_bytes,
            maximum_total_entry_bytes,
            maximum_entries,
            maximum_rows_per_report,
            maximum_field_characters,
            maximum_compression_ratio,
        )
        if any(type(value) is not int or value <= 0 for value in limits):
            raise ValueError("daily-bundle limits must be positive integers")
        self.maximum_bundle_bytes = maximum_bundle_bytes
        self.maximum_entry_bytes = maximum_entry_bytes
        self.maximum_total_entry_bytes = maximum_total_entry_bytes
        self.maximum_entries = maximum_entries
        self.maximum_rows_per_report = maximum_rows_per_report
        self.maximum_field_characters = maximum_field_characters
        self.maximum_compression_ratio = maximum_compression_ratio
        self.security_master_parser = NseCmSecurityMasterParser()

    def parse_bytes(
        self,
        payload: bytes,
        *,
        original_filename: str,
    ) -> ParsedNseDailyBundle:
        if not isinstance(payload, bytes):
            raise TypeError("daily bundle payload must be bytes")
        if not payload:
            raise DailyReportIntegrityError("daily bundle is empty")
        if len(payload) > self.maximum_bundle_bytes:
            raise DailyReportIntegrityError("daily bundle exceeds the size limit")
        if original_filename != NSE_DAILY_BUNDLE_FILENAME:
            raise DailyReportIntegrityError(
                "daily bundle filename does not match the official NSE download name"
            )

        inventories: list[BundleEntryInventory] = []
        reports: list[ParsedDailyReport] = []
        try:
            with zipfile.ZipFile(io.BytesIO(payload), mode="r") as archive:
                infos = archive.infolist()
                if not infos:
                    raise DailyReportIntegrityError("daily bundle contains no entries")
                if len(infos) > self.maximum_entries:
                    raise DailyReportIntegrityError("daily bundle exceeds the entry limit")
                names = [info.filename for info in infos]
                if len(set(names)) != len(names) or len({name.casefold() for name in names}) != len(
                    names
                ):
                    raise DailyReportIntegrityError("daily bundle has duplicate entry names")
                total_size = 0
                for info in infos:
                    self._validate_zip_entry_info(info)
                    total_size += info.file_size
                    if total_size > self.maximum_total_entry_bytes:
                        raise DailyReportIntegrityError(
                            "daily bundle exceeds the total expanded-size limit"
                        )

                for info in infos:
                    entry_bytes = archive.read(info)
                    if len(entry_bytes) != info.file_size:
                        raise DailyReportIntegrityError(
                            "daily bundle entry size disagrees with its ZIP metadata"
                        )
                    report = self._approved_report(info.filename, entry_bytes)
                    if report is None:
                        disposition = BundleEntryDisposition.IGNORED_UNAPPROVED
                        family = None
                    else:
                        disposition = report.disposition
                        family = report.family
                        reports.append(report)
                    inventories.append(
                        BundleEntryInventory(
                            name=info.filename,
                            byte_count=info.file_size,
                            compressed_byte_count=info.compress_size,
                            compression_method=info.compress_type,
                            crc32=info.CRC,
                            sha256=_sha256(entry_bytes),
                            disposition=disposition,
                            family=family,
                        )
                    )
        except DailyReportIntegrityError:
            raise
        except (OSError, RuntimeError, zipfile.BadZipFile, zipfile.LargeZipFile) as exc:
            raise DailyReportIntegrityError("daily bundle is not a valid complete ZIP") from exc

        ordered_reports = tuple(sorted(reports, key=lambda item: item.source_entry_name))
        self._validate_bundle_completeness(ordered_reports)
        return ParsedNseDailyBundle(
            original_filename=original_filename,
            raw_sha256=_sha256(payload),
            byte_count=len(payload),
            entries=tuple(sorted(inventories, key=lambda item: item.name)),
            reports=ordered_reports,
        )

    def _validate_zip_entry_info(self, info: zipfile.ZipInfo) -> None:
        if info.is_dir() or not _safe_basename(info.filename):
            raise DailyReportIntegrityError(
                "daily bundle entries must be flat regular-file basenames"
            )
        if info.flag_bits & 0x1:
            raise DailyReportIntegrityError("encrypted daily-bundle entries are forbidden")
        if info.file_size < 0 or info.compress_size < 0:
            raise DailyReportIntegrityError("daily-bundle entry has an invalid size")
        if info.file_size > self.maximum_entry_bytes:
            raise DailyReportIntegrityError("daily-bundle entry exceeds the size limit")
        if info.file_size and info.compress_size == 0:
            raise DailyReportIntegrityError("daily-bundle entry has an invalid compression ratio")
        if (
            info.compress_size
            and info.file_size / info.compress_size > self.maximum_compression_ratio
        ):
            raise DailyReportIntegrityError("daily-bundle entry is a compression bomb")

    def _approved_report(
        self,
        name: str,
        source_entry_bytes: bytes,
    ) -> ParsedDailyReport | None:
        match = _UDIFF_NAME.fullmatch(name)
        if match:
            return self._parse_udiff(name, source_entry_bytes, match.group(1))
        match = _FULL_NAME.fullmatch(name)
        if match:
            return self._parse_full_delivery(name, source_entry_bytes, match.group(1))
        match = _REG1_NAME.fullmatch(name)
        if match:
            return self._parse_reg1(name, source_entry_bytes, match.group(1))
        match = _MAIN_BANDS_NAME.fullmatch(name)
        if match:
            return self._parse_bands(
                name,
                source_entry_bytes,
                match.group(1),
                family=DailyReportFamily.COMPLETE_PRICE_BANDS,
                header=COMPLETE_PRICE_BANDS_HEADER,
                date_role=ReportDateRole.PUBLICATION_DATE_NEXT_SESSION_EFFECTIVE,
            )
        match = _SME_BANDS_NAME.fullmatch(name)
        if match:
            return self._parse_bands(
                name,
                source_entry_bytes,
                match.group(1),
                family=DailyReportFamily.SME_PRICE_BANDS,
                header=SME_PRICE_BANDS_HEADER,
                date_role=ReportDateRole.CLAIMED_EFFECTIVE_DATE,
            )
        match = _BAND_CHANGES_NAME.fullmatch(name)
        if match:
            return self._parse_band_changes(name, source_entry_bytes, match.group(1))
        if name == "series_change.csv":
            return self._parse_series_changes(name, source_entry_bytes)
        match = _SECURITY_MASTER_NAME.fullmatch(name)
        if match:
            return self._parse_security_master(name, source_entry_bytes)
        return None

    def _parse_csv(
        self,
        payload: bytes,
        *,
        expected_header: tuple[str, ...],
        report_name: str,
        allow_empty: bool,
    ) -> tuple[tuple[str, ...], tuple[tuple[str, ...], ...]]:
        if len(payload) > self.maximum_entry_bytes:
            raise DailyReportIntegrityError(f"{report_name} exceeds the size limit")
        try:
            text = payload.decode("utf-8-sig", errors="strict")
        except UnicodeDecodeError as exc:
            raise DailyReportIntegrityError(f"{report_name} is not strict UTF-8") from exc
        if "\x00" in text:
            raise DailyReportIntegrityError(f"{report_name} contains a NUL byte")
        reader = csv.reader(io.StringIO(text, newline=""), strict=True)
        try:
            header = tuple(next(reader))
        except (StopIteration, csv.Error) as exc:
            raise DailyReportIntegrityError(f"{report_name} has no readable header") from exc
        if header != expected_header:
            raise DailyReportIntegrityError(f"{report_name} has an unsupported header")
        rows: list[tuple[str, ...]] = []
        try:
            for row_number, raw_row in enumerate(reader, start=2):
                if row_number - 1 > self.maximum_rows_per_report:
                    raise DailyReportIntegrityError(f"{report_name} exceeds the row limit")
                row = tuple(raw_row)
                if len(row) != len(header):
                    raise DailyReportIntegrityError(
                        f"{report_name} row width disagrees with its header"
                    )
                if any(len(value) > self.maximum_field_characters for value in row):
                    raise DailyReportIntegrityError(
                        f"{report_name} contains an oversized field"
                    )
                if any(
                    any(ord(character) < 32 and character != "\t" for character in value)
                    for value in row
                ):
                    raise DailyReportIntegrityError(
                        f"{report_name} contains a control character"
                    )
                rows.append(row)
        except csv.Error as exc:
            raise DailyReportIntegrityError(f"{report_name} contains malformed CSV") from exc
        if not allow_empty and not rows:
            raise DailyReportIntegrityError(f"{report_name} contains no data rows")
        return header, tuple(rows)

    def _parse_udiff(
        self,
        name: str,
        source_entry_bytes: bytes,
        date_digits: str,
    ) -> ParsedDailyReport:
        claimed_date = _date_from_digits(date_digits, "%Y%m%d", "UDiFF filename")
        expected_inner_name = name[:-4]
        try:
            with zipfile.ZipFile(io.BytesIO(source_entry_bytes), mode="r") as archive:
                infos = archive.infolist()
                if len(infos) != 1:
                    raise DailyReportIntegrityError(
                        "UDiFF Bhavcopy ZIP must contain exactly one CSV"
                    )
                info = infos[0]
                self._validate_zip_entry_info(info)
                if info.filename != expected_inner_name:
                    raise DailyReportIntegrityError(
                        "UDiFF inner CSV name disagrees with its container"
                    )
                content_bytes = archive.read(info)
                if len(content_bytes) != info.file_size:
                    raise DailyReportIntegrityError("UDiFF inner CSV size mismatch")
        except DailyReportIntegrityError:
            raise
        except (OSError, RuntimeError, zipfile.BadZipFile, zipfile.LargeZipFile) as exc:
            raise DailyReportIntegrityError("UDiFF Bhavcopy is not a valid ZIP") from exc

        header, rows = self._parse_csv(
            content_bytes,
            expected_header=UDIFF_BHAVCOPY_HEADER,
            report_name=name,
            allow_empty=False,
        )
        seen_instrument_keys: set[tuple[str, str]] = set()
        seen_listing_keys: set[tuple[str, str, str]] = set()
        confirmed_dates: set[date] = set()
        for row in rows:
            values = _field_map(header, row)
            try:
                trade_date = date.fromisoformat(values["TradDt"])
                business_date = date.fromisoformat(values["BizDt"])
            except ValueError as exc:
                raise DailyReportIntegrityError("UDiFF row contains an invalid date") from exc
            if trade_date != claimed_date or business_date != claimed_date:
                raise DailyReportIntegrityError(
                    "UDiFF row dates disagree with the container filename"
                )
            confirmed_dates.add(trade_date)
            for field_name, expected in (
                ("Sgmt", "CM"),
                ("Src", "NSE"),
                ("FinInstrmTp", "STK"),
                ("SsnId", "F1"),
            ):
                if values[field_name] != expected:
                    raise DailyReportIntegrityError(
                        f"UDiFF {field_name} is outside the pinned CM final-session scope"
                    )
            instrument_id = _unsigned_integer(
                values["FinInstrmId"],
                "UDiFF FinInstrmId",
                minimum=1,
            )
            if _SYMBOL.fullmatch(values["TckrSymb"]) is None:
                raise DailyReportIntegrityError("UDiFF ticker symbol is invalid")
            if _SERIES.fullmatch(values["SctySrs"]) is None:
                raise DailyReportIntegrityError("UDiFF security series is invalid")
            if (
                _RAW_IDENTIFIER.fullmatch(values["ISIN"]) is None
                or validated_isin_or_none(values["ISIN"]) is None
            ):
                raise DailyReportIntegrityError("UDiFF ISIN is invalid")
            if not values["FinInstrmNm"]:
                raise DailyReportIntegrityError("UDiFF instrument name is required")
            instrument_key = (str(instrument_id), values["SsnId"])
            listing_key = (
                values["TckrSymb"],
                values["SctySrs"],
                values["SsnId"],
            )
            if instrument_key in seen_instrument_keys:
                raise DailyReportIntegrityError(
                    "UDiFF contains a duplicate session instrument ID"
                )
            if listing_key in seen_listing_keys:
                raise DailyReportIntegrityError(
                    "UDiFF contains a duplicate session symbol-series"
                )
            seen_instrument_keys.add(instrument_key)
            seen_listing_keys.add(listing_key)
            prices = {
                field_name: _decimal(values[field_name], f"UDiFF {field_name}")
                for field_name in (
                    "OpnPric",
                    "HghPric",
                    "LwPric",
                    "ClsPric",
                    "LastPric",
                    "PrvsClsgPric",
                )
            }
            if any(price <= 0 for price in prices.values()):
                raise DailyReportIntegrityError("UDiFF prices must be positive")
            if prices["HghPric"] < max(
                prices["OpnPric"], prices["LwPric"], prices["ClsPric"], prices["LastPric"]
            ) or prices["LwPric"] > min(
                prices["OpnPric"], prices["HghPric"], prices["ClsPric"], prices["LastPric"]
            ):
                raise DailyReportIntegrityError("UDiFF OHLC values are inconsistent")
            traded_volume = _unsigned_integer(
                values["TtlTradgVol"],
                "UDiFF TtlTradgVol",
                minimum=1,
            )
            traded_value = _decimal(values["TtlTrfVal"], "UDiFF TtlTrfVal")
            if traded_value <= 0:
                raise DailyReportIntegrityError("UDiFF traded value must be positive")
            precise_average = traded_value / Decimal(traded_volume)
            if not prices["LwPric"] <= precise_average <= prices["HghPric"]:
                raise DailyReportIntegrityError(
                    "UDiFF traded value implies an average outside its daily range"
                )
            _unsigned_integer(
                values["TtlNbOfTxsExctd"],
                "UDiFF TtlNbOfTxsExctd",
                minimum=1,
            )
            _unsigned_integer(values["NewBrdLotQty"], "UDiFF NewBrdLotQty", minimum=1)

        return self._report(
            source_entry_name=name,
            content_name=expected_inner_name,
            family=DailyReportFamily.UDIFF_BHAVCOPY,
            disposition=BundleEntryDisposition.SELECTED_VALIDATED,
            claimed_report_date=claimed_date,
            confirmed_row_dates=tuple(sorted(confirmed_dates)),
            date_status=ReportDateStatus.ROW_CONFIRMED,
            date_role=ReportDateRole.TRADE_DATE,
            source_entry_bytes=source_entry_bytes,
            content_bytes=content_bytes,
            header=header,
            rows=rows,
        )

    def _parse_full_delivery(
        self,
        name: str,
        payload: bytes,
        date_digits: str,
    ) -> ParsedDailyReport:
        claimed_date = _date_from_digits(date_digits, "%d%m%Y", "full Bhavcopy filename")
        header, rows = self._parse_csv(
            payload,
            expected_header=FULL_BHAVCOPY_DELIVERY_HEADER,
            report_name=name,
            allow_empty=False,
        )
        stripped_header = tuple(value.strip() for value in header)
        seen_keys: set[tuple[str, str]] = set()
        confirmed_dates: set[date] = set()
        for row in rows:
            values = _field_map(stripped_header, tuple(value.strip() for value in row))
            row_date = _nse_text_date(values["DATE1"], "full Bhavcopy DATE1")
            if row_date != claimed_date:
                raise DailyReportIntegrityError(
                    "full Bhavcopy row date disagrees with its filename"
                )
            confirmed_dates.add(row_date)
            if _SYMBOL.fullmatch(values["SYMBOL"]) is None:
                raise DailyReportIntegrityError("full Bhavcopy symbol is invalid")
            if _SERIES.fullmatch(values["SERIES"]) is None:
                raise DailyReportIntegrityError("full Bhavcopy series is invalid")
            key = (values["SYMBOL"], values["SERIES"])
            if key in seen_keys:
                raise DailyReportIntegrityError("full Bhavcopy contains a duplicate listing key")
            seen_keys.add(key)
            prices = {
                field_name: _decimal(values[field_name], f"full Bhavcopy {field_name}")
                for field_name in (
                    "PREV_CLOSE",
                    "OPEN_PRICE",
                    "HIGH_PRICE",
                    "LOW_PRICE",
                    "LAST_PRICE",
                    "CLOSE_PRICE",
                    "AVG_PRICE",
                )
            }
            if any(price <= 0 for price in prices.values()):
                raise DailyReportIntegrityError(
                    "full Bhavcopy prices must be positive"
                )
            if prices["HIGH_PRICE"] < max(
                prices["OPEN_PRICE"],
                prices["LOW_PRICE"],
                prices["LAST_PRICE"],
                prices["CLOSE_PRICE"],
            ) or prices["LOW_PRICE"] > min(
                prices["OPEN_PRICE"],
                prices["HIGH_PRICE"],
                prices["LAST_PRICE"],
                prices["CLOSE_PRICE"],
            ):
                raise DailyReportIntegrityError("full Bhavcopy OHLC values are inconsistent")
            traded = _unsigned_integer(
                values["TTL_TRD_QNTY"],
                "full Bhavcopy volume",
                minimum=1,
            )
            _decimal(values["TURNOVER_LACS"], "full Bhavcopy turnover")
            _unsigned_integer(
                values["NO_OF_TRADES"],
                "full Bhavcopy trade count",
                minimum=1,
            )
            if values["DELIV_QTY"] == "-" and values["DELIV_PER"] == "-":
                # NSE reports delivery as not applicable for some series.  A
                # dash is missingness, never a numeric zero.
                pass
            elif "-" in {values["DELIV_QTY"], values["DELIV_PER"]}:
                raise DailyReportIntegrityError(
                    "full Bhavcopy delivery missingness is inconsistent"
                )
            else:
                delivered = _unsigned_integer(
                    values["DELIV_QTY"],
                    "full Bhavcopy delivery",
                )
                delivery_percent = _decimal(
                    values["DELIV_PER"],
                    "full Bhavcopy delivery percent",
                    maximum=Decimal("100"),
                )
                expected_delivery_percent = (
                    (Decimal(100) * Decimal(delivered) / Decimal(traded)).quantize(
                        Decimal("0.01"),
                        rounding=ROUND_HALF_UP,
                    )
                    if traded
                    else Decimal(0)
                )
                if (
                    delivered > traded
                    or delivery_percent != expected_delivery_percent
                ):
                    raise DailyReportIntegrityError(
                        "full Bhavcopy delivery values are inconsistent"
                    )

        return self._report(
            source_entry_name=name,
            content_name=name,
            family=DailyReportFamily.FULL_BHAVCOPY_DELIVERY,
            disposition=BundleEntryDisposition.SELECTED_VALIDATED,
            claimed_report_date=claimed_date,
            confirmed_row_dates=tuple(sorted(confirmed_dates)),
            date_status=ReportDateStatus.ROW_CONFIRMED,
            date_role=ReportDateRole.TRADE_DATE,
            source_entry_bytes=payload,
            content_bytes=payload,
            header=header,
            rows=rows,
        )

    def _parse_reg1(
        self,
        name: str,
        payload: bytes,
        date_digits: str,
    ) -> ParsedDailyReport:
        claimed_date = _date_from_digits(date_digits, "%d%m%y", "REG1 filename")
        header, rows = self._parse_csv(
            payload,
            expected_header=REG1_SURVEILLANCE_HEADER,
            report_name=name,
            allow_empty=False,
        )
        seen_keys: set[tuple[str, str]] = set()
        filler_fields = tuple(value for value in header if value.startswith("Filler"))
        binary_indicator_fields = tuple(
            value
            for value in header[5:]
            if value not in _REG1_INDICATOR_DOMAINS and value not in filler_fields
        )
        for row in rows:
            values = _field_map(header, row)
            if values["ScripCode"] != "NA":
                raise DailyReportIntegrityError(
                    "REG1 ScripCode changed from its pinned non-identity sentinel"
                )
            if _SYMBOL.fullmatch(values["Symbol"]) is None:
                raise DailyReportIntegrityError("REG1 symbol is invalid")
            if _SERIES.fullmatch(values["Series"]) is None:
                raise DailyReportIntegrityError("REG1 series is invalid")
            if values["Nse Exclusive"] not in {"N", "Y"}:
                raise DailyReportIntegrityError("REG1 NSE-exclusive flag is invalid")
            if values["Status"] not in {"A", "I", "S"}:
                raise DailyReportIntegrityError("REG1 status is invalid")
            key = (values["Symbol"], values["Series"])
            if key in seen_keys:
                raise DailyReportIntegrityError("REG1 contains a duplicate symbol-series")
            seen_keys.add(key)
            for field_name, allowed in _REG1_INDICATOR_DOMAINS.items():
                if values[field_name] not in allowed:
                    raise DailyReportIntegrityError(
                        f"REG1 {field_name} has an unknown stage code"
                    )
            for field_name in binary_indicator_fields:
                if values[field_name] not in {"0", "100"}:
                    raise DailyReportIntegrityError(
                        f"REG1 {field_name} has an unknown indicator code"
                    )
            if any(values[field_name] for field_name in filler_fields):
                raise DailyReportIntegrityError("REG1 filler fields must remain blank")

        return self._report(
            source_entry_name=name,
            content_name=name,
            family=DailyReportFamily.SURVEILLANCE_REG1,
            disposition=BundleEntryDisposition.SELECTED_VALIDATED,
            claimed_report_date=claimed_date,
            confirmed_row_dates=(),
            date_status=ReportDateStatus.FILENAME_CLAIM_ONLY,
            date_role=ReportDateRole.PUBLICATION_DATE_NEXT_SESSION_EFFECTIVE,
            source_entry_bytes=payload,
            content_bytes=payload,
            header=header,
            rows=rows,
        )

    def _parse_bands(
        self,
        name: str,
        payload: bytes,
        date_digits: str,
        *,
        family: DailyReportFamily,
        header: tuple[str, ...],
        date_role: ReportDateRole,
    ) -> ParsedDailyReport:
        claimed_date = _date_from_digits(date_digits, "%d%m%Y", "band filename")
        parsed_header, rows = self._parse_csv(
            payload,
            expected_header=header,
            report_name=name,
            allow_empty=False,
        )
        seen_keys: set[tuple[str, str]] = set()
        for row in rows:
            values = _field_map(parsed_header, row)
            if _SYMBOL.fullmatch(values["Symbol"]) is None:
                raise DailyReportIntegrityError("price-band symbol is invalid")
            if _SERIES.fullmatch(values["Series"]) is None:
                raise DailyReportIntegrityError("price-band series is invalid")
            if not values.get("Security Name", values.get("Name", "")):
                raise DailyReportIntegrityError("price-band security name is required")
            if values["Band"] not in _BANDS:
                raise DailyReportIntegrityError("price-band value is outside the pinned domain")
            key = (values["Symbol"], values["Series"])
            if key in seen_keys:
                raise DailyReportIntegrityError("price-band report has a duplicate listing key")
            seen_keys.add(key)

        return self._report(
            source_entry_name=name,
            content_name=name,
            family=family,
            disposition=BundleEntryDisposition.SELECTED_VALIDATED,
            claimed_report_date=claimed_date,
            confirmed_row_dates=(),
            date_status=ReportDateStatus.FILENAME_CLAIM_ONLY,
            date_role=date_role,
            source_entry_bytes=payload,
            content_bytes=payload,
            header=parsed_header,
            rows=rows,
        )

    def _parse_band_changes(
        self,
        name: str,
        payload: bytes,
        date_digits: str,
    ) -> ParsedDailyReport:
        claimed_date = _date_from_digits(date_digits, "%d%m%Y", "band-change filename")
        header, rows = self._parse_csv(
            payload,
            expected_header=PRICE_BAND_CHANGES_HEADER,
            report_name=name,
            allow_empty=True,
        )
        seen_keys: set[tuple[str, str]] = set()
        for expected_number, row in enumerate(rows, start=1):
            values = _field_map(header, row)
            if _unsigned_integer(values["Sr. No"], "band-change serial", minimum=1) != expected_number:
                raise DailyReportIntegrityError("band-change serials are not contiguous")
            if _SYMBOL.fullmatch(values["Symbol"]) is None:
                raise DailyReportIntegrityError("band-change symbol is invalid")
            if _SERIES.fullmatch(values["Series"]) is None:
                raise DailyReportIntegrityError("band-change series is invalid")
            if not values["Security Name"]:
                raise DailyReportIntegrityError("band-change security name is required")
            if values["From"] not in _BANDS or values["To"] not in _BANDS:
                raise DailyReportIntegrityError("band change is outside the pinned band domain")
            if values["From"] == values["To"]:
                raise DailyReportIntegrityError("band change does not change the band")
            key = (values["Symbol"], values["Series"])
            if key in seen_keys:
                raise DailyReportIntegrityError("band changes contain a duplicate listing key")
            seen_keys.add(key)

        return self._report(
            source_entry_name=name,
            content_name=name,
            family=DailyReportFamily.PRICE_BAND_CHANGES,
            disposition=BundleEntryDisposition.SELECTED_VALIDATED,
            claimed_report_date=claimed_date,
            confirmed_row_dates=(),
            date_status=ReportDateStatus.FILENAME_CLAIM_ONLY,
            date_role=ReportDateRole.CLAIMED_EFFECTIVE_DATE,
            source_entry_bytes=payload,
            content_bytes=payload,
            header=header,
            rows=rows,
        )

    def _parse_series_changes(self, name: str, payload: bytes) -> ParsedDailyReport:
        header, rows = self._parse_csv(
            payload,
            expected_header=SERIES_CHANGES_HEADER,
            report_name=name,
            allow_empty=True,
        )
        seen_keys: set[tuple[date, str]] = set()
        confirmed_dates: set[date] = set()
        for row in rows:
            values = _field_map(header, row)
            if _SYMBOL.fullmatch(values["Symbol"]) is None:
                raise DailyReportIntegrityError("series-change symbol is invalid")
            if not values["Security"]:
                raise DailyReportIntegrityError("series-change security name is required")
            if _SERIES.fullmatch(values["From Series"]) is None or _SERIES.fullmatch(
                values["To Series"]
            ) is None:
                raise DailyReportIntegrityError("series-change series is invalid")
            if values["From Series"] == values["To Series"]:
                raise DailyReportIntegrityError("series change does not change the series")
            effective_date = _nse_text_date(
                values["Change Date"],
                "series-change effective date",
            )
            confirmed_dates.add(effective_date)
            key = (effective_date, values["Symbol"])
            if key in seen_keys:
                raise DailyReportIntegrityError("series changes contain a duplicate transition")
            seen_keys.add(key)

        return self._report(
            source_entry_name=name,
            content_name=name,
            family=DailyReportFamily.SERIES_CHANGES,
            disposition=BundleEntryDisposition.SELECTED_VALIDATED,
            claimed_report_date=None,
            confirmed_row_dates=tuple(sorted(confirmed_dates)),
            date_status=(
                ReportDateStatus.INTERNAL_DATES_ONLY
                if confirmed_dates
                else ReportDateStatus.NO_DATE_AVAILABLE
            ),
            date_role=ReportDateRole.INTERNAL_EFFECTIVE_DATES,
            source_entry_bytes=payload,
            content_bytes=payload,
            header=header,
            rows=rows,
        )

    def _parse_security_master(self, name: str, payload: bytes) -> ParsedDailyReport:
        try:
            parsed = self.security_master_parser.parse_bytes(
                payload,
                original_filename=name,
            )
        except Exception as strict_error:
            # The interoperability report uses the same filename and schema but
            # can contain BSE-exclusive ticker syntax (for example a trailing
            # '$') that is intentionally outside the NSE-only parser.  Inspect
            # only enough of that file to quarantine it; never normalize its
            # rows into the NSE universe.
            content_bytes = _bounded_gzip_decompress(
                payload,
                self.maximum_entry_bytes,
            )
            header, rows = self._parse_csv(
                content_bytes,
                expected_header=NSE_CM_MII_SECURITY_HEADER,
                report_name=name,
                allow_empty=False,
            )
            permitted_index = header.index("PrtdToTrad")
            counts = Counter(row[permitted_index] for row in rows)
            if not set(counts).issubset({"0", "1", "2"}) or counts["2"] == 0:
                raise DailyReportIntegrityError(
                    "NSE-only security-master entry is malformed or outside the pinned schema"
                ) from strict_error
            claimed_date = _date_from_digits(
                _SECURITY_MASTER_NAME.fullmatch(name).group(1),
                "%d%m%Y",
                "security-master filename",
            )
            return ParsedDailyReport(
                source_entry_name=name,
                content_name=name[:-3],
                family=DailyReportFamily.SECURITY_MASTER,
                disposition=(
                    BundleEntryDisposition.QUARANTINED_INTEROPERABILITY_SECURITY_MASTER
                ),
                claimed_report_date=claimed_date,
                confirmed_row_dates=(),
                date_status=ReportDateStatus.FILENAME_CLAIM_ONLY,
                date_role=ReportDateRole.CLAIMED_REPORT_DATE,
                source_entry_sha256=_sha256(payload),
                content_sha256=_sha256(content_bytes),
                source_entry_byte_count=len(payload),
                content_byte_count=len(content_bytes),
                header=header,
                header_sha256=_header_sha256(header),
                row_count=len(rows),
                ordered_row_digest=_ordered_row_digest(rows),
                rows=(),
            )
        if parsed.excluded_alternative_venue_count:
            disposition = (
                BundleEntryDisposition.QUARANTINED_INTEROPERABILITY_SECURITY_MASTER
            )
        else:
            disposition = BundleEntryDisposition.DEFERRED_NSE_ONLY_SECURITY_MASTER
        return ParsedDailyReport(
            source_entry_name=name,
            content_name=name[:-3],
            family=DailyReportFamily.SECURITY_MASTER,
            disposition=disposition,
            claimed_report_date=parsed.claimed_report_date,
            confirmed_row_dates=(),
            date_status=ReportDateStatus.FILENAME_CLAIM_ONLY,
            date_role=ReportDateRole.CLAIMED_REPORT_DATE,
            source_entry_sha256=_sha256(payload),
            content_sha256=parsed.uncompressed_sha256,
            source_entry_byte_count=len(payload),
            content_byte_count=parsed.uncompressed_byte_count,
            header=parsed.header,
            header_sha256=parsed.header_sha256,
            row_count=len(parsed.records),
            ordered_row_digest=parsed.ordered_row_digest,
            rows=(),
        )

    @staticmethod
    def _report(
        *,
        source_entry_name: str,
        content_name: str,
        family: DailyReportFamily,
        disposition: BundleEntryDisposition,
        claimed_report_date: date | None,
        confirmed_row_dates: tuple[date, ...],
        date_status: ReportDateStatus,
        date_role: ReportDateRole,
        source_entry_bytes: bytes,
        content_bytes: bytes,
        header: tuple[str, ...],
        rows: tuple[tuple[str, ...], ...],
    ) -> ParsedDailyReport:
        return ParsedDailyReport(
            source_entry_name=source_entry_name,
            content_name=content_name,
            family=family,
            disposition=disposition,
            claimed_report_date=claimed_report_date,
            confirmed_row_dates=confirmed_row_dates,
            date_status=date_status,
            date_role=date_role,
            source_entry_sha256=_sha256(source_entry_bytes),
            content_sha256=_sha256(content_bytes),
            source_entry_byte_count=len(source_entry_bytes),
            content_byte_count=len(content_bytes),
            header=header,
            header_sha256=_header_sha256(header),
            row_count=len(rows),
            ordered_row_digest=_ordered_row_digest(rows),
            rows=rows,
        )

    @staticmethod
    def _validate_bundle_completeness(reports: tuple[ParsedDailyReport, ...]) -> None:
        selected = tuple(
            report
            for report in reports
            if report.disposition is BundleEntryDisposition.SELECTED_VALIDATED
        )
        families = {report.family for report in selected}
        missing = _REQUIRED_SELECTED_FAMILIES - families
        if missing:
            names = ", ".join(sorted(family.value for family in missing))
            raise DailyReportIntegrityError(
                f"daily bundle is missing required report families: {names}"
            )
        if sum(report.family is DailyReportFamily.SERIES_CHANGES for report in selected) != 1:
            raise DailyReportIntegrityError("daily bundle must contain one series-change report")

        for family in _REQUIRED_SELECTED_FAMILIES - {DailyReportFamily.SERIES_CHANGES}:
            claimed_dates = [
                report.claimed_report_date
                for report in selected
                if report.family is family
            ]
            if len(set(claimed_dates)) != len(claimed_dates):
                raise DailyReportIntegrityError(
                    f"daily bundle contains duplicate dates for {family.value}"
                )

        udiff_dates = {
            report.claimed_report_date
            for report in selected
            if report.family is DailyReportFamily.UDIFF_BHAVCOPY
        }
        full_dates = {
            report.claimed_report_date
            for report in selected
            if report.family is DailyReportFamily.FULL_BHAVCOPY_DELIVERY
        }
        if udiff_dates != full_dates:
            raise DailyReportIntegrityError(
                "UDiFF and full-delivery Bhavcopy date coverage must match"
            )

        udiff_by_date: dict[date, dict[tuple[str, str], dict[str, str]]] = {}
        for report in selected:
            if report.family is not DailyReportFamily.UDIFF_BHAVCOPY:
                continue
            rows_for_date: dict[tuple[str, str], dict[str, str]] = {}
            for row in report.rows:
                values = _field_map(report.header, row)
                rows_for_date[(values["TckrSymb"], values["SctySrs"])] = values
            udiff_by_date[report.claimed_report_date] = rows_for_date

        exact_fields = {
            "PREV_CLOSE": "PrvsClsgPric",
            "OPEN_PRICE": "OpnPric",
            "HIGH_PRICE": "HghPric",
            "LOW_PRICE": "LwPric",
            "LAST_PRICE": "LastPric",
            "CLOSE_PRICE": "ClsPric",
            "TTL_TRD_QNTY": "TtlTradgVol",
            "NO_OF_TRADES": "TtlNbOfTxsExctd",
        }
        for report in selected:
            if report.family is not DailyReportFamily.FULL_BHAVCOPY_DELIVERY:
                continue
            udiff_rows = udiff_by_date[report.claimed_report_date]
            stripped_header = tuple(value.strip() for value in report.header)
            full_keys: set[tuple[str, str]] = set()
            covered_series: set[str] = set()
            for row in report.rows:
                full = _field_map(
                    stripped_header,
                    tuple(value.strip() for value in row),
                )
                full_key = (full["SYMBOL"], full["SERIES"])
                full_keys.add(full_key)
                covered_series.add(full["SERIES"])
                udiff = udiff_rows.get(full_key)
                if udiff is None:
                    raise DailyReportIntegrityError(
                        "full Bhavcopy row is missing from same-date UDiFF"
                    )
                for full_name, udiff_name in exact_fields.items():
                    if Decimal(full[full_name]) != Decimal(udiff[udiff_name]):
                        raise DailyReportIntegrityError(
                            "full Bhavcopy contradicts same-date UDiFF"
                        )
                traded_volume = Decimal(udiff["TtlTradgVol"])
                transfer_value = Decimal(udiff["TtlTrfVal"])
                precise_average = transfer_value / traded_volume
                expected_turnover_lacs = (
                    transfer_value / Decimal(100_000)
                ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                expected_average = precise_average.quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                )
                if (
                    Decimal(full["TURNOVER_LACS"]) != expected_turnover_lacs
                    or Decimal(full["AVG_PRICE"]) != expected_average
                ):
                    raise DailyReportIntegrityError(
                        "full Bhavcopy derived values contradict same-date UDiFF"
                    )
            expected_keys = {
                key for key in udiff_rows if key[1] in covered_series
            }
            if full_keys != expected_keys:
                raise DailyReportIntegrityError(
                    "full Bhavcopy incompletely covers a reported series"
                )
            udiff_series = {key[1] for key in udiff_rows}
            missing_core_series = (
                udiff_series & _CORE_FULL_BHAVCOPY_SERIES
            ) - covered_series
            if missing_core_series:
                raise DailyReportIntegrityError(
                    "full Bhavcopy omits a core equity series"
                )


def report_family_counts(parsed: ParsedNseDailyBundle) -> dict[str, int]:
    counts = Counter(
        report.family.value
        for report in parsed.reports
        if report.disposition is BundleEntryDisposition.SELECTED_VALIDATED
    )
    return dict(sorted(counts.items()))
