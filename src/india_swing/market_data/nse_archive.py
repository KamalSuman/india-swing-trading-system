from __future__ import annotations

import csv
import hashlib
import io
import re
import zipfile
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP, localcontext
from pathlib import Path
from typing import Mapping

from india_swing._filesystem import FileSafetyError, read_stable_regular_file
from india_swing.daily_reports.models import BundleEntryDisposition, DailyReportError
from india_swing.daily_reports.parser import (
    REG1_SURVEILLANCE_HEADER,
    NseDailyBundleParser,
    _RAW_IDENTIFIER,
    _SERIES,
    _SYMBOL,
    _date_from_digits,
    _decimal,
    _nse_text_date,
    _unsigned_integer,
)
from india_swing.identity import content_id
from india_swing.reference_data.models import ReferenceArtifactIntegrityError
from india_swing.reference_data.security_master import NseCmSecurityMasterParser

from .snapshot_store import LocalMarketSnapshotStore, StoredMarketSnapshot


NSE_HISTORICAL_ARCHIVE_EQ_DATASET = "nse-historical-archive-eq"
NSE_HISTORICAL_ARCHIVE_INDEX_DATASET = "nse-historical-archive-eq-index"
NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION_V1 = (
    "nse-historical-archive-eq-session/v1"
)
NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION_V2 = "nse-historical-archive-eq-session/v2"
NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION = "nse-historical-archive-eq-session/v3"
NSE_HISTORICAL_ARCHIVE_INDEX_SCHEMA_VERSION_V2 = (
    "nse-historical-archive-eq-index/v2"
)
NSE_HISTORICAL_ARCHIVE_INDEX_SCHEMA_VERSION = (
    "nse-historical-archive-eq-index/v3"
)
NSE_HISTORICAL_ARCHIVE_IMPORTER_VERSION = "nse-archive-eq-importer/v6"
NSE_HISTORICAL_ARCHIVE_PROVIDER = "NSE_ARCHIVE"
SOURCE_IDENTITY_CLAIM_KIND_LEGACY_BHAVCOPY_ISIN = "LEGACY_BHAVCOPY_ISIN"
SOURCE_IDENTITY_CLAIM_STATUS_SOURCE_CLAIMED_UNVERIFIED = "SOURCE_CLAIMED_UNVERIFIED"
MAXIMUM_ARCHIVE_BYTES = 128 * 1024 * 1024
MAXIMUM_ENTRY_BYTES = 64 * 1024 * 1024
MAXIMUM_RECORDS = 20_000

_ARCHIVE_NAME = re.compile(r"Reports-Archives-Multiple-(\d{8})\.zip\Z")
_SESSION_DIRECTORY = re.compile(r"\d{4}-\d{2}-\d{2}\Z")

EVIDENCE_PROFILE_PRICE_UDIFF = "PRICE_DELIVERY_UDIFF_ONLY"
EVIDENCE_PROFILE_PRICE_UDIFF_SECURITY = (
    "PRICE_DELIVERY_UDIFF_SECURITY_MASTER"
)
EVIDENCE_PROFILE_COMPLETE = (
    "PRICE_DELIVERY_UDIFF_SECURITY_MASTER_REG1"
)
EVIDENCE_PROFILE_UNRECONCILED = "PRICE_DELIVERY_UNRECONCILED"
_EVIDENCE_PROFILE_MISSING = {
    EVIDENCE_PROFILE_PRICE_UDIFF: (
        "REG1_SURVEILLANCE",
        "NSE_CM_SECURITY_MASTER",
    ),
    EVIDENCE_PROFILE_PRICE_UDIFF_SECURITY: ("REG1_SURVEILLANCE",),
    EVIDENCE_PROFILE_COMPLETE: (),
    EVIDENCE_PROFILE_UNRECONCILED: (
        "UDIFF_BHAVCOPY",
        "NSE_CM_SECURITY_MASTER",
        "REG1_SURVEILLANCE",
    ),
}
IDENTITY_STATUS_UDIFF_AND_SECURITY_MASTER_EVIDENCE_UNAVAILABLE = (
    "UDIFF_AND_SECURITY_MASTER_EVIDENCE_UNAVAILABLE"
)
_UDIFF_HISTORICAL_HEADER_ALIASES = {
    "Rsvd01": "Rsvd1",
    "Rsvd02": "Rsvd2",
    "Rsvd03": "Rsvd3",
    "Rsvd04": "Rsvd4",
}
_REG1_HISTORICAL_HEADER = (
    "Scrip Code",
    "Symbol",
    "Nse Exclusive",
    "Status",
    "Series",
    "GSM",
    "LTASM",
    "Unsolicited SMS",
    "Insolvency_Resoluti on_Process(IRP)",
    "STASM",
    "Default",
    "ICA",
    "Filler4",
    "Filler5",
    "Monthly Pledge",
    "Add On PB",
    "Daily Pledge",
    "Social Media Platform",
    "ESM",
    "Loss Making",
    "The Overall encumbered share in the scrip is more than 50 Percent",
    "Under BZ/SZ Series",
    "Company has failed to pay Annual listing fee",
    "Filler12",
    "Derivative contracts in the scrip to be moved out of F and O",
    "Scrip PE is greater than 50",
    "EPS in the scrip is zero",
    "Less than 100 unique PAN traded in previous 30 days",
    "Mandatory Market making period in SME scrip is over",
    "SME scrip is not regularly traded",
    "Close to Close price movement greater than 25% in previous 5 trading days",
    "Close to Close price movement greater than 40% in previous 15 trading days",
    "Close to Close price movement greater than 100% in previous 60 trading Days",
    "Close to Close price movement greater than 25% in previous 15 Days",
    "Close to Close price movement greater than 50% in previous 1 month",
    "Close to Close price movement greater than 90% in previous 3 months",
    "Close to Close price movement greater than 25% in previous 1 month",
    "Close to Close price movement greater than 50% in previous 3 months",
    "Close to Close price movement greater than 200% in previous 365 Days",
    "Close to Close price movement greater than 75% in previous 6 months",
    "Close to Close price movement greater than 100% in previous 365 days",
    "High low price variation greater than 75% in previous 1 month",
    "High low price variation greater than 150% in previous 3 months",
    "High low price variation greater than 75% in previous 3 months",
    "High low price variation greater than 300% in previous 365 Days",
    "High low price variation greater than 100% in previous 6 months",
    "High low price variation greater than 200% in previous 365 Days",
    "High low price variation greater than 150% in previous 12 months",
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


class NseHistoricalArchiveError(ValueError):
    pass


class NseHistoricalArchiveIntegrityError(NseHistoricalArchiveError):
    pass


@dataclass(frozen=True, slots=True)
class ParsedNseHistoricalArchiveSession:
    session: date
    source_mode: str
    source_container_sha256: str
    source_entry_sha256: tuple[tuple[str, str], ...]
    normalized_payload: Mapping[str, object]

    def __post_init__(self) -> None:
        if type(self.session) is not date:
            raise TypeError("historical archive session must be an exact date")
        if self.source_mode not in {
            "OFFICIAL_OUTER_ZIP",
            "VALIDATED_EXTRACTED_ENTRY_SET",
        }:
            raise ValueError("unsupported historical archive source mode")
        for value, name in (
            (self.source_container_sha256, "source_container_sha256"),
        ):
            if re.fullmatch(r"[0-9a-f]{64}", value) is None:
                raise ValueError(f"{name} must be a lowercase SHA-256")
        if (
            type(self.source_entry_sha256) is not tuple
            or len(self.source_entry_sha256) not in {1, 2, 3, 4}
            or self.source_entry_sha256
            != tuple(sorted(self.source_entry_sha256))
        ):
            raise ValueError("source entry hashes must be one to four ordered pairs")
        if not isinstance(self.normalized_payload, Mapping):
            raise TypeError("normalized payload must be a mapping")


@dataclass(frozen=True, slots=True)
class ImportedNseHistoricalArchiveSession:
    session: date
    snapshot_id: str
    record_count: int
    source_container_sha256: str
    identity_issue_count: int
    evidence_profile: str


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _expected_names(session: date) -> tuple[str, str, str, str]:
    return (
        f"sec_bhavdata_full_{session:%d%m%Y}.csv",
        f"BhavCopy_NSE_CM_0_0_0_{session:%Y%m%d}_F_0000.csv.zip",
        f"REG1_IND{session:%d%m%y}.csv",
        f"NSE_CM_security_{session:%d%m%Y}.csv.gz",
    )


_LEGACY_MONTH_ABBREVIATIONS = (
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN",
    "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
)
_LEGACY_FULL_BHAVCOPY_HEADER_NAMES = (
    "SYMBOL",
    "SERIES",
    "OPEN",
    "HIGH",
    "LOW",
    "CLOSE",
    "LAST",
    "PREVCLOSE",
    "TOTTRDQTY",
    "TOTTRDVAL",
    "TIMESTAMP",
    "TOTALTRADES",
    "ISIN",
)
_LEGACY_FULL_BHAVCOPY_HEADER = _LEGACY_FULL_BHAVCOPY_HEADER_NAMES + ("",)
# One proven official variant (10-Jul-2017) has the same exact 13 named
# columns but no trailing comma, so no terminal empty field on the header
# or any data row.
_LEGACY_FULL_BHAVCOPY_HEADER_NO_TERMINAL = _LEGACY_FULL_BHAVCOPY_HEADER_NAMES
_MTO_TITLE_LINE = "Security Wise Delivery Position - Compulsory Rolling Settlement"
_MTO_CONTROL_RECORD_TYPE = "10"
_MTO_DATA_RECORD_TYPE = "20"
_MTO_HEADER_RECORD = (
    # The real file omits a "Series" label even though every type-20 data
    # row carries series as its fourth field.
    "Record Type",
    "Sr No",
    "Name of Security",
    "Quantity Traded",
    "Deliverable Quantity(gross across client level)",
    "% of Deliverable Quantity to Traded Quantity",
)
_MTO_TRADE_DATE_FIELD = re.compile(r"Trade Date <(?P<value>[^>]+)>\Z")
# One proven official publisher typo: the leading "T" of "Trade Date" is
# dropped on some sessions' four-field settlement lines.
_MTO_TRADE_DATE_TYPO_FIELD = re.compile(r"rade Date <(?P<value>[^>]+)>\Z")
_MTO_SETTLEMENT_TYPE_FIELD = re.compile(r"Settlement Type <(?P<value>[A-Z0-9]{1,10})>\Z")
_MTO_SETTLEMENT_NO_FIELD = re.compile(r"Settlement No <(?P<value>[0-9]+)>\Z")
_MTO_SETTLEMENT_DATE_FIELD = re.compile(r"Settlement Date <(?P<value>[^>]+)>\Z")
_ZERO_PADDED_INTEGER = re.compile(r"[0-9]+\Z")
_PERCENT_TOLERANCE = Decimal("0.01")


def _zero_padded_unsigned_integer(value: str, field_name: str) -> int:
    # The real control record's total/count fields are zero-padded (e.g.
    # "0001716"), which the strict daily_reports _unsigned_integer (no
    # leading zeros) legitimately rejects.
    if _ZERO_PADDED_INTEGER.fullmatch(value) is None:
        raise NseHistoricalArchiveIntegrityError(
            f"{field_name} is not an unsigned integer"
        )
    return int(value)


def _legacy_bhavcopy_stem(session: date) -> str:
    # Locale-independent: never depends on strftime's %b, which is bound to
    # the process's C-library locale.
    return (
        f"cm{session.day:02d}"
        f"{_LEGACY_MONTH_ABBREVIATIONS[session.month - 1]}"
        f"{session.year:04d}bhav"
    )


def _expected_legacy_names(session: date) -> tuple[str, str]:
    return (
        f"{_legacy_bhavcopy_stem(session)}.csv.zip",
        f"MTO_{session:%d%m%Y}.DAT",
    )


def _evidence_profile_for_names(
    names: object,
    *,
    session: date,
) -> tuple[str, tuple[str, ...]]:
    if type(names) not in {tuple, set, frozenset}:
        raise NseHistoricalArchiveIntegrityError(
            "historical archive entry set is invalid"
        )
    expected = _expected_names(session)
    legacy = _expected_legacy_names(session)
    actual = set(names)
    profiles = {
        frozenset(expected[:1]): EVIDENCE_PROFILE_UNRECONCILED,
        frozenset(expected[:2]): EVIDENCE_PROFILE_PRICE_UDIFF,
        frozenset((expected[0], expected[1], expected[3])): (
            EVIDENCE_PROFILE_PRICE_UDIFF_SECURITY
        ),
        frozenset(expected): EVIDENCE_PROFILE_COMPLETE,
        frozenset(legacy): EVIDENCE_PROFILE_UNRECONCILED,
    }
    profile = profiles.get(frozenset(actual))
    if profile is None or len(actual) != len(names):
        raise NseHistoricalArchiveIntegrityError(
            "historical archive entry set is not an accepted evidence profile"
        )
    selected = legacy if frozenset(actual) == frozenset(legacy) else expected
    return profile, tuple(name for name in selected if name in actual)


def _session_from_archive_name(original_filename: str) -> date:
    if type(original_filename) is not str or Path(original_filename).name != original_filename:
        raise NseHistoricalArchiveIntegrityError(
            "historical archive filename must be a safe basename"
        )
    match = _ARCHIVE_NAME.fullmatch(original_filename)
    if match is None:
        raise NseHistoricalArchiveIntegrityError(
            "historical archive filename is not canonical"
        )
    try:
        return datetime.strptime(match.group(1), "%d%m%Y").date()
    except ValueError:
        raise NseHistoricalArchiveIntegrityError(
            "historical archive filename date is invalid"
        ) from None


def _field_map(header: tuple[str, ...], row: tuple[str, ...]) -> dict[str, str]:
    return dict(zip(header, row, strict=True))


def _eq_only_csv(
    payload: bytes,
    *,
    series_field: str,
    label: str,
    allow_single_trailing_empty_header: bool = False,
    header_aliases: Mapping[str, str] | None = None,
) -> bytes:
    try:
        text = payload.decode("utf-8-sig", errors="strict")
        rows = list(csv.reader(io.StringIO(text, newline=""), strict=True))
    except (UnicodeDecodeError, csv.Error):
        raise NseHistoricalArchiveIntegrityError(
            f"{label} is not strict UTF-8 CSV"
        ) from None
    if not rows:
        raise NseHistoricalArchiveIntegrityError(f"{label} is empty")
    if (
        allow_single_trailing_empty_header
        and rows[0]
        and rows[0][-1] == ""
        and all(len(row) == len(rows[0]) - 1 for row in rows[1:])
    ):
        rows[0] = rows[0][:-1]
    if header_aliases is not None:
        rows[0] = [header_aliases.get(value.strip(), value) for value in rows[0]]
    header = tuple(value.strip() for value in rows[0])
    if len(set(header)) != len(header):
        raise NseHistoricalArchiveIntegrityError(
            f"{label} header contains duplicate fields"
        )
    try:
        series_index = header.index(series_field)
    except ValueError:
        raise NseHistoricalArchiveIntegrityError(
            f"{label} lacks its series field"
        ) from None
    selected = [rows[0]]
    for row in rows[1:]:
        if len(row) != len(rows[0]):
            raise NseHistoricalArchiveIntegrityError(
                f"{label} row width is inconsistent"
            )
        if row[series_index].strip() == "EQ":
            selected.append(row)
    if len(selected) == 1:
        raise NseHistoricalArchiveIntegrityError(f"{label} contains no EQ rows")
    target = io.StringIO(newline="")
    csv.writer(target, lineterminator="\n").writerows(selected)
    return target.getvalue().encode("utf-8")


def _eq_only_udiff(payload: bytes) -> bytes:
    validator = NseDailyBundleParser()
    try:
        with zipfile.ZipFile(io.BytesIO(payload)) as source:
            infos = source.infolist()
            if len(infos) != 1 or infos[0].is_dir():
                raise NseHistoricalArchiveIntegrityError(
                    "UDiFF container must contain exactly one CSV"
                )
            validator._validate_zip_entry_info(infos[0])
            if infos[0].file_size > MAXIMUM_ENTRY_BYTES:
                raise NseHistoricalArchiveIntegrityError(
                    "UDiFF inner CSV exceeds its size limit"
                )
            if source.testzip() is not None:
                raise NseHistoricalArchiveIntegrityError("UDiFF CRC verification failed")
            inner_name = infos[0].filename
            inner_payload = source.read(infos[0])
    except NseHistoricalArchiveIntegrityError:
        raise
    except (OSError, RuntimeError, zipfile.BadZipFile, zipfile.LargeZipFile):
        raise NseHistoricalArchiveIntegrityError("UDiFF container is invalid") from None
    filtered = _eq_only_csv(
        inner_payload,
        series_field="SctySrs",
        label="UDiFF Bhavcopy",
        allow_single_trailing_empty_header=True,
        header_aliases=_UDIFF_HISTORICAL_HEADER_ALIASES,
    )
    target = io.BytesIO()
    with zipfile.ZipFile(target, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(inner_name, filtered)
    return target.getvalue()


def _canonicalize_reg1_header(payload: bytes) -> bytes:
    try:
        text = payload.decode("utf-8-sig", errors="strict")
        rows = list(csv.reader(io.StringIO(text, newline=""), strict=True))
    except (UnicodeDecodeError, csv.Error):
        return payload
    if not rows or tuple(rows[0]) != _REG1_HISTORICAL_HEADER:
        return payload
    if len(REG1_SURVEILLANCE_HEADER) != len(_REG1_HISTORICAL_HEADER) or any(
        len(row) != len(_REG1_HISTORICAL_HEADER) for row in rows[1:]
    ):
        raise NseHistoricalArchiveIntegrityError(
            "historical REG1 row width is inconsistent"
        )
    rows[0] = list(REG1_SURVEILLANCE_HEADER)
    target = io.StringIO(newline="")
    csv.writer(target, lineterminator="\n").writerows(rows)
    return target.getvalue().encode("utf-8")


def _extract_archive_entries(
    payload: bytes,
    *,
    session: date,
) -> dict[str, bytes]:
    if type(payload) is not bytes or not payload:
        raise NseHistoricalArchiveIntegrityError("historical archive is empty")
    if len(payload) > MAXIMUM_ARCHIVE_BYTES:
        raise NseHistoricalArchiveIntegrityError("historical archive exceeds its size limit")
    parser = NseDailyBundleParser()
    try:
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            infos = archive.infolist()
            names = tuple(info.filename for info in infos)
            _, selected_names = _evidence_profile_for_names(
                names,
                session=session,
            )
            if archive.testzip() is not None:
                raise NseHistoricalArchiveIntegrityError(
                    "historical archive CRC verification failed"
                )
            for info in infos:
                parser._validate_zip_entry_info(info)
                if info.file_size > MAXIMUM_ENTRY_BYTES:
                    raise NseHistoricalArchiveIntegrityError(
                        "historical archive entry exceeds its size limit"
                    )
            return {name: archive.read(name) for name in selected_names}
    except NseHistoricalArchiveIntegrityError:
        raise
    except (OSError, RuntimeError, zipfile.BadZipFile, zipfile.LargeZipFile):
        raise NseHistoricalArchiveIntegrityError("historical archive is invalid") from None


def _source_set_id(entries: Mapping[str, bytes]) -> str:
    return content_id(
        {
            "schema": "nse-historical-archive-entry-set/v1",
            "entries": tuple(
                (name, _sha256(payload)) for name, payload in sorted(entries.items())
            ),
        },
        length=64,
    )


def _reconcile_eq_rows(full: object, udiff: object) -> tuple[dict[str, object], ...]:
    udiff_rows: dict[tuple[str, str], dict[str, str]] = {}
    for row in udiff.rows:
        values = _field_map(udiff.header, row)
        key = (values["TckrSymb"], values["SctySrs"])
        udiff_rows[key] = values
    stripped_header = tuple(value.strip() for value in full.header)
    full_rows: dict[tuple[str, str], dict[str, str]] = {}
    for row in full.rows:
        values = _field_map(
            stripped_header,
            tuple(value.strip() for value in row),
        )
        key = (values["SYMBOL"], values["SERIES"])
        full_rows[key] = values
    if set(full_rows) != set(udiff_rows):
        raise NseHistoricalArchiveIntegrityError(
            "EQ coverage differs between full Bhavcopy and UDiFF"
        )

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
    reconciled: list[dict[str, object]] = []
    with localcontext() as context:
        context.prec = 50
        for key in sorted(full_rows):
            full_values = full_rows[key]
            udiff_values = udiff_rows[key]
            for full_name, udiff_name in exact_fields.items():
                if Decimal(full_values[full_name]) != Decimal(udiff_values[udiff_name]):
                    raise NseHistoricalArchiveIntegrityError(
                        "EQ OHLCV or trade fields contradict UDiFF"
                    )
            volume = Decimal(udiff_values["TtlTradgVol"])
            transfer_value = Decimal(udiff_values["TtlTrfVal"])
            turnover_lacs = (transfer_value / Decimal(100_000)).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            average_price = (transfer_value / volume).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            if (
                Decimal(full_values["TURNOVER_LACS"]) != turnover_lacs
                or Decimal(full_values["AVG_PRICE"]) != average_price
            ):
                raise NseHistoricalArchiveIntegrityError(
                    "EQ derived values contradict UDiFF"
                )
            reconciled.append(
                {
                    "key": key,
                    "full": full_values,
                    "udiff": udiff_values,
                }
            )
    return tuple(reconciled)


def _surveillance_by_key(reg1: object) -> dict[tuple[str, str], dict[str, str]]:
    result: dict[tuple[str, str], dict[str, str]] = {}
    identity_fields = {"ScripCode", "Symbol", "Nse Exclusive", "Status", "Series"}
    for row in reg1.rows:
        values = _field_map(reg1.header, row)
        key = (values["Symbol"], values["Series"])
        indicators = {
            name: value
            for name, value in values.items()
            if name not in identity_fields
            and not name.startswith("Filler")
            and value != ""
        }
        result[key] = indicators
    return result


def _unreconciled_eq_records(
    full: object, session: date
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    stripped_header = tuple(value.strip() for value in full.header)
    records: list[dict[str, object]] = []
    identity_issues: list[dict[str, object]] = []
    seen_keys: set[tuple[str, str]] = set()
    for row in full.rows:
        full_values = _field_map(
            stripped_header, tuple(value.strip() for value in row)
        )
        symbol = full_values["SYMBOL"]
        series = full_values["SERIES"]
        key = (symbol, series)
        if key in seen_keys:
            raise NseHistoricalArchiveIntegrityError(
                "full Bhavcopy contains a duplicate listing key"
            )
        seen_keys.add(key)
        issue = {
            "session": session,
            "listing_key": f"NSE:{symbol}",
            "series": series,
            "udiff_financial_instrument_id": None,
            "security_master_financial_instrument_id": None,
            "security_master_source_identifier": None,
            "udiff_source_identifier": None,
            "status": IDENTITY_STATUS_UDIFF_AND_SECURITY_MASTER_EVIDENCE_UNAVAILABLE,
        }
        issue["issue_id"] = content_id(
            {
                "schema": "nse-historical-archive-identity-issue/v1",
                **issue,
            },
            length=64,
        )
        identity_issues.append(issue)
        delivery_quantity = (
            None if full_values["DELIV_QTY"] == "-" else int(full_values["DELIV_QTY"])
        )
        delivery_percent = (
            None
            if full_values["DELIV_PER"] == "-"
            else Decimal(full_values["DELIV_PER"])
        )
        record: dict[str, object] = {
            "session": session,
            "listing_key": f"NSE:{symbol}",
            "symbol": symbol,
            "series": series,
            "financial_instrument_id": None,
            "security_master_financial_instrument_id": None,
            "security_source_record_id": None,
            "security_master_source_identifier": None,
            "udiff_source_identifier": None,
            "identity_status": IDENTITY_STATUS_UDIFF_AND_SECURITY_MASTER_EVIDENCE_UNAVAILABLE,
            "validated_isin": None,
            "normal_market_status": None,
            "normal_market_eligible": None,
            "permitted_to_trade": None,
            "delete_flag": None,
            "previous_close": Decimal(full_values["PREV_CLOSE"]),
            "open": Decimal(full_values["OPEN_PRICE"]),
            "high": Decimal(full_values["HIGH_PRICE"]),
            "low": Decimal(full_values["LOW_PRICE"]),
            "last": Decimal(full_values["LAST_PRICE"]),
            "close": Decimal(full_values["CLOSE_PRICE"]),
            "average_price": Decimal(full_values["AVG_PRICE"]),
            "volume": int(full_values["TTL_TRD_QNTY"]),
            "turnover_lacs": Decimal(full_values["TURNOVER_LACS"]),
            "trade_count": int(full_values["NO_OF_TRADES"]),
            "delivery_quantity": delivery_quantity,
            "delivery_percent": delivery_percent,
            "surveillance_indicators": {},
        }
        record["record_id"] = content_id(
            {
                "schema": "nse-historical-archive-eq-record/v1",
                **record,
            },
            length=64,
        )
        records.append(record)
    if not records or len(records) > MAXIMUM_RECORDS:
        raise NseHistoricalArchiveIntegrityError(
            "historical archive EQ record count is outside limits"
        )
    return records, identity_issues


def _legacy_full_bhavcopy_csv(payload: bytes, *, session: date) -> tuple[str, bytes]:
    inner_name = f"{_legacy_bhavcopy_stem(session)}.csv"
    validator = NseDailyBundleParser()
    try:
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            infos = archive.infolist()
            if len(infos) != 1 or infos[0].is_dir() or infos[0].filename != inner_name:
                raise NseHistoricalArchiveIntegrityError(
                    "legacy Bhavcopy container must contain exactly its canonical CSV"
                )
            validator._validate_zip_entry_info(infos[0])
            if infos[0].file_size > MAXIMUM_ENTRY_BYTES:
                raise NseHistoricalArchiveIntegrityError(
                    "legacy Bhavcopy inner CSV exceeds its size limit"
                )
            if archive.testzip() is not None:
                raise NseHistoricalArchiveIntegrityError(
                    "legacy Bhavcopy CRC verification failed"
                )
            inner_payload = archive.read(infos[0])
    except NseHistoricalArchiveIntegrityError:
        raise
    except (OSError, RuntimeError, zipfile.BadZipFile, zipfile.LargeZipFile):
        raise NseHistoricalArchiveIntegrityError(
            "legacy Bhavcopy container is invalid"
        ) from None
    if not inner_payload:
        raise NseHistoricalArchiveIntegrityError("legacy Bhavcopy inner CSV is empty")
    return inner_name, inner_payload


def _parse_legacy_full_bhavcopy(
    payload: bytes, *, session: date
) -> tuple[dict[str, object], ...]:
    try:
        text = payload.decode("utf-8-sig", errors="strict")
        rows = list(csv.reader(io.StringIO(text, newline=""), strict=True))
    except (UnicodeDecodeError, csv.Error):
        raise NseHistoricalArchiveIntegrityError(
            "legacy Bhavcopy is not strict UTF-8 CSV"
        ) from None
    if not rows:
        raise NseHistoricalArchiveIntegrityError("legacy Bhavcopy is empty")
    # Exactly two accepted whole-file shapes, selected by exact header
    # match: the standard 14-field header/rows ending in one empty field,
    # or the same 13 named fields with no terminal empty field at all
    # (one proven official variant, 10-Jul-2017). Never a mixture.
    if tuple(rows[0]) == _LEGACY_FULL_BHAVCOPY_HEADER:
        header = _LEGACY_FULL_BHAVCOPY_HEADER
        require_terminal_empty = True
    elif tuple(rows[0]) == _LEGACY_FULL_BHAVCOPY_HEADER_NO_TERMINAL:
        header = _LEGACY_FULL_BHAVCOPY_HEADER_NO_TERMINAL
        require_terminal_empty = False
    else:
        raise NseHistoricalArchiveIntegrityError(
            "legacy Bhavcopy header is not canonical"
        )
    data_rows = rows[1:]
    if not data_rows:
        raise NseHistoricalArchiveIntegrityError("legacy Bhavcopy contains no data rows")

    # Every row -- EQ or not -- is structurally validated: exact width and
    # terminal-field convention (matching whichever header shape was
    # selected), canonical symbol/series, exact session TIMESTAMP, unique
    # (symbol, series), valid ISIN shape, and numeric parseability of
    # every price/value/quantity/trade-count field. Only the stricter
    # business constraints (strictly positive prices, OHLC consistency,
    # strictly positive traded quantity/trade count) are gated to the
    # declared EQ lane, so a real excluded non-EQ business anomaly
    # (observed officially: a "YT" row with LAST=0) stays excluded rather
    # than rejecting the session, while a malformed or wrong-dated non-EQ
    # row (e.g. TIMESTAMP 01-JAN-1900) still fails closed. Excluded rows
    # are never normalized, retained, or joined.
    seen_keys: set[tuple[str, str]] = set()
    eq_rows: list[dict[str, object]] = []
    try:
        for row_number, row in enumerate(data_rows, start=2):
            if len(row) != len(header):
                raise NseHistoricalArchiveIntegrityError(
                    "legacy Bhavcopy row width is inconsistent"
                )
            if require_terminal_empty and row[-1] != "":
                raise NseHistoricalArchiveIntegrityError(
                    "legacy Bhavcopy row has a non-empty terminal field"
                )
            values = _field_map(header, tuple(row))
            row_date = _nse_text_date(values["TIMESTAMP"], "legacy Bhavcopy TIMESTAMP")
            if row_date != session:
                raise NseHistoricalArchiveIntegrityError(
                    "legacy Bhavcopy row date disagrees with its session"
                )
            if _SYMBOL.fullmatch(values["SYMBOL"]) is None:
                raise NseHistoricalArchiveIntegrityError("legacy Bhavcopy symbol is invalid")
            if _SERIES.fullmatch(values["SERIES"]) is None:
                raise NseHistoricalArchiveIntegrityError("legacy Bhavcopy series is invalid")
            key = (values["SYMBOL"], values["SERIES"])
            if key in seen_keys:
                raise NseHistoricalArchiveIntegrityError(
                    "legacy Bhavcopy contains a duplicate listing key"
                )
            seen_keys.add(key)
            if _RAW_IDENTIFIER.fullmatch(values["ISIN"]) is None:
                raise NseHistoricalArchiveIntegrityError("legacy Bhavcopy ISIN is invalid")
            prices = {
                field_name: _decimal(values[field_name], f"legacy Bhavcopy {field_name}")
                for field_name in ("OPEN", "HIGH", "LOW", "CLOSE", "LAST", "PREVCLOSE")
            }
            traded_quantity = _unsigned_integer(
                values["TOTTRDQTY"], "legacy Bhavcopy traded quantity"
            )
            total_traded_value = _decimal(
                values["TOTTRDVAL"], "legacy Bhavcopy traded value"
            )
            total_trades = _unsigned_integer(
                values["TOTALTRADES"], "legacy Bhavcopy trade count"
            )

            if values["SERIES"] != "EQ":
                continue

            if any(price <= 0 for price in prices.values()):
                raise NseHistoricalArchiveIntegrityError(
                    "legacy Bhavcopy prices must be positive"
                )
            if prices["HIGH"] < max(
                prices["OPEN"], prices["LOW"], prices["LAST"], prices["CLOSE"]
            ) or prices["LOW"] > min(
                prices["OPEN"], prices["HIGH"], prices["LAST"], prices["CLOSE"]
            ):
                raise NseHistoricalArchiveIntegrityError(
                    "legacy Bhavcopy OHLC values are inconsistent"
                )
            if traded_quantity < 1:
                raise NseHistoricalArchiveIntegrityError(
                    "legacy Bhavcopy traded quantity must be positive"
                )
            if total_trades < 1:
                raise NseHistoricalArchiveIntegrityError(
                    "legacy Bhavcopy trade count must be positive"
                )
            eq_rows.append(
                {
                    "symbol": values["SYMBOL"],
                    "series": values["SERIES"],
                    "isin": values["ISIN"],
                    "row_number": row_number,
                    "open": prices["OPEN"],
                    "high": prices["HIGH"],
                    "low": prices["LOW"],
                    "close": prices["CLOSE"],
                    "last": prices["LAST"],
                    "previous_close": prices["PREVCLOSE"],
                    "traded_quantity": traded_quantity,
                    "total_traded_value": total_traded_value,
                    "total_trades": total_trades,
                }
            )
    except DailyReportError:
        raise NseHistoricalArchiveIntegrityError(
            "legacy Bhavcopy row validation failed"
        ) from None
    if not eq_rows:
        raise NseHistoricalArchiveIntegrityError("legacy Bhavcopy contains no EQ rows")
    return tuple(eq_rows)


def _parse_legacy_mto(payload: bytes, *, session: date) -> tuple[dict[str, object], ...]:
    # Real grammar (confirmed against the authorized official sample):
    #   line 0: a plain title line, one field, no CSV structure otherwise.
    #   line 1: "10,MTO,DDMMYYYY,<total deliverable qty>,<record count>" --
    #     the fourth field is the sum of every data row's DELIVERABLE
    #     quantity, not traded quantity, despite the field's position
    #     suggesting otherwise.
    #   line 2: "Trade Date <dd-Mon-yyyy>,Settlement Type <..>,
    #     Settlement No <..>,Settlement Date <dd-Mon-yyyy>".
    #   line 3: a fixed 6-label header (no "Series" label, even though
    #     every data row carries series as its fourth field).
    #   line 4..EOF: exact 7-field type-20 rows. There is no trailing
    #     control/trailer record.
    try:
        text = payload.decode("utf-8-sig", errors="strict")
        rows = list(csv.reader(io.StringIO(text, newline=""), strict=True))
    except (UnicodeDecodeError, csv.Error):
        raise NseHistoricalArchiveIntegrityError("MTO is not strict UTF-8 CSV") from None
    if len(rows) < 4:
        raise NseHistoricalArchiveIntegrityError("MTO is missing its required structure")

    title_row = rows[0]
    if len(title_row) != 1 or title_row[0] != _MTO_TITLE_LINE:
        raise NseHistoricalArchiveIntegrityError("MTO title line is not canonical")

    control_row = rows[1]
    if (
        len(control_row) != 5
        or control_row[0] != _MTO_CONTROL_RECORD_TYPE
        or control_row[1] != "MTO"
    ):
        raise NseHistoricalArchiveIntegrityError("MTO control record is not canonical")
    try:
        control_date = _date_from_digits(
            control_row[2], "%d%m%Y", "MTO control record date"
        )
    except DailyReportError:
        raise NseHistoricalArchiveIntegrityError(
            "MTO control record date is invalid"
        ) from None
    if control_date != session:
        raise NseHistoricalArchiveIntegrityError(
            "MTO control record date disagrees with its session"
        )
    total_deliverable_quantity = _zero_padded_unsigned_integer(
        control_row[3], "MTO control total deliverable quantity"
    )
    record_count = _zero_padded_unsigned_integer(
        control_row[4], "MTO control record count"
    )

    # Three proven official settlement-line shapes: (A) the standard four
    # fields; (B) the same four fields but with the publisher typo "rade
    # Date" instead of "Trade Date"; (C) exactly two fields, Trade Date and
    # Settlement Type only (the shape begins in 2019 and genuinely omits
    # Settlement No/Settlement Date rather than having them go missing).
    settlement_row = rows[2]
    settlement_date_text: str | None = None
    if len(settlement_row) == 4:
        trade_field, type_field, no_field, date_field = settlement_row
        trade_date_match = _MTO_TRADE_DATE_FIELD.fullmatch(
            trade_field
        ) or _MTO_TRADE_DATE_TYPO_FIELD.fullmatch(trade_field)
        settlement_type_match = _MTO_SETTLEMENT_TYPE_FIELD.fullmatch(type_field)
        settlement_no_match = _MTO_SETTLEMENT_NO_FIELD.fullmatch(no_field)
        settlement_date_match = _MTO_SETTLEMENT_DATE_FIELD.fullmatch(date_field)
        if (
            trade_date_match is None
            or settlement_type_match is None
            or settlement_no_match is None
            or settlement_date_match is None
        ):
            raise NseHistoricalArchiveIntegrityError(
                "MTO settlement line is not canonical"
            )
        settlement_date_text = settlement_date_match.group("value")
    elif len(settlement_row) == 2:
        trade_field, type_field = settlement_row
        trade_date_match = _MTO_TRADE_DATE_FIELD.fullmatch(trade_field)
        settlement_type_match = _MTO_SETTLEMENT_TYPE_FIELD.fullmatch(type_field)
        if trade_date_match is None or settlement_type_match is None:
            raise NseHistoricalArchiveIntegrityError(
                "MTO settlement line is not canonical"
            )
    else:
        raise NseHistoricalArchiveIntegrityError("MTO settlement line is not canonical")
    try:
        trade_date = _nse_text_date(trade_date_match.group("value"), "MTO trade date")
        # Settlement date, when present, is a genuinely different date
        # (T+n settlement); only well-formedness is required, never
        # equality to the session.
        if settlement_date_text is not None:
            _nse_text_date(settlement_date_text, "MTO settlement date")
    except DailyReportError:
        raise NseHistoricalArchiveIntegrityError(
            "MTO settlement line dates are invalid"
        ) from None
    if trade_date != session:
        raise NseHistoricalArchiveIntegrityError(
            "MTO trade date disagrees with its session"
        )

    if tuple(rows[3]) != _MTO_HEADER_RECORD:
        raise NseHistoricalArchiveIntegrityError("MTO header record is not canonical")

    data_rows = rows[4:]
    # One proven official variant (12-Oct-2018) has exactly one trailing
    # empty CSV record, caused by a second terminal newline. csv.reader
    # yields a blank line as [] (not a 7-field row), so strip at most one
    # such record and only from the very end -- an interior [] row, or a
    # second trailing one, still fails the width check below.
    if data_rows and data_rows[-1] == []:
        data_rows = data_rows[:-1]
    if not data_rows:
        raise NseHistoricalArchiveIntegrityError("MTO contains no data rows")

    seen_keys: set[tuple[str, str]] = set()
    parsed_rows: list[dict[str, object]] = []
    total_deliverable_seen = 0
    try:
        for index, row in enumerate(data_rows, start=1):
            if len(row) != 7:
                raise NseHistoricalArchiveIntegrityError("MTO row width is inconsistent")
            (
                record_type,
                serial_text,
                symbol,
                series,
                traded_text,
                deliverable_text,
                percent_text,
            ) = row
            if record_type != _MTO_DATA_RECORD_TYPE:
                raise NseHistoricalArchiveIntegrityError("MTO record type is not canonical")
            serial = _unsigned_integer(serial_text, "MTO serial number", minimum=1)
            if serial != index:
                raise NseHistoricalArchiveIntegrityError(
                    "MTO serial numbers are not sequential"
                )
            if _SYMBOL.fullmatch(symbol) is None:
                raise NseHistoricalArchiveIntegrityError("MTO symbol is invalid")
            if _SERIES.fullmatch(series) is None:
                raise NseHistoricalArchiveIntegrityError("MTO series is invalid")
            key = (symbol, series)
            if key in seen_keys:
                raise NseHistoricalArchiveIntegrityError(
                    "MTO contains a duplicate listing key"
                )
            seen_keys.add(key)
            traded_quantity = _unsigned_integer(
                traded_text, "MTO traded quantity", minimum=1
            )
            deliverable_quantity = _unsigned_integer(
                deliverable_text, "MTO deliverable quantity", minimum=0
            )
            percent = _decimal(
                percent_text, "MTO delivery percent", maximum=Decimal("100")
            )
            if deliverable_quantity > traded_quantity:
                raise NseHistoricalArchiveIntegrityError(
                    "MTO deliverable quantity exceeds traded quantity"
                )
            with localcontext() as context:
                context.prec = 50
                expected_percent = (
                    Decimal(100)
                    * Decimal(deliverable_quantity)
                    / Decimal(traded_quantity)
                ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                percent_difference = abs(percent - expected_percent)
            # Published percentage is bounded corroboration, not the
            # validation authority -- quantities remain authoritative and
            # unaltered. 387 real sessions differ from exact ROUND_HALF_UP
            # by exactly +/-0.01 on some rows; the published value (never
            # the recalculated one) is what gets stored.
            if percent_difference > _PERCENT_TOLERANCE:
                raise NseHistoricalArchiveIntegrityError(
                    "MTO delivery percent disagrees with its traded and "
                    "deliverable quantities"
                )
            total_deliverable_seen += deliverable_quantity
            parsed_rows.append(
                {
                    "symbol": symbol,
                    "series": series,
                    "traded_quantity": traded_quantity,
                    "deliverable_quantity": deliverable_quantity,
                    "percent": percent,
                }
            )
    except DailyReportError:
        raise NseHistoricalArchiveIntegrityError("MTO row validation failed") from None
    if (
        record_count != len(parsed_rows)
        or total_deliverable_quantity != total_deliverable_seen
    ):
        raise NseHistoricalArchiveIntegrityError(
            "MTO control accounting disagrees with its data rows"
        )
    return tuple(parsed_rows)


def _legacy_unreconciled_eq_records(
    legacy_zip_payload: bytes,
    mto_payload: bytes,
    *,
    session: date,
) -> tuple[
    tuple[dict[str, object], ...],
    tuple[dict[str, object], ...],
    tuple[dict[str, object], ...],
]:
    # _parse_legacy_full_bhavcopy already returns EQ-only rows (mirroring
    # the modern format's _eq_only_csv precedent); _parse_legacy_mto
    # returns every series, filtered to EQ below.
    inner_name, inner_payload = _legacy_full_bhavcopy_csv(
        legacy_zip_payload, session=session
    )
    bhavcopy_rows = _parse_legacy_full_bhavcopy(inner_payload, session=session)
    inner_sha256 = _sha256(inner_payload)
    mto_rows = _parse_legacy_mto(mto_payload, session=session)

    bhavcopy_eq = {(row["symbol"], row["series"]): row for row in bhavcopy_rows}
    mto_eq = {
        (row["symbol"], row["series"]): row
        for row in mto_rows
        if row["series"] == "EQ"
    }
    if not bhavcopy_eq or not mto_eq:
        raise NseHistoricalArchiveIntegrityError("legacy pair contains no EQ rows")
    if set(bhavcopy_eq) != set(mto_eq):
        raise NseHistoricalArchiveIntegrityError(
            "EQ coverage differs between legacy Bhavcopy and MTO"
        )

    records: list[dict[str, object]] = []
    identity_issues: list[dict[str, object]] = []
    claims: list[dict[str, object]] = []
    with localcontext() as context:
        context.prec = 50
        for key in sorted(bhavcopy_eq):
            bhav = bhavcopy_eq[key]
            mto = mto_eq[key]
            if bhav["traded_quantity"] != mto["traded_quantity"]:
                raise NseHistoricalArchiveIntegrityError(
                    "EQ traded quantity disagrees between legacy Bhavcopy and MTO"
                )
            symbol, series = key
            listing_key = f"NSE:{symbol}"
            claim: dict[str, object] = {
                "session": session,
                "listing_key": listing_key,
                "symbol": symbol,
                "series": series,
                "claimed_isin": bhav["isin"],
                "source_kind": SOURCE_IDENTITY_CLAIM_KIND_LEGACY_BHAVCOPY_ISIN,
                "source_entry_name": inner_name,
                "source_entry_sha256": inner_sha256,
                "source_row_number": bhav["row_number"],
                "status": SOURCE_IDENTITY_CLAIM_STATUS_SOURCE_CLAIMED_UNVERIFIED,
            }
            claim["claim_id"] = content_id(
                {
                    "schema": "nse-historical-archive-source-identity-claim/v1",
                    **claim,
                },
                length=64,
            )
            claims.append(claim)
            total_value = bhav["total_traded_value"]
            volume = bhav["traded_quantity"]
            average_price = (total_value / Decimal(volume)).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            turnover_lacs = (total_value / Decimal(100_000)).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            issue = {
                "session": session,
                "listing_key": f"NSE:{symbol}",
                "series": series,
                "udiff_financial_instrument_id": None,
                "security_master_financial_instrument_id": None,
                "security_master_source_identifier": None,
                "udiff_source_identifier": None,
                "status": IDENTITY_STATUS_UDIFF_AND_SECURITY_MASTER_EVIDENCE_UNAVAILABLE,
            }
            issue["issue_id"] = content_id(
                {"schema": "nse-historical-archive-identity-issue/v1", **issue},
                length=64,
            )
            identity_issues.append(issue)
            record: dict[str, object] = {
                "session": session,
                "listing_key": f"NSE:{symbol}",
                "symbol": symbol,
                "series": series,
                "financial_instrument_id": None,
                "security_master_financial_instrument_id": None,
                "security_source_record_id": None,
                "security_master_source_identifier": None,
                "udiff_source_identifier": None,
                "identity_status": IDENTITY_STATUS_UDIFF_AND_SECURITY_MASTER_EVIDENCE_UNAVAILABLE,
                "validated_isin": None,
                "normal_market_status": None,
                "normal_market_eligible": None,
                "permitted_to_trade": None,
                "delete_flag": None,
                "previous_close": bhav["previous_close"],
                "open": bhav["open"],
                "high": bhav["high"],
                "low": bhav["low"],
                "last": bhav["last"],
                "close": bhav["close"],
                "average_price": average_price,
                "volume": volume,
                "turnover_lacs": turnover_lacs,
                "trade_count": bhav["total_trades"],
                "delivery_quantity": mto["deliverable_quantity"],
                "delivery_percent": mto["percent"],
                "surveillance_indicators": {},
            }
            record["record_id"] = content_id(
                {"schema": "nse-historical-archive-eq-record/v1", **record},
                length=64,
            )
            records.append(record)
    if not records or len(records) > MAXIMUM_RECORDS:
        raise NseHistoricalArchiveIntegrityError(
            "historical archive EQ record count is outside limits"
        )
    return tuple(records), tuple(identity_issues), tuple(claims)


def parse_nse_historical_archive_entries(
    entries: Mapping[str, bytes],
    *,
    session: date,
    source_mode: str,
    source_container_sha256: str,
) -> ParsedNseHistoricalArchiveSession:
    if type(session) is not date:
        raise TypeError("session must be an exact date")
    expected = _expected_names(session)
    evidence_profile, present_names = _evidence_profile_for_names(
        tuple(entries),
        session=session,
    )
    if (
        set(entries) != set(present_names)
        or any(type(value) is not bytes for value in entries.values())
    ):
        raise NseHistoricalArchiveIntegrityError(
            "historical archive entry mapping is not exact"
        )

    legacy_names = _expected_legacy_names(session)
    if present_names == legacy_names:
        records, identity_issues, source_identity_claims = (
            _legacy_unreconciled_eq_records(
                entries[legacy_names[0]],
                entries[legacy_names[1]],
                session=session,
            )
        )
        records = tuple(records)
        identity_issues = tuple(identity_issues)
        source_identity_claims = tuple(source_identity_claims)
        source_entry_sha256 = tuple(
            (name, _sha256(entries[name])) for name in sorted(present_names)
        )
        normalized_payload: dict[str, object] = {
            "schema_version": NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION,
            "session": session,
            "exchange": "NSE",
            "series_scope": ("EQ",),
            "evidence_profile": evidence_profile,
            "missing_evidence": _EVIDENCE_PROFILE_MISSING[evidence_profile],
            "source_mode": source_mode,
            "source_container_sha256": source_container_sha256,
            "source_entry_sha256": source_entry_sha256,
            "security_master_source_schema_version": None,
            "security_master_header_sha256": None,
            "scope_exclusion_policy": "ALL_NON_EQ_ROWS_EXCLUDED",
            "reg1_row_count": None,
            "identity_issue_count": len(identity_issues),
            "identity_issues": identity_issues,
            "source_identity_claims": source_identity_claims,
            "collection_only": True,
            "actionable": False,
            "training_eligible": False,
            "knowledge_time_status": "MANUAL_HISTORICAL_IMPORT_UNVERIFIED",
            "records": records,
        }
        return ParsedNseHistoricalArchiveSession(
            session=session,
            source_mode=source_mode,
            source_container_sha256=source_container_sha256,
            source_entry_sha256=source_entry_sha256,
            normalized_payload=normalized_payload,
        )

    parser = NseDailyBundleParser()
    try:
        full = parser._parse_full_delivery(
            expected[0],
            _eq_only_csv(
                entries[expected[0]],
                series_field="SERIES",
                label="full Bhavcopy",
            ),
            session.strftime("%d%m%Y"),
        )
        udiff = (
            parser._parse_udiff(
                expected[1],
                _eq_only_udiff(entries[expected[1]]),
                session.strftime("%Y%m%d"),
            )
            if expected[1] in entries
            else None
        )
        reg1 = (
            parser._parse_reg1(
                expected[2],
                _canonicalize_reg1_header(entries[expected[2]]),
                session.strftime("%d%m%y"),
            )
            if expected[2] in entries
            else None
        )
        security_master = (
            NseCmSecurityMasterParser().parse_bytes(
                entries[expected[3]], original_filename=expected[3]
            )
            if expected[3] in entries
            else None
        )
    except (DailyReportError, ReferenceArtifactIntegrityError):
        raise NseHistoricalArchiveIntegrityError(
            "historical archive report validation failed"
        ) from None
    if any(
        report.disposition is not BundleEntryDisposition.SELECTED_VALIDATED
        for report in (full, udiff, reg1)
        if report is not None
    ):
        raise NseHistoricalArchiveIntegrityError(
            "historical archive core report disposition is invalid"
        )
    if any(
        report.claimed_report_date != session
        for report in (full, udiff, reg1)
        if report is not None
    ):
        raise NseHistoricalArchiveIntegrityError(
            "historical archive report date disagrees with its session"
        )
    if (
        security_master is not None
        and security_master.claimed_report_date != session
    ):
        raise NseHistoricalArchiveIntegrityError(
            "security-master date disagrees with its session"
        )
    if (
        security_master is not None
        and security_master.excluded_alternative_venue_count
    ):
        raise NseHistoricalArchiveIntegrityError(
            "historical archive contains the interoperability security master"
        )

    if evidence_profile == EVIDENCE_PROFILE_UNRECONCILED:
        records, identity_issues = _unreconciled_eq_records(full, session)
        records = tuple(records)
        identity_issues = tuple(identity_issues)
        source_entry_sha256 = tuple(
            (name, _sha256(entries[name])) for name in sorted(present_names)
        )
        normalized_payload: dict[str, object] = {
            "schema_version": NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION,
            "session": session,
            "exchange": "NSE",
            "series_scope": ("EQ",),
            "evidence_profile": evidence_profile,
            "missing_evidence": _EVIDENCE_PROFILE_MISSING[evidence_profile],
            "source_mode": source_mode,
            "source_container_sha256": source_container_sha256,
            "source_entry_sha256": source_entry_sha256,
            "security_master_source_schema_version": None,
            "security_master_header_sha256": None,
            "scope_exclusion_policy": "ALL_NON_EQ_ROWS_EXCLUDED",
            "reg1_row_count": None,
            "identity_issue_count": len(identity_issues),
            "identity_issues": identity_issues,
            "source_identity_claims": (),
            "collection_only": True,
            "actionable": False,
            "training_eligible": False,
            "knowledge_time_status": "MANUAL_HISTORICAL_IMPORT_UNVERIFIED",
            "records": records,
        }
        return ParsedNseHistoricalArchiveSession(
            session=session,
            source_mode=source_mode,
            source_container_sha256=source_container_sha256,
            source_entry_sha256=source_entry_sha256,
            normalized_payload=normalized_payload,
        )

    reconciled = _reconcile_eq_rows(full, udiff)
    if not reconciled or len(reconciled) > MAXIMUM_RECORDS:
        raise NseHistoricalArchiveIntegrityError(
            "historical archive EQ record count is outside limits"
        )
    security_by_key = (
        {
            (record.ticker_symbol, record.security_series): record
            for record in security_master.records
        }
        if security_master is not None
        else {}
    )
    surveillance_by_key = _surveillance_by_key(reg1) if reg1 is not None else {}
    records: list[dict[str, object]] = []
    identity_issues: list[dict[str, object]] = []
    for item in reconciled:
        symbol, series = item["key"]
        full_values = item["full"]
        udiff_values = item["udiff"]
        security = security_by_key.get((symbol, series))
        udiff_financial_instrument_id = int(udiff_values["FinInstrmId"])
        if security_master is None:
            identity_status = "SECURITY_MASTER_EVIDENCE_UNAVAILABLE"
        elif security is None:
            identity_status = "SECURITY_MASTER_MISSING"
        elif security.financial_instrument_id != udiff_financial_instrument_id:
            identity_status = "FINANCIAL_INSTRUMENT_ID_MISMATCH"
        elif security.raw_source_identifier != udiff_values["ISIN"]:
            identity_status = "SOURCE_IDENTIFIER_MISMATCH"
        else:
            identity_status = "MATCHED_SAME_SESSION"
        if identity_status != "MATCHED_SAME_SESSION":
            issue = {
                "session": session,
                "listing_key": f"NSE:{symbol}",
                "series": series,
                "udiff_financial_instrument_id": udiff_financial_instrument_id,
                "security_master_financial_instrument_id": (
                    None if security is None else security.financial_instrument_id
                ),
                "security_master_source_identifier": (
                    None if security is None else security.raw_source_identifier
                ),
                "udiff_source_identifier": udiff_values["ISIN"],
                "status": identity_status,
            }
            issue["issue_id"] = content_id(
                {
                    "schema": "nse-historical-archive-identity-issue/v1",
                    **issue,
                },
                length=64,
            )
            identity_issues.append(issue)
        delivery_quantity = (
            None if full_values["DELIV_QTY"] == "-" else int(full_values["DELIV_QTY"])
        )
        delivery_percent = (
            None
            if full_values["DELIV_PER"] == "-"
            else Decimal(full_values["DELIV_PER"])
        )
        record: dict[str, object] = {
            "session": session,
            "listing_key": f"NSE:{symbol}",
            "symbol": symbol,
            "series": series,
            "financial_instrument_id": udiff_financial_instrument_id,
            "security_master_financial_instrument_id": (
                None if security is None else security.financial_instrument_id
            ),
            "security_source_record_id": (
                None if security is None else security.source_record_id
            ),
            "security_master_source_identifier": (
                None if security is None else security.raw_source_identifier
            ),
            "udiff_source_identifier": udiff_values["ISIN"],
            "identity_status": identity_status,
            "validated_isin": (
                security.validated_isin
                if identity_status == "MATCHED_SAME_SESSION"
                else None
            ),
            "normal_market_status": (
                None if security is None else security.market_eligibility[0].status
            ),
            "normal_market_eligible": (
                None if security is None else security.market_eligibility[0].eligible
            ),
            "permitted_to_trade": (
                None if security is None else security.permitted_to_trade
            ),
            "delete_flag": None if security is None else security.delete_flag,
            "previous_close": Decimal(full_values["PREV_CLOSE"]),
            "open": Decimal(full_values["OPEN_PRICE"]),
            "high": Decimal(full_values["HIGH_PRICE"]),
            "low": Decimal(full_values["LOW_PRICE"]),
            "last": Decimal(full_values["LAST_PRICE"]),
            "close": Decimal(full_values["CLOSE_PRICE"]),
            "average_price": Decimal(full_values["AVG_PRICE"]),
            "volume": int(full_values["TTL_TRD_QNTY"]),
            "turnover_lacs": Decimal(full_values["TURNOVER_LACS"]),
            "trade_count": int(full_values["NO_OF_TRADES"]),
            "delivery_quantity": delivery_quantity,
            "delivery_percent": delivery_percent,
            "surveillance_indicators": surveillance_by_key.get((symbol, series), {}),
        }
        record["record_id"] = content_id(
            {
                "schema": "nse-historical-archive-eq-record/v1",
                **record,
            },
            length=64,
        )
        records.append(record)

    source_entry_sha256 = tuple(
        (name, _sha256(entries[name])) for name in sorted(present_names)
    )
    normalized_payload: dict[str, object] = {
        "schema_version": NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION,
        "session": session,
        "exchange": "NSE",
        "series_scope": ("EQ",),
        "evidence_profile": evidence_profile,
        "missing_evidence": _EVIDENCE_PROFILE_MISSING[evidence_profile],
        "source_mode": source_mode,
        "source_container_sha256": source_container_sha256,
        "source_entry_sha256": source_entry_sha256,
        "security_master_source_schema_version": (
            None
            if security_master is None
            else security_master.source_schema_version
        ),
        "security_master_header_sha256": (
            None if security_master is None else security_master.header_sha256
        ),
        "scope_exclusion_policy": "ALL_NON_EQ_ROWS_EXCLUDED",
        "reg1_row_count": None if reg1 is None else reg1.row_count,
        "identity_issue_count": len(identity_issues),
        "identity_issues": tuple(identity_issues),
        "source_identity_claims": (),
        "collection_only": True,
        "actionable": False,
        "training_eligible": False,
        "knowledge_time_status": "MANUAL_HISTORICAL_IMPORT_UNVERIFIED",
        "records": tuple(records),
    }
    return ParsedNseHistoricalArchiveSession(
        session=session,
        source_mode=source_mode,
        source_container_sha256=source_container_sha256,
        source_entry_sha256=source_entry_sha256,
        normalized_payload=normalized_payload,
    )


def parse_nse_historical_archive_bytes(
    payload: bytes,
    *,
    original_filename: str,
) -> ParsedNseHistoricalArchiveSession:
    session = _session_from_archive_name(original_filename)
    entries = _extract_archive_entries(payload, session=session)
    return parse_nse_historical_archive_entries(
        entries,
        session=session,
        source_mode="OFFICIAL_OUTER_ZIP",
        source_container_sha256=_sha256(payload),
    )


def _put_session(
    parsed: ParsedNseHistoricalArchiveSession,
    *,
    store: LocalMarketSnapshotStore,
    observed_at: datetime,
) -> StoredMarketSnapshot:
    if type(observed_at) is not datetime or observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("observed_at must be a timezone-aware datetime")
    return store.put(
        dataset=NSE_HISTORICAL_ARCHIVE_EQ_DATASET,
        selection_key=parsed.session.isoformat(),
        provider=NSE_HISTORICAL_ARCHIVE_PROVIDER,
        provider_version=NSE_HISTORICAL_ARCHIVE_IMPORTER_VERSION,
        observed_at=observed_at.astimezone(timezone.utc),
        normalized_payload=parsed.normalized_payload,
    )


def import_nse_historical_archive_path(
    path: Path,
    *,
    store: LocalMarketSnapshotStore,
    observed_at: datetime,
) -> StoredMarketSnapshot:
    path = Path(path)
    try:
        payload = read_stable_regular_file(path, maximum_bytes=MAXIMUM_ARCHIVE_BYTES)
    except (FileSafetyError, OSError):
        raise NseHistoricalArchiveIntegrityError(
            "historical archive could not be read safely"
        ) from None
    parsed = parse_nse_historical_archive_bytes(
        payload,
        original_filename=path.name,
    )
    return _put_session(parsed, store=store, observed_at=observed_at)


def _read_nse_historical_entry_directory(
    path: Path,
) -> tuple[date, dict[str, bytes]]:
    path = Path(path)
    if not path.is_dir() or path.is_symlink() or _SESSION_DIRECTORY.fullmatch(path.name) is None:
        raise NseHistoricalArchiveIntegrityError(
            "historical entry directory is invalid"
        )
    session = date.fromisoformat(path.name)
    children = tuple(path.iterdir())
    try:
        _, selected_names = _evidence_profile_for_names(
            tuple(child.name for child in children),
            session=session,
        )
    except NseHistoricalArchiveIntegrityError:
        raise NseHistoricalArchiveIntegrityError(
            "historical entry directory is not exact"
        ) from None
    if {child.name for child in children} != set(selected_names) or any(
        child.is_symlink() or not child.is_file() for child in children
    ):
        raise NseHistoricalArchiveIntegrityError(
            "historical entry directory is not exact"
        )
    try:
        entries = {
            name: read_stable_regular_file(
                path / name, maximum_bytes=MAXIMUM_ENTRY_BYTES
            )
            for name in selected_names
        }
    except (FileSafetyError, OSError):
        raise NseHistoricalArchiveIntegrityError(
            "historical entry directory could not be read safely"
        ) from None
    return session, entries


def _parse_nse_historical_entry_directory(
    path: Path,
) -> ParsedNseHistoricalArchiveSession:
    session, entries = _read_nse_historical_entry_directory(path)
    return parse_nse_historical_archive_entries(
        entries,
        session=session,
        source_mode="VALIDATED_EXTRACTED_ENTRY_SET",
        source_container_sha256=_source_set_id(entries),
    )


def import_nse_historical_entry_directory(
    path: Path,
    *,
    store: LocalMarketSnapshotStore,
    observed_at: datetime,
) -> StoredMarketSnapshot:
    parsed = _parse_nse_historical_entry_directory(path)
    return _put_session(parsed, store=store, observed_at=observed_at)


def _import_range_session(
    arguments: tuple[str, str, str, str, str],
) -> ImportedNseHistoricalArchiveSession:
    session_text, staging_path_text, archive_path_text, store_root_text, observed_text = (
        arguments
    )
    session = date.fromisoformat(session_text)
    staging_path = Path(staging_path_text)
    archive_path = Path(archive_path_text)
    store = LocalMarketSnapshotStore(Path(store_root_text))
    observed_at = datetime.fromisoformat(observed_text)
    if archive_path.is_file() and not archive_path.is_symlink():
        try:
            archive_payload = read_stable_regular_file(
                archive_path, maximum_bytes=MAXIMUM_ARCHIVE_BYTES
            )
        except (FileSafetyError, OSError):
            raise NseHistoricalArchiveIntegrityError(
                "historical archive could not be read safely"
            ) from None
        archive_entries = _extract_archive_entries(
            archive_payload, session=session
        )
        landing_session, landing_entries = _read_nse_historical_entry_directory(
            staging_path
        )
        archive_hashes = {
            name: _sha256(payload) for name, payload in archive_entries.items()
        }
        landing_hashes = {
            name: _sha256(payload) for name, payload in landing_entries.items()
        }
        if landing_session != session or landing_hashes != archive_hashes:
            raise NseHistoricalArchiveIntegrityError(
                "archive and extracted landing evidence disagree"
            )
        parsed = parse_nse_historical_archive_entries(
            archive_entries,
            session=session,
            source_mode="OFFICIAL_OUTER_ZIP",
            source_container_sha256=_sha256(archive_payload),
        )
        stored = _put_session(parsed, store=store, observed_at=observed_at)
    else:
        stored = import_nse_historical_entry_directory(
            staging_path,
            store=store,
            observed_at=observed_at,
        )
    return ImportedNseHistoricalArchiveSession(
        session=session,
        snapshot_id=stored.manifest.snapshot_id,
        record_count=stored.manifest.record_count,
        source_container_sha256=str(
            stored.normalized_payload["source_container_sha256"]
        ),
        identity_issue_count=int(
            stored.normalized_payload["identity_issue_count"]
        ),
        evidence_profile=str(stored.normalized_payload["evidence_profile"]),
    )


def import_nse_historical_range(
    *,
    staging_root: Path,
    archive_root: Path,
    store: LocalMarketSnapshotStore,
    start: date,
    end: date,
    observed_at: datetime,
    workers: int = 1,
) -> tuple[tuple[ImportedNseHistoricalArchiveSession, ...], StoredMarketSnapshot]:
    if type(start) is not date or type(end) is not date or start > end:
        raise ValueError("historical import range is invalid")
    if type(workers) is not int or not 1 <= workers <= 16:
        raise ValueError("workers must be an integer from 1 to 16")
    if (
        type(observed_at) is not datetime
        or observed_at.tzinfo is None
        or observed_at.utcoffset() is None
    ):
        raise ValueError("observed_at must be a timezone-aware datetime")
    staging_root = Path(staging_root)
    archive_root = Path(archive_root)
    if not staging_root.is_dir() or staging_root.is_symlink():
        raise NseHistoricalArchiveIntegrityError("staging root is invalid")
    work: list[tuple[str, str, str, str, str]] = []
    for session_path in sorted(staging_root.iterdir(), key=lambda value: value.name):
        if (
            not session_path.is_dir()
            or session_path.is_symlink()
            or _SESSION_DIRECTORY.fullmatch(session_path.name) is None
        ):
            continue
        session = date.fromisoformat(session_path.name)
        if not start <= session <= end:
            continue
        archive_name = f"Reports-Archives-Multiple-{session:%d%m%Y}.zip"
        archive_path = archive_root / session.isoformat() / archive_name
        work.append(
            (
                session.isoformat(),
                str(session_path),
                str(archive_path),
                str(store.root),
                observed_at.isoformat(),
            )
        )
    if workers == 1:
        sessions = [_import_range_session(value) for value in work]
    else:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            sessions = list(executor.map(_import_range_session, work))
    if not sessions:
        raise NseHistoricalArchiveIntegrityError(
            "historical import range contains no staged sessions"
        )
    index_records = tuple(
        {
            "session": value.session,
            "snapshot_id": value.snapshot_id,
            "record_count": value.record_count,
            "source_container_sha256": value.source_container_sha256,
            "identity_issue_count": value.identity_issue_count,
            "evidence_profile": value.evidence_profile,
        }
        for value in sessions
    )
    identity_issue_count = sum(
        value.identity_issue_count for value in sessions
    )
    evidence_profile_counts = {
        profile: sum(value.evidence_profile == profile for value in sessions)
        for profile in _EVIDENCE_PROFILE_MISSING
    }
    index_payload = {
        "schema_version": NSE_HISTORICAL_ARCHIVE_INDEX_SCHEMA_VERSION,
        "range_start": start,
        "range_end": end,
        "collection_only": True,
        "actionable": False,
        "training_eligible": False,
        "identity_issue_count": identity_issue_count,
        "identity_quarantined_session_count": sum(
            value.identity_issue_count > 0 for value in sessions
        ),
        "incomplete_evidence_session_count": sum(
            value.evidence_profile != EVIDENCE_PROFILE_COMPLETE
            for value in sessions
        ),
        "evidence_profile_counts": evidence_profile_counts,
        "records": index_records,
    }
    index = store.put(
        dataset=NSE_HISTORICAL_ARCHIVE_INDEX_DATASET,
        selection_key=f"{start.isoformat()}:{end.isoformat()}",
        provider=NSE_HISTORICAL_ARCHIVE_PROVIDER,
        provider_version=NSE_HISTORICAL_ARCHIVE_IMPORTER_VERSION,
        observed_at=observed_at.astimezone(timezone.utc),
        normalized_payload=index_payload,
    )
    return tuple(sessions), index
