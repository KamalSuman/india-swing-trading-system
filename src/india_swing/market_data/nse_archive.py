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
from india_swing.daily_reports.models import BundleEntryDisposition
from india_swing.daily_reports.parser import NseDailyBundleParser
from india_swing.identity import content_id
from india_swing.reference_data.security_master import NseCmSecurityMasterParser

from .snapshot_store import LocalMarketSnapshotStore, StoredMarketSnapshot


NSE_HISTORICAL_ARCHIVE_EQ_DATASET = "nse-historical-archive-eq"
NSE_HISTORICAL_ARCHIVE_INDEX_DATASET = "nse-historical-archive-eq-index"
NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION = "nse-historical-archive-eq-session/v1"
NSE_HISTORICAL_ARCHIVE_INDEX_SCHEMA_VERSION = (
    "nse-historical-archive-eq-index/v2"
)
NSE_HISTORICAL_ARCHIVE_IMPORTER_VERSION = "nse-archive-eq-importer/v2"
NSE_HISTORICAL_ARCHIVE_PROVIDER = "NSE_ARCHIVE"
MAXIMUM_ARCHIVE_BYTES = 128 * 1024 * 1024
MAXIMUM_ENTRY_BYTES = 64 * 1024 * 1024
MAXIMUM_RECORDS = 20_000

_ARCHIVE_NAME = re.compile(r"Reports-Archives-Multiple-(\d{8})\.zip\Z")
_SESSION_DIRECTORY = re.compile(r"\d{4}-\d{2}-\d{2}\Z")


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
            or len(self.source_entry_sha256) != 4
            or self.source_entry_sha256
            != tuple(sorted(self.source_entry_sha256))
        ):
            raise ValueError("source entry hashes must be four ordered pairs")
        if not isinstance(self.normalized_payload, Mapping):
            raise TypeError("normalized payload must be a mapping")


@dataclass(frozen=True, slots=True)
class ImportedNseHistoricalArchiveSession:
    session: date
    snapshot_id: str
    record_count: int
    source_container_sha256: str
    identity_issue_count: int


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _expected_names(session: date) -> tuple[str, str, str, str]:
    return (
        f"sec_bhavdata_full_{session:%d%m%Y}.csv",
        f"BhavCopy_NSE_CM_0_0_0_{session:%Y%m%d}_F_0000.csv.zip",
        f"REG1_IND{session:%d%m%y}.csv",
        f"NSE_CM_security_{session:%d%m%Y}.csv.gz",
    )


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


def _eq_only_csv(payload: bytes, *, series_field: str, label: str) -> bytes:
    try:
        text = payload.decode("utf-8-sig", errors="strict")
        rows = list(csv.reader(io.StringIO(text, newline=""), strict=True))
    except (UnicodeDecodeError, csv.Error):
        raise NseHistoricalArchiveIntegrityError(
            f"{label} is not strict UTF-8 CSV"
        ) from None
    if not rows:
        raise NseHistoricalArchiveIntegrityError(f"{label} is empty")
    header = tuple(value.strip() for value in rows[0])
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
    )
    target = io.BytesIO()
    with zipfile.ZipFile(target, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(inner_name, filtered)
    return target.getvalue()


def _extract_archive_entries(
    payload: bytes,
    *,
    session: date,
) -> dict[str, bytes]:
    if type(payload) is not bytes or not payload:
        raise NseHistoricalArchiveIntegrityError("historical archive is empty")
    if len(payload) > MAXIMUM_ARCHIVE_BYTES:
        raise NseHistoricalArchiveIntegrityError("historical archive exceeds its size limit")
    expected = _expected_names(session)
    parser = NseDailyBundleParser()
    try:
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            infos = archive.infolist()
            names = tuple(info.filename for info in infos)
            if len(infos) != 4 or set(names) != set(expected):
                raise NseHistoricalArchiveIntegrityError(
                    "historical archive entry set is not exact"
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
            return {name: archive.read(name) for name in expected}
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
    if set(entries) != set(expected) or any(type(value) is not bytes for value in entries.values()):
        raise NseHistoricalArchiveIntegrityError(
            "historical archive entry mapping is not exact"
        )
    parser = NseDailyBundleParser()
    full = parser._parse_full_delivery(
        expected[0],
        _eq_only_csv(
            entries[expected[0]],
            series_field="SERIES",
            label="full Bhavcopy",
        ),
        session.strftime("%d%m%Y"),
    )
    udiff = parser._parse_udiff(
        expected[1],
        _eq_only_udiff(entries[expected[1]]),
        session.strftime("%Y%m%d"),
    )
    reg1 = parser._parse_reg1(
        expected[2], entries[expected[2]], session.strftime("%d%m%y")
    )
    security_master = NseCmSecurityMasterParser().parse_bytes(
        entries[expected[3]], original_filename=expected[3]
    )
    if any(
        report.disposition is not BundleEntryDisposition.SELECTED_VALIDATED
        for report in (full, udiff, reg1)
    ):
        raise NseHistoricalArchiveIntegrityError(
            "historical archive core report disposition is invalid"
        )
    if any(report.claimed_report_date != session for report in (full, udiff, reg1)):
        raise NseHistoricalArchiveIntegrityError(
            "historical archive report date disagrees with its session"
        )
    if security_master.claimed_report_date != session:
        raise NseHistoricalArchiveIntegrityError(
            "security-master date disagrees with its session"
        )
    if security_master.excluded_alternative_venue_count:
        raise NseHistoricalArchiveIntegrityError(
            "historical archive contains the interoperability security master"
        )

    reconciled = _reconcile_eq_rows(full, udiff)
    if not reconciled or len(reconciled) > MAXIMUM_RECORDS:
        raise NseHistoricalArchiveIntegrityError(
            "historical archive EQ record count is outside limits"
        )
    security_by_key = {
        (record.ticker_symbol, record.security_series): record
        for record in security_master.records
    }
    surveillance_by_key = _surveillance_by_key(reg1)
    records: list[dict[str, object]] = []
    identity_issues: list[dict[str, object]] = []
    for item in reconciled:
        symbol, series = item["key"]
        full_values = item["full"]
        udiff_values = item["udiff"]
        security = security_by_key.get((symbol, series))
        udiff_financial_instrument_id = int(udiff_values["FinInstrmId"])
        if security is None:
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
        (name, _sha256(entries[name])) for name in sorted(expected)
    )
    normalized_payload: dict[str, object] = {
        "schema_version": NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION,
        "session": session,
        "exchange": "NSE",
        "series_scope": ("EQ",),
        "source_mode": source_mode,
        "source_container_sha256": source_container_sha256,
        "source_entry_sha256": source_entry_sha256,
        "security_master_source_schema_version": security_master.source_schema_version,
        "security_master_header_sha256": security_master.header_sha256,
        "scope_exclusion_policy": "ALL_NON_EQ_ROWS_EXCLUDED",
        "reg1_row_count": reg1.row_count,
        "identity_issue_count": len(identity_issues),
        "identity_issues": tuple(identity_issues),
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
    expected = _expected_names(session)
    children = tuple(path.iterdir())
    if {child.name for child in children} != set(expected) or any(
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
            for name in expected
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
        }
        for value in sessions
    )
    identity_issue_count = sum(
        value.identity_issue_count for value in sessions
    )
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
