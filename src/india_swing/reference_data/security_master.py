from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import zlib
from collections import Counter
from datetime import date, datetime
from pathlib import PurePath

from india_swing.identity import content_id

from .models import (
    NSE_CM_SECURITY_DATASET,
    NSE_CM_SECURITY_SOURCE_SCHEMA_VERSION,
    MarketEligibility,
    NseCmSecurityRecord,
    ParsedNseCmSecurityMaster,
    ReferenceArtifactIntegrityError,
    SourceRowDisposition,
    validated_isin_or_none,
)


NSE_CM_MII_SECURITY_HEADER = (
    "FinInstrmId",
    "TckrSymb",
    "SctySrs",
    "FinInstrmNm",
    "ISIN",
    "NewBrdLotQty",
    "ParVal",
    "SctyTpFlg",
    "BidIntrvl",
    "TrckgInd",
    "CallAuctnInd",
    "BookClsrStartDt",
    "BookClsrEndDt",
    "NoDlvryStartDt",
    "NoDlvryEndDt",
    "IssdCptl",
    "PrtdToTrad",
    "PricRg",
    "SctyStsNrmlMkt",
    "ElgbltyNrmlMkt",
    "SctyStsOddLotMkt",
    "ElgbltyOddLotMkt",
    "SctyStsRETDBTMkt",
    "ElgbltyRETDBTMkt",
    "SctyStsAuctnMkt",
    "ElgbltyAuctnMkt",
    "SctyStsAddtlMkt1",
    "ElgbltyAddtlMkt1",
    "SctyStsAddtlMkt2",
    "ElgbltyAddtlMkt2",
    "IsseDt",
    "FrstPmtDt",
    "MtrtyDt",
    "MaxTradQtyPctg",
    "ListgDt",
    "RmvlDt",
    "RadmssnDt",
    "RcrdDt",
    "IndxPrtcptnInd",
    "AllOrNn",
    "MinFill",
    "SttlmTp",
    "Dvdd",
    "Rghts",
    "Bns",
    "Intrst",
    "AGMtg",
    "EGMtg",
    "MktMakrSprd",
    "MktMakrMinQty",
    "AddtlInf",
    "UpdDt",
    "DelFlg",
    "SpclExDt",
    "Xchg",
    "UnqPdctIdr",
    "SctyTp",
    "TickSz",
    "Rsvd01",
    "Sts",
    "ExDvddDt",
    "ExBnsDt",
    "ExRghtsDt",
    "FinInstrmTp",
    "InstrmTp",
    "TradgPrtd",
    "BuyBckInd",
    "TradToTradInd",
    "Indx",
    "IndxInstrm",
    "FinInstrmAttrbts",
    "MinLot",
    "UndrlygInstrmAsstClss",
    "UndrlygInstrm",
    "UndrlygFinInstrmId",
    "BlckDealAllwdFlg",
    "InstrmNm",
    "MktTpAndId",
    "UnitOfMeasr",
    "PricQtQty",
    "PricRgTp",
    "MaxPric",
    "MinPric",
    "SttlmMtd",
    "InitlMrgnTp",
    "BuyInitlMrgnRate",
    "IssePric",
    "MaxSnglTxnQty",
    "MaxSnglTxnVal",
    "AsstClss",
    "PricNmrtr",
    "Spcfctn",
    "PricDnmtr",
    "GnlNmrtr",
    "GnlDnmtr",
    "LotNmrtr",
    "LotDnmtr",
    "DcmlstnPric",
    "SrsSttlmTp",
    "FreeFltCptl",
    "SellInitlMrgnRate",
    "RatgDtls",
    "FinInstrmClssfctn",
    "SpclMrgnTp",
    "BuySpclMrgnRat",
    "SellSpclMrgnRat",
    "PreOpnAllwdFlg",
    "ClssfctnTp",
    "MtchgCrit",
    "ValMtd",
    "SLBMElgblty",
    "Sgmt",
    "Ccy",
    "SttlmCcy",
    "Rsvd02",
    "Rsvd03",
    "Rsvd04",
    "Rsvd05",
    "Rsvd06",
    "Rsvd07",
)

NSE_CM_MII_SECURITY_HEADER_INDEX = {
    name: index for index, name in enumerate(NSE_CM_MII_SECURITY_HEADER)
}
NSE_CM_MII_SECURITY_HEADER_SHA256 = hashlib.sha256(
    ",".join(NSE_CM_MII_SECURITY_HEADER).encode("utf-8")
).hexdigest()

_FILENAME = re.compile(r"NSE_CM_security_(\d{8})\.csv\.gz\Z")
_TICKER = re.compile(r"[A-Z0-9&-]{1,10}\Z")
_SERIES = re.compile(r"[A-Z0-9]{1,2}\Z")
_SOURCE_IDENTIFIER = re.compile(r"[A-Z0-9]{1,12}\Z")
_INTEGER = re.compile(r"(?:0|[1-9][0-9]*)\Z")

_MARKET_PAIRS = (
    ("SctyStsNrmlMkt", "ElgbltyNrmlMkt"),
    ("SctyStsOddLotMkt", "ElgbltyOddLotMkt"),
    ("SctyStsRETDBTMkt", "ElgbltyRETDBTMkt"),
    ("SctyStsAuctnMkt", "ElgbltyAuctnMkt"),
    ("SctyStsAddtlMkt1", "ElgbltyAddtlMkt1"),
    ("SctyStsAddtlMkt2", "ElgbltyAddtlMkt2"),
)

# These ISO-extension fields are blank in the current NSE-only Annexure 10
# security file. A nonblank value indicates a different or changed contract and
# must not be silently interpreted through the legacy equity flags above.
NSE_CM_MII_CURRENTLY_BLANK_SCOPE_FIELDS = (
    "Xchg",
    "Sgmt",
    "SctyTp",
    "FinInstrmTp",
    "InstrmTp",
)


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _field(row: tuple[str, ...], name: str) -> str:
    return row[NSE_CM_MII_SECURITY_HEADER_INDEX[name]]


def _integer_field(
    row: tuple[str, ...],
    name: str,
    *,
    minimum: int = 0,
    maximum: int = 2_147_483_647,
) -> int:
    raw = _field(row, name)
    if _INTEGER.fullmatch(raw) is None:
        raise ReferenceArtifactIntegrityError(f"{name} is not an unsigned integer")
    value = int(raw)
    if not minimum <= value <= maximum:
        raise ReferenceArtifactIntegrityError(f"{name} is outside its supported range")
    return value


def _canonical_row_bytes(row: tuple[str, ...]) -> bytes:
    return json.dumps(
        row,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _bounded_gzip_decompress(payload: bytes, *, maximum_output_bytes: int) -> bytes:
    if not payload.startswith(b"\x1f\x8b\x08"):
        raise ReferenceArtifactIntegrityError("source is not a gzip stream")
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
                    raise ReferenceArtifactIntegrityError(
                        "decompressed security master exceeds the size limit"
                    )
                output.extend(decompressor.decompress(chunk, remaining))
                if len(output) > maximum_output_bytes:
                    raise ReferenceArtifactIntegrityError(
                        "decompressed security master exceeds the size limit"
                    )
                chunk = decompressor.unconsumed_tail
            if decompressor.eof:
                if decompressor.unused_data or position != len(payload):
                    raise ReferenceArtifactIntegrityError(
                        "concatenated or trailing gzip data is forbidden"
                    )
                break
        remaining = maximum_output_bytes + 1 - len(output)
        output.extend(decompressor.flush(max(remaining, 0)))
    except zlib.error as exc:
        raise ReferenceArtifactIntegrityError("invalid or corrupt gzip stream") from exc
    if not decompressor.eof:
        raise ReferenceArtifactIntegrityError("truncated gzip stream")
    if len(output) > maximum_output_bytes:
        raise ReferenceArtifactIntegrityError(
            "decompressed security master exceeds the size limit"
        )
    return bytes(output)


class NseCmSecurityMasterParser:
    """Strict parser for the manually downloaded NSE CM MII security master."""

    def __init__(
        self,
        *,
        maximum_compressed_bytes: int = 32 * 1024 * 1024,
        maximum_uncompressed_bytes: int = 128 * 1024 * 1024,
        maximum_rows: int = 500_000,
        maximum_field_characters: int = 8_192,
    ) -> None:
        limits = (
            maximum_compressed_bytes,
            maximum_uncompressed_bytes,
            maximum_rows,
            maximum_field_characters,
        )
        if any(type(value) is not int or value <= 0 for value in limits):
            raise ValueError("parser limits must be positive integers")
        self.maximum_compressed_bytes = maximum_compressed_bytes
        self.maximum_uncompressed_bytes = maximum_uncompressed_bytes
        self.maximum_rows = maximum_rows
        self.maximum_field_characters = maximum_field_characters

    @staticmethod
    def claimed_report_date_from_filename(original_filename: str) -> date:
        if not isinstance(original_filename, str):
            raise TypeError("original filename must be text")
        if PurePath(original_filename).name != original_filename:
            raise ReferenceArtifactIntegrityError("original filename must be a basename")
        match = _FILENAME.fullmatch(original_filename)
        if match is None:
            raise ReferenceArtifactIntegrityError(
                "expected NSE_CM_security_DDMMYYYY.csv.gz"
            )
        try:
            return datetime.strptime(match.group(1), "%d%m%Y").date()
        except ValueError as exc:
            raise ReferenceArtifactIntegrityError(
                "security-master filename contains an invalid date"
            ) from exc

    def parse_bytes(
        self,
        payload: bytes,
        *,
        original_filename: str,
    ) -> ParsedNseCmSecurityMaster:
        if not isinstance(payload, bytes):
            raise TypeError("security-master payload must be bytes")
        if not payload:
            raise ReferenceArtifactIntegrityError("security-master payload is empty")
        if len(payload) > self.maximum_compressed_bytes:
            raise ReferenceArtifactIntegrityError(
                "compressed security master exceeds the size limit"
            )
        claimed_report_date = self.claimed_report_date_from_filename(
            original_filename
        )
        uncompressed = _bounded_gzip_decompress(
            payload,
            maximum_output_bytes=self.maximum_uncompressed_bytes,
        )
        try:
            text = uncompressed.decode("utf-8-sig", errors="strict")
        except UnicodeDecodeError as exc:
            raise ReferenceArtifactIntegrityError(
                "security master is not strict UTF-8 text"
            ) from exc
        if "\x00" in text:
            raise ReferenceArtifactIntegrityError("security master contains a NUL byte")

        reader = csv.reader(io.StringIO(text, newline=""), strict=True)
        try:
            header = tuple(next(reader))
        except (StopIteration, csv.Error) as exc:
            raise ReferenceArtifactIntegrityError(
                "security master has no readable header"
            ) from exc
        if header != NSE_CM_MII_SECURITY_HEADER:
            raise ReferenceArtifactIntegrityError(
                "unsupported NSE CM MII security-master header"
            )

        records: list[NseCmSecurityRecord] = []
        instrument_ids: set[int] = set()
        listing_keys: set[tuple[str, str]] = set()
        row_hashes: list[str] = []
        try:
            for source_row_number, raw_row in enumerate(reader, start=2):
                if source_row_number - 1 > self.maximum_rows:
                    raise ReferenceArtifactIntegrityError(
                        "security master exceeds the row limit"
                    )
                row = tuple(raw_row)
                if len(row) != len(NSE_CM_MII_SECURITY_HEADER):
                    raise ReferenceArtifactIntegrityError(
                        "security-master row field count does not match its header"
                    )
                if any(len(value) > self.maximum_field_characters for value in row):
                    raise ReferenceArtifactIntegrityError(
                        "security-master field exceeds the character limit"
                    )
                if any(
                    any(ord(character) < 32 and character not in "\t" for character in value)
                    for value in row
                ):
                    raise ReferenceArtifactIntegrityError(
                        "security-master field contains a control character"
                    )
                record = self._record(
                    row,
                    source_row_number,
                    claimed_report_date,
                )
                listing_key = (record.ticker_symbol, record.security_series)
                if record.financial_instrument_id in instrument_ids:
                    raise ReferenceArtifactIntegrityError(
                        "duplicate financial instrument ID in security master"
                    )
                if listing_key in listing_keys:
                    raise ReferenceArtifactIntegrityError(
                        "duplicate symbol-series in security master"
                    )
                instrument_ids.add(record.financial_instrument_id)
                listing_keys.add(listing_key)
                row_hashes.append(record.normalized_row_sha256)
                records.append(record)
        except csv.Error as exc:
            raise ReferenceArtifactIntegrityError("malformed security-master CSV") from exc
        if not records:
            raise ReferenceArtifactIntegrityError("security master contains no data records")

        disposition_counts = Counter(record.disposition for record in records)
        ordered_row_digest = _sha256("\n".join(row_hashes).encode("ascii"))
        return ParsedNseCmSecurityMaster(
            original_filename=original_filename,
            claimed_report_date=claimed_report_date,
            source_schema_version=NSE_CM_SECURITY_SOURCE_SCHEMA_VERSION,
            header=NSE_CM_MII_SECURITY_HEADER,
            header_sha256=NSE_CM_MII_SECURITY_HEADER_SHA256,
            raw_sha256=_sha256(payload),
            uncompressed_sha256=_sha256(uncompressed),
            compressed_byte_count=len(payload),
            uncompressed_byte_count=len(uncompressed),
            records=tuple(records),
            ordered_row_digest=ordered_row_digest,
            retained_unverified_equity_count=disposition_counts[
                SourceRowDisposition.RETAINED_UNVERIFIED_EQUITY
            ],
            excluded_non_equity_count=disposition_counts[
                SourceRowDisposition.EXCLUDED_NON_EQUITY
            ],
            excluded_test_security_count=disposition_counts[
                SourceRowDisposition.EXCLUDED_TEST_SECURITY
            ],
            excluded_alternative_venue_count=disposition_counts[
                SourceRowDisposition.EXCLUDED_ALTERNATIVE_VENUE
            ],
        )

    @staticmethod
    def _record(
        row: tuple[str, ...],
        source_row_number: int,
        claimed_report_date: date,
    ) -> NseCmSecurityRecord:
        financial_instrument_id = _integer_field(
            row,
            "FinInstrmId",
            minimum=1,
        )
        ticker_symbol = _field(row, "TckrSymb")
        security_series = _field(row, "SctySrs")
        instrument_name = _field(row, "FinInstrmNm")
        raw_source_identifier = _field(row, "ISIN")
        if _TICKER.fullmatch(ticker_symbol) is None:
            raise ReferenceArtifactIntegrityError("invalid ticker symbol")
        if _SERIES.fullmatch(security_series) is None:
            raise ReferenceArtifactIntegrityError("invalid security series")
        if not instrument_name or len(instrument_name) > 25:
            raise ReferenceArtifactIntegrityError("invalid instrument name")
        if _SOURCE_IDENTIFIER.fullmatch(raw_source_identifier) is None:
            raise ReferenceArtifactIntegrityError("invalid ISIN/source identifier")

        for field_name in NSE_CM_MII_CURRENTLY_BLANK_SCOPE_FIELDS:
            if _field(row, field_name):
                raise ReferenceArtifactIntegrityError(
                    f"unexpected value in currently blank scope field {field_name}"
                )

        board_lot_quantity = _integer_field(
            row,
            "NewBrdLotQty",
            minimum=1,
            maximum=999_999_999,
        )
        security_type_flag = _integer_field(row, "SctyTpFlg", maximum=4)
        bid_interval_paise = _integer_field(
            row,
            "BidIntrvl",
            minimum=1,
            maximum=1_000_000,
        )
        call_auction_indicator = _integer_field(row, "CallAuctnInd", maximum=5)
        permitted_to_trade = _integer_field(row, "PrtdToTrad", maximum=2)
        market_eligibility = tuple(
            MarketEligibility(
                status=_integer_field(row, status_name, minimum=1, maximum=6),
                eligible=bool(_integer_field(row, eligibility_name, maximum=1)),
            )
            for status_name, eligibility_name in _MARKET_PAIRS
        )
        listing_timestamp = _integer_field(row, "ListgDt")
        removal_timestamp = _integer_field(row, "RmvlDt")
        readmission_timestamp = _integer_field(row, "RadmssnDt")
        delete_flag = _field(row, "DelFlg")
        if delete_flag not in ("N", "Y"):
            raise ReferenceArtifactIntegrityError("invalid delete flag")

        normalized_row_sha256 = _sha256(_canonical_row_bytes(row))
        source_record_id = content_id(
            {
                "dataset": NSE_CM_SECURITY_DATASET,
                "claimed_report_date": claimed_report_date,
                "source_row_number": source_row_number,
                "normalized_row_sha256": normalized_row_sha256,
            },
            length=64,
        )
        if ticker_symbol.endswith("NSETEST"):
            disposition = SourceRowDisposition.EXCLUDED_TEST_SECURITY
        elif permitted_to_trade == 2:
            disposition = SourceRowDisposition.EXCLUDED_ALTERNATIVE_VENUE
        elif security_type_flag != 0:
            disposition = SourceRowDisposition.EXCLUDED_NON_EQUITY
        else:
            disposition = SourceRowDisposition.RETAINED_UNVERIFIED_EQUITY
        return NseCmSecurityRecord(
            source_row_number=source_row_number,
            source_record_id=source_record_id,
            normalized_row_sha256=normalized_row_sha256,
            financial_instrument_id=financial_instrument_id,
            ticker_symbol=ticker_symbol,
            security_series=security_series,
            instrument_name=instrument_name,
            raw_source_identifier=raw_source_identifier,
            validated_isin=validated_isin_or_none(raw_source_identifier),
            board_lot_quantity=board_lot_quantity,
            security_type_flag=security_type_flag,
            bid_interval_paise=bid_interval_paise,
            call_auction_indicator=call_auction_indicator,
            permitted_to_trade=permitted_to_trade,
            market_eligibility=market_eligibility,
            listing_timestamp=listing_timestamp,
            removal_timestamp=removal_timestamp,
            readmission_timestamp=readmission_timestamp,
            delete_flag=delete_flag,
            disposition=disposition,
            raw_fields=row,
        )
