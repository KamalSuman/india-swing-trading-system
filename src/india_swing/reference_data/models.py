from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path

from india_swing.reference.models import ReferenceReadiness


NSE_CM_SECURITY_DATASET = "nse-cm-mii-security"
REFERENCE_ARTIFACT_SCHEMA_VERSION = "reference-artifact/v2"
REFERENCE_NORMALIZED_CODEC_VERSION = "nse-cm-mii-security-json/v2"
NSE_CM_SECURITY_PARSER_VERSION = "nse-cm-mii-security-parser/v2"
NSE_CM_SECURITY_SOURCE_SCHEMA_VERSION = "nse-cm-mii-security/iso-tags-120/v1"
NSE_CM_SECURITY_SCOPE_POLICY_VERSION = "nse-cm-equity-scope/collection-only-v2"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_ISIN = re.compile(r"[A-Z]{2}[A-Z0-9]{9}[0-9]\Z")


class ReferenceArtifactError(RuntimeError):
    pass


class ReferenceArtifactIntegrityError(ReferenceArtifactError):
    pass


class ReferenceArtifactConflict(ReferenceArtifactError):
    pass


class ReferenceArtifactNotFound(ReferenceArtifactError):
    pass


class ReferenceArtifactStale(ReferenceArtifactError):
    pass


class ReferenceArtifactUnverifiedReportDate(ReferenceArtifactError):
    pass


class AcquisitionMode(str, Enum):
    UNVERIFIED_MANUAL_FILE = "UNVERIFIED_MANUAL_FILE"


class SourceRowDisposition(str, Enum):
    RETAINED_UNVERIFIED_EQUITY = "RETAINED_UNVERIFIED_EQUITY"
    EXCLUDED_NON_EQUITY = "EXCLUDED_NON_EQUITY"
    EXCLUDED_TEST_SECURITY = "EXCLUDED_TEST_SECURITY"
    EXCLUDED_ALTERNATIVE_VENUE = "EXCLUDED_ALTERNATIVE_VENUE"


def _require_sha256(value: str, field_name: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a full lowercase SHA-256 identifier")


def _require_aware(value: datetime, field_name: str) -> None:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


def validated_isin_or_none(raw_source_identifier: str) -> str | None:
    """Return a structurally valid, Luhn-checked ISIN without rewriting input."""
    if (
        not isinstance(raw_source_identifier, str)
        or _ISIN.fullmatch(raw_source_identifier) is None
    ):
        return None
    expanded = "".join(
        str(int(character, 36)) if character.isalpha() else character
        for character in raw_source_identifier
    )
    checksum = 0
    for position, character in enumerate(reversed(expanded)):
        value = int(character)
        if position % 2:
            value *= 2
        checksum += value // 10 + value % 10
    return raw_source_identifier if checksum % 10 == 0 else None


@dataclass(frozen=True, slots=True)
class MarketEligibility:
    status: int
    eligible: bool

    def __post_init__(self) -> None:
        if type(self.status) is not int or not 1 <= self.status <= 6:
            raise ValueError("market eligibility status must be an integer from 1 to 6")
        if type(self.eligible) is not bool:
            raise TypeError("market eligibility flag must be bool")


@dataclass(frozen=True, slots=True)
class NseCmSecurityRecord:
    source_row_number: int
    source_record_id: str
    normalized_row_sha256: str
    financial_instrument_id: int
    ticker_symbol: str
    security_series: str
    instrument_name: str
    raw_source_identifier: str
    validated_isin: str | None
    board_lot_quantity: int
    security_type_flag: int
    bid_interval_paise: int
    call_auction_indicator: int
    permitted_to_trade: int
    market_eligibility: tuple[MarketEligibility, ...]
    listing_timestamp: int
    removal_timestamp: int
    readmission_timestamp: int
    delete_flag: str
    disposition: SourceRowDisposition
    raw_fields: tuple[str, ...]

    def __post_init__(self) -> None:
        if type(self.source_row_number) is not int or self.source_row_number < 2:
            raise ValueError("source row number must include the header offset")
        _require_sha256(self.source_record_id, "source_record_id")
        _require_sha256(self.normalized_row_sha256, "normalized_row_sha256")
        if type(self.financial_instrument_id) is not int or self.financial_instrument_id <= 0:
            raise ValueError("financial instrument ID must be positive")
        if not isinstance(self.ticker_symbol, str) or not self.ticker_symbol:
            raise ValueError("ticker symbol is required")
        if not isinstance(self.security_series, str) or not self.security_series:
            raise ValueError("security series is required")
        if not isinstance(self.instrument_name, str) or not self.instrument_name:
            raise ValueError("instrument name is required")
        if not isinstance(self.raw_source_identifier, str) or not self.raw_source_identifier:
            raise ValueError("raw source identifier is required")
        if self.validated_isin != validated_isin_or_none(self.raw_source_identifier):
            raise ValueError(
                "validated ISIN must equal the check-digit-validated source identifier"
            )
        if type(self.board_lot_quantity) is not int or self.board_lot_quantity <= 0:
            raise ValueError("board lot quantity must be positive")
        if type(self.security_type_flag) is not int or not 0 <= self.security_type_flag <= 4:
            raise ValueError("security type flag must be between 0 and 4")
        if type(self.bid_interval_paise) is not int or self.bid_interval_paise <= 0:
            raise ValueError("bid interval must be positive")
        if (
            type(self.call_auction_indicator) is not int
            or not 0 <= self.call_auction_indicator <= 5
        ):
            raise ValueError("call auction indicator must be between 0 and 5")
        if self.permitted_to_trade not in (0, 1, 2):
            raise ValueError("permitted-to-trade must be 0, 1, or 2")
        if type(self.market_eligibility) is not tuple or len(self.market_eligibility) != 6:
            raise ValueError("all six market eligibility pairs are required")
        if any(type(item) is not MarketEligibility for item in self.market_eligibility):
            raise TypeError("market eligibility entries must be exact MarketEligibility values")
        for value, name in (
            (self.listing_timestamp, "listing_timestamp"),
            (self.removal_timestamp, "removal_timestamp"),
            (self.readmission_timestamp, "readmission_timestamp"),
        ):
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if self.delete_flag not in ("N", "Y"):
            raise ValueError("delete flag must be N or Y")
        if not isinstance(self.disposition, SourceRowDisposition):
            raise TypeError("source row disposition is required")
        if type(self.raw_fields) is not tuple or any(
            not isinstance(field, str) for field in self.raw_fields
        ):
            raise TypeError("raw fields must be an immutable text tuple")


@dataclass(frozen=True, slots=True)
class ParsedNseCmSecurityMaster:
    original_filename: str
    claimed_report_date: date
    source_schema_version: str
    header: tuple[str, ...]
    header_sha256: str
    raw_sha256: str
    uncompressed_sha256: str
    compressed_byte_count: int
    uncompressed_byte_count: int
    records: tuple[NseCmSecurityRecord, ...]
    ordered_row_digest: str
    retained_unverified_equity_count: int
    excluded_non_equity_count: int
    excluded_test_security_count: int
    excluded_alternative_venue_count: int

    def __post_init__(self) -> None:
        if not isinstance(self.original_filename, str) or not self.original_filename:
            raise ValueError("original filename is required")
        if type(self.claimed_report_date) is not date:
            raise TypeError("claimed report date must be a date")
        if not isinstance(self.source_schema_version, str) or not self.source_schema_version:
            raise ValueError("source schema version is required")
        if type(self.header) is not tuple or not self.header:
            raise ValueError("source header is required")
        for value, name in (
            (self.header_sha256, "header_sha256"),
            (self.raw_sha256, "raw_sha256"),
            (self.uncompressed_sha256, "uncompressed_sha256"),
            (self.ordered_row_digest, "ordered_row_digest"),
        ):
            _require_sha256(value, name)
        if type(self.compressed_byte_count) is not int or self.compressed_byte_count <= 0:
            raise ValueError("compressed byte count must be positive")
        if type(self.uncompressed_byte_count) is not int or self.uncompressed_byte_count <= 0:
            raise ValueError("uncompressed byte count must be positive")
        if type(self.records) is not tuple or not self.records:
            raise ValueError("security master must contain at least one record")
        if any(type(record) is not NseCmSecurityRecord for record in self.records):
            raise TypeError("security master records must be exact normalized records")
        counts = (
            self.retained_unverified_equity_count,
            self.excluded_non_equity_count,
            self.excluded_test_security_count,
            self.excluded_alternative_venue_count,
        )
        if any(type(value) is not int or value < 0 for value in counts):
            raise ValueError("disposition counts must be non-negative integers")
        if sum(counts) != len(self.records):
            raise ValueError("every source record must have exactly one disposition")


@dataclass(frozen=True, slots=True)
class ReferenceArtifactManifest:
    schema_version: str
    manifest_id: str
    artifact_id: str
    dataset: str
    claimed_authority: str
    acquisition_mode: AcquisitionMode
    readiness: ReferenceReadiness
    actionable: bool
    original_filename: str
    claimed_report_date: date
    verified_report_date: date | None
    claimed_source_catalog_url: str
    claimed_download_url: str
    source_media_type: str
    publication_time_status: str
    first_seen_at: datetime
    validated_at: datetime
    parser_version: str
    source_schema_version: str
    scope_policy_version: str
    normalized_codec_version: str
    compressed_byte_count: int
    uncompressed_byte_count: int
    raw_sha256: str
    uncompressed_sha256: str
    normalized_sha256: str
    header_sha256: str
    raw_row_count: int
    parsed_row_count: int
    retained_unverified_equity_count: int
    excluded_non_equity_count: int
    excluded_test_security_count: int
    excluded_alternative_venue_count: int
    ordered_row_digest: str
    raw_filename: str
    normalized_filename: str

    def __post_init__(self) -> None:
        for value, name in (
            (self.manifest_id, "manifest_id"),
            (self.artifact_id, "artifact_id"),
            (self.raw_sha256, "raw_sha256"),
            (self.uncompressed_sha256, "uncompressed_sha256"),
            (self.normalized_sha256, "normalized_sha256"),
            (self.header_sha256, "header_sha256"),
            (self.ordered_row_digest, "ordered_row_digest"),
        ):
            _require_sha256(value, name)
        if self.dataset != NSE_CM_SECURITY_DATASET or self.claimed_authority != "NSE":
            raise ValueError("unsupported claimed authority or reference dataset")
        if self.acquisition_mode is not AcquisitionMode.UNVERIFIED_MANUAL_FILE:
            raise ValueError("unsupported acquisition mode")
        if (
            self.readiness is not ReferenceReadiness.COLLECTION_ONLY
            or self.actionable is not False
        ):
            raise ValueError("an imported security master must remain collection-only")
        if type(self.claimed_report_date) is not date:
            raise TypeError("claimed report date must be a date")
        if self.verified_report_date is not None:
            raise ValueError(
                "an unverified manual file cannot carry a verified report date"
            )
        _require_aware(self.first_seen_at, "first_seen_at")
        _require_aware(self.validated_at, "validated_at")
        if (
            self.first_seen_at.utcoffset() != timezone.utc.utcoffset(None)
            or self.validated_at.utcoffset() != timezone.utc.utcoffset(None)
        ):
            raise ValueError("reference manifest timestamps must use UTC")
        object.__setattr__(
            self,
            "first_seen_at",
            self.first_seen_at.astimezone(timezone.utc),
        )
        object.__setattr__(
            self,
            "validated_at",
            self.validated_at.astimezone(timezone.utc),
        )
        if self.validated_at < self.first_seen_at:
            raise ValueError("validated_at cannot precede first_seen_at")
        for value, name in (
            (self.compressed_byte_count, "compressed_byte_count"),
            (self.uncompressed_byte_count, "uncompressed_byte_count"),
            (self.raw_row_count, "raw_row_count"),
            (self.parsed_row_count, "parsed_row_count"),
        ):
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be positive")
        if self.raw_row_count != self.parsed_row_count:
            raise ValueError("partial reference-data parsing is forbidden")
        disposition_total = (
            self.retained_unverified_equity_count
            + self.excluded_non_equity_count
            + self.excluded_test_security_count
            + self.excluded_alternative_venue_count
        )
        if disposition_total != self.parsed_row_count:
            raise ValueError("every parsed row must have one manifest disposition")


@dataclass(frozen=True, slots=True)
class StoredReferenceArtifact:
    path: Path
    manifest: ReferenceArtifactManifest
    parsed: ParsedNseCmSecurityMaster
    raw_bytes: bytes
    normalized_bytes: bytes
