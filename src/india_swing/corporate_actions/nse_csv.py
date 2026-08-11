"""Strict, point-in-time-safe import of NSE equity corporate-action CSVs.

The CSV does not carry reliable announcement timestamps or ISINs.  Callers
therefore supply the exact acquisition time and explicit symbol/series to
stable-identity bindings.  The acquisition time is retained as both the
conservative announcement boundary and knowledge time: an imported event can
never appear known before the bytes were actually acquired.  Unknown economic
purposes, missing identities, duplicate rows, and schema drift fail closed.
"""

from __future__ import annotations

import csv
import hashlib
import io
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation

from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness

from .models import (
    CorporateActionEvent,
    CorporateActionSnapshot,
    CorporateActionStatus,
    CorporateActionType,
)


NSE_CORPORATE_ACTION_CSV_IMPORT_POLICY_VERSION = (
    "nse-equity-corporate-action-csv/conservative-observation-time-v1"
)
NSE_CORPORATE_ACTION_CSV_IMPORT_SCHEMA_VERSION = (
    "nse-equity-corporate-action-csv-import/v1"
)

_HEADERS = (
    "SYMBOL",
    "COMPANY NAME",
    "SERIES",
    "PURPOSE",
    "FACE VALUE",
    "EX-DATE",
    "RECORD DATE",
    "BOOK CLOSURE START DATE",
    "BOOK CLOSURE END DATE",
)
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_SYMBOL = re.compile(r"[A-Z0-9&-]{1,32}\Z")
_SERIES = re.compile(r"[A-Z0-9]{1,4}\Z")
_MONEY = re.compile(
    r"R(?:S|E)\.?\s*([0-9]+(?:\.[0-9]+)?)\s*(?:/-)?\s*PER\s+SH(?:ARE)?",
    re.IGNORECASE,
)
_BONUS = re.compile(
    r"BONUS(?:\s+ISSUE)?(?:\s+OF)?[^0-9]{0,24}([0-9]+)\s*:\s*([0-9]+)",
    re.IGNORECASE,
)
_FROM_FACE_VALUE = re.compile(
    r"FROM.*?R(?:S|E)\.?\s*([0-9]+(?:\.[0-9]+)?)", re.IGNORECASE
)
_TO_FACE_VALUE = re.compile(
    r"\bTO\b.*?R(?:S|E)\.?\s*([0-9]+(?:\.[0-9]+)?)", re.IGNORECASE
)
_NON_PRICE_PURPOSES = (
    "ANNUAL GENERAL MEETING",
    "EXTRA ORDINARY GENERAL MEETING",
    "EXTRAORDINARY GENERAL MEETING",
    "AGM",
    "EGM",
    "BOOK CLOSURE",
    "INTEREST PAYMENT",
)


class NseCorporateActionCsvError(ValueError):
    """The NSE CSV could not be converted without weakening evidence."""


def _fail(message: str) -> None:
    raise NseCorporateActionCsvError(message)


def _utc(value: datetime) -> datetime:
    if type(value) is not datetime:
        _fail("corporate-action import timestamp is invalid")
    try:
        offset = value.utcoffset()
    except Exception:
        offset = None
    if value.tzinfo is None or offset is None:
        _fail("corporate-action import timestamp is invalid")
    return value.astimezone(timezone.utc)


def _sha(value: object) -> bool:
    return type(value) is str and _SHA256.fullmatch(value) is not None


def _decimal(value: str) -> Decimal:
    try:
        parsed = Decimal(value)
    except InvalidOperation:
        _fail("corporate-action economic terms are invalid")
    if not parsed.is_finite() or parsed <= 0:
        _fail("corporate-action economic terms are invalid")
    return parsed


def _date(value: str) -> date:
    try:
        return datetime.strptime(value.strip(), "%d-%b-%Y").date()
    except (TypeError, ValueError):
        _fail("corporate-action ex-date is invalid")


@dataclass(frozen=True, slots=True)
class NseCorporateActionListingBinding:
    symbol: str
    series: str
    stable_instrument_id: str
    stable_listing_id: str
    source_artifact_id: str
    knowledge_time: datetime
    binding_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            type(self.symbol) is not str
            or _SYMBOL.fullmatch(self.symbol) is None
            or type(self.series) is not str
            or _SERIES.fullmatch(self.series) is None
            or not _sha(self.stable_instrument_id)
            or not _sha(self.stable_listing_id)
            or not _sha(self.source_artifact_id)
        ):
            _fail("corporate-action listing binding is invalid")
        object.__setattr__(self, "knowledge_time", _utc(self.knowledge_time))
        object.__setattr__(
            self,
            "binding_id",
            content_id(
                {
                    "schema": NSE_CORPORATE_ACTION_CSV_IMPORT_SCHEMA_VERSION,
                    "policy": NSE_CORPORATE_ACTION_CSV_IMPORT_POLICY_VERSION,
                    "symbol": self.symbol,
                    "series": self.series,
                    "stable_instrument_id": self.stable_instrument_id,
                    "stable_listing_id": self.stable_listing_id,
                    "source_artifact_id": self.source_artifact_id,
                    "knowledge_time": self.knowledge_time,
                },
                length=64,
            ),
        )

    def verify_content_identity(self) -> None:
        expected = content_id(
            {
                "schema": NSE_CORPORATE_ACTION_CSV_IMPORT_SCHEMA_VERSION,
                "policy": NSE_CORPORATE_ACTION_CSV_IMPORT_POLICY_VERSION,
                "symbol": self.symbol,
                "series": self.series,
                "stable_instrument_id": self.stable_instrument_id,
                "stable_listing_id": self.stable_listing_id,
                "source_artifact_id": self.source_artifact_id,
                "knowledge_time": self.knowledge_time,
            },
            length=64,
        )
        if self.binding_id != expected:
            _fail("corporate-action listing binding failed verification")


@dataclass(frozen=True, slots=True)
class NseCorporateActionCsvImport:
    source_sha256: str
    source_row_count: int
    imported_row_count: int
    ignored_non_price_row_ids: tuple[str, ...]
    ignored_out_of_scope_row_ids: tuple[str, ...]
    in_scope_series: tuple[str, ...]
    listing_binding_ids: tuple[str, ...]
    snapshot: CorporateActionSnapshot
    import_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            not _sha(self.source_sha256)
            or type(self.source_row_count) is not int
            or type(self.imported_row_count) is not int
            or self.source_row_count <= 0
            or self.imported_row_count < 0
            or self.imported_row_count
            + len(self.ignored_non_price_row_ids)
            + len(self.ignored_out_of_scope_row_ids)
            != self.source_row_count
            or self.ignored_non_price_row_ids
            != tuple(sorted(set(self.ignored_non_price_row_ids)))
            or self.ignored_out_of_scope_row_ids
            != tuple(sorted(set(self.ignored_out_of_scope_row_ids)))
            or self.in_scope_series != tuple(sorted(set(self.in_scope_series)))
            or not self.in_scope_series
            or self.listing_binding_ids
            != tuple(sorted(set(self.listing_binding_ids)))
            or any(not _sha(value) for value in self.ignored_non_price_row_ids)
            or any(not _sha(value) for value in self.ignored_out_of_scope_row_ids)
            or any(_SERIES.fullmatch(value) is None for value in self.in_scope_series)
            or any(not _sha(value) for value in self.listing_binding_ids)
            or type(self.snapshot) is not CorporateActionSnapshot
        ):
            _fail("corporate-action import result is invalid")
        self.snapshot.verify_content_identity()
        if self.snapshot.source_artifact_ids != (self.source_sha256,):
            _fail("corporate-action import source lineage is invalid")
        object.__setattr__(self, "import_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": NSE_CORPORATE_ACTION_CSV_IMPORT_SCHEMA_VERSION,
                "policy": NSE_CORPORATE_ACTION_CSV_IMPORT_POLICY_VERSION,
                "source_sha256": self.source_sha256,
                "source_row_count": self.source_row_count,
                "imported_row_count": self.imported_row_count,
                "ignored_non_price_row_ids": self.ignored_non_price_row_ids,
                "ignored_out_of_scope_row_ids": self.ignored_out_of_scope_row_ids,
                "in_scope_series": self.in_scope_series,
                "listing_binding_ids": self.listing_binding_ids,
                "snapshot_id": self.snapshot.snapshot_id,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self.snapshot.verify_content_identity()
        if self.import_id != self._calculated_id():
            _fail("corporate-action import result failed verification")


def _economic_terms(purpose: str) -> tuple[
    CorporateActionType | None,
    Decimal | None,
    Decimal | None,
    Decimal | None,
    str | None,
]:
    upper = " ".join(purpose.upper().split())
    if "SPLIT" in upper or "SUB-DIVISION" in upper or "SUB DIVISION" in upper:
        from_match = _FROM_FACE_VALUE.search(upper)
        to_match = _TO_FACE_VALUE.search(upper)
        if from_match is None or to_match is None:
            _fail("corporate-action split terms are unsupported")
        old_face = _decimal(from_match.group(1))
        new_face = _decimal(to_match.group(1))
        if old_face == new_face:
            _fail("corporate-action split terms are invalid")
        return CorporateActionType.SPLIT, new_face, old_face, None, None
    if "BONUS" in upper:
        match = _BONUS.search(upper)
        if match is None:
            _fail("corporate-action bonus terms are unsupported")
        new_shares = _decimal(match.group(1))
        old_shares = _decimal(match.group(2))
        return (
            CorporateActionType.BONUS,
            old_shares,
            old_shares + new_shares,
            None,
            None,
        )
    if "DIVIDEND" in upper:
        amounts = tuple(_decimal(value) for value in _MONEY.findall(upper))
        if not amounts:
            _fail("corporate-action dividend terms are unsupported")
        return CorporateActionType.CASH_DIVIDEND, None, None, sum(amounts), "INR"
    if "RIGHT" in upper:
        return CorporateActionType.RIGHTS, None, None, None, None
    if "DEMERGER" in upper or "DE-MERGER" in upper:
        return CorporateActionType.DEMERGER, None, None, None, None
    if "MERGER" in upper or "AMALGAMATION" in upper:
        return CorporateActionType.MERGER, None, None, None, None
    if "SYMBOL CHANGE" in upper or "CHANGE IN SYMBOL" in upper:
        return CorporateActionType.SYMBOL_CHANGE, None, None, None, None
    if "ISIN CHANGE" in upper or "CHANGE IN ISIN" in upper:
        return CorporateActionType.ISIN_CHANGE, None, None, None, None
    if "DELIST" in upper:
        return CorporateActionType.DELISTING, None, None, None, None
    if "BUY BACK" in upper or "BUYBACK" in upper:
        return CorporateActionType.BUYBACK, None, None, None, None
    if any(value in upper for value in _NON_PRICE_PURPOSES):
        return None, None, None, None, None
    _fail("corporate-action purpose is unsupported")


def import_nse_corporate_action_csv(
    source_bytes: bytes,
    *,
    observed_at: datetime,
    cutoff: datetime,
    coverage_start: date,
    coverage_end: date,
    listing_bindings: tuple[NseCorporateActionListingBinding, ...],
    in_scope_series: tuple[str, ...] = ("EQ", "SM"),
    maximum_source_bytes: int = 32 * 1024 * 1024,
    maximum_rows: int = 100_000,
) -> NseCorporateActionCsvImport:
    """Convert exact NSE CSV bytes into one actionable snapshot.

    The caller's coverage interval is an explicit assertion about the NSE
    query used to obtain the bytes. Empty result files are intentionally not
    accepted by this v1 boundary because their completeness cannot be proven
    from the CSV alone.
    """

    if (
        type(source_bytes) is not bytes
        or not source_bytes
        or type(maximum_source_bytes) is not int
        or type(maximum_rows) is not int
        or maximum_source_bytes <= 0
        or maximum_rows <= 0
        or len(source_bytes) > maximum_source_bytes
    ):
        _fail("corporate-action source envelope is invalid")
    observed = _utc(observed_at)
    snapshot_cutoff = _utc(cutoff)
    if observed > snapshot_cutoff:
        _fail("corporate-action source is future-known")
    if (
        type(coverage_start) is not date
        or type(coverage_end) is not date
        or coverage_start > coverage_end
    ):
        _fail("corporate-action coverage is invalid")
    if type(listing_bindings) is not tuple or any(
        type(value) is not NseCorporateActionListingBinding
        for value in listing_bindings
    ):
        _fail("corporate-action listing bindings are invalid")
    if (
        type(in_scope_series) is not tuple
        or not in_scope_series
        or in_scope_series != tuple(sorted(set(in_scope_series)))
        or any(type(value) is not str or _SERIES.fullmatch(value) is None for value in in_scope_series)
    ):
        _fail("corporate-action import scope is invalid")
    by_key: dict[tuple[str, str], NseCorporateActionListingBinding] = {}
    for value in listing_bindings:
        value.verify_content_identity()
        key = (value.symbol, value.series)
        if key in by_key or value.knowledge_time > snapshot_cutoff:
            _fail("corporate-action listing bindings are ambiguous or future-known")
        by_key[key] = value

    try:
        text = source_bytes.decode("utf-8-sig", errors="strict")
        reader = csv.DictReader(io.StringIO(text, newline=""), strict=True)
        if tuple(reader.fieldnames or ()) != _HEADERS:
            _fail("corporate-action CSV header is unsupported")
        rows = tuple(reader)
    except (UnicodeDecodeError, csv.Error):
        _fail("corporate-action CSV is malformed")
    if not rows or len(rows) > maximum_rows:
        _fail("corporate-action CSV row count is invalid")

    source_sha256 = hashlib.sha256(source_bytes).hexdigest()
    events: list[CorporateActionEvent] = []
    ignored: list[str] = []
    out_of_scope: list[str] = []
    used_bindings: set[str] = set()
    seen_rows: set[str] = set()
    for row_number, row in enumerate(rows, start=2):
        if set(row) != set(_HEADERS) or any(
            type(value) is not str or len(value) > 8_192 for value in row.values()
        ):
            _fail("corporate-action CSV row is invalid")
        symbol = row["SYMBOL"].strip().upper()
        series = row["SERIES"].strip().upper()
        purpose = row["PURPOSE"].strip()
        if (
            _SYMBOL.fullmatch(symbol) is None
            or _SERIES.fullmatch(series) is None
            or not purpose
        ):
            _fail("corporate-action CSV identity is invalid")
        effective_session = _date(row["EX-DATE"])
        if not coverage_start <= effective_session <= coverage_end:
            _fail("corporate-action row lies outside declared coverage")
        row_id = content_id(
            {
                "schema": NSE_CORPORATE_ACTION_CSV_IMPORT_SCHEMA_VERSION,
                "source_sha256": source_sha256,
                "row_number": row_number,
                "row": tuple(row[name] for name in _HEADERS),
            },
            length=64,
        )
        if row_id in seen_rows:
            _fail("corporate-action CSV contains duplicate rows")
        seen_rows.add(row_id)
        if series not in in_scope_series:
            out_of_scope.append(row_id)
            continue
        binding = by_key.get((symbol, series))
        if binding is None:
            _fail("corporate-action listing identity is unavailable")
        action_type, pre, post, cash, currency = _economic_terms(purpose)
        if action_type is None:
            ignored.append(row_id)
            continue
        used_bindings.add(binding.binding_id)
        events.append(
            CorporateActionEvent(
                stable_instrument_id=binding.stable_instrument_id,
                stable_listing_id=binding.stable_listing_id,
                action_type=action_type,
                status=CorporateActionStatus.CONFIRMED,
                effective_session=effective_session,
                announcement_time=observed,
                knowledge_time=observed,
                source_artifact_id=source_sha256,
                source_row_id=row_id,
                pre_action_shares=pre,
                post_action_shares=post,
                cash_amount_per_share=cash,
                currency=currency,
            )
        )

    canonical_events = tuple(
        sorted(
            events,
            key=lambda value: (
                value.knowledge_time,
                value.effective_session,
                value.event_id,
            ),
        )
    )
    snapshot = CorporateActionSnapshot(
        cutoff=snapshot_cutoff,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        source_artifact_ids=(source_sha256,),
        events=canonical_events,
        readiness=ReferenceReadiness.POINT_IN_TIME_VERIFIED,
        complete=True,
        actionable=True,
        reason_codes=(),
    )
    return NseCorporateActionCsvImport(
        source_sha256=source_sha256,
        source_row_count=len(rows),
        imported_row_count=len(canonical_events),
        ignored_non_price_row_ids=tuple(sorted(ignored)),
        ignored_out_of_scope_row_ids=tuple(sorted(out_of_scope)),
        in_scope_series=in_scope_series,
        listing_binding_ids=tuple(sorted(used_bindings)),
        snapshot=snapshot,
    )
