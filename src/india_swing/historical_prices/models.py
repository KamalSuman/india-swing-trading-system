from __future__ import annotations

import re
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import PurePath

from india_swing.calendar_evidence.policy import final_report_not_before
from india_swing.daily_reports.models import (
    NSE_DAILY_BUNDLE_ARTIFACT_SCHEMA_VERSION,
    NSE_DAILY_BUNDLE_CODEC_VERSION,
    NSE_DAILY_BUNDLE_DATASET,
    NSE_DAILY_BUNDLE_PARSER_VERSION,
    BundleEntryDisposition,
    DailyBundleArtifactManifest,
    DailyReportFamily,
    ReportDateRole,
    ReportDateStatus,
)
from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference_data.models import AcquisitionMode, validated_isin_or_none


HISTORICAL_PRICE_SCHEMA_VERSION = "nse-cm-raw-eod-session/v1"
HISTORICAL_PRICE_POLICY_VERSION = "nse-cm-traded-rows-raw-unadjusted/v1"
HISTORICAL_PRICE_CODEC_VERSION = "nse-cm-raw-eod-session-json/v1"
RAW_UNADJUSTED = "RAW_UNADJUSTED"
TRADED_ROWS_ONLY = "TRADED_ROWS_ONLY"

ZERO = Decimal("0")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_REPORT_FAMILY_ORDER = (
    DailyReportFamily.UDIFF_BHAVCOPY,
    DailyReportFamily.FULL_BHAVCOPY_DELIVERY,
)


class HistoricalPriceError(ValueError):
    pass


class HistoricalPriceIntegrityError(HistoricalPriceError):
    pass


def _require_sha256(value: str, field_name: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a full lowercase SHA-256")


def _utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _require_safe_basename(value: str, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or PurePath(value).name != value
        or "/" in value
        or "\\" in value
    ):
        raise ValueError(f"{field_name} must be a safe basename")


def _require_decimal(value: Decimal, field_name: str, *, positive: bool = False) -> None:
    if type(value) is not Decimal:
        raise TypeError(f"{field_name} must be a Decimal")
    if not value.is_finite():
        raise ValueError(f"{field_name} must be finite")
    if positive and value <= ZERO:
        raise ValueError(f"{field_name} must be positive")


def _source_artifact_identity(
    manifest: DailyBundleArtifactManifest,
) -> dict[str, object]:
    return {
        "schema_version": manifest.schema_version,
        "dataset": manifest.dataset,
        "claimed_authority": manifest.claimed_authority,
        "acquisition_mode": manifest.acquisition_mode,
        "readiness": manifest.readiness,
        "actionable": manifest.actionable,
        "original_filename": manifest.original_filename,
        "claimed_source_catalog_url": manifest.claimed_source_catalog_url,
        "source_media_type": manifest.source_media_type,
        "parser_version": manifest.parser_version,
        "normalized_codec_version": manifest.normalized_codec_version,
        "raw_sha256": manifest.raw_sha256,
        "normalized_sha256": manifest.normalized_sha256,
        "byte_count": manifest.byte_count,
        "outer_entry_count": manifest.outer_entry_count,
        "selected_report_count": manifest.selected_report_count,
        "quarantined_report_count": manifest.quarantined_report_count,
        "deferred_report_count": manifest.deferred_report_count,
        "ignored_entry_count": manifest.ignored_entry_count,
        "selected_row_count": manifest.selected_row_count,
        "raw_filename": manifest.raw_filename,
        "normalized_filename": manifest.normalized_filename,
    }


def _source_manifest_identity(
    manifest: DailyBundleArtifactManifest,
) -> dict[str, object]:
    return {
        item.name: getattr(manifest, item.name)
        for item in fields(DailyBundleArtifactManifest)
        if item.name != "manifest_id"
    }


@dataclass(frozen=True, slots=True)
class PriceReportRef:
    """Content-bound lineage for one final-session price report."""

    bundle_artifact_id: str
    bundle_manifest_id: str
    bundle_raw_sha256: str
    bundle_normalized_sha256: str
    family: DailyReportFamily
    source_entry_name: str
    content_name: str
    source_entry_sha256: str
    content_sha256: str
    header_sha256: str
    ordered_row_digest: str
    claimed_report_date: date
    confirmed_row_dates: tuple[date, ...]
    date_status: ReportDateStatus
    date_role: ReportDateRole
    first_seen_at: datetime
    validated_at: datetime
    row_count: int
    report_ref_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.bundle_artifact_id, "price_report.bundle_artifact_id"),
            (self.bundle_manifest_id, "price_report.bundle_manifest_id"),
            (self.bundle_raw_sha256, "price_report.bundle_raw_sha256"),
            (
                self.bundle_normalized_sha256,
                "price_report.bundle_normalized_sha256",
            ),
            (self.source_entry_sha256, "price_report.source_entry_sha256"),
            (self.content_sha256, "price_report.content_sha256"),
            (self.header_sha256, "price_report.header_sha256"),
            (self.ordered_row_digest, "price_report.ordered_row_digest"),
        ):
            _require_sha256(value, name)
        if self.family not in _REPORT_FAMILY_ORDER:
            raise ValueError("price report must be UDiFF or full-delivery Bhavcopy")
        _require_safe_basename(self.source_entry_name, "source_entry_name")
        _require_safe_basename(self.content_name, "content_name")
        if type(self.claimed_report_date) is not date:
            raise TypeError("claimed_report_date must be a date")
        if self.confirmed_row_dates != (self.claimed_report_date,):
            raise HistoricalPriceIntegrityError(
                "price report must row-confirm exactly its claimed trade date"
            )
        if self.date_status is not ReportDateStatus.ROW_CONFIRMED:
            raise HistoricalPriceIntegrityError("price report must be row-confirmed")
        if self.date_role is not ReportDateRole.TRADE_DATE:
            raise HistoricalPriceIntegrityError("price report must carry the trade-date role")
        object.__setattr__(
            self,
            "first_seen_at",
            _utc(self.first_seen_at, "price_report.first_seen_at"),
        )
        object.__setattr__(
            self,
            "validated_at",
            _utc(self.validated_at, "price_report.validated_at"),
        )
        if self.validated_at < self.first_seen_at:
            raise ValueError("price report validation cannot precede first observation")
        if self.validated_at < final_report_not_before(self.claimed_report_date):
            raise HistoricalPriceIntegrityError(
                "price report predates the conservative final-report boundary"
            )
        if type(self.row_count) is not int or self.row_count <= 0:
            raise ValueError("price report must contain at least one row")
        object.__setattr__(self, "report_ref_id", self._calculated_report_ref_id())

    def _calculated_report_ref_id(self) -> str:
        return content_id(
            {
                "bundle_artifact_id": self.bundle_artifact_id,
                "bundle_manifest_id": self.bundle_manifest_id,
                "bundle_raw_sha256": self.bundle_raw_sha256,
                "bundle_normalized_sha256": self.bundle_normalized_sha256,
                "family": self.family,
                "source_entry_name": self.source_entry_name,
                "content_name": self.content_name,
                "source_entry_sha256": self.source_entry_sha256,
                "content_sha256": self.content_sha256,
                "header_sha256": self.header_sha256,
                "ordered_row_digest": self.ordered_row_digest,
                "claimed_report_date": self.claimed_report_date,
                "confirmed_row_dates": self.confirmed_row_dates,
                "date_status": self.date_status,
                "date_role": self.date_role,
                "first_seen_at": self.first_seen_at,
                "validated_at": self.validated_at,
                "row_count": self.row_count,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.report_ref_id != self._calculated_report_ref_id():
            raise HistoricalPriceIntegrityError("price-report identity verification failed")


@dataclass(frozen=True, slots=True)
class PriceRowRef:
    report_ref_id: str
    family: DailyReportFamily
    source_row_number: int
    row_sha256: str
    listing_key: tuple[str, str]

    def __post_init__(self) -> None:
        _require_sha256(self.report_ref_id, "price_row.report_ref_id")
        _require_sha256(self.row_sha256, "price_row.row_sha256")
        if self.family not in _REPORT_FAMILY_ORDER:
            raise ValueError("price row must come from a final price report")
        if type(self.source_row_number) is not int or self.source_row_number < 2:
            raise ValueError("source row number must include the header offset")
        if (
            type(self.listing_key) is not tuple
            or len(self.listing_key) != 2
            or any(
                not isinstance(value, str)
                or not value
                or value != value.strip().upper()
                for value in self.listing_key
            )
        ):
            raise ValueError("listing_key must be an uppercase symbol-series pair")


@dataclass(frozen=True, slots=True)
class RawNseEodBar:
    """One exchange-reported raw bar; its numeric ID is session-scoped."""

    market_session: date
    financial_instrument_id: int
    validated_isin: str
    symbol: str
    series: str
    session_id: str
    instrument_name: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    last: Decimal
    previous_close: Decimal
    volume: int
    traded_value: Decimal
    trade_count: int
    board_lot_quantity: int
    full_average_price: Decimal | None
    delivery_quantity: int | None
    delivery_percent: Decimal | None
    knowledge_time: datetime
    udiff_row_ref: PriceRowRef
    full_delivery_row_ref: PriceRowRef | None
    bar_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.market_session) is not date:
            raise TypeError("market_session must be a date")
        if type(self.financial_instrument_id) is not int or self.financial_instrument_id <= 0:
            raise ValueError("financial_instrument_id must be positive")
        if self.validated_isin != validated_isin_or_none(self.validated_isin):
            raise ValueError("validated_isin must be structurally and checksum valid")
        for value, name in (
            (self.symbol, "symbol"),
            (self.series, "series"),
            (self.session_id, "session_id"),
        ):
            if not isinstance(value, str) or not value or value != value.strip().upper():
                raise ValueError(f"{name} must be normalized uppercase text")
        if self.session_id != "F1":
            raise ValueError("only the pinned F1 final session is supported")
        if not isinstance(self.instrument_name, str) or not self.instrument_name:
            raise ValueError("instrument_name is required")
        for name in (
            "open",
            "high",
            "low",
            "close",
            "last",
            "previous_close",
            "traded_value",
        ):
            _require_decimal(getattr(self, name), name, positive=True)
        if self.high < max(self.open, self.low, self.close, self.last):
            raise ValueError("bar high is inconsistent with reported prices")
        if self.low > min(self.open, self.high, self.close, self.last):
            raise ValueError("bar low is inconsistent with reported prices")
        for value, name in (
            (self.volume, "volume"),
            (self.trade_count, "trade_count"),
            (self.board_lot_quantity, "board_lot_quantity"),
        ):
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if not self.low <= self.traded_value / Decimal(self.volume) <= self.high:
            raise ValueError("traded value implies an average outside the daily range")
        if self.full_delivery_row_ref is None:
            if any(
                value is not None
                for value in (
                    self.full_average_price,
                    self.delivery_quantity,
                    self.delivery_percent,
                )
            ):
                raise ValueError("full-report values require full-report row lineage")
        else:
            if self.full_delivery_row_ref.family is not DailyReportFamily.FULL_BHAVCOPY_DELIVERY:
                raise ValueError("full_delivery_row_ref has the wrong report family")
            if self.full_delivery_row_ref.listing_key != self.listing_key:
                raise ValueError("full-delivery row belongs to another listing")
            if self.full_average_price is None:
                raise ValueError("full-report lineage requires its reported average price")
            _require_decimal(self.full_average_price, "full_average_price", positive=True)
            if not self.low <= self.full_average_price <= self.high:
                raise ValueError("full-report average price is outside the daily range")
            if (self.delivery_quantity is None) != (self.delivery_percent is None):
                raise ValueError("delivery quantity and percent must share missingness")
            if self.delivery_quantity is not None:
                if (
                    type(self.delivery_quantity) is not int
                    or not 0 <= self.delivery_quantity <= self.volume
                ):
                    raise ValueError("delivery quantity is inconsistent with volume")
                assert self.delivery_percent is not None
                _require_decimal(self.delivery_percent, "delivery_percent")
                expected_percent = (
                    Decimal(100)
                    * Decimal(self.delivery_quantity)
                    / Decimal(self.volume)
                ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                if self.delivery_percent != expected_percent:
                    raise ValueError("delivery percent is inconsistent with quantity")
        object.__setattr__(
            self,
            "knowledge_time",
            _utc(self.knowledge_time, "bar.knowledge_time"),
        )
        if self.udiff_row_ref.family is not DailyReportFamily.UDIFF_BHAVCOPY:
            raise ValueError("udiff_row_ref has the wrong report family")
        if self.udiff_row_ref.listing_key != self.listing_key:
            raise ValueError("UDiFF row belongs to another listing")
        object.__setattr__(self, "bar_id", self._calculated_bar_id())

    @property
    def listing_key(self) -> tuple[str, str]:
        return (self.symbol, self.series)

    def _calculated_bar_id(self) -> str:
        return content_id(
            {
                "schema": "raw-nse-eod-bar/v1",
                "market_session": self.market_session,
                "financial_instrument_id": self.financial_instrument_id,
                "validated_isin": self.validated_isin,
                "symbol": self.symbol,
                "series": self.series,
                "session_id": self.session_id,
                "instrument_name": self.instrument_name,
                "open": self.open,
                "high": self.high,
                "low": self.low,
                "close": self.close,
                "last": self.last,
                "previous_close": self.previous_close,
                "volume": self.volume,
                "traded_value": self.traded_value,
                "trade_count": self.trade_count,
                "board_lot_quantity": self.board_lot_quantity,
                "full_average_price": self.full_average_price,
                "delivery_quantity": self.delivery_quantity,
                "delivery_percent": self.delivery_percent,
                "knowledge_time": self.knowledge_time,
                "udiff_row_ref": self.udiff_row_ref,
                "full_delivery_row_ref": self.full_delivery_row_ref,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.bar_id != self._calculated_bar_id():
            raise HistoricalPriceIntegrityError("bar identity verification failed")


@dataclass(frozen=True, slots=True)
class NseEodSessionArtifact:
    exchange: str
    segment: str
    market_session: date
    cutoff: datetime
    knowledge_time: datetime
    source_bundle_manifest: DailyBundleArtifactManifest
    report_refs: tuple[PriceReportRef, ...]
    bars: tuple[RawNseEodBar, ...]
    price_basis: str = RAW_UNADJUSTED
    coverage_scope: str = TRADED_ROWS_ONLY
    readiness: ReferenceReadiness = ReferenceReadiness.COLLECTION_ONLY
    actionable: bool = False
    policy_version: str = HISTORICAL_PRICE_POLICY_VERSION
    schema_version: str = HISTORICAL_PRICE_SCHEMA_VERSION
    artifact_id: str = field(init=False)

    def __post_init__(self) -> None:
        if self.exchange != "NSE" or self.segment != "CM":
            raise ValueError("historical-price artifacts are pinned to NSE CM")
        if type(self.market_session) is not date:
            raise TypeError("market_session must be a date")
        object.__setattr__(self, "cutoff", _utc(self.cutoff, "cutoff"))
        object.__setattr__(
            self,
            "knowledge_time",
            _utc(self.knowledge_time, "knowledge_time"),
        )
        if type(self.source_bundle_manifest) is not DailyBundleArtifactManifest:
            raise TypeError("source_bundle_manifest must be an exact manifest")
        manifest = self.source_bundle_manifest
        if (
            manifest.schema_version != NSE_DAILY_BUNDLE_ARTIFACT_SCHEMA_VERSION
            or manifest.dataset != NSE_DAILY_BUNDLE_DATASET
            or manifest.parser_version != NSE_DAILY_BUNDLE_PARSER_VERSION
            or manifest.normalized_codec_version != NSE_DAILY_BUNDLE_CODEC_VERSION
        ):
            raise HistoricalPriceIntegrityError(
                "source bundle uses an unsupported schema or parser contract"
            )
        if (
            manifest.acquisition_mode is not AcquisitionMode.UNVERIFIED_MANUAL_FILE
            or manifest.readiness is not ReferenceReadiness.COLLECTION_ONLY
            or manifest.actionable is not False
        ):
            raise HistoricalPriceIntegrityError(
                "source bundle must remain unverified and collection-only"
            )
        if content_id(_source_artifact_identity(manifest), length=64) != manifest.artifact_id:
            raise HistoricalPriceIntegrityError("source bundle artifact ID is invalid")
        if content_id(_source_manifest_identity(manifest), length=64) != manifest.manifest_id:
            raise HistoricalPriceIntegrityError("source bundle manifest ID is invalid")
        if self.knowledge_time != manifest.validated_at:
            raise HistoricalPriceIntegrityError(
                "artifact knowledge time must equal source validation time"
            )
        if self.knowledge_time > self.cutoff:
            raise HistoricalPriceIntegrityError("source was not validated by the cutoff")
        if type(self.report_refs) is not tuple or any(
            type(value) is not PriceReportRef for value in self.report_refs
        ):
            raise TypeError("report_refs must be an immutable exact tuple")
        if tuple(value.family for value in self.report_refs) != _REPORT_FAMILY_ORDER:
            raise HistoricalPriceIntegrityError(
                "artifact requires one ordered UDiFF/full report pair"
            )
        report_by_family = {value.family: value for value in self.report_refs}
        for reference in self.report_refs:
            reference.verify_content_identity()
            if (
                reference.bundle_artifact_id != manifest.artifact_id
                or reference.bundle_manifest_id != manifest.manifest_id
                or reference.bundle_raw_sha256 != manifest.raw_sha256
                or reference.bundle_normalized_sha256 != manifest.normalized_sha256
                or reference.first_seen_at != manifest.first_seen_at
                or reference.validated_at != manifest.validated_at
                or reference.claimed_report_date != self.market_session
            ):
                raise HistoricalPriceIntegrityError(
                    "report lineage disagrees with the source manifest or session"
                )
        if type(self.bars) is not tuple or not self.bars or any(
            type(value) is not RawNseEodBar for value in self.bars
        ):
            raise TypeError("bars must be a non-empty immutable exact tuple")
        expected_order = tuple(
            sorted(
                self.bars,
                key=lambda value: (
                    value.symbol,
                    value.series,
                    value.financial_instrument_id,
                ),
            )
        )
        if self.bars != expected_order:
            raise HistoricalPriceIntegrityError("bars must be deterministically sorted")
        if len({value.listing_key for value in self.bars}) != len(self.bars):
            raise HistoricalPriceIntegrityError("bars contain duplicate listing keys")
        if len({value.financial_instrument_id for value in self.bars}) != len(self.bars):
            raise HistoricalPriceIntegrityError(
                "bars contain duplicate session financial-instrument IDs"
            )
        if len({value.bar_id for value in self.bars}) != len(self.bars):
            raise HistoricalPriceIntegrityError("bars contain duplicate content identities")
        udiff_ref = report_by_family[DailyReportFamily.UDIFF_BHAVCOPY]
        full_ref = report_by_family[DailyReportFamily.FULL_BHAVCOPY_DELIVERY]
        udiff_rows: list[int] = []
        full_rows: list[int] = []
        for bar in self.bars:
            bar.verify_content_identity()
            if bar.market_session != self.market_session:
                raise HistoricalPriceIntegrityError("bar belongs to another session")
            if bar.knowledge_time != self.knowledge_time:
                raise HistoricalPriceIntegrityError("bar knowledge time disagrees with artifact")
            if bar.udiff_row_ref.report_ref_id != udiff_ref.report_ref_id:
                raise HistoricalPriceIntegrityError("bar UDiFF lineage is unbound")
            udiff_rows.append(bar.udiff_row_ref.source_row_number)
            if bar.full_delivery_row_ref is not None:
                if bar.full_delivery_row_ref.report_ref_id != full_ref.report_ref_id:
                    raise HistoricalPriceIntegrityError("bar full-report lineage is unbound")
                full_rows.append(bar.full_delivery_row_ref.source_row_number)
        if len(self.bars) != udiff_ref.row_count or sorted(udiff_rows) != list(
            range(2, udiff_ref.row_count + 2)
        ):
            raise HistoricalPriceIntegrityError(
                "every UDiFF row must own exactly one materialized bar"
            )
        if sorted(full_rows) != list(range(2, full_ref.row_count + 2)):
            raise HistoricalPriceIntegrityError(
                "every full-delivery row must attach to exactly one bar"
            )
        if self.price_basis != RAW_UNADJUSTED:
            raise ValueError("historical prices must remain raw and unadjusted")
        if self.coverage_scope != TRADED_ROWS_ONLY:
            raise ValueError("bhavcopy coverage cannot claim a complete universe")
        if self.readiness is not ReferenceReadiness.COLLECTION_ONLY or self.actionable is not False:
            raise ValueError("historical-price artifact must remain collection-only")
        if self.policy_version != HISTORICAL_PRICE_POLICY_VERSION:
            raise ValueError("unsupported historical-price policy")
        if self.schema_version != HISTORICAL_PRICE_SCHEMA_VERSION:
            raise ValueError("unsupported historical-price schema")
        object.__setattr__(self, "artifact_id", self._calculated_artifact_id())

    def _calculated_artifact_id(self) -> str:
        return content_id(
            {
                "schema_version": self.schema_version,
                "policy_version": self.policy_version,
                "exchange": self.exchange,
                "segment": self.segment,
                "market_session": self.market_session,
                "cutoff": self.cutoff,
                "knowledge_time": self.knowledge_time,
                "source_bundle_manifest": self.source_bundle_manifest,
                "report_refs": self.report_refs,
                "bars": self.bars,
                "price_basis": self.price_basis,
                "coverage_scope": self.coverage_scope,
                "readiness": self.readiness,
                "actionable": self.actionable,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if any(type(value) is not PriceReportRef for value in self.report_refs):
            raise HistoricalPriceIntegrityError("report graph contains an invalid type")
        if any(type(value) is not RawNseEodBar for value in self.bars):
            raise HistoricalPriceIntegrityError("bar graph contains an invalid type")
        for reference in self.report_refs:
            reference.verify_content_identity()
        for bar in self.bars:
            bar.verify_content_identity()
        if self.artifact_id != self._calculated_artifact_id():
            raise HistoricalPriceIntegrityError("artifact identity verification failed")
