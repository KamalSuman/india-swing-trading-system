from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from india_swing.daily_reports.artifact_store import (
    verify_stored_daily_bundle_provenance,
)
from india_swing.daily_reports.models import (
    BundleEntryDisposition,
    DailyReportFamily,
    DailyReportIntegrityError,
    ParsedDailyReport,
    ReportDateRole,
    ReportDateStatus,
    StoredDailyBundleArtifact,
)
from india_swing.daily_reports.parser import (
    FULL_BHAVCOPY_DELIVERY_HEADER,
    UDIFF_BHAVCOPY_HEADER,
)

from .models import (
    HistoricalPriceIntegrityError,
    NseEodSessionArtifact,
    PriceReportRef,
    PriceRowRef,
    RawNseEodBar,
)


_REPORT_FAMILY_ORDER = (
    DailyReportFamily.UDIFF_BHAVCOPY,
    DailyReportFamily.FULL_BHAVCOPY_DELIVERY,
)


def _row_sha256(row: tuple[str, ...]) -> str:
    return hashlib.sha256(
        json.dumps(
            row,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _field_map(report: ParsedDailyReport, row: tuple[str, ...]) -> dict[str, str]:
    try:
        return {
            name.strip(): value.strip()
            for name, value in zip(report.header, row, strict=True)
        }
    except ValueError as exc:
        raise HistoricalPriceIntegrityError("price row width disagrees with its header") from exc


def _decimal(value: str, field_name: str) -> Decimal:
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError) as exc:
        raise HistoricalPriceIntegrityError(f"{field_name} is not a decimal") from exc
    if not parsed.is_finite():
        raise HistoricalPriceIntegrityError(f"{field_name} must be finite")
    return parsed


def _positive_integer(value: str, field_name: str) -> int:
    if not value.isascii() or not value.isdigit():
        raise HistoricalPriceIntegrityError(f"{field_name} is not an unsigned integer")
    parsed = int(value)
    if parsed <= 0:
        raise HistoricalPriceIntegrityError(f"{field_name} must be positive")
    return parsed


def _non_negative_integer(value: str, field_name: str) -> int:
    if not value.isascii() or not value.isdigit():
        raise HistoricalPriceIntegrityError(f"{field_name} is not an unsigned integer")
    return int(value)


def _require_price_report(
    report: ParsedDailyReport,
    *,
    family: DailyReportFamily,
    market_session: date,
) -> None:
    if type(report) is not ParsedDailyReport:
        raise TypeError("price reports must be exact ParsedDailyReport values")
    if (
        report.family is not family
        or report.disposition is not BundleEntryDisposition.SELECTED_VALIDATED
        or report.claimed_report_date != market_session
        or report.confirmed_row_dates != (market_session,)
        or report.date_status is not ReportDateStatus.ROW_CONFIRMED
        or report.date_role is not ReportDateRole.TRADE_DATE
        or report.row_count <= 0
        or report.row_count != len(report.rows)
    ):
        raise HistoricalPriceIntegrityError(
            "price report is not an exact selected row-confirmed session report"
        )
    expected_header = (
        UDIFF_BHAVCOPY_HEADER
        if family is DailyReportFamily.UDIFF_BHAVCOPY
        else FULL_BHAVCOPY_DELIVERY_HEADER
    )
    if report.header != expected_header:
        raise HistoricalPriceIntegrityError("price report header is outside the pinned schema")


def _report_ref(
    source: StoredDailyBundleArtifact,
    report: ParsedDailyReport,
) -> PriceReportRef:
    manifest = source.manifest
    assert report.claimed_report_date is not None
    return PriceReportRef(
        bundle_artifact_id=manifest.artifact_id,
        bundle_manifest_id=manifest.manifest_id,
        bundle_raw_sha256=manifest.raw_sha256,
        bundle_normalized_sha256=manifest.normalized_sha256,
        family=report.family,
        source_entry_name=report.source_entry_name,
        content_name=report.content_name,
        source_entry_sha256=report.source_entry_sha256,
        content_sha256=report.content_sha256,
        header_sha256=report.header_sha256,
        ordered_row_digest=report.ordered_row_digest,
        claimed_report_date=report.claimed_report_date,
        confirmed_row_dates=report.confirmed_row_dates,
        date_status=report.date_status,
        date_role=report.date_role,
        first_seen_at=manifest.first_seen_at,
        validated_at=manifest.validated_at,
        row_count=report.row_count,
    )


def _row_ref(
    report_ref: PriceReportRef,
    *,
    source_row_number: int,
    row: tuple[str, ...],
    listing_key: tuple[str, str],
) -> PriceRowRef:
    return PriceRowRef(
        report_ref_id=report_ref.report_ref_id,
        family=report_ref.family,
        source_row_number=source_row_number,
        row_sha256=_row_sha256(row),
        listing_key=listing_key,
    )


def materialize_nse_eod_session(
    source: StoredDailyBundleArtifact,
    *,
    market_session: date,
    cutoff: datetime,
) -> NseEodSessionArtifact:
    """Materialize every traded UDiFF row without claiming universe completeness."""

    if type(source) is not StoredDailyBundleArtifact:
        raise TypeError("source must be an exact StoredDailyBundleArtifact")
    if type(market_session) is not date:
        raise TypeError("market_session must be a date")
    if not isinstance(cutoff, datetime) or cutoff.tzinfo is None or cutoff.utcoffset() is None:
        raise ValueError("cutoff must be a timezone-aware datetime")
    try:
        verify_stored_daily_bundle_provenance(source)
    except DailyReportIntegrityError as exc:
        raise HistoricalPriceIntegrityError(
            "daily-bundle source does not match sealed provenance"
        ) from exc
    if source.manifest.validated_at > cutoff:
        raise HistoricalPriceIntegrityError("daily bundle was not validated by the cutoff")

    reports: dict[DailyReportFamily, ParsedDailyReport] = {}
    for family in _REPORT_FAMILY_ORDER:
        matches = [
            report
            for report in source.parsed.reports
            if report.family is family and report.claimed_report_date == market_session
        ]
        if len(matches) != 1:
            raise HistoricalPriceIntegrityError(
                "session requires exactly one UDiFF/full-delivery report pair"
            )
        report = matches[0]
        _require_price_report(report, family=family, market_session=market_session)
        reports[family] = report

    report_refs = tuple(_report_ref(source, reports[family]) for family in _REPORT_FAMILY_ORDER)
    udiff_report = reports[DailyReportFamily.UDIFF_BHAVCOPY]
    full_report = reports[DailyReportFamily.FULL_BHAVCOPY_DELIVERY]
    udiff_ref, full_ref = report_refs

    full_by_listing: dict[
        tuple[str, str],
        tuple[dict[str, str], PriceRowRef],
    ] = {}
    for row_number, row in enumerate(full_report.rows, start=2):
        values = _field_map(full_report, row)
        listing_key = (values["SYMBOL"], values["SERIES"])
        if listing_key in full_by_listing:
            raise HistoricalPriceIntegrityError("full report contains duplicate listing rows")
        full_by_listing[listing_key] = (
            values,
            _row_ref(
                full_ref,
                source_row_number=row_number,
                row=row,
                listing_key=listing_key,
            ),
        )

    bars: list[RawNseEodBar] = []
    for row_number, row in enumerate(udiff_report.rows, start=2):
        values = _field_map(udiff_report, row)
        listing_key = (values["TckrSymb"], values["SctySrs"])
        full_match = full_by_listing.pop(listing_key, None)
        full_average_price: Decimal | None = None
        delivery_quantity: int | None = None
        delivery_percent: Decimal | None = None
        full_row_ref: PriceRowRef | None = None
        if full_match is not None:
            full_values, full_row_ref = full_match
            full_average_price = _decimal(
                full_values["AVG_PRICE"],
                "full Bhavcopy average price",
            )
            if full_values["DELIV_QTY"] == "-" and full_values["DELIV_PER"] == "-":
                pass
            elif "-" in {full_values["DELIV_QTY"], full_values["DELIV_PER"]}:
                raise HistoricalPriceIntegrityError(
                    "full Bhavcopy delivery missingness is inconsistent"
                )
            else:
                delivery_quantity = _non_negative_integer(
                    full_values["DELIV_QTY"],
                    "full Bhavcopy delivery quantity",
                )
                delivery_percent = _decimal(
                    full_values["DELIV_PER"],
                    "full Bhavcopy delivery percent",
                )

        bars.append(
            RawNseEodBar(
                market_session=market_session,
                financial_instrument_id=_positive_integer(
                    values["FinInstrmId"],
                    "UDiFF financial instrument ID",
                ),
                validated_isin=values["ISIN"],
                symbol=values["TckrSymb"],
                series=values["SctySrs"],
                session_id=values["SsnId"],
                instrument_name=values["FinInstrmNm"],
                open=_decimal(values["OpnPric"], "UDiFF open"),
                high=_decimal(values["HghPric"], "UDiFF high"),
                low=_decimal(values["LwPric"], "UDiFF low"),
                close=_decimal(values["ClsPric"], "UDiFF close"),
                last=_decimal(values["LastPric"], "UDiFF last"),
                previous_close=_decimal(values["PrvsClsgPric"], "UDiFF previous close"),
                volume=_positive_integer(values["TtlTradgVol"], "UDiFF volume"),
                traded_value=_decimal(values["TtlTrfVal"], "UDiFF traded value"),
                trade_count=_positive_integer(
                    values["TtlNbOfTxsExctd"],
                    "UDiFF trade count",
                ),
                board_lot_quantity=_positive_integer(
                    values["NewBrdLotQty"],
                    "UDiFF board lot quantity",
                ),
                full_average_price=full_average_price,
                delivery_quantity=delivery_quantity,
                delivery_percent=delivery_percent,
                knowledge_time=source.manifest.validated_at,
                udiff_row_ref=_row_ref(
                    udiff_ref,
                    source_row_number=row_number,
                    row=row,
                    listing_key=listing_key,
                ),
                full_delivery_row_ref=full_row_ref,
            )
        )
    if full_by_listing:
        raise HistoricalPriceIntegrityError(
            "full-delivery report contains rows absent from same-session UDiFF"
        )

    return NseEodSessionArtifact(
        exchange="NSE",
        segment="CM",
        market_session=market_session,
        cutoff=cutoff,
        knowledge_time=source.manifest.validated_at,
        source_bundle_manifest=source.manifest,
        report_refs=report_refs,
        bars=tuple(
            sorted(
                bars,
                key=lambda value: (
                    value.symbol,
                    value.series,
                    value.financial_instrument_id,
                ),
            )
        ),
    )
