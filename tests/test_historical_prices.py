from __future__ import annotations

import csv
import io
import json
import tempfile
import unittest
import zipfile
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from india_swing.daily_reports.artifact_store import LocalDailyBundleArtifactStore
from india_swing.daily_reports.models import DailyReportFamily
from india_swing.daily_reports.parser import (
    COMPLETE_PRICE_BANDS_HEADER,
    FULL_BHAVCOPY_DELIVERY_HEADER,
    NSE_DAILY_BUNDLE_FILENAME,
    PRICE_BAND_CHANGES_HEADER,
    REG1_SURVEILLANCE_HEADER,
    SERIES_CHANGES_HEADER,
    SME_PRICE_BANDS_HEADER,
    UDIFF_BHAVCOPY_HEADER,
)
from india_swing.historical_prices import (
    RAW_UNADJUSTED,
    TRADED_ROWS_ONLY,
    HistoricalPriceIntegrityError,
    encode_historical_price_artifact,
    materialize_nse_eod_session,
)
from india_swing.reference.models import ReferenceReadiness


UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
SESSION = date(2026, 7, 15)
FIRST_SEEN = datetime(2026, 7, 15, 14, 30, tzinfo=UTC)
VALIDATED = FIRST_SEEN + timedelta(seconds=2)
CUTOFF = datetime(2026, 7, 15, 15, 0, tzinfo=UTC)


def _csv(header: tuple[str, ...], rows: list[list[str]]) -> bytes:
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    return stream.getvalue().encode("utf-8")


def _zip(entries: list[tuple[str, bytes]]) -> bytes:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, payload in entries:
            info = zipfile.ZipInfo(name, date_time=(2026, 7, 15, 12, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, payload)
    return stream.getvalue()


def _clock(*values: datetime):
    iterator = iter(values)
    return lambda: next(iterator)


def _udiff_row(**overrides: str) -> list[str]:
    values = {name: "" for name in UDIFF_BHAVCOPY_HEADER}
    values.update(
        {
            "TradDt": SESSION.isoformat(),
            "BizDt": SESSION.isoformat(),
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


def _full_row() -> list[str]:
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
    names = tuple(name.strip() for name in FULL_BHAVCOPY_DELIVERY_HEADER)
    return [values[name] if index == 0 else f" {values[name]}" for index, name in enumerate(names)]


def _reg1_row(symbol: str, series: str) -> list[str]:
    values = {
        name: (
            ""
            if name.startswith("Filler")
            or name in {"ScripCode", "Symbol", "Nse Exclusive", "Status", "Series"}
            else "100"
        )
        for name in REG1_SURVEILLANCE_HEADER
    }
    values.update(
        {
            "ScripCode": "NA",
            "Symbol": symbol,
            "Nse Exclusive": "N",
            "Status": "A",
            "Series": series,
        }
    )
    return [values[name] for name in REG1_SURVEILLANCE_HEADER]


def _bundle_bytes() -> bytes:
    udiff_name = "BhavCopy_NSE_CM_0_0_0_20260715_F_0000.csv"
    udiff = _csv(
        UDIFF_BHAVCOPY_HEADER,
        [
            _udiff_row(),
            _udiff_row(
                FinInstrmId="3000",
                ISIN="INE470A01017",
                TckrSymb="FUND",
                SctySrs="IV",
                FinInstrmNm="FUND OBSERVATION",
            ),
        ],
    )
    return _zip(
        [
            (f"{udiff_name}.zip", _zip([(udiff_name, udiff)])),
            (
                "sec_bhavdata_full_15072026.csv",
                _csv(FULL_BHAVCOPY_DELIVERY_HEADER, [_full_row()]),
            ),
            (
                "REG1_IND140726.csv",
                _csv(REG1_SURVEILLANCE_HEADER, [_reg1_row("INFY", "EQ")]),
            ),
            (
                "sec_list_14072026.csv",
                _csv(
                    COMPLETE_PRICE_BANDS_HEADER,
                    [["INFY", "EQ", "INFOSYS LIMITED", "20", "-"]],
                ),
            ),
            (
                "sme_bands_complete_15072026.csv",
                _csv(
                    SME_PRICE_BANDS_HEADER,
                    [["SMECO", "SM", "SME COMPANY LIMITED", "5", "-"]],
                ),
            ),
            (
                "eq_band_changes_15072026.csv",
                _csv(
                    PRICE_BAND_CHANGES_HEADER,
                    [["1", "INFY", "EQ", "INFOSYS LIMITED", "10", "20"]],
                ),
            ),
            (
                "series_change.csv",
                _csv(
                    SERIES_CHANGES_HEADER,
                    [["INFY", "INFOSYS LIMITED", "BE", "EQ", "15-JUL-2026", "-"]],
                ),
            ),
        ]
    )


class HistoricalPriceMaterializationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        source = self.root / "source" / NSE_DAILY_BUNDLE_FILENAME
        source.parent.mkdir()
        source.write_bytes(_bundle_bytes())
        self.bundle = LocalDailyBundleArtifactStore(
            self.root / "store",
            clock=_clock(FIRST_SEEN, VALIDATED),
        ).import_bundle(source)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def materialize(self, cutoff: datetime = CUTOFF):
        return materialize_nse_eod_session(
            self.bundle,
            market_session=SESSION,
            cutoff=cutoff,
        )

    def test_every_udiff_row_is_retained_raw_and_collection_only(self) -> None:
        artifact = self.materialize()

        self.assertEqual([bar.listing_key for bar in artifact.bars], [("FUND", "IV"), ("INFY", "EQ")])
        self.assertEqual(len(artifact.bars), artifact.report_refs[0].row_count)
        self.assertEqual(
            {bar.udiff_row_ref.source_row_number for bar in artifact.bars},
            {2, 3},
        )
        fund, infy = artifact.bars
        self.assertIsNone(fund.full_delivery_row_ref)
        self.assertEqual(infy.open, Decimal("1600.00"))
        self.assertEqual(infy.traded_value, Decimal("160500.00"))
        self.assertEqual(infy.delivery_quantity, 50)
        self.assertEqual(infy.delivery_percent, Decimal("50.00"))
        self.assertEqual(infy.full_delivery_row_ref.source_row_number, 2)
        self.assertEqual(artifact.price_basis, RAW_UNADJUSTED)
        self.assertEqual(artifact.coverage_scope, TRADED_ROWS_ONLY)
        self.assertIs(artifact.readiness, ReferenceReadiness.COLLECTION_ONLY)
        self.assertFalse(artifact.actionable)
        self.assertEqual(artifact.source_bundle_manifest, self.bundle.manifest)
        self.assertEqual(artifact.knowledge_time, VALIDATED)

    def test_encoding_embeds_full_manifest_and_is_timezone_canonical(self) -> None:
        utc_artifact = self.materialize(CUTOFF)
        ist_artifact = self.materialize(CUTOFF.astimezone(IST))

        self.assertEqual(utc_artifact.artifact_id, ist_artifact.artifact_id)
        self.assertEqual(
            encode_historical_price_artifact(utc_artifact),
            encode_historical_price_artifact(ist_artifact),
        )
        payload = json.loads(encode_historical_price_artifact(utc_artifact))
        encoded_manifest = payload["source_bundle_manifest"]
        self.assertEqual(encoded_manifest["manifest_id"], self.bundle.manifest.manifest_id)
        self.assertEqual(encoded_manifest["raw_sha256"], self.bundle.manifest.raw_sha256)
        self.assertEqual(encoded_manifest["selected_row_count"], self.bundle.manifest.selected_row_count)
        self.assertEqual(payload["cutoff"], CUTOFF.isoformat())

    def test_cutoff_and_exact_report_pair_fail_closed(self) -> None:
        with self.assertRaisesRegex(HistoricalPriceIntegrityError, "not validated"):
            self.materialize(VALIDATED - timedelta(microseconds=1))
        with self.assertRaisesRegex(HistoricalPriceIntegrityError, "exactly one"):
            materialize_nse_eod_session(
                self.bundle,
                market_session=date(2026, 7, 14),
                cutoff=CUTOFF,
            )
        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            self.materialize(datetime(2026, 7, 15, 15, 0))

    def test_raw_and_manifest_substitution_fail_sealed_provenance(self) -> None:
        forged_raw = replace(self.bundle, raw_bytes=self.bundle.raw_bytes + b"x")
        with self.assertRaisesRegex(HistoricalPriceIntegrityError, "sealed provenance"):
            materialize_nse_eod_session(
                forged_raw,
                market_session=SESSION,
                cutoff=CUTOFF,
            )

        forged_manifest = replace(
            self.bundle.manifest,
            validated_at=self.bundle.manifest.validated_at + timedelta(seconds=1),
        )
        with self.assertRaisesRegex(HistoricalPriceIntegrityError, "sealed provenance"):
            materialize_nse_eod_session(
                replace(self.bundle, manifest=forged_manifest),
                market_session=SESSION,
                cutoff=CUTOFF,
            )

    def test_parsed_tree_substitution_is_rejected_by_full_raw_reparse(self) -> None:
        reports = tuple(
            report
            for report in self.bundle.parsed.reports
            if report.family is not DailyReportFamily.FULL_BHAVCOPY_DELIVERY
        )
        forged_parsed = replace(self.bundle.parsed, reports=reports)
        with self.assertRaisesRegex(HistoricalPriceIntegrityError, "sealed provenance"):
            materialize_nse_eod_session(
                replace(self.bundle, parsed=forged_parsed),
                market_session=SESSION,
                cutoff=CUTOFF,
            )

    def test_artifact_enforces_exact_row_ownership_and_count(self) -> None:
        artifact = self.materialize()
        with self.assertRaisesRegex(HistoricalPriceIntegrityError, "every UDiFF row"):
            replace(artifact, bars=artifact.bars[:-1])

        first, second = artifact.bars
        duplicate_ref = replace(
            second.udiff_row_ref,
            source_row_number=first.udiff_row_ref.source_row_number,
        )
        duplicate_bar = replace(second, udiff_row_ref=duplicate_ref)
        with self.assertRaisesRegex(HistoricalPriceIntegrityError, "every UDiFF row"):
            replace(artifact, bars=(first, duplicate_bar))

    def test_full_rows_cannot_be_detached_and_floats_are_rejected(self) -> None:
        artifact = self.materialize()
        fund, infy = artifact.bars
        detached = replace(
            infy,
            full_delivery_row_ref=None,
            full_average_price=None,
            delivery_quantity=None,
            delivery_percent=None,
        )
        with self.assertRaisesRegex(HistoricalPriceIntegrityError, "full-delivery row"):
            replace(artifact, bars=(fund, detached))
        with self.assertRaisesRegex(TypeError, "Decimal"):
            replace(infy, open=1600.0)

    def test_nonactionability_and_content_identity_are_immutable(self) -> None:
        artifact = self.materialize()
        with self.assertRaisesRegex(ValueError, "collection-only"):
            replace(artifact, actionable=True)
        object.__setattr__(artifact, "artifact_id", "0" * 64)
        with self.assertRaisesRegex(HistoricalPriceIntegrityError, "identity"):
            encode_historical_price_artifact(artifact)


if __name__ == "__main__":
    unittest.main()
