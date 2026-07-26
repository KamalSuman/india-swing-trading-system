from __future__ import annotations

import json
import os
import tempfile
import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from india_swing.daily_reports.artifact_store import LocalDailyBundleArtifactStore
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
    HistoricalPriceIntegrityError,
    materialize_nse_eod_session,
)
from india_swing.historical_prices.models import NseEodSessionArtifact
from india_swing.market_data.backfill_gaps import (
    HistoricalBackfillGapClassification,
    HistoricalBackfillGapIntegrityError,
)
from india_swing.market_data.gap_adjudication import (
    HISTORICAL_GAP_ADJUDICATION_DATASET,
    REPORT_FILENAME,
    HistoricalGapAdjudicationAction,
    HistoricalGapAdjudicationError,
    HistoricalGapAdjudicationIntegrityError,
    HistoricalGapNseStatus,
    LocalHistoricalGapAdjudicationReportStore,
    build_historical_gap_adjudication_report,
    decode_historical_gap_adjudication_report,
    encode_historical_gap_adjudication_report,
)
from tests.test_historical_backfill_gaps import gap_evidence
from tests.test_historical_prices import (
    CUTOFF,
    FIRST_SEEN,
    SESSION,
    VALIDATED,
    _bundle_bytes,
    _clock,
    _csv,
    _reg1_row,
    _zip,
)
from tests.test_historical_reconciliation import nse_artifact


UTC = timezone.utc
ADJUDICATED_AT = datetime(2026, 7, 17, 10, 0, tzinfo=UTC)
SESSION2 = date(2026, 7, 16)
FIRST_SEEN2 = datetime(2026, 7, 16, 14, 30, tzinfo=UTC)
VALIDATED2 = FIRST_SEEN2 + timedelta(seconds=2)


def _full_row2() -> list[str]:
    values = {
        "SYMBOL": "INFY",
        "SERIES": "EQ",
        "DATE1": "16-Jul-2026",
        "PREV_CLOSE": "1610.00",
        "OPEN_PRICE": "1611.00",
        "HIGH_PRICE": "1625.00",
        "LOW_PRICE": "1600.00",
        "LAST_PRICE": "1615.00",
        "CLOSE_PRICE": "1616.00",
        "AVG_PRICE": "1616.00",
        "TTL_TRD_QNTY": "110",
        "TURNOVER_LACS": "1.78",
        "NO_OF_TRADES": "11",
        "DELIV_QTY": "55",
        "DELIV_PER": "50.00",
    }
    names = tuple(name.strip() for name in FULL_BHAVCOPY_DELIVERY_HEADER)
    return [values[name] if index == 0 else f" {values[name]}" for index, name in enumerate(names)]


def _udiff_row2(**overrides: str) -> list[str]:
    values = {name: "" for name in UDIFF_BHAVCOPY_HEADER}
    values.update(
        {
            "TradDt": SESSION2.isoformat(),
            "BizDt": SESSION2.isoformat(),
            "Sgmt": "CM",
            "Src": "NSE",
            "FinInstrmTp": "STK",
            "FinInstrmId": "1594",
            "ISIN": "INE009A01021",
            "TckrSymb": "INFY",
            "SctySrs": "EQ",
            "FinInstrmNm": "INFOSYS LIMITED",
            "OpnPric": "1611.00",
            "HghPric": "1625.00",
            "LwPric": "1600.00",
            "ClsPric": "1616.00",
            "LastPric": "1615.00",
            "PrvsClsgPric": "1610.00",
            "TtlTradgVol": "110",
            "TtlTrfVal": "177760.00",
            "TtlNbOfTxsExctd": "11",
            "SsnId": "F1",
            "NewBrdLotQty": "1",
        }
    )
    values.update(overrides)
    return [values[name] for name in UDIFF_BHAVCOPY_HEADER]


def _bundle_bytes2() -> bytes:
    udiff_name = "BhavCopy_NSE_CM_0_0_0_20260716_F_0000.csv"
    udiff = _csv(UDIFF_BHAVCOPY_HEADER, [_udiff_row2()])
    return _zip(
        [
            (f"{udiff_name}.zip", _zip([(udiff_name, udiff)])),
            (
                "sec_bhavdata_full_16072026.csv",
                _csv(FULL_BHAVCOPY_DELIVERY_HEADER, [_full_row2()]),
            ),
            (
                "REG1_IND150726.csv",
                _csv(REG1_SURVEILLANCE_HEADER, [_reg1_row("INFY", "EQ")]),
            ),
            (
                "sec_list_15072026.csv",
                _csv(
                    COMPLETE_PRICE_BANDS_HEADER,
                    [["INFY", "EQ", "INFOSYS LIMITED", "20", "-"]],
                ),
            ),
            (
                "sme_bands_complete_16072026.csv",
                _csv(
                    SME_PRICE_BANDS_HEADER,
                    [["SMECO", "SM", "SME COMPANY LIMITED", "5", "-"]],
                ),
            ),
            (
                "eq_band_changes_16072026.csv",
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


def nse_artifact2(root: Path) -> NseEodSessionArtifact:
    source = root / "source" / NSE_DAILY_BUNDLE_FILENAME
    source.parent.mkdir(parents=True)
    source.write_bytes(_bundle_bytes2())
    bundle = LocalDailyBundleArtifactStore(
        root / "daily",
        clock=_clock(FIRST_SEEN2, VALIDATED2),
    ).import_bundle(source)
    return materialize_nse_eod_session(
        bundle,
        market_session=SESSION2,
        cutoff=CUTOFF + timedelta(days=1),
    )


def _conflicted_bundle_bytes() -> bytes:
    from tests.test_historical_prices import _udiff_row, _full_row

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
            _udiff_row(
                FinInstrmId="9999",
                ISIN="INE009A01021",
                TckrSymb="OLDINFY",
                SctySrs="BE",
                FinInstrmNm="INFY OLD SERIES",
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


def conflicted_artifact(root: Path) -> NseEodSessionArtifact:
    source = root / "source" / NSE_DAILY_BUNDLE_FILENAME
    source.parent.mkdir(parents=True)
    source.write_bytes(_conflicted_bundle_bytes())
    bundle = LocalDailyBundleArtifactStore(
        root / "daily",
        clock=_clock(FIRST_SEEN, VALIDATED),
    ).import_bundle(source)
    return materialize_nse_eod_session(bundle, market_session=SESSION, cutoff=CUTOFF)


class BuildReportClassificationTests(unittest.TestCase):
    def test_exact_traded_bar_present_for_both_original_classifications(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact = nse_artifact(Path(temp_dir))
            empty_gap = gap_evidence(request_id="b" * 64)
            rejection_gap = gap_evidence(
                request_id="d" * 64,
                classification=(
                    HistoricalBackfillGapClassification.UNRESOLVED_PROVIDER_REQUEST_REJECTION
                ),
            )
            report = build_historical_gap_adjudication_report(
                gaps=(empty_gap, rejection_gap),
                nse_artifacts=(artifact,),
                adjudicated_at=ADJUDICATED_AT,
            )

        infy_bar = next(bar for bar in artifact.bars if bar.symbol == "INFY")
        self.assertEqual(len(report.entries), 2)
        for entry in report.entries:
            self.assertEqual(entry.status, HistoricalGapNseStatus.EXACT_TRADED_BAR_PRESENT)
            self.assertEqual(entry.exact_nse_bar_id, infy_bar.bar_id)
            self.assertEqual(entry.related_nse_bar_ids, ())
            self.assertEqual(
                entry.action,
                HistoricalGapAdjudicationAction.REVIEW_PINNED_NSE_BAR_FOR_DATASET_USE,
            )
            self.assertEqual(entry.nse_artifact_id, artifact.artifact_id)
            self.assertIs(entry.collection_only, True)
            self.assertIs(entry.actionable, False)
            self.assertIs(entry.gap_resolved, False)
            self.assertIs(entry.training_eligible, False)
        self.assertEqual(
            {entry.original_classification for entry in report.entries},
            {
                HistoricalBackfillGapClassification.UNRESOLVED_EMPTY_PROVIDER_RESPONSE,
                HistoricalBackfillGapClassification.UNRESOLVED_PROVIDER_REQUEST_REJECTION,
            },
        )
        self.assertIs(report.collection_only, True)
        self.assertIs(report.actionable, False)
        self.assertIs(report.gaps_resolved, False)
        self.assertIs(report.training_eligible, False)

    def test_absent_traded_row_never_claims_no_trade_or_delisting(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact = nse_artifact(Path(temp_dir))
            gap = gap_evidence(
                listing_key="NSE:TCS",
                security_series="EQ",
                isin="INE467B01029",
            )
            report = build_historical_gap_adjudication_report(
                gaps=(gap,),
                nse_artifacts=(artifact,),
                adjudicated_at=ADJUDICATED_AT,
            )

        entry = report.entries[0]
        self.assertEqual(entry.status, HistoricalGapNseStatus.NSE_TRADED_BAR_ABSENT)
        self.assertIsNone(entry.exact_nse_bar_id)
        self.assertEqual(entry.related_nse_bar_ids, ())
        self.assertEqual(
            entry.action,
            HistoricalGapAdjudicationAction.OBTAIN_LISTING_OR_ALTERNATE_PROVIDER_EVIDENCE,
        )
        rendered = json.dumps(entry.status.value) + entry.action.value
        for banned in ("NO_TRADE", "SUSPEND", "DELIST", "INVALID_LISTING"):
            self.assertNotIn(banned, rendered)

    def test_same_isin_changed_symbol_series_is_a_conflict(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact = nse_artifact(Path(temp_dir))
            gap = gap_evidence(
                listing_key="NSE:OLDINFY",
                security_series="EQ",
                isin="INE009A01021",
            )
            report = build_historical_gap_adjudication_report(
                gaps=(gap,),
                nse_artifacts=(artifact,),
                adjudicated_at=ADJUDICATED_AT,
            )

        infy_bar = next(bar for bar in artifact.bars if bar.symbol == "INFY")
        entry = report.entries[0]
        self.assertEqual(entry.status, HistoricalGapNseStatus.RELATED_NSE_IDENTITY_CONFLICT)
        self.assertIsNone(entry.exact_nse_bar_id)
        self.assertEqual(entry.related_nse_bar_ids, (infy_bar.bar_id,))
        self.assertEqual(
            entry.action,
            HistoricalGapAdjudicationAction.REVIEW_POINT_IN_TIME_IDENTITY,
        )

    def test_same_symbol_series_conflicting_isin_is_a_conflict(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact = nse_artifact(Path(temp_dir))
            gap = gap_evidence(
                listing_key="NSE:INFY",
                security_series="EQ",
                isin="INE467B01029",
            )
            report = build_historical_gap_adjudication_report(
                gaps=(gap,),
                nse_artifacts=(artifact,),
                adjudicated_at=ADJUDICATED_AT,
            )

        infy_bar = next(bar for bar in artifact.bars if bar.symbol == "INFY")
        entry = report.entries[0]
        self.assertEqual(entry.status, HistoricalGapNseStatus.RELATED_NSE_IDENTITY_CONFLICT)
        self.assertEqual(entry.related_nse_bar_ids, (infy_bar.bar_id,))

    def test_exact_bar_coexists_with_conflicting_related_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact = conflicted_artifact(Path(temp_dir))
            gap = gap_evidence()
            report = build_historical_gap_adjudication_report(
                gaps=(gap,),
                nse_artifacts=(artifact,),
                adjudicated_at=ADJUDICATED_AT,
            )

        infy_bar = next(bar for bar in artifact.bars if bar.symbol == "INFY")
        old_bar = next(bar for bar in artifact.bars if bar.symbol == "OLDINFY")
        entry = report.entries[0]
        self.assertEqual(entry.status, HistoricalGapNseStatus.RELATED_NSE_IDENTITY_CONFLICT)
        self.assertIsNone(entry.exact_nse_bar_id)
        self.assertEqual(
            entry.related_nse_bar_ids,
            tuple(sorted((infy_bar.bar_id, old_bar.bar_id))),
        )

    def test_multiple_exact_bars_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact = nse_artifact(Path(temp_dir))
            infy = next(bar for bar in artifact.bars if bar.symbol == "INFY")
            duplicate = replace(
                infy, financial_instrument_id=infy.financial_instrument_id + 1
            )
            object.__setattr__(artifact, "bars", artifact.bars + (duplicate,))
            object.__setattr__(
                artifact, "artifact_id", artifact._calculated_artifact_id()
            )

            with self.assertRaises(HistoricalGapAdjudicationIntegrityError):
                build_historical_gap_adjudication_report(
                    gaps=(gap_evidence(),),
                    nse_artifacts=(artifact,),
                    adjudicated_at=ADJUDICATED_AT,
                )

    def test_report_id_is_independent_of_nse_artifact_input_order(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            artifact_one = nse_artifact(root / "one")
            artifact_two = nse_artifact2(root / "two")
            gap_one = gap_evidence(request_id="b" * 64, session=SESSION)
            gap_two = gap_evidence(
                request_id="d" * 64,
                session=SESSION2,
                response_observed_at=datetime(2026, 7, 16, 17, 0, tzinfo=UTC),
            )

            forward = build_historical_gap_adjudication_report(
                gaps=(gap_one, gap_two),
                nse_artifacts=(artifact_one, artifact_two),
                adjudicated_at=ADJUDICATED_AT,
            )
            backward = build_historical_gap_adjudication_report(
                gaps=(gap_one, gap_two),
                nse_artifacts=(artifact_two, artifact_one),
                adjudicated_at=ADJUDICATED_AT,
            )

        self.assertEqual(forward.report_id, backward.report_id)
        self.assertEqual(
            forward.nse_artifact_ids,
            (artifact_one.artifact_id, artifact_two.artifact_id),
        )
        self.assertEqual(forward.nse_artifact_ids, backward.nse_artifact_ids)


def _two_session_report(root: Path):
    artifact_one = nse_artifact(root / "one")
    artifact_two = nse_artifact2(root / "two")
    gap_one = gap_evidence(request_id="b" * 64, session=SESSION)
    gap_two = gap_evidence(
        request_id="d" * 64,
        session=SESSION2,
        response_observed_at=datetime(2026, 7, 16, 17, 0, tzinfo=UTC),
    )
    return build_historical_gap_adjudication_report(
        gaps=(gap_one, gap_two),
        nse_artifacts=(artifact_one, artifact_two),
        adjudicated_at=ADJUDICATED_AT,
    )


class BuildReportValidationTests(unittest.TestCase):
    def test_gaps_must_be_a_non_empty_exact_tuple(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact = nse_artifact(Path(temp_dir))
            for bad_gaps in ((), [gap_evidence()], (object(),)):
                with self.subTest(bad_gaps=bad_gaps):
                    with self.assertRaises(HistoricalGapAdjudicationError):
                        build_historical_gap_adjudication_report(
                            gaps=bad_gaps,
                            nse_artifacts=(artifact,),
                            adjudicated_at=ADJUDICATED_AT,
                        )

    def test_nse_artifacts_must_be_a_non_empty_exact_tuple(self) -> None:
        for bad_artifacts in ((), [], (object(),)):
            with self.subTest(bad_artifacts=bad_artifacts):
                with self.assertRaises(HistoricalGapAdjudicationError):
                    build_historical_gap_adjudication_report(
                        gaps=(gap_evidence(),),
                        nse_artifacts=bad_artifacts,
                        adjudicated_at=ADJUDICATED_AT,
                    )

    def test_cross_plan_gaps_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact = nse_artifact(Path(temp_dir))
            gap_one = gap_evidence(plan_id="a" * 64, request_id="b" * 64)
            gap_two = gap_evidence(plan_id="f" * 64, request_id="d" * 64)

            with self.assertRaises(HistoricalGapAdjudicationError):
                build_historical_gap_adjudication_report(
                    gaps=(gap_one, gap_two),
                    nse_artifacts=(artifact,),
                    adjudicated_at=ADJUDICATED_AT,
                )

    def test_duplicate_gaps_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact = nse_artifact(Path(temp_dir))
            gap = gap_evidence()

            with self.assertRaises(HistoricalGapAdjudicationError):
                build_historical_gap_adjudication_report(
                    gaps=(gap, gap),
                    nse_artifacts=(artifact,),
                    adjudicated_at=ADJUDICATED_AT,
                )

    def test_unsorted_gaps_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact = nse_artifact(Path(temp_dir))
            gap_b = gap_evidence(request_id="b" * 64)
            gap_c = gap_evidence(request_id="c" * 64)

            with self.assertRaises(HistoricalGapAdjudicationError):
                build_historical_gap_adjudication_report(
                    gaps=(gap_c, gap_b),
                    nse_artifacts=(artifact,),
                    adjudicated_at=ADJUDICATED_AT,
                )

    def test_missing_artifact_session_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            artifact_two = nse_artifact2(root / "two")
            gap_one = gap_evidence(request_id="b" * 64, session=SESSION)

            with self.assertRaises(HistoricalGapAdjudicationError):
                build_historical_gap_adjudication_report(
                    gaps=(gap_one,),
                    nse_artifacts=(artifact_two,),
                    adjudicated_at=ADJUDICATED_AT,
                )

    def test_extra_artifact_session_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            artifact_one = nse_artifact(root / "one")
            artifact_two = nse_artifact2(root / "two")
            gap_one = gap_evidence(request_id="b" * 64, session=SESSION)

            with self.assertRaises(HistoricalGapAdjudicationError):
                build_historical_gap_adjudication_report(
                    gaps=(gap_one,),
                    nse_artifacts=(artifact_one, artifact_two),
                    adjudicated_at=ADJUDICATED_AT,
                )

    def test_duplicate_artifact_sessions_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            artifact = nse_artifact(root)
            duplicate = replace(artifact)

            with self.assertRaises(HistoricalGapAdjudicationError):
                build_historical_gap_adjudication_report(
                    gaps=(gap_evidence(),),
                    nse_artifacts=(artifact, duplicate),
                    adjudicated_at=ADJUDICATED_AT,
                )

    def test_pre_evidence_adjudicated_at_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact = nse_artifact(Path(temp_dir))
            gap = gap_evidence()

            with self.assertRaises(HistoricalGapAdjudicationError):
                build_historical_gap_adjudication_report(
                    gaps=(gap,),
                    nse_artifacts=(artifact,),
                    adjudicated_at=gap.response_observed_at - timedelta(seconds=1),
                )
            with self.assertRaises(HistoricalGapAdjudicationError):
                build_historical_gap_adjudication_report(
                    gaps=(gap,),
                    nse_artifacts=(artifact,),
                    adjudicated_at=artifact.knowledge_time - timedelta(seconds=1),
                )

    def test_tampered_gap_content_identity_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact = nse_artifact(Path(temp_dir))
            gap = gap_evidence()
            object.__setattr__(gap, "isin", "INE467B01029")

            with self.assertRaises(HistoricalBackfillGapIntegrityError):
                build_historical_gap_adjudication_report(
                    gaps=(gap,),
                    nse_artifacts=(artifact,),
                    adjudicated_at=ADJUDICATED_AT,
                )

    def test_tampered_artifact_content_identity_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact = nse_artifact(Path(temp_dir))
            object.__setattr__(artifact, "market_session", date(2026, 7, 16))

            with self.assertRaises(HistoricalPriceIntegrityError):
                build_historical_gap_adjudication_report(
                    gaps=(gap_evidence(),),
                    nse_artifacts=(artifact,),
                    adjudicated_at=ADJUDICATED_AT,
                )


def _report(root: Path):
    artifact = nse_artifact(root)
    return build_historical_gap_adjudication_report(
        gaps=(gap_evidence(),),
        nse_artifacts=(artifact,),
        adjudicated_at=ADJUDICATED_AT,
    )


class CodecTests(unittest.TestCase):
    def test_exact_canonical_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            report = _report(Path(temp_dir))

        payload = encode_historical_gap_adjudication_report(report)
        decoded = decode_historical_gap_adjudication_report(payload)

        self.assertEqual(decoded, report)
        self.assertEqual(encode_historical_gap_adjudication_report(decoded), payload)

    def test_report_id_is_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            first = _report(root / "one")
            second = _report(root / "two")

        self.assertEqual(first.report_id, second.report_id)

    def test_duplicate_keys_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            report = _report(Path(temp_dir))
        payload = encode_historical_gap_adjudication_report(report)
        original = json.loads(payload)
        pairs = ",".join(
            f"{json.dumps(key)}:{json.dumps(value)}" for key, value in original.items()
        )
        duplicated = ("{" + pairs + f',"plan_id":{json.dumps(original["plan_id"])}' + "}").encode(
            "utf-8"
        )

        with self.assertRaises(HistoricalGapAdjudicationError):
            decode_historical_gap_adjudication_report(duplicated)

    def test_missing_key_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            report = _report(Path(temp_dir))
        original = json.loads(encode_historical_gap_adjudication_report(report))
        del original["plan_id"]

        with self.assertRaises(HistoricalGapAdjudicationIntegrityError):
            decode_historical_gap_adjudication_report(
                json.dumps(original).encode("utf-8")
            )

    def test_unknown_key_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            report = _report(Path(temp_dir))
        original = json.loads(encode_historical_gap_adjudication_report(report))
        original["unexpected"] = "x"

        with self.assertRaises(HistoricalGapAdjudicationIntegrityError):
            decode_historical_gap_adjudication_report(
                json.dumps(original).encode("utf-8")
            )

    def test_bool_as_int_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            report = _report(Path(temp_dir))
        original = json.loads(encode_historical_gap_adjudication_report(report))
        original["actionable"] = 0

        with self.assertRaises(HistoricalGapAdjudicationIntegrityError):
            decode_historical_gap_adjudication_report(
                json.dumps(original).encode("utf-8")
            )

    def test_nan_and_infinity_tokens_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            report = _report(Path(temp_dir))
        payload = encode_historical_gap_adjudication_report(report)
        text = payload.decode("utf-8")
        self.assertIn('"actionable":false', text)
        corrupted = text.replace('"actionable":false', '"actionable":NaN').encode("utf-8")

        with self.assertRaises(HistoricalGapAdjudicationIntegrityError):
            decode_historical_gap_adjudication_report(corrupted)

    def test_tampered_entry_field_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            report = _report(Path(temp_dir))
        original = json.loads(encode_historical_gap_adjudication_report(report))
        original["entries"][0]["provider_instrument_id"] = "999999"

        with self.assertRaises(HistoricalGapAdjudicationIntegrityError):
            decode_historical_gap_adjudication_report(
                json.dumps(original).encode("utf-8")
            )

    def test_tampered_report_id_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            report = _report(Path(temp_dir))
        original = json.loads(encode_historical_gap_adjudication_report(report))
        original["report_id"] = "0" * 64

        with self.assertRaises(HistoricalGapAdjudicationIntegrityError):
            decode_historical_gap_adjudication_report(
                json.dumps(original).encode("utf-8")
            )

    def test_nested_entry_object_tampering_detected_at_report_level(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            report = _report(Path(temp_dir))
        object.__setattr__(report.entries[0], "isin", "INE467B01029")

        with self.assertRaises(HistoricalGapAdjudicationIntegrityError):
            report.verify_content_identity()


class LocalHistoricalGapAdjudicationReportStoreTests(unittest.TestCase):
    def test_round_trip_and_idempotent_put(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            report = _report(root / "inputs")
            store = LocalHistoricalGapAdjudicationReportStore(root / "reports")

            stored = store.put(report)
            again = store.put(report)
            loaded = store.get(report.report_id)

        self.assertEqual(stored, report)
        self.assertEqual(again, report)
        self.assertEqual(loaded, report)

    def test_conflicting_content_at_the_same_path_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            report = _report(root / "inputs")
            store = LocalHistoricalGapAdjudicationReportStore(root / "reports")
            store.put(report)
            path = store.dataset_root / report.report_id / REPORT_FILENAME
            corrupted = json.loads(path.read_bytes())
            corrupted["adjudicated_at"] = (
                report.adjudicated_at + timedelta(seconds=1)
            ).isoformat()
            path.write_bytes(
                (json.dumps(corrupted, sort_keys=True, separators=(",", ":")) + "\n").encode(
                    "utf-8"
                )
            )

            with self.assertRaises(HistoricalGapAdjudicationIntegrityError):
                store.put(report)
            with self.assertRaises(HistoricalGapAdjudicationIntegrityError):
                store.get(report.report_id)

    def test_not_found_and_invalid_ids_are_distinct(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalHistoricalGapAdjudicationReportStore(Path(temp_dir))
            with self.assertRaises(ValueError):
                store.get("not-a-hash")
            with self.assertRaises(HistoricalGapAdjudicationError):
                store.get("a" * 64)

    def test_no_latest_list_find_or_select_operation_exists(self) -> None:
        store = LocalHistoricalGapAdjudicationReportStore(Path("unused"))
        for banned in ("latest", "list", "list_all", "find", "select"):
            self.assertFalse(hasattr(store, banned))

    def test_noncanonical_bytes_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            report = _report(root / "inputs")
            store = LocalHistoricalGapAdjudicationReportStore(root / "reports")
            store.put(report)
            path = store.dataset_root / report.report_id / REPORT_FILENAME
            value = json.loads(path.read_bytes())
            path.write_bytes(json.dumps(value, indent=2).encode("utf-8"))

            with self.assertRaises(HistoricalGapAdjudicationIntegrityError):
                store.get(report.report_id)

    def test_size_ceiling_is_enforced(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            report = _report(root / "inputs")
            store = LocalHistoricalGapAdjudicationReportStore(root / "reports")

            with patch(
                "india_swing.market_data.gap_adjudication."
                "MAXIMUM_GAP_ADJUDICATION_REPORT_BYTES",
                10,
            ):
                with self.assertRaises(HistoricalGapAdjudicationError):
                    store.put(report)

    def test_symlink_payload_directory_is_rejected_when_platform_allows_links(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            report = _report(root / "inputs")
            store = LocalHistoricalGapAdjudicationReportStore(root / "reports")
            store.put(report)
            real_dir = store.dataset_root / report.report_id
            link_dir = store.dataset_root / "linked"
            try:
                os.symlink(real_dir, link_dir, target_is_directory=True)
            except OSError as exc:
                self.skipTest(f"platform does not permit symlinks: {type(exc).__name__}")

            with self.assertRaises(HistoricalGapAdjudicationIntegrityError):
                store._read_path(link_dir)

    def test_atomic_publish_cleans_up_on_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            report = _report(root / "inputs")
            store = LocalHistoricalGapAdjudicationReportStore(root / "reports")

            with patch(
                "india_swing.market_data.gap_adjudication.os.replace",
                side_effect=OSError("publish failed"),
            ):
                with self.assertRaisesRegex(OSError, "publish failed"):
                    store.put(report)

            leftovers = [
                path
                for path in store.dataset_root.glob("*")
                if path.is_dir() and path.name != report.report_id
            ]
        self.assertEqual(leftovers, [])


class ReportInvariantRevalidationTests(unittest.TestCase):
    """Prove verify_content_identity re-derives every invariant, not just the hash.

    Every case here first builds a genuinely valid report, then tampers with
    it via object.__setattr__ (bypassing frozen-dataclass __post_init__) and
    patches report_id/entry_id to the recomputed hash of the tampered state.
    A caller that only checks 'does the hash match itself' would be fooled by
    this; verify_content_identity must not be.
    """

    def test_forged_fixed_flags_and_versions_are_rejected_even_with_recomputed_hash(
        self,
    ) -> None:
        mutations = {
            "collection_only": False,
            "actionable": True,
            "gaps_resolved": True,
            "training_eligible": True,
            "schema_version": "wrong-schema/v1",
            "policy_version": "wrong-policy/v1",
            "codec_version": "wrong-codec/v1",
        }
        for field_name, bad_value in mutations.items():
            with self.subTest(field=field_name):
                with tempfile.TemporaryDirectory() as temp_dir:
                    report = _report(Path(temp_dir))
                object.__setattr__(report, field_name, bad_value)
                object.__setattr__(report, "report_id", report._calculated_id())

                with self.assertRaises(ValueError):
                    report.verify_content_identity()

    def test_reordered_nse_artifact_ids_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            report = _two_session_report(Path(temp_dir))
        reversed_ids = tuple(reversed(report.nse_artifact_ids))
        self.assertNotEqual(reversed_ids, report.nse_artifact_ids)
        object.__setattr__(report, "nse_artifact_ids", reversed_ids)
        object.__setattr__(report, "report_id", report._calculated_id())

        with self.assertRaises(HistoricalGapAdjudicationIntegrityError):
            report.verify_content_identity()

    def test_unused_extra_artifact_id_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            report = _report(root / "single")
            extra_artifact = nse_artifact2(root / "extra")
        augmented_ids = report.nse_artifact_ids + (extra_artifact.artifact_id,)
        object.__setattr__(report, "nse_artifact_ids", augmented_ids)
        object.__setattr__(report, "report_id", report._calculated_id())

        with self.assertRaises(HistoricalGapAdjudicationIntegrityError):
            report.verify_content_identity()

    def test_missing_used_artifact_id_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            report = _two_session_report(Path(temp_dir))
        truncated_ids = report.nse_artifact_ids[:1]
        object.__setattr__(report, "nse_artifact_ids", truncated_ids)
        object.__setattr__(report, "report_id", report._calculated_id())

        with self.assertRaises(HistoricalGapAdjudicationIntegrityError):
            report.verify_content_identity()

    def test_two_entries_for_one_session_with_different_artifact_ids_are_rejected(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            artifact = nse_artifact(root / "same-session")
            other_artifact = nse_artifact2(root / "other-session")
            gap_one = gap_evidence(request_id="b" * 64)
            gap_two = gap_evidence(request_id="d" * 64)
            report = build_historical_gap_adjudication_report(
                gaps=(gap_one, gap_two),
                nse_artifacts=(artifact,),
                adjudicated_at=ADJUDICATED_AT,
            )

        tampered_entry = report.entries[1]
        object.__setattr__(
            tampered_entry, "nse_artifact_id", other_artifact.artifact_id
        )
        object.__setattr__(tampered_entry, "entry_id", tampered_entry._calculated_id())
        object.__setattr__(report, "report_id", report._calculated_id())

        with self.assertRaises(HistoricalGapAdjudicationIntegrityError):
            report.verify_content_identity()


class DecoderDirectPreconditionTests(unittest.TestCase):
    def test_non_bytes_input_is_rejected(self) -> None:
        with self.assertRaises(HistoricalGapAdjudicationIntegrityError):
            decode_historical_gap_adjudication_report("not-bytes")  # type: ignore[arg-type]

    def test_empty_bytes_is_rejected(self) -> None:
        with self.assertRaises(HistoricalGapAdjudicationIntegrityError):
            decode_historical_gap_adjudication_report(b"")

    def test_payload_above_the_fixed_size_limit_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            report = _report(Path(temp_dir))
        payload = encode_historical_gap_adjudication_report(report)

        with patch(
            "india_swing.market_data.gap_adjudication."
            "MAXIMUM_GAP_ADJUDICATION_REPORT_BYTES",
            len(payload) - 1,
        ):
            with self.assertRaises(HistoricalGapAdjudicationIntegrityError):
                decode_historical_gap_adjudication_report(payload)

    def test_ordinary_json_float_token_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            report = _report(Path(temp_dir))
        payload = encode_historical_gap_adjudication_report(report)
        text = payload.decode("utf-8")
        self.assertIn('"actionable":false', text)
        corrupted = text.replace('"actionable":false', '"actionable":0.5').encode(
            "utf-8"
        )

        with self.assertRaises(HistoricalGapAdjudicationIntegrityError):
            decode_historical_gap_adjudication_report(corrupted)

    def test_nan_and_infinity_tokens_are_rejected_directly_by_the_decoder(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            report = _report(Path(temp_dir))
        payload = encode_historical_gap_adjudication_report(report)
        text = payload.decode("utf-8")
        for replacement in ('"actionable":NaN', '"actionable":Infinity'):
            with self.subTest(replacement=replacement):
                corrupted = text.replace('"actionable":false', replacement).encode(
                    "utf-8"
                )
                with self.assertRaises(HistoricalGapAdjudicationIntegrityError):
                    decode_historical_gap_adjudication_report(corrupted)

    def test_missing_codec_version_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            report = _report(Path(temp_dir))
        original = json.loads(encode_historical_gap_adjudication_report(report))
        del original["codec_version"]

        with self.assertRaises(HistoricalGapAdjudicationIntegrityError):
            decode_historical_gap_adjudication_report(
                json.dumps(original).encode("utf-8")
            )

    def test_unknown_codec_version_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            report = _report(Path(temp_dir))
        original = json.loads(encode_historical_gap_adjudication_report(report))
        original["codec_version"] = "unknown-codec/v0"

        with self.assertRaises(HistoricalGapAdjudicationIntegrityError):
            decode_historical_gap_adjudication_report(
                json.dumps(original).encode("utf-8")
            )

    def test_noncanonical_but_structurally_equivalent_bytes_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            report = _report(Path(temp_dir))
        canonical = encode_historical_gap_adjudication_report(report)
        value = json.loads(canonical)
        noncanonical = json.dumps(value, indent=2).encode("utf-8")
        self.assertNotEqual(noncanonical, canonical)

        with self.assertRaises(HistoricalGapAdjudicationIntegrityError):
            decode_historical_gap_adjudication_report(noncanonical)


if __name__ == "__main__":
    unittest.main()
