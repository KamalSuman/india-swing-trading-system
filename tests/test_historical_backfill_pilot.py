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
from india_swing.historical_prices import materialize_nse_eod_session
from india_swing.market_data.backfill import (
    HistoricalBackfillCompletion,
    HistoricalBackfillProgress,
    HistoricalBackfillRunner,
    LocalHistoricalBackfillProgressStore,
)
from india_swing.market_data.backfill_pilot import (
    MAXIMUM_PILOT_TOTAL_REQUESTS,
    HistoricalBackfillPilotCompletion,
    HistoricalBackfillPilotError,
    HistoricalBackfillPilotIntegrityError,
    HistoricalBackfillPilotReconciliation,
    HistoricalBackfillPilotResult,
    HistoricalBackfillPilotService,
)
from india_swing.market_data.collection import (
    HistoricalReconciliationCollector,
    historical_dataset_name,
)
from india_swing.market_data.models import (
    HistoricalDailyCandle,
    HistoricalDailyCandleBatch,
    HistoricalResponsePage,
)
from india_swing.market_data.reconciliation import (
    HISTORICAL_RECONCILIATION_DATASET,
    HistoricalReconciliationStatus,
    reconcile_historical_batch,
)
from india_swing.market_data.snapshot_store import LocalMarketSnapshotStore
from tests.test_historical_backfill import DAY_ONE, RUN_CLOCK, plan
from tests.test_identity_registry import security_row, tcs_row


UTC = timezone.utc
OBSERVED_AT = datetime(2026, 7, 17, 11, 0, tzinfo=UTC)
FIRST_SEEN = datetime(2026, 7, 15, 14, 30, tzinfo=UTC)
VALIDATED = FIRST_SEEN + timedelta(seconds=2)
CUTOFF = datetime(2026, 7, 15, 15, 0, tzinfo=UTC)
RECONCILED_AT = datetime(2026, 7, 22, 10, 0, tzinfo=UTC)
EXTRA_SESSION = date(2026, 7, 1)


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


def _udiff_row(symbol: str, isin: str, fin_instrument_id: str) -> list[str]:
    values = {name: "" for name in UDIFF_BHAVCOPY_HEADER}
    values.update(
        {
            "TradDt": DAY_ONE.isoformat(),
            "BizDt": DAY_ONE.isoformat(),
            "Sgmt": "CM",
            "Src": "NSE",
            "FinInstrmTp": "STK",
            "FinInstrmId": fin_instrument_id,
            "ISIN": isin,
            "TckrSymb": symbol,
            "SctySrs": "EQ",
            "FinInstrmNm": f"{symbol} LIMITED",
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
    return [values[name] for name in UDIFF_BHAVCOPY_HEADER]


def _full_row(symbol: str) -> list[str]:
    values = {
        "SYMBOL": symbol,
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
    return [
        values[name] if index == 0 else f" {values[name]}"
        for index, name in enumerate(names)
    ]


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
            _udiff_row("INFY", "INE009A01021", "1594"),
            _udiff_row("TCS", "INE467B01029", "11536"),
        ],
    )
    return _zip(
        [
            (f"{udiff_name}.zip", _zip([(udiff_name, udiff)])),
            (
                "sec_bhavdata_full_15072026.csv",
                _csv(
                    FULL_BHAVCOPY_DELIVERY_HEADER,
                    [_full_row("INFY"), _full_row("TCS")],
                ),
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


def nse_artifact(root: Path):
    source = root / "source" / NSE_DAILY_BUNDLE_FILENAME
    source.parent.mkdir(parents=True)
    source.write_bytes(_bundle_bytes())
    bundle = LocalDailyBundleArtifactStore(
        root / "daily",
        clock=_clock(FIRST_SEEN, VALIDATED),
    ).import_bundle(source)
    return materialize_nse_eod_session(
        bundle,
        market_session=DAY_ONE,
        cutoff=CUTOFF,
    )


def _with_market_session(artifact, new_session: date):
    """Pure object-level relabeling used only to exercise coverage mismatches."""

    new_refs = tuple(
        replace(
            ref,
            claimed_report_date=new_session,
            confirmed_row_dates=(new_session,),
        )
        for ref in artifact.report_refs
    )
    ref_id_map = dict(
        zip(
            (ref.report_ref_id for ref in artifact.report_refs),
            (ref.report_ref_id for ref in new_refs),
        )
    )
    new_bars = tuple(
        replace(
            bar,
            market_session=new_session,
            udiff_row_ref=replace(
                bar.udiff_row_ref,
                report_ref_id=ref_id_map[bar.udiff_row_ref.report_ref_id],
            ),
            full_delivery_row_ref=(
                replace(
                    bar.full_delivery_row_ref,
                    report_ref_id=ref_id_map[
                        bar.full_delivery_row_ref.report_ref_id
                    ],
                )
                if bar.full_delivery_row_ref is not None
                else None
            ),
        )
        for bar in artifact.bars
    )
    return replace(
        artifact,
        market_session=new_session,
        report_refs=new_refs,
        bars=new_bars,
    )


def pilot_plan(root: Path):
    return plan(
        root,
        first_rows=[security_row(), tcs_row()],
        second_rows=[security_row(), tcs_row()],
        coverage_start=DAY_ONE,
        coverage_end=DAY_ONE,
    )


class FakePilotConnector:
    provider = "UPSTOX"
    provider_version = "fake-pilot-connector/v1"

    def __init__(self, *, close_by_listing_key: dict[str, str] | None = None) -> None:
        self.calls: list = []
        self.close_by_listing_key = close_by_listing_key or {}

    def fetch_historical_daily(self, request) -> HistoricalDailyCandleBatch:
        self.calls.append(request)
        close = Decimal(
            self.close_by_listing_key.get(request.binding.listing_key, "1610.00")
        )
        candles = tuple(
            HistoricalDailyCandle(
                session=session,
                open=Decimal("1600.00"),
                high=Decimal("1620.00"),
                low=Decimal("1590.00"),
                close=close,
                volume=100,
            )
            for session in request.sessions
        )
        page = HistoricalResponsePage(
            first_session=request.sessions[0],
            last_session=request.sessions[-1],
            payload_sha256="a" * 64,
            row_count=len(request.sessions),
        )
        return HistoricalDailyCandleBatch(
            request=request,
            observed_at=OBSERVED_AT,
            provider_version=self.provider_version,
            candles=candles,
            response_pages=(page,),
        )


class HistoricalBackfillPilotServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.plan = pilot_plan(self.root / "inputs")
        self.artifact = nse_artifact(self.root / "nse")
        self.snapshot_store = LocalMarketSnapshotStore(self.root / "snapshots")
        self.progress_store = LocalHistoricalBackfillProgressStore(
            self.root / "progress"
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def service(self, connector) -> HistoricalBackfillPilotService:
        runner = HistoricalBackfillRunner(
            connector,
            self.snapshot_store,
            self.progress_store,
            clock=lambda: RUN_CLOCK,
        )
        return HistoricalBackfillPilotService(
            runner, HistoricalReconciliationCollector(self.snapshot_store)
        )

    def test_empty_progress_collects_exactly_the_selected_prefix_in_plan_order(
        self,
    ) -> None:
        connector = FakePilotConnector()
        service = self.service(connector)

        result = service.run(
            self.plan, (self.artifact,), 2, reconciled_at=RECONCILED_AT
        )

        self.assertEqual(
            result.selected_request_ids,
            tuple(value.request_id for value in self.plan.requests),
        )
        self.assertEqual(
            [value.binding.listing_key for value in connector.calls],
            ["NSE:INFY", "NSE:TCS"],
        )
        self.assertIsInstance(result, HistoricalBackfillPilotResult)
        self.assertTrue(result.passed)
        self.assertTrue(result.collection_only)
        self.assertFalse(result.actionable)
        self.assertEqual(result.maximum_total_requests, 2)
        result.verify_content_identity()

    def test_cap_below_plan_size_selects_only_the_deterministic_prefix(self) -> None:
        connector = FakePilotConnector()
        service = self.service(connector)

        result = service.run(
            self.plan, (self.artifact,), 1, reconciled_at=RECONCILED_AT
        )

        self.assertEqual(len(result.selected_request_ids), 1)
        self.assertEqual(
            result.selected_request_ids[0], self.plan.requests[0].request_id
        )
        self.assertEqual(len(connector.calls), 1)

    def test_resume_respects_total_cap_and_repeat_run_is_connector_free(self) -> None:
        connector = FakePilotConnector()
        service = self.service(connector)

        first = service.run(
            self.plan, (self.artifact,), 1, reconciled_at=RECONCILED_AT
        )
        self.assertEqual(len(connector.calls), 1)
        self.assertEqual(len(first.selected_request_ids), 1)

        second = service.run(
            self.plan, (self.artifact,), 2, reconciled_at=RECONCILED_AT
        )
        self.assertEqual(len(connector.calls), 2)
        self.assertEqual(len(second.selected_request_ids), 2)

        third = service.run(
            self.plan, (self.artifact,), 2, reconciled_at=RECONCILED_AT
        )
        self.assertEqual(len(connector.calls), 2)
        self.assertEqual(third.result_id, second.result_id)

    def test_completed_retry_rejects_changed_connector_lineage(self) -> None:
        connector = FakePilotConnector()
        self.service(connector).run(
            self.plan, (self.artifact,), 2, reconciled_at=RECONCILED_AT
        )
        changed_connector = FakePilotConnector()
        changed_connector.provider_version = "fake-pilot-connector/v2"
        service = self.service(changed_connector)

        with self.assertRaisesRegex(
            HistoricalBackfillPilotError, "progress lineage mismatch"
        ):
            service.run(
                self.plan, (self.artifact,), 2, reconciled_at=RECONCILED_AT
            )

        self.assertEqual(changed_connector.calls, [])

    def test_completion_outside_selected_prefix_fails_before_connector_call(
        self,
    ) -> None:
        connector = FakePilotConnector()
        foreign_completion = HistoricalBackfillCompletion(
            request_id="9" * 64,
            snapshot_id="8" * 64,
            completed_at=RUN_CLOCK,
            recovered_existing=False,
        )
        forged = HistoricalBackfillProgress(
            plan_id=self.plan.plan_id,
            provider=self.plan.provider,
            connector_version=connector.provider_version,
            completions=(foreign_completion,),
            updated_at=RUN_CLOCK,
        )
        self.progress_store.save(forged)
        service = self.service(connector)

        with self.assertRaisesRegex(
            HistoricalBackfillPilotError, "outside the selected prefix"
        ):
            service.run(self.plan, (self.artifact,), 1, reconciled_at=RECONCILED_AT)

        self.assertEqual(connector.calls, [])

    def test_completion_snapshot_lineage_mismatch_is_rejected(self) -> None:
        connector = FakePilotConnector()
        service = self.service(connector)
        result = service.run(
            self.plan, (self.artifact,), 2, reconciled_at=RECONCILED_AT
        )
        infy_id = result.selected_request_ids[0]
        tcs_snapshot_id = result.completions[1].snapshot_id

        with self.assertRaises(HistoricalBackfillPilotError):
            service._verify_completed_batch(
                infy_id, tcs_snapshot_id, self.plan.provider, result.connector_version
            )

    def test_completion_snapshot_provider_version_mismatch_is_rejected(self) -> None:
        connector = FakePilotConnector()
        service = self.service(connector)
        result = service.run(
            self.plan, (self.artifact,), 2, reconciled_at=RECONCILED_AT
        )
        infy_id = result.selected_request_ids[0]
        infy_snapshot_id = result.completions[0].snapshot_id

        with self.assertRaises(HistoricalBackfillPilotError):
            service._verify_completed_batch(
                infy_id, infy_snapshot_id, self.plan.provider, "wrong-connector/v9"
            )

    def test_completion_missing_snapshot_is_rejected(self) -> None:
        connector = FakePilotConnector()
        service = self.service(connector)
        result = service.run(
            self.plan, (self.artifact,), 2, reconciled_at=RECONCILED_AT
        )
        infy_id = result.selected_request_ids[0]

        with self.assertRaises(HistoricalBackfillPilotError):
            service._verify_completed_batch(
                infy_id, "0" * 64, self.plan.provider, result.connector_version
            )

    def test_completion_snapshot_wrong_payload_type_is_rejected(self) -> None:
        connector = FakePilotConnector()
        service = self.service(connector)
        result = service.run(
            self.plan, (self.artifact,), 2, reconciled_at=RECONCILED_AT
        )
        infy_id = result.selected_request_ids[0]
        infy_request = self.plan.requests[0]
        wrong_type_report = reconcile_historical_batch(
            connector.fetch_historical_daily(infy_request),
            (self.artifact,),
            reconciled_at=RECONCILED_AT,
        )
        stored = self.snapshot_store.put(
            dataset=historical_dataset_name(self.plan.provider),
            selection_key=infy_id,
            provider=self.plan.provider,
            provider_version=result.connector_version,
            observed_at=RUN_CLOCK,
            normalized_payload=wrong_type_report,
        )

        with self.assertRaises(HistoricalBackfillPilotError):
            service._verify_completed_batch(
                infy_id,
                stored.manifest.snapshot_id,
                self.plan.provider,
                result.connector_version,
            )

    def test_completion_snapshot_disk_tampering_is_detected(self) -> None:
        connector = FakePilotConnector()
        service = self.service(connector)
        result = service.run(
            self.plan, (self.artifact,), 2, reconciled_at=RECONCILED_AT
        )
        infy_id = result.selected_request_ids[0]
        infy_snapshot_id = result.completions[0].snapshot_id
        stored = self.snapshot_store.get(
            historical_dataset_name(self.plan.provider), infy_snapshot_id
        )
        manifest_path = stored.path / "manifest.json"
        manifest_value = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest_value["provider_version"] = "tampered-version/v1"
        manifest_path.write_text(json.dumps(manifest_value), encoding="utf-8")

        with self.assertRaises(HistoricalBackfillPilotError):
            service._verify_completed_batch(
                infy_id,
                infy_snapshot_id,
                self.plan.provider,
                result.connector_version,
            )

    def test_persisted_reconciliation_lineage_mismatch_is_rejected(self) -> None:
        connector = FakePilotConnector()
        service = self.service(connector)
        first = service.run(
            self.plan, (self.artifact,), 2, reconciled_at=RECONCILED_AT
        )
        tcs_report_snapshot = self.snapshot_store.get(
            HISTORICAL_RECONCILIATION_DATASET,
            first.reconciliations[1].report_snapshot_id,
        )

        class SwappingCollector:
            def collect(self, report):
                return tcs_report_snapshot

        service.reconciliation_collector = SwappingCollector()

        with self.assertRaises(HistoricalBackfillPilotError):
            service.run(self.plan, (self.artifact,), 2, reconciled_at=RECONCILED_AT)

    def test_persisted_reconciliation_manifest_provider_and_version_are_bound(
        self,
    ) -> None:
        connector = FakePilotConnector()
        service = self.service(connector)
        first = service.run(
            self.plan, (self.artifact,), 2, reconciled_at=RECONCILED_AT
        )
        stored_report = self.snapshot_store.get(
            HISTORICAL_RECONCILIATION_DATASET,
            first.reconciliations[0].report_snapshot_id,
        )

        for field_name, wrong_value in (
            ("provider", "WRONG_PROVIDER"),
            ("provider_version", "wrong-policy/v1"),
        ):
            with self.subTest(field_name=field_name):
                forged = replace(
                    stored_report,
                    manifest=replace(
                        stored_report.manifest,
                        **{field_name: wrong_value},
                    ),
                )

                class ForgedManifestCollector:
                    def collect(self, report):
                        return forged

                service.reconciliation_collector = ForgedManifestCollector()
                with self.assertRaisesRegex(
                    HistoricalBackfillPilotError, "snapshot lineage mismatch"
                ):
                    service.run(
                        self.plan,
                        (self.artifact,),
                        2,
                        reconciled_at=RECONCILED_AT,
                    )

    def test_nse_artifacts_missing_selected_session_is_rejected(self) -> None:
        connector = FakePilotConnector()
        service = self.service(connector)
        misplaced = _with_market_session(self.artifact, EXTRA_SESSION)

        with self.assertRaises(HistoricalBackfillPilotError):
            service.run(self.plan, (misplaced,), 2, reconciled_at=RECONCILED_AT)
        self.assertEqual(connector.calls, [])

    def test_nse_artifacts_extra_session_is_rejected(self) -> None:
        connector = FakePilotConnector()
        service = self.service(connector)
        extra = _with_market_session(self.artifact, EXTRA_SESSION)

        with self.assertRaises(HistoricalBackfillPilotError):
            service.run(
                self.plan, (self.artifact, extra), 2, reconciled_at=RECONCILED_AT
            )
        self.assertEqual(connector.calls, [])

    def test_nse_artifacts_must_be_session_unique(self) -> None:
        connector = FakePilotConnector()
        service = self.service(connector)

        with self.assertRaises(HistoricalBackfillPilotError):
            service.run(
                self.plan,
                (self.artifact, self.artifact),
                2,
                reconciled_at=RECONCILED_AT,
            )
        self.assertEqual(connector.calls, [])

    def test_nse_artifacts_wrong_type_is_rejected(self) -> None:
        connector = FakePilotConnector()
        service = self.service(connector)

        with self.assertRaises(HistoricalBackfillPilotError):
            service.run(
                self.plan, ("not-an-artifact",), 2, reconciled_at=RECONCILED_AT
            )
        with self.assertRaises(HistoricalBackfillPilotError):
            service.run(self.plan, [self.artifact], 2, reconciled_at=RECONCILED_AT)
        with self.assertRaises(HistoricalBackfillPilotError):
            service.run(self.plan, (), 2, reconciled_at=RECONCILED_AT)
        self.assertEqual(connector.calls, [])

    def test_nse_artifacts_tampering_is_detected(self) -> None:
        connector = FakePilotConnector()
        service = self.service(connector)
        object.__setattr__(self.artifact, "artifact_id", "0" * 64)

        with self.assertRaises(HistoricalBackfillPilotError):
            service.run(self.plan, (self.artifact,), 2, reconciled_at=RECONCILED_AT)
        self.assertEqual(connector.calls, [])

    def test_reconciled_at_before_evidence_fails_closed(self) -> None:
        connector = FakePilotConnector()
        service = self.service(connector)

        with self.assertRaises(HistoricalBackfillPilotError):
            service.run(
                self.plan,
                (self.artifact,),
                2,
                reconciled_at=OBSERVED_AT - timedelta(days=1),
            )

    def test_reconciled_at_must_be_aware_datetime(self) -> None:
        connector = FakePilotConnector()
        service = self.service(connector)

        with self.assertRaises(HistoricalBackfillPilotError):
            service.run(
                self.plan,
                (self.artifact,),
                2,
                reconciled_at=datetime(2026, 7, 22, 10, 0),
            )
        with self.assertRaises(HistoricalBackfillPilotError):
            service.run(self.plan, (self.artifact,), 2, reconciled_at="not-a-datetime")

    def test_maximum_total_requests_bad_values_rejected_before_any_call(self) -> None:
        connector = FakePilotConnector()
        service = self.service(connector)
        for bad in (True, False, 0, -1, MAXIMUM_PILOT_TOTAL_REQUESTS + 1, "2", 2.0):
            with self.subTest(bad=bad):
                with self.assertRaises(HistoricalBackfillPilotError):
                    service.run(
                        self.plan, (self.artifact,), bad, reconciled_at=RECONCILED_AT
                    )
        self.assertEqual(connector.calls, [])

    def test_wrong_plan_type_is_rejected(self) -> None:
        connector = FakePilotConnector()
        service = self.service(connector)

        with self.assertRaises(HistoricalBackfillPilotError):
            service.run(
                "not-a-plan", (self.artifact,), 2, reconciled_at=RECONCILED_AT
            )
        self.assertEqual(connector.calls, [])

    def test_empty_plan_is_rejected(self) -> None:
        suspended = security_row(SctyStsNrmlMkt="1", ElgbltyNrmlMkt="0")
        empty = plan(
            self.root / "empty-inputs",
            first_rows=[suspended],
            second_rows=[suspended],
            coverage_start=DAY_ONE,
            coverage_end=DAY_ONE,
        )
        connector = FakePilotConnector()
        service = self.service(connector)

        with self.assertRaisesRegex(HistoricalBackfillPilotError, "non-empty plan"):
            service.run(empty, (self.artifact,), 2, reconciled_at=RECONCILED_AT)
        self.assertEqual(connector.calls, [])

    def test_blocking_plan_is_rejected(self) -> None:
        from tests.test_historical_backfill import DAY_ZERO, calendar

        blocking = plan(
            self.root / "blocking-inputs",
            selected_calendar=calendar(DAY_ZERO, DAY_ONE),
            coverage_start=DAY_ZERO,
            coverage_end=DAY_ONE,
        )
        self.assertTrue(blocking.has_blocking_issues)
        connector = FakePilotConnector()
        service = self.service(connector)

        with self.assertRaisesRegex(HistoricalBackfillPilotError, "blocking plan"):
            service.run(blocking, (self.artifact,), 2, reconciled_at=RECONCILED_AT)
        self.assertEqual(connector.calls, [])

    def test_mismatched_close_produces_failed_reconciliation_not_an_exception(
        self,
    ) -> None:
        connector = FakePilotConnector(close_by_listing_key={"NSE:INFY": "1608.00"})
        service = self.service(connector)

        result = service.run(
            self.plan, (self.artifact,), 2, reconciled_at=RECONCILED_AT
        )

        self.assertFalse(result.passed)
        self.assertFalse(result.actionable)
        self.assertTrue(result.collection_only)
        self.assertFalse(result.reconciliations[0].passed)
        self.assertTrue(result.reconciliations[1].passed)
        result.verify_content_identity()

    def test_missing_nse_bar_produces_failed_reconciliation_not_an_exception(
        self,
    ) -> None:
        original_request = self.plan.requests[0]
        missing_binding = replace(
            original_request.binding,
            listing_key="NSE:WIPRO",
            isin="INE075A01022",
            provider_instrument_id="NSE_EQ|INE075A01022",
        )
        missing_request = replace(
            original_request,
            binding=missing_binding,
        )
        missing_plan = replace(
            self.plan,
            requests=(missing_request,),
        )
        connector = FakePilotConnector()
        service = self.service(connector)

        result = service.run(
            missing_plan, (self.artifact,), 1, reconciled_at=RECONCILED_AT
        )

        self.assertFalse(result.passed)
        self.assertFalse(result.actionable)
        stored_report = self.snapshot_store.get(
            HISTORICAL_RECONCILIATION_DATASET,
            result.reconciliations[0].report_snapshot_id,
        )
        self.assertEqual(
            stored_report.normalized_payload.rows[0].status,
            HistoricalReconciliationStatus.MISSING_NSE_BAR,
        )

    def test_wrong_service_construction_types_are_rejected(self) -> None:
        connector = FakePilotConnector()
        runner = HistoricalBackfillRunner(
            connector,
            self.snapshot_store,
            self.progress_store,
            clock=lambda: RUN_CLOCK,
        )
        collector = HistoricalReconciliationCollector(self.snapshot_store)

        with self.assertRaises(TypeError):
            HistoricalBackfillPilotService("not-a-runner", collector)
        with self.assertRaises(TypeError):
            HistoricalBackfillPilotService(runner, "not-a-collector")


class HistoricalBackfillPilotResultTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.plan = pilot_plan(self.root / "inputs")
        self.artifact = nse_artifact(self.root / "nse")
        self.snapshot_store = LocalMarketSnapshotStore(self.root / "snapshots")
        self.progress_store = LocalHistoricalBackfillProgressStore(
            self.root / "progress"
        )
        connector = FakePilotConnector()
        runner = HistoricalBackfillRunner(
            connector,
            self.snapshot_store,
            self.progress_store,
            clock=lambda: RUN_CLOCK,
        )
        service = HistoricalBackfillPilotService(
            runner, HistoricalReconciliationCollector(self.snapshot_store)
        )
        self.result = service.run(
            self.plan, (self.artifact,), 2, reconciled_at=RECONCILED_AT
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_valid_result_round_trips_identity(self) -> None:
        self.result.verify_content_identity()
        self.assertTrue(self.result.passed)
        self.assertTrue(self.result.collection_only)
        self.assertFalse(self.result.actionable)

    def test_maximum_total_requests_out_of_range_values_rejected(self) -> None:
        for bad in (True, False, 0, -1, MAXIMUM_PILOT_TOTAL_REQUESTS + 1):
            with self.subTest(bad=bad):
                with self.assertRaises((TypeError, ValueError)):
                    replace(self.result, maximum_total_requests=bad)

    def test_selected_request_ids_duplicate_is_rejected(self) -> None:
        ids = self.result.selected_request_ids
        with self.assertRaises(ValueError):
            replace(self.result, selected_request_ids=(ids[0], ids[0]))

    def test_completions_must_exactly_cover_prefix_in_order(self) -> None:
        with self.assertRaises(ValueError):
            replace(
                self.result, completions=tuple(reversed(self.result.completions))
            )
        with self.assertRaises(ValueError):
            replace(self.result, completions=self.result.completions[:1])

    def test_reconciliations_must_exactly_cover_prefix_in_order(self) -> None:
        with self.assertRaises(ValueError):
            replace(
                self.result,
                reconciliations=tuple(reversed(self.result.reconciliations)),
            )
        with self.assertRaises(ValueError):
            replace(self.result, reconciliations=self.result.reconciliations[:1])

    def test_passed_flag_must_match_reconciliation_rows(self) -> None:
        with self.assertRaises(ValueError):
            replace(self.result, passed=not self.result.passed)

    def test_collection_only_and_actionable_are_fixed(self) -> None:
        with self.assertRaises(ValueError):
            replace(self.result, collection_only=False)
        with self.assertRaises(ValueError):
            replace(self.result, actionable=True)

    def test_unsupported_schema_and_policy_are_rejected(self) -> None:
        with self.assertRaises(ValueError):
            replace(self.result, schema_version="wrong/v0")
        with self.assertRaises(ValueError):
            replace(self.result, policy_version="wrong/v0")

    def test_forged_ids_are_rejected(self) -> None:
        with self.assertRaises(ValueError):
            replace(self.result, plan_id="not-a-sha256")
        with self.assertRaises(ValueError):
            replace(self.result, progress_id="not-a-sha256")
        with self.assertRaises(ValueError):
            replace(self.result, provider="lower_case_bad")

    def test_forged_result_id_is_detected(self) -> None:
        object.__setattr__(self.result, "result_id", "0" * 64)
        with self.assertRaises(HistoricalBackfillPilotIntegrityError):
            self.result.verify_content_identity()

    def test_nested_row_tampering_is_detected_and_result_id_untouched(self) -> None:
        original_result_id = self.result.result_id
        object.__setattr__(self.result.completions[0], "snapshot_id", "0" * 64)

        with self.assertRaises(HistoricalBackfillPilotIntegrityError):
            self.result.verify_content_identity()
        self.assertEqual(self.result.result_id, original_result_id)


class HistoricalBackfillPilotRowTests(unittest.TestCase):
    def test_completion_row_rejects_bad_ids(self) -> None:
        with self.assertRaises(ValueError):
            HistoricalBackfillPilotCompletion(request_id="short", snapshot_id="a" * 64)
        with self.assertRaises(ValueError):
            HistoricalBackfillPilotCompletion(request_id="a" * 64, snapshot_id="short")

    def test_completion_row_tamper_detected(self) -> None:
        row = HistoricalBackfillPilotCompletion(
            request_id="a" * 64, snapshot_id="b" * 64
        )
        object.__setattr__(row, "snapshot_id", "c" * 64)
        with self.assertRaises(HistoricalBackfillPilotIntegrityError):
            row.verify_content_identity()

    def test_reconciliation_row_rejects_bad_passed_type(self) -> None:
        with self.assertRaises(TypeError):
            HistoricalBackfillPilotReconciliation(
                request_id="a" * 64,
                report_id="b" * 64,
                report_snapshot_id="c" * 64,
                passed=1,
            )

    def test_reconciliation_row_tamper_detected(self) -> None:
        row = HistoricalBackfillPilotReconciliation(
            request_id="a" * 64,
            report_id="b" * 64,
            report_snapshot_id="c" * 64,
            passed=True,
        )
        object.__setattr__(row, "passed", False)
        with self.assertRaises(HistoricalBackfillPilotIntegrityError):
            row.verify_content_identity()


if __name__ == "__main__":
    unittest.main()
