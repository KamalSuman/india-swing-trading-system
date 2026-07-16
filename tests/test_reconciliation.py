from __future__ import annotations

import csv
import gzip
import io
import json
import os
import tempfile
import unittest
import zipfile
from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from india_swing.daily_reports.artifact_store import (
    MANIFEST_FILENAME as DAILY_MANIFEST_FILENAME,
    NORMALIZED_FILENAME as DAILY_NORMALIZED_FILENAME,
    RAW_FILENAME as DAILY_RAW_FILENAME,
    LocalDailyBundleArtifactStore,
    _artifact_identity as daily_artifact_identity,
    _manifest_json as daily_manifest_json,
    _manifest_identity as daily_manifest_identity,
)
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
from india_swing.reconciliation import (
    EffectiveSessionResolution,
    ReconciliationDisposition,
    ReconciliationIntegrityError,
    ReconciliationScope,
    encode_reconciliation,
    reconcile_collection_only,
)
from india_swing.reconciliation.cli import main as evidence_main
from india_swing.identity import content_id
from india_swing.reference.calendar import (
    CalendarDay,
    CalendarDayKind,
    CalendarSnapshot,
    SessionWindow,
    SessionWindowPhase,
)
from india_swing.reference.models import ExternalRecordRef, ReferenceReadiness
from india_swing.reference_data.artifact_store import (
    MANIFEST_FILENAME as REFERENCE_MANIFEST_FILENAME,
    NORMALIZED_FILENAME as REFERENCE_NORMALIZED_FILENAME,
    RAW_FILENAME as REFERENCE_RAW_FILENAME,
    LocalReferenceArtifactStore,
    _artifact_identity as reference_artifact_identity,
    _manifest_json as reference_manifest_json,
    _manifest_identity as reference_manifest_identity,
)
from india_swing.reference_data.security_master import NSE_CM_MII_SECURITY_HEADER


UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
SESSION = date(2026, 7, 15)
MASTER_FIRST_SEEN = datetime(2026, 7, 15, 12, 0, tzinfo=UTC)
MASTER_VALIDATED = MASTER_FIRST_SEEN + timedelta(seconds=2)
BUNDLE_FIRST_SEEN = datetime(2026, 7, 15, 14, 30, tzinfo=UTC)
BUNDLE_VALIDATED = BUNDLE_FIRST_SEEN + timedelta(seconds=2)
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


def _security_row(**overrides: str) -> list[str]:
    values = {name: "" for name in NSE_CM_MII_SECURITY_HEADER}
    values.update(
        {
            "FinInstrmId": "1594",
            "TckrSymb": "INFY",
            "SctySrs": "EQ",
            "FinInstrmNm": "INFOSYS LIMITED",
            "ISIN": "INE009A01021",
            "NewBrdLotQty": "1",
            "ParVal": "500",
            "SctyTpFlg": "0",
            "BidIntrvl": "5",
            "TrckgInd": "0",
            "CallAuctnInd": "1",
            "PrtdToTrad": "0",
            "PricRg": "0.00-99999.00",
            "SctyStsNrmlMkt": "6",
            "ElgbltyNrmlMkt": "1",
            "SctyStsOddLotMkt": "2",
            "ElgbltyOddLotMkt": "1",
            "SctyStsRETDBTMkt": "2",
            "ElgbltyRETDBTMkt": "0",
            "SctyStsAuctnMkt": "2",
            "ElgbltyAuctnMkt": "1",
            "SctyStsAddtlMkt1": "1",
            "ElgbltyAddtlMkt1": "0",
            "SctyStsAddtlMkt2": "1",
            "ElgbltyAddtlMkt2": "0",
            "ListgDt": "476668800",
            "RmvlDt": "0",
            "RadmssnDt": "0",
            "DelFlg": "N",
        }
    )
    values.update(overrides)
    return [values[name] for name in NSE_CM_MII_SECURITY_HEADER]


def _master_bytes() -> bytes:
    rows = [
        _security_row(),
        _security_row(
            FinInstrmId="2000",
            TckrSymb="RSDFIN",
            FinInstrmNm="RSD FINANCE LIMITED",
            ISIN="INE467B01029",
        ),
        _security_row(
            FinInstrmId="2001",
            TckrSymb="SMECO",
            SctySrs="SM",
            FinInstrmNm="SME COMPANY LIMITED",
            ISIN="INE144J01027",
        ),
        # Reusing an ISIN across series is deliberate: reconciliation must not
        # collapse same-vintage listing keys into a made-up stable identity.
        _security_row(
            FinInstrmId="2002",
            TckrSymb="OLD",
            SctySrs="BE",
            FinInstrmNm="OLD SERIES LIMITED",
            ISIN="INE009A01021",
        ),
    ]
    return gzip.compress(_csv(NSE_CM_MII_SECURITY_HEADER, rows), mtime=0)


def _udiff_row(*, trade_date: date = SESSION, **overrides: str) -> list[str]:
    values = {name: "" for name in UDIFF_BHAVCOPY_HEADER}
    values.update(
        {
            "TradDt": trade_date.isoformat(),
            "BizDt": trade_date.isoformat(),
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


def _full_row(*, symbol: str = "INFY", trade_date: date = SESSION) -> list[str]:
    values = {
        "SYMBOL": symbol,
        "SERIES": "EQ",
        "DATE1": trade_date.strftime("%d-%b-%Y"),
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


def _reg1_row(symbol: str, series: str, **overrides: str) -> list[str]:
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
    values.update(overrides)
    return [values[name] for name in REG1_SURVEILLANCE_HEADER]


def _bundle_bytes(
    *,
    trade_dates: tuple[date, ...] = (SESSION,),
    infy_band: str = "20",
    sme_band: str = "5",
    infy_financial_instrument_id: str = "1594",
    infy_udiff_symbol: str = "INFY",
    infy_udiff_isin: str = "INE009A01021",
    infy_udiff_name: str = "INFOSYS LIMITED",
    infy_udiff_board_lot: str = "1",
    band_change_from: str = "10",
    band_change_to: str = "20",
    series_change_symbol: str = "INFY",
    series_change_from: str = "BE",
    series_change_to: str = "EQ",
    ignored_entry: tuple[str, bytes] | None = None,
) -> bytes:
    if (
        type(trade_dates) is not tuple
        or not trade_dates
        or tuple(sorted(set(trade_dates))) != trade_dates
    ):
        raise ValueError("trade_dates must be a non-empty sorted unique tuple")
    final_report_entries: list[tuple[str, bytes]] = []
    for trade_date in trade_dates:
        date_token = trade_date.strftime("%Y%m%d")
        udiff_name = f"BhavCopy_NSE_CM_0_0_0_{date_token}_F_0000.csv"
        udiff = _csv(
            UDIFF_BHAVCOPY_HEADER,
            [
                _udiff_row(
                    trade_date=trade_date,
                    FinInstrmId=infy_financial_instrument_id,
                    TckrSymb=infy_udiff_symbol,
                    ISIN=infy_udiff_isin,
                    FinInstrmNm=infy_udiff_name,
                    NewBrdLotQty=infy_udiff_board_lot,
                ),
                _udiff_row(
                    trade_date=trade_date,
                    FinInstrmId="3000",
                    ISIN="INE470A01017",
                    TckrSymb="FUND",
                    SctySrs="IV",
                    FinInstrmNm="FUND OBSERVATION",
                ),
            ],
        )
        final_report_entries.extend(
            [
                (f"{udiff_name}.zip", _zip([(udiff_name, udiff)])),
                (
                    f"sec_bhavdata_full_{trade_date.strftime('%d%m%Y')}.csv",
                    _csv(
                        FULL_BHAVCOPY_DELIVERY_HEADER,
                        [
                            _full_row(
                                symbol=infy_udiff_symbol,
                                trade_date=trade_date,
                            )
                        ],
                    ),
                ),
            ]
        )
    entries = final_report_entries + [
            (
                "REG1_IND140726.csv",
                _csv(
                    REG1_SURVEILLANCE_HEADER,
                    [
                        _reg1_row("INFY", "EQ"),
                        _reg1_row("RSDFIN", "EQ"),
                        _reg1_row("SMECO", "SM", ESM="1"),
                    ],
                ),
            ),
            (
                "sec_list_14072026.csv",
                _csv(
                    COMPLETE_PRICE_BANDS_HEADER,
                    [
                        ["INFY", "EQ", "INFOSYS LIMITED", infy_band, "-"],
                        ["RSDFIN", "EQ", "RSD FINANCE LIMITED", "10", "-"],
                        ["SMECO", "SM", "SME COMPANY LIMITED", "5", "-"],
                    ],
                ),
            ),
            (
                "sme_bands_complete_15072026.csv",
                _csv(
                    SME_PRICE_BANDS_HEADER,
                    [["SMECO", "SM", "SME COMPANY LIMITED", sme_band, "-"]],
                ),
            ),
            (
                "eq_band_changes_15072026.csv",
                _csv(
                    PRICE_BAND_CHANGES_HEADER,
                    [
                        [
                            "1",
                            "INFY",
                            "EQ",
                            "INFOSYS LIMITED",
                            band_change_from,
                            band_change_to,
                        ]
                    ],
                ),
            ),
            (
                "series_change.csv",
                _csv(
                    SERIES_CHANGES_HEADER,
                    [
                        [
                            series_change_symbol,
                            (
                                "INFOSYS LIMITED"
                                if series_change_symbol == "INFY"
                                else "RSD FINANCE LIMITED"
                            ),
                            series_change_from,
                            series_change_to,
                            "15-JUL-2026",
                            "-",
                        ]
                    ],
                ),
            ),
        ]
    if ignored_entry is not None:
        entries.append(ignored_entry)
    return _zip(entries)


def _calendar() -> CalendarSnapshot:
    source_id = "9" * 64
    reference_time = datetime(2026, 7, 1, 0, 0, tzinfo=UTC)
    days = []
    for day in (date(2026, 7, 14), date(2026, 7, 15)):
        reference = ExternalRecordRef(
            event_time=datetime.combine(day, time(0), tzinfo=IST),
            knowledge_time=reference_time,
            source="NSE_SYNTHETIC_CALENDAR_FIXTURE",
            content_hash=("a" if day.day == 14 else "b") * 64,
            source_snapshot_id=source_id,
        )
        days.append(
            CalendarDay(
                day=day,
                kind=CalendarDayKind.REGULAR,
                reference=reference,
                session_windows=(
                    SessionWindow(
                        opens_at=datetime.combine(day, time(9, 15), tzinfo=IST),
                        closes_at=datetime.combine(day, time(15, 30), tzinfo=IST),
                        phase=SessionWindowPhase.LIVE_CONTINUOUS,
                    ),
                ),
                data_ready_at=datetime.combine(day, time(15, 45), tzinfo=IST),
            )
        )
    return CalendarSnapshot.create(
        exchange="NSE",
        segment="CM",
        cutoff=datetime(2026, 7, 15, 11, 0, tzinfo=UTC),
        coverage_start=date(2026, 7, 14),
        coverage_end=date(2026, 7, 15),
        days=tuple(days),
        source_snapshot_ids=(source_id,),
        readiness=ReferenceReadiness.SYNTHETIC_TEST,
    )


class CollectionReconciliationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        master_source = self.root / "source" / "NSE_CM_security_15072026.csv.gz"
        master_source.parent.mkdir()
        master_source.write_bytes(_master_bytes())
        self.master = LocalReferenceArtifactStore(
            self.root / "reference",
            clock=_clock(MASTER_FIRST_SEEN, MASTER_VALIDATED),
        ).import_security_master(master_source)
        self.bundle = self._import_bundle(_bundle_bytes(), "daily-one")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _import_bundle(
        self,
        payload: bytes,
        name: str,
        *,
        first_seen: datetime = BUNDLE_FIRST_SEEN,
        validated: datetime = BUNDLE_VALIDATED,
    ):
        source = self.root / name / NSE_DAILY_BUNDLE_FILENAME
        source.parent.mkdir()
        source.write_bytes(payload)
        return LocalDailyBundleArtifactStore(
            self.root / f"store-{name}",
            clock=_clock(first_seen, validated),
        ).import_bundle(source)

    def test_without_calendar_preserves_candidates_but_never_marks_them_effective(self) -> None:
        first = reconcile_collection_only(
            security_master=self.master,
            daily_bundles=(self.bundle,),
            market_session=SESSION,
            cutoff=CUTOFF,
        )
        second = reconcile_collection_only(
            security_master=self.master,
            daily_bundles=(self.bundle,),
            market_session=SESSION,
            cutoff=CUTOFF,
        )

        self.assertEqual(first.snapshot_id, second.snapshot_id)
        self.assertEqual(encode_reconciliation(first), encode_reconciliation(second))
        self.assertFalse(first.actionable)
        self.assertIs(first.readiness, ReferenceReadiness.COLLECTION_ONLY)
        self.assertEqual(first.retained_row_count, 4)
        self.assertEqual(first.main_scope_count, 2)
        self.assertEqual(first.sme_scope_count, 1)
        self.assertEqual(first.unsupported_series_count, 1)
        self.assertEqual(first.unresolved_count, 3)
        self.assertEqual(first.traded_row_count, 1)
        self.assertIn("EXPLICIT_CALENDAR_NOT_SUPPLIED", first.global_reason_codes)
        self.assertIn(
            "PUBLICATION_EFFECTIVE_STATE_UNRESOLVED",
            first.global_reason_codes,
        )
        infy = next(entry for entry in first.entries if entry.symbol == "INFY")
        rsdfin = next(entry for entry in first.entries if entry.symbol == "RSDFIN")
        self.assertEqual(len(infy.reg1_observations), 1)
        self.assertIsNone(infy.reg1_observations[0].effective_session)
        self.assertIsNone(infy.effective_reg1)
        self.assertIsNotNone(infy.udiff_trade_row)
        self.assertIsNotNone(infy.full_delivery_row)
        self.assertIsNone(rsdfin.udiff_trade_row)
        self.assertEqual(
            tuple(entry.source_record_id for entry in first.entries),
            first.retained_source_row_ids,
        )
        self.assertEqual(first.security_master_manifest_id, self.master.manifest.manifest_id)
        self.assertEqual(first.daily_bundle_manifest_ids, (self.bundle.manifest.manifest_id,))
        publication_bindings = [
            value
            for value in first.report_bindings
            if value.family.value in {"SURVEILLANCE_REG1", "COMPLETE_PRICE_BANDS"}
        ]
        self.assertTrue(publication_bindings)
        self.assertTrue(
            all(
                value.effective_session_resolution
                is EffectiveSessionResolution.UNRESOLVED_NO_CALENDAR
                for value in publication_bindings
            )
        )
        self.assertTrue(
            any(value.symbol == "FUND" and value.series == "IV" for value in first.orphan_report_keys)
        )

    def test_explicit_calendar_resolves_only_calendar_derived_publication_state(self) -> None:
        snapshot = reconcile_collection_only(
            security_master=self.master,
            daily_bundles=(self.bundle,),
            market_session=SESSION,
            cutoff=CUTOFF,
            calendar=_calendar(),
        )

        infy = next(entry for entry in snapshot.entries if entry.symbol == "INFY")
        rsdfin = next(entry for entry in snapshot.entries if entry.symbol == "RSDFIN")
        sme = next(entry for entry in snapshot.entries if entry.symbol == "SMECO")
        old = next(entry for entry in snapshot.entries if entry.symbol == "OLD")
        self.assertIs(infy.scope, ReconciliationScope.MAIN_EQ)
        self.assertIs(
            infy.disposition,
            ReconciliationDisposition.UNVERIFIED_MAIN_SCOPE,
        )
        self.assertEqual(infy.effective_reg1.effective_session, SESSION)
        self.assertEqual(infy.effective_complete_band.effective_session, SESSION)
        self.assertIs(
            rsdfin.disposition,
            ReconciliationDisposition.UNVERIFIED_MAIN_SCOPE,
        )
        self.assertIsNone(rsdfin.udiff_trade_row)
        self.assertIs(
            sme.disposition,
            ReconciliationDisposition.UNVERIFIED_SME_WATCH_SCOPE,
        )
        self.assertEqual(sme.effective_reg1.esm_code, "1")
        self.assertIn("ESM", dict(sme.effective_reg1.indicator_codes))
        self.assertIs(
            old.disposition,
            ReconciliationDisposition.EXCLUDED_UNSUPPORTED_SERIES,
        )
        self.assertEqual(len(infy.relevant_series_changes), 1)
        self.assertNotIn(
            "PUBLICATION_EFFECTIVE_STATE_UNRESOLVED",
            snapshot.global_reason_codes,
        )
        self.assertFalse(snapshot.actionable)

    def test_cutoff_conflicts_and_cross_report_contradictions_fail_closed(self) -> None:
        with self.assertRaisesRegex(ReconciliationIntegrityError, "cutoff"):
            reconcile_collection_only(
                security_master=self.master,
                daily_bundles=(self.bundle,),
                market_session=SESSION,
                cutoff=BUNDLE_VALIDATED - timedelta(microseconds=1),
            )

        conflicting = self._import_bundle(
            _bundle_bytes(infy_band="10"),
            "daily-conflict",
            first_seen=BUNDLE_FIRST_SEEN + timedelta(seconds=10),
            validated=BUNDLE_VALIDATED + timedelta(seconds=10),
        )
        with self.assertRaisesRegex(ReconciliationIntegrityError, "conflicting"):
            reconcile_collection_only(
                security_master=self.master,
                daily_bundles=(self.bundle, conflicting),
                market_session=SESSION,
                cutoff=CUTOFF,
            )

        contradictory = self._import_bundle(
            _bundle_bytes(sme_band="10"),
            "daily-contradiction",
        )
        with self.assertRaisesRegex(ReconciliationIntegrityError, "contradict"):
            reconcile_collection_only(
                security_master=self.master,
                daily_bundles=(contradictory,),
                market_session=SESSION,
                cutoff=CUTOFF,
                calendar=_calendar(),
            )

        identity_conflict = self._import_bundle(
            _bundle_bytes(infy_financial_instrument_id="1595"),
            "daily-identity-conflict",
        )
        with self.assertRaisesRegex(ReconciliationIntegrityError, "identity or board-lot"):
            reconcile_collection_only(
                security_master=self.master,
                daily_bundles=(identity_conflict,),
                market_session=SESSION,
                cutoff=CUTOFF,
            )

        descriptive_name_difference = self._import_bundle(
            _bundle_bytes(infy_udiff_name="INFOSYS TECHNOLOGIES LIMITED"),
            "daily-descriptive-name-difference",
        )
        snapshot = reconcile_collection_only(
            security_master=self.master,
            daily_bundles=(descriptive_name_difference,),
            market_session=SESSION,
            cutoff=CUTOFF,
        )
        infy = next(entry for entry in snapshot.entries if entry.symbol == "INFY")
        self.assertIn(
            "UDIFF_MASTER_INSTRUMENT_NAME_MISMATCH",
            infy.reason_codes,
        )

    def test_reverse_identity_board_lot_and_band_change_conflicts_fail_closed(self) -> None:
        renamed_identity = self._import_bundle(
            _bundle_bytes(infy_udiff_symbol="INFYNEW"),
            "daily-renamed-identity",
        )
        with self.assertRaisesRegex(
            ReconciliationIntegrityError,
            "instrument ID maps to a different",
        ):
            reconcile_collection_only(
                security_master=self.master,
                daily_bundles=(renamed_identity,),
                market_session=SESSION,
                cutoff=CUTOFF,
            )

        board_lot_conflict = self._import_bundle(
            _bundle_bytes(infy_udiff_board_lot="10"),
            "daily-board-lot-conflict",
        )
        with self.assertRaisesRegex(ReconciliationIntegrityError, "board-lot"):
            reconcile_collection_only(
                security_master=self.master,
                daily_bundles=(board_lot_conflict,),
                market_session=SESSION,
                cutoff=CUTOFF,
            )

        band_change_conflict = self._import_bundle(
            _bundle_bytes(band_change_from="20", band_change_to="10"),
            "daily-band-change-conflict",
        )
        with self.assertRaisesRegex(
            ReconciliationIntegrityError,
            "band change contradicts",
        ):
            reconcile_collection_only(
                security_master=self.master,
                daily_bundles=(band_change_conflict,),
                market_session=SESSION,
                cutoff=CUTOFF,
                calendar=_calendar(),
            )

    def test_same_vintage_and_complete_manifest_lineage_are_required(self) -> None:
        old_source = self.root / "old" / "NSE_CM_security_14072026.csv.gz"
        old_source.parent.mkdir()
        old_source.write_bytes(_master_bytes())
        old_master = LocalReferenceArtifactStore(
            self.root / "old-reference",
            clock=_clock(MASTER_FIRST_SEEN, MASTER_VALIDATED),
        ).import_security_master(old_source)
        with self.assertRaisesRegex(ReconciliationIntegrityError, "same-vintage"):
            reconcile_collection_only(
                security_master=old_master,
                daily_bundles=(self.bundle,),
                market_session=SESSION,
                cutoff=CUTOFF,
            )

        tampered_master = replace(
            self.master,
            manifest=replace(self.master.manifest, manifest_id="f" * 64),
        )
        with self.assertRaisesRegex(ReconciliationIntegrityError, "manifest ID"):
            reconcile_collection_only(
                security_master=tampered_master,
                daily_bundles=(self.bundle,),
                market_session=SESSION,
                cutoff=CUTOFF,
            )

        changed_master_time = replace(
            self.master.manifest,
            validated_at=self.master.manifest.validated_at + timedelta(microseconds=1),
        )
        forged_master = replace(
            self.master,
            manifest=replace(
                changed_master_time,
                manifest_id=content_id(
                    reference_manifest_identity(changed_master_time),
                    length=64,
                ),
            ),
        )
        changed_bundle_time = replace(
            self.bundle.manifest,
            validated_at=self.bundle.manifest.validated_at + timedelta(microseconds=1),
        )
        forged_bundle = replace(
            self.bundle,
            manifest=replace(
                changed_bundle_time,
                manifest_id=content_id(
                    daily_manifest_identity(changed_bundle_time),
                    length=64,
                ),
            ),
        )
        for master, bundles in (
            (forged_master, (self.bundle,)),
            (self.master, (forged_bundle,)),
        ):
            with self.subTest(), self.assertRaisesRegex(
                ReconciliationIntegrityError,
                "sealed provenance",
            ):
                reconcile_collection_only(
                    security_master=master,
                    daily_bundles=bundles,
                    market_session=SESSION,
                    cutoff=CUTOFF,
                )

    def test_sealed_store_is_fully_reparsed_and_manifest_counters_are_bound(self) -> None:
        changed_bundle_counts = replace(
            self.bundle.manifest,
            selected_row_count=self.bundle.manifest.selected_row_count + 999,
        )
        changed_bundle_counts = replace(
            changed_bundle_counts,
            artifact_id=content_id(
                daily_artifact_identity(changed_bundle_counts),
                length=64,
            ),
        )
        changed_bundle_counts = replace(
            changed_bundle_counts,
            manifest_id=content_id(
                daily_manifest_identity(changed_bundle_counts),
                length=64,
            ),
        )
        forged_bundle_path = (
            self.bundle.path.parent / changed_bundle_counts.artifact_id
        )
        forged_bundle_path.mkdir()
        (forged_bundle_path / DAILY_MANIFEST_FILENAME).write_bytes(
            daily_manifest_json(changed_bundle_counts)
        )
        (forged_bundle_path / DAILY_RAW_FILENAME).write_bytes(self.bundle.raw_bytes)
        (forged_bundle_path / DAILY_NORMALIZED_FILENAME).write_bytes(
            self.bundle.normalized_bytes
        )
        counter_forged_bundle = replace(
            self.bundle,
            path=forged_bundle_path,
            manifest=changed_bundle_counts,
        )

        changed_master_counts = replace(
            self.master.manifest,
            raw_row_count=self.master.manifest.raw_row_count + 1,
            parsed_row_count=self.master.manifest.parsed_row_count + 1,
            retained_unverified_equity_count=(
                self.master.manifest.retained_unverified_equity_count + 1
            ),
        )
        changed_master_counts = replace(
            changed_master_counts,
            artifact_id=content_id(
                reference_artifact_identity(changed_master_counts),
                length=64,
            ),
        )
        changed_master_counts = replace(
            changed_master_counts,
            manifest_id=content_id(
                reference_manifest_identity(changed_master_counts),
                length=64,
            ),
        )
        forged_master_path = (
            self.master.path.parent / changed_master_counts.artifact_id
        )
        forged_master_path.mkdir()
        (forged_master_path / REFERENCE_MANIFEST_FILENAME).write_bytes(
            reference_manifest_json(changed_master_counts)
        )
        (forged_master_path / REFERENCE_RAW_FILENAME).write_bytes(self.master.raw_bytes)
        (forged_master_path / REFERENCE_NORMALIZED_FILENAME).write_bytes(
            self.master.normalized_bytes
        )
        counter_forged_master = replace(
            self.master,
            path=forged_master_path,
            manifest=changed_master_counts,
        )

        for master, bundles in (
            (self.master, (counter_forged_bundle,)),
            (counter_forged_master, (self.bundle,)),
        ):
            with self.subTest(), self.assertRaisesRegex(
                ReconciliationIntegrityError,
                "sealed provenance",
            ):
                reconcile_collection_only(
                    security_master=master,
                    daily_bundles=bundles,
                    market_session=SESSION,
                    cutoff=CUTOFF,
                )

        with self.assertRaisesRegex(ValueError, "must use UTC"):
            replace(
                self.bundle.manifest,
                validated_at=self.bundle.manifest.validated_at.astimezone(IST),
            )
        with self.assertRaisesRegex(ValueError, "must use UTC"):
            replace(
                self.master.manifest,
                validated_at=self.master.manifest.validated_at.astimezone(IST),
            )

    def test_daily_bundle_lineage_preserves_exact_artifact_manifest_pairs(self) -> None:
        same_reports_different_archive = self._import_bundle(
            _bundle_bytes(ignored_entry=("collection-note.txt", b"second archive")),
            "daily-same-reports",
            first_seen=BUNDLE_FIRST_SEEN + timedelta(minutes=1),
            validated=BUNDLE_VALIDATED + timedelta(minutes=1),
        )
        snapshot = reconcile_collection_only(
            security_master=self.master,
            daily_bundles=(self.bundle, same_reports_different_archive),
            market_session=SESSION,
            cutoff=CUTOFF,
        )

        self.assertEqual(len(snapshot.daily_bundle_manifests), 2)
        real_pairs = {
            (value.artifact_id, value.manifest_id)
            for value in snapshot.daily_bundle_manifests
        }
        self.assertTrue(
            all(
                (binding.artifact_id, binding.manifest_id) in real_pairs
                for binding in snapshot.report_bindings
            )
        )
        forged = replace(
            snapshot.daily_bundle_manifests[1],
            manifest_id="f" * 64,
        )
        forged_lineage = tuple(
            sorted(
                (snapshot.daily_bundle_manifests[0], forged),
                key=lambda value: (value.artifact_id, value.manifest_id),
            )
        )
        with self.assertRaisesRegex(ValueError, "lineage manifest ID is invalid"):
            replace(snapshot, daily_bundle_manifests=forged_lineage)

    def test_future_target_and_mutable_series_change_vintages_fail_closed(self) -> None:
        early_master_source = (
            self.root / "early" / "NSE_CM_security_15072026.csv.gz"
        )
        early_master_source.parent.mkdir()
        early_master_source.write_bytes(_master_bytes())
        early_first_seen = datetime(2026, 7, 14, 14, 0, tzinfo=UTC)
        early_master = LocalReferenceArtifactStore(
            self.root / "early-reference",
            clock=_clock(
                early_first_seen,
                early_first_seen + timedelta(seconds=1),
            ),
        ).import_security_master(early_master_source)
        early_bundle = self._import_bundle(
            _bundle_bytes(),
            "daily-early",
            first_seen=early_first_seen,
            validated=early_first_seen + timedelta(seconds=1),
        )
        with self.assertRaisesRegex(ReconciliationIntegrityError, "boundary"):
            reconcile_collection_only(
                security_master=early_master,
                daily_bundles=(early_bundle,),
                market_session=SESSION,
                cutoff=datetime(2026, 7, 14, 15, 0, tzinfo=UTC),
            )

        updated_series = self._import_bundle(
            _bundle_bytes(series_change_symbol="RSDFIN"),
            "daily-updated-series",
            first_seen=BUNDLE_FIRST_SEEN + timedelta(minutes=10),
            validated=BUNDLE_VALIDATED + timedelta(minutes=10),
        )
        snapshot = reconcile_collection_only(
            security_master=self.master,
            daily_bundles=(self.bundle, updated_series),
            market_session=SESSION,
            cutoff=CUTOFF,
        )
        rsdfin = next(entry for entry in snapshot.entries if entry.symbol == "RSDFIN")
        infy = next(entry for entry in snapshot.entries if entry.symbol == "INFY")
        self.assertEqual(len(rsdfin.relevant_series_changes), 1)
        self.assertEqual(len(infy.relevant_series_changes), 1)
        mutable_bindings = [
            binding
            for binding in snapshot.report_bindings
            if binding.family.value == "SERIES_CHANGES"
        ]
        self.assertEqual(
            {binding.manifest_id for binding in mutable_bindings},
            {
                self.bundle.manifest.manifest_id,
                updated_series.manifest.manifest_id,
            },
        )

        contradictory_series = self._import_bundle(
            _bundle_bytes(series_change_from="SM", series_change_to="EQ"),
            "daily-contradictory-series",
            first_seen=BUNDLE_FIRST_SEEN + timedelta(minutes=15),
            validated=BUNDLE_VALIDATED + timedelta(minutes=15),
        )
        with self.assertRaisesRegex(ReconciliationIntegrityError, "contradictory transitions"):
            reconcile_collection_only(
                security_master=self.master,
                daily_bundles=(self.bundle, contradictory_series),
                market_session=SESSION,
                cutoff=CUTOFF,
            )
        tampered_bundle = replace(
            self.bundle,
            manifest=replace(self.bundle.manifest, manifest_id="e" * 64),
        )
        with self.assertRaisesRegex(ReconciliationIntegrityError, "manifest ID"):
            reconcile_collection_only(
                security_master=self.master,
                daily_bundles=(tampered_bundle,),
                market_session=SESSION,
                cutoff=CUTOFF,
            )

    def test_lineage_type_and_actionability_tampering_are_rejected(self) -> None:
        snapshot = reconcile_collection_only(
            security_master=self.master,
            daily_bundles=(self.bundle,),
            market_session=SESSION,
            cutoff=CUTOFF,
        )
        infy = next(entry for entry in snapshot.entries if entry.symbol == "INFY")
        self.assertIsNotNone(infy.udiff_trade_row)
        wrong_family = replace(
            infy.udiff_trade_row,
            family=infy.full_delivery_row.family,
        )
        with self.assertRaisesRegex(ValueError, "UDiFF entry field"):
            replace(infy, udiff_trade_row=wrong_family)
        with self.assertRaisesRegex(ValueError, "collection-only"):
            replace(snapshot, actionable=True)
        unknown_binding = replace(infy.udiff_trade_row, binding_id="f" * 64)
        unbound_entry = replace(infy, udiff_trade_row=unknown_binding)
        with self.assertRaisesRegex(ValueError, "report-binding lineage"):
            replace(
                snapshot,
                entries=tuple(
                    unbound_entry if entry is infy else entry
                    for entry in snapshot.entries
                ),
            )
        future_master_manifest = replace(
            snapshot.security_master_manifest,
            validated_at=CUTOFF + timedelta(seconds=1),
        )
        future_master_manifest = replace(
            future_master_manifest,
            manifest_id=content_id(
                reference_manifest_identity(future_master_manifest),
                length=64,
            ),
        )
        with self.assertRaisesRegex(ValueError, "follows.*cutoff"):
            replace(
                snapshot,
                security_master_manifest=future_master_manifest,
            )
        with self.assertRaisesRegex(ValueError, "lineage manifest ID is invalid"):
            replace(
                snapshot,
                security_master_manifest=replace(
                    snapshot.security_master_manifest,
                    manifest_id="f" * 64,
                ),
            )

        with self.assertRaisesRegex(ValueError, "target final-report boundary"):
            replace(snapshot, market_session=date(2030, 1, 2))
        with self.assertRaisesRegex(ValueError, "every retained master row"):
            replace(
                snapshot,
                entries=snapshot.entries[:-1],
                retained_source_row_ids=snapshot.retained_source_row_ids[:-1],
            )

        rsdfin = next(entry for entry in snapshot.entries if entry.symbol == "RSDFIN")
        old = next(entry for entry in snapshot.entries if entry.symbol == "OLD")
        with self.assertRaisesRegex(ValueError, "listing key does not belong"):
            replace(rsdfin, udiff_trade_row=infy.udiff_trade_row)
        with self.assertRaisesRegex(ValueError, "scope disagrees"):
            replace(
                old,
                scope=ReconciliationScope.MAIN_EQ,
                disposition=ReconciliationDisposition.UNRESOLVED,
            )
        duplicate_old = replace(
            old,
            symbol=infy.symbol,
            series=infy.series,
            financial_instrument_id=infy.financial_instrument_id,
            scope=ReconciliationScope.MAIN_EQ,
            disposition=ReconciliationDisposition.UNRESOLVED,
        )
        with self.assertRaisesRegex(ValueError, "listing keys must be unique"):
            replace(
                snapshot,
                entries=tuple(
                    duplicate_old if entry is old else entry
                    for entry in snapshot.entries
                ),
            )

        series_orphan = next(
            value
            for value in snapshot.orphan_report_keys
            if value.family.value == "SERIES_CHANGES"
            and ("INFY", "EQ") in value.row_ref.listing_keys
        )
        retained_orphan = replace(series_orphan, symbol="INFY", series="EQ")
        forged_orphans = tuple(
            sorted(
                (
                    retained_orphan if value is series_orphan else value
                    for value in snapshot.orphan_report_keys
                ),
                key=lambda value: (
                    value.family.value,
                    value.claimed_date.isoformat() if value.claimed_date else "",
                    value.symbol,
                    value.series,
                    value.row_ref.row_sha256,
                ),
            )
        )
        with self.assertRaisesRegex(ValueError, "overlaps retained membership"):
            replace(snapshot, orphan_report_keys=forged_orphans)

        duplicate_binding = replace(
            snapshot.report_bindings[0],
            content_sha256="f" * 64,
        )
        duplicate_bindings = tuple(
            sorted(
                (*snapshot.report_bindings, duplicate_binding),
                key=lambda value: (
                    value.family.value,
                    (
                        value.claimed_report_date.isoformat()
                        if value.claimed_report_date
                        else ""
                    ),
                    value.source_entry_name,
                    value.binding_id,
                ),
            )
        )
        with self.assertRaisesRegex(ValueError, "source entries must be unique"):
            replace(snapshot, report_bindings=duplicate_bindings)

        forged_reg1 = replace(
            infy.reg1_observations[0],
            publication_date_claim=date(2030, 1, 1),
            effective_session=date(2030, 1, 2),
        )
        forged_reg1_entry = replace(
            infy,
            reg1_observations=(forged_reg1,),
        )
        with self.assertRaisesRegex(ValueError, "REG1 observation disagrees"):
            replace(
                snapshot,
                entries=tuple(
                    forged_reg1_entry if entry is infy else entry
                    for entry in snapshot.entries
                ),
            )

        with self.assertRaisesRegex(ValueError, "outside the pinned domain"):
            replace(infy.complete_band_observations[0], band="NOT_A_BAND")

        out_of_range_row = replace(
            infy.udiff_trade_row,
            source_row_number=10_000,
        )
        out_of_range_entry = replace(infy, udiff_trade_row=out_of_range_row)
        with self.assertRaisesRegex(ValueError, "row number exceeds"):
            replace(
                snapshot,
                entries=tuple(
                    out_of_range_entry if entry is infy else entry
                    for entry in snapshot.entries
                ),
            )

        payload = json.loads(encode_reconciliation(snapshot))
        self.assertEqual(payload["snapshot_id"], snapshot.snapshot_id)
        self.assertEqual(
            payload["security_master_manifest_id"],
            self.master.manifest.manifest_id,
        )
        self.assertEqual(
            payload["security_master_claimed_report_date"],
            SESSION.isoformat(),
        )

    def test_equivalent_cutoff_offsets_have_one_identity_and_encoding(self) -> None:
        utc_snapshot = reconcile_collection_only(
            security_master=self.master,
            daily_bundles=(self.bundle,),
            market_session=SESSION,
            cutoff=CUTOFF,
        )
        ist_snapshot = reconcile_collection_only(
            security_master=self.master,
            daily_bundles=(self.bundle,),
            market_session=SESSION,
            cutoff=CUTOFF.astimezone(IST),
        )

        self.assertEqual(utc_snapshot.snapshot_id, ist_snapshot.snapshot_id)
        self.assertEqual(
            encode_reconciliation(utc_snapshot),
            encode_reconciliation(ist_snapshot),
        )

    def test_cli_emits_only_collection_only_summaries(self) -> None:
        environment = {
            "INDIA_SWING_REFERENCE_DATA_ROOT": str(self.root / "reference"),
            "INDIA_SWING_DAILY_REPORTS_ROOT": str(self.root / "store-daily-one"),
        }
        observed_stdout = io.StringIO()
        with patch.dict(os.environ, environment, clear=True), patch(
            "sys.stdout", observed_stdout
        ):
            exit_code = evidence_main(
                [
                    "observed-dates",
                    "--daily-bundle-id",
                    self.bundle.manifest.artifact_id,
                    "--cutoff",
                    BUNDLE_VALIDATED.isoformat(),
                ]
            )
        observed = json.loads(observed_stdout.getvalue())
        self.assertEqual(exit_code, 0)
        self.assertEqual(observed["observed_dates"], [SESSION.isoformat()])
        self.assertEqual(observed["readiness"], "COLLECTION_ONLY")
        self.assertFalse(observed["actionable"])

        reconcile_stdout = io.StringIO()
        with patch.dict(os.environ, environment, clear=True), patch(
            "sys.stdout", reconcile_stdout
        ):
            exit_code = evidence_main(
                [
                    "reconcile",
                    "--security-master-id",
                    self.master.manifest.artifact_id,
                    "--daily-bundle-id",
                    self.bundle.manifest.artifact_id,
                    "--market-session",
                    SESSION.isoformat(),
                    "--cutoff",
                    CUTOFF.isoformat(),
                ]
            )
        diagnostic = json.loads(reconcile_stdout.getvalue())
        self.assertEqual(exit_code, 0)
        self.assertEqual(diagnostic["retained_row_count"], 4)
        self.assertEqual(diagnostic["unresolved_count"], 3)
        self.assertEqual(diagnostic["readiness"], "COLLECTION_ONLY")
        self.assertFalse(diagnostic["actionable"])

        stderr = io.StringIO()
        with patch.dict(os.environ, environment, clear=True), patch(
            "sys.stderr", stderr
        ):
            exit_code = evidence_main(
                [
                    "observed-dates",
                    "--daily-bundle-id",
                    "access_token=distinct-secret",
                    "--cutoff",
                    CUTOFF.isoformat(),
                ]
            )
        self.assertEqual(exit_code, 2)
        self.assertNotIn("distinct-secret", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
