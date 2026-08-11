from __future__ import annotations

import inspect
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from india_swing.corporate_actions import (
    CorporateActionType,
    NseCorporateActionCsvError,
    NseCorporateActionListingBinding,
    import_nse_corporate_action_csv,
)
from india_swing.corporate_actions import nse_csv as module
from india_swing.corporate_actions.nse_csv_cli import main as cli_main
from india_swing.corporate_actions.snapshot_store import (
    LocalCorporateActionSnapshotStore,
)

from tests.test_nse_archive_research_dataset import _fake_sha256
from tests.test_reference_data_import import security_master_bytes


HEADER = (
    "SYMBOL,COMPANY NAME,SERIES,PURPOSE,FACE VALUE,EX-DATE,RECORD DATE,"
    "BOOK CLOSURE START DATE,BOOK CLOSURE END DATE\n"
)


def _row(symbol: str, purpose: str, ex_date: str = "23-Jul-2026") -> str:
    return f'{symbol},Example Limited,EQ,"{purpose}",10,{ex_date},,,\n'


class NseCorporateActionCsvTests(unittest.TestCase):
    def setUp(self) -> None:
        self.observed = datetime(2026, 7, 20, 12, tzinfo=timezone.utc)
        self.cutoff = self.observed + timedelta(hours=1)
        self.bindings = tuple(
            NseCorporateActionListingBinding(
                symbol=symbol,
                series="EQ",
                stable_instrument_id=_fake_sha256(f"instrument-{symbol}"),
                stable_listing_id=_fake_sha256(f"listing-{symbol}"),
                source_artifact_id=_fake_sha256(f"binding-source-{symbol}"),
                knowledge_time=self.observed - timedelta(days=1),
            )
            for symbol in ("AAA", "BBB", "CCC", "DDD")
        )

    def _import(self, body: str, **changes):
        values = {
            "observed_at": self.observed,
            "cutoff": self.cutoff,
            "coverage_start": date(2026, 7, 1),
            "coverage_end": date(2026, 7, 31),
            "listing_bindings": self.bindings,
        }
        values.update(changes)
        return import_nse_corporate_action_csv(
            (HEADER + body).encode("utf-8"), **values
        )

    def test_imports_split_bonus_and_summed_dividend_without_lookahead(self) -> None:
        result = self._import(
            _row("AAA", "Face Value Split (Sub-Division) From Rs 10 To Rs 2")
            + _row("BBB", "Bonus Issue of 1:1")
            + _row(
                "CCC",
                "Dividend - Rs 70 Per Share/Special Dividend - Rs 35 Per Share",
            )
        )
        self.assertEqual(result.imported_row_count, 3)
        by_type = {value.action_type: value for value in result.snapshot.events}
        self.assertEqual(
            set(by_type),
            {
                CorporateActionType.SPLIT,
                CorporateActionType.BONUS,
                CorporateActionType.CASH_DIVIDEND,
            },
        )
        split = by_type[CorporateActionType.SPLIT]
        bonus = by_type[CorporateActionType.BONUS]
        dividend = by_type[CorporateActionType.CASH_DIVIDEND]
        self.assertEqual((split.pre_action_shares, split.post_action_shares), (2, 10))
        self.assertEqual((bonus.pre_action_shares, bonus.post_action_shares), (1, 2))
        self.assertEqual(dividend.cash_amount_per_share, 105)
        self.assertTrue(
            all(value.knowledge_time == self.observed for value in result.snapshot.events)
        )
        result.verify_content_identity()

    def test_non_price_meeting_is_accounted_but_not_made_an_event(self) -> None:
        result = self._import(_row("AAA", "Annual General Meeting"))
        self.assertEqual(result.imported_row_count, 0)
        self.assertEqual(len(result.ignored_non_price_row_ids), 1)
        self.assertEqual(result.ignored_out_of_scope_row_ids, ())
        self.assertEqual(result.snapshot.events, ())

    def test_out_of_scope_reit_distribution_is_accounted_without_parsing(self) -> None:
        result = self._import(
            "PGINVIT,Example Trust,IV,Distribution - Re 0.43 Per Unit,10,"
            "23-Jul-2026,,,\n"
        )
        self.assertEqual(result.imported_row_count, 0)
        self.assertEqual(len(result.ignored_out_of_scope_row_ids), 1)

    def test_abbreviated_dividend_split_and_buyback_are_retained(self) -> None:
        result = self._import(
            _row("AAA", "Dividend - Re 0.21 Per Sh")
            + _row(
                "BBB",
                "Face Value Split (Sub-Division) - From Rs 10/- Per Share To Re 1/- Per Share",
            )
            + _row("CCC", "Buy Back")
        )
        self.assertEqual(
            {value.action_type for value in result.snapshot.events},
            {
                CorporateActionType.CASH_DIVIDEND,
                CorporateActionType.SPLIT,
                CorporateActionType.BUYBACK,
            },
        )

    def test_unknown_purpose_missing_binding_and_future_source_fail_closed(self) -> None:
        with self.assertRaises(NseCorporateActionCsvError):
            self._import(_row("AAA", "Mystery restructuring"))
        with self.assertRaises(NseCorporateActionCsvError):
            self._import(_row("ZZZ", "Dividend - Re 1 Per Share"))
        with self.assertRaises(NseCorporateActionCsvError):
            self._import(
                _row("AAA", "Dividend - Re 1 Per Share"),
                observed_at=self.cutoff + timedelta(seconds=1),
            )

    def test_duplicate_binding_and_schema_drift_fail_closed(self) -> None:
        with self.assertRaises(NseCorporateActionCsvError):
            self._import(
                _row("AAA", "Dividend - Re 1 Per Share"),
                listing_bindings=self.bindings + (self.bindings[0],),
            )
        bad = (HEADER.replace("PURPOSE", "PURPOSE2") + _row("AAA", "AGM")).encode()
        with self.assertRaises(NseCorporateActionCsvError):
            import_nse_corporate_action_csv(
                bad,
                observed_at=self.observed,
                cutoff=self.cutoff,
                coverage_start=date(2026, 7, 1),
                coverage_end=date(2026, 7, 31),
                listing_bindings=self.bindings,
            )

    def test_module_has_no_io_clock_network_store_or_execution_capability(self) -> None:
        source = inspect.getsource(module).lower()
        for token in (
            "builtins.open(",
            "path(",
            "datetime.now(",
            "os.environ",
            "requests.",
            "google.cloud",
            "kite",
            "telegram",
            "place_order",
        ):
            self.assertNotIn(token, source)

    def test_cli_builds_bindings_from_exact_master_and_persists_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            action_path = root / "CF-CA-equities-23-Jul-2026.csv"
            master_path = root / "NSE_CM_security_15072026.csv.gz"
            action_path.write_bytes(
                (HEADER + _row("INFY", "Dividend - Re 1 Per Share")).encode()
            )
            master_path.write_bytes(security_master_bytes())
            output = io.StringIO()
            with redirect_stdout(output):
                code = cli_main(
                    (
                        "--corporate-action-csv",
                        str(action_path),
                        "--security-master",
                        str(master_path),
                        "--observed-at",
                        self.observed.isoformat(),
                        "--security-master-observed-at",
                        (self.observed - timedelta(days=1)).isoformat(),
                        "--cutoff",
                        self.cutoff.isoformat(),
                        "--coverage-start",
                        "2026-07-01",
                        "--coverage-end",
                        "2026-07-31",
                        "--state-root",
                        str(root),
                    )
                )
            self.assertEqual(code, 0)
            result = json.loads(output.getvalue())
            self.assertEqual(result["status"], "NSE_CORPORATE_ACTION_SNAPSHOT_READY")
            stored = LocalCorporateActionSnapshotStore(root).get(
                result["snapshot_id"]
            )
            self.assertEqual(len(stored.events), 1)
            self.assertEqual(stored.events[0].action_type, CorporateActionType.CASH_DIVIDEND)


if __name__ == "__main__":
    unittest.main()
