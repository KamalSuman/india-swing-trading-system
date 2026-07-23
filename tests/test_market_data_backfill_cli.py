from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from india_swing.historical_prices import LocalHistoricalPriceArtifactStore
from india_swing.market_data.backfill_cli import main, parser
from india_swing.market_data.backfill_pilot import MAXIMUM_PILOT_TOTAL_REQUESTS
from india_swing.market_data.collection import historical_dataset_name
from india_swing.market_data.snapshot_store import LocalMarketSnapshotStore
from tests.test_historical_backfill import (
    DAY_ONE,
    DAY_TWO,
    REQUESTED_AT,
    plan,
    two_session_body,
)
from tests.test_historical_backfill_pilot import (
    RECONCILED_AT as PILOT_RECONCILED_AT,
    FakePilotConnector,
    nse_artifact as pilot_nse_artifact,
    pilot_plan,
)
from tests.test_historical_reconciliation import (
    RECONCILED_AT,
    nse_artifact,
    provider_batch,
)
from tests.test_upstox_market_data import FakeTransport, adapter, response
from tests.test_upstox_instruments import (
    OBSERVED_AT as CATALOG_OBSERVED_AT,
    equity_row,
    raw_catalog,
)


def plan_arguments(command: str) -> list[str]:
    return [
        command,
        "--identity-registry-id",
        "a" * 64,
        "--calendar-materialization-id",
        "b" * 64,
        "--upstox-catalog-id",
        "c" * 64,
        "--coverage-start",
        DAY_ONE.isoformat(),
        "--coverage-end",
        DAY_TWO.isoformat(),
        "--requested-at",
        REQUESTED_AT.isoformat(),
    ]


class HistoricalBackfillCliTests(unittest.TestCase):
    def test_catalog_import_is_credential_free_and_persists_raw_lineage(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / "NSE.json.gz"
            source.write_bytes(raw_catalog(equity_row()))
            output = io.StringIO()
            with (
                patch.dict(
                    "os.environ",
                    {"INDIA_SWING_MARKET_DATA_ROOT": str(root / "market")},
                    clear=False,
                ),
                patch(
                    "india_swing.market_data.backfill_cli.UpstoxCredentials.from_env",
                    side_effect=AssertionError("credentials must not be read"),
                ),
                redirect_stdout(output),
            ):
                exit_code = main(
                    [
                        "catalog-import",
                        "--source-file",
                        str(source),
                        "--observed-at",
                        CATALOG_OBSERVED_AT.isoformat(),
                    ]
                )

        payload = json.loads(output.getvalue())
        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["status"], "UPSTOX_CATALOG_READY")
        self.assertEqual(payload["nse_equity_instrument_count"], 1)
        self.assertFalse(payload["actionable"])

    def test_plan_is_credential_free_and_reports_exact_counts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            value = plan(Path(temp_dir))
            output = io.StringIO()
            with (
                patch(
                    "india_swing.market_data.backfill_cli._configured_plan",
                    return_value=value,
                ),
                patch(
                    "india_swing.market_data.backfill_cli.UpstoxCredentials.from_env",
                    side_effect=AssertionError("credentials must not be read"),
                ),
                redirect_stdout(output),
            ):
                exit_code = main(plan_arguments("plan"))

        payload = json.loads(output.getvalue())
        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["status"], "PLAN_READY")
        self.assertEqual(payload["safe_request_count"], 2)
        self.assertEqual(payload["safe_session_count"], 4)
        self.assertTrue(payload["coverage_complete"])

    def test_run_blocks_coverage_issues_before_reading_credentials(self) -> None:
        from tests.test_historical_backfill import DAY_ZERO, calendar

        with tempfile.TemporaryDirectory() as temp_dir:
            value = plan(
                Path(temp_dir),
                selected_calendar=calendar(DAY_ZERO, DAY_TWO),
                coverage_start=DAY_ZERO,
            )
            args = plan_arguments("run")
            args[args.index("--coverage-start") + 1] = DAY_ZERO.isoformat()
            output = io.StringIO()
            with (
                patch(
                    "india_swing.market_data.backfill_cli._configured_plan",
                    return_value=value,
                ),
                patch(
                    "india_swing.market_data.backfill_cli.UpstoxCredentials.from_env",
                    side_effect=AssertionError("credentials must not be read"),
                ),
                redirect_stdout(output),
            ):
                exit_code = main(args)

        payload = json.loads(output.getvalue())
        self.assertEqual(exit_code, 3)
        self.assertEqual(payload["status"], "BLOCKED_COVERAGE")
        self.assertFalse(payload["coverage_complete"])

    def test_run_command_is_bounded_and_resumes_from_durable_progress(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value = plan(root / "inputs")
            transport = FakeTransport(
                response(two_session_body()),
                response(two_session_body()),
            )
            connector = adapter(transport)
            environment = {
                "INDIA_SWING_MARKET_DATA_ROOT": str(root / "market"),
                "INDIA_SWING_UPSTOX_ACCESS_TOKEN": "runtime-only-token",
            }
            first_output = io.StringIO()
            second_output = io.StringIO()
            with (
                patch.dict("os.environ", environment, clear=False),
                patch(
                    "india_swing.market_data.backfill_cli._configured_plan",
                    return_value=value,
                ),
                patch(
                    "india_swing.market_data.backfill_cli.UpstoxHistoricalDataAdapter",
                    return_value=connector,
                ),
            ):
                with redirect_stdout(first_output):
                    first_exit = main(
                        plan_arguments("run")
                        + ["--maximum-requests", "1"]
                    )
                with redirect_stdout(second_output):
                    second_exit = main(plan_arguments("run"))

        first = json.loads(first_output.getvalue())
        second = json.loads(second_output.getvalue())
        self.assertEqual((first_exit, second_exit), (0, 0))
        self.assertEqual(first["status"], "SAFE_REQUESTS_PARTIAL")
        self.assertEqual(first["completed_request_count"], 1)
        self.assertEqual(second["status"], "SAFE_REQUESTS_COMPLETE")
        self.assertEqual(second["completed_request_count"], 2)
        self.assertEqual(len(transport.calls), 2)

    def test_reconcile_command_persists_a_passing_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            market_root = root / "market"
            historical_root = root / "historical"
            artifact = nse_artifact(root)
            stored_artifact = LocalHistoricalPriceArtifactStore(
                historical_root,
                root / "daily",
            ).put(artifact)
            batch = provider_batch()
            market_store = LocalMarketSnapshotStore(market_root)
            stored_batch = market_store.put(
                dataset=historical_dataset_name(batch.provider),
                selection_key=batch.request.request_id,
                provider=batch.provider,
                provider_version=batch.provider_version,
                observed_at=batch.observed_at,
                normalized_payload=batch,
            )
            environment = {
                "INDIA_SWING_MARKET_DATA_ROOT": str(market_root),
                "INDIA_SWING_HISTORICAL_PRICES_ROOT": str(historical_root),
                "INDIA_SWING_DAILY_REPORTS_ROOT": str(root / "daily"),
            }
            output = io.StringIO()
            stderr = io.StringIO()
            with (
                patch.dict("os.environ", environment, clear=False),
                redirect_stdout(output),
                redirect_stderr(stderr),
            ):
                exit_code = main(
                    [
                        "reconcile",
                        "--provider",
                        "UPSTOX",
                        "--provider-snapshot-id",
                        stored_batch.manifest.snapshot_id,
                        "--nse-artifact-id",
                        stored_artifact.manifest.artifact_id,
                        "--reconciled-at",
                        RECONCILED_AT.isoformat(),
                    ]
                )

        payload = json.loads(output.getvalue())
        self.assertEqual(stderr.getvalue(), "")
        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["status"], "RECONCILIATION_PASSED")
        self.assertTrue(payload["passed"])
        self.assertFalse(payload["actionable"])


def pilot_arguments() -> list[str]:
    return plan_arguments("pilot") + [
        "--maximum-total-requests",
        "2",
        "--nse-artifact-id",
        "a" * 64,
        "--reconciled-at",
        PILOT_RECONCILED_AT.isoformat(),
    ]


class HistoricalBackfillPilotCliTests(unittest.TestCase):
    def test_pilot_parser_requires_cap_evidence_and_reconciled_at(self) -> None:
        with self.assertRaises(SystemExit):
            parser().parse_args(plan_arguments("pilot"))

        args = parser().parse_args(pilot_arguments())

        self.assertEqual(args.command, "pilot")
        self.assertEqual(args.maximum_total_requests, 2)
        self.assertEqual(args.nse_artifact_ids, ["a" * 64])
        self.assertEqual(args.reconciled_at, PILOT_RECONCILED_AT)

    def test_pilot_passes_and_persists_reconciliation_reports(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value = pilot_plan(root / "inputs")
            artifact = pilot_nse_artifact(root)
            historical_root = root / "historical"
            stored_artifact = LocalHistoricalPriceArtifactStore(
                historical_root,
                root / "daily",
            ).put(artifact)
            connector = FakePilotConnector()
            environment = {
                "INDIA_SWING_MARKET_DATA_ROOT": str(root / "market"),
                "INDIA_SWING_HISTORICAL_PRICES_ROOT": str(historical_root),
                "INDIA_SWING_DAILY_REPORTS_ROOT": str(root / "daily"),
                "INDIA_SWING_UPSTOX_ACCESS_TOKEN": "runtime-only-token",
            }
            args = plan_arguments("pilot") + [
                "--maximum-total-requests",
                "2",
                "--nse-artifact-id",
                stored_artifact.manifest.artifact_id,
                "--reconciled-at",
                PILOT_RECONCILED_AT.isoformat(),
            ]
            output = io.StringIO()
            with (
                patch.dict("os.environ", environment, clear=False),
                patch(
                    "india_swing.market_data.backfill_cli._configured_plan",
                    return_value=value,
                ),
                patch(
                    "india_swing.market_data.backfill_cli.UpstoxHistoricalDataAdapter",
                    return_value=connector,
                ),
                redirect_stdout(output),
            ):
                exit_code = main(args)

        payload = json.loads(output.getvalue())
        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["status"], "PILOT_PASSED")
        self.assertTrue(payload["passed"])
        self.assertFalse(payload["actionable"])
        self.assertTrue(payload["collection_only"])
        self.assertEqual(payload["maximum_total_requests"], 2)
        self.assertEqual(payload["selected_request_count"], 2)
        self.assertEqual(payload["completed_request_count"], 2)
        self.assertEqual(payload["reconciliation_report_count"], 2)
        self.assertEqual(payload["passed_reconciliation_count"], 2)

    def test_pilot_reconciliation_failure_returns_exit_four(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value = pilot_plan(root / "inputs")
            artifact = pilot_nse_artifact(root)
            historical_root = root / "historical"
            stored_artifact = LocalHistoricalPriceArtifactStore(
                historical_root,
                root / "daily",
            ).put(artifact)
            connector = FakePilotConnector(
                close_by_listing_key={"NSE:INFY": "1608.00"}
            )
            environment = {
                "INDIA_SWING_MARKET_DATA_ROOT": str(root / "market"),
                "INDIA_SWING_HISTORICAL_PRICES_ROOT": str(historical_root),
                "INDIA_SWING_DAILY_REPORTS_ROOT": str(root / "daily"),
                "INDIA_SWING_UPSTOX_ACCESS_TOKEN": "runtime-only-token",
            }
            args = plan_arguments("pilot") + [
                "--maximum-total-requests",
                "2",
                "--nse-artifact-id",
                stored_artifact.manifest.artifact_id,
                "--reconciled-at",
                PILOT_RECONCILED_AT.isoformat(),
            ]
            output = io.StringIO()
            with (
                patch.dict("os.environ", environment, clear=False),
                patch(
                    "india_swing.market_data.backfill_cli._configured_plan",
                    return_value=value,
                ),
                patch(
                    "india_swing.market_data.backfill_cli.UpstoxHistoricalDataAdapter",
                    return_value=connector,
                ),
                redirect_stdout(output),
            ):
                exit_code = main(args)

        payload = json.loads(output.getvalue())
        self.assertEqual(exit_code, 4)
        self.assertEqual(payload["status"], "PILOT_RECONCILIATION_FAILED")
        self.assertFalse(payload["passed"])
        self.assertFalse(payload["actionable"])

    def test_pilot_enforces_the_fixed_fifty_request_ceiling(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value = pilot_plan(root / "inputs")
            environment = {
                "INDIA_SWING_MARKET_DATA_ROOT": str(root / "market"),
                "INDIA_SWING_UPSTOX_ACCESS_TOKEN": "runtime-only-token",
            }
            args = plan_arguments("pilot") + [
                "--maximum-total-requests",
                str(MAXIMUM_PILOT_TOTAL_REQUESTS + 1),
                "--nse-artifact-id",
                "a" * 64,
                "--reconciled-at",
                PILOT_RECONCILED_AT.isoformat(),
            ]
            output = io.StringIO()
            stderr = io.StringIO()
            with (
                patch.dict("os.environ", environment, clear=False),
                patch(
                    "india_swing.market_data.backfill_cli._configured_plan",
                    return_value=value,
                ),
                patch(
                    "india_swing.market_data.backfill_cli.UpstoxHistoricalDataAdapter",
                    return_value=FakePilotConnector(),
                ),
                redirect_stdout(output),
                redirect_stderr(stderr),
            ):
                exit_code = main(args)

        self.assertEqual(exit_code, 2)
        payload = json.loads(stderr.getvalue())
        self.assertEqual(payload["status"], "FAILED")

    def test_pilot_sanitized_exception_does_not_leak_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value = pilot_plan(root / "inputs")
            secret_token = "distinct-pilot-secret-token"
            environment = {
                "INDIA_SWING_MARKET_DATA_ROOT": str(root / "market"),
                "INDIA_SWING_UPSTOX_ACCESS_TOKEN": secret_token,
            }
            args = plan_arguments("pilot") + [
                "--maximum-total-requests",
                "2",
                "--nse-artifact-id",
                "a" * 64,
                "--reconciled-at",
                PILOT_RECONCILED_AT.isoformat(),
            ]
            output = io.StringIO()
            stderr = io.StringIO()
            with (
                patch.dict("os.environ", environment, clear=False),
                patch(
                    "india_swing.market_data.backfill_cli._configured_plan",
                    return_value=value,
                ),
                patch(
                    "india_swing.market_data.backfill_cli.UpstoxHistoricalDataAdapter",
                    return_value=FakePilotConnector(),
                ),
                redirect_stdout(output),
                redirect_stderr(stderr),
            ):
                exit_code = main(args)

        self.assertEqual(exit_code, 2)
        self.assertEqual(output.getvalue(), "")
        self.assertNotIn(secret_token, stderr.getvalue())
        payload = json.loads(stderr.getvalue())
        self.assertEqual(payload["status"], "FAILED")
        self.assertNotIn("token", json.dumps(payload).lower())


if __name__ == "__main__":
    unittest.main()
