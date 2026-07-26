from __future__ import annotations

import argparse
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from datetime import date, datetime, timezone
from decimal import Decimal

from india_swing.historical_prices import LocalHistoricalPriceArtifactStore
from india_swing.market_data.backfill import build_historical_backfill_plan
from india_swing.market_data.backfill_cli import (
    main,
    parser,
    _connector_for_plan,
    _kite_credentials,
    _require_provider_evidence,
    _resolver_for_provider,
)
from india_swing.market_data.backfill_gaps import (
    HistoricalBackfillGapClassification,
    LocalHistoricalBackfillSessionGapStore,
)
from india_swing.market_data.backfill_pilot import MAXIMUM_PILOT_TOTAL_REQUESTS
from india_swing.market_data.collection import historical_dataset_name
from india_swing.market_data.config import KiteCredentials, KiteLoginCredentials
from india_swing.market_data.kite import KiteMarketDataAdapter
from india_swing.market_data.kite_auth import KiteInteractiveAuthenticator
from india_swing.market_data.kite_instruments import (
    KITE_PROVIDER,
    KiteInstrumentSnapshotResolver,
)
from india_swing.market_data.models import (
    HistoricalDailyCandle,
    HistoricalDailyCandleBatch,
    HistoricalResponsePage,
)
from india_swing.market_data.provider import (
    HistoricalEmptyProviderResponseError,
    HistoricalProviderRequestRejectedError,
)
from india_swing.market_data.snapshot_store import LocalMarketSnapshotStore
from india_swing.market_data.upstox import UPSTOX_PROVIDER, UpstoxHistoricalDataAdapter
from tests.test_historical_backfill import (
    DAY_ONE,
    DAY_TWO,
    REQUESTED_AT,
    calendar,
    plan,
    registry,
    security_master_sources,
    two_session_body,
)
from tests.test_historical_backfill_gaps import gap_evidence
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
from tests.test_identity_registry import security_row
from tests.test_kite_instruments import instrument_snapshot
from tests.test_market_data import FakeKiteClient
from tests.test_market_data import adapter as kite_test_adapter
from tests.test_upstox_market_data import FakeTransport, adapter, response
from tests.test_upstox_instruments import (
    OBSERVED_AT as CATALOG_OBSERVED_AT,
    equity_row,
    raw_catalog,
)


class FakeDailySessionCache:
    def __init__(self, cached: KiteCredentials | None = None) -> None:
        self.cached = cached
        self.load_calls: list[tuple[str, datetime]] = []
        self.save_calls: list[tuple[KiteCredentials, datetime]] = []
        self.clear_calls = 0

    def load(self, api_key: str, *, now: datetime) -> KiteCredentials | None:
        self.load_calls.append((api_key, now))
        return self.cached

    def save(
        self,
        credentials: KiteCredentials,
        *,
        authenticated_at: datetime,
    ) -> datetime:
        self.save_calls.append((credentials, authenticated_at))
        return authenticated_at

    def clear(self) -> None:
        self.clear_calls += 1
        self.cached = None


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


class FakeKiteHistoricalConnector:
    provider = KITE_PROVIDER
    provider_version = "fake-kite-historical-connector/v1"

    def __init__(self) -> None:
        self.calls: list = []

    def fetch_historical_daily(self, request) -> HistoricalDailyCandleBatch:
        self.calls.append(request)
        candles = tuple(
            HistoricalDailyCandle(
                session=session,
                open=Decimal("1600.00"),
                high=Decimal("1620.00"),
                low=Decimal("1590.00"),
                close=Decimal("1610.00"),
                volume=100,
            )
            for session in request.sessions
        )
        page = HistoricalResponsePage(
            first_session=request.sessions[0],
            last_session=request.sessions[-1],
            payload_sha256="b" * 64,
            row_count=len(request.sessions),
        )
        return HistoricalDailyCandleBatch(
            request=request,
            observed_at=datetime(2026, 7, 17, 11, 0, tzinfo=timezone.utc),
            provider_version=self.provider_version,
            candles=candles,
            response_pages=(page,),
        )


def kite_plan(root):
    identity = registry(root / "identity", [security_row()], [security_row()])
    stored_snapshot = instrument_snapshot(root / "kite-snapshot")
    resolver = KiteInstrumentSnapshotResolver(stored_snapshot)
    value = build_historical_backfill_plan(
        registry=identity,
        security_master_sources=security_master_sources(root / "identity", identity),
        calendar=calendar(DAY_ONE, DAY_ONE),
        resolver=resolver,
        coverage_start=DAY_ONE,
        coverage_end=DAY_ONE,
        requested_at=REQUESTED_AT,
    )
    return value, stored_snapshot


class ProviderEvidenceValidationTests(unittest.TestCase):
    def _args(self, **overrides):
        values = {
            "provider": UPSTOX_PROVIDER,
            "upstox_catalog_id": "c" * 64,
            "kite_instrument_snapshot_id": None,
        }
        values.update(overrides)
        return argparse.Namespace(**values)

    def test_upstox_requires_its_own_evidence_id(self) -> None:
        with self.assertRaises(ValueError):
            _require_provider_evidence(
                self._args(upstox_catalog_id=None)
            )

    def test_upstox_rejects_kite_evidence_id(self) -> None:
        with self.assertRaises(ValueError):
            _require_provider_evidence(
                self._args(kite_instrument_snapshot_id="d" * 64)
            )

    def test_kite_requires_its_own_evidence_id(self) -> None:
        with self.assertRaises(ValueError):
            _require_provider_evidence(
                self._args(
                    provider=KITE_PROVIDER,
                    upstox_catalog_id=None,
                    kite_instrument_snapshot_id=None,
                )
            )

    def test_kite_rejects_upstox_evidence_id(self) -> None:
        with self.assertRaises(ValueError):
            _require_provider_evidence(
                self._args(
                    provider=KITE_PROVIDER,
                    upstox_catalog_id="c" * 64,
                    kite_instrument_snapshot_id="d" * 64,
                )
            )

    def test_exact_matching_evidence_id_passes(self) -> None:
        _require_provider_evidence(self._args())
        _require_provider_evidence(
            self._args(
                provider=KITE_PROVIDER,
                upstox_catalog_id=None,
                kite_instrument_snapshot_id="d" * 64,
            )
        )

    def test_unsupported_provider_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            _require_provider_evidence(self._args(provider="ZERODHA_FUTURES"))


class ProviderParserTests(unittest.TestCase):
    def test_provider_defaults_to_upstox_for_backward_compatibility(self) -> None:
        args = parser().parse_args(plan_arguments("plan"))
        self.assertEqual(args.provider, UPSTOX_PROVIDER)
        self.assertIsNone(args.kite_instrument_snapshot_id)

    def test_explicit_kite_provider_parses(self) -> None:
        args_list = plan_arguments("plan")
        args_list += ["--provider", KITE_PROVIDER]
        args = parser().parse_args(args_list)
        self.assertEqual(args.provider, KITE_PROVIDER)

    def test_unsupported_provider_choice_is_rejected_by_argparse(self) -> None:
        args_list = plan_arguments("plan") + ["--provider", "ZERODHA_FUTURES"]
        with self.assertRaises(SystemExit):
            parser().parse_args(args_list)

    def test_kite_interactive_login_flag_available_on_run_pilot_and_fetch(
        self,
    ) -> None:
        run_args = parser().parse_args(
            plan_arguments("run")
            + ["--kite-interactive-login", "--kite-refresh-login"]
        )
        self.assertTrue(run_args.kite_interactive_login)
        self.assertTrue(run_args.kite_refresh_login)

        pilot_args = parser().parse_args(
            plan_arguments("pilot")
            + [
                "--maximum-total-requests",
                "1",
                "--nse-artifact-id",
                "a" * 64,
                "--reconciled-at",
                PILOT_RECONCILED_AT.isoformat(),
                "--kite-interactive-login",
                "--kite-refresh-login",
            ]
        )
        self.assertTrue(pilot_args.kite_interactive_login)
        self.assertTrue(pilot_args.kite_refresh_login)

        fetch_args = parser().parse_args(
            [
                "kite-instruments-fetch",
                "--kite-interactive-login",
                "--kite-refresh-login",
            ]
        )
        self.assertTrue(fetch_args.kite_interactive_login)
        self.assertTrue(fetch_args.kite_refresh_login)

    def test_quarantine_empty_responses_flag_exists_only_on_run_and_defaults_false(
        self,
    ) -> None:
        run_args = parser().parse_args(plan_arguments("run"))
        self.assertFalse(run_args.quarantine_empty_responses)
        self.assertFalse(run_args.quarantine_request_rejections)

        explicit_args = parser().parse_args(
            plan_arguments("run") + ["--quarantine-empty-responses"]
        )
        self.assertTrue(explicit_args.quarantine_empty_responses)

        rejection_args = parser().parse_args(
            plan_arguments("run") + ["--quarantine-request-rejections"]
        )
        self.assertTrue(rejection_args.quarantine_request_rejections)

        plan_only_args = parser().parse_args(plan_arguments("plan"))
        self.assertFalse(hasattr(plan_only_args, "quarantine_empty_responses"))
        self.assertFalse(hasattr(plan_only_args, "quarantine_request_rejections"))

        pilot_args = parser().parse_args(pilot_arguments())
        self.assertFalse(hasattr(pilot_args, "quarantine_empty_responses"))
        self.assertFalse(hasattr(pilot_args, "quarantine_request_rejections"))

        fetch_args = parser().parse_args(["kite-instruments-fetch"])
        self.assertFalse(hasattr(fetch_args, "quarantine_empty_responses"))
        self.assertFalse(hasattr(fetch_args, "quarantine_request_rejections"))


class ResolverForProviderTests(unittest.TestCase):
    def test_kite_resolver_wiring_is_credential_free(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stored_snapshot = instrument_snapshot(root)
            market_config = type(
                "Config", (), {"data_root": root}
            )()
            args = argparse.Namespace(
                provider=KITE_PROVIDER,
                kite_instrument_snapshot_id=stored_snapshot.manifest.snapshot_id,
            )
            with patch(
                "india_swing.market_data.backfill_cli.KiteCredentials.from_env",
                side_effect=AssertionError("credentials must not be read"),
            ):
                resolver = _resolver_for_provider(args, market_config)

        self.assertIsInstance(resolver, KiteInstrumentSnapshotResolver)
        self.assertEqual(resolver.provider, KITE_PROVIDER)


class ConnectorFactoryTests(unittest.TestCase):
    def test_upstox_plan_uses_upstox_credentials_and_adapter(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            value = plan(Path(temp_dir))
            args = argparse.Namespace(
                kite_interactive_login=False,
                kite_refresh_login=False,
            )
            fake_connector = object()
            with (
                patch(
                    "india_swing.market_data.backfill_cli.UpstoxCredentials.from_env",
                    return_value="fake-credentials",
                ),
                patch(
                    "india_swing.market_data.backfill_cli.UpstoxHistoricalDataAdapter",
                    return_value=fake_connector,
                ) as adapter_cls,
            ):
                connector = _connector_for_plan(value, args)

        self.assertIs(connector, fake_connector)
        adapter_cls.assert_called_once_with("fake-credentials")

    def test_kite_interactive_login_is_rejected_for_upstox_plan(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            value = plan(Path(temp_dir))
            args = argparse.Namespace(kite_interactive_login=True)
            with self.assertRaises(ValueError):
                _connector_for_plan(value, args)

    def test_kite_refresh_login_is_rejected_for_upstox_plan(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            value = plan(Path(temp_dir))
            args = argparse.Namespace(
                kite_interactive_login=False,
                kite_refresh_login=True,
            )
            with self.assertRaises(ValueError):
                _connector_for_plan(value, args)

    def test_kite_plan_uses_environment_credentials_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            value, _ = kite_plan(Path(temp_dir))
            args = argparse.Namespace(
                kite_interactive_login=False,
                kite_refresh_login=False,
            )
            fake_connector = object()
            with (
                patch(
                    "india_swing.market_data.backfill_cli.KiteCredentials.from_env",
                    return_value="fake-kite-credentials",
                ),
                patch(
                    "india_swing.market_data.backfill_cli.KiteMarketDataAdapter"
                    ".from_official_sdk",
                    return_value=fake_connector,
                ) as adapter_cls,
                patch(
                    "india_swing.market_data.backfill_cli.KiteInteractiveAuthenticator"
                    ".from_official_sdk",
                    side_effect=AssertionError(
                        "interactive login must not be used"
                    ),
                ),
            ):
                connector = _connector_for_plan(value, args)

        self.assertIs(connector, fake_connector)
        adapter_cls.assert_called_once_with("fake-kite-credentials")

    def test_kite_plan_uses_interactive_login_only_when_flag_is_set(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            value, _ = kite_plan(Path(temp_dir))
            args = argparse.Namespace(
                kite_interactive_login=True,
                kite_refresh_login=False,
            )
            login_credentials = KiteLoginCredentials("app-key", "app-secret")
            interactive_credentials = KiteCredentials(
                "app-key",
                "interactive-token",
            )
            fake_authenticator = type(
                "FakeAuthenticator",
                (),
                {"login": lambda self: interactive_credentials},
            )()
            cache = FakeDailySessionCache()
            fake_connector = object()
            with (
                patch(
                    "india_swing.market_data.backfill_cli.KiteLoginCredentials.from_env",
                    return_value=login_credentials,
                ),
                patch(
                    "india_swing.market_data.backfill_cli.default_kite_session_cache_path",
                    return_value=Path("cache.json"),
                ),
                patch(
                    "india_swing.market_data.backfill_cli.KiteDailySessionCache",
                    return_value=cache,
                ),
                patch(
                    "india_swing.market_data.backfill_cli.LoopbackKiteCallbackReceiver",
                    return_value="fake-receiver",
                ),
                patch(
                    "india_swing.market_data.backfill_cli.KiteInteractiveAuthenticator"
                    ".from_official_sdk",
                    return_value=fake_authenticator,
                ) as authenticator_factory,
                patch(
                    "india_swing.market_data.backfill_cli.KiteCredentials.from_env",
                    side_effect=AssertionError(
                        "non-interactive credentials must not be used"
                    ),
                ),
                patch(
                    "india_swing.market_data.backfill_cli.KiteMarketDataAdapter"
                    ".from_official_sdk",
                    return_value=fake_connector,
                ) as adapter_cls,
            ):
                connector = _connector_for_plan(value, args)

        self.assertIs(connector, fake_connector)
        authenticator_factory.assert_called_once_with(
            login_credentials, "fake-receiver"
        )
        adapter_cls.assert_called_once_with(interactive_credentials)
        self.assertEqual(cache.load_calls[0][0], "app-key")
        self.assertEqual(len(cache.save_calls), 1)
        self.assertIs(cache.save_calls[0][0], interactive_credentials)

    def test_kite_cached_session_skips_receiver_browser_and_login(self) -> None:
        args = argparse.Namespace(
            kite_interactive_login=True,
            kite_refresh_login=False,
        )
        login_credentials = KiteLoginCredentials("app-key", "app-secret")
        cached = KiteCredentials("app-key", "cached-access-token")
        cache = FakeDailySessionCache(cached)
        with (
            patch(
                "india_swing.market_data.backfill_cli.KiteLoginCredentials.from_env",
                return_value=login_credentials,
            ),
            patch(
                "india_swing.market_data.backfill_cli.default_kite_session_cache_path",
                return_value=Path("cache.json"),
            ),
            patch(
                "india_swing.market_data.backfill_cli.KiteDailySessionCache",
                return_value=cache,
            ),
            patch(
                "india_swing.market_data.backfill_cli.KiteCachedCredentialValidator"
                ".from_official_sdk",
                return_value=type(
                    "ValidCachedSession",
                    (),
                    {"is_valid": lambda self: True},
                )(),
            ) as validator_factory,
            patch(
                "india_swing.market_data.backfill_cli.LoopbackKiteCallbackReceiver",
                side_effect=AssertionError("receiver must not be constructed"),
            ),
            patch(
                "india_swing.market_data.backfill_cli.KiteInteractiveAuthenticator"
                ".from_official_sdk",
                side_effect=AssertionError("browser login must not be used"),
            ),
        ):
            result = _kite_credentials(args)

        self.assertIs(result, cached)
        self.assertEqual(cache.load_calls[0][0], "app-key")
        self.assertEqual(cache.save_calls, [])
        self.assertEqual(cache.clear_calls, 0)
        validator_factory.assert_called_once_with(cached)

    def test_invalid_cached_session_is_cleared_and_fresh_login_is_used(
        self,
    ) -> None:
        args = argparse.Namespace(
            kite_interactive_login=True,
            kite_refresh_login=False,
        )
        login_credentials = KiteLoginCredentials("app-key", "app-secret")
        cached = KiteCredentials("app-key", "invalid-cached-access-token")
        fresh = KiteCredentials("app-key", "fresh-access-token")
        cache = FakeDailySessionCache(cached)
        authenticator = type(
            "FakeAuthenticator",
            (),
            {"login": lambda self: fresh},
        )()
        validator = type(
            "InvalidCachedSession",
            (),
            {"is_valid": lambda self: False},
        )()
        with (
            patch(
                "india_swing.market_data.backfill_cli.KiteLoginCredentials.from_env",
                return_value=login_credentials,
            ),
            patch(
                "india_swing.market_data.backfill_cli.default_kite_session_cache_path",
                return_value=Path("cache.json"),
            ),
            patch(
                "india_swing.market_data.backfill_cli.KiteDailySessionCache",
                return_value=cache,
            ),
            patch(
                "india_swing.market_data.backfill_cli.KiteCachedCredentialValidator"
                ".from_official_sdk",
                return_value=validator,
            ),
            patch(
                "india_swing.market_data.backfill_cli.LoopbackKiteCallbackReceiver",
                return_value="receiver",
            ),
            patch(
                "india_swing.market_data.backfill_cli.KiteInteractiveAuthenticator"
                ".from_official_sdk",
                return_value=authenticator,
            ),
        ):
            result = _kite_credentials(args)

        self.assertIs(result, fresh)
        self.assertEqual(cache.clear_calls, 1)
        self.assertEqual(len(cache.save_calls), 1)
        self.assertIs(cache.save_calls[0][0], fresh)

    def test_refresh_clears_cache_and_performs_fresh_login(self) -> None:
        args = argparse.Namespace(
            kite_interactive_login=True,
            kite_refresh_login=True,
        )
        login_credentials = KiteLoginCredentials("app-key", "app-secret")
        cached = KiteCredentials("app-key", "cached-access-token")
        fresh = KiteCredentials("app-key", "fresh-access-token")
        cache = FakeDailySessionCache(cached)
        authenticator = type(
            "FakeAuthenticator",
            (),
            {"login": lambda self: fresh},
        )()
        with (
            patch(
                "india_swing.market_data.backfill_cli.KiteLoginCredentials.from_env",
                return_value=login_credentials,
            ),
            patch(
                "india_swing.market_data.backfill_cli.default_kite_session_cache_path",
                return_value=Path("cache.json"),
            ),
            patch(
                "india_swing.market_data.backfill_cli.KiteDailySessionCache",
                return_value=cache,
            ),
            patch(
                "india_swing.market_data.backfill_cli.LoopbackKiteCallbackReceiver",
                return_value="receiver",
            ),
            patch(
                "india_swing.market_data.backfill_cli.KiteInteractiveAuthenticator"
                ".from_official_sdk",
                return_value=authenticator,
            ),
        ):
            result = _kite_credentials(args)

        self.assertIs(result, fresh)
        self.assertEqual(cache.clear_calls, 1)
        self.assertEqual(cache.load_calls, [])
        self.assertEqual(len(cache.save_calls), 1)
        self.assertIs(cache.save_calls[0][0], fresh)

    def test_refresh_requires_interactive_login(self) -> None:
        args = argparse.Namespace(
            kite_interactive_login=False,
            kite_refresh_login=True,
        )

        with self.assertRaises(ValueError):
            _kite_credentials(args)


class KiteInstrumentsFetchCliTests(unittest.TestCase):
    def test_kite_instruments_fetch_stores_one_batch_and_returns_only_metadata(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            client = FakeKiteClient()
            fake_adapter = kite_test_adapter(client)
            environment = {
                "INDIA_SWING_MARKET_DATA_ROOT": str(root),
                "INDIA_SWING_KITE_API_KEY": "runtime-key",
                "INDIA_SWING_KITE_ACCESS_TOKEN": "runtime-only-secret-token",
            }
            output = io.StringIO()
            with (
                patch.dict("os.environ", environment, clear=False),
                patch(
                    "india_swing.market_data.backfill_cli.KiteMarketDataAdapter"
                    ".from_official_sdk",
                    return_value=fake_adapter,
                ),
                redirect_stdout(output),
            ):
                exit_code = main(["kite-instruments-fetch"])

        payload = json.loads(output.getvalue())
        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["status"], "KITE_INSTRUMENTS_READY")
        self.assertEqual(payload["exchange"], "NSE")
        self.assertEqual(payload["instrument_count"], 1)
        self.assertNotIn("runtime-only-secret-token", json.dumps(payload))
        self.assertEqual(client.instrument_calls, 1)

    def test_kite_instruments_fetch_uses_interactive_login_only_when_flagged(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            client = FakeKiteClient()
            fake_adapter = kite_test_adapter(client)

            class FakeAuthenticator:
                def login(self) -> KiteCredentials:
                    return KiteCredentials("interactive-key", "interactive-token")

            environment = {"INDIA_SWING_MARKET_DATA_ROOT": str(root)}
            cache = FakeDailySessionCache()
            output = io.StringIO()
            with (
                patch.dict("os.environ", environment, clear=False),
                patch(
                    "india_swing.market_data.backfill_cli.KiteLoginCredentials.from_env",
                    return_value=KiteLoginCredentials("app-key", "app-secret"),
                ),
                patch(
                    "india_swing.market_data.backfill_cli.default_kite_session_cache_path",
                    return_value=Path("cache.json"),
                ),
                patch(
                    "india_swing.market_data.backfill_cli.KiteDailySessionCache",
                    return_value=cache,
                ),
                patch(
                    "india_swing.market_data.backfill_cli.LoopbackKiteCallbackReceiver",
                    return_value=object(),
                ),
                patch(
                    "india_swing.market_data.backfill_cli.KiteInteractiveAuthenticator"
                    ".from_official_sdk",
                    return_value=FakeAuthenticator(),
                ),
                patch(
                    "india_swing.market_data.backfill_cli.KiteCredentials.from_env",
                    side_effect=AssertionError(
                        "non-interactive credentials must not be used"
                    ),
                ),
                patch(
                    "india_swing.market_data.backfill_cli.KiteMarketDataAdapter"
                    ".from_official_sdk",
                    return_value=fake_adapter,
                ),
                redirect_stdout(output),
            ):
                exit_code = main(
                    ["kite-instruments-fetch", "--kite-interactive-login"]
                )

        payload = json.loads(output.getvalue())
        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["status"], "KITE_INSTRUMENTS_READY")


class KitePlanRunPilotCliTests(unittest.TestCase):
    def test_kite_run_command_is_credential_wired_through_the_closed_factory(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value, _ = kite_plan(root / "inputs")
            connector = FakeKiteHistoricalConnector()
            environment = {"INDIA_SWING_MARKET_DATA_ROOT": str(root / "market")}
            args = plan_arguments("run") + [
                "--provider",
                KITE_PROVIDER,
                "--kite-instrument-snapshot-id",
                "d" * 64,
            ]
            args.remove("--upstox-catalog-id")
            args.remove("c" * 64)
            output = io.StringIO()
            with (
                patch.dict("os.environ", environment, clear=False),
                patch(
                    "india_swing.market_data.backfill_cli._configured_plan",
                    return_value=value,
                ),
                patch(
                    "india_swing.market_data.backfill_cli.KiteMarketDataAdapter"
                    ".from_official_sdk",
                    return_value=connector,
                ),
                patch(
                    "india_swing.market_data.backfill_cli.KiteCredentials.from_env",
                    return_value=KiteCredentials("k", "runtime-only-secret"),
                ),
                redirect_stdout(output),
            ):
                exit_code = main(args)

        payload = json.loads(output.getvalue())
        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["status"], "SAFE_REQUESTS_COMPLETE")
        self.assertEqual(payload["provider"], KITE_PROVIDER)
        self.assertGreater(len(connector.calls), 0)
        self.assertNotIn("runtime-only-secret", output.getvalue())


class FakeKiteQuarantineConnector:
    provider = KITE_PROVIDER
    provider_version = "fake-kite-quarantine-connector/v1"

    def __init__(self) -> None:
        self.calls: list = []

    def fetch_historical_daily(self, request) -> HistoricalDailyCandleBatch:
        self.calls.append(request)
        raise HistoricalEmptyProviderResponseError(
            provider=self.provider,
            provider_version=self.provider_version,
            provider_instrument_id=request.binding.provider_instrument_id,
            session=request.sessions[-1],
            observed_at=request.requested_at,
            normalized_response_sha256="c" * 64,
        )


class FakeKiteRequestRejectionConnector:
    provider = KITE_PROVIDER
    provider_version = "fake-kite-request-rejection-connector/v1"

    def __init__(self) -> None:
        self.calls: list = []

    def fetch_historical_daily(self, request) -> HistoricalDailyCandleBatch:
        self.calls.append(request)
        raise HistoricalProviderRequestRejectedError(
            provider=self.provider,
            provider_version=self.provider_version,
            provider_instrument_id=request.binding.provider_instrument_id,
            session=request.sessions[-1],
            observed_at=request.requested_at,
            upstream_error_type="InputException",
            normalized_response_sha256="d" * 64,
        )


def _kite_run_args(root: Path, *, extra: list[str] | None = None) -> list[str]:
    args = plan_arguments("run") + [
        "--provider",
        KITE_PROVIDER,
        "--kite-instrument-snapshot-id",
        "d" * 64,
    ]
    args.remove("--upstox-catalog-id")
    args.remove("c" * 64)
    return args + (extra or [])


class RunQuarantineCliTests(unittest.TestCase):
    def test_flag_persists_a_gap_and_reports_only_sanitized_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value, _ = kite_plan(root / "inputs")
            connector = FakeKiteQuarantineConnector()
            environment = {"INDIA_SWING_MARKET_DATA_ROOT": str(root / "market")}
            args = _kite_run_args(root, extra=["--quarantine-empty-responses"])
            output = io.StringIO()
            with (
                patch.dict("os.environ", environment, clear=False),
                patch(
                    "india_swing.market_data.backfill_cli._configured_plan",
                    return_value=value,
                ),
                patch(
                    "india_swing.market_data.backfill_cli.KiteMarketDataAdapter"
                    ".from_official_sdk",
                    return_value=connector,
                ),
                patch(
                    "india_swing.market_data.backfill_cli.KiteCredentials.from_env",
                    return_value=KiteCredentials("k", "runtime-only-secret"),
                ),
                patch(
                    "india_swing.market_data.backfill_cli.HistoricalBackfillRunner.is_complete",
                    return_value=True,
                ),
                redirect_stdout(output),
            ):
                exit_code = main(args)

            payload = json.loads(output.getvalue())
            gap_store = LocalHistoricalBackfillSessionGapStore(root / "market")
            gaps = gap_store.load_unresolved(value.plan_id)

        self.assertEqual(exit_code, 0)
        self.assertNotEqual(payload["status"], "SAFE_REQUESTS_COMPLETE")
        self.assertEqual(payload["status"], "SAFE_REQUESTS_PARTIAL")
        self.assertFalse(payload["safe_requests_complete"])
        self.assertEqual(payload["unresolved_gap_count"], 1)
        self.assertEqual(len(payload["unresolved_gap_evidence_ids"]), 1)
        self.assertNotIn("runtime-only-secret", output.getvalue())
        self.assertEqual(len(gaps), 1)
        self.assertEqual(gaps[0].evidence_id, payload["unresolved_gap_evidence_ids"][0])

    def test_without_the_flag_an_empty_response_aborts_and_writes_no_gap(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value, _ = kite_plan(root / "inputs")
            connector = FakeKiteQuarantineConnector()
            environment = {"INDIA_SWING_MARKET_DATA_ROOT": str(root / "market")}
            args = _kite_run_args(root)
            output = io.StringIO()
            stderr = io.StringIO()
            with (
                patch.dict("os.environ", environment, clear=False),
                patch(
                    "india_swing.market_data.backfill_cli._configured_plan",
                    return_value=value,
                ),
                patch(
                    "india_swing.market_data.backfill_cli.KiteMarketDataAdapter"
                    ".from_official_sdk",
                    return_value=connector,
                ),
                patch(
                    "india_swing.market_data.backfill_cli.KiteCredentials.from_env",
                    return_value=KiteCredentials("k", "runtime-only-secret"),
                ),
                redirect_stdout(output),
                redirect_stderr(stderr),
            ):
                exit_code = main(args)

            gaps_exist = (root / "market" / "historical-backfill-session-gaps").exists()

        self.assertEqual(exit_code, 2)
        self.assertEqual(output.getvalue(), "")
        payload = json.loads(stderr.getvalue())
        self.assertEqual(payload["error_type"], "HistoricalEmptyProviderResponseError")
        self.assertNotIn("runtime-only-secret", stderr.getvalue())
        self.assertFalse(gaps_exist)

    def test_request_rejection_flag_persists_non_actionable_gap(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            value, _ = kite_plan(root / "inputs")
            connector = FakeKiteRequestRejectionConnector()
            environment = {"INDIA_SWING_MARKET_DATA_ROOT": str(root / "market")}
            args = _kite_run_args(
                root, extra=["--quarantine-request-rejections"]
            )
            output = io.StringIO()
            with (
                patch.dict("os.environ", environment, clear=False),
                patch(
                    "india_swing.market_data.backfill_cli._configured_plan",
                    return_value=value,
                ),
                patch(
                    "india_swing.market_data.backfill_cli.KiteMarketDataAdapter"
                    ".from_official_sdk",
                    return_value=connector,
                ),
                patch(
                    "india_swing.market_data.backfill_cli.KiteCredentials.from_env",
                    return_value=KiteCredentials("k", "runtime-only-secret"),
                ),
                redirect_stdout(output),
            ):
                exit_code = main(args)

            payload = json.loads(output.getvalue())
            gaps = LocalHistoricalBackfillSessionGapStore(
                root / "market"
            ).load_unresolved(value.plan_id)

        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["status"], "SAFE_REQUESTS_PARTIAL")
        self.assertEqual(payload["unresolved_gap_count"], 1)
        self.assertEqual(
            payload["unresolved_gaps_by_classification"],
            {"UNRESOLVED_PROVIDER_REQUEST_REJECTION": 1},
        )
        self.assertEqual(len(gaps), 1)
        self.assertIs(
            gaps[0].classification,
            HistoricalBackfillGapClassification.UNRESOLVED_PROVIDER_REQUEST_REJECTION,
        )

    def test_pilot_has_no_quarantine_flag_or_gap_store_wiring(self) -> None:
        args = parser().parse_args(pilot_arguments())
        self.assertFalse(hasattr(args, "quarantine_empty_responses"))


class GapAdjudicateCliTests(unittest.TestCase):
    def test_parser_accepts_only_its_exact_required_arguments(self) -> None:
        args = parser().parse_args(
            [
                "gap-adjudicate",
                "--plan-id",
                "a" * 64,
                "--nse-artifact-id",
                "b" * 64,
                "--adjudicated-at",
                "2026-07-17T10:00:00+00:00",
            ]
        )

        self.assertEqual(args.command, "gap-adjudicate")
        self.assertEqual(args.plan_id, "a" * 64)
        self.assertEqual(args.nse_artifact_ids, ["b" * 64])
        self.assertFalse(hasattr(args, "kite_interactive_login"))
        self.assertFalse(hasattr(args, "provider"))
        self.assertFalse(hasattr(args, "quarantine_empty_responses"))

        for missing in (
            ["gap-adjudicate", "--nse-artifact-id", "b" * 64, "--adjudicated-at", "2026-07-17T10:00:00+00:00"],
            ["gap-adjudicate", "--plan-id", "a" * 64, "--adjudicated-at", "2026-07-17T10:00:00+00:00"],
            ["gap-adjudicate", "--plan-id", "a" * 64, "--nse-artifact-id", "b" * 64],
        ):
            with self.subTest(missing=missing):
                with self.assertRaises(SystemExit):
                    parser().parse_args(missing)

    def test_end_to_end_persists_one_report_and_leaves_gap_files_byte_identical(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            market_root = root / "market"
            historical_root = root / "historical"
            daily_root = root / "nse-source" / "daily"

            artifact = nse_artifact(root / "nse-source")
            LocalHistoricalPriceArtifactStore(historical_root, daily_root).put(
                artifact
            )

            gap = gap_evidence()
            LocalHistoricalBackfillSessionGapStore(market_root).put(gap)
            gap_path = (
                market_root
                / "historical-backfill-session-gaps"
                / gap.plan_id
                / gap.request_id
                / f"{gap.session.isoformat()}.json"
            )
            before_bytes = gap_path.read_bytes()

            environment = {
                "INDIA_SWING_MARKET_DATA_ROOT": str(market_root),
                "INDIA_SWING_HISTORICAL_PRICES_ROOT": str(historical_root),
                "INDIA_SWING_DAILY_REPORTS_ROOT": str(daily_root),
            }
            output = io.StringIO()
            with (
                patch.dict("os.environ", environment, clear=False),
                patch(
                    "india_swing.market_data.backfill_cli.KiteCredentials.from_env",
                    side_effect=AssertionError("credentials must not be read"),
                ),
                patch(
                    "india_swing.market_data.backfill_cli.UpstoxCredentials.from_env",
                    side_effect=AssertionError("credentials must not be read"),
                ),
                patch(
                    "india_swing.market_data.backfill_cli.KiteMarketDataAdapter"
                    ".from_official_sdk",
                    side_effect=AssertionError(
                        "provider adapter must not be constructed"
                    ),
                ),
                redirect_stdout(output),
            ):
                exit_code = main(
                    [
                        "gap-adjudicate",
                        "--plan-id",
                        gap.plan_id,
                        "--nse-artifact-id",
                        artifact.artifact_id,
                        "--adjudicated-at",
                        "2026-07-17T10:00:00+00:00",
                    ]
                )

            after_bytes = gap_path.read_bytes()

        payload = json.loads(output.getvalue())
        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["status"], "GAP_ADJUDICATION_REPORTED")
        self.assertEqual(payload["plan_id"], gap.plan_id)
        self.assertEqual(payload["gap_count"], 1)
        self.assertEqual(
            payload["counts_by_original_classification"],
            {"UNRESOLVED_EMPTY_PROVIDER_RESPONSE": 1},
        )
        self.assertEqual(
            payload["counts_by_nse_status"], {"EXACT_TRADED_BAR_PRESENT": 1}
        )
        self.assertEqual(
            payload["counts_by_action"],
            {"REVIEW_PINNED_NSE_BAR_FOR_DATASET_USE": 1},
        )
        self.assertEqual(payload["nse_artifact_ids"], [artifact.artifact_id])
        self.assertTrue(payload["collection_only"])
        self.assertFalse(payload["actionable"])
        self.assertFalse(payload["gaps_resolved"])
        self.assertFalse(payload["training_eligible"])
        self.assertEqual(after_bytes, before_bytes)
        self.assertNotIn("candle", json.dumps(payload).lower())

    def test_no_unresolved_gaps_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            environment = {"INDIA_SWING_MARKET_DATA_ROOT": str(root / "market")}
            output = io.StringIO()
            stderr = io.StringIO()
            with (
                patch.dict("os.environ", environment, clear=False),
                redirect_stdout(output),
                redirect_stderr(stderr),
            ):
                exit_code = main(
                    [
                        "gap-adjudicate",
                        "--plan-id",
                        "a" * 64,
                        "--nse-artifact-id",
                        "b" * 64,
                        "--adjudicated-at",
                        "2026-07-17T10:00:00+00:00",
                    ]
                )

        self.assertEqual(exit_code, 2)
        self.assertEqual(output.getvalue(), "")
        payload = json.loads(stderr.getvalue())
        self.assertEqual(payload["error_type"], "ValueError")

    def test_session_coverage_disagreement_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            market_root = root / "market"
            historical_root = root / "historical"
            daily_root = root / "nse-source" / "daily"
            artifact = nse_artifact(root / "nse-source")
            LocalHistoricalPriceArtifactStore(historical_root, daily_root).put(
                artifact
            )
            gap = gap_evidence(session=date(2026, 7, 20))
            LocalHistoricalBackfillSessionGapStore(market_root).put(gap)

            environment = {
                "INDIA_SWING_MARKET_DATA_ROOT": str(market_root),
                "INDIA_SWING_HISTORICAL_PRICES_ROOT": str(historical_root),
                "INDIA_SWING_DAILY_REPORTS_ROOT": str(daily_root),
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
                        "gap-adjudicate",
                        "--plan-id",
                        gap.plan_id,
                        "--nse-artifact-id",
                        artifact.artifact_id,
                        "--adjudicated-at",
                        "2026-07-21T10:00:00+00:00",
                    ]
                )

        self.assertEqual(exit_code, 2)
        self.assertEqual(output.getvalue(), "")
        payload = json.loads(stderr.getvalue())
        self.assertEqual(payload["error_type"], "HistoricalGapAdjudicationError")


if __name__ == "__main__":
    unittest.main()
