from __future__ import annotations

import argparse
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import MagicMock, patch

from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

from india_swing.historical_prices import LocalHistoricalPriceArtifactStore
from india_swing.market_data.backfill import (
    HistoricalBackfillRunner,
    LocalHistoricalBackfillProgressStore,
    build_historical_backfill_plan,
)
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
from india_swing.market_data.reconciliation_run import (
    MAXIMUM_RECONCILIATIONS_PER_RUN,
    HistoricalReconciliationIndex,
    HistoricalReconciliationIndexEntry,
)
from india_swing.market_data.snapshot_store import LocalMarketSnapshotStore
from india_swing.market_data.upstox import UPSTOX_PROVIDER, UpstoxHistoricalDataAdapter
from tests.test_historical_backfill import (
    DAY_ONE,
    DAY_TWO,
    REQUESTED_AT,
    RUN_CLOCK,
    calendar,
    plan,
    registry,
    security_master_sources,
    two_session_body,
)
from tests.test_historical_evaluation_corpus import (
    BUILT_AT as CORPUS_BUILT_AT,
    build_two_symbol_fixture,
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


def reconcile_plan_arguments(**overrides) -> list[str]:
    values = {
        "--expected-plan-id": "d" * 64,
        "--expected-progress-id": "e" * 64,
        "--nse-artifact-id": "f" * 64,
        "--maximum-requests": "2",
        "--reconciled-at": PILOT_RECONCILED_AT.isoformat(),
    }
    values.update(overrides)
    arguments = plan_arguments("reconcile-plan")
    for name, value in values.items():
        arguments += [name, value]
    return arguments


def _stub_reconciliation_index(**overrides) -> SimpleNamespace:
    values = dict(
        index_id="1" * 64,
        prior_index_id=None,
        plan_id="d" * 64,
        progress_id="e" * 64,
        provider=UPSTOX_PROVIDER,
        updated_at=PILOT_RECONCILED_AT,
        total_completion_count=4,
        indexed_count=4,
        remaining_count=0,
        passed_count=3,
        failed_count=1,
        complete=True,
        collection_only=True,
        actionable=False,
        training_eligible=False,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


class ReconcilePlanCliTests(unittest.TestCase):
    def test_parser_requires_exact_arguments_and_rejects_provider_flags(
        self,
    ) -> None:
        args = parser().parse_args(
            reconcile_plan_arguments() + ["--prior-index-id", "2" * 64]
        )

        self.assertEqual(args.command, "reconcile-plan")
        self.assertEqual(args.expected_plan_id, "d" * 64)
        self.assertEqual(args.expected_progress_id, "e" * 64)
        self.assertEqual(args.nse_artifact_ids, ["f" * 64])
        self.assertEqual(args.maximum_requests, 2)
        self.assertEqual(args.reconciled_at, PILOT_RECONCILED_AT)
        self.assertEqual(args.prior_index_id, "2" * 64)

        self.assertFalse(hasattr(args, "kite_interactive_login"))
        self.assertFalse(hasattr(args, "kite_refresh_login"))
        self.assertFalse(hasattr(args, "quarantine_empty_responses"))
        self.assertFalse(hasattr(args, "quarantine_request_rejections"))
        self.assertFalse(hasattr(args, "allow_collection_with_issues"))

        self.assertIsNone(parser().parse_args(reconcile_plan_arguments()).prior_index_id)

        for flag in (
            "--kite-interactive-login",
            "--kite-refresh-login",
            "--access-token",
            "--api-key",
        ):
            with self.subTest(flag=flag):
                with self.assertRaises(SystemExit):
                    parser().parse_args(reconcile_plan_arguments() + [flag])

    def test_parser_rejects_each_missing_required_argument(self) -> None:
        for name in (
            "--expected-plan-id",
            "--expected-progress-id",
            "--nse-artifact-id",
            "--maximum-requests",
            "--reconciled-at",
        ):
            with self.subTest(missing=name):
                arguments = reconcile_plan_arguments()
                index = arguments.index(name)
                with self.assertRaises(SystemExit):
                    parser().parse_args(arguments[:index] + arguments[index + 2 :])

    def test_arguments_and_stores_are_threaded_into_the_service(self) -> None:
        stub_plan = object()
        stub_artifact = object()
        mock_service_instance = MagicMock()
        mock_service_instance.run.return_value = _stub_reconciliation_index()
        mock_service_class = MagicMock(return_value=mock_service_instance)
        mock_artifact_store = MagicMock()
        mock_artifact_store.return_value.get.return_value = SimpleNamespace(
            artifact=stub_artifact
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            environment = {
                "INDIA_SWING_MARKET_DATA_ROOT": str(Path(temp_dir) / "market"),
                "INDIA_SWING_HISTORICAL_PRICES_ROOT": str(
                    Path(temp_dir) / "historical"
                ),
                "INDIA_SWING_DAILY_REPORTS_ROOT": str(Path(temp_dir) / "daily"),
            }
            output = io.StringIO()
            with (
                patch.dict("os.environ", environment, clear=False),
                patch(
                    "india_swing.market_data.backfill_cli._configured_plan",
                    return_value=stub_plan,
                ),
                patch(
                    "india_swing.market_data.backfill_cli"
                    ".LocalHistoricalPriceArtifactStore",
                    mock_artifact_store,
                ),
                patch(
                    "india_swing.market_data.backfill_cli"
                    ".HistoricalBulkReconciliationService",
                    mock_service_class,
                ),
                patch(
                    "india_swing.market_data.backfill_cli.UpstoxCredentials.from_env",
                    side_effect=AssertionError("credentials must not be read"),
                ),
                patch(
                    "india_swing.market_data.backfill_cli.UpstoxHistoricalDataAdapter",
                    side_effect=AssertionError("provider must not be constructed"),
                ),
                redirect_stdout(output),
            ):
                exit_code = main(reconcile_plan_arguments())

        payload = json.loads(output.getvalue())
        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["status"], "HISTORICAL_RECONCILIATION_INDEX_COMPLETE")
        self.assertEqual(payload["index_id"], "1" * 64)
        self.assertIsNone(payload["prior_index_id"])
        self.assertEqual(payload["plan_id"], "d" * 64)
        self.assertEqual(payload["progress_id"], "e" * 64)
        self.assertEqual(payload["provider"], UPSTOX_PROVIDER)
        self.assertEqual(payload["updated_at"], PILOT_RECONCILED_AT.isoformat())
        self.assertEqual(payload["total_completion_count"], 4)
        self.assertEqual(payload["indexed_count"], 4)
        self.assertEqual(payload["newly_indexed_count"], 4)
        self.assertEqual(payload["remaining_count"], 0)
        self.assertEqual(payload["passed_count"], 3)
        self.assertEqual(payload["failed_count"], 1)
        self.assertTrue(payload["complete"])
        self.assertTrue(payload["collection_only"])
        self.assertFalse(payload["actionable"])
        self.assertFalse(payload["training_eligible"])

        mock_service_instance.run.assert_called_once_with(
            plan=stub_plan,
            expected_plan_id="d" * 64,
            expected_progress_id="e" * 64,
            nse_artifacts=(stub_artifact,),
            maximum_requests=2,
            reconciled_at=PILOT_RECONCILED_AT,
            prior_index_id=None,
        )
        mock_artifact_store.return_value.get.assert_called_once_with("f" * 64)

    def test_partial_index_reports_resume_counts_and_exits_zero(self) -> None:
        mock_service_instance = MagicMock()
        mock_service_instance.run.return_value = _stub_reconciliation_index(
            prior_index_id="2" * 64,
            indexed_count=3,
            remaining_count=1,
            passed_count=3,
            failed_count=0,
            complete=False,
        )
        mock_service_class = MagicMock(return_value=mock_service_instance)
        mock_index_store = MagicMock()
        mock_index_store.return_value.get.return_value = SimpleNamespace(
            entries=("first",)
        )
        mock_artifact_store = MagicMock()
        mock_artifact_store.return_value.get.return_value = SimpleNamespace(
            artifact=object()
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            environment = {
                "INDIA_SWING_MARKET_DATA_ROOT": str(Path(temp_dir) / "market"),
                "INDIA_SWING_HISTORICAL_PRICES_ROOT": str(
                    Path(temp_dir) / "historical"
                ),
                "INDIA_SWING_DAILY_REPORTS_ROOT": str(Path(temp_dir) / "daily"),
            }
            output = io.StringIO()
            with (
                patch.dict("os.environ", environment, clear=False),
                patch(
                    "india_swing.market_data.backfill_cli._configured_plan",
                    return_value=object(),
                ),
                patch(
                    "india_swing.market_data.backfill_cli"
                    ".LocalHistoricalPriceArtifactStore",
                    mock_artifact_store,
                ),
                patch(
                    "india_swing.market_data.backfill_cli"
                    ".LocalHistoricalReconciliationIndexStore",
                    mock_index_store,
                ),
                patch(
                    "india_swing.market_data.backfill_cli"
                    ".HistoricalBulkReconciliationService",
                    mock_service_class,
                ),
                redirect_stdout(output),
            ):
                exit_code = main(
                    reconcile_plan_arguments() + ["--prior-index-id", "2" * 64]
                )

        payload = json.loads(output.getvalue())
        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["status"], "HISTORICAL_RECONCILIATION_INDEX_PARTIAL")
        self.assertEqual(payload["prior_index_id"], "2" * 64)
        self.assertEqual(payload["indexed_count"], 3)
        self.assertEqual(payload["newly_indexed_count"], 2)
        self.assertEqual(payload["remaining_count"], 1)
        self.assertFalse(payload["complete"])
        self.assertEqual(
            mock_service_instance.run.call_args.kwargs["prior_index_id"],
            "2" * 64,
        )
        mock_index_store.return_value.get.assert_called_once_with("2" * 64)

    def test_service_failure_produces_sanitized_stderr_json(self) -> None:
        mock_service_class = MagicMock(
            side_effect=ValueError("secret internal lineage detail")
        )
        mock_artifact_store = MagicMock()
        mock_artifact_store.return_value.get.return_value = SimpleNamespace(
            artifact=object()
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            environment = {
                "INDIA_SWING_MARKET_DATA_ROOT": str(Path(temp_dir) / "market"),
                "INDIA_SWING_HISTORICAL_PRICES_ROOT": str(
                    Path(temp_dir) / "historical"
                ),
                "INDIA_SWING_DAILY_REPORTS_ROOT": str(Path(temp_dir) / "daily"),
            }
            output = io.StringIO()
            stderr = io.StringIO()
            with (
                patch.dict("os.environ", environment, clear=False),
                patch(
                    "india_swing.market_data.backfill_cli._configured_plan",
                    return_value=object(),
                ),
                patch(
                    "india_swing.market_data.backfill_cli"
                    ".LocalHistoricalPriceArtifactStore",
                    mock_artifact_store,
                ),
                patch(
                    "india_swing.market_data.backfill_cli"
                    ".HistoricalBulkReconciliationService",
                    mock_service_class,
                ),
                redirect_stdout(output),
                redirect_stderr(stderr),
            ):
                exit_code = main(reconcile_plan_arguments())

        self.assertEqual(exit_code, 2)
        self.assertEqual(output.getvalue(), "")
        payload = json.loads(stderr.getvalue())
        self.assertEqual(set(payload), {"status", "error_type"})
        self.assertEqual(payload["status"], "FAILED")
        self.assertNotIn("secret internal lineage detail", stderr.getvalue())

    def test_end_to_end_is_credential_free_and_resumes_from_an_exact_index(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            market_root = root / "market"
            historical_root = root / "historical"
            value = pilot_plan(root / "inputs")
            artifact = pilot_nse_artifact(root)
            stored_artifact = LocalHistoricalPriceArtifactStore(
                historical_root,
                root / "daily",
            ).put(artifact)
            snapshot_store = LocalMarketSnapshotStore(market_root)
            progress = HistoricalBackfillRunner(
                FakePilotConnector(),
                snapshot_store,
                LocalHistoricalBackfillProgressStore(market_root),
                clock=lambda: RUN_CLOCK,
            ).run(value, maximum_requests=len(value.requests))
            environment = {
                "INDIA_SWING_MARKET_DATA_ROOT": str(market_root),
                "INDIA_SWING_HISTORICAL_PRICES_ROOT": str(historical_root),
                "INDIA_SWING_DAILY_REPORTS_ROOT": str(root / "daily"),
            }
            shared = {
                "--expected-plan-id": value.plan_id,
                "--expected-progress-id": progress.progress_id,
                "--nse-artifact-id": stored_artifact.manifest.artifact_id,
                "--reconciled-at": PILOT_RECONCILED_AT.isoformat(),
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
                    "india_swing.market_data.backfill_cli.UpstoxCredentials.from_env",
                    side_effect=AssertionError("credentials must not be read"),
                ),
                patch(
                    "india_swing.market_data.backfill_cli.KiteCredentials.from_env",
                    side_effect=AssertionError("credentials must not be read"),
                ),
                patch(
                    "india_swing.market_data.backfill_cli.UpstoxHistoricalDataAdapter",
                    side_effect=AssertionError("provider must not be constructed"),
                ),
                patch(
                    "india_swing.market_data.backfill_cli.KiteMarketDataAdapter"
                    ".from_official_sdk",
                    side_effect=AssertionError("provider must not be constructed"),
                ),
            ):
                with redirect_stdout(first_output):
                    first_exit = main(
                        reconcile_plan_arguments(
                            **{**shared, "--maximum-requests": "1"}
                        )
                    )
                first = json.loads(first_output.getvalue())
                with redirect_stdout(second_output):
                    second_exit = main(
                        reconcile_plan_arguments(
                            **{
                                **shared,
                                "--maximum-requests": str(
                                    MAXIMUM_RECONCILIATIONS_PER_RUN
                                ),
                            }
                        )
                        + ["--prior-index-id", first["index_id"]]
                    )
            second = json.loads(second_output.getvalue())

        self.assertEqual((first_exit, second_exit), (0, 0))
        self.assertEqual(first["status"], "HISTORICAL_RECONCILIATION_INDEX_PARTIAL")
        self.assertEqual(first["indexed_count"], 1)
        self.assertEqual(first["newly_indexed_count"], 1)
        self.assertEqual(first["remaining_count"], 1)
        self.assertFalse(first["complete"])
        self.assertEqual(second["status"], "HISTORICAL_RECONCILIATION_INDEX_COMPLETE")
        self.assertEqual(second["prior_index_id"], first["index_id"])
        self.assertEqual(second["indexed_count"], 2)
        self.assertEqual(second["newly_indexed_count"], 1)
        self.assertEqual(second["remaining_count"], 0)
        self.assertEqual(second["passed_count"], 2)
        self.assertEqual(second["failed_count"], 0)
        self.assertTrue(second["complete"])
        self.assertTrue(second["collection_only"])
        self.assertFalse(second["actionable"])
        self.assertFalse(second["training_eligible"])


def _stub_admission_report(**overrides) -> SimpleNamespace:
    values = dict(
        report_id="a" * 64,
        plan_id="b" * 64,
        progress_id="c" * 64,
        provider="UPSTOX",
        assessed_at=datetime(2026, 7, 23, 10, 0, tzinfo=timezone.utc),
        coverage_complete=True,
        safe_requests_complete=True,
        admitted_request_count=1,
        total_request_count=1,
        gap_adjudication_report_id=None,
        collection_only=True,
        actionable=False,
        training_eligible=False,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


class DatasetAdmitCliTests(unittest.TestCase):
    def test_parser_requires_exact_arguments_and_rejects_login_flags(self) -> None:
        args = parser().parse_args(
            [
                "dataset-admit",
                "--identity-registry-id",
                "a" * 64,
                "--calendar-materialization-id",
                "b" * 64,
                "--upstox-catalog-id",
                "c" * 64,
                "--coverage-start",
                "2026-07-01",
                "--coverage-end",
                "2026-07-01",
                "--requested-at",
                "2026-07-01T09:00:00+00:00",
                "--expected-plan-id",
                "d" * 64,
                "--expected-progress-id",
                "e" * 64,
                "--reconciliation-snapshot-id",
                "f" * 64,
                "--expected-gap-evidence-id",
                "0" * 64,
                "--gap-adjudication-report-id",
                "1" * 64,
                "--assessed-at",
                "2026-07-23T10:00:00+00:00",
            ]
        )

        self.assertEqual(args.command, "dataset-admit")
        self.assertEqual(args.expected_plan_id, "d" * 64)
        self.assertEqual(args.expected_progress_id, "e" * 64)
        self.assertEqual(args.reconciliation_snapshot_ids, ["f" * 64])
        self.assertEqual(args.expected_gap_evidence_ids, ["0" * 64])
        self.assertEqual(args.gap_adjudication_report_id, "1" * 64)
        self.assertFalse(hasattr(args, "kite_interactive_login"))
        self.assertFalse(hasattr(args, "kite_refresh_login"))
        self.assertFalse(hasattr(args, "maximum_requests"))

        required = [
            "dataset-admit",
            "--identity-registry-id",
            "a" * 64,
            "--calendar-materialization-id",
            "b" * 64,
            "--upstox-catalog-id",
            "c" * 64,
            "--coverage-start",
            "2026-07-01",
            "--coverage-end",
            "2026-07-01",
            "--requested-at",
            "2026-07-01T09:00:00+00:00",
            "--expected-plan-id",
            "d" * 64,
            "--expected-progress-id",
            "e" * 64,
            "--assessed-at",
            "2026-07-23T10:00:00+00:00",
        ]
        parser().parse_args(required)
        for drop_index in (
            required.index("--expected-plan-id"),
            required.index("--expected-progress-id"),
            required.index("--assessed-at"),
        ):
            with self.subTest(missing=required[drop_index]):
                with self.assertRaises(SystemExit):
                    parser().parse_args(required[:drop_index] + required[drop_index + 2 :])

    def test_parser_allows_omitting_repeated_and_optional_arguments(self) -> None:
        args = parser().parse_args(
            [
                "dataset-admit",
                "--identity-registry-id",
                "a" * 64,
                "--calendar-materialization-id",
                "b" * 64,
                "--upstox-catalog-id",
                "c" * 64,
                "--coverage-start",
                "2026-07-01",
                "--coverage-end",
                "2026-07-01",
                "--requested-at",
                "2026-07-01T09:00:00+00:00",
                "--expected-plan-id",
                "d" * 64,
                "--expected-progress-id",
                "e" * 64,
                "--assessed-at",
                "2026-07-23T10:00:00+00:00",
            ]
        )
        self.assertIsNone(args.reconciliation_snapshot_ids)
        self.assertIsNone(args.expected_gap_evidence_ids)
        self.assertIsNone(args.gap_adjudication_report_id)

    def test_coverage_complete_success_threads_arguments_and_exits_zero(self) -> None:
        stub_plan = object()
        stub_result = SimpleNamespace(
            report=_stub_admission_report(),
            disposition_counts=(("ADMITTED", 1),),
        )
        mock_service_instance = MagicMock()
        mock_service_instance.run.return_value = stub_result
        mock_service_class = MagicMock(return_value=mock_service_instance)

        with tempfile.TemporaryDirectory() as temp_dir:
            environment = {
                "INDIA_SWING_MARKET_DATA_ROOT": str(Path(temp_dir) / "market")
            }
            output = io.StringIO()
            with (
                patch.dict("os.environ", environment, clear=False),
                patch(
                    "india_swing.market_data.backfill_cli._configured_plan",
                    return_value=stub_plan,
                ),
                patch(
                    "india_swing.market_data.backfill_cli"
                    ".HistoricalDatasetAdmissionService",
                    mock_service_class,
                ),
                redirect_stdout(output),
            ):
                exit_code = main(
                    [
                        "dataset-admit",
                        "--identity-registry-id",
                        "a" * 64,
                        "--calendar-materialization-id",
                        "b" * 64,
                        "--upstox-catalog-id",
                        "c" * 64,
                        "--coverage-start",
                        "2026-07-01",
                        "--coverage-end",
                        "2026-07-01",
                        "--requested-at",
                        "2026-07-01T09:00:00+00:00",
                        "--expected-plan-id",
                        "b" * 64,
                        "--expected-progress-id",
                        "c" * 64,
                        "--reconciliation-snapshot-id",
                        "d" * 64,
                        "--reconciliation-snapshot-id",
                        "e" * 64,
                        "--expected-gap-evidence-id",
                        "f" * 64,
                        "--gap-adjudication-report-id",
                        "1" * 64,
                        "--assessed-at",
                        "2026-07-23T10:00:00+00:00",
                    ]
                )

        self.assertEqual(exit_code, 0)
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["status"], "DATASET_ADMISSION_COVERAGE_COMPLETE")
        self.assertEqual(payload["report_id"], "a" * 64)
        self.assertEqual(payload["plan_id"], "b" * 64)
        self.assertEqual(payload["progress_id"], "c" * 64)
        self.assertEqual(payload["provider"], "UPSTOX")
        self.assertEqual(payload["assessed_at"], "2026-07-23T10:00:00+00:00")
        self.assertTrue(payload["coverage_complete"])
        self.assertTrue(payload["safe_requests_complete"])
        self.assertEqual(payload["admitted_request_count"], 1)
        self.assertEqual(payload["total_request_count"], 1)
        self.assertEqual(payload["disposition_counts"], {"ADMITTED": 1})
        self.assertIsNone(payload["gap_adjudication_report_id"])
        self.assertTrue(payload["collection_only"])
        self.assertFalse(payload["actionable"])
        self.assertFalse(payload["training_eligible"])

        mock_service_instance.run.assert_called_once_with(
            plan=stub_plan,
            expected_plan_id="b" * 64,
            expected_progress_id="c" * 64,
            reconciliation_snapshot_ids=("d" * 64, "e" * 64),
            expected_gap_evidence_ids=("f" * 64,),
            gap_adjudication_report_id="1" * 64,
            assessed_at=datetime(2026, 7, 23, 10, 0, tzinfo=timezone.utc),
        )

    def test_incomplete_coverage_exits_four(self) -> None:
        stub_result = SimpleNamespace(
            report=_stub_admission_report(
                coverage_complete=False, safe_requests_complete=False
            ),
            disposition_counts=(("RECONCILIATION_MISSING_OR_FAILED", 1),),
        )
        mock_service_instance = MagicMock()
        mock_service_instance.run.return_value = stub_result
        mock_service_class = MagicMock(return_value=mock_service_instance)

        with tempfile.TemporaryDirectory() as temp_dir:
            environment = {
                "INDIA_SWING_MARKET_DATA_ROOT": str(Path(temp_dir) / "market")
            }
            output = io.StringIO()
            with (
                patch.dict("os.environ", environment, clear=False),
                patch(
                    "india_swing.market_data.backfill_cli._configured_plan",
                    return_value=object(),
                ),
                patch(
                    "india_swing.market_data.backfill_cli"
                    ".HistoricalDatasetAdmissionService",
                    mock_service_class,
                ),
                redirect_stdout(output),
            ):
                exit_code = main(
                    [
                        "dataset-admit",
                        "--identity-registry-id",
                        "a" * 64,
                        "--calendar-materialization-id",
                        "b" * 64,
                        "--upstox-catalog-id",
                        "c" * 64,
                        "--coverage-start",
                        "2026-07-01",
                        "--coverage-end",
                        "2026-07-01",
                        "--requested-at",
                        "2026-07-01T09:00:00+00:00",
                        "--expected-plan-id",
                        "b" * 64,
                        "--expected-progress-id",
                        "c" * 64,
                        "--assessed-at",
                        "2026-07-23T10:00:00+00:00",
                    ]
                )

        self.assertEqual(exit_code, 4)
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["status"], "DATASET_ADMISSION_COVERAGE_INCOMPLETE")
        self.assertFalse(payload["coverage_complete"])
        self.assertFalse(payload["safe_requests_complete"])

    def test_service_failure_produces_sanitized_stderr_json(self) -> None:
        mock_service_class = MagicMock(
            side_effect=ValueError("secret internal lineage detail")
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            environment = {
                "INDIA_SWING_MARKET_DATA_ROOT": str(Path(temp_dir) / "market")
            }
            output = io.StringIO()
            stderr = io.StringIO()
            with (
                patch.dict("os.environ", environment, clear=False),
                patch(
                    "india_swing.market_data.backfill_cli._configured_plan",
                    return_value=object(),
                ),
                patch(
                    "india_swing.market_data.backfill_cli"
                    ".HistoricalDatasetAdmissionService",
                    mock_service_class,
                ),
                redirect_stdout(output),
                redirect_stderr(stderr),
            ):
                exit_code = main(
                    [
                        "dataset-admit",
                        "--identity-registry-id",
                        "a" * 64,
                        "--calendar-materialization-id",
                        "b" * 64,
                        "--upstox-catalog-id",
                        "c" * 64,
                        "--coverage-start",
                        "2026-07-01",
                        "--coverage-end",
                        "2026-07-01",
                        "--requested-at",
                        "2026-07-01T09:00:00+00:00",
                        "--expected-plan-id",
                        "b" * 64,
                        "--expected-progress-id",
                        "c" * 64,
                        "--assessed-at",
                        "2026-07-23T10:00:00+00:00",
                    ]
                )

        self.assertEqual(exit_code, 2)
        self.assertEqual(output.getvalue(), "")
        payload = json.loads(stderr.getvalue())
        self.assertEqual(set(payload), {"status", "error_type"})
        self.assertEqual(payload["status"], "FAILED")
        self.assertEqual(payload["error_type"], "ValueError")
        self.assertNotIn("secret internal lineage detail", stderr.getvalue())


def _reconciliation_index(
    *,
    plan_id: str,
    progress_id: str,
    entry_count: int = 2,
    complete: bool = True,
) -> HistoricalReconciliationIndex:
    entries = tuple(
        HistoricalReconciliationIndexEntry(
            request_id=f"{index * 10 + 1:064x}",
            provider_snapshot_id=f"{index * 10 + 2:064x}",
            historical_batch_id=f"{index * 10 + 3:064x}",
            reconciliation_report_id=f"{index * 10 + 4:064x}",
            reconciliation_snapshot_id=f"{index * 10 + 5:064x}",
            reconciled_at=PILOT_RECONCILED_AT,
            passed=True,
        )
        for index in range(entry_count)
    )
    return HistoricalReconciliationIndex(
        plan_id=plan_id,
        progress_id=progress_id,
        provider=UPSTOX_PROVIDER,
        connector_version="fake-pilot-connector/v1",
        nse_artifact_ids=(f"{99:064x}",),
        prior_index_id=None,
        entries=entries,
        total_completion_count=entry_count if complete else entry_count + 1,
        updated_at=PILOT_RECONCILED_AT,
        complete=complete,
    )


def _dataset_admit_arguments(*extra: str) -> list[str]:
    return [
        "dataset-admit",
        "--identity-registry-id",
        "a" * 64,
        "--calendar-materialization-id",
        "b" * 64,
        "--upstox-catalog-id",
        "c" * 64,
        "--coverage-start",
        "2026-07-01",
        "--coverage-end",
        "2026-07-01",
        "--requested-at",
        "2026-07-01T09:00:00+00:00",
        "--expected-plan-id",
        "b" * 64,
        "--expected-progress-id",
        "c" * 64,
        "--assessed-at",
        "2026-07-23T10:00:00+00:00",
        *extra,
    ]


class DatasetAdmitReconciliationIndexCliTests(unittest.TestCase):
    def _run(self, index, *, coverage_complete=True):
        stub_result = SimpleNamespace(
            report=_stub_admission_report(
                coverage_complete=coverage_complete,
                safe_requests_complete=coverage_complete,
            ),
            disposition_counts=(("ADMITTED", 1),),
        )
        mock_service_instance = MagicMock()
        mock_service_instance.run.return_value = stub_result
        mock_service_class = MagicMock(return_value=mock_service_instance)
        mock_index_store = MagicMock()
        mock_index_store.return_value.get.return_value = index

        with tempfile.TemporaryDirectory() as temp_dir:
            environment = {
                "INDIA_SWING_MARKET_DATA_ROOT": str(Path(temp_dir) / "market")
            }
            output = io.StringIO()
            stderr = io.StringIO()
            with (
                patch.dict("os.environ", environment, clear=False),
                patch(
                    "india_swing.market_data.backfill_cli._configured_plan",
                    return_value=object(),
                ),
                patch(
                    "india_swing.market_data.backfill_cli"
                    ".LocalHistoricalReconciliationIndexStore",
                    mock_index_store,
                ),
                patch(
                    "india_swing.market_data.backfill_cli"
                    ".HistoricalDatasetAdmissionService",
                    mock_service_class,
                ),
                redirect_stdout(output),
                redirect_stderr(stderr),
            ):
                exit_code = main(
                    _dataset_admit_arguments(
                        "--reconciliation-index-id", "1" * 64
                    )
                )
        return exit_code, output, stderr, mock_index_store, mock_service_instance

    def test_parser_accepts_an_index_id_and_rejects_mixing_with_manual_ids(
        self,
    ) -> None:
        args = parser().parse_args(
            _dataset_admit_arguments("--reconciliation-index-id", "1" * 64)
        )
        self.assertEqual(args.reconciliation_index_id, "1" * 64)
        self.assertIsNone(args.reconciliation_snapshot_ids)

        manual = parser().parse_args(
            _dataset_admit_arguments("--reconciliation-snapshot-id", "d" * 64)
        )
        self.assertIsNone(manual.reconciliation_index_id)
        self.assertEqual(manual.reconciliation_snapshot_ids, ["d" * 64])

        neither = parser().parse_args(_dataset_admit_arguments())
        self.assertIsNone(neither.reconciliation_index_id)
        self.assertIsNone(neither.reconciliation_snapshot_ids)

        with self.assertRaises(SystemExit):
            parser().parse_args(
                _dataset_admit_arguments(
                    "--reconciliation-snapshot-id",
                    "d" * 64,
                    "--reconciliation-index-id",
                    "1" * 64,
                )
            )

    def test_exact_index_is_loaded_and_its_ordered_ids_are_threaded(self) -> None:
        index = _reconciliation_index(plan_id="b" * 64, progress_id="c" * 64)
        exit_code, output, _, index_store, service = self._run(index)

        self.assertEqual(exit_code, 0)
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["status"], "DATASET_ADMISSION_COVERAGE_COMPLETE")
        index_store.return_value.get.assert_called_once_with("1" * 64)
        self.assertEqual(
            service.run.call_args.kwargs["reconciliation_snapshot_ids"],
            index.reconciliation_snapshot_ids,
        )

    def test_partial_index_is_accepted_only_as_blocked_admission_evidence(
        self,
    ) -> None:
        index = _reconciliation_index(
            plan_id="b" * 64, progress_id="c" * 64, complete=False
        )
        exit_code, output, _, _, service = self._run(
            index, coverage_complete=False
        )

        self.assertEqual(exit_code, 4)
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["status"], "DATASET_ADMISSION_COVERAGE_INCOMPLETE")
        self.assertFalse(payload["coverage_complete"])
        self.assertFalse(index.complete)
        self.assertEqual(
            service.run.call_args.kwargs["reconciliation_snapshot_ids"],
            index.reconciliation_snapshot_ids,
        )

    def test_index_bound_to_another_plan_or_progress_is_rejected(self) -> None:
        for override in ({"plan_id": "9" * 64}, {"progress_id": "9" * 64}):
            with self.subTest(override=override):
                values = {"plan_id": "b" * 64, "progress_id": "c" * 64}
                values.update(override)
                exit_code, output, stderr, _, service = self._run(
                    _reconciliation_index(**values)
                )

                self.assertEqual(exit_code, 2)
                self.assertEqual(output.getvalue(), "")
                payload = json.loads(stderr.getvalue())
                self.assertEqual(set(payload), {"status", "error_type"})
                self.assertEqual(payload["status"], "FAILED")
                service.run.assert_not_called()

    def test_manual_and_empty_reconciliation_evidence_are_unchanged(self) -> None:
        for extra, expected in (
            (("--reconciliation-snapshot-id", "d" * 64), ("d" * 64,)),
            ((), ()),
        ):
            with self.subTest(extra=extra):
                mock_service_instance = MagicMock()
                mock_service_instance.run.return_value = SimpleNamespace(
                    report=_stub_admission_report(),
                    disposition_counts=(("ADMITTED", 1),),
                )
                mock_index_store = MagicMock(
                    side_effect=AssertionError(
                        "the index store must not be constructed"
                    )
                )
                with tempfile.TemporaryDirectory() as temp_dir:
                    environment = {
                        "INDIA_SWING_MARKET_DATA_ROOT": str(
                            Path(temp_dir) / "market"
                        )
                    }
                    output = io.StringIO()
                    with (
                        patch.dict("os.environ", environment, clear=False),
                        patch(
                            "india_swing.market_data.backfill_cli._configured_plan",
                            return_value=object(),
                        ),
                        patch(
                            "india_swing.market_data.backfill_cli"
                            ".LocalHistoricalReconciliationIndexStore",
                            mock_index_store,
                        ),
                        patch(
                            "india_swing.market_data.backfill_cli"
                            ".HistoricalDatasetAdmissionService",
                            MagicMock(return_value=mock_service_instance),
                        ),
                        redirect_stdout(output),
                    ):
                        exit_code = main(_dataset_admit_arguments(*extra))

                self.assertEqual(exit_code, 0)
                self.assertEqual(
                    mock_service_instance.run.call_args.kwargs[
                        "reconciliation_snapshot_ids"
                    ],
                    expected,
                )


def _stub_corpus_index(**overrides) -> SimpleNamespace:
    values = dict(
        corpus_id="a" * 64,
        admission_report_id="b" * 64,
        reconciliation_index_id="c" * 64,
        plan_id="d" * 64,
        progress_id="e" * 64,
        provider="UPSTOX",
        connector_version="upstox-test-connector/v1",
        assessed_at=datetime(2026, 7, 23, 10, 0, tzinfo=timezone.utc),
        built_at=datetime(2026, 7, 24, 9, 0, tzinfo=timezone.utc),
        partition_ids=("f" * 64,),
        partition_sessions=(date(2026, 7, 14),),
        all_entry_ids=("g" * 64,),
        admitted_entry_ids=("g" * 64,),
        blocked_entry_ids=(),
        disposition_counts=(("ADMITTED", 1),),
        safe_requests_complete=True,
        coverage_complete=True,
        collection_only=True,
        actionable=False,
        training_eligible=False,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


def _corpus_build_arguments(*extra: str) -> list[str]:
    return [
        "corpus-build",
        "--admission-report-id",
        "b" * 64,
        "--reconciliation-index-id",
        "c" * 64,
        "--built-at",
        "2026-07-24T09:00:00+00:00",
        *extra,
    ]


class CorpusBuildCliTests(unittest.TestCase):
    def test_parser_requires_exact_arguments(self) -> None:
        args = parser().parse_args(_corpus_build_arguments())
        self.assertEqual(args.command, "corpus-build")
        self.assertEqual(args.admission_report_id, "b" * 64)
        self.assertEqual(args.reconciliation_index_id, "c" * 64)
        self.assertEqual(
            args.built_at, datetime(2026, 7, 24, 9, 0, tzinfo=timezone.utc)
        )
        self.assertFalse(hasattr(args, "identity_registry_id"))
        self.assertFalse(hasattr(args, "kite_interactive_login"))

        required = _corpus_build_arguments()
        for flag in (
            "--admission-report-id",
            "--reconciliation-index-id",
            "--built-at",
        ):
            index = required.index(flag)
            with self.subTest(missing=flag):
                with self.assertRaises(SystemExit):
                    parser().parse_args(required[:index] + required[index + 2 :])

    def test_coverage_complete_success_threads_arguments_and_exits_zero(self) -> None:
        stub_index = _stub_corpus_index()
        mock_service_instance = MagicMock()
        mock_service_instance.build.return_value = stub_index
        mock_service_class = MagicMock(return_value=mock_service_instance)

        with tempfile.TemporaryDirectory() as temp_dir:
            environment = {
                "INDIA_SWING_MARKET_DATA_ROOT": str(Path(temp_dir) / "market")
            }
            output = io.StringIO()
            with (
                patch.dict("os.environ", environment, clear=False),
                patch(
                    "india_swing.market_data.backfill_cli"
                    ".HistoricalEvaluationCorpusService",
                    mock_service_class,
                ),
                redirect_stdout(output),
            ):
                exit_code = main(_corpus_build_arguments())

        self.assertEqual(exit_code, 0)
        payload = json.loads(output.getvalue())
        self.assertEqual(
            payload["status"], "HISTORICAL_EVALUATION_CORPUS_COVERAGE_COMPLETE"
        )
        self.assertEqual(payload["corpus_id"], stub_index.corpus_id)
        self.assertEqual(payload["admission_report_id"], stub_index.admission_report_id)
        self.assertEqual(
            payload["reconciliation_index_id"], stub_index.reconciliation_index_id
        )
        self.assertEqual(payload["plan_id"], stub_index.plan_id)
        self.assertEqual(payload["progress_id"], stub_index.progress_id)
        self.assertEqual(payload["provider"], stub_index.provider)
        self.assertEqual(payload["session_count"], 1)
        self.assertEqual(payload["first_session"], "2026-07-14")
        self.assertEqual(payload["last_session"], "2026-07-14")
        self.assertEqual(payload["total_entry_count"], 1)
        self.assertEqual(payload["admitted_entry_count"], 1)
        self.assertEqual(payload["blocked_entry_count"], 0)
        self.assertEqual(payload["disposition_counts"], {"ADMITTED": 1})
        self.assertTrue(payload["safe_requests_complete"])
        self.assertTrue(payload["coverage_complete"])
        self.assertTrue(payload["collection_only"])
        self.assertFalse(payload["actionable"])
        self.assertFalse(payload["training_eligible"])
        self.assertNotIn("bars", payload)
        self.assertNotIn("partitions", payload)
        self.assertNotIn("partition_ids", payload)

        mock_service_instance.build.assert_called_once_with(
            admission_report_id="b" * 64,
            reconciliation_index_id="c" * 64,
            built_at=datetime(2026, 7, 24, 9, 0, tzinfo=timezone.utc),
        )

    def test_incomplete_coverage_exits_four(self) -> None:
        stub_index = _stub_corpus_index(
            coverage_complete=False,
            safe_requests_complete=False,
            admitted_entry_ids=(),
            blocked_entry_ids=("g" * 64,),
            disposition_counts=(("MISSING_COMPLETION", 1),),
        )
        mock_service_instance = MagicMock()
        mock_service_instance.build.return_value = stub_index

        with tempfile.TemporaryDirectory() as temp_dir:
            environment = {
                "INDIA_SWING_MARKET_DATA_ROOT": str(Path(temp_dir) / "market")
            }
            output = io.StringIO()
            with (
                patch.dict("os.environ", environment, clear=False),
                patch(
                    "india_swing.market_data.backfill_cli"
                    ".HistoricalEvaluationCorpusService",
                    MagicMock(return_value=mock_service_instance),
                ),
                redirect_stdout(output),
            ):
                exit_code = main(_corpus_build_arguments())

        self.assertEqual(exit_code, 4)
        payload = json.loads(output.getvalue())
        self.assertEqual(
            payload["status"], "HISTORICAL_EVALUATION_CORPUS_COVERAGE_INCOMPLETE"
        )
        self.assertFalse(payload["coverage_complete"])
        self.assertFalse(payload["safe_requests_complete"])
        self.assertEqual(payload["blocked_entry_count"], 1)

    def test_service_failure_produces_sanitized_stderr_json(self) -> None:
        mock_service_class = MagicMock(
            side_effect=ValueError("secret internal lineage detail")
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            environment = {
                "INDIA_SWING_MARKET_DATA_ROOT": str(Path(temp_dir) / "market")
            }
            output = io.StringIO()
            stderr = io.StringIO()
            with (
                patch.dict("os.environ", environment, clear=False),
                patch(
                    "india_swing.market_data.backfill_cli"
                    ".HistoricalEvaluationCorpusService",
                    mock_service_class,
                ),
                redirect_stdout(output),
                redirect_stderr(stderr),
            ):
                exit_code = main(_corpus_build_arguments())

        self.assertEqual(exit_code, 2)
        self.assertEqual(output.getvalue(), "")
        payload = json.loads(stderr.getvalue())
        self.assertEqual(set(payload), {"status", "error_type"})
        self.assertEqual(payload["status"], "FAILED")
        self.assertEqual(payload["error_type"], "ValueError")
        self.assertNotIn("secret internal lineage detail", stderr.getvalue())

    def test_malformed_id_and_datetime_arguments_are_rejected(self) -> None:
        with self.assertRaises(SystemExit):
            parser().parse_args(
                _corpus_build_arguments()[:5] + ["--built-at", "not-a-datetime"]
            )

    def test_no_login_provider_or_network_route_is_reachable(self) -> None:
        stub_index = _stub_corpus_index()
        mock_service_instance = MagicMock()
        mock_service_instance.build.return_value = stub_index

        with tempfile.TemporaryDirectory() as temp_dir:
            environment = {
                "INDIA_SWING_MARKET_DATA_ROOT": str(Path(temp_dir) / "market")
            }
            output = io.StringIO()
            with (
                patch.dict("os.environ", environment, clear=False),
                patch(
                    "india_swing.market_data.backfill_cli"
                    ".HistoricalEvaluationCorpusService",
                    MagicMock(return_value=mock_service_instance),
                ),
                patch(
                    "india_swing.market_data.backfill_cli.UpstoxCredentials.from_env",
                    side_effect=AssertionError("credentials must not be read"),
                ),
                patch(
                    "india_swing.market_data.backfill_cli.KiteCredentials.from_env",
                    side_effect=AssertionError("credentials must not be read"),
                ),
                patch(
                    "india_swing.market_data.backfill_cli"
                    ".UpstoxHistoricalDataAdapter",
                    side_effect=AssertionError("provider must not be constructed"),
                ),
                patch(
                    "india_swing.market_data.backfill_cli.KiteMarketDataAdapter"
                    ".from_official_sdk",
                    side_effect=AssertionError("provider must not be constructed"),
                ),
                redirect_stdout(output),
            ):
                exit_code = main(_corpus_build_arguments())

        self.assertEqual(exit_code, 0)


class CorpusShowCliTests(unittest.TestCase):
    def test_parser_requires_corpus_id(self) -> None:
        args = parser().parse_args(["corpus-show", "--corpus-id", "a" * 64])
        self.assertEqual(args.command, "corpus-show")
        self.assertEqual(args.corpus_id, "a" * 64)
        with self.assertRaises(SystemExit):
            parser().parse_args(["corpus-show"])

    def test_shows_sanitized_summary(self) -> None:
        stub_index = _stub_corpus_index()
        mock_store = MagicMock()
        mock_store.return_value.get.return_value = (stub_index, ("unused-partitions",))

        with tempfile.TemporaryDirectory() as temp_dir:
            environment = {
                "INDIA_SWING_MARKET_DATA_ROOT": str(Path(temp_dir) / "market")
            }
            output = io.StringIO()
            with (
                patch.dict("os.environ", environment, clear=False),
                patch(
                    "india_swing.market_data.backfill_cli"
                    ".LocalHistoricalEvaluationCorpusStore",
                    mock_store,
                ),
                redirect_stdout(output),
            ):
                exit_code = main(["corpus-show", "--corpus-id", stub_index.corpus_id])

        self.assertEqual(exit_code, 0)
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["corpus_id"], stub_index.corpus_id)
        self.assertEqual(payload["session_count"], 1)
        self.assertNotIn("bars", payload)
        self.assertNotIn("partitions", payload)
        mock_store.return_value.get.assert_called_once_with(stub_index.corpus_id)

    def test_not_found_produces_sanitized_stderr_json(self) -> None:
        mock_store = MagicMock()
        mock_store.return_value.get.side_effect = ValueError(
            "no such corpus in var/historical-evaluation-corpora"
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            environment = {
                "INDIA_SWING_MARKET_DATA_ROOT": str(Path(temp_dir) / "market")
            }
            output = io.StringIO()
            stderr = io.StringIO()
            with (
                patch.dict("os.environ", environment, clear=False),
                patch(
                    "india_swing.market_data.backfill_cli"
                    ".LocalHistoricalEvaluationCorpusStore",
                    mock_store,
                ),
                redirect_stdout(output),
                redirect_stderr(stderr),
            ):
                exit_code = main(["corpus-show", "--corpus-id", "a" * 64])

        self.assertEqual(exit_code, 2)
        self.assertEqual(output.getvalue(), "")
        payload = json.loads(stderr.getvalue())
        self.assertEqual(set(payload), {"status", "error_type"})
        self.assertEqual(payload["status"], "FAILED")
        self.assertNotIn("var/", stderr.getvalue())


class CorpusBuildAndShowEndToEndCliTests(unittest.TestCase):
    def test_real_fixture_builds_and_shows_through_the_cli(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            fixture = build_two_symbol_fixture(root)
            environment = {"INDIA_SWING_MARKET_DATA_ROOT": str(root / "market")}

            build_output = io.StringIO()
            with (
                patch.dict("os.environ", environment, clear=False),
                redirect_stdout(build_output),
            ):
                build_exit = main(
                    [
                        "corpus-build",
                        "--admission-report-id",
                        fixture["admission_report"].report_id,
                        "--reconciliation-index-id",
                        fixture["reconciliation_index"].index_id,
                        "--built-at",
                        CORPUS_BUILT_AT.isoformat(),
                    ]
                )
            build_payload = json.loads(build_output.getvalue())

            show_output = io.StringIO()
            with (
                patch.dict("os.environ", environment, clear=False),
                redirect_stdout(show_output),
            ):
                show_exit = main(
                    ["corpus-show", "--corpus-id", build_payload["corpus_id"]]
                )
            show_payload = json.loads(show_output.getvalue())

        self.assertEqual(build_exit, 0)
        self.assertEqual(show_exit, 0)
        self.assertEqual(
            build_payload["status"], "HISTORICAL_EVALUATION_CORPUS_COVERAGE_COMPLETE"
        )
        self.assertEqual(build_payload, show_payload)
        self.assertEqual(build_payload["session_count"], 2)
        self.assertEqual(build_payload["admitted_entry_count"], 2)
        self.assertEqual(build_payload["blocked_entry_count"], 0)
        self.assertEqual(build_payload["disposition_counts"], {"ADMITTED": 2})


if __name__ == "__main__":
    unittest.main()
