from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

from india_swing.identity_registry import (
    CrossVintageIdentityRegistry,
    materialize_cross_vintage_identity_registry,
)
from india_swing.market_data.backfill import (
    HistoricalBackfillError,
    HistoricalBackfillIssueCode,
    HistoricalBackfillRunner,
    HistoricalBackfillStateError,
    LocalHistoricalBackfillProgressStore,
    UpstoxIsinInstrumentResolver,
    build_historical_backfill_plan,
)
from india_swing.market_data.collection import HistoricalMarketDataCollector
from india_swing.market_data.snapshot_store import LocalMarketSnapshotStore
from india_swing.reference.calendar import (
    CalendarDay,
    CalendarDayKind,
    CalendarSnapshot,
    SessionWindow,
    SessionWindowPhase,
)
from india_swing.reference.models import ExternalRecordRef, ReferenceReadiness
from india_swing.reference_data.artifact_store import LocalReferenceArtifactStore
from tests.test_identity_registry import (
    CUTOFF,
    DAY_ONE_FIRST_SEEN,
    DAY_ONE_VALIDATED,
    DAY_TWO_FIRST_SEEN,
    DAY_TWO_VALIDATED,
    clock_sequence,
    master_bytes,
    security_row,
    tcs_row,
)
from tests.test_upstox_market_data import (
    FakeTransport,
    adapter as upstox_adapter,
    candle_row,
    response,
    success_body,
)


UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
DAY_ZERO = date(2026, 7, 14)
DAY_ONE = date(2026, 7, 15)
DAY_TWO = date(2026, 7, 16)
REQUESTED_AT = datetime(2026, 7, 17, 10, 0, tzinfo=UTC)
RUN_CLOCK = datetime(2026, 7, 22, 10, 0, tzinfo=UTC)
CALENDAR_SOURCE_ID = "c" * 64


def registry(
    root: Path,
    first_rows: list[list[str]],
    second_rows: list[list[str]],
) -> CrossVintageIdentityRegistry:
    root.mkdir(parents=True, exist_ok=True)
    first_file = root / "NSE_CM_security_15072026.csv.gz"
    second_file = root / "NSE_CM_security_16072026.csv.gz"
    first_file.write_bytes(master_bytes(first_rows))
    second_file.write_bytes(master_bytes(second_rows))
    store = LocalReferenceArtifactStore(
        root / "reference",
        clock=clock_sequence(
            DAY_ONE_FIRST_SEEN,
            DAY_ONE_VALIDATED,
            DAY_TWO_FIRST_SEEN,
            DAY_TWO_VALIDATED,
        ),
    )
    sources = (
        store.import_security_master(first_file),
        store.import_security_master(second_file),
    )
    return materialize_cross_vintage_identity_registry(
        sources=sources,
        cutoff=CUTOFF,
    )


def calendar(
    coverage_start: date = DAY_ONE,
    coverage_end: date = DAY_TWO,
    *,
    cutoff: datetime = CUTOFF,
) -> CalendarSnapshot:
    days: list[CalendarDay] = []
    current = coverage_start
    while current <= coverage_end:
        reference = ExternalRecordRef(
            event_time=datetime.combine(current, time.min, tzinfo=IST),
            knowledge_time=min(
                cutoff,
                datetime.combine(current, time.min, tzinfo=IST).astimezone(UTC),
            ),
            source="NSE_TEST_CALENDAR",
            content_hash="d" * 64,
            source_snapshot_id=CALENDAR_SOURCE_ID,
        )
        days.append(
            CalendarDay(
                day=current,
                kind=CalendarDayKind.REGULAR,
                reference=reference,
                session_windows=(
                    SessionWindow(
                        opens_at=datetime.combine(
                            current,
                            time(9, 15),
                            tzinfo=IST,
                        ),
                        closes_at=datetime.combine(
                            current,
                            time(15, 30),
                            tzinfo=IST,
                        ),
                        phase=SessionWindowPhase.LIVE_CONTINUOUS,
                    ),
                ),
            )
        )
        current += timedelta(days=1)
    return CalendarSnapshot.create(
        exchange="NSE",
        segment="CM",
        cutoff=cutoff,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        days=tuple(days),
        source_snapshot_ids=(CALENDAR_SOURCE_ID,),
        readiness=ReferenceReadiness.COLLECTION_ONLY,
    )


def plan(
    root: Path,
    *,
    first_rows: list[list[str]] | None = None,
    second_rows: list[list[str]] | None = None,
    selected_calendar: CalendarSnapshot | None = None,
    coverage_start: date = DAY_ONE,
    coverage_end: date = DAY_TWO,
    resolver=None,
):
    return build_historical_backfill_plan(
        registry=registry(
            root,
            first_rows or [security_row(), tcs_row()],
            second_rows or [security_row(), tcs_row()],
        ),
        calendar=selected_calendar or calendar(coverage_start, coverage_end),
        resolver=resolver or UpstoxIsinInstrumentResolver(),
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        requested_at=REQUESTED_AT,
    )


def two_session_body() -> bytes:
    return success_body([candle_row(DAY_TWO), candle_row(DAY_ONE)])


class HistoricalBackfillPlanningTests(unittest.TestCase):
    def test_exact_positive_vintages_form_provider_neutral_requests(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            value = plan(Path(temp_dir))

        self.assertEqual(value.provider, "UPSTOX")
        self.assertEqual(value.safe_request_count, 2)
        self.assertEqual(value.safe_session_count, 4)
        self.assertEqual(value.issues, ())
        self.assertTrue(value.collection_only)
        self.assertEqual(
            {request.binding.listing_key for request in value.requests},
            {"NSE:INFY", "NSE:TCS"},
        )
        for request in value.requests:
            self.assertEqual(request.sessions, (DAY_ONE, DAY_TWO))
            self.assertEqual(request.binding.security_series, "EQ")
            self.assertEqual(
                request.binding.provider_instrument_id,
                f"NSE_EQ|{request.binding.isin}",
            )
            self.assertIn(value.identity_registry_id, request.binding.source_snapshot_ids)
            self.assertIn(value.calendar_snapshot_id, request.binding.source_snapshot_ids)
        value.verify_content_identity()

    def test_missing_master_dates_are_explicit_not_interpolated(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            value = plan(
                Path(temp_dir),
                selected_calendar=calendar(DAY_ZERO, DAY_TWO),
                coverage_start=DAY_ZERO,
            )

        gaps = [
            issue
            for issue in value.issues
            if issue.code
            is HistoricalBackfillIssueCode.MISSING_SECURITY_MASTER_VINTAGE
        ]
        self.assertEqual(len(gaps), 1)
        self.assertEqual(gaps[0].affected_dates, (DAY_ZERO,))
        self.assertTrue(value.has_coverage_issues)
        self.assertTrue(
            all(DAY_ZERO not in request.sessions for request in value.requests)
        )

    def test_concurrent_series_with_one_provider_key_is_not_silently_collapsed(
        self,
    ) -> None:
        rows = [
            security_row(),
            security_row(SctySrs="BE", FinInstrmId="1595"),
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            value = plan(
                Path(temp_dir),
                first_rows=rows,
                second_rows=rows,
            )

        self.assertEqual(value.requests, ())
        self.assertEqual(
            {issue.code for issue in value.issues},
            {HistoricalBackfillIssueCode.AMBIGUOUS_PROVIDER_KEY},
        )
        self.assertEqual(
            {issue.affected_dates for issue in value.issues},
            {(DAY_ONE,), (DAY_TWO,)},
        )

    def test_custom_resolver_can_add_a_provider_without_changing_models(self) -> None:
        class CustomResolver:
            provider = "CUSTOM_DATA"
            resolver_version = "custom-test/v1"

            @staticmethod
            def resolve(observation):
                return f"CUSTOM|{observation.validated_isin}"

        with tempfile.TemporaryDirectory() as temp_dir:
            value = plan(Path(temp_dir), resolver=CustomResolver())

        self.assertEqual(value.provider, "CUSTOM_DATA")
        self.assertTrue(
            all(
                request.binding.provider == "CUSTOM_DATA"
                and request.binding.provider_instrument_id.startswith("CUSTOM|")
                for request in value.requests
            )
        )

    def test_future_knowledge_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            identity = registry(root, [security_row()], [security_row()])
            with self.assertRaisesRegex(HistoricalBackfillError, "not known"):
                build_historical_backfill_plan(
                    registry=identity,
                    calendar=calendar(),
                    resolver=UpstoxIsinInstrumentResolver(),
                    coverage_start=DAY_ONE,
                    coverage_end=DAY_TWO,
                    requested_at=CUTOFF - timedelta(seconds=1),
                )


class HistoricalBackfillRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.plan = plan(self.root / "inputs")
        self.snapshot_store = LocalMarketSnapshotStore(self.root / "snapshots")
        self.progress_store = LocalHistoricalBackfillProgressStore(
            self.root / "progress"
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def runner(self, transport: FakeTransport) -> HistoricalBackfillRunner:
        return HistoricalBackfillRunner(
            upstox_adapter(transport),
            self.snapshot_store,
            self.progress_store,
            clock=lambda: RUN_CLOCK,
        )

    def test_bounded_run_resumes_without_repeating_completed_requests(self) -> None:
        transport = FakeTransport(
            response(two_session_body()),
            response(two_session_body()),
        )
        first_runner = self.runner(transport)

        first = first_runner.run(self.plan, maximum_requests=1)
        second = self.runner(transport).run(self.plan)

        self.assertEqual(len(first.completions), 1)
        self.assertEqual(len(second.completions), 2)
        self.assertEqual(len(transport.calls), 2)
        self.assertTrue(HistoricalBackfillRunner.is_complete(self.plan, second))
        self.assertEqual(self.progress_store.load(self.plan.plan_id), second)

    def test_snapshot_written_before_checkpoint_is_recovered_without_refetch(
        self,
    ) -> None:
        transport = FakeTransport(
            response(two_session_body()),
            response(two_session_body()),
        )
        connector = upstox_adapter(transport)
        existing = HistoricalMarketDataCollector(
            connector,
            self.snapshot_store,
        ).collect(self.plan.requests[0])

        progress = HistoricalBackfillRunner(
            connector,
            self.snapshot_store,
            self.progress_store,
            clock=lambda: RUN_CLOCK,
        ).run(self.plan)

        self.assertEqual(len(transport.calls), 2)
        recovered = next(
            value
            for value in progress.completions
            if value.request_id == self.plan.requests[0].request_id
        )
        self.assertTrue(recovered.recovered_existing)
        self.assertEqual(recovered.snapshot_id, existing.manifest.snapshot_id)

    def test_tampered_progress_fails_before_another_provider_call(self) -> None:
        transport = FakeTransport(
            response(two_session_body()),
            response(two_session_body()),
        )
        runner = self.runner(transport)
        runner.run(self.plan, maximum_requests=1)
        state_path = self.progress_store.path_for(self.plan.plan_id)
        value = json.loads(state_path.read_text(encoding="utf-8"))
        value["provider"] = "FORGED"
        state_path.write_text(json.dumps(value), encoding="utf-8")

        with self.assertRaises(HistoricalBackfillStateError):
            runner.run(self.plan)

        self.assertEqual(len(transport.calls), 1)

    def test_missing_completed_snapshot_fails_before_another_provider_call(
        self,
    ) -> None:
        transport = FakeTransport(
            response(two_session_body()),
            response(two_session_body()),
        )
        runner = self.runner(transport)
        progress = runner.run(self.plan, maximum_requests=1)
        completed = progress.completions[0]
        stored = self.snapshot_store.get(
            "historical-daily-upstox-nse",
            completed.snapshot_id,
        )
        shutil.rmtree(stored.path)

        with self.assertRaisesRegex(
            HistoricalBackfillStateError,
            "unavailable",
        ):
            runner.run(self.plan)

        self.assertEqual(len(transport.calls), 1)

    def test_connector_provider_mismatch_fails_before_state_or_network(self) -> None:
        class WrongConnector:
            provider = "ZERODHA_KITE"
            provider_version = "wrong/v1"

            def __init__(self) -> None:
                self.calls = 0

            def fetch_historical_daily(self, request):
                self.calls += 1
                raise AssertionError("must not be called")

        connector = WrongConnector()
        runner = HistoricalBackfillRunner(
            connector,
            self.snapshot_store,
            self.progress_store,
            clock=lambda: RUN_CLOCK,
        )

        with self.assertRaises(HistoricalBackfillError):
            runner.run(self.plan)

        self.assertEqual(connector.calls, 0)
        self.assertIsNone(self.progress_store.load(self.plan.plan_id))


if __name__ == "__main__":
    unittest.main()
