from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

from india_swing.identity import content_id
from india_swing.identity_decisions import (
    STABLE_INSTRUMENT_ID_SCHEME,
    STABLE_LISTING_ID_SCHEME,
    AdjudicatedIdentitySnapshot,
    CandidateIdentityResolution,
    EffectiveStableListingObservation,
)
from india_swing.identity_registry import (
    CrossVintageIdentityRegistry,
    build_identity_adjudication_queue,
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


def security_master_sources(
    root: Path,
    identity: CrossVintageIdentityRegistry,
):
    store = LocalReferenceArtifactStore(root / "reference")
    return tuple(
        store.get(value) for value in identity.source_artifact_ids
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
    identity = registry(
        root,
        first_rows or [security_row(), tcs_row()],
        second_rows or [security_row(), tcs_row()],
    )
    return build_historical_backfill_plan(
        registry=identity,
        security_master_sources=security_master_sources(root, identity),
        calendar=selected_calendar or calendar(coverage_start, coverage_end),
        resolver=resolver or UpstoxIsinInstrumentResolver(),
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        requested_at=REQUESTED_AT,
    )


def two_session_body() -> bytes:
    return success_body([candle_row(DAY_TWO), candle_row(DAY_ONE)])


def reviewed_snapshot(
    value: CrossVintageIdentityRegistry,
    *,
    corrected_isin: str = "INE009A01021",
    cutoff: datetime = CUTOFF,
) -> AdjudicatedIdentitySnapshot:
    queue = build_identity_adjudication_queue(value)
    observations = {
        item.observation_id: item for item in value.observations
    }
    resolutions = []
    listings = []
    for case in queue.cases:
        stable_instrument_id = content_id(
            {
                "scheme": STABLE_INSTRUMENT_ID_SCHEME,
                "exchange": "NSE",
                "segment": "CM",
                "validated_isin": corrected_isin,
            },
            length=64,
        )
        accepted = tuple(
            sorted(
                content_id(
                    {
                        "test": "accepted-review",
                        "candidate_id": case.candidate_id,
                        "requirement": requirement.value,
                    },
                    length=64,
                )
                for requirement in case.requirements
            )
        )
        resolutions.append(
            CandidateIdentityResolution(
                candidate_id=case.candidate_id,
                required_requirements=case.requirements,
                accepted_decision_ids=accepted,
                rejected_decision_ids=(),
                missing_requirements=(),
                blocker_codes=(),
                stable_instrument_id=stable_instrument_id,
            )
        )
        for observation_id in case.observation_ids:
            observation = observations[observation_id]
            stable_listing_id = content_id(
                {
                    "scheme": STABLE_LISTING_ID_SCHEME,
                    "stable_instrument_id": stable_instrument_id,
                    "exchange": "NSE",
                    "segment": "CM",
                    "series": observation.security_series,
                },
                length=64,
            )
            listings.append(
                EffectiveStableListingObservation(
                    candidate_id=case.candidate_id,
                    source_observation_id=observation.observation_id,
                    stable_instrument_id=stable_instrument_id,
                    stable_listing_id=stable_listing_id,
                    effective_on=observation.claimed_report_date,
                    symbol=observation.ticker_symbol,
                    series=observation.security_series,
                    isin=corrected_isin,
                )
            )
    return AdjudicatedIdentitySnapshot(
        source_registry_id=value.registry_id,
        source_queue_id=queue.queue_id,
        cutoff=cutoff,
        knowledge_time=cutoff,
        evidence_artifact_ids=("e" * 64,),
        review_bundle_ids=("f" * 64,),
        resolutions=tuple(
            sorted(resolutions, key=lambda item: item.candidate_id)
        ),
        listing_observations=tuple(
            sorted(
                listings,
                key=lambda item: (
                    item.effective_on,
                    item.stable_listing_id,
                    item.source_observation_id,
                ),
            )
        ),
    )


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
            security_row(SctySrs="SM", FinInstrmId="1595"),
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

    def test_deleted_legacy_alias_does_not_block_active_same_isin(self) -> None:
        rows = [
            security_row(
                TckrSymb="OLDINFY",
                FinInstrmId="2000",
                DelFlg="Y",
                SctyStsNrmlMkt="3",
                ElgbltyNrmlMkt="0",
            ),
            security_row(),
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            value = plan(
                Path(temp_dir),
                first_rows=rows,
                second_rows=rows,
            )

        self.assertEqual(value.safe_request_count, 1)
        self.assertEqual(value.safe_session_count, 2)
        self.assertEqual(value.requests[0].binding.listing_key, "NSE:INFY")
        self.assertEqual(
            {issue.code for issue in value.issues},
            {HistoricalBackfillIssueCode.DELETED_SECURITY},
        )
        self.assertFalse(value.has_blocking_issues)

    def test_normal_market_ineligible_lane_is_explicitly_excluded(self) -> None:
        suspended = security_row(
            SctyStsNrmlMkt="1",
            ElgbltyNrmlMkt="0",
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            value = plan(
                Path(temp_dir),
                first_rows=[suspended],
                second_rows=[suspended],
            )

        self.assertEqual(value.requests, ())
        self.assertEqual(
            {issue.code for issue in value.issues},
            {HistoricalBackfillIssueCode.INELIGIBLE_NORMAL_MARKET},
        )
        self.assertEqual(value.exclusion_issue_count, 2)
        self.assertFalse(value.has_blocking_issues)

    def test_migrated_sme_lane_selects_only_normal_market_eligible_series(
        self,
    ) -> None:
        rows = [
            security_row(),
            security_row(
                SctySrs="SM",
                FinInstrmId="1595",
                SctyStsNrmlMkt="1",
                ElgbltyNrmlMkt="0",
            ),
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            value = plan(
                Path(temp_dir),
                first_rows=rows,
                second_rows=rows,
            )

        self.assertEqual(value.safe_request_count, 1)
        self.assertEqual(value.requests[0].binding.security_series, "EQ")
        self.assertEqual(
            {issue.code for issue in value.issues},
            {HistoricalBackfillIssueCode.INELIGIBLE_NORMAL_MARKET},
        )
        self.assertFalse(value.has_blocking_issues)

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

    def test_valid_non_equity_isin_is_reported_not_sent_to_equity_provider(self) -> None:
        non_equity = security_row(
            TckrSymb="FUSIONPP",
            SctySrs="E1",
            ISIN="IN9139R01028",
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            value = plan(
                Path(temp_dir),
                first_rows=[non_equity],
                second_rows=[non_equity],
            )

        self.assertEqual(value.requests, ())
        self.assertEqual(
            {issue.code for issue in value.issues},
            {HistoricalBackfillIssueCode.UNSUPPORTED_LISTING_LANE},
        )
        self.assertEqual(
            {issue.affected_dates for issue in value.issues},
            {(DAY_ONE,), (DAY_TWO,)},
        )

    def test_future_knowledge_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            identity = registry(root, [security_row()], [security_row()])
            with self.assertRaisesRegex(HistoricalBackfillError, "not known"):
                build_historical_backfill_plan(
                    registry=identity,
                    security_master_sources=security_master_sources(
                        root, identity
                    ),
                    calendar=calendar(),
                    resolver=UpstoxIsinInstrumentResolver(),
                    coverage_start=DAY_ONE,
                    coverage_end=DAY_TWO,
                    requested_at=CUTOFF - timedelta(seconds=1),
                )

    def test_security_master_source_lineage_must_exactly_match_registry(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            identity = registry(root, [security_row()], [security_row()])
            sources = security_master_sources(root, identity)
            with self.assertRaisesRegex(
                HistoricalBackfillError,
                "source lineage",
            ):
                build_historical_backfill_plan(
                    registry=identity,
                    security_master_sources=tuple(reversed(sources)),
                    calendar=calendar(),
                    resolver=UpstoxIsinInstrumentResolver(),
                    coverage_start=DAY_ONE,
                    coverage_end=DAY_TWO,
                    requested_at=REQUESTED_AT,
                )

    def test_reviewed_identifier_correction_can_enter_a_bound_plan(self) -> None:
        dummy = security_row(ISIN="DUMMY1594")
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            identity = registry(root, [dummy], [dummy])
            snapshot = reviewed_snapshot(identity)
            value = build_historical_backfill_plan(
                registry=identity,
                security_master_sources=security_master_sources(
                    root, identity
                ),
                calendar=calendar(),
                resolver=UpstoxIsinInstrumentResolver(),
                coverage_start=DAY_ONE,
                coverage_end=DAY_TWO,
                requested_at=REQUESTED_AT,
                identity_snapshot=snapshot,
            )

        self.assertEqual(value.identity_snapshot_id, snapshot.snapshot_id)
        self.assertEqual(value.safe_session_count, 2)
        self.assertFalse(
            any(
                issue.code
                is HistoricalBackfillIssueCode.UNVALIDATED_IDENTIFIER
                for issue in value.issues
            )
        )
        self.assertEqual(
            {request.binding.isin for request in value.requests},
            {"INE009A01021"},
        )
        self.assertTrue(
            all(
                snapshot.snapshot_id
                in request.binding.source_snapshot_ids
                for request in value.requests
            )
        )

    def test_corrected_identity_requires_snapshot_known_by_requested_at(
        self,
    ) -> None:
        dummy = security_row(ISIN="DUMMY1594")
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            identity = registry(root, [dummy], [dummy])
            snapshot = reviewed_snapshot(
                identity,
                cutoff=REQUESTED_AT + timedelta(seconds=1),
            )
            with self.assertRaisesRegex(
                HistoricalBackfillError,
                "incompatible",
            ):
                build_historical_backfill_plan(
                    registry=identity,
                    security_master_sources=security_master_sources(
                        root, identity
                    ),
                    calendar=calendar(),
                    resolver=UpstoxIsinInstrumentResolver(),
                    coverage_start=DAY_ONE,
                    coverage_end=DAY_TWO,
                    requested_at=REQUESTED_AT,
                    identity_snapshot=snapshot,
                )

    def test_corrected_identity_requires_provider_isin_capability(self) -> None:
        class ObservationOnlyResolver:
            provider = "CUSTOM_DATA"
            resolver_version = "observation-only/v1"

            @staticmethod
            def resolve(observation):
                return f"CUSTOM|{observation.validated_isin}"

        dummy = security_row(ISIN="DUMMY1594")
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            identity = registry(root, [dummy], [dummy])
            value = build_historical_backfill_plan(
                registry=identity,
                security_master_sources=security_master_sources(
                    root, identity
                ),
                calendar=calendar(),
                resolver=ObservationOnlyResolver(),
                coverage_start=DAY_ONE,
                coverage_end=DAY_TWO,
                requested_at=REQUESTED_AT,
                identity_snapshot=reviewed_snapshot(identity),
            )

        self.assertEqual(value.requests, ())
        self.assertEqual(
            {issue.code for issue in value.issues},
            {HistoricalBackfillIssueCode.PROVIDER_KEY_UNAVAILABLE},
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
