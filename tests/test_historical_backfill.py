from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
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
    HistoricalBackfillIntegrityError,
    HistoricalBackfillIssueCode,
    HistoricalBackfillPlan,
    HistoricalBackfillRunner,
    HistoricalBackfillStateError,
    LocalHistoricalBackfillProgressStore,
    UpstoxIsinInstrumentResolver,
    build_historical_backfill_plan,
)
from india_swing.market_data.backfill_gaps import (
    HistoricalBackfillGapClassification,
    HistoricalBackfillSessionGapEvidence,
    LocalHistoricalBackfillSessionGapStore,
)
from india_swing.market_data.collection import HistoricalMarketDataCollector
from india_swing.market_data.models import (
    HistoricalDailyCandle,
    HistoricalDailyCandleBatch,
    HistoricalDailyRequest,
    HistoricalInstrumentBinding,
    HistoricalResponsePage,
)
from india_swing.market_data.provider import (
    HistoricalEmptyProviderResponseError,
    HistoricalProviderRequestRejectedError,
)
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


class FakeGapConnector:
    provider = "UPSTOX"
    provider_version = "fake-gap-connector/v1"

    def __init__(self, outcomes: dict | None = None) -> None:
        self.outcomes = outcomes or {}
        self.calls: list[str] = []

    def fetch_historical_daily(self, request) -> HistoricalDailyCandleBatch:
        self.calls.append(request.request_id)
        outcome = self.outcomes.get(request.request_id)
        if isinstance(outcome, Exception):
            raise outcome
        if outcome == "raise":
            raise HistoricalEmptyProviderResponseError(
                provider=self.provider,
                provider_version=self.provider_version,
                provider_instrument_id=request.binding.provider_instrument_id,
                session=request.sessions[-1],
                observed_at=request.requested_at,
                normalized_response_sha256="c" * 64,
            )
        if outcome == "reject":
            raise HistoricalProviderRequestRejectedError(
                provider=self.provider,
                provider_version=self.provider_version,
                provider_instrument_id=request.binding.provider_instrument_id,
                session=request.sessions[-1],
                observed_at=request.requested_at,
                upstream_error_type="InputException",
                normalized_response_sha256="d" * 64,
            )
        candles = tuple(
            HistoricalDailyCandle(
                session=session,
                open=Decimal("100.00"),
                high=Decimal("101.00"),
                low=Decimal("99.00"),
                close=Decimal("100.50"),
                volume=1000,
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
            observed_at=request.requested_at,
            provider_version=self.provider_version,
            candles=candles,
            response_pages=(page,),
        )


class HistoricalBackfillRunnerQuarantineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.plan = plan(self.root / "inputs")
        self.snapshot_store = LocalMarketSnapshotStore(self.root / "snapshots")
        self.progress_store = LocalHistoricalBackfillProgressStore(
            self.root / "progress"
        )
        self.gapped_request = self.plan.requests[0]
        self.safe_request = self.plan.requests[1]

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _gap_evidence(self, request, **overrides):
        values = dict(
            plan_id=self.plan.plan_id,
            request_id=request.request_id,
            provider=request.binding.provider,
            provider_version="fake-gap-connector/v1",
            provider_instrument_id=request.binding.provider_instrument_id,
            listing_key=request.binding.listing_key,
            security_series=request.binding.security_series,
            isin=request.binding.isin,
            session=request.sessions[-1],
            response_observed_at=request.requested_at,
            normalized_response_sha256="c" * 64,
        )
        values.update(overrides)
        return HistoricalBackfillSessionGapEvidence(**values)

    def test_default_behavior_aborts_with_no_gap_or_completion(self) -> None:
        connector = FakeGapConnector({self.gapped_request.request_id: "raise"})
        runner = HistoricalBackfillRunner(
            connector,
            self.snapshot_store,
            self.progress_store,
            clock=lambda: RUN_CLOCK,
        )

        with self.assertRaises(HistoricalEmptyProviderResponseError):
            runner.run(self.plan)

        progress = self.progress_store.load(self.plan.plan_id)
        self.assertEqual(progress.completions, ())

    def test_quarantine_flag_requires_an_injected_gap_store(self) -> None:
        connector = FakeGapConnector()
        runner = HistoricalBackfillRunner(
            connector,
            self.snapshot_store,
            self.progress_store,
            clock=lambda: RUN_CLOCK,
        )

        with self.assertRaises(ValueError):
            runner.run(self.plan, quarantine_empty_responses=True)

    def test_one_empty_response_persists_one_gap_and_collection_continues(
        self,
    ) -> None:
        connector = FakeGapConnector({self.gapped_request.request_id: "raise"})
        gap_store = LocalHistoricalBackfillSessionGapStore(self.root / "gaps")
        runner = HistoricalBackfillRunner(
            connector,
            self.snapshot_store,
            self.progress_store,
            gap_store=gap_store,
            clock=lambda: RUN_CLOCK,
        )

        progress = runner.run(self.plan, quarantine_empty_responses=True)

        self.assertEqual(
            {value.request_id for value in progress.completions},
            {self.safe_request.request_id},
        )
        gaps = gap_store.load_unresolved(self.plan.plan_id)
        self.assertEqual(len(gaps), 1)
        self.assertEqual(gaps[0].request_id, self.gapped_request.request_id)
        self.assertGreater(len(self.gapped_request.sessions), 1)
        self.assertEqual(gaps[0].session, self.gapped_request.sessions[-1])
        self.assertFalse(HistoricalBackfillRunner.is_complete(self.plan, progress))

    def test_rerun_skips_the_quarantined_request_and_reaches_later_work(
        self,
    ) -> None:
        connector = FakeGapConnector({self.gapped_request.request_id: "raise"})
        gap_store = LocalHistoricalBackfillSessionGapStore(self.root / "gaps")
        first_runner = HistoricalBackfillRunner(
            connector,
            self.snapshot_store,
            self.progress_store,
            gap_store=gap_store,
            clock=lambda: RUN_CLOCK,
        )

        first_runner.run(self.plan, maximum_requests=1, quarantine_empty_responses=True)

        self.assertEqual(connector.calls.count(self.gapped_request.request_id), 1)
        self.assertEqual(len(gap_store.load_unresolved(self.plan.plan_id)), 1)

        second_runner = HistoricalBackfillRunner(
            connector,
            self.snapshot_store,
            self.progress_store,
            gap_store=gap_store,
            clock=lambda: RUN_CLOCK,
        )
        progress = second_runner.run(
            self.plan, maximum_requests=1, quarantine_empty_responses=True
        )

        self.assertEqual(connector.calls.count(self.gapped_request.request_id), 1)
        self.assertEqual(
            {value.request_id for value in progress.completions},
            {self.safe_request.request_id},
        )

    def test_lineage_mismatches_abort_before_a_gap_is_accepted(self) -> None:
        target = self.gapped_request

        def make_error(**overrides):
            values = dict(
                provider=target.binding.provider,
                provider_version="fake-gap-connector/v1",
                provider_instrument_id=target.binding.provider_instrument_id,
                session=target.sessions[-1],
                observed_at=target.requested_at,
                normalized_response_sha256="c" * 64,
            )
            values.update(overrides)
            return HistoricalEmptyProviderResponseError(**values)

        cases = {
            "wrong_provider": make_error(provider="ZERODHA_KITE"),
            "wrong_provider_version": make_error(provider_version="other-connector/v1"),
            "wrong_provider_instrument_id": make_error(provider_instrument_id="999999"),
            "session_outside_request": make_error(session=date(2099, 1, 1)),
            "pre_request_observed_at": make_error(
                observed_at=target.requested_at - timedelta(days=1)
            ),
        }
        for name, error in cases.items():
            with self.subTest(case=name):
                gap_store = LocalHistoricalBackfillSessionGapStore(
                    self.root / "gaps" / name
                )
                progress_store = LocalHistoricalBackfillProgressStore(
                    self.root / "progress" / name
                )
                connector = FakeGapConnector({target.request_id: error})
                runner = HistoricalBackfillRunner(
                    connector,
                    self.snapshot_store,
                    progress_store,
                    gap_store=gap_store,
                    clock=lambda: RUN_CLOCK,
                )

                with self.assertRaises(HistoricalBackfillIntegrityError):
                    runner.run(self.plan, quarantine_empty_responses=True)

                self.assertEqual(gap_store.load_unresolved(self.plan.plan_id), ())

        malformed_hash_error = make_error()
        malformed_hash_error.normalized_response_sha256 = "not-a-hash"
        gap_store = LocalHistoricalBackfillSessionGapStore(
            self.root / "gaps" / "malformed_hash"
        )
        progress_store = LocalHistoricalBackfillProgressStore(
            self.root / "progress" / "malformed_hash"
        )
        connector = FakeGapConnector({target.request_id: malformed_hash_error})
        runner = HistoricalBackfillRunner(
            connector,
            self.snapshot_store,
            progress_store,
            gap_store=gap_store,
            clock=lambda: RUN_CLOCK,
        )

        with self.assertRaises(ValueError):
            runner.run(self.plan, quarantine_empty_responses=True)

        self.assertEqual(gap_store.load_unresolved(self.plan.plan_id), ())

    def test_existing_gap_lineage_is_reverified_before_provider_calls(self) -> None:
        target = self.gapped_request
        cases = {
            "wrong_provider_version": self._gap_evidence(
                target, provider_version="other-connector/v1"
            ),
            "pre_request_observed_at": self._gap_evidence(
                target,
                response_observed_at=target.requested_at - timedelta(seconds=1),
            ),
            "future_observed_at": self._gap_evidence(
                target, response_observed_at=RUN_CLOCK + timedelta(seconds=1)
            ),
        }
        for index, (name, evidence) in enumerate(cases.items()):
            with self.subTest(case=name):
                gap_store = LocalHistoricalBackfillSessionGapStore(
                    self.root / "existing-gap" / str(index)
                )
                gap_store.put(evidence)
                connector = FakeGapConnector()
                runner = HistoricalBackfillRunner(
                    connector,
                    self.snapshot_store,
                    LocalHistoricalBackfillProgressStore(
                        self.root / "existing-progress" / str(index)
                    ),
                    gap_store=gap_store,
                    clock=lambda: RUN_CLOCK,
                )

                with self.assertRaises(HistoricalBackfillStateError):
                    runner.run(self.plan, quarantine_empty_responses=True)

                self.assertEqual(connector.calls, [])

    def test_completed_request_cannot_also_have_an_unresolved_gap(self) -> None:
        connector = FakeGapConnector()
        runner = HistoricalBackfillRunner(
            connector,
            self.snapshot_store,
            self.progress_store,
            clock=lambda: RUN_CLOCK,
        )
        progress = runner.run(self.plan)
        self.assertTrue(HistoricalBackfillRunner.is_complete(self.plan, progress))

        gap_store = LocalHistoricalBackfillSessionGapStore(self.root / "overlap-gaps")
        gap_store.put(self._gap_evidence(self.gapped_request))
        connector.calls.clear()
        overlap_runner = HistoricalBackfillRunner(
            connector,
            self.snapshot_store,
            self.progress_store,
            gap_store=gap_store,
            clock=lambda: RUN_CLOCK,
        )

        with self.assertRaises(HistoricalBackfillStateError):
            overlap_runner.run(self.plan, quarantine_empty_responses=True)

        self.assertEqual(connector.calls, [])

    def test_fresh_future_empty_response_is_not_persisted(self) -> None:
        target = self.gapped_request
        error = HistoricalEmptyProviderResponseError(
            provider=target.binding.provider,
            provider_version="fake-gap-connector/v1",
            provider_instrument_id=target.binding.provider_instrument_id,
            session=target.sessions[-1],
            observed_at=RUN_CLOCK + timedelta(seconds=1),
            normalized_response_sha256="c" * 64,
        )
        connector = FakeGapConnector({target.request_id: error})
        gap_store = LocalHistoricalBackfillSessionGapStore(self.root / "future-gap")
        runner = HistoricalBackfillRunner(
            connector,
            self.snapshot_store,
            self.progress_store,
            gap_store=gap_store,
            clock=lambda: RUN_CLOCK,
        )

        with self.assertRaises(HistoricalBackfillStateError):
            runner.run(self.plan, quarantine_empty_responses=True)

        self.assertEqual(gap_store.load_unresolved(self.plan.plan_id), ())

    def test_request_rejection_requires_its_explicit_option(self) -> None:
        connector = FakeGapConnector({self.gapped_request.request_id: "reject"})
        gap_store = LocalHistoricalBackfillSessionGapStore(
            self.root / "request-rejection-default"
        )
        runner = HistoricalBackfillRunner(
            connector,
            self.snapshot_store,
            self.progress_store,
            gap_store=gap_store,
            clock=lambda: RUN_CLOCK,
        )

        with self.assertRaises(HistoricalProviderRequestRejectedError):
            runner.run(self.plan, quarantine_empty_responses=True)

        self.assertEqual(gap_store.load_unresolved(self.plan.plan_id), ())

    def test_request_rejection_is_durable_and_unrelated_work_continues(self) -> None:
        connector = FakeGapConnector({self.gapped_request.request_id: "reject"})
        gap_store = LocalHistoricalBackfillSessionGapStore(
            self.root / "request-rejection-enabled"
        )
        runner = HistoricalBackfillRunner(
            connector,
            self.snapshot_store,
            self.progress_store,
            gap_store=gap_store,
            clock=lambda: RUN_CLOCK,
        )

        progress = runner.run(
            self.plan,
            quarantine_request_rejections=True,
        )

        self.assertEqual(
            {value.request_id for value in progress.completions},
            {self.safe_request.request_id},
        )
        gaps = gap_store.load_unresolved(self.plan.plan_id)
        self.assertEqual(len(gaps), 1)
        self.assertIs(
            gaps[0].classification,
            HistoricalBackfillGapClassification.UNRESOLVED_PROVIDER_REQUEST_REJECTION,
        )
        self.assertFalse(HistoricalBackfillRunner.is_complete(self.plan, progress))

    def test_three_consecutive_request_rejections_trip_the_safety_ceiling(self) -> None:
        source_binding = self.safe_request.binding
        third_binding = HistoricalInstrumentBinding(
            exchange=source_binding.exchange,
            listing_key="NSE:ZZZTEST",
            security_series=source_binding.security_series,
            isin="INE123A01016",
            provider=source_binding.provider,
            provider_instrument_id="NSE_EQ|INE123A01016",
            valid_from=source_binding.valid_from,
            valid_through=source_binding.valid_through,
            source_snapshot_ids=source_binding.source_snapshot_ids,
        )
        third_request = HistoricalDailyRequest(
            binding=third_binding,
            sessions=self.safe_request.sessions,
            requested_at=self.safe_request.requested_at,
        )
        three_request_plan = HistoricalBackfillPlan(
            provider=self.plan.provider,
            resolver_version=self.plan.resolver_version,
            identity_registry_id=self.plan.identity_registry_id,
            calendar_snapshot_id=self.plan.calendar_snapshot_id,
            coverage_start=self.plan.coverage_start,
            coverage_end=self.plan.coverage_end,
            requested_at=self.plan.requested_at,
            requests=self.plan.requests + (third_request,),
            issues=self.plan.issues,
            identity_snapshot_id=self.plan.identity_snapshot_id,
        )
        connector = FakeGapConnector(
            {request.request_id: "reject" for request in three_request_plan.requests}
        )
        gap_store = LocalHistoricalBackfillSessionGapStore(
            self.root / "three-request-rejections"
        )
        runner = HistoricalBackfillRunner(
            connector,
            LocalMarketSnapshotStore(self.root / "three-request-snapshots"),
            LocalHistoricalBackfillProgressStore(
                self.root / "three-request-progress"
            ),
            gap_store=gap_store,
            clock=lambda: RUN_CLOCK,
        )

        with self.assertRaises(HistoricalBackfillIntegrityError):
            runner.run(
                three_request_plan,
                quarantine_request_rejections=True,
            )

        self.assertEqual(len(connector.calls), 3)
        self.assertEqual(
            len(gap_store.load_unresolved(three_request_plan.plan_id)), 3
        )


if __name__ == "__main__":
    unittest.main()
