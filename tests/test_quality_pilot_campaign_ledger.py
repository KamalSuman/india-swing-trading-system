from __future__ import annotations

import ast
import inspect
import unittest
from dataclasses import replace
from datetime import datetime, time, timedelta

from india_swing.domain.models import INDIA_STANDARD_TIME
from india_swing.quality_pilot import campaign_ledger as ledger_module
from india_swing.quality_pilot.campaign_ledger import (
    MAXIMUM_EXPECTED_CAPTURE_SPECS,
    QUALITY_PILOT_CAMPAIGN_PLAN_SCHEMA_VERSION,
    QUALITY_PILOT_COMPLETENESS_LEDGER_SCHEMA_VERSION,
    CampaignCompletenessStatus,
    QualityPilotCampaignCompletenessLedger,
    QualityPilotCampaignLedgerError,
    QualityPilotCampaignPlan,
)
from india_swing.quality_pilot.canonical_response import (
    PILOT_PROTOCOL_SHA256,
    PROVIDER_ZERODHA_KITE,
    EndpointFamily,
    ObservationWindowSpec,
    ResponseClassification,
    ScheduledWindowKind,
)
from india_swing.quality_pilot.capture_runner import (
    QualityPilotCaptureRunner,
    QualityPilotCaptureSpec,
    QualityPilotCollectionResult,
)
from tests.test_quality_pilot_canonical_response import (
    PILOT_RUN_ID,
    _catalog_payload,
    _instrument,
    _ohlcv_payload,
    _quote,
    _quote_payload,
)
from tests.test_quality_pilot_capture_runner import _campaign
from tests.test_quality_pilot_observation_store import FakeStateObjectWriter


BUCKET = "quality-pilot-test-bucket"
KEYS = ("NSE:INFY", "NSE:TCS")


def _at(session, hour: int, minute: int) -> datetime:
    return datetime.combine(session, time(hour, minute), tzinfo=INDIA_STANDARD_TIME)


def _window(session, kind: ScheduledWindowKind) -> ObservationWindowSpec:
    values = {
        ScheduledWindowKind.CATALOG_PREOPEN: (
            EndpointFamily.CATALOG,
            (8, 45),
            (9, 0),
        ),
        ScheduledWindowKind.QUOTE_0920: (
            EndpointFamily.FULL_QUOTE,
            (9, 20),
            (9, 25),
        ),
        ScheduledWindowKind.QUOTE_CLOSE: (
            EndpointFamily.FULL_QUOTE,
            (15, 40),
            (16, 0),
        ),
        ScheduledWindowKind.OHLCV_CLOSE: (
            EndpointFamily.DAILY_OHLCV,
            (16, 15),
            (18, 0),
        ),
    }
    family, opens, closes = values[kind]
    return ObservationWindowSpec(
        pilot_run_id=PILOT_RUN_ID,
        market_session=session,
        window_kind=kind,
        endpoint_family=family,
        opens_at=_at(session, *opens),
        closes_at=_at(session, *closes),
        protocol_sha256=PILOT_PROTOCOL_SHA256,
    )


def _capture_spec(
    campaign,
    window: ObservationWindowSpec,
    *,
    requested_keys: tuple[str, ...],
    token: int | None,
    chunk_index: int,
    chunk_count: int,
) -> QualityPilotCaptureSpec:
    return QualityPilotCaptureSpec(
        campaign=campaign,
        window=window,
        provider=PROVIDER_ZERODHA_KITE,
        provider_version="kite-3.0",
        requested_keys=requested_keys,
        provider_instrument_token=token,
        chunk_index=chunk_index,
        chunk_count=chunk_count,
        protocol_sha256=PILOT_PROTOCOL_SHA256,
    )


def _plan() -> QualityPilotCampaignPlan:
    campaign = _campaign()
    specs: list[QualityPilotCaptureSpec] = []
    for session in campaign.confirmed_sessions:
        specs.append(
            _capture_spec(
                campaign,
                _window(session, ScheduledWindowKind.CATALOG_PREOPEN),
                requested_keys=(),
                token=None,
                chunk_index=1,
                chunk_count=1,
            )
        )
        for kind in (
            ScheduledWindowKind.QUOTE_0920,
            ScheduledWindowKind.QUOTE_CLOSE,
        ):
            specs.append(
                _capture_spec(
                    campaign,
                    _window(session, kind),
                    requested_keys=KEYS,
                    token=None,
                    chunk_index=1,
                    chunk_count=1,
                )
            )
        ohlcv_window = _window(session, ScheduledWindowKind.OHLCV_CLOSE)
        for index, (key, token) in enumerate(zip(KEYS, (101, 202)), start=1):
            specs.append(
                _capture_spec(
                    campaign,
                    ohlcv_window,
                    requested_keys=(key,),
                    token=token,
                    chunk_index=index,
                    chunk_count=len(KEYS),
                )
            )
    return QualityPilotCampaignPlan(campaign=campaign, capture_specs=tuple(specs))


def _prefix_plan(session_count: int) -> QualityPilotCampaignPlan:
    full = _plan()
    specs_per_session = len(full.capture_specs) // len(full.campaign.confirmed_sessions)
    return QualityPilotCampaignPlan(
        campaign=full.campaign,
        capture_specs=full.capture_specs[: session_count * specs_per_session],
    )


def _payload(spec: QualityPilotCaptureSpec):
    window = spec.window
    if window.endpoint_family is EndpointFamily.CATALOG:
        return _catalog_payload(
            window,
            (
                _instrument(token=101, symbol="INFY"),
                _instrument(token=202, symbol="TCS"),
            ),
        )
    if window.endpoint_family is EndpointFamily.FULL_QUOTE:
        quotes = tuple(
            _quote(listing_key=key, token=index + 101, window=window)
            for index, key in enumerate(spec.requested_keys)
        )
        return _quote_payload(window, spec.requested_keys, quotes)
    return _ohlcv_payload(
        window,
        window.market_session,
        token=spec.provider_instrument_token,
    )


class StaticCollector:
    def __init__(self, result: QualityPilotCollectionResult) -> None:
        self.result = result

    def collect(self, spec: QualityPilotCaptureSpec) -> QualityPilotCollectionResult:
        return self.result


def _run(
    spec: QualityPilotCaptureSpec,
    classification: ResponseClassification = ResponseClassification.SUCCESS,
):
    payload = _payload(spec) if classification is ResponseClassification.SUCCESS else None
    result = QualityPilotCollectionResult(
        request_started_at=spec.window.opens_at,
        request_ended_at=spec.window.opens_at + timedelta(seconds=1),
        response_classification=classification,
        payload=payload,
    )
    return QualityPilotCaptureRunner().run(
        spec,
        StaticCollector(result),
        BUCKET,
        FakeStateObjectWriter(),
    )


class CampaignPlanTests(unittest.TestCase):
    def test_exact_twenty_session_plan_is_deterministic_and_complete(self) -> None:
        first = _plan()
        second = _plan()
        self.assertEqual(first.plan_id, second.plan_id)
        self.assertEqual(first.expected_capture_count, 100)
        self.assertEqual(first.planned_sessions, first.campaign.confirmed_sessions)
        self.assertEqual(len(first.campaign.confirmed_sessions), 20)
        self.assertEqual(MAXIMUM_EXPECTED_CAPTURE_SPECS, 700_000)
        first.verify_content_identity()

    def test_empty_and_prefix_plans_support_catalog_discovered_future_universes(self) -> None:
        campaign = _campaign()
        empty = QualityPilotCampaignPlan(campaign, ())
        first_session = _prefix_plan(1)
        self.assertEqual(empty.planned_sessions, ())
        self.assertEqual(empty.expected_capture_count, 0)
        self.assertEqual(first_session.planned_sessions, campaign.confirmed_sessions[:1])
        self.assertEqual(first_session.expected_capture_count, 5)

        object.__setattr__(first_session, "planned_sessions", campaign.confirmed_sessions[:2])
        with self.assertRaises(QualityPilotCampaignLedgerError):
            first_session.verify_content_identity()

        full = _plan()
        second_session_only = full.capture_specs[5:10]
        with self.assertRaises(QualityPilotCampaignLedgerError):
            QualityPilotCampaignPlan(campaign, second_session_only)

    def test_rejects_missing_reordered_and_duplicate_specs(self) -> None:
        plan = _plan()
        cases = (
            plan.capture_specs[:-1],
            (plan.capture_specs[1], plan.capture_specs[0]) + plan.capture_specs[2:],
            plan.capture_specs + (plan.capture_specs[-1],),
        )
        for specs in cases:
            with self.subTest(length=len(specs)), self.assertRaises(
                QualityPilotCampaignLedgerError
            ):
                QualityPilotCampaignPlan(plan.campaign, specs)

    def test_rejects_window_outside_authorized_schedule(self) -> None:
        plan = _plan()
        original = plan.capture_specs[1]
        bad_window = ObservationWindowSpec(
            pilot_run_id=PILOT_RUN_ID,
            market_session=original.window.market_session,
            window_kind=ScheduledWindowKind.QUOTE_0920,
            endpoint_family=EndpointFamily.FULL_QUOTE,
            opens_at=_at(original.window.market_session, 9, 21),
            closes_at=_at(original.window.market_session, 9, 25),
            protocol_sha256=PILOT_PROTOCOL_SHA256,
        )
        bad = replace(original, window=bad_window)
        specs = plan.capture_specs[:1] + (bad,) + plan.capture_specs[2:]
        with self.assertRaises(QualityPilotCampaignLedgerError):
            QualityPilotCampaignPlan(plan.campaign, specs)

    def test_public_schedule_validator_accepts_and_rejects_the_same_gates(self) -> None:
        from india_swing.quality_pilot.campaign_ledger import is_window_inside_authorized_schedule

        plan = _plan()
        good_window = plan.capture_specs[0].window
        self.assertTrue(is_window_inside_authorized_schedule(good_window))

        bad_window = ObservationWindowSpec(
            pilot_run_id=PILOT_RUN_ID,
            market_session=good_window.market_session,
            window_kind=ScheduledWindowKind.CATALOG_PREOPEN,
            endpoint_family=EndpointFamily.CATALOG,
            opens_at=_at(good_window.market_session, 0, 1),
            closes_at=_at(good_window.market_session, 0, 2),
            protocol_sha256=PILOT_PROTOCOL_SHA256,
        )
        self.assertFalse(is_window_inside_authorized_schedule(bad_window))

    def test_public_schedule_validator_fails_closed_on_wrong_type(self) -> None:
        from india_swing.quality_pilot.campaign_ledger import is_window_inside_authorized_schedule

        self.assertFalse(is_window_inside_authorized_schedule(None))
        self.assertFalse(is_window_inside_authorized_schedule("not-a-window"))

    def test_rejects_cross_route_universe_or_provider_token_mismatch(self) -> None:
        plan = _plan()
        close_index = 2
        close = replace(plan.capture_specs[close_index], requested_keys=("NSE:INFY",))
        with self.assertRaises(QualityPilotCampaignLedgerError):
            QualityPilotCampaignPlan(
                plan.campaign,
                plan.capture_specs[:close_index]
                + (close,)
                + plan.capture_specs[close_index + 1 :],
            )

        second_ohlcv_index = 4
        duplicate_token = replace(
            plan.capture_specs[second_ohlcv_index], provider_instrument_token=101
        )
        with self.assertRaises(QualityPilotCampaignLedgerError):
            QualityPilotCampaignPlan(
                plan.campaign,
                plan.capture_specs[:second_ohlcv_index]
                + (duplicate_token,)
                + plan.capture_specs[second_ohlcv_index + 1 :],
            )


class CompletenessLedgerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.plan = _plan()
        cls.success_runs = tuple(_run(spec) for spec in cls.plan.capture_specs)

    def test_not_started_due_incomplete_and_in_progress_are_distinct(self) -> None:
        before = self.plan.capture_specs[0].window.opens_at - timedelta(seconds=1)
        empty = QualityPilotCampaignCompletenessLedger(
            self.plan, (), before, BUCKET
        )
        self.assertEqual(empty.status, CampaignCompletenessStatus.NOT_STARTED)
        self.assertEqual(empty.pending_capture_count, 100)
        self.assertEqual(empty.missing_due_capture_count, 0)
        self.assertEqual(empty.unplanned_session_count, 0)

        first_session = self.plan.campaign.confirmed_sessions[0]
        after_first = _at(first_session, 18, 0)
        missing = QualityPilotCampaignCompletenessLedger(
            self.plan, (), after_first, BUCKET
        )
        self.assertEqual(missing.status, CampaignCompletenessStatus.DUE_INCOMPLETE)
        self.assertEqual(missing.missing_due_capture_count, 5)
        self.assertEqual(missing.fully_due_session_count, 1)

        progress = QualityPilotCampaignCompletenessLedger(
            self.plan, self.success_runs[:5], after_first, BUCKET
        )
        self.assertEqual(progress.status, CampaignCompletenessStatus.IN_PROGRESS)
        self.assertEqual(progress.missing_due_capture_count, 0)
        self.assertEqual(progress.pending_capture_count, 95)
        self.assertEqual(progress.completed_session_count, 1)

    def test_unplanned_future_session_is_not_claimed_but_overdue_is_incomplete(self) -> None:
        plan = _prefix_plan(1)
        first_runs = tuple(_run(spec) for spec in plan.capture_specs)
        first_session = plan.campaign.confirmed_sessions[0]
        first_complete = QualityPilotCampaignCompletenessLedger(
            plan, first_runs, _at(first_session, 18, 0), BUCKET
        )
        self.assertEqual(first_complete.status, CampaignCompletenessStatus.IN_PROGRESS)
        self.assertEqual(first_complete.planned_session_count, 1)
        self.assertEqual(first_complete.unplanned_session_count, 19)
        self.assertEqual(first_complete.unplanned_due_sessions, ())
        self.assertEqual(first_complete.completed_session_count, 1)
        self.assertEqual(first_complete.fully_due_session_count, 1)

        second_session = plan.campaign.confirmed_sessions[1]
        still_future = QualityPilotCampaignCompletenessLedger(
            plan, first_runs, _at(second_session, 9, 24), BUCKET
        )
        self.assertEqual(still_future.status, CampaignCompletenessStatus.IN_PROGRESS)
        overdue = QualityPilotCampaignCompletenessLedger(
            plan, first_runs, _at(second_session, 9, 25), BUCKET
        )
        self.assertEqual(overdue.status, CampaignCompletenessStatus.DUE_INCOMPLETE)
        self.assertEqual(overdue.unplanned_due_sessions, (second_session,))

    def test_all_outcomes_complete_means_review_ready_not_research_eligible(self) -> None:
        evaluated_at = _at(self.plan.campaign.confirmed_sessions[-1], 18, 0)
        ledger = QualityPilotCampaignCompletenessLedger(
            self.plan, self.success_runs, evaluated_at, BUCKET
        )
        self.assertEqual(ledger.status, CampaignCompletenessStatus.OUTCOMES_COMPLETE)
        self.assertTrue(ledger.ready_for_aggregate_quality_review)
        self.assertEqual(ledger.completed_capture_count, 100)
        self.assertEqual(ledger.completed_session_count, 20)
        self.assertEqual(ledger.fully_due_session_count, 20)
        self.assertEqual(ledger.classified_gap_count, 0)
        self.assertEqual(ledger.published_object_count, 100)
        self.assertGreater(ledger.published_encoded_byte_count, 0)
        self.assertFalse(ledger.research_partition_eligible)
        self.assertFalse(ledger.paper_trade_eligible)
        self.assertFalse(ledger.capital_eligible)
        ledger.verify_content_identity()

    def test_classified_gap_is_counted_without_becoming_success(self) -> None:
        gap = _run(
            self.plan.capture_specs[0],
            ResponseClassification.CATALOG_GAP,
        )
        runs = (gap,) + self.success_runs[1:]
        ledger = QualityPilotCampaignCompletenessLedger(
            self.plan,
            runs,
            _at(self.plan.campaign.confirmed_sessions[-1], 18, 0),
            BUCKET,
        )
        counts = {
            value.classification: value.count for value in ledger.classification_counts
        }
        self.assertEqual(counts[ResponseClassification.CATALOG_GAP], 1)
        self.assertEqual(counts[ResponseClassification.SUCCESS], 99)
        self.assertEqual(ledger.classified_gap_count, 1)
        self.assertTrue(ledger.ready_for_aggregate_quality_review)

    def test_rejects_duplicate_reordered_foreign_bucket_and_future_outcomes(self) -> None:
        first = self.success_runs[0]
        second = self.success_runs[1]
        after_all = _at(self.plan.campaign.confirmed_sessions[-1], 18, 0)
        cases = (
            ((first, first), after_all, BUCKET),
            ((second, first), after_all, BUCKET),
            ((first,), after_all, "other-quality-bucket"),
            ((first,), first.observation.request.request_ended_at - timedelta(microseconds=1), BUCKET),
        )
        for runs, evaluated_at, bucket in cases:
            with self.subTest(bucket=bucket, count=len(runs)), self.assertRaises(
                QualityPilotCampaignLedgerError
            ):
                QualityPilotCampaignCompletenessLedger(
                    self.plan, runs, evaluated_at, bucket
                )

    def test_successful_catalog_must_equal_the_planned_session_universe(self) -> None:
        spec = self.plan.capture_specs[0]
        mismatched_payload = _catalog_payload(
            spec.window, (_instrument(token=101, symbol="INFY"),)
        )
        result = QualityPilotCollectionResult(
            request_started_at=spec.window.opens_at,
            request_ended_at=spec.window.opens_at + timedelta(seconds=1),
            response_classification=ResponseClassification.SUCCESS,
            payload=mismatched_payload,
        )
        mismatched_run = QualityPilotCaptureRunner().run(
            spec,
            StaticCollector(result),
            BUCKET,
            FakeStateObjectWriter(),
        )
        with self.assertRaises(QualityPilotCampaignLedgerError):
            QualityPilotCampaignCompletenessLedger(
                self.plan,
                (mismatched_run,),
                _at(self.plan.campaign.confirmed_sessions[0], 9, 0),
                BUCKET,
            )

    def test_tampered_nested_and_ledger_id_are_rejected(self) -> None:
        ledger = QualityPilotCampaignCompletenessLedger(
            self.plan,
            self.success_runs[:5],
            _at(self.plan.campaign.confirmed_sessions[0], 18, 0),
            BUCKET,
        )
        object.__setattr__(ledger, "ledger_id", "0" * 64)
        with self.assertRaises(QualityPilotCampaignLedgerError):
            ledger.verify_content_identity()

        ledger = QualityPilotCampaignCompletenessLedger(
            self.plan,
            self.success_runs[:5],
            _at(self.plan.campaign.confirmed_sessions[0], 18, 0),
            BUCKET,
        )
        object.__setattr__(ledger, "completed_session_count", 19)
        with self.assertRaises(QualityPilotCampaignLedgerError):
            ledger.verify_content_identity()


class PostureAndCapabilityTests(unittest.TestCase):
    def test_versions_and_posture_are_pinned(self) -> None:
        plan = _plan()
        ledger = QualityPilotCampaignCompletenessLedger(
            plan, (), plan.capture_specs[0].window.opens_at - timedelta(seconds=1), BUCKET
        )
        self.assertEqual(
            QUALITY_PILOT_CAMPAIGN_PLAN_SCHEMA_VERSION,
            "quality_pilot_campaign_plan_v1",
        )
        self.assertEqual(
            QUALITY_PILOT_COMPLETENESS_LEDGER_SCHEMA_VERSION,
            "quality_pilot_completeness_ledger_v1",
        )
        for value in (plan, ledger):
            self.assertTrue(value.quality_only)
            for name in ledger_module._POSTURE_NAMES:
                self.assertEqual(getattr(value, name), name == "quality_only")

    def test_module_has_no_clock_storage_network_or_trading_capability(self) -> None:
        source = inspect.getsource(ledger_module)
        tree = ast.parse(source)
        forbidden_modules = {
            "os", "pathlib", "socket", "subprocess", "requests", "urllib",
            "httpx", "google", "kiteconnect", "sqlite3", "pickle", "shelve",
        }
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0])
        self.assertEqual(imported & forbidden_modules, set())
        lowered = source.lower()
        for token in (
            "datetime.now(", "utcnow(", "getenv(", "environ", "sleep(",
            "list_blobs(", ".delete(", "fetch_instruments(", "fetch_full_quotes(",
            "fetch_daily_candle(", "place_order(", "generate_signal(",
            "run_paper_trade(",
        ):
            self.assertNotIn(token, lowered, msg=token)


if __name__ == "__main__":
    unittest.main()
