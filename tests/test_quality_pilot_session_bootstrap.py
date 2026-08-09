from __future__ import annotations

import ast
import inspect
import unittest
from dataclasses import replace
from datetime import datetime, time, timedelta, timezone

from decimal import Decimal

from india_swing.daily_pipeline.acquisition import GCSObjectPayload
from india_swing.daily_pipeline.state_publication import PublishedStateObject
from india_swing.domain.models import INDIA_STANDARD_TIME
from india_swing.market_data.models import DailyCandle, DailyCandleBatch, InstrumentBatch, NseSessionFinality
from india_swing.quality_pilot import session_bootstrap as bootstrap_module
from india_swing.quality_pilot.canonical_response import (
    MAXIMUM_QUOTE_REQUEST_KEYS,
    EndpointFamily,
    ResponseClassification,
    ScheduledWindowKind,
)
from india_swing.quality_pilot.capture_runner import (
    QualityPilotCaptureRunner,
    QualityPilotCollectionResult,
)
from india_swing.quality_pilot.control_plane_store import (
    decode_quality_pilot_campaign_plan,
    decode_quality_pilot_completeness_snapshot,
    pinned_quality_pilot_control_artifact_request,
    pinned_quality_pilot_ledger_transition_request,
    publish_quality_pilot_control_artifact,
    publish_quality_pilot_ledger_transition,
)
from india_swing.quality_pilot.resumable_service import (
    QualityPilotResumableCaptureRequest,
    QualityPilotResumableCaptureService,
)
from india_swing.quality_pilot.session_bootstrap import (
    QUALITY_PILOT_SESSION_BOOTSTRAP_REQUEST_SCHEMA_VERSION,
    QUALITY_PILOT_SESSION_BOOTSTRAP_RESULT_SCHEMA_VERSION,
    QualityPilotSessionBootstrapError,
    QualityPilotSessionBootstrapRequest,
    QualityPilotSessionBootstrapResult,
    QualityPilotSessionBootstrapService,
    _derive_k_eq_universe,
)
from tests.test_quality_pilot_campaign_ledger import BUCKET, _capture_spec, _window
from tests.test_quality_pilot_canonical_response import (
    _catalog_payload,
    _instrument,
    _quote,
    _quote_payload,
)
from tests.test_quality_pilot_capture_runner import _campaign
from tests.test_quality_pilot_observation_store import FakeStateObjectWriter


class FakeReader:
    def __init__(self, store: dict) -> None:
        self.store = store
        self.calls: list[tuple[str, str, int]] = []
        self.raise_error: Exception | None = None
        self.malicious_result: object = None

    def read_generation(self, *, bucket, object_name, generation, maximum_bytes):
        self.calls.append((bucket, object_name, generation))
        if self.raise_error is not None:
            raise self.raise_error
        if self.malicious_result is not None:
            return self.malicious_result
        key = (bucket, object_name, generation)
        if key not in self.store:
            raise KeyError("no such pinned object in the fake reader store")
        return GCSObjectPayload(content_bytes=self.store[key], generation=generation)


class SharedFakeWriter(FakeStateObjectWriter):
    def __init__(self, reader_store: dict) -> None:
        super().__init__()
        self.reader_store = reader_store

    def create_or_verify(self, *, bucket, object_name, content_bytes, content_type, maximum_bytes):
        result = super().create_or_verify(
            bucket=bucket, object_name=object_name, content_bytes=content_bytes,
            content_type=content_type, maximum_bytes=maximum_bytes,
        )
        if self.malicious_result is None and self.raise_error is None:
            stored_bytes, _ = self.store[(bucket, object_name)]
            self.reader_store[(bucket, object_name, result.generation)] = stored_bytes
        return result


class StaticCollector:
    def __init__(self, result) -> None:
        self.result = result
        self.calls = 0

    def collect(self, spec):
        self.calls += 1
        return self.result


def _catalog_spec(campaign, session):
    return _capture_spec(
        campaign, _window(session, ScheduledWindowKind.CATALOG_PREOPEN),
        requested_keys=(), token=None, chunk_index=1, chunk_count=1,
    )


def _catalog_run_result(campaign, session, instruments, writer, *, offset_seconds=1):
    spec = _catalog_spec(campaign, session)
    payload = _catalog_payload(spec.window, instruments)
    collection = QualityPilotCollectionResult(
        request_started_at=spec.window.opens_at,
        request_ended_at=spec.window.opens_at + timedelta(seconds=offset_seconds),
        response_classification=ResponseClassification.SUCCESS,
        payload=payload,
    )
    return QualityPilotCaptureRunner().run(spec, StaticCollector(collection), BUCKET, writer)


def _bootstrap_request(campaign, catalog_run_result, session, *, prior_plan_pin=None, prior_transition_pin=None, **overrides):
    kwargs = dict(
        campaign=campaign,
        catalog_run_result=catalog_run_result,
        quote_0920_window=_window(session, ScheduledWindowKind.QUOTE_0920),
        quote_close_window=_window(session, ScheduledWindowKind.QUOTE_CLOSE),
        ohlcv_close_window=_window(session, ScheduledWindowKind.OHLCV_CLOSE),
        bucket=BUCKET,
        prior_plan_pin=prior_plan_pin,
        prior_transition_pin=prior_transition_pin,
    )
    kwargs.update(overrides)
    return QualityPilotSessionBootstrapRequest(**kwargs)


def _payload_for(spec, start):
    if spec.window.endpoint_family is EndpointFamily.FULL_QUOTE:
        quotes = tuple(
            _quote(listing_key=key, token=200 + i, window=spec.window) for i, key in enumerate(spec.requested_keys)
        )
        return _quote_payload(spec.window, spec.requested_keys, quotes, requested_at=start, observed_at=start)
    finality = NseSessionFinality.regular_collection_guard(spec.window.market_session)
    candle = DailyCandle(
        instrument_token=spec.provider_instrument_token,
        timestamp=datetime.combine(spec.window.market_session, time(0, 0), tzinfo=INDIA_STANDARD_TIME),
        open=Decimal("100"), high=Decimal("110"), low=Decimal("90"), close=Decimal("105"),
        volume=100, open_interest=None,
    )
    return DailyCandleBatch(
        instrument_token=spec.provider_instrument_token, session_finality=finality,
        observed_at=start, provider_version="kite-3.0", candles=(candle,),
    )


class GenericCollector:
    def __init__(self, start) -> None:
        self.start = start
        self.calls = 0

    def collect(self, spec):
        self.calls += 1
        return QualityPilotCollectionResult(
            request_started_at=self.start,
            request_ended_at=self.start + timedelta(seconds=1),
            response_classification=ResponseClassification.SUCCESS,
            payload=_payload_for(spec, self.start),
        )


def _complete_plan(plan, plan_pin, first_published_transition, reader_store, writer):
    """Drive the real resumable service through every spec after the first
    (already-completed catalog) one, returning the final published_transition."""

    current_transition_pub = first_published_transition
    last_evaluated_at = plan.capture_specs[0].window.opens_at + timedelta(seconds=1)
    service = QualityPilotResumableCaptureService()
    for index in range(1, len(plan.capture_specs)):
        spec = plan.capture_specs[index]
        invocation_at = max(spec.window.opens_at, last_evaluated_at)
        transition_pin = pinned_quality_pilot_ledger_transition_request(current_transition_pub)
        request = QualityPilotResumableCaptureRequest(
            plan_pin=plan_pin, predecessor_transition_pin=transition_pin,
            target_capture_spec_id=spec.capture_spec_id, invocation_at=invocation_at,
        )
        result = service.run(request, FakeReader(reader_store), GenericCollector(invocation_at), writer)
        current_transition_pub = result.published_transition
        last_evaluated_at = invocation_at + timedelta(seconds=1)
    return current_transition_pub


class Fixture:
    def __init__(self, *, instrument_count: int = 3) -> None:
        self.campaign = _campaign()
        self.reader_store: dict = {}
        self.writer = SharedFakeWriter(self.reader_store)
        self.service = QualityPilotSessionBootstrapService()
        self.instruments = tuple(
            _instrument(token=100 + i, symbol=f"SYM{i:03d}") for i in range(instrument_count)
        )

    def reader(self) -> FakeReader:
        return FakeReader(self.reader_store)

    def genesis_catalog_run(self, session=None, *, instruments=None, offset_seconds=1):
        session = session or self.campaign.confirmed_sessions[0]
        return _catalog_run_result(
            self.campaign, session, instruments or self.instruments, self.writer, offset_seconds=offset_seconds
        )

    def bootstrap_genesis(self, catalog_run_result=None, **overrides):
        session = self.campaign.confirmed_sessions[0]
        catalog_run_result = catalog_run_result or self.genesis_catalog_run(session)
        request = _bootstrap_request(self.campaign, catalog_run_result, session, **overrides)
        return self.service.run(request, self.reader(), self.writer), request

    def complete_session(self, bootstrap_result):
        plan_key = (bootstrap_result.published_plan.bucket, bootstrap_result.published_plan.object_name, bootstrap_result.published_plan.generation)
        plan = decode_quality_pilot_campaign_plan(self.reader_store[plan_key])
        plan_pin = pinned_quality_pilot_control_artifact_request(bootstrap_result.published_plan)
        return _complete_plan(plan, plan_pin, bootstrap_result.published_transition, self.reader_store, self.writer), plan, plan_pin

    def bootstrap_extension(self, prior_bootstrap_result, prior_final_transition_pub, prior_plan_pin, session, **overrides):
        catalog_run_result = _catalog_run_result(self.campaign, session, self.instruments, self.writer)
        prior_transition_pin = pinned_quality_pilot_ledger_transition_request(prior_final_transition_pub)
        request = _bootstrap_request(
            self.campaign, catalog_run_result, session,
            prior_plan_pin=prior_plan_pin, prior_transition_pin=prior_transition_pin, **overrides
        )
        return self.service.run(request, self.reader(), self.writer), request


class GenesisHappyPathTests(unittest.TestCase):
    def test_genesis_produces_one_session_complete_plan_and_zero_reads(self) -> None:
        fixture = Fixture()
        reader = fixture.reader()
        catalog_run_result = fixture.genesis_catalog_run()
        request = _bootstrap_request(fixture.campaign, catalog_run_result, fixture.campaign.confirmed_sessions[0])
        writer_calls_before = len(fixture.writer.calls)
        result = fixture.service.run(request, reader, fixture.writer)

        self.assertEqual(len(reader.calls), 0)
        self.assertEqual(len(fixture.writer.calls) - writer_calls_before, 3)
        result.verify_content_identity()
        self.assertIsNone(result.previous_plan_id)
        self.assertIsNone(result.previous_snapshot_id)
        self.assertEqual(result.target_session, fixture.campaign.confirmed_sessions[0])

        plan_key = (result.published_plan.bucket, result.published_plan.object_name, result.published_plan.generation)
        plan = decode_quality_pilot_campaign_plan(fixture.reader_store[plan_key])
        self.assertEqual(plan.planned_sessions, (fixture.campaign.confirmed_sessions[0],))
        # catalog + 1 quote_0920 chunk + 1 quote_close chunk + 3 ohlcv = 6
        self.assertEqual(len(plan.capture_specs), 6)

        snap_key = (result.published_snapshot.bucket, result.published_snapshot.object_name, result.published_snapshot.generation)
        snapshot = decode_quality_pilot_completeness_snapshot(fixture.reader_store[snap_key])
        self.assertEqual(len(snapshot.completed_capture_spec_ids), 1)
        self.assertEqual(len(snapshot.pending_capture_spec_ids), 5)
        self.assertEqual(snapshot.missing_due_capture_spec_ids, ())

        first_quote_spec = plan.capture_specs[1]
        self.assertEqual(result.next_capture_spec_id, first_quote_spec.capture_spec_id)
        self.assertEqual(first_quote_spec.window.window_kind, ScheduledWindowKind.QUOTE_0920)

    def test_resumable_service_remains_three_reads_after_genesis_bootstrap(self) -> None:
        fixture = Fixture()
        result, request = fixture.bootstrap_genesis()
        plan_pin = pinned_quality_pilot_control_artifact_request(result.published_plan)
        transition_pin = pinned_quality_pilot_ledger_transition_request(result.published_transition)
        target_window = _window(fixture.campaign.confirmed_sessions[0], ScheduledWindowKind.QUOTE_0920)
        resumable_request = QualityPilotResumableCaptureRequest(
            plan_pin=plan_pin, predecessor_transition_pin=transition_pin,
            target_capture_spec_id=result.next_capture_spec_id, invocation_at=target_window.opens_at,
        )
        reader = FakeReader(fixture.reader_store)
        resumable_result = QualityPilotResumableCaptureService().run(
            resumable_request, reader, GenericCollector(target_window.opens_at), fixture.writer
        )
        resumable_result.verify_content_identity()
        self.assertEqual(len(reader.calls), 3)


class ExtensionHappyPathTests(unittest.TestCase):
    def test_extension_preserves_prior_state_and_appends_next_session(self) -> None:
        fixture = Fixture()
        bootstrap0, _ = fixture.bootstrap_genesis()
        final_transition_pub, plan0, plan_pin0 = fixture.complete_session(bootstrap0)

        session1 = fixture.campaign.confirmed_sessions[1]
        reader1 = FakeReader(fixture.reader_store)
        catalog_run1 = _catalog_run_result(fixture.campaign, session1, fixture.instruments, fixture.writer)
        prior_transition_pin = pinned_quality_pilot_ledger_transition_request(final_transition_pub)
        request1 = _bootstrap_request(
            fixture.campaign, catalog_run1, session1,
            prior_plan_pin=plan_pin0, prior_transition_pin=prior_transition_pin,
        )
        writer_calls_before = len(fixture.writer.calls)
        bootstrap1 = fixture.service.run(request1, reader1, fixture.writer)

        bootstrap1.verify_content_identity()
        self.assertEqual(len(fixture.writer.calls) - writer_calls_before, 3)
        # audit replay reads every prior observation (6) plus plan+transition+snapshot loads (3) = 9
        self.assertEqual(len(reader1.calls), 9)
        self.assertEqual(bootstrap1.previous_plan_id, plan0.plan_id)

        plan_key1 = (bootstrap1.published_plan.bucket, bootstrap1.published_plan.object_name, bootstrap1.published_plan.generation)
        plan1 = decode_quality_pilot_campaign_plan(fixture.reader_store[plan_key1])
        self.assertEqual(len(plan1.capture_specs), len(plan0.capture_specs) * 2)
        self.assertEqual(plan1.capture_specs[: len(plan0.capture_specs)], plan0.capture_specs)

    def test_resumable_service_remains_three_reads_after_extension_bootstrap(self) -> None:
        fixture = Fixture()
        bootstrap0, _ = fixture.bootstrap_genesis()
        final_transition_pub, plan0, plan_pin0 = fixture.complete_session(bootstrap0)
        session1 = fixture.campaign.confirmed_sessions[1]
        bootstrap1, _ = fixture.bootstrap_extension(bootstrap0, final_transition_pub, plan_pin0, session1)

        plan_pin1 = pinned_quality_pilot_control_artifact_request(bootstrap1.published_plan)
        transition_pin1 = pinned_quality_pilot_ledger_transition_request(bootstrap1.published_transition)
        target_window = _window(session1, ScheduledWindowKind.QUOTE_0920)
        resumable_request = QualityPilotResumableCaptureRequest(
            plan_pin=plan_pin1, predecessor_transition_pin=transition_pin1,
            target_capture_spec_id=bootstrap1.next_capture_spec_id, invocation_at=target_window.opens_at,
        )
        reader = FakeReader(fixture.reader_store)
        resumable_result = QualityPilotResumableCaptureService().run(
            resumable_request, reader, GenericCollector(target_window.opens_at), fixture.writer
        )
        resumable_result.verify_content_identity()
        self.assertEqual(len(reader.calls), 3)


class UniverseTests(unittest.TestCase):
    def test_unordered_catalog_rows_are_sorted_by_listing_key(self) -> None:
        fixture = Fixture(instrument_count=0)
        unordered = (
            _instrument(token=103, symbol="ZZZ"),
            _instrument(token=101, symbol="AAA"),
            _instrument(token=102, symbol="MMM"),
        )
        catalog_run = fixture.genesis_catalog_run(instruments=unordered)
        result, _ = fixture.bootstrap_genesis(catalog_run)
        plan_key = (result.published_plan.bucket, result.published_plan.object_name, result.published_plan.generation)
        plan = decode_quality_pilot_campaign_plan(fixture.reader_store[plan_key])
        quote_spec = plan.capture_specs[1]
        self.assertEqual(quote_spec.requested_keys, ("NSE:AAA", "NSE:MMM", "NSE:ZZZ"))

    def test_non_eq_instruments_are_excluded(self) -> None:
        # The accepted canonical_response.py layer already requires every
        # instrument in a valid catalog SUCCESS payload to satisfy the
        # strictly narrower is_nse_eq_record (exchange, segment, AND
        # instrument_type all NSE/EQ) before the observation itself can be
        # constructed at all -- so a genuine QualityPilotCaptureRunResult can
        # never carry a non-EQ instrument in the first place. This unit-tests
        # _derive_k_eq_universe's own frozen two-field filter directly
        # against a hand-built InstrumentBatch, independent of that upstream
        # enforcement.
        observed_at = datetime(2026, 7, 15, 3, 15, tzinfo=timezone.utc)
        mixed = (
            _instrument(token=101, symbol="AAA", instrument_type="EQ"),
            _instrument(token=102, symbol="BBB", instrument_type="FUT"),
        )
        payload = InstrumentBatch(exchange="NSE", observed_at=observed_at, provider_version="kite-3.0", instruments=mixed)
        eligible = _derive_k_eq_universe(payload)
        self.assertEqual(tuple(item.listing_key for item in eligible), ("NSE:AAA",))

    def test_single_key_universe(self) -> None:
        fixture = Fixture(instrument_count=1)
        result, _ = fixture.bootstrap_genesis()
        plan_key = (result.published_plan.bucket, result.published_plan.object_name, result.published_plan.generation)
        plan = decode_quality_pilot_campaign_plan(fixture.reader_store[plan_key])
        # catalog + 1 quote_0920 + 1 quote_close + 1 ohlcv = 4
        self.assertEqual(len(plan.capture_specs), 4)

    def test_exact_quote_ceiling_keys_produce_one_chunk(self) -> None:
        fixture = Fixture(instrument_count=MAXIMUM_QUOTE_REQUEST_KEYS)
        result, _ = fixture.bootstrap_genesis()
        plan_key = (result.published_plan.bucket, result.published_plan.object_name, result.published_plan.generation)
        plan = decode_quality_pilot_campaign_plan(fixture.reader_store[plan_key])
        quote_0920_specs = [
            spec for spec in plan.capture_specs if spec.window.window_kind is ScheduledWindowKind.QUOTE_0920
        ]
        self.assertEqual(len(quote_0920_specs), 1)
        self.assertEqual(len(quote_0920_specs[0].requested_keys), MAXIMUM_QUOTE_REQUEST_KEYS)

    def test_ceiling_plus_one_keys_produce_two_chunks(self) -> None:
        fixture = Fixture(instrument_count=MAXIMUM_QUOTE_REQUEST_KEYS + 1)
        result, _ = fixture.bootstrap_genesis()
        plan_key = (result.published_plan.bucket, result.published_plan.object_name, result.published_plan.generation)
        plan = decode_quality_pilot_campaign_plan(fixture.reader_store[plan_key])
        quote_0920_specs = sorted(
            (spec for spec in plan.capture_specs if spec.window.window_kind is ScheduledWindowKind.QUOTE_0920),
            key=lambda spec: spec.chunk_index,
        )
        self.assertEqual(len(quote_0920_specs), 2)
        self.assertEqual(quote_0920_specs[0].chunk_index, 1)
        self.assertEqual(quote_0920_specs[1].chunk_index, 2)
        self.assertEqual(len(quote_0920_specs[0].requested_keys) + len(quote_0920_specs[1].requested_keys), MAXIMUM_QUOTE_REQUEST_KEYS + 1)

    def test_identical_morning_close_and_ohlcv_universes(self) -> None:
        fixture = Fixture(instrument_count=5)
        result, _ = fixture.bootstrap_genesis()
        plan_key = (result.published_plan.bucket, result.published_plan.object_name, result.published_plan.generation)
        plan = decode_quality_pilot_campaign_plan(fixture.reader_store[plan_key])
        morning_keys = tuple(
            key for spec in plan.capture_specs if spec.window.window_kind is ScheduledWindowKind.QUOTE_0920 for key in spec.requested_keys
        )
        close_keys = tuple(
            key for spec in plan.capture_specs if spec.window.window_kind is ScheduledWindowKind.QUOTE_CLOSE for key in spec.requested_keys
        )
        ohlcv_keys = tuple(
            key for spec in plan.capture_specs if spec.window.window_kind is ScheduledWindowKind.OHLCV_CLOSE for key in spec.requested_keys
        )
        self.assertEqual(morning_keys, close_keys)
        self.assertEqual(morning_keys, ohlcv_keys)


class RejectionTests(unittest.TestCase):
    def test_rejects_empty_eligible_universe(self) -> None:
        # As with the non-EQ-filter unit test above: a genuine catalog
        # QualityPilotCaptureRunResult can never carry an all-non-EQ catalog
        # (canonical_response.py already requires is_nse_eq_record for every
        # instrument), so this is exercised directly against
        # _derive_k_eq_universe with a hand-built InstrumentBatch.
        observed_at = datetime(2026, 7, 15, 3, 15, tzinfo=timezone.utc)
        non_eq = (_instrument(token=101, symbol="AAA", instrument_type="FUT"),)
        payload = InstrumentBatch(exchange="NSE", observed_at=observed_at, provider_version="kite-3.0", instruments=non_eq)
        with self.assertRaises(QualityPilotSessionBootstrapError):
            _derive_k_eq_universe(payload)

    def test_rejects_non_success_catalog_classification(self) -> None:
        fixture = Fixture()
        spec = _catalog_spec(fixture.campaign, fixture.campaign.confirmed_sessions[0])
        gap_result = QualityPilotCollectionResult(
            request_started_at=spec.window.opens_at, request_ended_at=spec.window.opens_at + timedelta(seconds=1),
            response_classification=ResponseClassification.CATALOG_GAP, payload=None,
        )
        catalog_run = QualityPilotCaptureRunner().run(spec, StaticCollector(gap_result), BUCKET, fixture.writer)
        with self.assertRaises(QualityPilotSessionBootstrapError):
            fixture.bootstrap_genesis(catalog_run)

    def test_rejects_wrong_target_session_for_genesis(self) -> None:
        fixture = Fixture()
        catalog_run = fixture.genesis_catalog_run(session=fixture.campaign.confirmed_sessions[1])
        request = _bootstrap_request(fixture.campaign, catalog_run, fixture.campaign.confirmed_sessions[1])
        with self.assertRaises(QualityPilotSessionBootstrapError):
            fixture.service.run(request, fixture.reader(), fixture.writer)

    def test_rejects_wrong_bucket_shape(self) -> None:
        fixture = Fixture()
        catalog_run = fixture.genesis_catalog_run()
        with self.assertRaises(QualityPilotSessionBootstrapError):
            _bootstrap_request(fixture.campaign, catalog_run, fixture.campaign.confirmed_sessions[0], bucket="X")

    def test_rejects_bucket_mismatching_the_catalog_run_result(self) -> None:
        # A validly-shaped bucket that simply disagrees with the bucket the
        # catalog was actually captured/published under.
        fixture = Fixture()
        catalog_run = fixture.genesis_catalog_run()
        request = _bootstrap_request(
            fixture.campaign, catalog_run, fixture.campaign.confirmed_sessions[0], bucket="a-different-quality-bucket"
        )
        with self.assertRaises(QualityPilotSessionBootstrapError):
            fixture.service.run(request, fixture.reader(), fixture.writer)

    def test_rejects_only_one_predecessor_pin_present(self) -> None:
        fixture = Fixture()
        bootstrap0, _ = fixture.bootstrap_genesis()
        plan_pin = pinned_quality_pilot_control_artifact_request(bootstrap0.published_plan)
        catalog_run1 = _catalog_run_result(fixture.campaign, fixture.campaign.confirmed_sessions[1], fixture.instruments, fixture.writer)
        with self.assertRaises(QualityPilotSessionBootstrapError):
            _bootstrap_request(
                fixture.campaign, catalog_run1, fixture.campaign.confirmed_sessions[1],
                prior_plan_pin=plan_pin, prior_transition_pin=None,
            )

    def test_rejects_incomplete_predecessor_plan(self) -> None:
        fixture = Fixture()
        bootstrap0, _ = fixture.bootstrap_genesis()
        plan_pin0 = pinned_quality_pilot_control_artifact_request(bootstrap0.published_plan)
        transition_pin0 = pinned_quality_pilot_ledger_transition_request(bootstrap0.published_transition)
        session1 = fixture.campaign.confirmed_sessions[1]
        catalog_run1 = _catalog_run_result(fixture.campaign, session1, fixture.instruments, fixture.writer)
        request = _bootstrap_request(
            fixture.campaign, catalog_run1, session1,
            prior_plan_pin=plan_pin0, prior_transition_pin=transition_pin0,
        )
        with self.assertRaises(QualityPilotSessionBootstrapError):
            fixture.service.run(request, fixture.reader(), fixture.writer)

    def test_rejects_target_session_that_skips_a_session(self) -> None:
        fixture = Fixture()
        bootstrap0, _ = fixture.bootstrap_genesis()
        final_transition_pub, plan0, plan_pin0 = fixture.complete_session(bootstrap0)
        session2 = fixture.campaign.confirmed_sessions[2]  # skip session 1
        catalog_run2 = _catalog_run_result(fixture.campaign, session2, fixture.instruments, fixture.writer)
        prior_transition_pin = pinned_quality_pilot_ledger_transition_request(final_transition_pub)
        request = _bootstrap_request(
            fixture.campaign, catalog_run2, session2,
            prior_plan_pin=plan_pin0, prior_transition_pin=prior_transition_pin,
        )
        with self.assertRaises(QualityPilotSessionBootstrapError):
            fixture.service.run(request, fixture.reader(), fixture.writer)

    def test_rejects_corrupted_prior_observation(self) -> None:
        fixture = Fixture()
        bootstrap0, _ = fixture.bootstrap_genesis()
        final_transition_pub, plan0, plan_pin0 = fixture.complete_session(bootstrap0)

        session1 = fixture.campaign.confirmed_sessions[1]
        catalog_run1 = _catalog_run_result(fixture.campaign, session1, fixture.instruments, fixture.writer)
        prior_transition_pin = pinned_quality_pilot_ledger_transition_request(final_transition_pub)

        # Corrupt one of session 0's observation objects (paths under
        # quality-pilot/v1/<pilot_run_id>/<session>/... rather than the
        # .../control/... control-artifact lane) so audit replay must fail.
        corrupted_key = None
        for (bucket, object_name, generation) in list(fixture.reader_store):
            if "/control/" not in object_name:
                corrupted_key = (bucket, object_name, generation)
                break
        self.assertIsNotNone(corrupted_key)
        fixture.reader_store[corrupted_key] = b"not-json-at-all"

        request = _bootstrap_request(
            fixture.campaign, catalog_run1, session1,
            prior_plan_pin=plan_pin0, prior_transition_pin=prior_transition_pin,
        )
        with self.assertRaises(QualityPilotSessionBootstrapError):
            fixture.service.run(request, fixture.reader(), fixture.writer)


class AdversarialPublicationTests(unittest.TestCase):
    def test_malicious_plan_writer_result_is_rejected(self) -> None:
        fixture = Fixture()
        catalog_run = fixture.genesis_catalog_run()
        request = _bootstrap_request(fixture.campaign, catalog_run, fixture.campaign.confirmed_sessions[0])
        fixture.writer.malicious_result = PublishedStateObject(
            object_name="wrong/path.json", generation=1, byte_count=1, sha256="0" * 64
        )
        with self.assertRaises(QualityPilotSessionBootstrapError):
            fixture.service.run(request, fixture.reader(), fixture.writer)

    def test_writer_exception_is_sanitized(self) -> None:
        secret = "SECRET-BOOTSTRAP-WRITER/C:/private"
        fixture = Fixture()
        catalog_run = fixture.genesis_catalog_run()
        request = _bootstrap_request(fixture.campaign, catalog_run, fixture.campaign.confirmed_sessions[0])
        fixture.writer.raise_error = RuntimeError(secret)
        with self.assertRaises(QualityPilotSessionBootstrapError) as raised:
            fixture.service.run(request, fixture.reader(), fixture.writer)
        self.assertNotIn(secret, str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)
        self.assertIsNone(raised.exception.__context__)

    def test_forged_result_transition_byte_count_is_rejected(self) -> None:
        fixture = Fixture()
        result, _ = fixture.bootstrap_genesis()
        forged = replace(result.published_transition, encoded_byte_count=1)
        self._assert_forged_result_rejected(result, forged)

    def test_forged_result_transition_hash_is_rejected(self) -> None:
        fixture = Fixture()
        result, _ = fixture.bootstrap_genesis()
        forged = replace(result.published_transition, encoded_sha256="0" * 64)
        self._assert_forged_result_rejected(result, forged)

    def _assert_forged_result_rejected(self, result, forged_publication) -> None:
        self._assert_forged_result_construction_rejected(result, published_transition=forged_publication)

    def _assert_forged_result_construction_rejected(self, result, **overrides) -> None:
        kwargs = dict(
            request_id=result.request_id,
            request=result.request,
            target_session=result.target_session,
            catalog_observation_id=result.catalog_observation_id,
            catalog_run_result_id=result.catalog_run_result_id,
            catalog_capture_spec_id=result.catalog_capture_spec_id,
            previous_plan_id=result.previous_plan_id,
            previous_snapshot_id=result.previous_snapshot_id,
            predecessor_plan=result.predecessor_plan,
            predecessor_transition=result.predecessor_transition,
            predecessor_snapshot=result.predecessor_snapshot,
            plan_id=result.plan_id,
            plan=result.plan,
            published_plan=result.published_plan,
            snapshot_id=result.snapshot_id,
            snapshot=result.snapshot,
            published_snapshot=result.published_snapshot,
            transition=result.transition,
            transition_id=result.transition_id,
            published_transition=result.published_transition,
            next_capture_spec_id=result.next_capture_spec_id,
        )
        kwargs.update(overrides)
        construct_failed = False
        forged_result = None
        try:
            forged_result = QualityPilotSessionBootstrapResult(**kwargs)
        except Exception:
            construct_failed = True
        verify_ok = False
        if forged_result is not None:
            try:
                forged_result.verify_content_identity()
                verify_ok = True
            except Exception:
                verify_ok = False
        self.assertTrue(construct_failed or not verify_ok)

    def test_forged_target_session_is_rejected(self) -> None:
        fixture = Fixture()
        result, _ = fixture.bootstrap_genesis()
        forged_session = result.target_session + timedelta(days=365)
        self._assert_forged_result_construction_rejected(result, target_session=forged_session)

    def test_forged_catalog_observation_id_is_rejected(self) -> None:
        fixture = Fixture()
        result, _ = fixture.bootstrap_genesis()
        self._assert_forged_result_construction_rejected(result, catalog_observation_id="0" * 64)

    def test_forged_next_capture_spec_id_is_rejected(self) -> None:
        fixture = Fixture()
        result, _ = fixture.bootstrap_genesis()
        self._assert_forged_result_construction_rejected(result, next_capture_spec_id="1" * 64)

    def test_combined_forged_target_session_observation_and_next_spec_is_rejected(self) -> None:
        # Reproduces Codex's exact QP-SESSION-BOOTSTRAP-R1-BLOCKER-001 finding:
        # target_session, catalog_observation_id, and next_capture_spec_id
        # forged simultaneously via dataclasses.replace() on a valid result.
        fixture = Fixture()
        result, _ = fixture.bootstrap_genesis()
        result.verify_content_identity()  # the valid result is itself accepted

        replace_failed = False
        forged = None
        try:
            forged = replace(
                result,
                target_session=result.target_session + timedelta(days=365),
                catalog_observation_id="0" * 64,
                next_capture_spec_id="1" * 64,
            )
        except Exception:
            replace_failed = True
        verify_ok = False
        if forged is not None:
            try:
                forged.verify_content_identity()
                verify_ok = True
            except Exception:
                verify_ok = False
        self.assertTrue(replace_failed or not verify_ok)

    def test_swapping_plan_between_two_independent_branches_is_rejected(self) -> None:
        fixture_a = Fixture()
        result_a, _ = fixture_a.bootstrap_genesis()
        fixture_b = Fixture(instrument_count=5)
        result_b, _ = fixture_b.bootstrap_genesis()
        self._assert_forged_result_construction_rejected(result_a, plan=result_b.plan, plan_id=result_b.plan.plan_id)

    def test_swapping_snapshot_between_two_independent_branches_is_rejected(self) -> None:
        fixture_a = Fixture()
        result_a, _ = fixture_a.bootstrap_genesis()
        fixture_b = Fixture(instrument_count=5)
        result_b, _ = fixture_b.bootstrap_genesis()
        self._assert_forged_result_construction_rejected(
            result_a, snapshot=result_b.snapshot, snapshot_id=result_b.snapshot.snapshot_id
        )

    def test_swapping_request_between_two_independent_branches_is_rejected(self) -> None:
        fixture_a = Fixture()
        result_a, _ = fixture_a.bootstrap_genesis()
        fixture_b = Fixture(instrument_count=5)
        result_b, _ = fixture_b.bootstrap_genesis()
        self._assert_forged_result_construction_rejected(
            result_a, request=result_b.request, request_id=result_b.request.request_id
        )

    def test_swapping_transition_between_two_independent_branches_is_rejected(self) -> None:
        fixture_a = Fixture()
        result_a, _ = fixture_a.bootstrap_genesis()
        fixture_b = Fixture(instrument_count=5)
        result_b, _ = fixture_b.bootstrap_genesis()
        self._assert_forged_result_construction_rejected(
            result_a, transition=result_b.transition, transition_id=result_b.transition.transition_id,
            published_transition=result_b.published_transition,
        )

    def test_swapping_published_plan_between_two_independent_branches_is_rejected(self) -> None:
        fixture_a = Fixture()
        result_a, _ = fixture_a.bootstrap_genesis()
        fixture_b = Fixture(instrument_count=5)
        result_b, _ = fixture_b.bootstrap_genesis()
        self._assert_forged_result_construction_rejected(result_a, published_plan=result_b.published_plan)

    def test_swapping_published_snapshot_between_two_independent_branches_is_rejected(self) -> None:
        fixture_a = Fixture()
        result_a, _ = fixture_a.bootstrap_genesis()
        fixture_b = Fixture(instrument_count=5)
        result_b, _ = fixture_b.bootstrap_genesis()
        self._assert_forged_result_construction_rejected(result_a, published_snapshot=result_b.published_snapshot)

    def test_forged_published_plan_byte_count_with_valid_ids_is_rejected(self) -> None:
        fixture = Fixture()
        result, _ = fixture.bootstrap_genesis()
        forged_published_plan = replace(result.published_plan, encoded_byte_count=1)
        self._assert_forged_result_construction_rejected(result, published_plan=forged_published_plan)

    def test_forged_published_plan_hash_with_valid_ids_is_rejected(self) -> None:
        fixture = Fixture()
        result, _ = fixture.bootstrap_genesis()
        forged_published_plan = replace(result.published_plan, encoded_sha256="0" * 64)
        self._assert_forged_result_construction_rejected(result, published_plan=forged_published_plan)

    def test_forged_published_snapshot_byte_count_with_valid_ids_is_rejected(self) -> None:
        fixture = Fixture()
        result, _ = fixture.bootstrap_genesis()
        forged_published_snapshot = replace(result.published_snapshot, encoded_byte_count=1)
        self._assert_forged_result_construction_rejected(result, published_snapshot=forged_published_snapshot)

    def test_forged_published_snapshot_hash_with_valid_ids_is_rejected(self) -> None:
        fixture = Fixture()
        result, _ = fixture.bootstrap_genesis()
        forged_published_snapshot = replace(result.published_snapshot, encoded_sha256="0" * 64)
        self._assert_forged_result_construction_rejected(result, published_snapshot=forged_published_snapshot)

    def test_tampering_carried_plan_after_construction_is_detected(self) -> None:
        fixture = Fixture()
        result, _ = fixture.bootstrap_genesis()
        object.__setattr__(result, "plan", result.plan)  # sanity: same object still verifies
        result.verify_content_identity()
        object.__setattr__(result, "target_session", result.target_session + timedelta(days=1))
        with self.assertRaises(QualityPilotSessionBootstrapError):
            result.verify_content_identity()

    def test_tampering_carried_snapshot_after_construction_is_detected(self) -> None:
        fixture = Fixture()
        result, _ = fixture.bootstrap_genesis()
        object.__setattr__(result.snapshot, "completed_capture_spec_ids", ("0" * 64,))
        with self.assertRaises(QualityPilotSessionBootstrapError):
            result.verify_content_identity()

    def test_tampering_carried_request_after_construction_is_detected(self) -> None:
        fixture = Fixture()
        result, _ = fixture.bootstrap_genesis()
        object.__setattr__(result.request, "bucket", "a-different-quality-bucket")
        with self.assertRaises(QualityPilotSessionBootstrapError):
            result.verify_content_identity()

    def test_tampering_carried_catalog_run_result_after_construction_is_detected(self) -> None:
        fixture = Fixture()
        result, _ = fixture.bootstrap_genesis()
        object.__setattr__(result.request.catalog_run_result, "run_result_id", "0" * 64)
        with self.assertRaises(QualityPilotSessionBootstrapError):
            result.verify_content_identity()

    def test_tampering_published_plan_object_name_is_detected(self) -> None:
        fixture = Fixture()
        result, _ = fixture.bootstrap_genesis()
        object.__setattr__(result.published_plan, "object_name", "quality-pilot/v1/tampered.json")
        with self.assertRaises(QualityPilotSessionBootstrapError):
            result.verify_content_identity()

    def test_tampering_transition_run_result_id_is_detected(self) -> None:
        fixture = Fixture()
        result, _ = fixture.bootstrap_genesis()
        object.__setattr__(result.transition, "run_result_id", "0" * 64)
        with self.assertRaises(QualityPilotSessionBootstrapError):
            result.verify_content_identity()

    def test_tampering_posture_via_setattr_is_structurally_impossible(self) -> None:
        fixture = Fixture()
        result, _ = fixture.bootstrap_genesis()
        with self.assertRaises(AttributeError):
            object.__setattr__(result, "quality_only", False)


OTHER_BUCKET = "another-quality-bucket"


class PredecessorAndBucketAdversarialTests(unittest.TestCase):
    """Reproduces Codex's revision-2 QP-SESSION-BOOTSTRAP-R2-BLOCKER-002
    (forged predecessor snapshot lineage) and QP-SESSION-BOOTSTRAP-R2-
    BLOCKER-003 (forged publication bucket) findings, plus their required
    isolated-field/prefix-swap/individual-bucket-mismatch coverage."""

    def _extension_bootstrap(self, instrument_count: int = 3):
        fixture = Fixture(instrument_count=instrument_count)
        bootstrap0, _ = fixture.bootstrap_genesis()
        final_transition_pub, plan0, plan_pin0 = fixture.complete_session(bootstrap0)
        session1 = fixture.campaign.confirmed_sessions[1]
        bootstrap1, _ = fixture.bootstrap_extension(bootstrap0, final_transition_pub, plan_pin0, session1)
        bootstrap1.verify_content_identity()
        return bootstrap1, fixture

    def _assert_construction_rejected(self, result, **overrides) -> None:
        kwargs = dict(
            request_id=result.request_id,
            request=result.request,
            target_session=result.target_session,
            catalog_observation_id=result.catalog_observation_id,
            catalog_run_result_id=result.catalog_run_result_id,
            catalog_capture_spec_id=result.catalog_capture_spec_id,
            previous_plan_id=result.previous_plan_id,
            previous_snapshot_id=result.previous_snapshot_id,
            predecessor_plan=result.predecessor_plan,
            predecessor_transition=result.predecessor_transition,
            predecessor_snapshot=result.predecessor_snapshot,
            plan_id=result.plan_id,
            plan=result.plan,
            published_plan=result.published_plan,
            snapshot_id=result.snapshot_id,
            snapshot=result.snapshot,
            published_snapshot=result.published_snapshot,
            transition=result.transition,
            transition_id=result.transition_id,
            published_transition=result.published_transition,
            next_capture_spec_id=result.next_capture_spec_id,
        )
        kwargs.update(overrides)
        construct_failed = False
        forged_result = None
        try:
            forged_result = QualityPilotSessionBootstrapResult(**kwargs)
        except Exception:
            construct_failed = True
        verify_ok = False
        if forged_result is not None:
            try:
                forged_result.verify_content_identity()
                verify_ok = True
            except Exception:
                verify_ok = False
        self.assertTrue(construct_failed or not verify_ok)

    def test_forged_extension_previous_snapshot_id_is_rejected(self) -> None:
        # Reproduces Codex's exact QP-SESSION-BOOTSTRAP-R2-BLOCKER-002
        # finding: previous_snapshot_id forged to '0'*64, with a freshly
        # rebuilt (internally self-consistent) transition/publication
        # around it -- must still be rejected because it disagrees with
        # the carried predecessor transition/snapshot.
        result, fixture = self._extension_bootstrap()
        forged_transition = replace(result.transition, previous_snapshot_id="0" * 64)
        forged_bytes_writer = fixture.writer
        forged_published_transition = publish_quality_pilot_ledger_transition(
            forged_transition, BUCKET, forged_bytes_writer
        )
        self._assert_construction_rejected(
            result,
            previous_snapshot_id="0" * 64,
            transition=forged_transition,
            transition_id=forged_transition.transition_id,
            published_transition=forged_published_transition,
        )

    def test_swapping_predecessor_plan_between_two_branches_is_rejected(self) -> None:
        result_a, _ = self._extension_bootstrap(instrument_count=3)
        result_b, _ = self._extension_bootstrap(instrument_count=5)
        self._assert_construction_rejected(result_a, predecessor_plan=result_b.predecessor_plan)

    def test_swapping_predecessor_transition_between_two_branches_is_rejected(self) -> None:
        result_a, _ = self._extension_bootstrap(instrument_count=3)
        result_b, _ = self._extension_bootstrap(instrument_count=5)
        self._assert_construction_rejected(result_a, predecessor_transition=result_b.predecessor_transition)

    def test_swapping_predecessor_snapshot_between_two_branches_is_rejected(self) -> None:
        result_a, _ = self._extension_bootstrap(instrument_count=3)
        result_b, _ = self._extension_bootstrap(instrument_count=5)
        self._assert_construction_rejected(result_a, predecessor_snapshot=result_b.predecessor_snapshot)

    def test_swapping_prior_plan_pin_between_two_branches_is_rejected(self) -> None:
        result_a, _ = self._extension_bootstrap(instrument_count=3)
        result_b, _ = self._extension_bootstrap(instrument_count=5)
        forged_request = replace(result_a.request, prior_plan_pin=result_b.request.prior_plan_pin)
        self._assert_construction_rejected(result_a, request=forged_request)

    def test_forged_predecessor_snapshot_bucket_is_rejected(self) -> None:
        result, _ = self._extension_bootstrap()
        forged_pins = tuple(
            replace(pin, bucket=OTHER_BUCKET) for pin in result.predecessor_snapshot.pinned_observations
        )
        forged_predecessor_snapshot = replace(
            result.predecessor_snapshot, bucket=OTHER_BUCKET, pinned_observations=forged_pins
        )
        self._assert_construction_rejected(result, predecessor_snapshot=forged_predecessor_snapshot)

    def test_forged_bucket_moved_consistently_across_all_current_publications_is_rejected(self) -> None:
        # Reproduces Codex's exact QP-SESSION-BOOTSTRAP-R2-BLOCKER-003
        # finding: published_plan, published_snapshot, transition.next_snapshot,
        # and published_transition all moved together to a different valid
        # bucket with correctly recomputed canonical bytes/hash/path, while
        # request.bucket remains the original bucket -- must be rejected.
        fixture = Fixture()
        result, _ = fixture.bootstrap_genesis()
        forged_pins = tuple(replace(pin, bucket=OTHER_BUCKET) for pin in result.snapshot.pinned_observations)
        forged_snapshot = replace(result.snapshot, bucket=OTHER_BUCKET, pinned_observations=forged_pins)
        forged_published_snapshot = publish_quality_pilot_control_artifact(forged_snapshot, OTHER_BUCKET, fixture.writer)
        forged_transition = replace(result.transition, next_snapshot=forged_published_snapshot)
        forged_published_transition = publish_quality_pilot_ledger_transition(forged_transition, OTHER_BUCKET, fixture.writer)
        forged_published_plan = publish_quality_pilot_control_artifact(result.plan, OTHER_BUCKET, fixture.writer)
        self._assert_construction_rejected(
            result,
            published_plan=forged_published_plan,
            snapshot=forged_snapshot,
            snapshot_id=forged_snapshot.snapshot_id,
            published_snapshot=forged_published_snapshot,
            transition=forged_transition,
            transition_id=forged_transition.transition_id,
            published_transition=forged_published_transition,
        )

    def test_forged_published_plan_bucket_alone_is_rejected(self) -> None:
        fixture = Fixture()
        result, _ = fixture.bootstrap_genesis()
        forged_published_plan = replace(result.published_plan, bucket=OTHER_BUCKET)
        self._assert_construction_rejected(result, published_plan=forged_published_plan)

    def test_forged_published_snapshot_bucket_alone_is_rejected(self) -> None:
        fixture = Fixture()
        result, _ = fixture.bootstrap_genesis()
        forged_published_snapshot = replace(result.published_snapshot, bucket=OTHER_BUCKET)
        self._assert_construction_rejected(result, published_snapshot=forged_published_snapshot)

    def test_forged_published_transition_bucket_alone_is_rejected(self) -> None:
        fixture = Fixture()
        result, _ = fixture.bootstrap_genesis()
        forged_published_transition = replace(result.published_transition, bucket=OTHER_BUCKET)
        self._assert_construction_rejected(result, published_transition=forged_published_transition)


class IdempotencyAndConcurrencyTests(unittest.TestCase):
    def test_exact_retry_returns_identical_identities(self) -> None:
        fixture = Fixture()
        catalog_run = fixture.genesis_catalog_run()
        request = _bootstrap_request(fixture.campaign, catalog_run, fixture.campaign.confirmed_sessions[0])
        first = fixture.service.run(request, fixture.reader(), fixture.writer)
        second = fixture.service.run(request, fixture.reader(), fixture.writer)
        self.assertEqual(first.result_id, second.result_id)
        self.assertEqual(first.plan_id, second.plan_id)
        self.assertEqual(first.snapshot_id, second.snapshot_id)
        self.assertEqual(first.transition_id, second.transition_id)
        self.assertEqual(first.published_plan.generation, second.published_plan.generation)

    def test_competing_catalog_result_at_the_same_route_fails_closed(self) -> None:
        fixture = Fixture()
        catalog_run_a = fixture.genesis_catalog_run(offset_seconds=1)
        request_a = _bootstrap_request(fixture.campaign, catalog_run_a, fixture.campaign.confirmed_sessions[0])
        result_a = fixture.service.run(request_a, fixture.reader(), fixture.writer)
        result_a.verify_content_identity()

        # A different (but still valid) catalog observation for the SAME
        # session/window -- different observation timestamp, so a
        # different run_result_id/plan_id/snapshot_id/transition_id, but
        # the transition path is derived only from (predecessor, capture
        # spec), which is identical for both attempts.
        catalog_run_b = fixture.genesis_catalog_run(offset_seconds=2)
        request_b = _bootstrap_request(fixture.campaign, catalog_run_b, fixture.campaign.confirmed_sessions[0])
        with self.assertRaises(QualityPilotSessionBootstrapError):
            fixture.service.run(request_b, fixture.reader(), fixture.writer)


class RegressionAndCapabilityTests(unittest.TestCase):
    def test_versions_are_pinned(self) -> None:
        self.assertEqual(
            QUALITY_PILOT_SESSION_BOOTSTRAP_REQUEST_SCHEMA_VERSION, "quality_pilot_session_bootstrap_request_v1"
        )
        self.assertEqual(
            QUALITY_PILOT_SESSION_BOOTSTRAP_RESULT_SCHEMA_VERSION, "quality_pilot_session_bootstrap_result_v1"
        )

    def test_module_has_no_clock_filesystem_network_or_trading_capability(self) -> None:
        source = inspect.getsource(bootstrap_module)
        tree = ast.parse(source)
        forbidden_modules = {
            "os", "pathlib", "socket", "subprocess", "requests", "urllib", "httpx",
            "google", "kiteconnect", "time", "random", "threading", "asyncio",
            "sqlite3", "pickle", "shelve",
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
            "datetime.now(", "utcnow(", "getenv(", "environ", "sleep(", "retry",
            "list_blobs(", ".delete(", ".overwrite(", "fetch_instruments(",
            "fetch_full_quotes(", "fetch_daily_candle(", "place_order(",
            "generate_signal(", "run_paper_trade(", "cloud_run", "scheduler.",
            "backgroundscheduler", "crontab", "kronos", "openai", "anthropic",
            "kiteconnect", "collect(",
        ):
            self.assertNotIn(token, lowered, msg=token)

    def test_module_defines_no_scheduler_clock_or_collector_helpers(self) -> None:
        source = inspect.getsource(bootstrap_module)
        tree = ast.parse(source)
        defined_names = {
            node.name.lower()
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.ClassDef))
        }
        self.assertEqual(
            defined_names & {"scheduler", "clock", "now", "sleep", "retry", "collector"}, set()
        )

    def test_all_public_values_have_fixed_quality_only_posture(self) -> None:
        fixture = Fixture()
        result, request = fixture.bootstrap_genesis()
        for value in (request, result):
            self.assertTrue(value.quality_only)
            for name in bootstrap_module._POSTURE_NAMES:
                self.assertEqual(getattr(value, name), name == "quality_only")


if __name__ == "__main__":
    unittest.main()
